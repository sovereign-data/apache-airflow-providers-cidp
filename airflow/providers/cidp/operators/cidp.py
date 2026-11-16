"""Job, project-run, deployment and SQL-statement operators (cf. the Databricks provider's operators)."""

from __future__ import annotations

import re
import time
from collections.abc import Sequence
from functools import cached_property
from typing import Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import (
    CidpHook,
    RunState,
    SQLStatementState,
    idempotency_key,
)
from airflow.providers.cidp.hooks.cidp_base import DEFAULT_CONN_ID
from airflow.providers.cidp.plugins.cidp import (
    XCOM_RUN_ID_KEY,
    XCOM_RUN_PAGE_URL_KEY,
    XCOM_STATEMENT_ID_KEY,
    CidpJobRunLink,
)
from airflow.providers.cidp.triggers.cidp import (
    CidpExecutionTrigger,
    CidpSQLStatementExecutionTrigger,
)
from airflow.providers.cidp.version_compat import BaseOperator, Context

_TAG_UNSAFE = re.compile(r"[^A-Za-z0-9_.:\-]")


def _default_deferrable() -> bool:
    return conf.getboolean("operators", "default_deferrable", fallback=False)


def task_idempotency_key(context: Context, *parts: Any) -> str:
    """A key unique to this task try: a repeat of the same try gets the same run, a retry a new one."""
    ti = context["ti"]
    return idempotency_key(
        "airflow", ti.dag_id, ti.task_id, ti.run_id, getattr(ti, "map_index", -1), ti.try_number, *parts
    )


def airflow_query_tag(context: Context) -> str:
    """``airflow:<dag_id>.<task_id>``, cut to the API's query-tag pattern."""
    ti = context["ti"]
    return _TAG_UNSAFE.sub("_", f"airflow:{ti.dag_id}.{ti.task_id}")[:128]


class _CidpOperator(BaseOperator):
    def __init__(
        self,
        *,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        workspace: str | None = None,
        cidp_retry_limit: int = 3,
        cidp_retry_delay: float = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cidp_conn_id = cidp_conn_id
        self.workspace = workspace
        self.cidp_retry_limit = cidp_retry_limit
        self.cidp_retry_delay = cidp_retry_delay

    @cached_property
    def hook(self) -> CidpHook:
        return CidpHook(
            self.cidp_conn_id,
            workspace=self.workspace,
            retry_limit=self.cidp_retry_limit,
            retry_delay=self.cidp_retry_delay,
        )


class _RunOperator(_CidpOperator):
    """Start a run, push its id and page, and wait for its end (polling or deferred)."""

    operator_extra_links = (CidpJobRunLink(),)

    def __init__(
        self,
        *,
        polling_period_seconds: int = 30,
        wait_for_termination: bool = True,
        deferrable: bool | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.polling_period_seconds = polling_period_seconds
        self.wait_for_termination = wait_for_termination
        self.deferrable = _default_deferrable() if deferrable is None else deferrable
        self.run_id: str | None = None

    # subclass hooks
    project: str
    job: str | None = None
    repair_run: bool = False

    def _page_url(self, run_id: str) -> str:
        if self.job:
            return self.hook.get_run_page_url(self.project, self.job, run_id)
        return self.hook.get_project_page_url(self.project)

    def _get_run(self, run_id: str) -> dict:
        if self.job:
            return self.hook.get_run(self.project, self.job, run_id)
        return self.hook.get_project_run(self.project, run_id)

    def _publish(self, context: Context, run_id: str) -> str:
        self.run_id = run_id
        page = self._page_url(run_id)
        ti = context["ti"]
        ti.xcom_push(key=XCOM_RUN_ID_KEY, value=run_id)
        ti.xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=page)
        self.log.info("run %s: %s", run_id, page)
        return page

    def _wait(self, context: Context, run: dict) -> str:
        """Wait for ``run`` (as just started) to end; repair it once when asked. Answers the run id."""
        run_id = str(run["run_id"])
        page = self._publish(context, run_id)
        if not self.wait_for_termination:
            return run_id
        if self.deferrable and not RunState.from_run(run).is_terminal:
            self._defer(run_id, page)
        while not RunState.from_run(run).is_terminal:
            time.sleep(self.polling_period_seconds)
            run = self._get_run(run_id)
        state = RunState.from_run(run)
        if not state.is_successful and self._should_repair(state, run.get("repair_of")):
            return self._wait(context, self._repair(context, run_id))
        self._raise_if_failed(run_id, state)
        return run_id

    def _defer(self, run_id: str, page: str) -> None:
        self.defer(
            trigger=CidpExecutionTrigger(
                run_id,
                self.cidp_conn_id,
                project=self.project,
                job=self.job,
                workspace=self.workspace,
                polling_period_seconds=self.polling_period_seconds,
                retry_limit=self.cidp_retry_limit,
                retry_delay=self.cidp_retry_delay,
                run_page_url=page,
                repair_run=self.repair_run,
            ),
            method_name="execute_complete",
            # Without a timeout a deferred wait on a hung run never ends.
            timeout=self.execution_timeout,
        )

    def _should_repair(self, state: RunState, repair_of: str | None) -> bool:
        # One repair per task try: a run that is itself a repair is not repaired again.
        return bool(self.job and self.repair_run and state.status == "failed" and not repair_of)

    def _repair(self, context: Context, run_id: str) -> dict:
        if self.job is None:  # pragma: no cover - set in __init__
            raise AirflowException("job is required")
        self.log.info("run %s failed; repairing its failed tasks", run_id)
        return self.hook.repair_run(
            self.project, self.job, run_id, idempotency_token=task_idempotency_key(context, "repair", run_id)
        )

    def _raise_if_failed(self, run_id: str, state: RunState) -> None:
        if not state.is_successful:
            what = f"job {self.project}/{self.job}" if self.job else f"project {self.project}"
            raise AirflowException(f"{what} run {run_id} ended {state.status}: {state.message}".rstrip(": "))

    def execute_complete(self, context: Context, event: dict) -> str:
        run_id = str(event["run_id"])
        state = RunState(str(event.get("status", "")), str(event.get("message") or ""))
        if not state.is_successful and self._should_repair(state, event.get("repair_of")):
            repaired = self._repair(context, run_id)
            return self._wait(context, repaired)
        self._raise_if_failed(run_id, state)
        return run_id

    def on_kill(self) -> None:
        if not self.run_id:
            return
        try:
            if self.job:
                self.hook.cancel_run(self.project, self.job, self.run_id)
            else:
                self.hook.cancel_project_run(self.project, self.run_id)
            self.log.info("cancelled run %s", self.run_id)
        except Exception:  # noqa: BLE001 — best effort, the task is being killed anyway
            self.log.warning("failed to cancel run %s", self.run_id)


class CidpRunNowOperator(_RunOperator):
    """Run a deployed job now (cf. ``DatabricksRunNowOperator``).

    The run executes as the job's identity (its ``run_as``), not as the connection.

    :param project: the project slug that holds the job.
    :param job: the job's key.
    :param job_parameters: values for the job's declared parameters (sent as strings).
    :param idempotency_token: the ``Idempotency-Key``; by default one per task try, so a repeat
        of the same request answers the same run and an Airflow retry starts a new one.
    :param repair_run: on failure, retry the failed tasks and their dependants once (Databricks' repair).
    """

    template_fields: Sequence[str] = (
        "project",
        "job",
        "job_parameters",
        "idempotency_token",
        "workspace",
        "cidp_conn_id",
    )
    template_fields_renderers = {"job_parameters": "json"}  # noqa: RUF012 — Airflow's convention

    def __init__(
        self,
        *,
        project: str,
        job: str,
        job_parameters: dict[str, Any] | None = None,
        idempotency_token: str | None = None,
        repair_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project = project
        self.job = job
        self.job_parameters = job_parameters
        self.idempotency_token = idempotency_token
        self.repair_run = repair_run

    def execute(self, context: Context) -> str:
        if self.job is None:  # pragma: no cover - set in __init__
            raise AirflowException("job is required")
        run = self.hook.run_now(
            self.project,
            self.job,
            job_parameters=self.job_parameters,
            idempotency_token=self.idempotency_token
            or task_idempotency_key(context, "run-now", self.workspace, self.project, self.job),
        )
        return self._wait(context, run)


class CidpProjectActionOperator(_RunOperator):
    """Run an action on a project file: ``dbt.build``, ``python.run``, ``notebook.run_all``, ``sql.run``...

    The one-off counterpart of a job (cf. ``DatabricksSubmitRunOperator`` / ``DatabricksNotebookOperator``):
    it runs as the connection's identity on the branch's files as they are now. SQL actions answer
    at once with their result, which the operator returns; the others start a run it waits for.
    """

    template_fields: Sequence[str] = (
        "project",
        "action",
        "branch",
        "path",
        "selection",
        "namespace",
        "action_params",
        "catalog",
        "workspace",
        "cidp_conn_id",
    )

    def __init__(
        self,
        *,
        project: str,
        action: str,
        path: str,
        branch: str = "main",
        selection: str | None = None,
        namespace: str | None = None,
        action_params: dict[str, Any] | None = None,
        catalog: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project = project
        self.action = action
        self.path = path
        self.branch = branch
        self.selection = selection
        self.namespace = namespace
        # ``params`` is BaseOperator's own argument: the action's parameters go as ``action_params``.
        self.action_params = action_params
        self.catalog = catalog

    def execute(self, context: Context) -> Any:
        result = self.hook.run_project_action(
            self.project,
            self.action,
            branch=self.branch,
            path=self.path,
            selection=self.selection,
            namespace=self.namespace,
            params=self.action_params,
            catalog=self.catalog,
        )
        run = result.get("run")
        if run is None:  # a SQL action: its result or plan, nothing to wait for
            query = result.get("query") or {}
            if query.get("error"):
                raise AirflowException(f"action {self.action} on {self.path} failed: {query['error']}")
            return {k: result.get(k) for k in ("action", "query", "plan")}
        return self._wait(context, run)


class CidpDeployJobsOperator(_CidpOperator):
    """Deploy a project's jobs from its bundle (cf. ``DatabricksCreateJobsOperator``).

    Jobs are defined in the project (``databricks.yml`` bundle or job files), not in the DAG:
    this deploys the bundle ``target`` and fails when the deployment reports errors.

    :param validate_only: only validate the deployment (nothing changes).
    :param fail_on_warnings: fail when the deployment reports warnings too.
    """

    template_fields: Sequence[str] = ("project", "target", "branch", "workspace", "cidp_conn_id")

    def __init__(
        self,
        *,
        project: str,
        target: str | None = None,
        branch: str | None = None,
        validate_only: bool = False,
        fail_on_warnings: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project = project
        self.target = target
        self.branch = branch
        self.validate_only = validate_only
        self.fail_on_warnings = fail_on_warnings

    def execute(self, context: Context) -> dict:
        if self.validate_only:
            out = self.hook.validate_deployment(self.project, target=self.target, branch=self.branch)
        else:
            out = self.hook.deploy(self.project, target=self.target)
        problems = list(out.get("errors") or [])
        if self.fail_on_warnings:
            problems += list(out.get("warnings") or [])
        for warning in out.get("warnings") or []:
            self.log.warning("deployment warning: %s", warning)
        if problems:
            raise AirflowException(f"deployment of {self.project} reported {len(problems)} problem(s): {problems}")
        self.log.info(
            "deployed %s: %d deployed, %d changed, %d undeployed",
            self.project,
            len(out.get("deployed") or []),
            len(out.get("changed") or []),
            len(out.get("undeployed") or []),
        )
        return out


class CidpSQLStatementsOperator(_CidpOperator):
    """Submit one SQL statement and wait for it (cf. ``DatabricksSQLStatementsOperator``).

    Pushes ``statement_id`` to XCom; read the rows with ``CidpHook.iter_statement_rows``
    or use ``CidpSqlOperator`` to return them.

    :param statement: the SQL.
    :param engine: ``sqe`` (default on the server) or ``spark``.
    :param timeout: seconds before the statement is cancelled and the task fails.
    :param query_tag: a tag kept with the statement; by default ``airflow:<dag_id>.<task_id>``.
    """

    template_fields: Sequence[str] = ("statement", "catalog", "schema", "query_tag", "workspace", "cidp_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"statement": "sql"}  # noqa: RUF012 — Airflow's convention

    def __init__(
        self,
        statement: str,
        *,
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        row_limit: int | None = None,
        query_tag: str | None = None,
        include_airflow_query_tags: bool = True,
        polling_period_seconds: int = 30,
        wait_for_termination: bool = True,
        timeout: float = 3600,
        deferrable: bool | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.statement = statement
        self.engine = engine
        self.catalog = catalog
        self.schema = schema
        self.row_limit = row_limit
        self.query_tag = query_tag
        self.include_airflow_query_tags = include_airflow_query_tags
        self.polling_period_seconds = polling_period_seconds
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.deferrable = _default_deferrable() if deferrable is None else deferrable
        self.statement_id: str | None = None

    def execute(self, context: Context) -> str:
        tag = self.query_tag or (airflow_query_tag(context) if self.include_airflow_query_tags else None)
        info = self.hook.submit_statement(
            self.statement,
            engine=self.engine,
            catalog=self.catalog,
            schema=self.schema,
            wait_timeout=0,
            row_limit=self.row_limit,
            query_tag=tag,
            idempotency_token=task_idempotency_key(context, "statement"),
        )
        self.statement_id = str(info["id"])
        context["ti"].xcom_push(key=XCOM_STATEMENT_ID_KEY, value=self.statement_id)
        if not self.wait_for_termination:
            return self.statement_id
        state = SQLStatementState.from_statement(info)
        if self.deferrable and not state.is_terminal:
            self.defer(
                trigger=CidpSQLStatementExecutionTrigger(
                    self.statement_id,
                    self.cidp_conn_id,
                    end_time=time.time() + self.timeout,
                    workspace=self.workspace,
                    polling_period_seconds=self.polling_period_seconds,
                    retry_limit=self.cidp_retry_limit,
                    retry_delay=self.cidp_retry_delay,
                ),
                method_name="execute_complete",
            )
        info = self.hook.wait_for_statement(info, timeout=self.timeout, long_poll=min(self.polling_period_seconds, 30))
        state = SQLStatementState.from_statement(info)
        self._raise_if_failed(state)
        return self.statement_id

    def _raise_if_failed(self, state: SQLStatementState) -> None:
        if not state.is_successful:
            raise AirflowException(
                f"statement {self.statement_id} ended {state.state}: {state.error_code} {state.error_message}".rstrip()
            )

    def execute_complete(self, context: Context, event: dict) -> str:
        self.statement_id = str(event["statement_id"])
        error = event.get("error") or {}
        self._raise_if_failed(
            SQLStatementState(
                str(event.get("state", "")),
                str(error.get("error_code") or ""),
                str(error.get("error_message") or ""),
            )
        )
        return self.statement_id

    def on_kill(self) -> None:
        if not self.statement_id:
            return
        try:
            self.hook.cancel_sql_statement(self.statement_id)
            self.log.info("cancelled statement %s", self.statement_id)
        except Exception:  # noqa: BLE001 — best effort
            self.log.warning("failed to cancel statement %s", self.statement_id)
