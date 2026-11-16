"""SQL statement sensor (cf. ``airflow.providers.databricks.sensors.databricks``)."""

from __future__ import annotations

import time
from collections.abc import Sequence
from functools import cached_property

from airflow.configuration import conf
from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import CidpHook, SQLStatementState
from airflow.providers.cidp.hooks.cidp_base import DEFAULT_CONN_ID
from airflow.providers.cidp.operators.cidp import airflow_query_tag, task_idempotency_key
from airflow.providers.cidp.plugins.cidp import XCOM_STATEMENT_ID_KEY
from airflow.providers.cidp.triggers.cidp import CidpSQLStatementExecutionTrigger
from airflow.providers.cidp.version_compat import BaseSensorOperator, Context


class CidpSQLStatementsSensor(BaseSensorOperator):
    """Wait for a SQL statement to end: submit ``statement``, or watch ``statement_id``
    (cf. ``DatabricksSQLStatementsSensor``). Fails when it ends other than ``SUCCEEDED``.
    """

    template_fields: Sequence[str] = (
        "statement",
        "statement_id",
        "catalog",
        "schema",
        "query_tag",
        "workspace",
        "cidp_conn_id",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"statement": "sql"}  # noqa: RUF012 — Airflow's convention

    def __init__(
        self,
        *,
        statement: str | None = None,
        statement_id: str | None = None,
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        query_tag: str | None = None,
        include_airflow_query_tags: bool = True,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        workspace: str | None = None,
        polling_period_seconds: int = 30,
        cidp_retry_limit: int = 3,
        cidp_retry_delay: float = 1,
        deferrable: bool | None = None,
        **kwargs,
    ):
        if bool(statement) == bool(statement_id):
            raise ValueError("pass exactly one of statement and statement_id")
        super().__init__(**kwargs)
        self.statement = statement
        self.statement_id = statement_id
        self.engine = engine
        self.catalog = catalog
        self.schema = schema
        self.query_tag = query_tag
        self.include_airflow_query_tags = include_airflow_query_tags
        self.cidp_conn_id = cidp_conn_id
        self.workspace = workspace
        self.polling_period_seconds = polling_period_seconds
        self.cidp_retry_limit = cidp_retry_limit
        self.cidp_retry_delay = cidp_retry_delay
        self._submitted = False  # only a statement this sensor submitted is cancelled on kill
        self.deferrable = (
            conf.getboolean("operators", "default_deferrable", fallback=False) if deferrable is None else deferrable
        )

    @cached_property
    def hook(self) -> CidpHook:
        return CidpHook(
            self.cidp_conn_id,
            workspace=self.workspace,
            retry_limit=self.cidp_retry_limit,
            retry_delay=self.cidp_retry_delay,
        )

    def _submit(self, context: Context) -> None:
        if self.statement_id:
            return
        if self.statement is None:  # pragma: no cover - set in __init__
            raise AirflowException("statement is required")
        tag = self.query_tag or (airflow_query_tag(context) if self.include_airflow_query_tags else None)
        info = self.hook.submit_statement(
            self.statement,
            engine=self.engine,
            catalog=self.catalog,
            schema=self.schema,
            query_tag=tag,
            idempotency_token=task_idempotency_key(context, "sensor-statement"),
        )
        self.statement_id = str(info["id"])
        self._submitted = True
        context["ti"].xcom_push(key=XCOM_STATEMENT_ID_KEY, value=self.statement_id)

    def execute(self, context: Context):
        self._submit(context)
        if not self.deferrable:
            return super().execute(context)
        if self.statement_id is None:  # pragma: no cover - set in __init__
            raise AirflowException("statement_id is required")
        if not self.poke(context):
            self.defer(
                trigger=CidpSQLStatementExecutionTrigger(
                    self.statement_id,
                    self.cidp_conn_id,
                    end_time=time.time() + float(self.timeout),
                    workspace=self.workspace,
                    polling_period_seconds=self.polling_period_seconds,
                    retry_limit=self.cidp_retry_limit,
                    retry_delay=self.cidp_retry_delay,
                ),
                method_name="execute_complete",
            )
        return None

    def poke(self, context: Context) -> bool:
        if self.statement_id is None:  # pragma: no cover - set in __init__
            raise AirflowException("statement_id is required")
        state = self.hook.get_sql_statement_state(self.statement_id)
        if not state.is_terminal:
            return False
        self._raise_if_failed(state)
        return True

    def _raise_if_failed(self, state: SQLStatementState) -> None:
        if not state.is_successful:
            raise AirflowException(
                f"statement {self.statement_id} ended {state.state}: {state.error_code} {state.error_message}".rstrip()
            )

    def execute_complete(self, context: Context, event: dict) -> None:
        self.statement_id = str(event["statement_id"])
        error = event.get("error") or {}
        self._raise_if_failed(
            SQLStatementState(
                str(event.get("state", "")),
                str(error.get("error_code") or ""),
                str(error.get("error_message") or ""),
            )
        )

    def on_kill(self) -> None:
        if not (self._submitted and self.statement_id):
            return
        try:
            self.hook.cancel_sql_statement(self.statement_id)
            self.log.info("cancelled statement %s", self.statement_id)
        except Exception:  # noqa: BLE001 — best effort
            self.log.warning("failed to cancel statement %s", self.statement_id)
