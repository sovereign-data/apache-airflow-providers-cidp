"""Resource methods of the data API, and run/statement state adapters (cf. the Databricks hook)."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from airflow.providers.cidp.exceptions import CidpApiError
from airflow.providers.cidp.hooks.cidp_base import CidpBaseHook, _seg

# Job runs and project runs. Anything else (a state added later) counts as not ended.
RUN_ENDED = frozenset({"succeeded", "failed", "cancelled", "skipped", "expired"})
RUN_SUCCEEDED = "succeeded"
# Statements (Databricks Statement Execution states). CLOSED: ended, its result already removed.
STATEMENT_ENDED = frozenset({"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"})
STATEMENT_SUCCEEDED = "SUCCEEDED"
_MAX_MESSAGE = 2000
_STATEMENT_WAIT_MAX = 50  # the contract's cap on wait_timeout
_IDEMPOTENCY_KEY_MAX = 64


def idempotency_key(*parts: Any) -> str:
    """A stable key of at most 64 characters for the ``Idempotency-Key`` header."""
    return hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).hexdigest()[:_IDEMPOTENCY_KEY_MAX]


@dataclass
class RunState:
    """The state of a job run or a project run (cf. Databricks ``RunState``)."""

    status: str
    message: str = ""

    def __post_init__(self):
        # A run's error can carry a whole log; keep the tail, where the error is.
        self.message = (self.message or "")[-_MAX_MESSAGE:]

    @property
    def is_terminal(self) -> bool:
        return self.status in RUN_ENDED

    @property
    def is_successful(self) -> bool:
        return self.status == RUN_SUCCEEDED

    @classmethod
    def from_run(cls, run: dict) -> RunState:
        return cls(str(run.get("status", "")), str(run.get("error") or ""))


@dataclass
class SQLStatementState:
    """The state of a SQL statement (cf. Databricks ``SQLStatementState``)."""

    state: str
    error_code: str = ""
    error_message: str = ""
    row_count: int | None = None
    truncated: bool = False

    @property
    def is_terminal(self) -> bool:
        return self.state in STATEMENT_ENDED

    @property
    def is_running(self) -> bool:
        return not self.is_terminal

    @property
    def is_successful(self) -> bool:
        return self.state == STATEMENT_SUCCEEDED

    @classmethod
    def from_statement(cls, info: dict) -> SQLStatementState:
        error = info.get("error") or {}
        manifest = info.get("manifest") or {}
        return cls(
            state=str(info.get("state", "")),
            error_code=str(error.get("code") or ""),
            error_message=str(error.get("message") or "")[-_MAX_MESSAGE:],
            row_count=manifest.get("row_count"),
            truncated=bool(manifest.get("truncated", False)),
        )


@dataclass
class StatementResult:
    """Rows of a succeeded statement. Never logged."""

    columns: list[dict] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    truncated: bool = False

    @property
    def column_names(self) -> list[str]:
        return [str(c.get("name")) for c in self.columns]


class CidpHook(CidpBaseHook):
    """Jobs, project runs, SQL statements, projects, deployments, quality and volumes."""

    # ---- jobs ----
    def _job(self, project: str, job: str) -> str:
        return f"{self._ws()}/projects/{_seg(project)}/jobs/{_seg(job)}"

    def run_now(
        self,
        project: str,
        job: str,
        *,
        job_parameters: dict[str, Any] | None = None,
        idempotency_token: str | None = None,
    ) -> dict:
        """Start a job run (Databricks ``jobs/run-now``); answers ``JobRunInfo``."""
        body: dict[str, Any] = {}
        if job_parameters:
            body["job_parameters"] = {str(k): str(v) for k, v in job_parameters.items()}
        headers = {"Idempotency-Key": idempotency_token} if idempotency_token else None
        return self._do_api_call("POST", f"{self._job(project, job)}/run-now", json=body, headers=headers)

    def get_job(self, project: str, job: str) -> dict:
        return self._do_api_call("GET", self._job(project, job))

    def set_job_enabled(self, project: str, job: str, enabled: bool) -> dict:
        return self._do_api_call("PATCH", self._job(project, job), json={"enabled": enabled})

    def list_jobs(self, project: str) -> list[dict]:
        return list(self._paged(f"{self._ws()}/projects/{_seg(project)}/jobs"))

    def list_runs(self, project: str, job: str, *, limit: int | None = None) -> list[dict]:
        return list(self._paged(f"{self._job(project, job)}/runs", limit=limit))

    def get_run(self, project: str, job: str, run_id: str) -> dict:
        return self._do_api_call("GET", f"{self._job(project, job)}/runs/{_seg(run_id)}")

    async def a_get_run(self, project: str, job: str, run_id: str) -> dict:
        return await self._a_do_api_call("GET", f"{self._job(project, job)}/runs/{_seg(run_id)}")

    def get_run_state(self, project: str, job: str, run_id: str) -> RunState:
        return RunState.from_run(self.get_run(project, job, run_id))

    async def a_get_run_state(self, project: str, job: str, run_id: str) -> RunState:
        return RunState.from_run(await self.a_get_run(project, job, run_id))

    def cancel_run(self, project: str, job: str, run_id: str) -> dict:
        return self._do_api_call("POST", f"{self._job(project, job)}/runs/{_seg(run_id)}/cancel")

    async def a_cancel_run(self, project: str, job: str, run_id: str) -> dict:
        return await self._a_do_api_call("POST", f"{self._job(project, job)}/runs/{_seg(run_id)}/cancel")

    def repair_run(self, project: str, job: str, run_id: str, *, idempotency_token: str | None = None) -> dict:
        """Databricks' repair: a new run of the failed tasks and their dependants (``repair_of`` names it)."""
        headers = {"Idempotency-Key": idempotency_token} if idempotency_token else None
        return self._do_api_call("POST", f"{self._job(project, job)}/runs/{_seg(run_id)}/retry", headers=headers)

    def get_run_page_url(self, project: str, job: str, run_id: str) -> str:
        return (
            f"{self.ui_url()}/data/{_seg(self.workspace)}/projects/{_seg(project)}/jobs/{_seg(job)}/runs/{_seg(run_id)}"
        )

    def get_project_page_url(self, project: str) -> str:
        return f"{self.ui_url()}/data/{_seg(self.workspace)}/projects/{_seg(project)}"

    # ---- project actions and runs ----
    def _project(self, project: str) -> str:
        return f"{self._ws()}/projects/{_seg(project)}"

    def list_actions(self, project: str, *, branch: str, path: str) -> dict:
        return self._do_api_call("GET", f"{self._project(project)}/actions", params={"branch": branch, "path": path})

    def run_project_action(
        self,
        project: str,
        action: str,
        *,
        branch: str,
        path: str,
        selection: str | None = None,
        namespace: str | None = None,
        params: dict[str, Any] | None = None,
        catalog: str | None = None,
    ) -> dict:
        """Run an action on a project file (``dbt.build``, ``python.run``, ``sql.run``...).

        SQL actions answer their result (``query``) or ``plan``; the others start a ``run``.
        """
        body: dict[str, Any] = {"branch": branch, "path": path}
        for key, value in (("selection", selection), ("namespace", namespace), ("catalog", catalog)):
            if value is not None:
                body[key] = value
        if params:
            body["params"] = params
        return self._do_api_call("POST", f"{self._project(project)}/actions/{_seg(action)}", json=body)

    def get_project_run(self, project: str, run_id: str) -> dict:
        return self._do_api_call("GET", f"{self._project(project)}/runs/{_seg(run_id)}")

    async def a_get_project_run(self, project: str, run_id: str) -> dict:
        return await self._a_do_api_call("GET", f"{self._project(project)}/runs/{_seg(run_id)}")

    def get_project_run_state(self, project: str, run_id: str) -> RunState:
        return RunState.from_run(self.get_project_run(project, run_id))

    async def a_get_project_run_state(self, project: str, run_id: str) -> RunState:
        return RunState.from_run(await self.a_get_project_run(project, run_id))

    def cancel_project_run(self, project: str, run_id: str) -> dict:
        return self._do_api_call("POST", f"{self._project(project)}/runs/{_seg(run_id)}/cancel")

    async def a_cancel_project_run(self, project: str, run_id: str) -> dict:
        return await self._a_do_api_call("POST", f"{self._project(project)}/runs/{_seg(run_id)}/cancel")

    # ---- deployment (bundles) ----
    def deploy(self, project: str, *, target: str | None = None) -> dict:
        """Deploy the project's jobs (the bundle's ``target``); answers ``JobsDeployOut``."""
        return self._do_api_call("PUT", f"{self._project(project)}/deployment", json={"target": target})

    def validate_deployment(self, project: str, *, target: str | None = None, branch: str | None = None) -> dict:
        body = {k: v for k, v in (("target", target), ("branch", branch)) if v is not None}
        return self._do_api_call("POST", f"{self._project(project)}/deployment/validate", json=body)

    def destroy_deployment(self, project: str) -> dict:
        return self._do_api_call("DELETE", f"{self._project(project)}/deployment")

    # ---- projects (cf. Databricks Repos) ----
    def create_project(self, slug: str, *, idempotency_token: str | None = None, **fields: Any) -> dict:
        body = {"slug": slug, **{k: v for k, v in fields.items() if v is not None}}
        headers = {"Idempotency-Key": idempotency_token} if idempotency_token else None
        return self._do_api_call("POST", f"{self._ws()}/projects", json=body, headers=headers)

    def get_project(self, project: str) -> dict:
        return self._do_api_call("GET", self._project(project))

    def delete_project(self, project: str) -> None:
        # ``confirm`` must repeat the slug: the API's guard against deleting the wrong project.
        self._do_api_call("DELETE", self._project(project), params={"confirm": project})

    def pull_project(self, project: str, *, branch: str) -> dict:
        return self._do_api_call("POST", f"{self._project(project)}/pull", json={"branch": branch})

    # ---- SQL statements (cf. Databricks Statement Execution API) ----
    def submit_statement(
        self,
        statement: str,
        *,
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        wait_timeout: int = 0,
        on_wait_timeout: str | None = None,
        row_limit: int | None = None,
        query_tag: str | None = None,
        idempotency_token: str | None = None,
    ) -> dict:
        """Submit a statement as the connection's identity; answers ``StatementInfo``."""
        wait = max(0, min(int(wait_timeout), _STATEMENT_WAIT_MAX))
        body: dict[str, Any] = {"sql": statement, "wait_timeout": wait}
        for key, value in (
            ("engine", engine),
            ("catalog", catalog),
            ("schema", schema),
            ("on_wait_timeout", on_wait_timeout),
            ("row_limit", row_limit),
            ("query_tag", query_tag),
        ):
            if value is not None:
                body[key] = value
        headers = {"Idempotency-Key": idempotency_token} if idempotency_token else None
        return self._do_api_call(
            "POST",
            f"{self._ws()}/statements",
            json=body,
            headers=headers,
            timeout=max(self.timeout_seconds, wait + 30),
        )

    def get_statement(self, statement_id: str, *, wait_timeout: int = 0) -> dict:
        wait = max(0, min(int(wait_timeout), _STATEMENT_WAIT_MAX))
        return self._do_api_call(
            "GET",
            f"{self._ws()}/statements/{_seg(statement_id)}",
            params={"wait_timeout": wait} if wait else None,
            timeout=max(self.timeout_seconds, wait + 30),
        )

    async def a_get_statement(self, statement_id: str, *, wait_timeout: int = 0) -> dict:
        wait = max(0, min(int(wait_timeout), _STATEMENT_WAIT_MAX))
        return await self._a_do_api_call(
            "GET",
            f"{self._ws()}/statements/{_seg(statement_id)}",
            params={"wait_timeout": wait} if wait else None,
            timeout=max(self.timeout_seconds, wait + 30),
        )

    def get_sql_statement_state(self, statement_id: str, *, wait_timeout: int = 0) -> SQLStatementState:
        return SQLStatementState.from_statement(self.get_statement(statement_id, wait_timeout=wait_timeout))

    async def a_get_sql_statement_state(self, statement_id: str, *, wait_timeout: int = 0) -> SQLStatementState:
        info = await self.a_get_statement(statement_id, wait_timeout=wait_timeout)
        return SQLStatementState.from_statement(info)

    def cancel_sql_statement(self, statement_id: str) -> dict:
        return self._do_api_call("POST", f"{self._ws()}/statements/{_seg(statement_id)}/cancel")

    async def a_cancel_sql_statement(self, statement_id: str) -> dict:
        return await self._a_do_api_call("POST", f"{self._ws()}/statements/{_seg(statement_id)}/cancel")

    def get_statement_result_page(self, statement_id: str, *, cursor: str | None = None) -> dict:
        return self._do_api_call(
            "GET",
            f"{self._ws()}/statements/{_seg(statement_id)}/result",
            params={"cursor": cursor} if cursor else None,
        )

    def iter_statement_rows(self, info: dict) -> Iterator[list[Any]]:
        """Every row of a succeeded statement: the first page from ``info``, then the rest by cursor."""
        page = info.get("result") or self.get_statement_result_page(info["id"])
        while True:
            yield from page.get("rows") or []
            cursor = page.get("next_cursor")
            if not cursor:
                return
            page = self.get_statement_result_page(info["id"], cursor=cursor)

    def wait_for_statement(self, info: dict, *, timeout: float | None = None, long_poll: int = 30) -> dict:
        """Long-poll a statement until it ends; cancel it and raise when ``timeout`` (seconds) passes."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while info.get("state") not in STATEMENT_ENDED:
            if deadline is not None and time.monotonic() >= deadline:
                self.cancel_sql_statement(info["id"])
                raise CidpApiError(f"statement {info['id']} did not end within {timeout}s; cancelled")
            wait = long_poll if deadline is None else max(1, min(long_poll, int(deadline - time.monotonic())))
            info = self.get_statement(info["id"], wait_timeout=wait)
        return info

    def run_sql(
        self,
        statement: str,
        *,
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        row_limit: int | None = None,
        query_tag: str | None = None,
        timeout: float | None = None,
    ) -> StatementResult:
        """Run one statement to its end and answer its rows (raises when it did not succeed)."""
        info = self.submit_statement(
            statement,
            engine=engine,
            catalog=catalog,
            schema=schema,
            wait_timeout=30,
            row_limit=row_limit,
            query_tag=query_tag,
        )
        info = self.wait_for_statement(info, timeout=timeout)
        state = SQLStatementState.from_statement(info)
        if not state.is_successful:
            raise CidpApiError(
                f"statement {info.get('id')} ended {state.state}: {state.error_code} {state.error_message}".strip()
            )
        manifest = info.get("manifest") or {}
        return StatementResult(
            columns=list(manifest.get("columns") or []),
            rows=list(self.iter_statement_rows(info)),
            truncated=bool(manifest.get("truncated", False)),
        )

    # ---- catalog, quality, volumes ----
    def _table(self, catalog: str, schema: str, table: str) -> str:
        return f"{self._ws()}/catalogs/{_seg(catalog)}/schemas/{_seg(schema)}/tables/{_seg(table)}"

    def get_table(self, catalog: str, schema: str, table: str) -> dict:
        return self._do_api_call("GET", self._table(catalog, schema, table))

    def run_quality(self, catalog: str, schema: str, table: str, *, check: str | None = None) -> dict:
        """Run a table's quality checks (preview API); answers ``RunOut``."""
        body = {"check": check} if check else None
        return self._do_api_call("POST", f"{self._table(catalog, schema, table)}/quality-runs", json=body)

    def _volume(self, catalog: str, schema: str, volume: str) -> str:
        return f"{self._ws()}/catalogs/{_seg(catalog)}/schemas/{_seg(schema)}/volumes/{_seg(volume)}/files"

    def upload_file(
        self, catalog: str, schema: str, volume: str, path: str, data: bytes, *, overwrite: bool = False
    ) -> dict:
        resp = self._do_api_call(
            "PUT",
            self._volume(catalog, schema, volume),
            params={"path": path, "overwrite": str(overwrite).lower()},
            headers={"Content-Type": "application/octet-stream"},
            content=data,
            raw=True,
        )
        return resp.json() if resp.content else {}

    def download_file(self, catalog: str, schema: str, volume: str, path: str) -> bytes:
        resp = self._do_api_call(
            "GET",
            f"{self._volume(catalog, schema, volume)}/content",
            params={"path": path},
            headers={"Accept": "application/octet-stream"},
            raw=True,
        )
        return resp.content

    # ---- paging ----
    def _paged(self, endpoint: str, *, limit: int | None = None) -> Iterator[dict]:
        cursor: str | None = None
        seen = 0
        while True:
            params: dict[str, Any] = {}
            if cursor:
                params["cursor"] = cursor
            page = self._do_api_call("GET", endpoint, params=params or None)
            for item in page.get("items") or []:
                yield item
                seen += 1
                if limit is not None and seen >= limit:
                    return
            cursor = page.get("next_cursor")
            if not cursor:
                return
