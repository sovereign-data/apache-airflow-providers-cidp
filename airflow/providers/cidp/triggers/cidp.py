from __future__ import annotations

import asyncio
import time
from typing import Any

from airflow.providers.cidp.hooks.cidp import CidpHook, RunState
from airflow.providers.cidp.hooks.cidp_base import DEFAULT_CONN_ID
from airflow.providers.cidp.version_compat import BaseTrigger, TriggerEvent

_MODULE = "airflow.providers.cidp.triggers.cidp"
_LONG_POLL_MAX = 30


def _ti_state(ti) -> str | None:
    try:  # Airflow 3: ask the API server through the triggerer's supervisor
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    except ImportError:  # Airflow 2
        from airflow.models.taskinstance import TaskInstance
        from airflow.utils.session import create_session

        with create_session() as session:
            return (
                session.query(TaskInstance.state)
                .filter_by(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, map_index=ti.map_index)
                .scalar()
            )
    states = RuntimeTaskInstance.get_task_states(
        dag_id=ti.dag_id, task_ids=[ti.task_id], run_ids=[ti.run_id], map_index=ti.map_index
    )
    key = ti.task_id if ti.map_index < 0 else f"{ti.task_id}_{ti.map_index}"
    return states.get(ti.run_id, {}).get(key)


async def _task_was_killed(trigger: BaseTrigger) -> bool:
    """On trigger cancellation: True when the task left ``deferred`` (failed, cleared, marked).

    A triggerer restart or trigger rebalancing also cancels the trigger, but the task is
    still deferred then and the backend work must keep running.
    """
    ti = trigger.task_instance
    if ti is None:
        return False
    try:
        state = await asyncio.to_thread(_ti_state, ti)
    except Exception:
        trigger.log.warning("could not read task state; not cancelling backend work", exc_info=True)
        return False
    return state is not None and state != "deferred"


class CidpExecutionTrigger(BaseTrigger):
    """Polls a job run (``job`` set) or a project run (``job`` unset) to its end."""

    def __init__(
        self,
        run_id: str,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        *,
        project: str,
        job: str | None = None,
        workspace: str | None = None,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: float = 10,
        run_page_url: str | None = None,
        repair_run: bool = False,
    ):
        super().__init__()
        self.run_id = run_id
        self.cidp_conn_id = cidp_conn_id
        self.project = project
        self.job = job
        self.workspace = workspace
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.run_page_url = run_page_url
        self.repair_run = repair_run

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{_MODULE}.CidpExecutionTrigger", {
            "run_id": self.run_id,
            "cidp_conn_id": self.cidp_conn_id,
            "project": self.project,
            "job": self.job,
            "workspace": self.workspace,
            "polling_period_seconds": self.polling_period_seconds,
            "retry_limit": self.retry_limit,
            "retry_delay": self.retry_delay,
            "run_page_url": self.run_page_url,
            "repair_run": self.repair_run,
        }

    def _hook(self) -> CidpHook:
        return CidpHook(
            self.cidp_conn_id,
            workspace=self.workspace,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
        )

    async def run(self):
        hook = self._hook()
        try:
            while True:
                if self.job:
                    run = await hook.a_get_run(self.project, self.job, self.run_id)
                else:
                    run = await hook.a_get_project_run(self.project, self.run_id)
                state = RunState.from_run(run)
                if state.is_terminal:
                    yield TriggerEvent(
                        {
                            "run_id": self.run_id,
                            "project": self.project,
                            "job": self.job,
                            "status": state.status,
                            "message": state.message,
                            "run_page_url": self.run_page_url,
                            "repair_of": run.get("repair_of"),
                            "repair_run": self.repair_run,
                        }
                    )
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except asyncio.CancelledError:
            # on_kill never runs for a deferred task; this is its deferred-mode equivalent.
            if await _task_was_killed(self):
                if self.job:
                    await hook.a_cancel_run(self.project, self.job, self.run_id)
                else:
                    await hook.a_cancel_project_run(self.project, self.run_id)
                self.log.info("cancelled run %s", self.run_id)
            raise


class CidpSQLStatementExecutionTrigger(BaseTrigger):
    """Long-polls a SQL statement to its end, cancelling it at ``end_time``."""

    def __init__(
        self,
        statement_id: str,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        *,
        end_time: float,
        workspace: str | None = None,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: float = 10,
    ):
        super().__init__()
        self.statement_id = statement_id
        self.cidp_conn_id = cidp_conn_id
        self.end_time = end_time
        self.workspace = workspace
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{_MODULE}.CidpSQLStatementExecutionTrigger", {
            "statement_id": self.statement_id,
            "cidp_conn_id": self.cidp_conn_id,
            "end_time": self.end_time,
            "workspace": self.workspace,
            "polling_period_seconds": self.polling_period_seconds,
            "retry_limit": self.retry_limit,
            "retry_delay": self.retry_delay,
        }

    async def run(self):
        hook = CidpHook(
            self.cidp_conn_id,
            workspace=self.workspace,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
        )
        try:
            while True:
                remaining = self.end_time - time.time()
                if remaining <= 0:
                    await hook.a_cancel_sql_statement(self.statement_id)
                    yield TriggerEvent(
                        {
                            "statement_id": self.statement_id,
                            "state": "TIMEOUT",
                            "error": {
                                "error_code": "timeout",
                                "error_message": "the statement did not end in time; cancelled",
                            },
                        }
                    )
                    return
                # The GET blocks server-side until the statement ends or the wait passes.
                wait = max(1, min(self.polling_period_seconds, _LONG_POLL_MAX, int(remaining)))
                state = await hook.a_get_sql_statement_state(self.statement_id, wait_timeout=wait)
                if state.is_terminal:
                    yield TriggerEvent(
                        {
                            "statement_id": self.statement_id,
                            "state": state.state,
                            "error": {"error_code": state.error_code, "error_message": state.error_message},
                        }
                    )
                    return
        except asyncio.CancelledError:
            if await _task_was_killed(self):
                await hook.a_cancel_sql_statement(self.statement_id)
                self.log.info("cancelled statement %s", self.statement_id)
            raise
