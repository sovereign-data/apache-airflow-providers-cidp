from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.providers.cidp.hooks.cidp import SQLStatementState
from airflow.providers.cidp.triggers import cidp as mod
from airflow.providers.cidp.triggers.cidp import (
    CidpExecutionTrigger,
    CidpSQLStatementExecutionTrigger,
    _task_was_killed,
    _ti_state,
)

HOOK = "airflow.providers.cidp.triggers.cidp.CidpHook"


def _job_trigger(**kw):
    return CidpExecutionTrigger(
        "r1", "c", project="p", job="j", workspace="ws", polling_period_seconds=0, run_page_url="u", **kw
    )


async def _first(trigger):
    """The trigger's one event: it must end after it."""
    events = [e async for e in trigger.run()]
    assert len(events) == 1
    return events[0]


def test_execution_trigger_serializes():
    path, kwargs = _job_trigger(repair_run=True).serialize()
    assert path == "airflow.providers.cidp.triggers.cidp.CidpExecutionTrigger"
    assert CidpExecutionTrigger(**kwargs).serialize() == (path, kwargs)
    assert kwargs["repair_run"] is True


def test_statement_trigger_serializes():
    path, kwargs = CidpSQLStatementExecutionTrigger("s", "c", end_time=5.0, workspace="ws").serialize()
    assert path.endswith(".CidpSQLStatementExecutionTrigger")
    assert CidpSQLStatementExecutionTrigger(**kwargs).serialize() == (path, kwargs)


async def test_job_run_polls_to_its_end():
    with patch(HOOK) as cls:
        hook = cls.return_value
        hook.a_get_run = AsyncMock(
            side_effect=[{"status": "running"}, {"status": "failed", "error": "boom", "repair_of": None}]
        )
        event = await _first(_job_trigger(repair_run=True))
    assert event.payload == {
        "run_id": "r1",
        "project": "p",
        "job": "j",
        "status": "failed",
        "message": "boom",
        "run_page_url": "u",
        "repair_of": None,
        "repair_run": True,
    }
    cls.assert_called_once_with("c", workspace="ws", retry_limit=3, retry_delay=10)


async def test_project_run_is_polled_when_no_job():
    trig = CidpExecutionTrigger("r1", "c", project="p", polling_period_seconds=0)
    with patch(HOOK) as cls:
        cls.return_value.a_get_project_run = AsyncMock(return_value={"status": "succeeded"})
        event = await _first(trig)
    assert (event.payload["status"], event.payload["job"]) == ("succeeded", None)


async def _cancel_while_polling(trigger, hook):
    task = asyncio.ensure_future(_first(trigger))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize(("job", "state", "cancelled"), [("j", "failed", True), (None, "success", True)])
async def test_killed_task_cancels_the_run(job, state, cancelled):
    trig = CidpExecutionTrigger("r1", "c", project="p", job=job, polling_period_seconds=1)
    trig.task_instance = MagicMock()
    with patch(HOOK) as cls, patch(f"{mod.__name__}._ti_state", return_value=state):
        hook = cls.return_value
        hook.a_get_run = AsyncMock(return_value={"status": "running"})
        hook.a_get_project_run = AsyncMock(return_value={"status": "running"})
        hook.a_cancel_run = AsyncMock()
        hook.a_cancel_project_run = AsyncMock()
        await _cancel_while_polling(trig, hook)
    if job:
        hook.a_cancel_run.assert_awaited_once_with("p", "j", "r1")
    else:
        hook.a_cancel_project_run.assert_awaited_once_with("p", "r1")


async def test_still_deferred_task_keeps_the_run():
    trig = _job_trigger()
    trig.polling_period_seconds = 1
    trig.task_instance = MagicMock()
    with patch(HOOK) as cls, patch(f"{mod.__name__}._ti_state", return_value="deferred"):
        hook = cls.return_value
        hook.a_get_run = AsyncMock(return_value={"status": "running"})
        hook.a_cancel_run = AsyncMock()
        await _cancel_while_polling(trig, hook)
    hook.a_cancel_run.assert_not_awaited()


async def test_task_was_killed_edge_cases():
    trig = _job_trigger()
    trig.task_instance = None
    assert await _task_was_killed(trig) is False
    trig.task_instance = MagicMock()
    with patch(f"{mod.__name__}._ti_state", side_effect=RuntimeError("api down")):
        assert await _task_was_killed(trig) is False
    with patch(f"{mod.__name__}._ti_state", return_value=None):
        assert await _task_was_killed(trig) is False


@pytest.mark.parametrize(("map_index", "key"), [(-1, "t"), (2, "t_2")])
def test_ti_state_reads_the_api(map_index, key):
    ti = MagicMock(dag_id="d", task_id="t", run_id="r", map_index=map_index)
    with patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states") as get:
        get.return_value = {"r": {key: "failed"}}
        assert _ti_state(ti) == "failed"
    get.assert_called_once_with(dag_id="d", task_ids=["t"], run_ids=["r"], map_index=map_index)


# ---- statements ----
async def test_statement_long_polls_to_its_end():
    trig = CidpSQLStatementExecutionTrigger("s", "c", end_time=time.time() + 100, polling_period_seconds=60)
    with patch(HOOK) as cls:
        hook = cls.return_value
        hook.a_get_sql_statement_state = AsyncMock(
            side_effect=[SQLStatementState("RUNNING"), SQLStatementState("FAILED", "x", "y")]
        )
        event = await _first(trig)
    assert event.payload == {"statement_id": "s", "state": "FAILED", "error": {"error_code": "x", "error_message": "y"}}
    assert hook.a_get_sql_statement_state.await_args.kwargs["wait_timeout"] == 30


async def test_statement_past_end_time_is_cancelled():
    trig = CidpSQLStatementExecutionTrigger("s", "c", end_time=time.time() - 1)
    with patch(HOOK) as cls:
        hook = cls.return_value
        hook.a_cancel_sql_statement = AsyncMock()
        event = await _first(trig)
    assert event.payload["state"] == "TIMEOUT"
    hook.a_cancel_sql_statement.assert_awaited_once_with("s")


async def test_killed_task_cancels_the_statement():
    trig = CidpSQLStatementExecutionTrigger("s", "c", end_time=time.time() + 100)
    trig.task_instance = MagicMock()

    async def slow(*_a, **_k):
        await asyncio.sleep(10)

    with patch(HOOK) as cls, patch(f"{mod.__name__}._ti_state", return_value="failed"):
        hook = cls.return_value
        hook.a_get_sql_statement_state = slow
        hook.a_cancel_sql_statement = AsyncMock()
        await _cancel_while_polling(trig, hook)
    hook.a_cancel_sql_statement.assert_awaited_once_with("s")


async def test_still_deferred_statement_is_kept():
    trig = CidpSQLStatementExecutionTrigger("s", "c", end_time=time.time() + 100)
    trig.task_instance = MagicMock()

    async def slow(*_a, **_k):
        await asyncio.sleep(10)

    with patch(HOOK) as cls, patch(f"{mod.__name__}._ti_state", return_value="deferred"):
        hook = cls.return_value
        hook.a_get_sql_statement_state = slow
        hook.a_cancel_sql_statement = AsyncMock()
        await _cancel_while_polling(trig, hook)
    hook.a_cancel_sql_statement.assert_not_awaited()
