from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import idempotency_key
from airflow.providers.cidp.operators import cidp as mod
from airflow.providers.cidp.operators.cidp import (
    CidpDeployJobsOperator,
    CidpProjectActionOperator,
    CidpRunNowOperator,
    CidpSQLStatementsOperator,
    airflow_query_tag,
    task_idempotency_key,
)
from airflow.providers.cidp.triggers.cidp import (
    CidpExecutionTrigger,
    CidpSQLStatementExecutionTrigger,
)
from airflow.providers.cidp.version_compat import TaskDeferred


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)


def _with_hook(op):
    hook = MagicMock()
    hook.get_run_page_url.return_value = "https://ui/run"
    hook.get_project_page_url.return_value = "https://ui/project"
    op.hook = hook
    return op, hook


def _run_now(**kw):
    return _with_hook(CidpRunNowOperator(task_id="t", project="p", job="j", deferrable=False, **kw))


def test_keys_and_tags(context):
    key = task_idempotency_key(context, "x")
    assert key == idempotency_key("airflow", "dag", "task", "manual__1", -1, 1, "x")
    context["ti"].dag_id = "my dag/with*odd"
    assert airflow_query_tag(context) == "airflow:my_dag_with_odd.task"


def test_hook_is_built_from_operator_args(set_conn):
    op = CidpRunNowOperator(task_id="t", project="p", job="j", workspace="w", cidp_retry_limit=5, cidp_retry_delay=2)
    assert (op.hook.workspace, op.hook.retry_limit, op.hook.retry_delay) == ("w", 5, 2)


def test_run_now_polls_and_publishes(context):
    op, hook = _run_now(job_parameters={"d": "x"})
    hook.run_now.return_value = {"run_id": "r1", "status": "queued"}
    hook.get_run.side_effect = [{"status": "running"}, {"status": "succeeded"}]
    assert op.execute(context) == "r1"
    kwargs = hook.run_now.call_args.kwargs
    assert kwargs["job_parameters"] == {"d": "x"}
    assert kwargs["idempotency_token"] == task_idempotency_key(context, "run-now", None, "p", "j")
    context["ti"].xcom_push.assert_any_call(key="run_id", value="r1")
    context["ti"].xcom_push.assert_any_call(key="run_page_url", value="https://ui/run")


def test_run_now_uses_given_token_and_can_skip_waiting(context):
    op, hook = _run_now(idempotency_token="mine", wait_for_termination=False)
    hook.run_now.return_value = {"run_id": "r1", "status": "queued"}
    assert op.execute(context) == "r1"
    assert hook.run_now.call_args.kwargs["idempotency_token"] == "mine"
    hook.get_run.assert_not_called()


def test_run_now_failure_raises(context):
    op, hook = _run_now()
    hook.run_now.return_value = {"run_id": "r1", "status": "failed", "error": "task x failed"}
    with pytest.raises(AirflowException, match="job p/j run r1 ended failed: task x failed"):
        op.execute(context)
    hook.repair_run.assert_not_called()


def test_repair_once_then_succeed(context):
    op, hook = _run_now(repair_run=True)
    hook.run_now.return_value = {"run_id": "r1", "status": "failed"}
    hook.repair_run.return_value = {"run_id": "r2", "status": "running", "repair_of": "r1"}
    hook.get_run.return_value = {"status": "succeeded", "repair_of": "r1"}
    assert op.execute(context) == "r2"
    assert hook.repair_run.call_args.kwargs["idempotency_token"] == task_idempotency_key(context, "repair", "r1")


def test_a_failed_repair_is_not_repaired_again(context):
    op, hook = _run_now(repair_run=True)
    hook.run_now.return_value = {"run_id": "r1", "status": "failed"}
    hook.repair_run.return_value = {"run_id": "r2", "status": "failed", "repair_of": "r1"}
    with pytest.raises(AirflowException, match="run r2 ended failed"):
        op.execute(context)
    assert hook.repair_run.call_count == 1


def test_cancelled_run_is_not_repaired(context):
    op, hook = _run_now(repair_run=True)
    hook.run_now.return_value = {"run_id": "r1", "status": "cancelled"}
    with pytest.raises(AirflowException, match="ended cancelled"):
        op.execute(context)
    hook.repair_run.assert_not_called()


def test_deferrable_run_now_defers(context):
    op, hook = _run_now(repair_run=True)
    op.deferrable = True
    hook.run_now.return_value = {"run_id": "r1", "status": "queued"}
    with pytest.raises(TaskDeferred) as d:
        op.execute(context)
    trig = d.value.trigger
    assert isinstance(trig, CidpExecutionTrigger)
    assert (trig.run_id, trig.job, trig.run_page_url, trig.repair_run) == ("r1", "j", "https://ui/run", True)
    assert d.value.method_name == "execute_complete"


def test_deferrable_run_already_ended_does_not_defer(context):
    op, hook = _run_now()
    op.deferrable = True
    hook.run_now.return_value = {"run_id": "r1", "status": "succeeded"}
    assert op.execute(context) == "r1"


def test_execute_complete(context):
    op, hook = _run_now(repair_run=True)
    assert op.execute_complete(context, {"run_id": "r1", "status": "succeeded"}) == "r1"
    with pytest.raises(AirflowException, match="ended failed: x"):
        op.execute_complete(context, {"run_id": "r2", "status": "failed", "message": "x", "repair_of": "r0"})
    hook.repair_run.return_value = {"run_id": "r3", "status": "succeeded", "repair_of": "r1"}
    assert op.execute_complete(context, {"run_id": "r1", "status": "failed", "repair_of": None}) == "r3"


def test_on_kill(context):
    op, hook = _run_now()
    op.on_kill()
    hook.cancel_run.assert_not_called()
    op.run_id = "r1"
    op.on_kill()
    hook.cancel_run.assert_called_once_with("p", "j", "r1")
    hook.cancel_run.side_effect = RuntimeError("x")
    op.on_kill()


# ---- project actions ----
def _action(**kw):
    return _with_hook(
        CidpProjectActionOperator(task_id="t", project="p", action="dbt.build", path="m", deferrable=False, **kw)
    )


def test_action_run_is_waited_for(context):
    op, hook = _action(selection="tag:x", action_params={"a": 1}, catalog="c")
    hook.run_project_action.return_value = {"run": {"run_id": "r1", "status": "running"}}
    hook.get_project_run.return_value = {"status": "succeeded"}
    assert op.execute(context) == "r1"
    assert hook.run_project_action.call_args.kwargs == {
        "branch": "main",
        "path": "m",
        "selection": "tag:x",
        "namespace": None,
        "params": {"a": 1},
        "catalog": "c",
    }
    context["ti"].xcom_push.assert_any_call(key="run_page_url", value="https://ui/project")


def test_action_failure_names_the_project(context):
    op, hook = _action()
    hook.run_project_action.return_value = {"run": {"run_id": "r1", "status": "failed", "error": "e"}}
    with pytest.raises(AirflowException, match="project p run r1 ended failed: e"):
        op.execute(context)


def test_sql_action_answers_its_result(context):
    op, hook = _action()
    hook.run_project_action.return_value = {"action": "sql.run", "query": {"rows": [[1]]}, "plan": None}
    assert op.execute(context) == {"action": "sql.run", "query": {"rows": [[1]]}, "plan": None}
    hook.run_project_action.return_value = {"action": "sql.run", "query": {"error": "syntax"}}
    with pytest.raises(AirflowException, match="failed: syntax"):
        op.execute(context)


def test_action_on_kill_cancels_project_run():
    op, hook = _action()
    op.run_id = "r1"
    op.on_kill()
    hook.cancel_project_run.assert_called_once_with("p", "r1")


def test_action_defers_with_project_trigger(context):
    op, hook = _action()
    op.deferrable = True
    hook.run_project_action.return_value = {"run": {"run_id": "r1", "status": "queued"}}
    with pytest.raises(TaskDeferred) as d:
        op.execute(context)
    assert d.value.trigger.job is None


# ---- deploy ----
def _deploy(**kw):
    return _with_hook(CidpDeployJobsOperator(task_id="t", project="p", target="prod", **kw))


def test_deploy(context):
    op, hook = _deploy()
    hook.deploy.return_value = {"deployed": ["a"], "warnings": ["w"]}
    assert op.execute(context)["deployed"] == ["a"]
    hook.deploy.assert_called_once_with("p", target="prod")


def test_deploy_errors_and_warnings(context):
    op, hook = _deploy()
    hook.deploy.return_value = {"errors": ["bad"]}
    with pytest.raises(AirflowException, match="1 problem"):
        op.execute(context)
    op, hook = _deploy(fail_on_warnings=True)
    hook.deploy.return_value = {"warnings": ["w"]}
    with pytest.raises(AirflowException, match="1 problem"):
        op.execute(context)


def test_validate_only(context):
    op, hook = _deploy(validate_only=True, branch="dev")
    hook.validate_deployment.return_value = {}
    op.execute(context)
    hook.validate_deployment.assert_called_once_with("p", target="prod", branch="dev")
    hook.deploy.assert_not_called()


# ---- statements ----
def _stmt(**kw):
    kw.setdefault("deferrable", False)
    return _with_hook(CidpSQLStatementsOperator(task_id="t", statement="SELECT 1", **kw))


def test_statement_sync(context):
    op, hook = _stmt(polling_period_seconds=10, catalog="c")
    hook.submit_statement.return_value = {"id": "s", "state": "PENDING"}
    hook.wait_for_statement.return_value = {"id": "s", "state": "SUCCEEDED"}
    assert op.execute(context) == "s"
    kw = hook.submit_statement.call_args.kwargs
    assert (kw["query_tag"], kw["catalog"], kw["wait_timeout"]) == ("airflow:dag.task", "c", 0)
    assert kw["idempotency_token"] == task_idempotency_key(context, "statement")
    hook.wait_for_statement.assert_called_once_with({"id": "s", "state": "PENDING"}, timeout=3600, long_poll=10)
    context["ti"].xcom_push.assert_called_once_with(key="statement_id", value="s")


def test_statement_failure_and_tags(context):
    op, hook = _stmt(include_airflow_query_tags=False)
    hook.submit_statement.return_value = {"id": "s", "state": "PENDING"}
    hook.wait_for_statement.return_value = {"id": "s", "state": "CLOSED"}
    with pytest.raises(AirflowException, match="statement s ended CLOSED"):
        op.execute(context)
    assert hook.submit_statement.call_args.kwargs["query_tag"] is None


def test_statement_no_wait(context):
    op, hook = _stmt(wait_for_termination=False, query_tag="mine")
    hook.submit_statement.return_value = {"id": "s", "state": "PENDING"}
    assert op.execute(context) == "s"
    assert hook.submit_statement.call_args.kwargs["query_tag"] == "mine"
    hook.wait_for_statement.assert_not_called()


def test_statement_defers(context):
    op, hook = _stmt(deferrable=True, timeout=60)
    hook.submit_statement.return_value = {"id": "s", "state": "RUNNING"}
    with pytest.raises(TaskDeferred) as d:
        op.execute(context)
    assert isinstance(d.value.trigger, CidpSQLStatementExecutionTrigger)
    assert d.value.trigger.statement_id == "s"


def test_statement_execute_complete(context):
    op, _ = _stmt()
    assert op.execute_complete(context, {"statement_id": "s", "state": "SUCCEEDED"}) == "s"
    with pytest.raises(AirflowException, match="ended TIMEOUT: timeout late"):
        op.execute_complete(
            context,
            {"statement_id": "s", "state": "TIMEOUT", "error": {"error_code": "timeout", "error_message": "late"}},
        )


def test_statement_on_kill():
    op, hook = _stmt()
    op.on_kill()
    hook.cancel_sql_statement.assert_not_called()
    op.statement_id = "s"
    op.on_kill()
    hook.cancel_sql_statement.assert_called_once_with("s")
    hook.cancel_sql_statement.side_effect = RuntimeError("x")
    op.on_kill()


def test_default_deferrable_from_config(monkeypatch):
    monkeypatch.setattr(mod.conf, "getboolean", lambda *_a, **_k: True)
    assert CidpSQLStatementsOperator(task_id="t", statement="x").deferrable is True
