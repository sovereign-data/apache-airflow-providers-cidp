from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import SQLStatementState, StatementResult
from airflow.providers.cidp.operators.cidp import task_idempotency_key
from airflow.providers.cidp.sensors.cidp import CidpSQLStatementsSensor
from airflow.providers.cidp.sensors.cidp_sql import (
    CidpPartitionSensor,
    CidpSqlSensor,
    _literal,
)
from airflow.providers.cidp.triggers.cidp import CidpSQLStatementExecutionTrigger
from airflow.providers.cidp.version_compat import TaskDeferred


def _sensor(**kw):
    kw.setdefault("deferrable", False)
    op = CidpSQLStatementsSensor(task_id="t", **kw)
    op.hook = MagicMock()
    return op


@pytest.mark.parametrize("kw", [{}, {"statement": "x", "statement_id": "s"}])
def test_exactly_one_of_statement_and_id(kw):
    with pytest.raises(ValueError, match="exactly one"):
        CidpSQLStatementsSensor(task_id="t", **kw)


def test_hook_from_args(set_conn):
    s = CidpSQLStatementsSensor(task_id="t", statement_id="s", workspace="w", cidp_retry_limit=2)
    assert (s.hook.workspace, s.hook.retry_limit) == ("w", 2)


def test_submits_then_pokes(context):
    op = _sensor(statement="SELECT 1", catalog="c", poke_interval=0)
    op.hook.submit_statement.return_value = {"id": "s"}
    op.hook.get_sql_statement_state.side_effect = [SQLStatementState("RUNNING"), SQLStatementState("SUCCEEDED")]
    op.execute(context)
    kw = op.hook.submit_statement.call_args.kwargs
    assert (kw["catalog"], kw["query_tag"]) == ("c", "airflow:dag.task")
    assert kw["idempotency_token"] == task_idempotency_key(context, "sensor-statement")
    context["ti"].xcom_push.assert_called_once_with(key="statement_id", value="s")


def test_watches_given_statement(context):
    op = _sensor(statement_id="s", include_airflow_query_tags=False)
    op.hook.get_sql_statement_state.return_value = SQLStatementState("FAILED", "e", "m")
    with pytest.raises(AirflowException, match="statement s ended FAILED: e m"):
        op.poke(context)
    op._submit(context)
    op.hook.submit_statement.assert_not_called()


def test_deferrable(context):
    op = _sensor(statement_id="s", deferrable=True, timeout=60)
    op.hook.get_sql_statement_state.return_value = SQLStatementState("RUNNING")
    with pytest.raises(TaskDeferred) as d:
        op.execute(context)
    assert isinstance(d.value.trigger, CidpSQLStatementExecutionTrigger)
    op.hook.get_sql_statement_state.return_value = SQLStatementState("SUCCEEDED")
    assert op.execute(context) is None


def test_execute_complete(context):
    op = _sensor(statement_id="s")
    op.execute_complete(context, {"statement_id": "s", "state": "SUCCEEDED"})
    with pytest.raises(AirflowException, match="ended CANCELED"):
        op.execute_complete(context, {"statement_id": "s", "state": "CANCELED", "error": {}})


def test_on_kill_cancels_only_a_submitted_statement(context):
    watched = _sensor(statement_id="s")
    watched.on_kill()
    watched.hook.cancel_sql_statement.assert_not_called()  # someone else's statement

    op = _sensor(statement="SELECT 1")
    op.on_kill()  # nothing submitted yet
    op.hook.submit_statement.return_value = {"id": "s2"}
    op._submit(context)
    op.on_kill()
    op.hook.cancel_sql_statement.assert_called_once_with("s2")
    op.hook.cancel_sql_statement.side_effect = RuntimeError("down")
    op.on_kill()  # best effort: no raise


# ---- SQL and partition sensors ----
def test_sql_sensor(context, set_conn):
    op = CidpSqlSensor(task_id="t", sql="SELECT 1", catalog="c", workspace="w")
    assert op.hook.workspace == "w"
    op.hook = MagicMock()
    op.hook.run_sql.return_value = StatementResult(rows=[])
    assert op.poke(context) is False
    op.hook.run_sql.return_value = StatementResult(rows=[[1]])
    assert op.poke(context) is True
    assert op.hook.run_sql.call_args.kwargs["row_limit"] == 1000
    op.handler = lambda rows: rows[0][0] > 5
    assert op.poke(context) is False


def _partition(**kw):
    return CidpPartitionSensor(task_id="t", table_name="orders", **kw)


def test_partition_sql():
    op = _partition(catalog="lake", schema="raw", partitions={"d": "2026-01-01", "region": ["eu", "o'x"]})
    assert op._get_sql() == (
        'SELECT 1 FROM "lake"."raw"."orders" WHERE "d" = \'2026-01-01\' '
        "AND (\"region\" = 'eu' OR \"region\" = 'o''x') LIMIT 1"
    )
    op = _partition(partitions={"n": 5}, partition_operator=">=")
    assert op._get_sql() == 'SELECT 1 FROM "default"."orders" WHERE "n" >= 5 LIMIT 1'


def test_partition_poke_runs_the_query(context):
    op = _partition(partitions={"d": 1})
    op.hook = MagicMock()
    op.hook.run_sql.return_value = StatementResult(rows=[[1]])
    assert op.poke(context) is True
    assert op.hook.run_sql.call_args.args[0].startswith('SELECT 1 FROM "default"."orders"')


@pytest.mark.parametrize(
    ("kw", "match"),
    [
        ({"partitions": {}}, "at least one"),
        ({"partitions": {"d": []}}, "lists no values"),
        ({"partitions": {"d; DROP": 1}}, "not a plain SQL identifier"),
        ({"partitions": {"d": 1}, "schema": "x.y"}, "not a plain SQL identifier"),
    ],
)
def test_partition_rejects(kw, match):
    with pytest.raises(ValueError, match=match):
        _partition(**kw)._get_sql()


def test_partition_operator_validated():
    with pytest.raises(ValueError, match="partition_operator"):
        _partition(partitions={"d": 1}, partition_operator="LIKE")


@pytest.mark.parametrize(
    ("value", "sql"),
    [
        (True, "TRUE"),
        (False, "FALSE"),
        (1.5, "1.5"),
        (datetime.date(2026, 1, 2), "DATE '2026-01-02'"),
        (datetime.datetime(2026, 1, 2, 3, 4, 5), "TIMESTAMP '2026-01-02 03:04:05'"),
        ("a'b", "'a''b'"),
    ],
)
def test_literals(value, sql):
    assert _literal(value) == sql
