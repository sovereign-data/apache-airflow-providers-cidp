from __future__ import annotations

import csv
import json
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import StatementResult
from airflow.providers.cidp.operators.cidp_sql import CidpSqlOperator, split_statements

R1 = StatementResult(columns=[{"name": "a"}], rows=[[1]])
R2 = StatementResult(columns=[{"name": "a"}, {"name": "b"}], rows=[[2, "x"], [3, None]], truncated=True)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT 1; SELECT 2;", ["SELECT 1", "SELECT 2"]),
        ("SELECT ';' AS a; SELECT 'it''s'", ["SELECT ';' AS a", "SELECT 'it''s'"]),
        ('SELECT "a;b" FROM t', ['SELECT "a;b" FROM t']),
        ("SELECT `x;y`", ["SELECT `x;y`"]),
        ("SELECT 1 -- no; split\n; SELECT 2", ["SELECT 1 -- no; split", "SELECT 2"]),
        ("SELECT 1 -- trailing; comment", ["SELECT 1 -- trailing; comment"]),
        ("SELECT /* a; b */ 1; SELECT 2", ["SELECT /* a; b */ 1", "SELECT 2"]),
        ("SELECT /* open; comment", ["SELECT /* open; comment"]),
        (" ; ;", []),
        (["SELECT 1", " ", "SELECT 2;"], ["SELECT 1", "SELECT 2;"]),
    ],
)
def test_split_statements(sql, expected):
    assert split_statements(sql) == expected


def _op(**kw):
    op = CidpSqlOperator(task_id="t", **kw)
    op.hook = MagicMock()
    return op


def test_returns_last_rows(context):
    op = _op(sql="SELECT 1; SELECT 2", catalog="c", row_limit=10)
    op.hook.run_sql.side_effect = [R1, R2]
    assert op.execute(context) == R2.rows
    assert [c.args[0] for c in op.hook.run_sql.call_args_list] == ["SELECT 1", "SELECT 2"]
    kw = op.hook.run_sql.call_args.kwargs
    assert (kw["catalog"], kw["row_limit"], kw["query_tag"], kw["timeout"]) == ("c", 10, "airflow:dag.task", 3600)


def test_returns_every_statement(context):
    op = _op(sql=["SELECT 1", "SELECT 2"], return_last=False, include_airflow_query_tags=False)
    op.hook.run_sql.side_effect = [R1, R2]
    assert op.execute(context) == [R1.rows, R2.rows]
    assert op.hook.run_sql.call_args.kwargs["query_tag"] is None


def test_no_split(context):
    op = _op(sql="SELECT ';'; SELECT 2", split_statements=False)
    op.hook.run_sql.return_value = R1
    op.execute(context)
    assert op.hook.run_sql.call_args.args[0] == "SELECT ';'; SELECT 2"
    op = _op(sql=["a", "b"], split_statements=False)
    op.hook.run_sql.return_value = R1
    op.execute(context)
    assert op.hook.run_sql.call_count == 2


def test_empty_sql(context):
    with pytest.raises(AirflowException, match="no SQL"):
        _op(sql=" ; ").execute(context)


def test_bad_format():
    with pytest.raises(ValueError, match="output_format"):
        CidpSqlOperator(task_id="t", sql="x", output_format="xml")


def test_csv_output(context, tmp_path):
    out = tmp_path / "o.csv"
    op = _op(sql="x", output_path=str(out), csv_params={"delimiter": ";"})
    op.hook.run_sql.return_value = R2
    assert op.execute(context) is None
    assert list(csv.reader(out.open(), delimiter=";")) == [["a", "b"], ["2", "x"], ["3", ""]]
    op = _op(sql="x", output_path=str(out), csv_params={"header": False})
    op.hook.run_sql.return_value = R2
    op.execute(context)
    assert out.read_text().splitlines()[0] == "2,x"


def test_json_outputs(context, tmp_path):
    out = tmp_path / "o.json"
    op = _op(sql="x", output_path=str(out), output_format="json")
    op.hook.run_sql.return_value = R2
    op.execute(context)
    assert json.loads(out.read_text()) == [{"a": 2, "b": "x"}, {"a": 3, "b": None}]
    op = _op(sql="x; y", output_path=str(out), output_format="jsonl", return_last=False)
    op.hook.run_sql.side_effect = [R1, R2]
    op.execute(context)
    assert [json.loads(line) for line in out.read_text().splitlines()] == [
        {"a": 1},
        {"a": 2, "b": "x"},
        {"a": 3, "b": None},
    ]
