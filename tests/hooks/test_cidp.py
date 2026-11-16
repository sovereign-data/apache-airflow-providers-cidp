from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
import respx

from airflow.providers.cidp.exceptions import CidpApiError
from airflow.providers.cidp.hooks import cidp as mod
from airflow.providers.cidp.hooks.cidp import (
    RunState,
    SQLStatementState,
    StatementResult,
    idempotency_key,
)
from tests.conftest import HOST, WS

JOB = f"{WS}/projects/p/jobs/j"


def _body(route) -> dict:
    return json.loads(route.calls.last.request.content or b"null")


# ---- state adapters ----
def test_idempotency_key_is_stable_and_short():
    a = idempotency_key("a", 1, None)
    assert a == idempotency_key("a", 1, None)
    assert a != idempotency_key("a", 2, None)
    assert len(a) == 64


def test_run_state():
    s = RunState.from_run({"status": "failed", "error": "x" * 3000 + "END"})
    assert s.is_terminal
    assert not s.is_successful
    assert s.message.endswith("END")
    assert len(s.message) == 2000
    assert RunState.from_run({"status": "succeeded"}).is_successful
    assert not RunState.from_run({"status": "pending"}).is_terminal
    assert not RunState.from_run({}).is_terminal


def test_statement_state():
    s = SQLStatementState.from_statement(
        {"state": "FAILED", "error": {"code": "syntax", "message": "bad"}, "manifest": {"row_count": 3}}
    )
    assert (s.is_terminal, s.is_running, s.is_successful) == (True, False, False)
    assert (s.error_code, s.error_message, s.row_count, s.truncated) == ("syntax", "bad", 3, False)
    assert SQLStatementState.from_statement({"state": "RUNNING"}).is_running
    closed = SQLStatementState("CLOSED")
    assert closed.is_terminal
    assert not closed.is_successful


def test_statement_result_columns():
    assert StatementResult(columns=[{"name": "a"}, {"name": "b"}]).column_names == ["a", "b"]


# ---- jobs ----
@respx.mock
def test_run_now(hook):
    route = respx.post(f"{JOB}/run-now").respond(201, json={"run_id": "r1", "status": "queued"})
    assert hook.run_now("p", "j", job_parameters={"d": 1, "x": "y"}, idempotency_token="k")["run_id"] == "r1"
    assert _body(route) == {"job_parameters": {"d": "1", "x": "y"}}
    assert route.calls.last.request.headers["Idempotency-Key"] == "k"
    hook.run_now("p", "j")
    assert _body(route) == {}
    assert "Idempotency-Key" not in route.calls.last.request.headers


@respx.mock
def test_job_reads_and_writes(hook):
    respx.get(JOB).respond(json={"key": "j"})
    patch = respx.patch(JOB).respond(json={"enabled": False})
    respx.get(f"{JOB}/runs/r%2F1").respond(json={"status": "running", "run_id": "r/1"})
    cancel = respx.post(f"{JOB}/runs/r1/cancel").respond(json={})
    retry = respx.post(f"{JOB}/runs/r1/retry").respond(json={"run_id": "r2", "repair_of": "r1"})
    assert hook.get_job("p", "j") == {"key": "j"}
    hook.set_job_enabled("p", "j", False)
    assert _body(patch) == {"enabled": False}
    assert hook.get_run_state("p", "j", "r/1").status == "running"
    hook.cancel_run("p", "j", "r1")
    assert cancel.called
    assert hook.repair_run("p", "j", "r1", idempotency_token="k")["repair_of"] == "r1"
    assert retry.calls.last.request.headers["Idempotency-Key"] == "k"


@respx.mock
def test_paging(hook):
    route = respx.get(f"{JOB}/runs")
    route.side_effect = [
        httpx.Response(200, json={"items": [{"n": 1}, {"n": 2}], "next_cursor": "c2"}),
        httpx.Response(200, json={"items": [{"n": 3}], "next_cursor": None}),
    ]
    assert [r["n"] for r in hook.list_runs("p", "j")] == [1, 2, 3]
    assert route.calls.last.request.url.params["cursor"] == "c2"
    route.side_effect = [httpx.Response(200, json={"items": [{"n": 1}, {"n": 2}], "next_cursor": "c2"})]
    assert len(hook.list_runs("p", "j", limit=1)) == 1
    respx.get(f"{WS}/projects/p/jobs").respond(json={})
    assert hook.list_jobs("p") == []


def test_page_urls(set_conn, hook):
    assert hook.get_run_page_url("p", "j", "r 1") == f"{HOST}/data/ws/projects/p/jobs/j/runs/r%201"
    assert hook.get_project_page_url("p") == f"{HOST}/data/ws/projects/p"


@respx.mock
async def test_async_job_methods(hook):
    respx.get(f"{JOB}/runs/r1").respond(json={"status": "succeeded"})
    cancel = respx.post(f"{JOB}/runs/r1/cancel").respond(json={})
    assert (await hook.a_get_run_state("p", "j", "r1")).is_successful
    await hook.a_cancel_run("p", "j", "r1")
    assert cancel.called


# ---- project actions and runs ----
@respx.mock
def test_project_actions_and_runs(hook):
    base = f"{WS}/projects/p"
    lst = respx.get(f"{base}/actions").respond(json={"actions": []})
    act = respx.post(f"{base}/actions/dbt.build").respond(json={"run": {"run_id": "r"}})
    respx.get(f"{base}/runs/r").respond(json={"status": "failed", "error": "e"})
    cancel = respx.post(f"{base}/runs/r/cancel").respond(json={})
    hook.list_actions("p", branch="main", path="models")
    assert dict(lst.calls.last.request.url.params) == {"branch": "main", "path": "models"}
    hook.run_project_action("p", "dbt.build", branch="main", path="m", selection="tag:x", params={"a": 1})
    assert _body(act) == {"branch": "main", "path": "m", "selection": "tag:x", "params": {"a": 1}}
    hook.run_project_action("p", "dbt.build", branch="main", path="m")
    assert _body(act) == {"branch": "main", "path": "m"}
    assert hook.get_project_run_state("p", "r") == RunState("failed", "e")
    hook.cancel_project_run("p", "r")
    assert cancel.called


@respx.mock
async def test_async_project_methods(hook):
    respx.get(f"{WS}/projects/p/runs/r").respond(json={"status": "running"})
    cancel = respx.post(f"{WS}/projects/p/runs/r/cancel").respond(json={})
    assert (await hook.a_get_project_run_state("p", "r")).status == "running"
    await hook.a_cancel_project_run("p", "r")
    assert cancel.called


# ---- deployment and projects ----
@respx.mock
def test_deployment(hook):
    base = f"{WS}/projects/p/deployment"
    put = respx.put(base).respond(json={"deployed": []})
    val = respx.post(f"{base}/validate").respond(json={})
    delete = respx.delete(base).respond(204)
    hook.deploy("p", target="prod")
    assert _body(put) == {"target": "prod"}
    hook.validate_deployment("p", branch="dev")
    assert _body(val) == {"branch": "dev"}
    hook.destroy_deployment("p")
    assert delete.called


@respx.mock
def test_projects(hook):
    create = respx.post(f"{WS}/projects").respond(201, json={"slug": "p"})
    respx.get(f"{WS}/projects/p").respond(json={"slug": "p"})
    delete = respx.delete(f"{WS}/projects/p").respond(204)
    pull = respx.post(f"{WS}/projects/p/pull").respond(json={})
    hook.create_project("p", name="P", git_url=None, idempotency_token="k")
    assert _body(create) == {"slug": "p", "name": "P"}
    assert hook.get_project("p") == {"slug": "p"}
    hook.delete_project("p")
    assert delete.calls.last.request.url.params["confirm"] == "p"
    hook.pull_project("p", branch="main")
    assert _body(pull) == {"branch": "main"}


# ---- statements ----
@respx.mock
def test_submit_statement(hook):
    route = respx.post(f"{WS}/statements").respond(json={"id": "s", "state": "PENDING"})
    hook.submit_statement("SELECT 1", engine="sqe", catalog="c", wait_timeout=99, row_limit=5, query_tag="t")
    assert _body(route) == {
        "sql": "SELECT 1",
        "wait_timeout": 50,
        "engine": "sqe",
        "catalog": "c",
        "row_limit": 5,
        "query_tag": "t",
    }
    hook.submit_statement("SELECT 1", wait_timeout=-1, idempotency_token="k")
    assert _body(route)["wait_timeout"] == 0
    assert route.calls.last.request.headers["Idempotency-Key"] == "k"


@respx.mock
def test_get_and_cancel_statement(hook):
    get = respx.get(f"{WS}/statements/s").respond(json={"id": "s", "state": "RUNNING"})
    cancel = respx.post(f"{WS}/statements/s/cancel").respond(json={})
    assert hook.get_sql_statement_state("s").is_running
    assert "wait_timeout" not in get.calls.last.request.url.params
    hook.get_statement("s", wait_timeout=20)
    assert get.calls.last.request.url.params["wait_timeout"] == "20"
    hook.cancel_sql_statement("s")
    assert cancel.called


@respx.mock
async def test_async_statement(hook):
    get = respx.get(f"{WS}/statements/s").respond(json={"id": "s", "state": "SUCCEEDED"})
    respx.post(f"{WS}/statements/s/cancel").respond(json={})
    assert (await hook.a_get_sql_statement_state("s", wait_timeout=10)).is_successful
    assert get.calls.last.request.url.params["wait_timeout"] == "10"
    await hook.a_cancel_sql_statement("s")


@respx.mock
def test_iter_statement_rows_pages(hook):
    route = respx.get(f"{WS}/statements/s/result")
    route.side_effect = [httpx.Response(200, json={"rows": [[2]], "next_cursor": "c"}), httpx.Response(200, json={})]
    rows = list(hook.iter_statement_rows({"id": "s", "result": {"rows": [[1]], "next_cursor": "b"}}))
    assert rows == [[1], [2]]
    assert route.calls[0].request.url.params["cursor"] == "b"
    route.side_effect = None
    route.return_value = httpx.Response(200, json={"rows": [[9]]})
    assert list(hook.iter_statement_rows({"id": "s"})) == [[9]]


@respx.mock
def test_wait_for_statement_long_polls(hook):
    route = respx.get(f"{WS}/statements/s")
    route.side_effect = [
        httpx.Response(200, json={"id": "s", "state": "RUNNING"}),
        httpx.Response(200, json={"id": "s", "state": "SUCCEEDED"}),
    ]
    info = hook.wait_for_statement({"id": "s", "state": "PENDING"}, long_poll=7)
    assert info["state"] == "SUCCEEDED"
    assert route.calls.last.request.url.params["wait_timeout"] == "7"
    route.side_effect = [httpx.Response(200, json={"id": "s", "state": "SUCCEEDED"})]
    hook.wait_for_statement({"id": "s", "state": "PENDING"}, timeout=1000, long_poll=30)
    assert route.calls.last.request.url.params["wait_timeout"] == "30"


@respx.mock
def test_wait_for_statement_timeout_cancels(hook, monkeypatch):
    clock = iter([0.0, 5.0, 5.0, 11.0])
    monkeypatch.setattr(mod, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    respx.get(f"{WS}/statements/s").respond(json={"id": "s", "state": "RUNNING"})
    cancel = respx.post(f"{WS}/statements/s/cancel").respond(json={})
    with pytest.raises(CidpApiError, match="did not end within 10s"):
        hook.wait_for_statement({"id": "s", "state": "RUNNING"}, timeout=10)
    assert cancel.called


@respx.mock
def test_run_sql(hook):
    respx.post(f"{WS}/statements").respond(
        json={
            "id": "s",
            "state": "SUCCEEDED",
            "manifest": {"columns": [{"name": "n"}], "truncated": True},
            "result": {"rows": [[1], [2]]},
        }
    )
    res = hook.run_sql("SELECT n", catalog="c")
    assert (res.column_names, res.rows, res.truncated) == (["n"], [[1], [2]], True)


@respx.mock
def test_run_sql_failure(hook):
    respx.post(f"{WS}/statements").respond(json={"id": "s", "state": "FAILED", "error": {"code": "x", "message": "y"}})
    with pytest.raises(CidpApiError, match="statement s ended FAILED: x y"):
        hook.run_sql("SELECT")


# ---- catalog, quality, volumes ----
@respx.mock
def test_table_and_quality(hook):
    table = f"{WS}/catalogs/c/schemas/s/tables/t"
    respx.get(table).respond(json={"name": "t"})
    q = respx.post(f"{table}/quality-runs").respond(json={"id": "q"})
    assert hook.get_table("c", "s", "t") == {"name": "t"}
    hook.run_quality("c", "s", "t", check="not_null")
    assert _body(q) == {"check": "not_null"}
    hook.run_quality("c", "s", "t")
    assert q.calls.last.request.content == b""


@respx.mock
def test_volumes(hook):
    files = f"{WS}/catalogs/c/schemas/s/volumes/v/files"
    put = respx.put(files).respond(json={"path": "a.csv"})
    respx.get(f"{files}/content").respond(content=b"a,b\n")
    assert hook.upload_file("c", "s", "v", "a.csv", b"a,b\n", overwrite=True) == {"path": "a.csv"}
    req = put.calls.last.request
    assert (req.content, req.headers["Content-Type"]) == (b"a,b\n", "application/octet-stream")
    assert dict(req.url.params) == {"path": "a.csv", "overwrite": "true"}
    put.respond(204)
    assert hook.upload_file("c", "s", "v", "a.csv", b"") == {}
    assert hook.download_file("c", "s", "v", "a.csv") == b"a,b\n"
