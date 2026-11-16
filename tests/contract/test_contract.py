"""Every request the hook makes exists in the data API's OpenAPI contract (vendored from the SDK).

The hook's methods run against a recorder; each request must match a contract path and method,
send only declared query parameters, an ``Idempotency-Key`` only where declared, and JSON bodies
with only the request schema's properties.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import pytest
import respx

from tests.conftest import HOST

SPEC = json.loads((Path(__file__).parent / "openapi-v1.json").read_text())
_METHODS = ("get", "post", "put", "patch", "delete")


def _resolve(schema: dict) -> dict:
    while "$ref" in schema:
        schema = SPEC["components"]["schemas"][schema["$ref"].rsplit("/", 1)[-1]]
    for variant in schema.get("anyOf") or schema.get("oneOf") or []:
        if variant.get("type") != "null":
            return _resolve(variant)
    return schema


def _operation(method: str, path: str) -> dict | None:
    best = None
    for template, ops in SPEC["paths"].items():
        pattern = "^" + re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(template)) + "$"
        if re.match(pattern, path) and method.lower() in ops:
            # The most literal match wins (``/statements/execute`` over ``/statements/{id}``).
            score = template.count("{")
            if best is None or score < best[0]:
                best = (score, ops[method.lower()])
    return best[1] if best else None


def _answer(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"id": "s", "state": "SUCCEEDED", "run_id": "r", "status": "succeeded"})


def _calls(h):
    yield lambda: h.run_now("p", "j", job_parameters={"a": 1}, idempotency_token="k")
    yield lambda: h.get_job("p", "j")
    yield lambda: h.set_job_enabled("p", "j", True)
    yield lambda: h.list_jobs("p")
    yield lambda: h.list_runs("p", "j")
    yield lambda: h.get_run("p", "j", "r")
    yield lambda: h.cancel_run("p", "j", "r")
    yield lambda: h.repair_run("p", "j", "r", idempotency_token="k")
    yield lambda: h.list_actions("p", branch="main", path="m")
    yield lambda: h.run_project_action(
        "p", "dbt.build", branch="main", path="m", selection="s", namespace="n", params={"a": 1}, catalog="c"
    )
    yield lambda: h.get_project_run("p", "r")
    yield lambda: h.cancel_project_run("p", "r")
    yield lambda: h.deploy("p", target="prod")
    yield lambda: h.validate_deployment("p", target="prod", branch="main")
    yield lambda: h.destroy_deployment("p")
    yield lambda: h.create_project(
        "p",
        name="P",
        description="d",
        git_url="https://git.test/r.git",
        branch="main",
        template="t",
        owner_group="g",
        idempotency_token="k",
    )
    yield lambda: h.get_project("p")
    yield lambda: h.delete_project("p")
    yield lambda: h.pull_project("p", branch="main")
    yield lambda: h.submit_statement(
        "SELECT 1",
        engine="sqe",
        catalog="c",
        schema="s",
        wait_timeout=10,
        on_wait_timeout="CONTINUE",
        row_limit=1,
        query_tag="t",
        idempotency_token="k",
    )
    yield lambda: h.get_statement("s", wait_timeout=5)
    yield lambda: h.cancel_sql_statement("s")
    yield lambda: h.get_statement_result_page("s", cursor="c")
    yield lambda: h.get_table("c", "s", "t")
    yield lambda: h.run_quality("c", "s", "t", check="x")
    yield lambda: h.upload_file("c", "s", "v", "a.csv", b"x", overwrite=True)
    yield lambda: h.download_file("c", "s", "v", "a.csv")
    yield lambda: h.test_connection()


@pytest.fixture
def requests(hook):
    with respx.mock(base_url=HOST) as router:
        router.route().mock(side_effect=_answer)
        for call in _calls(hook):
            call()
        return [c.request for c in router.calls]


def test_every_hook_method_was_exercised(requests):
    assert len(requests) == 28


def test_requests_match_the_contract(requests):
    problems = []
    for req in requests:
        what = f"{req.method} {req.url.path}"
        op = _operation(req.method, req.url.path)
        if op is None:
            problems.append(f"{what}: not in the contract")
            continue
        declared = {p["name"]: p["in"] for p in op.get("parameters", [])}
        for name in req.url.params:
            if declared.get(name) != "query":
                problems.append(f"{what}: undeclared query parameter {name!r}")
        if "Idempotency-Key" in req.headers and declared.get("Idempotency-Key") != "header":
            problems.append(f"{what}: Idempotency-Key is not declared")
        if req.headers.get("content-type") == "application/json":
            body = json.loads(req.content)
            content = op.get("requestBody", {}).get("content", {})
            if "application/json" not in content:
                problems.append(f"{what}: sends JSON, the contract takes {list(content)}")
                continue
            schema = _resolve(content["application/json"]["schema"])
            props = schema.get("properties", {})
            if schema.get("additionalProperties") is not True:
                problems += [f"{what}: body field {k!r} not in the schema" for k in body if k not in props]
            problems += [f"{what}: required {k!r} missing" for k in schema.get("required", []) if k not in body]
    assert not problems, "\n".join(problems)
