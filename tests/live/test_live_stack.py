"""Live tests against a running CIDP data service (the ai-portal-data dev stack or any deployment).

Skipped unless both are set:

* ``CIDP_LIVE_HOST``: the data service, for example ``http://localhost:8095``.
* ``CIDP_LIVE_TOKEN``: an access token for a person who may query the table and create a project.

Optional: ``CIDP_LIVE_WORKSPACE`` (``workspace1``), ``CIDP_LIVE_TABLE`` (``workspace1.default.customers``, needs a
``country`` column with a row for ``NL``), ``CIDP_LIVE_RUNNER`` (``1``: also run ``python.run`` on the runs executor).
The tests make a project and a volume with random names and delete both. See ``docs/live-testing.rst``.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from unittest import mock

import httpx
import pytest

HOST = os.environ.get("CIDP_LIVE_HOST", "")
TOKEN = os.environ.get("CIDP_LIVE_TOKEN", "")
WS = os.environ.get("CIDP_LIVE_WORKSPACE", "workspace1")
TABLE = os.environ.get("CIDP_LIVE_TABLE", "workspace1.default.customers")
if not re.fullmatch(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*", TABLE):
    raise ValueError("CIDP_LIVE_TABLE must be catalog.schema.table (plain identifiers)")
CATALOG, SCHEMA, NAME = TABLE.split(".")

pytestmark = pytest.mark.skipif(not (HOST and TOKEN), reason="CIDP_LIVE_HOST and CIDP_LIVE_TOKEN not set")


@pytest.fixture(scope="module", autouse=True)
def _connection():
    insecure = HOST.startswith("http://")
    conn = {
        "conn_type": "cidp",
        "host": HOST,
        "password": TOKEN,
        "extra": {"auth_type": "token", "workspace": WS, "allow_insecure_http": insecure},
    }
    with mock.patch.dict(os.environ, {"AIRFLOW_CONN_CIDP_DEFAULT": json.dumps(conn)}):
        yield


@pytest.fixture(scope="module")
def hook(_connection):
    from airflow.providers.cidp.hooks.cidp import CidpHook

    return CidpHook()


@pytest.fixture(scope="module")
def api():
    with httpx.Client(base_url=f"{HOST}/api/data/v1", headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60) as c:
        yield c


def _ctx(task_id: str) -> dict:
    ti = mock.MagicMock(dag_id="live", task_id=task_id, run_id="manual__live", map_index=-1, try_number=1)
    return {
        "ti": ti,
        "task_instance": ti,
        "dag_run": mock.MagicMock(run_id="manual__live"),
        "dag": mock.MagicMock(dag_id="live"),
        "run_id": "manual__live",
    }


def test_connection(hook):
    ok, message = hook.test_connection()
    assert ok, message


def test_run_sql(hook):
    result = hook.run_sql(f"SELECT count(*) AS n FROM {TABLE}")  # noqa: S608 (TABLE is checked above)
    assert result.column_names == ["n"]
    assert int(result.rows[0][0]) > 0


def test_statement_submit_wait_rows(hook):
    info = hook.submit_statement(f"SELECT country, count(*) AS n FROM {TABLE} GROUP BY country")  # noqa: S608 (TABLE is checked above)
    info = hook.wait_for_statement(info, timeout=120)
    assert list(hook.iter_statement_rows(info))


def test_sql_operator(_connection):
    from airflow.providers.cidp.operators.cidp_sql import CidpSqlOperator

    CidpSqlOperator(task_id="sql", sql="SELECT 1; SELECT 2").execute(_ctx("sql"))


def test_sql_sensor(_connection):
    from airflow.providers.cidp.sensors.cidp_sql import CidpSqlSensor

    assert CidpSqlSensor(task_id="s", sql=f"SELECT 1 FROM {TABLE} LIMIT 1").poke(_ctx("s"))  # noqa: S608 (TABLE is checked above)


def test_partition_sensor(_connection):
    from airflow.providers.cidp.sensors.cidp_sql import CidpPartitionSensor

    sensor = CidpPartitionSensor(
        task_id="p", catalog=CATALOG, schema=SCHEMA, table_name=NAME, partitions={"country": "NL"}
    )
    assert sensor.poke(_ctx("p"))


def test_get_table(hook):
    assert hook.get_table(CATALOG, SCHEMA, NAME)


@pytest.fixture(scope="module")
def project(hook):
    slug = f"af-live-{uuid.uuid4().hex[:6]}"
    hook.create_project(slug, name="airflow live test")
    yield slug
    hook.delete_project(slug)


def test_project_and_jobs(hook, project):
    assert hook.get_project(project).get("slug") == project
    assert isinstance(hook.list_jobs(project), (list, dict))


def test_project_action_sql_run(hook, api, project):
    r = api.post(
        f"/workspaces/{WS}/projects/{project}/files", json={"branch": "main", "path": "q.sql", "starter": "sql-query"}
    )
    assert r.status_code in (200, 201), r.text[:300]
    assert hook.run_project_action(project, "sql.run", branch="main", path="q.sql")


@pytest.mark.skipif(os.environ.get("CIDP_LIVE_RUNNER", "1") != "1", reason="CIDP_LIVE_RUNNER=0")
def test_project_action_python_run(api, project):
    from airflow.providers.cidp.operators.cidp import CidpProjectActionOperator

    r = api.post(
        f"/workspaces/{WS}/projects/{project}/files",
        json={"branch": "main", "path": "p.py", "starter": "python-script"},
    )
    assert r.status_code in (200, 201), r.text[:300]
    CidpProjectActionOperator(
        task_id="py", project=project, action="python.run", path="p.py", polling_period_seconds=3
    ).execute(_ctx("py"))


def test_volume_upload_download(hook, api):
    vol = f"afvol{uuid.uuid4().hex[:6]}"
    base = f"/workspaces/{WS}/catalogs/{CATALOG}/schemas/{SCHEMA}/volumes"
    r = api.post(base, json={"name": vol})
    assert r.status_code in (200, 201), r.text[:300]
    try:
        hook.upload_file(CATALOG, SCHEMA, vol, "a/hello.txt", b"hello cidp")
        assert hook.download_file(CATALOG, SCHEMA, vol, "a/hello.txt") == b"hello cidp"
    finally:
        api.delete(f"{base}/{vol}", params={"confirm": vol})
