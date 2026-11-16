from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from airflow.models.connection import Connection

from airflow.providers.cidp.hooks.cidp import CidpHook
from airflow.providers.cidp.hooks.cidp_base import CidpBaseHook

HOST = "https://cidp.test"
API = f"{HOST}/api/data/v1"
WS = f"{API}/workspaces/ws"


def make_conn(extra: dict | None = None, **kw) -> Connection:
    fields = {"conn_id": "cidp_default", "conn_type": "cidp", "host": HOST, "password": "tok"}
    fields.update(kw)
    return Connection(extra=json.dumps({"workspace": "ws", **(extra or {})}), **fields)


@pytest.fixture
def set_conn(monkeypatch):
    """Point every hook at a connection built from ``extra`` and fields."""

    def _set(extra: dict | None = None, **kw) -> Connection:
        conn = make_conn(extra, **kw)
        monkeypatch.setattr(CidpBaseHook, "get_connection", classmethod(lambda cls, _id: conn))
        return conn

    _set()
    return _set


@pytest.fixture
def hook(set_conn) -> CidpHook:
    return CidpHook(retry_delay=0)


@pytest.fixture
def context():
    ti = MagicMock(dag_id="dag", task_id="task", run_id="manual__1", map_index=-1, try_number=1)
    return {"ti": ti}
