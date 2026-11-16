from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.cidp.exceptions import CidpConflictError, CidpNotFoundError
from airflow.providers.cidp.operators.cidp import task_idempotency_key
from airflow.providers.cidp.operators.cidp_projects import (
    CidpProjectCreateOperator,
    CidpProjectDeleteOperator,
    CidpProjectUpdateOperator,
)
from airflow.providers.cidp.operators.cidp_quality import CidpQualityRunOperator


def _op(cls, **kw):
    op = cls(task_id="t", **kw)
    op.hook = MagicMock()
    return op


def test_create_project(context):
    op = _op(CidpProjectCreateOperator, slug="p", git_url="https://g/r.git", branch="main")
    op.hook.create_project.return_value = {"slug": "p"}
    assert op.execute(context) == {"slug": "p"}
    kw = op.hook.create_project.call_args.kwargs
    assert kw["git_url"] == "https://g/r.git"
    assert kw["idempotency_token"] == task_idempotency_key(context, "create-project", None, "p")


def test_create_existing_project(context):
    op = _op(CidpProjectCreateOperator, slug="p")
    op.hook.create_project.side_effect = CidpConflictError("taken", status_code=409)
    with pytest.raises(CidpConflictError):
        op.execute(context)
    op.ignore_existing_project = True
    op.hook.get_project.return_value = {"slug": "p", "existing": True}
    assert op.execute(context)["existing"] is True


def test_update_project(context):
    op = _op(CidpProjectUpdateOperator, project="p", branch="dev")
    op.execute(context)
    op.hook.pull_project.assert_called_once_with("p", branch="dev")


def test_delete_project(context):
    op = _op(CidpProjectDeleteOperator, project="p")
    op.execute(context)
    op.hook.delete_project.assert_called_once_with("p")
    op.hook.delete_project.side_effect = CidpNotFoundError("gone", status_code=404)
    with pytest.raises(CidpNotFoundError):
        op.execute(context)
    op.ignore_missing = True
    op.execute(context)


def _quality(**kw):
    return _op(CidpQualityRunOperator, catalog="c", schema="s", table="t", **kw)


def test_quality_passes(context):
    op = _quality(check="not_null", min_score=0.9)
    op.hook.run_quality.return_value = {"id": "q", "passed": 3, "failed": 0, "errored": 0, "score": 1.0}
    assert op.execute(context)["id"] == "q"
    op.hook.run_quality.assert_called_once_with("c", "s", "t", check="not_null")


@pytest.mark.parametrize(
    ("kw", "run", "match"),
    [
        ({}, {"failed": 1}, "1 failed, 0 errored"),
        ({}, {"errored": 2}, "0 failed, 2 errored"),
        ({"fail_on_failure": False, "min_score": 0.9}, {"failed": 1, "score": 0.5}, "below 0.9"),
        ({"min_score": 0.9}, {}, "score None"),
    ],
)
def test_quality_fails(context, kw, run, match):
    op = _quality(**kw)
    op.hook.run_quality.return_value = run
    with pytest.raises(AirflowException, match=match):
        op.execute(context)


def test_quality_failure_tolerated(context):
    op = _quality(fail_on_failure=False)
    op.hook.run_quality.return_value = {"failed": 1}
    assert op.execute(context) == {"failed": 1}
