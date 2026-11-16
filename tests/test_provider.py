from __future__ import annotations

import importlib
import json
from pathlib import Path
from unittest.mock import patch

import jsonschema
import pytest

import airflow
from airflow.providers.cidp.get_provider_info import get_provider_info
from airflow.providers.cidp.plugins.cidp import CidpJobRunLink

EXAMPLES = Path(__file__).parents[1] / "airflow/providers/cidp/example_dags"


def _modules(info: dict) -> list[str]:
    return [m for key in ("operators", "hooks", "sensors", "triggers") for e in info[key] for m in e["python-modules"]]


def test_provider_info_registers_the_connection():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-cidp"
    conn = info["connection-types"][0]
    assert conn["connection-type"] == "cidp"
    assert conn["hook-class-name"] == "airflow.providers.cidp.hooks.cidp.CidpHook"
    assert info["extra-links"] == ["airflow.providers.cidp.plugins.cidp.CidpJobRunLink"]


def test_package_name_passes_airflow_coherence_check():
    # Airflow treats ``apache-airflow*`` packages as built-in and refuses their hooks unless the
    # module path matches the name. Third-party providers use ``airflow-provider-*`` (ASF policy);
    # the old ``apache-airflow-providers-portal-data`` name failed this check.
    try:  # Airflow >= 3.2
        from airflow.sdk._shared.providers_discovery.providers_discovery import _check_builtin_provider_prefix
    except ImportError:  # Airflow 3.0 / 3.1 (MWAA)
        from airflow.providers_manager import _check_builtin_provider_prefix

    info = get_provider_info()
    for path in [c["hook-class-name"] for c in info["connection-types"]] + info["extra-links"]:
        assert _check_builtin_provider_prefix(info["package-name"], path), path
    hook = info["connection-types"][0]["hook-class-name"]
    assert not _check_builtin_provider_prefix("apache-airflow-providers-portal-data", hook)


def test_provider_info_matches_airflow_schema_and_imports():
    info = get_provider_info()
    schema = json.loads((Path(airflow.__file__).parent / "provider_info.schema.json").read_text())
    jsonschema.validate(info, schema)
    for module in _modules(info):
        importlib.import_module(module)
    for path in [c["hook-class-name"] for c in info["connection-types"]] + info["extra-links"]:
        module, _, name = path.rpartition(".")
        assert hasattr(importlib.import_module(module), name), path


def test_every_module_is_listed():
    info = get_provider_info()
    listed = set(_modules(info))
    root = Path(__file__).parents[1]
    for kind in ("operators", "hooks", "sensors", "triggers"):
        for f in (root / "airflow/providers/cidp" / kind).glob("*.py"):
            if f.stem != "__init__":
                assert f"airflow.providers.cidp.{kind}.{f.stem}" in listed, f


def test_run_link_reads_xcom():
    with patch("airflow.providers.cidp.version_compat.XCom.get_value", return_value="https://ui/run") as get:
        assert CidpJobRunLink().get_link(None, ti_key="k") == "https://ui/run"
    get.assert_called_once_with(ti_key="k", key="run_page_url")
    with patch("airflow.providers.cidp.version_compat.XCom.get_value", return_value=None):
        assert CidpJobRunLink().get_link(None, ti_key="k") == ""


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("example_*.py")), ids=lambda p: p.stem)
def test_example_dag_imports(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.dag
    assert dag.dag_id == path.stem
    assert dag.task_dict
