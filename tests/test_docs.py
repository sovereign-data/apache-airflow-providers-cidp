"""Keep the docs in step with the code: every parameter table matches the signature.

A plain text parse of the RST, so it runs without Sphinx (and on Airflow 3.0.6).
"""

from __future__ import annotations

import importlib
import inspect
import re
from pathlib import Path

import pytest

from airflow.providers.cidp.get_provider_info import get_provider_info
from airflow.providers.cidp.hooks.cidp import CidpHook

ROOT = Path(__file__).parents[1]
DOCS = ROOT / "docs"
STOP = {"BaseOperator", "BaseSensorOperator", "BaseTrigger", "BaseHook", "LoggingMixin", "object"}
NOT_ACCEPTED = {"CidpPartitionSensor": {"sql"}}  # inherited, but set by the sensor itself
KINDS = {"operators": "operator", "sensors": "sensor", "triggers": "trigger"}
LABEL = re.compile(r"^\.\. _howto/(\w+):(\w+):$", re.MULTILINE)


def _sections() -> dict[tuple[str, str], str]:
    out = {}
    for page in DOCS.rglob("*.rst"):
        if "_build" in page.parts:
            continue
        text = page.read_text()
        marks = list(LABEL.finditer(text))
        for i, m in enumerate(marks):
            end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
            out[(m[1], m[2])] = text[m.end() : end]
    return out


SECTIONS = _sections()


def _public_classes() -> list[tuple[str, type]]:
    info = get_provider_info()
    found: list[tuple[str, type]] = [("hook", CidpHook)]
    for key, kind in KINDS.items():
        for entry in info[key]:
            for name in entry["python-modules"]:
                module = importlib.import_module(name)
                found += [
                    (kind, c)
                    for n, c in vars(module).items()
                    if inspect.isclass(c) and c.__module__ == name and n.startswith("Cidp")
                ]
    return found


def _params(cls: type) -> dict[str, object]:
    out: dict[str, object] = {}
    for k in cls.__mro__:
        if k.__name__ in STOP:
            break
        if "__init__" not in vars(k):
            continue
        for n, p in inspect.signature(k.__init__).parameters.items():
            if n != "self" and p.kind not in (p.VAR_KEYWORD, p.VAR_POSITIONAL):
                out.setdefault(n, p.default)
    for n in NOT_ACCEPTED.get(cls.__name__, ()):
        out.pop(n)
    return out


def _table(section: str) -> list[list[str]]:
    """The rows of the first list-table whose header starts with ``Parameter``."""
    for block in section.split(".. list-table::")[1:]:
        rows: list[list[str]] = []
        for line in block.splitlines():
            if line.startswith("   * - "):
                rows.append([line[7:].strip()])
            elif line.startswith("     - ") and rows:
                rows[-1].append(line[7:].strip())
            elif line and not line.startswith("   "):
                break
        if rows and rows[0][0] == "Parameter":
            return rows
    raise AssertionError("no parameter table")


def _default(value: object) -> str:
    if value is inspect.Parameter.empty:
        return "required"
    if callable(value):
        return "any row"
    return f"``{value!r}``"


CLASSES = _public_classes()


@pytest.mark.parametrize(("kind", "cls"), CLASSES, ids=[c.__name__ for _, c in CLASSES])
def test_parameter_table_matches_signature(kind, cls):
    section = SECTIONS.get((kind, cls.__name__))
    assert section is not None, f"no '.. _howto/{kind}:{cls.__name__}:' section in docs/"
    header, *rows = _table(section)
    documented = {r[0].strip("`"): r for r in rows}
    params = _params(cls)
    assert list(documented) == list(params)
    templated = getattr(cls, "template_fields", None)
    assert ("Templated" in header) == (templated is not None)
    for name, default in params.items():
        row = documented[name]
        assert row[1] == _default(default), name
        if templated is not None:
            assert row[2] == ("yes" if name in templated else "no"), name
        assert row[-1], f"{name} has no description"


def test_every_connection_extra_is_documented():
    source = (ROOT / "airflow/providers/cidp/hooks/cidp_base.py").read_text()
    extras = set(re.findall(r'(?:_extra\(\)|extra)(?:\.get\(|\[)"(\w+)"', source))
    assert len(extras) >= 8, extras  # the pattern still finds them
    page = (DOCS / "connections/cidp.rst").read_text()
    missing = {e for e in extras if f"``{e}``" not in page}
    assert not missing


def test_every_workspace_parameter_is_templated():
    for _, cls in CLASSES:
        if "workspace" in _params(cls) and hasattr(cls, "template_fields"):
            assert "workspace" in cls.template_fields, cls.__name__
