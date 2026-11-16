"""SQL and partition sensors (cf. ``DatabricksSqlSensor`` and ``DatabricksPartitionSensor``)."""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable, Sequence
from functools import cached_property
from typing import Any

from airflow.providers.cidp.hooks.cidp import CidpHook
from airflow.providers.cidp.hooks.cidp_base import DEFAULT_CONN_ID
from airflow.providers.cidp.version_compat import BaseSensorOperator, Context

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OPERATORS = ("=", "!=", "<>", "<", "<=", ">", ">=")


def _rows_exist(rows: list[list[Any]]) -> bool:
    return len(rows) > 0


class CidpSqlSensor(BaseSensorOperator):
    """Poke until ``sql`` returns what ``handler`` accepts: by default, any row (cf. ``DatabricksSqlSensor``)."""

    template_fields: Sequence[str] = ("sql", "catalog", "schema", "workspace", "cidp_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}  # noqa: RUF012 — Airflow's convention

    def __init__(
        self,
        *,
        sql: str,
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        handler: Callable[[list[list[Any]]], bool] = _rows_exist,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        workspace: str | None = None,
        statement_timeout: float | None = 600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.engine = engine
        self.catalog = catalog
        self.schema = schema
        self.handler = handler
        self.cidp_conn_id = cidp_conn_id
        self.workspace = workspace
        self.statement_timeout = statement_timeout

    @cached_property
    def hook(self) -> CidpHook:
        return CidpHook(self.cidp_conn_id, workspace=self.workspace)

    def _get_sql(self) -> str:
        return self.sql

    def poke(self, context: Context) -> bool:
        result = self.hook.run_sql(
            self._get_sql(),
            engine=self.engine,
            catalog=self.catalog,
            schema=self.schema,
            row_limit=1000,
            timeout=self.statement_timeout,
        )
        return bool(self.handler(result.rows))


def _ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"not a plain SQL identifier: {name!r}")
    return f'"{name}"'


def _literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, datetime.datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


class CidpPartitionSensor(CidpSqlSensor):
    """Poke until ``table_name`` has rows matching ``partitions`` (cf. ``DatabricksPartitionSensor``).

    ``partitions`` maps a column to a value or a list of values (any of them). Identifiers must be
    plain names; values are quoted as SQL literals.
    """

    template_fields: Sequence[str] = (
        "table_name",
        "partitions",
        "catalog",
        "schema",
        "workspace",
        "cidp_conn_id",
    )

    def __init__(
        self,
        *,
        table_name: str,
        partitions: dict[str, Any],
        partition_operator: str = "=",
        catalog: str | None = None,
        schema: str = "default",
        **kwargs,
    ):
        if partition_operator not in _OPERATORS:
            raise ValueError(f"partition_operator must be one of {_OPERATORS}")
        super().__init__(sql="", catalog=catalog, schema=schema, **kwargs)
        self.table_name = table_name
        self.partitions = partitions
        self.partition_operator = partition_operator

    def _get_sql(self) -> str:
        if not self.partitions:
            raise ValueError("partitions must name at least one column")
        parts = [p for p in (self.catalog, self.schema) if p] + [self.table_name]
        table = ".".join(_ident(p) for p in parts)
        conditions = []
        for column, value in self.partitions.items():
            col = _ident(column)
            if isinstance(value, (list, tuple, set)):
                if not value:
                    raise ValueError(f"partition {column!r} lists no values")
                ors = " OR ".join(f"{col} {self.partition_operator} {_literal(v)}" for v in value)
                conditions.append(f"({ors})")
            else:
                conditions.append(f"{col} {self.partition_operator} {_literal(value)}")
        return f"SELECT 1 FROM {table} WHERE {' AND '.join(conditions)} LIMIT 1"  # noqa: S608 — quoted above
