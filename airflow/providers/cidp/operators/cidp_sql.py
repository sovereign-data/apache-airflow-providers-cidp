"""Run SQL and return its rows (cf. ``DatabricksSqlOperator``)."""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException

from airflow.providers.cidp.hooks.cidp import StatementResult
from airflow.providers.cidp.operators.cidp import _CidpOperator, airflow_query_tag
from airflow.providers.cidp.version_compat import Context

_FORMATS = ("csv", "json", "jsonl")


def split_statements(sql: str | list[str]) -> list[str]:
    """A list as is; a string split on ``;`` outside quotes and comments."""
    if isinstance(sql, list):
        return [s.strip() for s in sql if s and s.strip()]
    out: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:  # '' or "" escapes the quote
                    buf.append(sql[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end < 0 else end
            buf.append(sql[i:end])
            i = end
            continue
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = n if end < 0 else end + 2
            buf.append(sql[i:end])
            i = end
            continue
        elif ch == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


class CidpSqlOperator(_CidpOperator):
    """Run one or more SQL statements and return the rows (cf. ``DatabricksSqlOperator``).

    Statements run one after another as the connection's identity, through the statements API.
    Returns the last statement's rows (``return_last``) or every statement's, as lists. With
    ``output_path`` the rows are written there (csv, json or jsonl) instead of being returned.
    Rows are never logged.
    """

    template_fields: Sequence[str] = (
        "sql",
        "catalog",
        "schema",
        "query_tag",
        "output_path",
        "workspace",
        "cidp_conn_id",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}  # noqa: RUF012 — Airflow's convention

    def __init__(
        self,
        *,
        sql: str | list[str],
        engine: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        row_limit: int | None = None,
        return_last: bool = True,
        split_statements: bool = True,
        output_path: str | None = None,
        output_format: str = "csv",
        csv_params: dict[str, Any] | None = None,
        query_tag: str | None = None,
        include_airflow_query_tags: bool = True,
        timeout: float | None = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if output_format not in _FORMATS:
            raise ValueError(f"output_format must be one of {_FORMATS}")
        self.sql = sql
        self.engine = engine
        self.catalog = catalog
        self.schema = schema
        self.row_limit = row_limit
        self.return_last = return_last
        self.split_statements = split_statements
        self.output_path = output_path
        self.output_format = output_format
        self.csv_params = csv_params or {}
        self.query_tag = query_tag
        self.include_airflow_query_tags = include_airflow_query_tags
        self.timeout = timeout

    def _statements(self) -> list[str]:
        if self.split_statements:
            return split_statements(self.sql)
        return [self.sql] if isinstance(self.sql, str) else list(self.sql)

    def execute(self, context: Context) -> Any:
        statements = self._statements()
        if not statements:
            raise AirflowException("no SQL to run")
        tag = self.query_tag or (airflow_query_tag(context) if self.include_airflow_query_tags else None)
        results: list[StatementResult] = []
        for i, statement in enumerate(statements, 1):
            self.log.info("running statement %d of %d", i, len(statements))
            results.append(
                self.hook.run_sql(
                    statement,
                    engine=self.engine,
                    catalog=self.catalog,
                    schema=self.schema,
                    row_limit=self.row_limit,
                    query_tag=tag,
                    timeout=self.timeout,
                )
            )
            if results[-1].truncated:
                self.log.warning("statement %d: the result was cut at the row limit", i)
        if self.output_path:
            self._write(results[-1] if self.return_last else _concat(results))
            return None
        if self.return_last:
            return results[-1].rows
        return [r.rows for r in results]

    def _write(self, result: StatementResult) -> None:
        if self.output_path is None:  # pragma: no cover - set in __init__
            raise AirflowException("output_path is required")
        names = result.column_names
        with open(self.output_path, "w", newline="", encoding="utf-8") as fh:
            if self.output_format == "csv":
                params = dict(self.csv_params)
                header = params.pop("header", True)
                writer = csv.writer(fh, **params)
                if header and names:
                    writer.writerow(names)
                writer.writerows(result.rows)
            elif self.output_format == "json":
                json.dump([dict(zip(names, row, strict=False)) for row in result.rows], fh, default=str)
            else:
                for row in result.rows:
                    fh.write(json.dumps(dict(zip(names, row, strict=False)), default=str) + "\n")
        self.log.info("wrote %d rows to %s", len(result.rows), self.output_path)


def _concat(results: list[StatementResult]) -> StatementResult:
    """Every statement's rows under the last one's columns (for ``return_last=False`` with a file)."""
    return StatementResult(
        columns=results[-1].columns,
        rows=[row for r in results for row in r.rows],
        truncated=any(r.truncated for r in results),
    )
