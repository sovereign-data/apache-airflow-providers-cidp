"""Data-quality runs (no Databricks counterpart; the API is ``preview``)."""

from __future__ import annotations

from collections.abc import Sequence

from airflow.exceptions import AirflowException

from airflow.providers.cidp.operators.cidp import _CidpOperator
from airflow.providers.cidp.version_compat import Context


class CidpQualityRunOperator(_CidpOperator):
    """Run a table's quality checks (or one ``check``) and fail when any check failed or errored.

    :param fail_on_failure: raise when ``failed`` or ``errored`` is not zero (default True).
    :param min_score: raise when the run's score is below this.
    """

    template_fields: Sequence[str] = ("catalog", "schema", "table", "check", "workspace", "cidp_conn_id")

    def __init__(
        self,
        *,
        catalog: str,
        schema: str,
        table: str,
        check: str | None = None,
        fail_on_failure: bool = True,
        min_score: float | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.check = check
        self.fail_on_failure = fail_on_failure
        self.min_score = min_score

    def execute(self, context: Context) -> dict:
        run = self.hook.run_quality(self.catalog, self.schema, self.table, check=self.check)
        name = f"{self.catalog}.{self.schema}.{self.table}"
        failed, errored = int(run.get("failed") or 0), int(run.get("errored") or 0)
        self.log.info(
            "quality run %s on %s: %s passed, %s failed, %s errored of %s (score %s)",
            run.get("id"),
            name,
            run.get("passed"),
            failed,
            errored,
            run.get("total"),
            run.get("score"),
        )
        if self.fail_on_failure and (failed or errored):
            raise AirflowException(f"quality run on {name}: {failed} failed, {errored} errored")
        score = run.get("score")
        if self.min_score is not None and (score is None or float(score) < self.min_score):
            raise AirflowException(f"quality run on {name}: score {score} is below {self.min_score}")
        return run
