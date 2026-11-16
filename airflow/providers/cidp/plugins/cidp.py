from __future__ import annotations

from airflow.providers.cidp.version_compat import BaseOperatorLink

XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"
XCOM_STATEMENT_ID_KEY = "statement_id"


class CidpJobRunLink(BaseOperatorLink):
    """``See CIDP Run``: the run's page in the CIDP UI, from XCom (cf. ``DatabricksJobRunLink``)."""

    name = "See CIDP Run"

    def get_link(self, operator, *, ti_key=None) -> str:
        from airflow.providers.cidp.version_compat import XCom

        value = XCom.get_value(ti_key=ti_key, key=XCOM_RUN_PAGE_URL_KEY)
        return value or ""
