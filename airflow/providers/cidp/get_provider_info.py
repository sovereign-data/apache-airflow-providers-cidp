from airflow.providers.cidp import __version__

_PKG = "airflow.providers.cidp"
_INTEGRATION = "CIDP"


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-cidp",
        "name": "Cloud Independent Data Platform (CIDP)",
        "description": "Jobs, SQL statements and projects of CIDP, the Cloud Independent Data Platform.",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": _INTEGRATION,
                "external-doc-url": "https://github.com/schubergphilis/ai-portal-data",
                "tags": ["service"],
            }
        ],
        "operators": [
            {
                "integration-name": _INTEGRATION,
                "python-modules": [
                    f"{_PKG}.operators.cidp",
                    f"{_PKG}.operators.cidp_sql",
                    f"{_PKG}.operators.cidp_projects",
                    f"{_PKG}.operators.cidp_quality",
                ],
            }
        ],
        "hooks": [
            {
                "integration-name": _INTEGRATION,
                "python-modules": [f"{_PKG}.hooks.cidp", f"{_PKG}.hooks.cidp_base"],
            }
        ],
        "sensors": [
            {
                "integration-name": _INTEGRATION,
                "python-modules": [f"{_PKG}.sensors.cidp", f"{_PKG}.sensors.cidp_sql"],
            }
        ],
        "triggers": [{"integration-name": _INTEGRATION, "python-modules": [f"{_PKG}.triggers.cidp"]}],
        "connection-types": [
            {
                "connection-type": "cidp",
                "hook-class-name": f"{_PKG}.hooks.cidp.CidpHook",
            }
        ],
        "extra-links": [f"{_PKG}.plugins.cidp.CidpJobRunLink"],
    }
