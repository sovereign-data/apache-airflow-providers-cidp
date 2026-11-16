#!/usr/bin/env bash
# Run the test suite against another Airflow version, installed with that version's official
# constraints (as MWAA does). Default: 3.0.6, the version MWAA runs.
#
#   scripts/test-airflow-version.sh            # Airflow 3.0.6, Python 3.12
#   scripts/test-airflow-version.sh 3.1.0 3.12 -k trigger   # extra args go to pytest
set -euo pipefail

AIRFLOW_VERSION="${1:-3.0.6}"
PYTHON_VERSION="${2:-3.12}"
shift $(( $# < 2 ? $# : 2 ))  # the rest goes to pytest
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${ROOT}/.venv-airflow-${AIRFLOW_VERSION}"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv venv -q --allow-existing -p "${PYTHON_VERSION}" "${WORK}"
uv pip install -q -p "${WORK}" -c "${CONSTRAINTS}" \
    "apache-airflow==${AIRFLOW_VERSION}" -e "${ROOT}" \
    pytest pytest-asyncio respx jsonschema

export AIRFLOW_HOME="${WORK}/airflow-home"
cd "${ROOT}"
"${WORK}/bin/python" -m pytest -q -p no:cacheprovider "$@"
