"""Airflow 2/3 import shims (mypy checks against the pinned Airflow 3).

Airflow 3.0.x (MWAA) takes some fallbacks: ``airflow.sdk.BaseHook`` arrived in 3.1, so 3.0 uses
``airflow.hooks.base``, and ``TaskDeferred`` comes from ``airflow.exceptions``. Run
``scripts/test-airflow-version.sh`` after changing an import here.
"""

# ruff: noqa: F401
# mypy: disable-error-code="no-redef,attr-defined,assignment"

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2
    from airflow.hooks.base import BaseHook

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

try:  # Airflow 3
    from airflow.sdk.bases.sensor import BaseSensorOperator
except ImportError:  # Airflow 2
    from airflow.sensors.base import BaseSensorOperator

try:  # Airflow 3
    from airflow.sdk import BaseOperatorLink
except ImportError:  # Airflow 2
    from airflow.models import BaseOperatorLink

try:  # Airflow 3
    from airflow.sdk.execution_time.xcom import XCom
except ImportError:  # Airflow 2
    from airflow.models.xcom import XCom

try:  # Airflow 3
    from airflow.sdk.exceptions import TaskDeferred
except ImportError:  # Airflow 2
    from airflow.exceptions import TaskDeferred

try:  # Airflow 3
    from airflow.sdk.log import mask_secret
except ImportError:  # Airflow 2
    from airflow.utils.log.secrets_masker import mask_secret

try:  # Airflow 3
    from airflow.sdk import Context
except ImportError:  # Airflow 2
    from airflow.utils.context import Context

from airflow.triggers.base import BaseTrigger, TriggerEvent
