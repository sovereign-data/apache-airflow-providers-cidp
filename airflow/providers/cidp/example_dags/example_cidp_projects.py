"""Manage a CIDP project, check a table's quality and wait on SQL.

Uses the ``cidp_default`` connection; see ``example_cidp`` for its fields.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cidp.operators.cidp_projects import (
    CidpProjectCreateOperator,
    CidpProjectDeleteOperator,
    CidpProjectUpdateOperator,
)
from airflow.providers.cidp.operators.cidp_quality import CidpQualityRunOperator
from airflow.providers.cidp.sensors.cidp import CidpSQLStatementsSensor
from airflow.providers.cidp.sensors.cidp_sql import CidpSqlSensor

with DAG(
    dag_id="example_cidp_projects",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "cidp"],
) as dag:
    # [START howto_operator_cidp_project_create]
    create = CidpProjectCreateOperator(
        task_id="create_project",
        slug="ledger-etl",
        name="Ledger ETL",
        git_url="https://git.example.com/data/ledger-etl.git",
        branch="main",
        ignore_existing_project=True,
    )
    # [END howto_operator_cidp_project_create]

    # [START howto_operator_cidp_project_update]
    pull = CidpProjectUpdateOperator(task_id="pull_project", project="ledger-etl", branch="main")
    # [END howto_operator_cidp_project_update]

    # [START howto_sensor_cidp_sql]
    loaded = CidpSqlSensor(
        task_id="wait_for_load",
        sql="SELECT 1 FROM lake.raw.load_log WHERE load_date = DATE '{{ ds }}' AND status = 'done'",
        poke_interval=300,
        mode="reschedule",
    )
    # [END howto_sensor_cidp_sql]

    # [START howto_sensor_cidp_sql_statements]
    compact = CidpSQLStatementsSensor(
        task_id="compact_orders",
        statement="ALTER TABLE lake.raw.orders EXECUTE optimize",
        timeout=1800,
        deferrable=True,
    )
    # [END howto_sensor_cidp_sql_statements]

    # [START howto_operator_cidp_quality]
    quality = CidpQualityRunOperator(
        task_id="check_orders",
        catalog="lake",
        schema="raw",
        table="orders",
        min_score=0.95,
    )
    # [END howto_operator_cidp_quality]

    # [START howto_operator_cidp_project_delete]
    cleanup = CidpProjectDeleteOperator(task_id="delete_sandbox", project="ledger-etl-sandbox", ignore_missing=True)
    # [END howto_operator_cidp_project_delete]

    create >> pull >> loaded >> compact >> quality >> cleanup
