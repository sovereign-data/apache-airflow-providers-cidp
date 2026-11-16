"""Run a deployed job, a dbt build and SQL against CIDP, the Cloud Independent Data Platform.

Connection ``cidp_default``: host = the CIDP URL, login/password = a project service
principal's client id/secret, extra = {"auth_type": "client_credentials", "workspace": "analytics"}.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cidp.operators.cidp import (
    CidpDeployJobsOperator,
    CidpProjectActionOperator,
    CidpRunNowOperator,
    CidpSQLStatementsOperator,
)
from airflow.providers.cidp.operators.cidp_sql import CidpSqlOperator
from airflow.providers.cidp.sensors.cidp_sql import CidpPartitionSensor

with DAG(
    dag_id="example_cidp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "cidp"],
) as dag:
    # [START howto_sensor_cidp_partition]
    landed = CidpPartitionSensor(
        task_id="wait_for_orders",
        catalog="lake",
        schema="raw",
        table_name="orders",
        partitions={"order_date": "{{ ds }}"},
        poke_interval=300,
        timeout=6 * 3600,
        mode="reschedule",
    )
    # [END howto_sensor_cidp_partition]

    # [START howto_operator_cidp_deploy_jobs]
    deploy = CidpDeployJobsOperator(task_id="deploy_jobs", project="ledger-etl", target="prod")
    # [END howto_operator_cidp_deploy_jobs]

    # [START howto_operator_cidp_run_now]
    nightly = CidpRunNowOperator(
        task_id="run_nightly",
        project="ledger-etl",
        job="nightly-ledger",
        job_parameters={"run_date": "{{ ds }}"},
        repair_run=True,
        deferrable=True,
        execution_timeout=timedelta(hours=2),
    )
    # [END howto_operator_cidp_run_now]

    # [START howto_operator_cidp_project_action]
    dbt = CidpProjectActionOperator(
        task_id="dbt_build_marts",
        project="ledger-etl",
        action="dbt.build",
        path="models/marts",
        selection="tag:daily",
        deferrable=True,
    )
    # [END howto_operator_cidp_project_action]

    # [START howto_operator_cidp_sql_statements]
    optimize = CidpSQLStatementsOperator(
        task_id="optimize",
        statement="ALTER TABLE lake.marts.ledger EXECUTE optimize",
        timeout=1800,
        deferrable=True,
    )
    # [END howto_operator_cidp_sql_statements]

    # [START howto_operator_cidp_sql]
    counts = CidpSqlOperator(
        task_id="row_count",
        sql="SELECT count(*) FROM lake.marts.ledger WHERE ledger_date = DATE '{{ ds }}'",
    )
    # [END howto_operator_cidp_sql]

    landed >> deploy >> nightly >> dbt >> optimize >> counts
