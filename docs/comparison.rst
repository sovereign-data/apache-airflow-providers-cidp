Compared with the Databricks and Amazon providers
=================================================

This provider follows ``apache-airflow-providers-databricks`` closely, so a DAG written for
Databricks moves over with few changes. This page lists what is the same, what differs, and how
the Amazon provider covers the same ground.

Compared versions: **Databricks 7.7.1** and **Amazon 9.12.0**, the versions in the Airflow 3.0.6
constraints that Amazon MWAA installs. Newer Databricks releases add parameters not covered here
(``query_tags`` as a dict, ``performance_target``, ``durable``...).

Operator mapping
----------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - CIDP
     - Databricks
     - Amazon
   * - ``CidpRunNowOperator``
     - ``DatabricksRunNowOperator``
     - ``GlueJobOperator`` (an existing job), ``EmrServerlessStartJobOperator``
   * - ``CidpProjectActionOperator``
     - ``DatabricksSubmitRunOperator``, ``DatabricksNotebookOperator``
     - ``GlueJobOperator`` with a ``script_location``
   * - ``CidpDeployJobsOperator``
     - ``DatabricksCreateJobsOperator``
     - ``GlueJobOperator`` with ``update_config=True`` / ``create_job_kwargs``
   * - ``CidpSQLStatementsOperator``
     - ``DatabricksSQLStatementsOperator``
     - ``AthenaOperator``, ``RedshiftDataOperator``
   * - ``CidpSqlOperator``
     - ``DatabricksSqlOperator``
     - ``RedshiftDataOperator(return_sql_result=True)``; Athena writes results to S3
   * - ``CidpSQLStatementsSensor``
     - ``DatabricksSQLStatementsSensor``
     - ``AthenaSensor``
   * - ``CidpSqlSensor``
     - ``DatabricksSqlSensor``
     - ``SqlSensor`` (common.sql) on an Athena or Redshift connection
   * - ``CidpPartitionSensor``
     - ``DatabricksPartitionSensor``
     - ``GlueCatalogPartitionSensor``
   * - ``CidpProjectCreateOperator`` / ``Update`` / ``Delete``
     - ``DatabricksReposCreateOperator`` / ``Update`` / ``Delete``
     - none
   * - ``CidpQualityRunOperator`` (preview)
     - none
     - ``GlueDataQualityRuleSetEvaluationRunOperator``
   * - ``CidpHook.upload_file`` / ``download_file`` (volumes)
     - none in the operators
     - ``S3Hook``
   * - ``CidpExecutionTrigger``, ``CidpSQLStatementExecutionTrigger``
     - ``DatabricksExecutionTrigger``, ``DatabricksSQLStatementExecutionTrigger``
     - one trigger per service (``GlueJobCompleteTrigger``, ``AthenaTrigger``...)
   * - ``CidpJobRunLink`` (**See CIDP Run**)
     - ``DatabricksJobRunLink`` (**See Databricks Job Run**)
     - console links per service

Not provided: Databricks' ``DatabricksCopyIntoOperator``, ``DatabricksTaskOperator`` and the
``DatabricksWorkflowTaskGroup``. CIDP has no clusters or warehouses, so nothing takes
``new_cluster``, ``existing_cluster_id`` or ``warehouse_id``; ``COPY INTO``-style loads run as SQL
or as a project action.

Connection and authentication
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 18 27 27 28

   * -
     - CIDP
     - Databricks
     - Amazon
   * - Connection type
     - ``cidp``
     - ``databricks``
     - ``aws``
   * - Parameter, default
     - ``cidp_conn_id='cidp_default'``
     - ``databricks_conn_id='databricks_default'``
     - ``aws_conn_id='aws_default'``
   * - Sign-in
     - A bearer token, or OAuth2 ``client_credentials`` of a project service principal, with the
       token endpoint found by OIDC discovery.
     - A personal access token (password or ``token`` extra), Databricks OAuth for service
       principals (``service_principal_oauth``), Azure AD service principals or managed identity
       (``use_azure_managed_identity``, ``azure_tenant_id``, ``azure_resource_id``).
     - The boto3 credential chain: keys, profiles, an assumed role (``role_arn``), the
       environment's IAM role.
   * - Scope
     - A ``workspace`` (extra, or per task).
     - The workspace is the host.
     - ``region_name`` (extra, or per task).
   * - TLS
     - ``ca_cert``, ``verify_ssl``; https enforced unless ``allow_insecure_http``.
     - System CAs.
     - ``verify`` per task, ``botocore_config``.
   * - Connection test
     - Lists one project of the workspace.
     - Yes.
     - Yes (STS ``GetCallerIdentity``).

Whose identity runs the work
----------------------------

* **CIDP**: a job run executes as the job's ``run_as``; the connection only needs to be allowed to
  run it. Project actions, SQL, project changes and quality runs execute as the connection. The
  data service's own account (``portal-data-svc``) cannot be used.
* **Databricks**: a job run executes as the job's ``run_as`` too; ``DatabricksSubmitRunOperator``
  and SQL run as the connection.
* **Amazon**: Glue and EMR jobs run as their IAM role (``iam_role_name``/``iam_role_arn``,
  ``execution_role_arn``); Athena and the Redshift Data API run as the connection's credentials
  (or ``db_user`` / ``secret_arn`` on Redshift).

Parameter differences
---------------------

Run a job
~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - CIDP ``CidpRunNowOperator``
     - Databricks ``DatabricksRunNowOperator``
     - Note
   * - ``project`` + ``job`` (the job's key)
     - ``job_id`` or ``job_name``
     - A job is named by its project and key, which stay stable across redeploys.
   * - ``job_parameters`` (values sent as strings)
     - ``job_parameters``, ``notebook_params``, ``python_params``, ``python_named_params``,
       ``jar_params``, ``spark_submit_params``, ``dbt_commands``
     - CIDP jobs declare parameters; there are no per-task-type overrides.
   * - none
     - ``json``
     - No raw request body.
   * - ``idempotency_token``, default **one per task try**
     - ``idempotency_token``, default none
     - CIDP always sends a key; see `Idempotency`_.
   * - ``repair_run`` (once per try, ``failed`` runs only)
     - ``repair_run``, ``databricks_repair_reason_new_settings``
     - No settings change on repair.
   * - none
     - ``cancel_previous_runs``
     - Cancel earlier runs with ``CidpHook.list_runs`` and ``cancel_run``.
   * - ``cidp_retry_limit``, ``cidp_retry_delay``
     - ``databricks_retry_limit``, ``databricks_retry_delay``, ``databricks_retry_args``
     - No tenacity override.
   * - ``workspace``
     - none
     - A connection can serve several workspaces.

Run SQL
~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - CIDP (``CidpSQLStatementsOperator`` / ``CidpSqlOperator``)
     - Databricks (``DatabricksSQLStatementsOperator`` / ``DatabricksSqlOperator``)
     - Amazon (``AthenaOperator`` / ``RedshiftDataOperator``)
   * - ``engine`` (``sqe`` or ``spark``), optional
     - ``warehouse_id`` (statements); ``http_path`` or ``sql_endpoint_name`` (SQL operator)
     - ``workgroup``; ``cluster_identifier`` or ``workgroup_name``
   * - ``catalog``, ``schema``
     - ``catalog``, ``schema``
     - ``catalog``, ``database``; ``database``
   * - none (use Jinja; see :doc:`security`)
     - ``parameters`` (bind parameters)
     - ``parameters`` (Redshift)
   * - ``query_tag`` (one string), ``include_airflow_query_tags``
     - none in 7.7.1; ``query_tags`` dict in later releases
     - none
   * - ``row_limit``
     - none
     - none
   * - ``timeout`` (cancel after; both), ``polling_period_seconds`` (statements)
     - ``timeout``, ``polling_period_seconds`` (statements only)
     - ``max_polling_attempts`` and ``sleep_time``; ``poll_interval``
   * - ``output_path``, ``output_format`` (csv, json, jsonl), ``csv_params`` (SQL operator)
     - ``output_path``, ``output_format`` (csv, json, jsonl), ``csv_params`` (SQL operator)
     - ``output_location`` (S3, Athena)
   * - ``split_statements``, ``return_last``
     - ``split_statements``, ``return_last``
     - ``sql`` list (Redshift)
   * - none
     - ``session_configuration``, ``http_headers``, ``client_parameters``
     - ``result_configuration``, ``query_execution_context``

Projects (Databricks Repos)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A project is named by its ``slug``; there is no ``repo_id`` or ``repo_path``.
* ``CidpProjectCreateOperator`` takes ``git_url`` and ``branch``, plus ``name``, ``description``,
  ``template`` and ``owner_group``. There is no ``git_provider`` (it is read from the URL) and no
  ``tag``.
* ``ignore_existing_project`` matches Databricks' ``ignore_existing_repo``.
* ``CidpProjectUpdateOperator`` pulls a branch; it cannot check out a tag.

Waiting and deferral
--------------------

* Polling: ``polling_period_seconds`` (default 30), as on Databricks. Amazon names it per service:
  ``sleep_time``, ``poll_interval``, ``waiter_delay`` with ``waiter_max_attempts``,
  ``job_poll_interval``.
* ``wait_for_termination`` on the run and statement operators, as on Databricks; Amazon uses
  ``wait_for_completion``.
* ``deferrable``: all three default to ``[operators] default_deferrable``. CIDP reads it when the
  operator is created (``deferrable=None``); Databricks and Amazon read it when their module is
  imported.
* A deferred run wait is bounded by ``execution_timeout``; a deferred statement wait by
  ``timeout``.
* ``CidpSQLStatementsOperator`` and every deferred statement wait long-poll the server (up to
  30 s per request), so the end of a statement is seen at once rather than at the next poll. A
  non-deferred ``CidpSQLStatementsSensor`` checks once per ``poke_interval``, like Databricks.

Run and statement states
------------------------

* Runs: ``succeeded``, ``failed``, ``cancelled``, ``skipped``, ``expired`` (lower case, one field).
  Databricks splits ``life_cycle_state`` and ``result_state``. An unknown state counts as not ended
  instead of raising.
* Statements: the Databricks states (``PENDING``, ``RUNNING``, ``SUCCEEDED``, ``FAILED``,
  ``CANCELED``, ``CLOSED``). ``CLOSED`` fails the task, as on Databricks.
* Run ids are strings; Databricks uses integers.

Idempotency
-----------

CIDP sends an ``Idempotency-Key`` on run starts, repairs, project creation and the statement
submits of ``CidpSQLStatementsOperator`` and ``CidpSQLStatementsSensor``. The key is derived from
the dag, task, run, map index and try number, so a request repeated within one try (a retried
``5xx``, a reschedule) returns the same run or statement, and an Airflow retry starts a new one.
Databricks sends a token only when you pass ``idempotency_token``; Amazon only when you pass
``client_request_token`` (Athena, EMR Serverless).

XCom and links
--------------

* ``run_id`` and ``run_page_url`` (run operators) and ``statement_id`` (statement operator and
  sensor): the same keys as Databricks.
* The **See CIDP Run** link reads ``run_page_url``, like ``DatabricksJobRunLink``. For a project
  action it opens the project page.

Not supported yet
-----------------

* OpenLineage facets.
* Bind parameters in SQL.
* ``DatabricksWorkflowTaskGroup``-style task groups that build one job from several Airflow tasks.
