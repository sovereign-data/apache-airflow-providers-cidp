Operators and sensors
=====================

.. toctree::
   :maxdepth: 1

   run_now
   project_action
   deploy_jobs
   sql_statements
   sql
   sensors
   projects
   quality

.. list-table::
   :header-rows: 1
   :widths: 30 34 36

   * - Class
     - Module
     - Does
   * - :ref:`CidpRunNowOperator <howto/operator:CidpRunNowOperator>`
     - ``operators.cidp``
     - Run a deployed job and wait; optional repair.
   * - :ref:`CidpProjectActionOperator <howto/operator:CidpProjectActionOperator>`
     - ``operators.cidp``
     - Run an action (dbt, Python, notebook, SQL) on a project file.
   * - :ref:`CidpDeployJobsOperator <howto/operator:CidpDeployJobsOperator>`
     - ``operators.cidp``
     - Deploy or validate a project's jobs from its bundle.
   * - :ref:`CidpSQLStatementsOperator <howto/operator:CidpSQLStatementsOperator>`
     - ``operators.cidp``
     - Submit one SQL statement and wait; returns its id.
   * - :ref:`CidpSqlOperator <howto/operator:CidpSqlOperator>`
     - ``operators.cidp_sql``
     - Run statements and return or write their rows.
   * - :ref:`CidpProjectCreateOperator <howto/operator:CidpProjectCreateOperator>`
     - ``operators.cidp_projects``
     - Create a project, optionally from git.
   * - :ref:`CidpProjectUpdateOperator <howto/operator:CidpProjectUpdateOperator>`
     - ``operators.cidp_projects``
     - Pull a project's branch from git.
   * - :ref:`CidpProjectDeleteOperator <howto/operator:CidpProjectDeleteOperator>`
     - ``operators.cidp_projects``
     - Delete a project.
   * - :ref:`CidpQualityRunOperator <howto/operator:CidpQualityRunOperator>`
     - ``operators.cidp_quality``
     - Run a table's quality checks (preview API).
   * - :ref:`CidpSQLStatementsSensor <howto/sensor:CidpSQLStatementsSensor>`
     - ``sensors.cidp``
     - Wait for a statement to end.
   * - :ref:`CidpSqlSensor <howto/sensor:CidpSqlSensor>`
     - ``sensors.cidp_sql``
     - Wait until a query returns what a handler accepts.
   * - :ref:`CidpPartitionSensor <howto/sensor:CidpPartitionSensor>`
     - ``sensors.cidp_sql``
     - Wait until a table has rows for a partition.

Modules are under ``airflow.providers.cidp``.

Common parameters
-----------------

Every operator and sensor takes these, on top of ``BaseOperator``'s own (``task_id``, ``retries``,
``execution_timeout``...). Each page lists them again with the class's other parameters.

.. list-table::
   :header-rows: 1
   :widths: 22 18 60

   * - Parameter
     - Default
     - Description
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - The Airflow connection. Templated.
   * - ``workspace``
     - ``None``
     - The workspace slug; default the connection's ``workspace`` extra. Templated.
   * - ``cidp_retry_limit``
     - ``3``
     - Attempts per API call, the first one included. Not on ``CidpSqlSensor`` and
       ``CidpPartitionSensor``.
   * - ``cidp_retry_delay``
     - ``1``
     - Base of the exponential back-off, in seconds (at most 60 s). Not on ``CidpSqlSensor`` and
       ``CidpPartitionSensor``.

What is retried
    A connect error, ``429``, and a ``5xx`` on a request that is safe to repeat: ``GET``, ``PUT``,
    ``DELETE``, or a ``POST`` that carries an ``Idempotency-Key``. A ``5xx`` on a ``POST``
    without one is not repeated, because the server may already have acted on it. Redirects are
    never followed.

XCom and links
--------------

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Class
     - Pushes
   * - ``CidpRunNowOperator``
     - ``run_id``, ``run_page_url``; returns the run id. **See CIDP Run** link.
   * - ``CidpProjectActionOperator``
     - Runs: ``run_id``, ``run_page_url`` (the project page); returns the run id. SQL actions:
       returns ``{action, query, plan}``, no keys.
   * - ``CidpSQLStatementsOperator``, ``CidpSQLStatementsSensor``
     - ``statement_id``. The operator returns it.
   * - ``CidpSqlOperator``
     - Returns the rows (a list of lists), or ``None`` with ``output_path``.
   * - ``CidpDeployJobsOperator``
     - Returns the deployment result (``deployed``, ``changed``, ``undeployed``, ``errors``,
       ``warnings``...).
   * - ``CidpProjectCreateOperator``, ``CidpProjectUpdateOperator``
     - Returns the project.
   * - ``CidpQualityRunOperator``
     - Returns the quality run (``passed``, ``failed``, ``errored``, ``total``, ``score``...).

The return value goes to XCom as ``return_value`` unless ``do_xcom_push=False``.
