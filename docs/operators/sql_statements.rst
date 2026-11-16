SQL statements
==============

The statements API runs one statement asynchronously on an engine (``sqe`` by default, or
``spark``) and keeps its result for a while. These two classes submit a statement and wait for it,
like ``DatabricksSQLStatementsOperator`` and ``DatabricksSQLStatementsSensor``. Neither needs a
warehouse id: the engine is a per-statement choice.

Statements run as the **connection's** identity.

.. _howto/operator:CidpSQLStatementsOperator:

CidpSQLStatementsOperator
-------------------------

``airflow.providers.cidp.operators.cidp.CidpSQLStatementsOperator``

Submits one statement, waits for it and returns its ``statement_id``. Use it for DDL, DML and
maintenance (``OPTIMIZE``, ``MERGE``, ``CREATE TABLE AS``) where you do not need the rows. To get
rows, use :ref:`CidpSqlOperator <howto/operator:CidpSqlOperator>`, or read them in a later task
with ``CidpHook.iter_statement_rows``.

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_sql_statements]
   :end-before: [END howto_operator_cidp_sql_statements]
   :dedent: 4

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``statement``
     - required
     - yes
     - The SQL: one statement. A path ending in ``.sql`` is read as a template.
   * - ``engine``
     - ``None``
     - no
     - ``sqe`` or ``spark``. Default: the server's default engine (``sqe``).
   * - ``catalog``
     - ``None``
     - yes
     - The default catalog for unqualified names.
   * - ``schema``
     - ``None``
     - yes
     - The default schema for unqualified names.
   * - ``row_limit``
     - ``None``
     - no
     - The most rows the statement keeps in its result.
   * - ``query_tag``
     - ``None``
     - yes
     - A tag kept with the statement. Default: ``airflow:<dag_id>.<task_id>``. No personal data.
   * - ``include_airflow_query_tags``
     - ``True``
     - no
     - Send ``airflow:<dag_id>.<task_id>`` when ``query_tag`` is not set.
   * - ``polling_period_seconds``
     - ``30``
     - no
     - Seconds between state checks while waiting, in the worker or the triggerer.
   * - ``wait_for_termination``
     - ``True``
     - no
     - Wait for the statement to end. ``False`` returns once it is submitted.
   * - ``timeout``
     - ``3600``
     - no
     - Seconds before the statement is cancelled and the task fails.
   * - ``deferrable``
     - ``None``
     - no
     - Wait in the triggerer instead of holding a worker slot. ``None`` takes ``[operators] default_deferrable``.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - yes
     - The Airflow connection to use.
   * - ``workspace``
     - ``None``
     - yes
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``cidp_retry_limit``
     - ``3``
     - no
     - Attempts per API call, the first one included. Connect errors, ``429`` and a ``5xx`` on a request that is safe to repeat are retried.
   * - ``cidp_retry_delay``
     - ``1``
     - no
     - Base of the exponential back-off between attempts, in seconds (at most 60 s).

``statement`` is the first positional argument, as on Databricks. It is rendered with Jinja; a
value ending in ``.sql`` is read from the DAG folder.

Behaviour:

* The submit sends an ``Idempotency-Key`` per task try, so a repeated submit within one try
  answers the same statement.
* ``statement_id`` goes to XCom as soon as the statement is submitted.
* The worker long-polls (up to ``polling_period_seconds``, at most 30 s per request). A statement
  that has not ended after ``timeout`` seconds is cancelled and the task fails. Deferred, the
  trigger does the same at the same deadline.
* ``SUCCEEDED`` passes. ``FAILED``, ``CANCELED`` and ``CLOSED`` fail the task with the error code
  and message.
* ``on_kill`` cancels the statement.

.. _howto/sensor:CidpSQLStatementsSensor:

CidpSQLStatementsSensor
-----------------------

``airflow.providers.cidp.sensors.cidp.CidpSQLStatementsSensor``

Waits for a statement to end: one it submits (``statement``) or one that already exists
(``statement_id``, for example from another task's XCom). Fails when the statement ends other than
``SUCCEEDED``.

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_sensor_cidp_sql_statements]
   :end-before: [END howto_sensor_cidp_sql_statements]
   :dedent: 4

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``statement``
     - ``None``
     - yes
     - SQL to submit and wait for. Pass this or ``statement_id``.
   * - ``statement_id``
     - ``None``
     - yes
     - An existing statement to wait for. Pass this or ``statement``.
   * - ``engine``
     - ``None``
     - no
     - ``sqe`` or ``spark``. Default: the server's default engine (``sqe``).
   * - ``catalog``
     - ``None``
     - yes
     - The default catalog for unqualified names.
   * - ``schema``
     - ``None``
     - yes
     - The default schema for unqualified names.
   * - ``query_tag``
     - ``None``
     - yes
     - A tag kept with the statement. Default: ``airflow:<dag_id>.<task_id>``. No personal data.
   * - ``include_airflow_query_tags``
     - ``True``
     - no
     - Send ``airflow:<dag_id>.<task_id>`` when ``query_tag`` is not set.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - yes
     - The Airflow connection to use.
   * - ``workspace``
     - ``None``
     - yes
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``polling_period_seconds``
     - ``30``
     - no
     - Seconds between state checks while waiting, in the worker or the triggerer.
   * - ``cidp_retry_limit``
     - ``3``
     - no
     - Attempts per API call, the first one included. Connect errors, ``429`` and a ``5xx`` on a request that is safe to repeat are retried.
   * - ``cidp_retry_delay``
     - ``1``
     - no
     - Base of the exponential back-off between attempts, in seconds (at most 60 s).
   * - ``deferrable``
     - ``None``
     - no
     - Wait in the triggerer instead of holding a worker slot. ``None`` takes ``[operators] default_deferrable``.

Pass exactly one of ``statement`` and ``statement_id``. The sensor also takes
``BaseSensorOperator``'s parameters: ``poke_interval``, ``timeout``, ``mode`` and so on.

Behaviour:

* With ``statement`` the sensor submits once per task try (with an ``Idempotency-Key``, so a
  reschedule finds the same statement) and pushes ``statement_id``.
* Each poke reads the statement's state.
* ``deferrable=True``: after a first poke, the wait moves to
  :ref:`CidpSQLStatementExecutionTrigger <howto/trigger:CidpSQLStatementExecutionTrigger>`, which
  cancels the statement when the sensor's ``timeout`` passes.
* Not deferred, a sensor timeout fails the task but leaves the statement running.
* ``on_kill`` cancels the statement only when this sensor submitted it, never one it was given by
  ``statement_id``.
