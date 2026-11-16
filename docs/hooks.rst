Hook
====

.. _howto/hook:CidpHook:

CidpHook
--------

``airflow.providers.cidp.hooks.cidp.CidpHook`` is what the operators use, and covers more of the
API than they do. Use it in a ``@task`` for anything without an operator. It subclasses
``CidpBaseHook`` (``hooks.cidp_base``), which does the transport and sign-in.

.. list-table::
   :header-rows: 1
   :widths: 24 18 58

   * - Parameter
     - Default
     - Description
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - The Airflow connection to use.
   * - ``workspace``
     - ``None``
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``timeout_seconds``
     - ``180``
     - HTTP timeout per request, in seconds.
   * - ``retry_limit``
     - ``3``
     - Attempts per API call, the first one included.
   * - ``retry_delay``
     - ``1.0``
     - Base of the exponential back-off, in seconds (at most 60 s).

.. code-block:: python

   from airflow.decorators import task
   from airflow.providers.cidp.hooks.cidp import CidpHook


   @task
   def latest_runs():
       hook = CidpHook("cidp_default")
       return [r["run_id"] for r in hook.list_runs("ledger-etl", "nightly-ledger", limit=5)]

Methods
-------

Methods with an ``a_`` twin also exist as ``async`` for triggers.

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Area
     - Methods
   * - Jobs
     - ``list_jobs``, ``get_job``, ``set_job_enabled``, ``run_now``, ``list_runs``, ``get_run`` /
       ``a_get_run``, ``get_run_state`` / ``a_get_run_state``, ``cancel_run`` / ``a_cancel_run``,
       ``repair_run``, ``get_run_page_url``
   * - Project actions
     - ``list_actions``, ``run_project_action``, ``get_project_run`` / ``a_get_project_run``,
       ``get_project_run_state`` / ``a_get_project_run_state``, ``cancel_project_run`` /
       ``a_cancel_project_run``, ``get_project_page_url``
   * - Deployments
     - ``deploy``, ``validate_deployment``, ``destroy_deployment``
   * - Projects
     - ``create_project``, ``get_project``, ``pull_project``, ``delete_project``
   * - SQL statements
     - ``submit_statement``, ``get_statement`` / ``a_get_statement``, ``get_sql_statement_state`` /
       ``a_get_sql_statement_state``, ``cancel_sql_statement`` / ``a_cancel_sql_statement``,
       ``wait_for_statement``, ``get_statement_result_page``, ``iter_statement_rows``, ``run_sql``
   * - Tables and quality
     - ``get_table``, ``run_quality`` (preview)
   * - Volumes
     - ``upload_file(catalog, schema, volume, path, data, overwrite=False)``,
       ``download_file(catalog, schema, volume, path)``
   * - Connection
     - ``test_connection``, ``get_ui_field_behaviour``

``run_sql`` submits a statement, waits (cancelling it after ``timeout``), and returns a
``StatementResult`` with ``columns``, ``column_names``, ``rows`` and ``truncated``. It fetches every
result page.

Reading the rows of an earlier ``CidpSQLStatementsOperator``:

.. code-block:: python

   @task
   def rows(statement_id: str):
       hook = CidpHook()
       info = hook.get_statement(statement_id)
       return list(hook.iter_statement_rows(info))


   rows(optimize.output)

A result is kept on the server for a while; a statement whose result is gone is ``CLOSED``.

Volumes:

.. code-block:: python

   @task
   def publish_report(path: str):
       hook = CidpHook()
       with open(path, "rb") as fh:
           hook.upload_file("lake", "reports", "daily", "ledger/latest.csv", fh.read(), overwrite=True)

State classes
-------------

``RunState(status, message)``
    ``is_terminal`` for ``succeeded``, ``failed``, ``cancelled``, ``skipped``, ``expired``;
    ``is_successful`` for ``succeeded``. A state the provider does not know counts as not ended.
    Databricks' ``RunState`` has ``life_cycle_state`` and ``result_state`` instead.

``SQLStatementState(state, error_code, error_message)``
    The Databricks states: ``PENDING``, ``RUNNING``, ``SUCCEEDED``, ``FAILED``, ``CANCELED``,
    ``CLOSED``. ``is_terminal``, ``is_running``, ``is_successful``.

Exceptions
----------

All in ``airflow.providers.cidp.exceptions``. ``CidpApiError`` subclasses ``AirflowException``
and carries ``status_code``, the API's error ``code`` and ``request_id`` (the ``x-request-id``);
the others subclass it.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Exception
     - Raised for
   * - ``CidpAuthError``
     - ``401``/``403``, a refused sign-in, a missing host, password or client id, a non-https
       issuer or token endpoint.
   * - ``CidpNotFoundError``
     - ``404``.
   * - ``CidpConflictError``
     - ``409``: a taken project slug, or an ``Idempotency-Key`` reused for another request.
   * - ``CidpUnavailableError``
     - ``503`` on a request that is not retried (a ``POST`` without an ``Idempotency-Key``).
   * - ``CidpApiError``
     - Any other error answer, and a retried error (connect error, ``429``, ``5xx``) after the
       last attempt.
