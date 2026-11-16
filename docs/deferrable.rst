Deferrable mode and triggers
============================

``CidpRunNowOperator``, ``CidpProjectActionOperator``, ``CidpSQLStatementsOperator`` and
``CidpSQLStatementsSensor`` can wait in the triggerer instead of in a worker slot. Turn it on per
task with ``deferrable=True``, or for every task with:

.. code-block:: ini

   [operators]
   default_deferrable = True

``deferrable=None`` (the default) reads that setting when the operator is created. On Amazon MWAA,
set ``operators.default_deferrable`` as an Airflow configuration option; MWAA runs a triggerer.

The operator starts the work in the worker, defers only if it has not ended yet, and finishes in
``execute_complete`` in a worker. Repair, failure messages and XCom are the same as without
deferral.

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Task
     - Trigger
     - Deadline
   * - ``CidpRunNowOperator``, ``CidpProjectActionOperator``
     - ``CidpExecutionTrigger``
     - ``execution_timeout`` (the deferral timeout). None: no deadline.
   * - ``CidpSQLStatementsOperator``
     - ``CidpSQLStatementExecutionTrigger``
     - ``timeout``; the statement is cancelled then.
   * - ``CidpSQLStatementsSensor``
     - ``CidpSQLStatementExecutionTrigger``
     - The sensor's ``timeout``; the statement is cancelled then.

Cancelling deferred work
------------------------

``on_kill`` does not run for a deferred task. Instead, when the triggerer cancels a trigger, the
trigger reads the task's state (through the Airflow 3 task SDK, or the database on Airflow 2). When
the task has left ``deferred`` (marked failed or success, cleared) it cancels the run or statement.

When the task is still ``deferred``, such as after a triggerer restart or when triggers move to
another triggerer, the work keeps running and the next triggerer picks up the wait. When the state
cannot be read, the work is left running too. This path is unit-tested only; it has not been run in
a live triggerer.

.. _howto/trigger:CidpExecutionTrigger:

CidpExecutionTrigger
--------------------

``airflow.providers.cidp.triggers.cidp.CidpExecutionTrigger``

Polls a job run (``job`` set) or a project run (``job`` unset) until it ends, then fires one event:
``run_id``, ``project``, ``job``, ``status``, ``message``, ``run_page_url``, ``repair_of`` and
``repair_run``.

.. list-table::
   :header-rows: 1
   :widths: 24 18 58

   * - Parameter
     - Default
     - Description
   * - ``run_id``
     - required
     - The run to wait for.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - The Airflow connection to use.
   * - ``project``
     - required
     - The project slug.
   * - ``job``
     - ``None``
     - The job's key for a job run; unset for a project run.
   * - ``workspace``
     - ``None``
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``polling_period_seconds``
     - ``30``
     - Seconds between state checks while waiting, in the worker or the triggerer.
   * - ``retry_limit``
     - ``3``
     - Attempts per API call.
   * - ``retry_delay``
     - ``10``
     - Base of the back-off between attempts, in seconds.
   * - ``run_page_url``
     - ``None``
     - Passed through to the event.
   * - ``repair_run``
     - ``False``
     - Passed through to the event, so the operator can repair.

``polling_period_seconds`` is the sleep between reads. ``run_id`` is a string, where Databricks
uses an integer.

.. _howto/trigger:CidpSQLStatementExecutionTrigger:

CidpSQLStatementExecutionTrigger
--------------------------------

``airflow.providers.cidp.triggers.cidp.CidpSQLStatementExecutionTrigger``

Long-polls a statement until it ends or ``end_time`` passes. The event holds ``statement_id``,
``state`` and ``error`` (``error_code``, ``error_message``). At ``end_time`` it cancels the
statement and fires ``state: TIMEOUT``.

.. list-table::
   :header-rows: 1
   :widths: 24 18 58

   * - Parameter
     - Default
     - Description
   * - ``statement_id``
     - required
     - The statement to wait for.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - The Airflow connection to use.
   * - ``end_time``
     - required
     - Epoch seconds. At this time the statement is cancelled and the event is ``TIMEOUT``.
   * - ``workspace``
     - ``None``
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``polling_period_seconds``
     - ``30``
     - Long-poll window per request, in seconds (at most 30).
   * - ``retry_limit``
     - ``3``
     - Attempts per API call.
   * - ``retry_delay``
     - ``10``
     - Base of the back-off between attempts, in seconds.

Each read blocks on the server for up to ``polling_period_seconds`` (at most 30 s) or until the
statement ends, so a statement's end is seen at once.
