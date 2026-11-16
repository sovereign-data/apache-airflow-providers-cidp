.. _howto/operator:CidpRunNowOperator:

CidpRunNowOperator
==================

``airflow.providers.cidp.operators.cidp.CidpRunNowOperator``

Runs a job that is already deployed in a project, waits for it to end and fails the task unless it
ends ``succeeded``. The counterpart of ``DatabricksRunNowOperator``.

Jobs are defined in the project (its bundle), not in the DAG. Deploy them with
:ref:`CidpDeployJobsOperator <howto/operator:CidpDeployJobsOperator>` or from the CIDP UI.

The run executes as the job's identity (its ``run_as``), not as the connection. The connection only
needs permission to run the job.

Example
-------

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_run_now]
   :end-before: [END howto_operator_cidp_run_now]
   :dedent: 4

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``project``
     - required
     - yes
     - The project slug that holds the job.
   * - ``job``
     - required
     - yes
     - The job's key, as defined in the project's bundle.
   * - ``job_parameters``
     - ``None``
     - yes
     - Values for the job's declared parameters. Every value is sent as a string.
   * - ``idempotency_token``
     - ``None``
     - yes
     - The ``Idempotency-Key`` of the run start. Default: one per task try, so a repeated request answers the same run and an Airflow retry starts a new one.
   * - ``repair_run``
     - ``False``
     - no
     - When the run ends ``failed``, repair it once: rerun the failed tasks and their dependants, then wait for the repair.
   * - ``polling_period_seconds``
     - ``30``
     - no
     - Seconds between state checks while waiting, in the worker or the triggerer.
   * - ``wait_for_termination``
     - ``True``
     - no
     - Wait for the run to end. ``False`` returns once the run has started.
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

Behaviour
---------

* The run start sends an ``Idempotency-Key``: by default a hash of dag, task, run, map index, try
  number, workspace, project and job. Repeating the request within one try answers the same run; an
  Airflow retry starts a new one. ``idempotency_token`` replaces it.
* ``run_id`` and ``run_page_url`` go to XCom as soon as the run starts; the **See CIDP Run** link
  reads ``run_page_url``.
* A run ends ``succeeded``, ``failed``, ``cancelled``, ``skipped`` or ``expired``. Anything other
  than ``succeeded`` fails the task, with the run's message.
* ``repair_run=True``: a run that ends ``failed`` is repaired once per task try, and the operator
  waits for the repair. A run that is itself a repair is not repaired again. ``cancelled``,
  ``skipped`` and ``expired`` are not repaired.
* ``deferrable=True``: the wait moves to the triggerer
  (:ref:`CidpExecutionTrigger <howto/trigger:CidpExecutionTrigger>`), with ``execution_timeout`` as
  the deferral timeout. Without ``execution_timeout`` a deferred wait on a run that never ends
  never ends either; set one.
* ``on_kill`` (task marked failed, timed out in the worker) cancels the run. For a deferred task see
  :doc:`../deferrable`.
