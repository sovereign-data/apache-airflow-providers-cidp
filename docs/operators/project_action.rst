.. _howto/operator:CidpProjectActionOperator:

CidpProjectActionOperator
=========================

``airflow.providers.cidp.operators.cidp.CidpProjectActionOperator``

Runs an action on a file or folder of a project, on a branch's files as they are now: ``dbt.build``,
``python.run``, ``notebook.run_all``, ``sql.run``, ``sql.explain``... It is the one-off
counterpart of a job, like ``DatabricksSubmitRunOperator`` or ``DatabricksNotebookOperator``, but
nothing is defined in the DAG: the action and its file live in the project.

It runs as the **connection's** identity, so the connection needs access to the data the action
touches. ``CidpHook.list_actions(project, branch=..., path=...)`` lists the actions a file offers
and their parameters.

Example
-------

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_project_action]
   :end-before: [END howto_operator_cidp_project_action]
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
     - The project slug.
   * - ``action``
     - required
     - yes
     - The action: ``dbt.build``, ``dbt.run``, ``python.run``, ``notebook.run_all``, ``sql.run``... ``CidpHook.list_actions`` lists what a file offers.
   * - ``path``
     - required
     - yes
     - The file or folder in the project the action runs on.
   * - ``branch``
     - ``'main'``
     - yes
     - The branch whose files are used, as they are now.
   * - ``selection``
     - ``None``
     - yes
     - A selection for the action, such as a dbt selector (``tag:daily``).
   * - ``namespace``
     - ``None``
     - yes
     - The namespace, for actions that take one.
   * - ``action_params``
     - ``None``
     - yes
     - The action's parameters. Named so because ``params`` belongs to ``BaseOperator``.
   * - ``catalog``
     - ``None``
     - yes
     - The catalog the action runs against.
   * - ``polling_period_seconds``
     - ``30``
     - no
     - Seconds between state checks while waiting, in the worker or the triggerer.
   * - ``wait_for_termination``
     - ``True``
     - no
     - Wait for the run to end. ``False`` returns once the run has started. SQL actions always answer at once.
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

* **Actions that run** (dbt, Python, notebooks) start a project run. The operator pushes ``run_id``
  and ``run_page_url``, waits like ``CidpRunNowOperator`` (polling or deferred) and returns the run
  id. The link goes to the project's page, since a project run has no page of its own.
  ``on_kill`` cancels the run.
* **SQL actions** (``sql.run``, ``sql.explain``) answer at once. The operator returns
  ``{"action", "query", "plan"}`` and fails when ``query.error`` is set. Nothing is pushed besides
  the return value, and the wait parameters do not apply.
* There is no repair and no idempotency key: a retry runs the action again.
