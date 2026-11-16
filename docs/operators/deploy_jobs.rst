.. _howto/operator:CidpDeployJobsOperator:

CidpDeployJobsOperator
======================

``airflow.providers.cidp.operators.cidp.CidpDeployJobsOperator``

Deploys a project's jobs from its bundle (``databricks.yml`` or job files in the project) to a
target, and fails when the deployment reports errors. The counterpart of
``DatabricksCreateJobsOperator``, except that the job definitions come from the project, not from
JSON in the DAG.

Example
-------

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_deploy_jobs]
   :end-before: [END howto_operator_cidp_deploy_jobs]
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
   * - ``target``
     - ``None``
     - yes
     - The bundle target to deploy, such as ``prod``. Default: the server's choice.
   * - ``branch``
     - ``None``
     - yes
     - The branch to validate. Used only with ``validate_only=True``; a deploy uses the project's deployed branch.
   * - ``validate_only``
     - ``False``
     - no
     - Only validate the deployment; nothing changes.
   * - ``fail_on_warnings``
     - ``False``
     - no
     - Fail when the deployment reports warnings, not only errors.
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

* ``validate_only=False`` deploys ``target``. ``branch`` is not sent; the server deploys the
  project's deployed branch.
* ``validate_only=True`` validates ``target`` on ``branch`` and changes nothing; use it in a CI
  DAG or before a deploy.
* Errors fail the task. Warnings are logged, and fail the task with ``fail_on_warnings=True``.
* Returns the server's result: ``deployed``, ``changed``, ``undeployed``, ``errors``,
  ``warnings``...
