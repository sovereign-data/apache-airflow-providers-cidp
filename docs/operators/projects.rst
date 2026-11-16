Projects
========

A CIDP project is a git-backed folder of code (dbt, Python, notebooks, SQL) and job definitions,
like a Databricks Git folder (Repo). These operators correspond to ``DatabricksReposCreateOperator``,
``DatabricksReposUpdateOperator`` and ``DatabricksReposDeleteOperator``. They run as the
connection's identity.

.. _howto/operator:CidpProjectCreateOperator:

CidpProjectCreateOperator
-------------------------

``airflow.providers.cidp.operators.cidp_projects.CidpProjectCreateOperator``

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_operator_cidp_project_create]
   :end-before: [END howto_operator_cidp_project_create]
   :dedent: 4

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``slug``
     - required
     - yes
     - The new project's slug.
   * - ``name``
     - ``None``
     - yes
     - The display name.
   * - ``description``
     - ``None``
     - yes
     - A description.
   * - ``git_url``
     - ``None``
     - yes
     - Create the project from this git repository.
   * - ``branch``
     - ``None``
     - yes
     - The branch to check out from ``git_url``.
   * - ``template``
     - ``None``
     - yes
     - A project template to start from.
   * - ``owner_group``
     - ``None``
     - yes
     - The group that owns the project.
   * - ``ignore_existing_project``
     - ``False``
     - no
     - When the slug is taken, return the existing project instead of failing.
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

Sends an ``Idempotency-Key`` per task try. Returns the created project. With
``ignore_existing_project=True``, a taken slug returns the existing project instead of failing; it
is not compared with the arguments.

.. _howto/operator:CidpProjectUpdateOperator:

CidpProjectUpdateOperator
-------------------------

``airflow.providers.cidp.operators.cidp_projects.CidpProjectUpdateOperator``

Pulls a branch of the project from its git remote. Returns the project.

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_operator_cidp_project_update]
   :end-before: [END howto_operator_cidp_project_update]
   :dedent: 4

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
   * - ``branch``
     - ``'main'``
     - yes
     - The branch to pull from the project's git remote.
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

Databricks' ``tag``, ``repo_id`` and ``repo_path`` have no counterpart: a project is named by its
slug, and only branches are pulled.

.. _howto/operator:CidpProjectDeleteOperator:

CidpProjectDeleteOperator
-------------------------

``airflow.providers.cidp.operators.cidp_projects.CidpProjectDeleteOperator``

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_operator_cidp_project_delete]
   :end-before: [END howto_operator_cidp_project_delete]
   :dedent: 4

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
   * - ``ignore_missing``
     - ``False``
     - no
     - Succeed when the project does not exist.
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
