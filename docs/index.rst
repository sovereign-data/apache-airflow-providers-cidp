``airflow-provider-cidp``
=========================

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Basics

   Home <self>
   Changelog <changelog>

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Guides

   Connection types <connections/cidp>
   Operators and sensors <operators/index>
   Deferrable mode and triggers <deferrable>
   Hook <hooks>
   Compared with Databricks and Amazon <comparison>
   Security <security>
   Live testing <live-testing>

Package ``airflow-provider-cidp`` connects Apache Airflow to CIDP, the Cloud Independent Data
Platform, through its data API (``/api/data/v1``). It is shaped after
``apache-airflow-providers-databricks``: the same operator families, parameter names, XCom keys,
deferrable triggers and run link, adapted to CIDP's projects, jobs and SQL engines.

All classes live under ``airflow.providers.cidp``.

What it covers
--------------

* **Jobs**: run a deployed job (with repair), deploy a project's jobs from its bundle.
* **Project actions**: one-off ``dbt.build``, ``python.run``, ``notebook.run_all``, ``sql.run`` on a
  project file.
* **SQL**: submit a statement and wait, run statements and return or write their rows, wait for a
  statement, a query or a partition.
* **Projects**: create, pull and delete projects.
* **Quality** (preview API): run a table's quality checks.
* **Hook**: every call above, plus tables, volumes (upload and download files) and run listing.

Installation
------------

.. code-block:: bash

   pip install airflow-provider-cidp

On Amazon MWAA, add the package to ``requirements.txt`` together with the Airflow constraints
file, so pip keeps MWAA's own versions:

.. code-block:: text

   --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt"
   airflow-provider-cidp==0.1.0

The provider's own dependencies, ``httpx`` and ``tenacity``, are pinned in that constraints file
(``httpx==0.28.1``, ``tenacity==9.1.2``) and fit the provider's ranges. For an environment without
internet access, put the wheel in ``plugins.zip`` and install it from there.

Requirements
------------

.. list-table::
   :header-rows: 1

   * - Package
     - Version
   * - ``apache-airflow``
     - ``>=2.9.0``
   * - ``httpx``
     - ``>=0.27``
   * - ``tenacity``
     - ``>=8.0``
   * - Python
     - ``>=3.10``

Compatibility
-------------

.. list-table::
   :header-rows: 1

   * - Airflow
     - Status
   * - 3.0.6 (Amazon MWAA)
     - Supported. The full test suite runs against it with the official constraints
       (``scripts/test-airflow-version.sh``).
   * - 3.3.x
     - Supported; the development version (``uv.lock`` pins 3.3.2).
   * - 3.1, 3.2
     - Expected to work: the import fallbacks cover them, but they are not tested.
   * - 2.9, 2.10
     - Declared, not tested. Use at your own risk.

The provider registers itself through the ``apache_airflow_provider`` entry point. Airflow then
shows the **CIDP** connection type, the **See CIDP Run** extra link and the hook's connection test.

Quick start
-----------

1. Create a connection ``cidp_default`` (see :ref:`howto/connection:cidp`).
2. Use an operator:

.. literalinclude:: ../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_run_now]
   :end-before: [END howto_operator_cidp_run_now]
   :dedent: 4

The example DAGs in ``airflow/providers/cidp/example_dags`` use every operator and sensor.
