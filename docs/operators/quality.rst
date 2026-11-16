.. _howto/operator:CidpQualityRunOperator:

CidpQualityRunOperator
======================

``airflow.providers.cidp.operators.cidp_quality.CidpQualityRunOperator``

.. warning::

   This uses a **preview** API (``POST .../tables/{table}/quality-runs``). Its shape may change
   in a later CIDP release.

Runs a table's quality checks, or one of them, and fails when any check failed or errored, or when
the score is below ``min_score``. The Databricks provider has no counterpart; on AWS it is
``GlueDataQualityRuleSetEvaluationRunOperator``. The checks are defined on the table in CIDP, not
in the DAG.

Example
-------

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_operator_cidp_quality]
   :end-before: [END howto_operator_cidp_quality]
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
   * - ``catalog``
     - required
     - yes
     - The table's catalog.
   * - ``schema``
     - required
     - yes
     - The table's schema.
   * - ``table``
     - required
     - yes
     - The table.
   * - ``check``
     - ``None``
     - yes
     - Run only this check. Default: every check on the table.
   * - ``fail_on_failure``
     - ``True``
     - no
     - Fail when any check failed or errored.
   * - ``min_score``
     - ``None``
     - no
     - Fail when the run's score is below this (or has no score).
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

Returns the quality run: ``id``, ``passed``, ``failed``, ``errored``, ``total``, ``score``... It
runs as the connection's identity. The operator acts on the answer of one API call; it does not
poll.
