Changelog
=========

0.1.0
-----

First release.

* Connection type ``cidp`` with token and ``client_credentials`` sign-in, OIDC discovery, custom
  CA, connection test.
* ``CidpRunNowOperator`` (with repair), ``CidpProjectActionOperator``, ``CidpDeployJobsOperator``.
* ``CidpSQLStatementsOperator``, ``CidpSqlOperator``, ``CidpSQLStatementsSensor``,
  ``CidpSqlSensor``, ``CidpPartitionSensor``.
* ``CidpProjectCreateOperator``, ``CidpProjectUpdateOperator``, ``CidpProjectDeleteOperator``.
* ``CidpQualityRunOperator`` (preview API).
* Deferrable mode for the run and statement operators and the statements sensor.
* The **See CIDP Run** extra link.
* Tested on Airflow 3.0.6 (MWAA) and 3.3.2.
