Live testing
============

The unit tests (``tests/``) run against recorded responses and the data API's OpenAPI document. The
live tests in ``tests/live/`` run the same hook, operators and sensors against a real CIDP data
service, so they show that the provider works against the platform itself, not only against its contract.
They are skipped unless you point them at a service.

What they check
---------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Test
     - What it does as the person whose token you pass
   * - ``test_connection``
     - ``CidpHook.test_connection``: the token is accepted and the workspace is reachable.
   * - ``test_run_sql``
     - ``CidpHook.run_sql``: a ``count(*)`` on the table returns one named column and a row.
   * - ``test_statement_submit_wait_rows``
     - ``submit_statement``, ``wait_for_statement`` and ``iter_statement_rows``: the asynchronous
       statement API, polled to the end, rows read back in pages.
   * - ``test_sql_operator``
     - ``CidpSqlOperator`` with two statements in one task.
   * - ``test_sql_sensor``
     - ``CidpSqlSensor.poke`` returns true when the query has a row.
   * - ``test_partition_sensor``
     - ``CidpPartitionSensor.poke`` finds ``country = 'NL'`` in the table.
   * - ``test_get_table``
     - ``CidpHook.get_table``: the table's metadata through the catalog.
   * - ``test_project_and_jobs``
     - ``create_project``, ``get_project``, ``list_jobs`` (empty for a new project), and
       ``delete_project`` when the module ends.
   * - ``test_project_action_sql_run``
     - A ``sql-query`` file in the project, then ``run_project_action(..., "sql.run")``.
   * - ``test_project_action_python_run``
     - A ``python-script`` file, then ``CidpProjectActionOperator(action="python.run")``: the
       run goes to the service's runs executor (Docker in the dev stack) and the operator polls it
       until it succeeds. ``CIDP_LIVE_RUNNER=0`` skips it on a service without an executor.
   * - ``test_volume_upload_download``
     - A volume in the table's schema, ``upload_file`` and ``download_file`` of a small file, then
       the volume is deleted (the API asks for ``?confirm=<name>``).

Every object the tests create has a random name and is deleted at the end, also when a test fails.

Not covered yet: ``CidpRunNowOperator`` with repair and ``CidpDeployJobsOperator``. They need a
project backed by git with a job bundle, and the owner's consent for an offline token. The data
service's own live tests (``tests/data_stack/test_live_jobs.py`` in ai-portal-data) cover that
path.

Running them against the dev stack
----------------------------------

Start the ai-portal-data quickstart (``make up`` and ``make dev`` there). The data service is on
``http://localhost:8095`` and Keycloak on ``http://localhost:8180``. Get a token for the
``engineer`` test user from the ``portal-dev-tests`` client, without printing it:

.. code-block:: bash

   cd ../ai-portal-data
   set -a; . quickstart/.env; set +a        # DATA_DEV_USER_PASSWORD, never printed
   export CIDP_LIVE_TOKEN=$(curl -s \
       -d grant_type=password -d client_id=portal-dev-tests -d username=engineer \
       --data-urlencode "password=$DATA_DEV_USER_PASSWORD" -d scope=openid \
       http://localhost:8180/realms/portal/protocol/openid-connect/token \
     | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
   unset DATA_DEV_USER_PASSWORD

   cd ../ai-portal-data-airflow
   CIDP_LIVE_HOST=http://localhost:8095 uv run pytest tests/live -q

The password grant exists only on the dev realm's test client. Against a shared deployment, use a
token from your normal sign-in, or a ``client_credentials`` token for a service principal.

Plain ``http://`` is accepted only because the tests set ``allow_insecure_http`` for an
``http://`` host. Airflow logs a warning about credentials over plaintext HTTP. Use ``https://``
anywhere but your own machine.

Settings
--------

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Variable
     - Default
     - Meaning
   * - ``CIDP_LIVE_HOST``
     - (none: skipped)
     - The data service, scheme included.
   * - ``CIDP_LIVE_TOKEN``
     - (none: skipped)
     - A bearer token. Never logged; the tests pass it in ``AIRFLOW_CONN_CIDP_DEFAULT`` for the
       test run only.
   * - ``CIDP_LIVE_WORKSPACE``
     - ``workspace1``
     - The workspace of the connection.
   * - ``CIDP_LIVE_TABLE``
     - ``workspace1.default.customers``
     - ``catalog.schema.table`` with a ``country`` column and a row for ``NL`` (the quickstart's
       seed table).
   * - ``CIDP_LIVE_RUNNER``
     - ``1``
     - ``0`` skips ``python.run`` when the service has no runs executor.

Last run
--------

17 November 2026, against the ai-portal-data dev stack on a MacBook (Docker runs executor, SQE as
the engine), as ``engineer``: **11 passed**. Statements took 0.1 to 0.5 s each on an idle machine.
``python.run`` took 6 to 16 s from submit to success, most of it starting the runner container.
