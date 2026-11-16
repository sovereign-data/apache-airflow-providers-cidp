Security
========

Identity
--------

The provider calls the data API as the identity in the Airflow connection, and only as that:

* ``auth_type: token``: a bearer token in the connection password. It is used as it is and never
  refreshed.
* ``auth_type: client_credentials``: a project service principal's client id and secret. The hook
  gets a token from the issuer's token endpoint and caches it until 30 s before it expires. A
  ``401`` fetches a new token once.

There is no password grant, no impersonation and no "on behalf of". The data service's own
platform account (``portal-data-svc``) is refused as a client id, because the service account never
reads for a user. A job started with ``CidpRunNowOperator`` runs as the job's own ``run_as``,
which the API checks. The caller needs permission to run the job, not to read its data. Project
actions, SQL, project management and quality runs run as the connection.

Give each DAG, or each group of DAGs, its own service principal with the least access it needs.
Avoid personal tokens in shared connections.

Transport
---------

* The API, the issuer and the token endpoint must be https. ``allow_insecure_http: true`` lifts
  this for local development only.
* ``verify_ssl: false`` turns off certificate checks and logs a warning. Use ``ca_cert`` for a
  private CA instead.
* Redirects are never followed, so a bearer token cannot be sent to another origin.
* Every path segment (workspace, project, job, run id, statement id, catalog, schema, table,
  volume) is percent-encoded, so a templated value cannot change the URL's path.

Secrets and logs
----------------

* The connection password and every fetched access token go through Airflow's ``mask_secret``.
* Error messages carry the HTTP status, the API's error ``code``, its ``message`` cut to 500
  characters, and the ``x-request-id``. They never carry the request body or headers.
* A refused sign-in reports only the issuer's ``error`` code (or the HTTP status), never
  ``error_description``, which may quote the request.
* The SQL operators and sensors never log result rows. They log only statement counts, state and
  row counts. ``CidpSqlOperator`` returns its rows, so by default they land in XCom (the
  metadata database). For sensitive results, set ``output_path`` to a protected location, or
  ``do_xcom_push=False``.
* Query tags default to ``airflow:<dag_id>.<task_id>``. Do not put personal data in ``query_tag``.

SQL
---

The statements API has no bind parameters. Templated SQL is plain text substitution, so never
template values from untrusted input (DAG run ``conf``, external files) into ``sql`` or
``statement``. ``CidpPartitionSensor`` is the exception: it checks identifiers and quotes values
itself.

Cancellation
------------

* In ``on_kill`` the run and statement operators cancel the run or statement they started.
  ``CidpSQLStatementsSensor`` cancels only a statement it submitted itself. A statement that does
  not end within ``timeout`` is cancelled.
* Deferred tasks: when the trigger is cancelled and the task has left ``deferred`` (marked failed,
  cleared), the trigger tries to cancel the backend work. It reads the task state through the
  Airflow 3 task SDK (``RuntimeTaskInstance.get_task_states``), or the database on Airflow 2. If
  that read fails it leaves the work running. A triggerer restart or rebalance also leaves the work
  running, and the next triggerer picks the wait up again. This path is unit-tested only; it has
  not been run in a live triggerer.

Idempotency
-----------

Run starts, repairs, project creation and the statement submits of the SQLStatements operator and
sensor send an ``Idempotency-Key`` derived from the dag, task, run, map index and try number
(sha256, 64 hex characters). Retrying a request inside one try returns the same run rather than
starting a second one. On ``CidpRunNowOperator``, pass ``idempotency_token`` to set the key
yourself. ``CidpSqlOperator``, the query sensors and project actions send no key, so a ``5xx`` on
their submit is not retried.
