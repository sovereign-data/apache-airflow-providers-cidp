# airflow-provider-cidp

An Airflow provider for CIDP, the Cloud Independent Data Platform (`/api/data/v1`): run
deployed jobs and project actions (dbt, Python, notebooks), run SQL, deploy job bundles, manage projects and wait on tables.

It is shaped after the [Databricks provider](https://airflow.apache.org/docs/apache-airflow-providers-databricks/):
the same operator families, parameter names (`job_parameters`, `idempotency_token`, `repair_run`,
`wait_for_termination`, `deferrable`, `polling_period_seconds`, `statement`, `catalog`, `schema`,
`timeout`, `partitions`, `partition_operator`...), XCom keys (`run_id`, `run_page_url`,
`statement_id`), deferrable triggers and a "See run" extra link. A DAG written for Databricks ports
with mostly mechanical renames; see [the mapping](#databricks--cidp).

## Documentation

Reference docs in the Airflow provider style live in [`docs/`](docs/index.rst) (Sphinx RST):

- [Connection](docs/connections/cidp.rst): auth types, every extra, MWAA Secrets Manager.
- [Operators and sensors](docs/operators/index.rst): one page per class, with every parameter,
  its default and whether it is templated, plus examples from `example_dags/`.
- [Deferrable mode and triggers](docs/deferrable.rst), [the hook](docs/hooks.rst),
  [security](docs/security.rst), [changelog](docs/changelog.rst).
- [Compared with the Databricks and Amazon providers](docs/comparison.rst): operator mapping,
  parameter differences and what is not supported, against the provider versions in the
  Airflow 3.0.6 (MWAA) constraints.

- [Live testing](docs/live-testing.rst): `tests/live/` against a running data service
  (`CIDP_LIVE_HOST`, `CIDP_LIVE_TOKEN`; skipped without them).

Build the HTML with `uv run --group docs sphinx-build -W -b html docs docs/_build`.
`tests/test_docs.py` fails when a parameter table no longer matches the code.

## Install

Not published on PyPI. Install from git, or from your internal index:

```bash
pip install "airflow-provider-cidp @ git+<repo-url>@<tag>"
```

Requires Python 3.10+, `apache-airflow>=2.9`, `httpx>=0.27` and `tenacity>=8`.

Supported and tested:

| Airflow | Where                    | How it is tested                                                    |
|---------|--------------------------|---------------------------------------------------------------------|
| 3.0.6   | Amazon MWAA              | `scripts/test-airflow-version.sh` (official 3.0.6 constraints, Python 3.12) |
| 3.3.x   | Locked dev environment   | `uv run pytest`, plus mypy                                          |

The Airflow 2 import paths are kept but not tested.

On MWAA, install it with the environment's constraints file, so `httpx` and `tenacity` stay at
the versions MWAA pins (3.0.6 pins `httpx==0.28.1` and `tenacity==9.1.2`, both within range). For
an environment without internet access, put the wheel (`uv build`) in `plugins.zip` and point
`requirements.txt` at it.

## Connection

Connection type **CIDP** (`cidp`), default id `cidp_default`.

| Field      | Value                                                                      |
|------------|----------------------------------------------------------------------------|
| Host       | The CIDP URL, e.g. `https://cidp.example.com` (https is assumed)       |
| Login      | Client id of a **project service principal** (`client_credentials` only)   |
| Password   | The client secret, or a bearer token (`token`)                             |
| Extra      | JSON, see below                                                            |

| Extra key             | Default         | Meaning                                                                 |
|-----------------------|-----------------|-------------------------------------------------------------------------|
| `auth_type`           | `token`         | `token` (static bearer) or `client_credentials` (OAuth2)                |
| `workspace`           | none            | Default workspace slug; operators can override it with `workspace=`     |
| `issuer`              | from `/config`  | Override the OIDC issuer the API advertises                             |
| `ui_url`              | host            | CIDP UI origin for run links                                          |
| `api_prefix`          | `/api/data/v1`  | API path prefix                                                         |
| `ca_cert`             | none            | CA bundle path for TLS                                                  |
| `verify_ssl`          | `true`          | `false` disables TLS verification (logged as a warning)                 |
| `allow_insecure_http` | `false`         | Allow `http://` for the API, issuer and token endpoint (local dev only) |

```bash
airflow connections add cidp_default \
  --conn-type cidp \
  --conn-host https://cidp.example.com \
  --conn-login ledger-etl-sp \
  --conn-password "$CLIENT_SECRET" \
  --conn-extra '{"auth_type": "client_credentials", "workspace": "analytics"}'
```

With `client_credentials` the hook reads the issuer from the API's unauthenticated `/config`, then
the token endpoint from the issuer's `/.well-known/openid-configuration`, and caches the token
until 30 s before it expires. The client id `portal-data-svc` (the data service's own platform
account) is refused: Airflow always signs in as a project service principal or a user token, never
as the service. "Test connection" lists one project in the workspace.

## Operators, sensors and triggers

| CIDP                          | Databricks counterpart                                  | What it does                                                                                       |
|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `CidpRunNowOperator`           | `DatabricksRunNowOperator`                              | Start a deployed job (`project`, `job`, `job_parameters`) and wait; `repair_run` retries failed tasks once |
| `CidpProjectActionOperator`    | `DatabricksSubmitRunOperator` / `DatabricksNotebookOperator` | Run an action on a project file (`dbt.build`, `python.run`, `notebook.run_all`, `sql.run`...) |
| `CidpDeployJobsOperator`       | `DatabricksCreateJobsOperator`                          | Deploy the project's job bundle (`target`), or `validate_only`                                     |
| `CidpSQLStatementsOperator`    | `DatabricksSQLStatementsOperator`                       | Submit one statement, push `statement_id`, wait (sync or deferred), cancel on timeout/kill         |
| `CidpSqlOperator`              | `DatabricksSqlOperator`                                 | Run one or more statements; return rows or write csv/json/jsonl to `output_path`                   |
| `CidpProjectCreateOperator`    | `DatabricksReposCreateOperator`                         | Create a project, optionally from `git_url`; `ignore_existing_project`                             |
| `CidpProjectUpdateOperator`    | `DatabricksReposUpdateOperator`                         | Pull a project branch from its git remote                                                          |
| `CidpProjectDeleteOperator`    | `DatabricksReposDeleteOperator`                         | Delete a project; `ignore_missing`                                                                 |
| `CidpQualityRunOperator`       | none                                                    | Run a table's quality checks; fail on failures or below `min_score` (**preview API**)               |
| `CidpSQLStatementsSensor`      | `DatabricksSQLStatementsSensor`                         | Wait for a statement (submit `statement` or watch `statement_id`); deferrable                     |
| `CidpSqlSensor`                | `DatabricksSqlSensor`                                   | Poke until `sql` returns rows (or `handler` accepts them)                                          |
| `CidpPartitionSensor`          | `DatabricksPartitionSensor`                             | Poke until `table_name` has rows for `partitions`                                                  |
| `CidpExecutionTrigger`         | `DatabricksExecutionTrigger`                            | Poll a job run or project run in the triggerer                                                     |
| `CidpSQLStatementExecutionTrigger` | `DatabricksSQLStatementExecutionTrigger`            | Long-poll a statement in the triggerer; cancel it at `end_time`                                    |
| `CidpHook`                     | `DatabricksHook`                                        | Jobs, runs, project actions, statements, projects, deployment, tables, quality, volume files       |

All operators take `cidp_conn_id`, `workspace`, `cidp_retry_limit` and
`cidp_retry_delay`. These match Databricks' `databricks_conn_id` and
`databricks_retry_*`.

### Example

```python
from airflow.providers.cidp.operators.cidp import CidpRunNowOperator
from airflow.providers.cidp.operators.cidp_sql import CidpSqlOperator

nightly = CidpRunNowOperator(
    task_id="run_nightly",
    project="ledger-etl",
    job="nightly-ledger",
    job_parameters={"run_date": "{{ ds }}"},
    repair_run=True,
    deferrable=True,
)

counts = CidpSqlOperator(
    task_id="row_count",
    sql="SELECT count(*) FROM lake.marts.ledger WHERE ledger_date = DATE '{{ ds }}'",
)
```

`CidpSqlOperator` splits a string on `;` outside quotes and comments. Only a doubled
quote (`''`) counts as an escape, so with backslash-escaped quotes pass a list of statements or set
`split_statements=False`.

A full DAG is in [`example_dags/example_cidp.py`](airflow/providers/cidp/example_dags/example_cidp.py).

## Databricks → CIDP

| Databricks                                   | CIDP                                                      |
|----------------------------------------------|------------------------------------------------------------------|
| `job_id` / `job_name`                        | `project` + `job` (the job's key in the project's bundle)        |
| `notebook_params`, `python_params`, `jar_params`... | `job_parameters` (the only kind; values are sent as strings) |
| `warehouse_id` / `http_path` / `sql_endpoint_name` | `engine` (`sqe` default, or `spark`); no warehouses        |
| `query_tags` (dict, releases after 7.7.1)    | `query_tag` (one string; default `airflow:<dag_id>.<task_id>`)   |
| `parameters` (statement parameters)          | none: the statements API takes no bind parameters                |
| `repo_path` / `repo_id`                      | `project` (slug)                                                 |
| `ignore_existing_repo`                       | `ignore_existing_project`                                        |
| `DatabricksJobRunLink` ("See Databricks Job Run") | `CidpJobRunLink` ("See CIDP Run")           |
| `databricks_conn_id`                         | `cidp_conn_id`                                            |

## Differences from the Databricks provider

The full list, with the Amazon provider alongside, is in [docs/comparison.rst](docs/comparison.rst).

- **Identity.** Everything runs as the connection's identity: a user token or a project service
  principal. A job run executes as the job's own `run_as`. There is no password grant and no
  impersonation.
- **Jobs are defined in the project, not in the DAG.** There is no `json=` job spec and no
  submit-run with an inline cluster. Deploy the bundle with `CidpDeployJobsOperator`. For
  one-off work, run an action on a file with `CidpProjectActionOperator`.
- **Run states** are lower case and flat: `queued`, `running`, `succeeded`, `failed`,
  `cancelled`, `skipped` or `expired`. There is no life-cycle/result split. Unknown states count
  as not ended.
- **Repair** is `POST …/runs/{id}/retry`. It starts a new run of the failed tasks and their
  dependants, and that run's `repair_of` names the original. The operator repairs at most once
  per task try, and only a run that ended `failed`.
- **Statement `CLOSED`** ends the wait but is not success, as in Databricks. The API reports it
  when a finished statement's result has expired, so the original outcome is unknown.
- **Idempotency.** `run_now`, repairs, project creation and the statement submits of
  `CidpSQLStatementsOperator` / `CidpSQLStatementsSensor` send an `Idempotency-Key`. By default it is a hash of the dag, task, run, map index and try number. A
  repeat of the same try then gets the same run back, and an Airflow retry starts a new one. A
  keyed POST is safe to retry on 5xx and transport errors. An unkeyed POST (`CidpSqlOperator`,
  `CidpSqlSensor`, `CidpPartitionSensor` and other actions) is retried only on 429 or
  connect errors.
- **Project runs have no per-run page** in the CIDP UI. Their link opens the project page.
- **Volumes** (`upload_file`, `download_file`) are hook methods only, with no operators.

### Preview APIs

`CidpQualityRunOperator` (`…/quality-runs`) uses an API the contract marks as preview. It
may change without a version bump. `/statements/execute` is also preview, so the SQL operators use
the stable submit / long-poll / result-page path instead.

## Security

- The token or client secret, and any token fetched with it, go through `mask_secret`, so they
  never appear in task logs.
- The API, issuer and token endpoint must be https unless `allow_insecure_http` is set.
- Redirects are not followed, so a bearer token is never sent to another origin.
- Path segments (workspace, project, job, run and statement ids) are percent-encoded.
- Error messages keep the API's `code`, `message` (cut to 500 characters) and request id. A
  failed sign-in reports only the issuer's error code, never its description, which may quote
  the request.
- Statement rows are never logged.
- When a deferred task is killed (marked failed, cleared), the trigger tries to cancel the backend
  run or statement. This is best effort: if it cannot read the task state, it leaves the work
  running, as it does on a triggerer restart where the task is still deferred.

More in [docs/security.rst](docs/security.rst).

## Development

```bash
uv sync
AIRFLOW_HOME=$PWD/.airflow uv run coverage run -m pytest && uv run coverage report
scripts/test-airflow-version.sh          # the same suite on Airflow 3.0.6 (MWAA)
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run --group docs sphinx-build -W -b html docs docs/_build   # the docs
```

`tests/contract/openapi-v1.json` is the data API contract, vendored from `ai-portal-data-sdk`. The
contract test runs every hook method and checks each request against it: the path and method
exist, query parameters and `Idempotency-Key` are declared, and JSON bodies use only schema
fields. Refresh the file when the API changes.

## License

Apache 2.0, see [LICENSE](LICENSE).
