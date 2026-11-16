.. _howto/operator:CidpSqlOperator:

CidpSqlOperator
===============

``airflow.providers.cidp.operators.cidp_sql.CidpSqlOperator``

Runs one or more statements one after another and returns their rows, or writes them to a file.
The counterpart of ``DatabricksSqlOperator``, but it goes through the statements API over HTTPS,
so there is no SQL driver, ``http_path`` or warehouse to configure. Statements run as the
**connection's** identity.

Example
-------

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_operator_cidp_sql]
   :end-before: [END howto_operator_cidp_sql]
   :dedent: 4

Writing to a file:

.. code-block:: python

   export = CidpSqlOperator(
       task_id="export_ledger",
       sql="SELECT * FROM lake.marts.ledger WHERE ledger_date = DATE '{{ ds }}'",
       output_path="/tmp/ledger_{{ ds_nodash }}.csv",
       output_format="csv",
       csv_params={"delimiter": ";", "header": True},
   )

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``sql``
     - required
     - yes
     - The SQL: a string, a list of statements, or a path ending in ``.sql``.
   * - ``engine``
     - ``None``
     - no
     - ``sqe`` or ``spark``. Default: the server's default engine (``sqe``).
   * - ``catalog``
     - ``None``
     - yes
     - The default catalog for unqualified names.
   * - ``schema``
     - ``None``
     - yes
     - The default schema for unqualified names.
   * - ``row_limit``
     - ``None``
     - no
     - The most rows each statement keeps. A cut result logs a warning.
   * - ``return_last``
     - ``True``
     - no
     - Return only the last statement's rows. ``False`` returns a list per statement.
   * - ``split_statements``
     - ``True``
     - no
     - Split a string on ``;`` outside quotes and comments. See the note below.
   * - ``output_path``
     - ``None``
     - yes
     - Write the rows to this file instead of returning them; the task returns ``None``.
   * - ``output_format``
     - ``'csv'``
     - no
     - ``csv``, ``json`` (one array) or ``jsonl`` (one object per line).
   * - ``csv_params``
     - ``None``
     - no
     - Keyword arguments for ``csv.writer``. ``header`` (default ``True``) writes the column names first.
   * - ``query_tag``
     - ``None``
     - yes
     - A tag kept with the statement. Default: ``airflow:<dag_id>.<task_id>``. No personal data.
   * - ``include_airflow_query_tags``
     - ``True``
     - no
     - Send ``airflow:<dag_id>.<task_id>`` when ``query_tag`` is not set.
   * - ``timeout``
     - ``3600``
     - no
     - Seconds each statement may take before it is cancelled and the task fails.
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

* Returns the last statement's rows as a list of lists (``return_last=True``), or one list per
  statement. Column names are not returned; with ``output_path`` they are written (``csv`` header,
  ``json``/``jsonl`` keys).
* With ``output_path`` and ``return_last=False``, every statement's rows go to the file, under the
  last statement's columns.
* A result cut at ``row_limit`` (or the server's limit) logs a warning; it does not fail.
* Rows are never logged, but the return value goes to XCom (the metadata database). For large or
  sensitive results, use ``output_path`` or ``do_xcom_push=False``.
* No idempotency key is sent, so a ``5xx`` on a submit is not retried; an Airflow retry runs the
  statements again. Make the SQL safe to repeat.
* A statement that is still running after ``timeout`` seconds is cancelled and the task fails. Not
  deferrable.

.. note::

   ``split_statements`` splits on ``;`` outside quotes and comments. It does not understand
   procedural blocks (``BEGIN ... END;``) or dollar quoting; pass such SQL as a list, or with
   ``split_statements=False``.

``sql`` is rendered with Jinja. Do not template untrusted values into SQL; the statements API has
no bind parameters.
