Query sensors
=============

Both sensors run a query on each poke as the **connection's** identity, through the statements
API. They are not deferrable; use ``mode="reschedule"`` to free the worker slot between pokes. For
waiting on a statement, see
:ref:`CidpSQLStatementsSensor <howto/sensor:CidpSQLStatementsSensor>`.

They take ``BaseSensorOperator``'s parameters (``poke_interval``, ``timeout``, ``mode``,
``exponential_backoff``...) on top of the ones below, and use the hook's default retries.

.. _howto/sensor:CidpSqlSensor:

CidpSqlSensor
-------------

``airflow.providers.cidp.sensors.cidp_sql.CidpSqlSensor``

Pokes until ``sql`` returns what ``handler`` accepts; by default, any row. The counterpart of
``DatabricksSqlSensor``.

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp_projects.py
   :language: python
   :start-after: [START howto_sensor_cidp_sql]
   :end-before: [END howto_sensor_cidp_sql]
   :dedent: 4

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
     - The query to run on each poke. A path ending in ``.sql`` is read as a template.
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
   * - ``handler``
     - any row
     - no
     - Called with the rows (at most 1000); the sensor succeeds when it returns true. Default: any row.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - yes
     - The Airflow connection to use.
   * - ``workspace``
     - ``None``
     - yes
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``statement_timeout``
     - ``600``
     - no
     - Seconds each poke's query may take before it is cancelled.

Each poke fetches at most 1000 rows. A handler that counts:

.. code-block:: python

   CidpSqlSensor(
       task_id="enough_orders",
       sql="SELECT count(*) FROM lake.raw.orders WHERE order_date = DATE '{{ ds }}'",
       handler=lambda rows: rows[0][0] >= 1000,
   )

.. _howto/sensor:CidpPartitionSensor:

CidpPartitionSensor
-------------------

``airflow.providers.cidp.sensors.cidp_sql.CidpPartitionSensor``

Pokes until a table has a row matching ``partitions``. The counterpart of
``DatabricksPartitionSensor`` and of Amazon's ``GlueCatalogPartitionSensor``; it reads the table
rather than catalog metadata, so it works for any column, partitioned or not.

.. literalinclude:: ../../airflow/providers/cidp/example_dags/example_cidp.py
   :language: python
   :start-after: [START howto_sensor_cidp_partition]
   :end-before: [END howto_sensor_cidp_partition]
   :dedent: 4

.. list-table::
   :header-rows: 1
   :widths: 22 18 9 51

   * - Parameter
     - Default
     - Templated
     - Description
   * - ``table_name``
     - required
     - yes
     - The table. A plain name; it is double-quoted.
   * - ``partitions``
     - required
     - yes
     - Column to value. A list means any of its values. Values become SQL literals; ``date`` and ``datetime`` become ``DATE`` and ``TIMESTAMP``.
   * - ``partition_operator``
     - ``'='``
     - no
     - ``=``, ``!=``, ``<>``, ``<``, ``<=``, ``>`` or ``>=``.
   * - ``catalog``
     - ``None``
     - yes
     - The table's catalog. Default: the server's.
   * - ``schema``
     - ``'default'``
     - yes
     - The table's schema.
   * - ``engine``
     - ``None``
     - no
     - ``sqe`` or ``spark``. Default: the server's default engine (``sqe``).
   * - ``handler``
     - any row
     - no
     - Called with the rows; the default (any row) fits the generated query.
   * - ``cidp_conn_id``
     - ``'cidp_default'``
     - yes
     - The Airflow connection to use.
   * - ``workspace``
     - ``None``
     - yes
     - The workspace slug. Default: the connection's ``workspace`` extra.
   * - ``statement_timeout``
     - ``600``
     - no
     - Seconds each poke's query may take before it is cancelled.

The sensor builds ``SELECT 1 FROM "catalog"."schema"."table" WHERE ... LIMIT 1``:

* ``{"region": ["eu", "us"], "day": "2026-01-01"}`` becomes
  ``("region" = 'eu' OR "region" = 'us') AND "day" = '2026-01-01'``.
* Table, schema, catalog and column names must be plain identifiers (a letter or underscore, then
  letters, digits, underscores); anything else raises.
* Values are SQL literals: strings quoted and escaped, numbers and booleans as they are,
  ``datetime.date`` as ``DATE '...'``, ``datetime.datetime`` as ``TIMESTAMP '...'``. A templated
  ``{{ ds }}`` is a string; compare it with a string column, or pass a date object.
* ``partition_operator`` applies to every column.
* It does not take ``sql``.
