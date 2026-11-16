.. _howto/connection:cidp:

CIDP connection
===============

The **CIDP** connection type (``cidp``) holds the URL of a CIDP installation and the identity the
provider calls the data API with. Every operator, sensor and trigger takes it as ``cidp_conn_id``;
the default is ``cidp_default``.

The provider calls the API **only** as this identity. There is no impersonation and no
"on behalf of"; see :doc:`../security`.

Authenticating
--------------

Pick one ``auth_type`` in the extra.

``token`` (the default)
    A bearer token in **Password**, used as it is. It is never refreshed, so it has to outlive the
    tasks that use it. Fit for tests and short-lived setups.

``client_credentials``
    A project service principal: client id in **Login**, client secret in **Password**. The hook
    asks the API's ``/config`` for the OIDC issuer (or takes the ``issuer`` extra), reads the
    issuer's discovery document for the token endpoint, and signs in with the OAuth2
    ``client_credentials`` grant. The token is cached until 30 s before it expires; a ``401``
    gets a new one once. This is the recommended type.

The data service's own platform account, ``portal-data-svc``, is refused as a client id.

Fields
------

.. list-table::
   :header-rows: 1
   :widths: 18 22 60

   * - Field
     - UI label
     - Meaning
   * - Host
     - CIDP URL
     - The CIDP origin, such as ``https://cidp.example.com``. Without a scheme, ``https://`` is
       added. Required.
   * - Login
     - Client ID (client_credentials)
     - The service principal's client id. Only used with ``client_credentials``.
   * - Password
     - Token or client secret
     - The bearer token (``token``) or the client secret (``client_credentials``). Required.
       Masked in logs.
   * - Schema, Port
     - hidden
     - Not used. Put the port in the host if you need one.
   * - Extra
     - Extra
     - A JSON object; see below.

.. _howto/connection:cidp:extras:

Extra
-----

.. list-table::
   :header-rows: 1
   :widths: 20 22 58

   * - Key
     - Default
     - Meaning
   * - ``auth_type``
     - ``token``
     - ``token`` or ``client_credentials``.
   * - ``workspace``
     - none
     - The default workspace slug. A task's ``workspace`` argument overrides it. One of the two is
       required.
   * - ``issuer``
     - from ``/config``
     - The OIDC issuer for ``client_credentials``, when you do not want it discovered.
   * - ``ui_url``
     - the host
     - The CIDP UI origin, used for the **See CIDP Run** link and ``run_page_url``.
   * - ``api_prefix``
     - ``/api/data/v1``
     - The data API's path under the host.
   * - ``ca_cert``
     - none
     - Path to a CA bundle (PEM) on the workers and triggerer, for a private CA. Takes precedence
       over ``verify_ssl``.
   * - ``verify_ssl``
     - ``true``
     - ``false`` turns certificate checks off and logs a warning. Prefer ``ca_cert``.
   * - ``allow_insecure_http``
     - ``false``
     - Allow ``http://`` for the API, the issuer and the token endpoint, and so send credentials in
       plain text. Local development only.

Examples
--------

As an environment variable, in Airflow's JSON form:

.. code-block:: bash

   export AIRFLOW_CONN_CIDP_DEFAULT='{
     "conn_type": "cidp",
     "host": "https://cidp.example.com",
     "login": "ledger-etl-sp",
     "password": "<client secret>",
     "extra": {"auth_type": "client_credentials", "workspace": "analytics"}
   }'

With the CLI:

.. code-block:: bash

   airflow connections add cidp_default \
     --conn-type cidp \
     --conn-host https://cidp.example.com \
     --conn-login ledger-etl-sp \
     --conn-password "$CIDP_CLIENT_SECRET" \
     --conn-extra '{"auth_type": "client_credentials", "workspace": "analytics"}'

On Amazon MWAA, keep the connection in AWS Secrets Manager (the secrets backend) rather than in the
metadata database, in the same JSON form.

A private CA and a separate UI origin:

.. code-block:: json

   {
     "auth_type": "client_credentials",
     "workspace": "analytics",
     "ca_cert": "/usr/local/airflow/certs/corp-ca.pem",
     "ui_url": "https://portal.example.com"
   }

Testing the connection
----------------------

**Test** in the connection form (when ``[core] test_connection`` is enabled) signs in and lists
one project of the workspace. It answers ``Connected to workspace '<slug>'`` or the API's error.
