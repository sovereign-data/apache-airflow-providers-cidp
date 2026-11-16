"""Transport and authentication for the CIDP data API (``/api/data/v1``).

Everything runs as the connection's identity: a static bearer token, or the client
credentials of a project service principal (OAuth2 ``client_credentials``). There is no
password grant and no impersonation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from functools import cached_property
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.providers.cidp import __version__
from airflow.providers.cidp.exceptions import (
    CidpApiError,
    CidpAuthError,
    CidpConflictError,
    CidpNotFoundError,
    CidpUnavailableError,
)
from airflow.providers.cidp.version_compat import BaseHook, mask_secret

DEFAULT_API_PREFIX = "/api/data/v1"
DEFAULT_CONN_ID = "cidp_default"
AUTH_TOKEN = "token"  # noqa: S105 — an auth type name, not a secret
AUTH_CLIENT_CREDENTIALS = "client_credentials"
# The data service's own platform account: never a caller's identity.
SERVICE_ACCOUNT = "portal-data-svc"
_TOKEN_SKEW_SECONDS = 30
_MAX_ERROR_TEXT = 500
_IDEMPOTENT = {"GET", "HEAD", "PUT", "DELETE"}
_USER_AGENT = f"airflow-provider-cidp/{__version__}"


def _seg(value: Any) -> str:
    """Percent-encode one URL path segment so an id can't inject ``/``, ``..``, ``?`` or ``#``."""
    return quote(str(value), safe="")


def _truthy(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() not in ("false", "0", "no", "off", "")
    return bool(value)


class _Transient(CidpApiError):
    """Retryable: a connect error, 429, or a 5xx on a request that is safe to repeat."""


class CidpBaseHook(BaseHook):
    """Authenticated HTTP to the data API, sync (operators) and async (triggers).

    Connection fields:

    * ``host``: the CIDP origin, e.g. ``https://cidp.example.com``.
    * ``password``: the bearer token (``auth_type=token``) or the client secret.
    * ``login``: the client id (``auth_type=client_credentials``).
    * extra ``auth_type``: ``token`` (default) or ``client_credentials``.
    * extra ``workspace``: the default workspace slug.
    * extra ``issuer``: override the OIDC issuer the API advertises at ``/config``.
    * extra ``verify_ssl`` / ``ca_cert``: TLS verification.
    * extra ``api_prefix``: default ``/api/data/v1``.
    * extra ``ui_url``: the CIDP UI origin for run links (default: ``host``).
    """

    conn_name_attr = "cidp_conn_id"
    default_conn_name = DEFAULT_CONN_ID
    conn_type = "cidp"
    hook_name = "CIDP"

    def __init__(
        self,
        cidp_conn_id: str = DEFAULT_CONN_ID,
        *,
        workspace: str | None = None,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ):
        super().__init__()
        if retry_limit < 1:
            raise ValueError("retry_limit must be at least 1")
        self.cidp_conn_id = cidp_conn_id
        self._workspace_override = workspace
        self.timeout_seconds = timeout_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0
        self._token_endpoint: str | None = None
        self._tls_warned = False

    # ---- connection helpers ----
    @cached_property
    def _conn(self):
        return self.get_connection(self.cidp_conn_id)

    def _extra(self) -> dict:
        return self._conn.extra_dejson or {}

    def _host(self) -> str:
        host = (self._conn.host or "").strip().rstrip("/")
        if not host:
            raise CidpAuthError(f"connection {self.cidp_conn_id!r} has no host")
        if "://" not in host:
            host = f"https://{host}"
        return host

    def _base_url(self) -> str:
        prefix = "/" + str(self._extra().get("api_prefix", DEFAULT_API_PREFIX)).strip("/")
        return f"{self._host()}{prefix}"

    def _auth_type(self) -> str:
        return str(self._extra().get("auth_type", AUTH_TOKEN))

    @property
    def workspace(self) -> str:
        ws = self._workspace_override or self._extra().get("workspace")
        if not ws:
            raise CidpApiError(f"no workspace: pass workspace= or set extra.workspace on {self.cidp_conn_id!r}")
        return str(ws)

    def _ws(self) -> str:
        """``/workspaces/{workspace}``, encoded."""
        return f"/workspaces/{_seg(self.workspace)}"

    def ui_url(self) -> str:
        return str(self._extra().get("ui_url") or self._host()).rstrip("/")

    def _verify(self) -> bool | str:
        extra = self._extra()
        if extra.get("ca_cert"):
            return str(extra["ca_cert"])
        verify = _truthy(extra.get("verify_ssl"), True)
        if not verify and not self._tls_warned:
            self._tls_warned = True
            self.log.warning("TLS verification is disabled for connection %s", self.cidp_conn_id)
        return verify

    def _check_scheme(self, url: str, what: str) -> None:
        """Credentials go over https only, unless the connection says ``allow_insecure_http``."""
        if urlsplit(url).scheme.lower() == "https":
            return
        if _truthy(self._extra().get("allow_insecure_http"), False):
            self.log.warning("sending credentials over plaintext HTTP to the %s", what)
            return
        raise CidpAuthError(f"the {what} is not https; set extra.allow_insecure_http=true only for local development")

    # ---- auth ----
    def _discover_token_endpoint(self) -> str:
        """``/config`` (no token) names the issuer; its discovery document names the token endpoint."""
        if self._token_endpoint:
            return self._token_endpoint
        verify = self._verify()
        issuer = self._extra().get("issuer")
        try:
            if not issuer:
                cfg = httpx.get(
                    f"{self._base_url()}/config",
                    headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
                    timeout=self.timeout_seconds,
                    verify=verify,
                )
                if cfg.status_code >= 400:
                    raise CidpAuthError(f"GET /config answered HTTP {cfg.status_code}", status_code=cfg.status_code)
                issuer = cfg.json().get("issuer")
            if not issuer or not isinstance(issuer, str):
                raise CidpAuthError("the data API names no OIDC issuer at /config")
            issuer = issuer.rstrip("/")
            self._check_scheme(issuer, "OIDC issuer")
            disc = httpx.get(
                f"{issuer}/.well-known/openid-configuration",
                headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
                timeout=self.timeout_seconds,
                verify=verify,
            )
            if disc.status_code >= 400:
                raise CidpAuthError(
                    f"the issuer's discovery document answered HTTP {disc.status_code}",
                    status_code=disc.status_code,
                )
            endpoint = disc.json().get("token_endpoint")
        except (httpx.HTTPError, ValueError) as exc:
            raise CidpAuthError(f"OIDC discovery failed ({type(exc).__name__})") from None
        if not endpoint or not isinstance(endpoint, str):
            raise CidpAuthError("the issuer's discovery document names no token_endpoint")
        self._check_scheme(endpoint, "token endpoint")
        self._token_endpoint = endpoint
        return endpoint

    def _token(self) -> str:
        if self._cached_token and time.monotonic() < self._token_expiry:
            return self._cached_token
        conn, auth_type = self._conn, self._auth_type()
        if not conn.password:
            raise CidpAuthError(f"connection {self.cidp_conn_id!r} has no password/token set")
        if auth_type == AUTH_TOKEN:
            mask_secret(conn.password)
            self._cached_token, self._token_expiry = conn.password, float("inf")
            return conn.password
        if auth_type != AUTH_CLIENT_CREDENTIALS:
            raise CidpAuthError(f"unknown auth_type {auth_type!r}: use {AUTH_TOKEN!r} or {AUTH_CLIENT_CREDENTIALS!r}")
        client_id = conn.login
        if not client_id:
            raise CidpAuthError("auth_type client_credentials needs the client id as login")
        if client_id == SERVICE_ACCOUNT:
            raise CidpAuthError(
                f"{SERVICE_ACCOUNT} is the data service's platform account and never signs in from "
                "Airflow: use a project service principal"
            )
        mask_secret(conn.password)
        endpoint = self._discover_token_endpoint()
        try:
            resp = httpx.post(
                endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": conn.password,
                },
                headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
                timeout=self.timeout_seconds,
                verify=self._verify(),
            )
        except httpx.HTTPError as exc:
            raise CidpAuthError(f"the token endpoint did not answer ({type(exc).__name__})") from None
        try:
            body = resp.json()
        except ValueError:
            body = {}
        token = body.get("access_token") if isinstance(body, dict) else None
        if resp.status_code >= 400 or not isinstance(token, str) or not token:
            # The issuer's error code only: its description may quote what was sent.
            error = body.get("error") if isinstance(body, dict) else None
            raise CidpAuthError(
                f"the issuer refused the sign-in ({error if isinstance(error, str) else f'HTTP {resp.status_code}'})",
                status_code=resp.status_code,
            )
        mask_secret(token)
        self.log.info("signed in to the data API as client %s", client_id)
        self._cached_token = token
        self._token_expiry = time.monotonic() + float(body.get("expires_in") or 300) - _TOKEN_SKEW_SECONDS
        return token

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Accept": "application/json",
            "User-Agent": _USER_AGENT,
            "X-Request-Id": uuid.uuid4().hex,
            **(extra or {}),
        }

    # ---- transport ----
    @staticmethod
    def _repeatable(method: str, headers: dict[str, str] | None) -> bool:
        """GET/PUT/DELETE, or a POST carrying an ``Idempotency-Key`` (the server answers a repeat as the first)."""
        return method.upper() in _IDEMPOTENT or bool(headers and headers.get("Idempotency-Key"))

    def _send_error(self, repeatable: bool, method: str, exc: httpx.TransportError) -> Exception:
        # A POST that may have reached the API must not be replayed unless it is keyed; a
        # connect error means it was never sent.
        if repeatable or isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
            return _Transient(f"{method} {type(exc).__name__}")
        return CidpApiError(f"{method} {type(exc).__name__}: the request may have been processed")

    def _stale_token(self, resp: httpx.Response) -> bool:
        """A client-credentials token revoked before its expiry: drop it so the next send refetches."""
        if resp.status_code == 401 and self._auth_type() == AUTH_CLIENT_CREDENTIALS:
            self._cached_token = None
            return True
        return False

    def _retrying(self):
        return retry(
            stop=stop_after_attempt(self.retry_limit),
            wait=wait_exponential(multiplier=self.retry_delay, max=60),
            retry=retry_if_exception_type(_Transient),
            reraise=True,
        )

    def _do_api_call(
        self,
        method: str,
        endpoint: str,
        *,
        json: Any = None,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
        timeout: float | None = None,
        raw: bool = False,
    ) -> Any:
        """Call ``{base}{endpoint}``; answers the JSON body (or the response with ``raw=True``)."""
        url = f"{self._base_url()}{endpoint}"
        repeatable = self._repeatable(method, headers)
        self._check_scheme(url, "data API")

        def _send() -> httpx.Response:
            try:
                return httpx.request(
                    method,
                    url,
                    headers=self._headers(headers),
                    json=json,
                    params=params,
                    content=content,
                    timeout=timeout or self.timeout_seconds,
                    verify=self._verify(),
                    follow_redirects=False,
                )
            except httpx.TransportError as exc:
                raise self._send_error(repeatable, method, exc) from None

        @self._retrying()
        def _call():
            resp = _send()
            if self._stale_token(resp):
                resp = _send()
            return self._parse(resp, repeatable=repeatable, raw=raw)

        return _call()

    async def _a_do_api_call(
        self,
        method: str,
        endpoint: str,
        *,
        json: Any = None,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any:
        # Connection lookup and the token fetch block; keep them off the triggerer's event loop.
        url, verify = await asyncio.to_thread(lambda: (f"{self._base_url()}{endpoint}", self._verify()))
        repeatable = self._repeatable(method, headers)
        self._check_scheme(url, "data API")

        async def _send() -> httpx.Response:
            hdrs = await asyncio.to_thread(self._headers, headers)
            try:
                async with httpx.AsyncClient(
                    timeout=timeout or self.timeout_seconds, verify=verify, follow_redirects=False
                ) as client:
                    return await client.request(method, url, headers=hdrs, json=json, params=params)
            except httpx.TransportError as exc:
                raise self._send_error(repeatable, method, exc) from None

        @self._retrying()
        async def _call():
            resp = await _send()
            if self._stale_token(resp):
                resp = await _send()
            return self._parse(resp, repeatable=repeatable)

        return await _call()

    @staticmethod
    def _parse(resp: httpx.Response, *, repeatable: bool = True, raw: bool = False) -> Any:
        status = resp.status_code
        if status < 300:
            if raw:
                return resp
            return resp.json() if resp.content else {}
        request_id = resp.headers.get("x-request-id")
        code: str | None = None
        message = ""
        try:
            body = resp.json()
            if isinstance(body, dict):
                code = body.get("code") if isinstance(body.get("code"), str) else None
                message = str(body.get("message") or "")
                request_id = request_id or body.get("request_id")
        except ValueError:
            message = resp.text
        text = f"HTTP {status}" + (f" {code}" if code else "") + (f": {message[:_MAX_ERROR_TEXT]}" if message else "")
        if request_id:
            text += f" (request id {request_id})"
        kw = {"status_code": status, "code": code, "request_id": request_id}
        # 429 means "not processed"; a 5xx on an unkeyed POST may have been, so it is not replayed.
        if status == 429 or (repeatable and status >= 500):
            raise _Transient(text, **kw)
        if status in (401, 403):
            raise CidpAuthError(text, **kw)
        if status == 404:
            raise CidpNotFoundError(text, **kw)
        if status == 409:
            raise CidpConflictError(text, **kw)
        if status == 503:
            raise CidpUnavailableError(text, **kw)
        raise CidpApiError(text, **kw)

    # ---- connection form ----
    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "CIDP URL",
                "login": "Client ID (client_credentials)",
                "password": "Token or client secret",
            },
            "placeholders": {
                "host": "https://cidp.example.com",
                "login": "my-project-sp",
                "extra": '{"auth_type": "client_credentials", "workspace": "analytics"}',
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            self._do_api_call("GET", f"{self._ws()}/projects", params={"limit": 1})
        except Exception as exc:  # noqa: BLE001 — the UI shows the message
            return False, str(exc)
        return True, f"Connected to workspace {self.workspace!r}"
