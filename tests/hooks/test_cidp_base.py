from __future__ import annotations

import httpx
import pytest
import respx

from airflow.providers.cidp.exceptions import (
    CidpApiError,
    CidpAuthError,
    CidpConflictError,
    CidpNotFoundError,
    CidpUnavailableError,
)
from airflow.providers.cidp.hooks.cidp import CidpHook
from airflow.providers.cidp.hooks.cidp_base import (
    CidpBaseHook,
    _seg,
    _Transient,
    _truthy,
)
from tests.conftest import API, HOST, WS

ISSUER = "https://idp.test/realms/cidp"
TOKEN_URL = f"{ISSUER}/protocol/openid-connect/token"
CC = {"auth_type": "client_credentials"}


def _discovery(router, issuer: str = ISSUER, token_url: str = TOKEN_URL):
    router.get(f"{API}/config").respond(json={"issuer": issuer})
    router.get(f"{issuer}/.well-known/openid-configuration").respond(json={"token_endpoint": token_url})


# ---- connection helpers ----
def test_retry_limit_must_be_positive():
    with pytest.raises(ValueError, match="retry_limit"):
        CidpBaseHook(retry_limit=0)


def test_seg_and_truthy():
    assert _seg("a/b?c#d") == "a%2Fb%3Fc%23d"
    assert _truthy(None, True) is True
    assert _truthy("False", True) is False
    assert _truthy(" yes ", False) is True
    assert _truthy(0, True) is False


@respx.mock
def test_token_request_headers_and_encoded_workspace(set_conn):
    set_conn({"workspace": "my ws"})
    route = respx.get(f"{API}/workspaces/my%20ws/projects").respond(json={"items": []})
    h = CidpBaseHook()
    assert h._do_api_call("GET", f"{h._ws()}/projects") == {"items": []}
    req = route.calls.last.request
    assert req.headers["Authorization"] == "Bearer tok"
    assert req.headers["User-Agent"].startswith("airflow-provider-cidp/")
    assert len(req.headers["X-Request-Id"]) == 32


@respx.mock
def test_host_without_scheme_gets_https(set_conn):
    set_conn(host="cidp.test/")
    respx.get(f"{WS}/projects").respond(json={})
    assert CidpBaseHook()._do_api_call("GET", "/workspaces/ws/projects") == {}


def test_no_host(set_conn):
    set_conn(host="")
    with pytest.raises(CidpAuthError, match="no host"):
        CidpBaseHook()._base_url()


def test_workspace_override_and_missing(set_conn):
    assert CidpBaseHook(workspace="other").workspace == "other"
    set_conn({"workspace": None})
    with pytest.raises(CidpApiError, match="no workspace"):
        CidpBaseHook().workspace  # noqa: B018


def test_api_prefix_and_ui_url(set_conn):
    set_conn({"api_prefix": "/x/v2/", "ui_url": "https://ui.test/"})
    h = CidpBaseHook()
    assert h._base_url() == f"{HOST}/x/v2"
    assert h.ui_url() == "https://ui.test"


def test_plain_http_refused_unless_allowed(set_conn):
    set_conn(host="http://localhost:8000")
    with pytest.raises(CidpAuthError, match="not https"):
        CidpBaseHook()._do_api_call("GET", "/x")
    set_conn({"allow_insecure_http": "true"}, host="http://localhost:8000")
    with respx.mock:
        respx.get("http://localhost:8000/api/data/v1/x").respond(json={"ok": 1})
        assert CidpBaseHook()._do_api_call("GET", "/x") == {"ok": 1}


def test_verify(set_conn, capsys):
    assert CidpBaseHook()._verify() is True
    set_conn({"ca_cert": "/ca.pem", "verify_ssl": False})
    assert CidpBaseHook()._verify() == "/ca.pem"
    set_conn({"verify_ssl": "false"})
    h = CidpBaseHook()
    assert h._verify() is False
    assert h._verify() is False
    assert capsys.readouterr().out.count("TLS verification is disabled") == 1


# ---- auth ----
def test_token_auth_needs_password(set_conn):
    set_conn(password=None)
    with pytest.raises(CidpAuthError, match="no password"):
        CidpBaseHook()._token()


def test_unknown_auth_type(set_conn):
    set_conn({"auth_type": "password"}, login="u")
    with pytest.raises(CidpAuthError, match="unknown auth_type"):
        CidpBaseHook()._token()


def test_client_credentials_needs_login(set_conn):
    set_conn(CC)
    with pytest.raises(CidpAuthError, match="client id"):
        CidpBaseHook()._token()


def test_service_account_is_refused(set_conn):
    set_conn(CC, login="portal-data-svc")
    with pytest.raises(CidpAuthError, match="project service principal"):
        CidpBaseHook()._token()


@respx.mock
def test_client_credentials_discovers_and_caches(set_conn):
    set_conn(CC, login="sp", password="secret")
    _discovery(respx)
    token = respx.post(TOKEN_URL).respond(json={"access_token": "at", "expires_in": 600})
    api = respx.get(f"{WS}/projects").respond(json={})
    h = CidpBaseHook()
    h._do_api_call("GET", "/workspaces/ws/projects")
    h._do_api_call("GET", "/workspaces/ws/projects")
    assert token.call_count == 1
    form = dict(httpx.QueryParams(token.calls.last.request.content.decode()))
    assert form == {"grant_type": "client_credentials", "client_id": "sp", "client_secret": "secret"}
    assert api.calls.last.request.headers["Authorization"] == "Bearer at"


@respx.mock
def test_issuer_extra_skips_config(set_conn):
    set_conn({**CC, "issuer": ISSUER + "/"}, login="sp")
    config = respx.get(f"{API}/config")
    respx.get(f"{ISSUER}/.well-known/openid-configuration").respond(json={"token_endpoint": TOKEN_URL})
    respx.post(TOKEN_URL).respond(json={"access_token": "at"})
    h = CidpBaseHook()
    assert h._token() == "at"
    assert h._discover_token_endpoint() == TOKEN_URL
    assert not config.called


@respx.mock
@pytest.mark.parametrize(
    ("config", "disc", "match"),
    [
        (httpx.Response(500), None, "/config answered HTTP 500"),
        (httpx.Response(200, json={}), None, "names no OIDC issuer"),
        (httpx.Response(200, json={"issuer": "http://idp.test"}), None, "issuer is not https"),
        (httpx.Response(200, json={"issuer": ISSUER}), httpx.Response(404), "discovery document answered"),
        (httpx.Response(200, json={"issuer": ISSUER}), httpx.Response(200, json={}), "no token_endpoint"),
        (
            httpx.Response(200, json={"issuer": ISSUER}),
            httpx.Response(200, json={"token_endpoint": "http://idp.test/token"}),
            "token endpoint is not https",
        ),
        (httpx.Response(200, text="<html>"), None, "OIDC discovery failed"),
        (httpx.ConnectError("x"), None, "OIDC discovery failed"),
    ],
)
def test_discovery_failures(set_conn, config, disc, match):
    set_conn(CC, login="sp")
    route = respx.get(f"{API}/config")
    if isinstance(config, Exception):
        route.side_effect = config
    else:
        route.return_value = config
    if disc is not None:
        respx.get(f"{ISSUER}/.well-known/openid-configuration").mock(return_value=disc)
    with pytest.raises(CidpAuthError, match=match):
        CidpBaseHook()._token()


@respx.mock
@pytest.mark.parametrize(
    ("resp", "match"),
    [
        (httpx.Response(401, json={"error": "invalid_client", "error_description": "secret s3cr3t"}), "invalid_client"),
        (httpx.Response(500, text="boom"), "HTTP 500"),
        (httpx.Response(200, json={"token_type": "bearer"}), "HTTP 200"),
        (httpx.Response(200, json=["x"]), "HTTP 200"),
    ],
)
def test_token_endpoint_refusals(set_conn, resp, match):
    set_conn(CC, login="sp")
    _discovery(respx)
    respx.post(TOKEN_URL).mock(return_value=resp)
    with pytest.raises(CidpAuthError, match=match) as err:
        CidpBaseHook()._token()
    assert "s3cr3t" not in str(err.value)


@respx.mock
def test_token_endpoint_unreachable(set_conn):
    set_conn(CC, login="sp")
    _discovery(respx)
    respx.post(TOKEN_URL).side_effect = httpx.ConnectError("x")
    with pytest.raises(CidpAuthError, match="did not answer"):
        CidpBaseHook()._token()


@respx.mock
def test_401_refetches_client_credentials_token_once(set_conn):
    set_conn(CC, login="sp")
    _discovery(respx)
    token = respx.post(TOKEN_URL)
    token.side_effect = [
        httpx.Response(200, json={"access_token": "old"}),
        httpx.Response(200, json={"access_token": "new"}),
    ]
    api = respx.get(f"{WS}/projects")
    api.side_effect = [httpx.Response(401), httpx.Response(200, json={"ok": True})]
    assert CidpBaseHook()._do_api_call("GET", "/workspaces/ws/projects") == {"ok": True}
    assert [c.request.headers["Authorization"] for c in api.calls] == ["Bearer old", "Bearer new"]


@respx.mock
def test_401_with_static_token_is_an_auth_error(hook):
    respx.get(f"{WS}/projects").respond(401, json={"code": "unauthenticated", "message": "expired"})
    with pytest.raises(CidpAuthError, match="HTTP 401 unauthenticated: expired"):
        hook._do_api_call("GET", "/workspaces/ws/projects")


# ---- retries and errors ----
@respx.mock
def test_get_retries_5xx_then_succeeds(hook):
    route = respx.get(f"{WS}/x")
    route.side_effect = [httpx.Response(502), httpx.Response(503), httpx.Response(200, json={"ok": 1})]
    assert hook._do_api_call("GET", "/workspaces/ws/x") == {"ok": 1}
    assert route.call_count == 3


@respx.mock
def test_retries_stop_at_the_limit(hook):
    route = respx.get(f"{WS}/x").respond(503)
    with pytest.raises(_Transient):
        hook._do_api_call("GET", "/workspaces/ws/x")
    assert route.call_count == 3


@respx.mock
def test_unkeyed_post_5xx_is_not_replayed(hook):
    route = respx.post(f"{WS}/x").respond(500, json={"code": "internal", "message": "x" * 900})
    with pytest.raises(CidpApiError) as err:
        hook._do_api_call("POST", "/workspaces/ws/x")
    assert route.call_count == 1
    assert not isinstance(err.value, _Transient)
    assert len(str(err.value)) < 600
    assert err.value.status_code == 500


@respx.mock
def test_keyed_post_5xx_and_429_are_replayed(hook):
    route = respx.post(f"{WS}/x")
    route.side_effect = [httpx.Response(502), httpx.Response(429), httpx.Response(201, json={"id": 1})]
    assert hook._do_api_call("POST", "/workspaces/ws/x", headers={"Idempotency-Key": "k"}) == {"id": 1}
    assert {c.request.headers["Idempotency-Key"] for c in route.calls} == {"k"}


@respx.mock
def test_unkeyed_post_429_is_replayed(hook):
    route = respx.post(f"{WS}/x")
    route.side_effect = [httpx.Response(429), httpx.Response(200, json={})]
    assert hook._do_api_call("POST", "/workspaces/ws/x") == {}


@respx.mock
def test_transport_errors(hook):
    respx.get(f"{WS}/g").side_effect = httpx.ReadTimeout("x")
    with pytest.raises(_Transient, match="ReadTimeout"):
        hook._do_api_call("GET", "/workspaces/ws/g")
    post = respx.post(f"{WS}/p")
    post.side_effect = httpx.ReadTimeout("x")
    with pytest.raises(CidpApiError, match="may have been processed"):
        hook._do_api_call("POST", "/workspaces/ws/p")
    assert post.call_count == 1
    connect = respx.post(f"{WS}/c")
    connect.side_effect = [httpx.ConnectError("x"), httpx.Response(200, json={})]
    assert hook._do_api_call("POST", "/workspaces/ws/c") == {}


@respx.mock
@pytest.mark.parametrize(
    ("status", "exc"),
    [
        (400, CidpApiError),
        (403, CidpAuthError),
        (404, CidpNotFoundError),
        (409, CidpConflictError),
        (503, CidpUnavailableError),
    ],
)
def test_error_mapping(hook, status, exc):
    respx.post(f"{WS}/x").respond(status, json={"code": "c", "message": "m", "request_id": "body-id"})
    with pytest.raises(exc) as err:
        hook._do_api_call("POST", "/workspaces/ws/x")
    assert type(err.value) is exc
    assert (err.value.code, err.value.request_id) == ("c", "body-id")
    assert "(request id body-id)" in str(err.value)


@respx.mock
def test_error_text_body_and_header_request_id(hook):
    respx.post(f"{WS}/x").respond(400, text="bad gateway page", headers={"x-request-id": "hdr"})
    with pytest.raises(CidpApiError, match=r"HTTP 400: bad gateway page \(request id hdr\)"):
        hook._do_api_call("POST", "/workspaces/ws/x")
    respx.post(f"{WS}/y").respond(400, json=[1])
    with pytest.raises(CidpApiError, match=r"^HTTP 400$"):
        hook._do_api_call("POST", "/workspaces/ws/y")


@respx.mock
def test_empty_and_raw_answers(hook):
    respx.delete(f"{WS}/x").respond(204)
    assert hook._do_api_call("DELETE", "/workspaces/ws/x") == {}
    respx.get(f"{WS}/r").respond(200, content=b"bytes")
    assert hook._do_api_call("GET", "/workspaces/ws/r", raw=True).content == b"bytes"


@respx.mock
def test_redirects_are_not_followed(hook):
    respx.get(f"{WS}/x").respond(302, headers={"location": "https://evil.test/"})
    with pytest.raises(CidpApiError, match="HTTP 302"):
        hook._do_api_call("GET", "/workspaces/ws/x")


# ---- async ----
@respx.mock
async def test_async_call_and_refetch(set_conn):
    set_conn(CC, login="sp")
    _discovery(respx)
    token = respx.post(TOKEN_URL)
    token.side_effect = [
        httpx.Response(200, json={"access_token": "old"}),
        httpx.Response(200, json={"access_token": "new"}),
    ]
    api = respx.get(f"{WS}/projects/p/runs/r")
    api.side_effect = [httpx.Response(401), httpx.Response(502), httpx.Response(200, json={"status": "running"})]
    h = CidpHook(retry_delay=0)
    assert await h.a_get_project_run("p", "r") == {"status": "running"}
    assert api.calls.last.request.headers["Authorization"] == "Bearer new"


@respx.mock
async def test_async_transport_error(hook):
    respx.post(f"{WS}/statements/s/cancel").side_effect = httpx.ReadError("x")
    with pytest.raises(CidpApiError, match="may have been processed"):
        await hook.a_cancel_sql_statement("s")


# ---- connection form ----
def test_ui_field_behaviour():
    ui = CidpBaseHook.get_ui_field_behaviour()
    assert ui["hidden_fields"] == ["schema", "port"]
    assert "CIDP URL" in ui["relabeling"].values()


@respx.mock
def test_test_connection(hook):
    route = respx.get(f"{WS}/projects").respond(json={"items": []})
    assert hook.test_connection() == (True, "Connected to workspace 'ws'")
    assert route.calls.last.request.url.params["limit"] == "1"
    route.respond(403, json={"message": "no"})
    ok, msg = hook.test_connection()
    assert not ok
    assert "403" in msg
