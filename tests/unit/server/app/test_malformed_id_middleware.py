"""Tests for MalformedIdDiagnosticMiddleware (src/server/app/setup.py).

TEMP diagnostic (malformed-id-diag). These guard the prod-logging contract: if the
middleware silently logged nothing, the next real occurrence would teach us
nothing, so we pin that it (a) logs malformed ids with their Referer, (b) stays
silent on valid UUIDs / OPTIONS / non-http scopes, and (c) never breaks a
request even if the detector raises. Remove with the middleware.
"""

import logging

import pytest

from src.server.app.setup import MalformedIdDiagnosticMiddleware

MALFORMED_WS = "/api/v1/workspaces/my_notes.md"
VALID_WS = "/api/v1/workspaces/12345678-1234-5678-1234-567812345678"


def _scope(path, method="GET", headers=None, query=b""):
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query,
        "headers": [(k.encode(), v.encode()) for k, v in (headers or {}).items()],
    }


def _downstream():
    state = {"calls": 0}

    async def app(scope, receive, send):
        state["calls"] += 1

    return app, state


async def _noop_receive():  # pragma: no cover - unused by the middleware
    return {}


async def _noop_send(_message):  # pragma: no cover - unused by the middleware
    return None


@pytest.mark.asyncio
async def test_logs_malformed_id_with_referer(caplog):
    app, state = _downstream()
    mw = MalformedIdDiagnosticMiddleware(app)
    referer = "https://app.example.com/chat/some-workspace"
    with caplog.at_level(logging.WARNING):
        await mw(_scope(MALFORMED_WS, headers={"referer": referer}), _noop_receive, _noop_send)

    assert state["calls"] == 1  # request always passes through
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("malformed-id-diag" in m for m in warnings)
    assert any(referer in m for m in warnings)
    assert any("my_notes.md" in m for m in warnings)


@pytest.mark.asyncio
async def test_valid_uuid_is_silent(caplog):
    app, state = _downstream()
    mw = MalformedIdDiagnosticMiddleware(app)
    with caplog.at_level(logging.WARNING):
        await mw(_scope(VALID_WS), _noop_receive, _noop_send)

    assert state["calls"] == 1
    assert not [r for r in caplog.records if "malformed-id-diag" in r.getMessage()]


@pytest.mark.asyncio
async def test_options_preflight_is_skipped(caplog):
    app, state = _downstream()
    mw = MalformedIdDiagnosticMiddleware(app)
    with caplog.at_level(logging.WARNING):
        await mw(_scope(MALFORMED_WS, method="OPTIONS"), _noop_receive, _noop_send)

    assert state["calls"] == 1
    assert not [r for r in caplog.records if "malformed-id-diag" in r.getMessage()]


@pytest.mark.asyncio
async def test_non_http_scope_passes_through(caplog):
    app, state = _downstream()
    mw = MalformedIdDiagnosticMiddleware(app)
    with caplog.at_level(logging.WARNING):
        await mw({"type": "websocket", "path": MALFORMED_WS}, _noop_receive, _noop_send)
        await mw({"type": "lifespan"}, _noop_receive, _noop_send)

    assert state["calls"] == 2
    assert not [r for r in caplog.records if "malformed-id-diag" in r.getMessage()]


@pytest.mark.asyncio
async def test_detector_error_never_breaks_request(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise RuntimeError("detector exploded")

    monkeypatch.setattr("src.server.app.setup.find_malformed_route_ids", _boom)
    app, state = _downstream()
    mw = MalformedIdDiagnosticMiddleware(app)

    await mw(_scope(MALFORMED_WS), _noop_receive, _noop_send)  # must not raise
    assert state["calls"] == 1
