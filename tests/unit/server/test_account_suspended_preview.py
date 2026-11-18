"""The preview proxy, an app of its own the router-table test never sees, ends
a suspended owner's previews at each of its entry points: a request, a
socket, and a stream or socket already open. Nothing more is read with the
``teams`` flag off."""

from __future__ import annotations

import asyncio
import inspect
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.preview_proxy import app as proxy_app
from src.server.preview_proxy import artifacts as proxy_artifacts
from src.server.preview_proxy import ws as proxy_ws
from src.server.preview_proxy.app import PreviewProxy
from src.server.services import preview_capabilities as caps
from src.server.services.account_contract import NO_ORG
from src.server.services.preview_base import PreviewBase
from tests.unit.server.test_account_suspended_work import SUSPENDED, _account

TOKEN = "a" * 32
HOST = f"8080-{TOKEN}.preview.example.com"
BASE = PreviewBase("https", "preview.example.com")


def _record(**more):
    return {
        "v": 2,
        "sandbox_id": "sb-1",
        "port": 8080,
        "upstream": "https://8080-sb-1.e2b.app",
        "workspace_id": "ws-1",
        "generation": 7,
        "expires_at": int(time.time()) + 600,
        "headers": {},
        **more,
    }


def _redis(record):
    cache = MagicMock(get_strict=AsyncMock(return_value=record))
    generation = MagicMock(get=AsyncMock(return_value=b"7"))
    return (
        patch.object(caps, "get_cache_client", return_value=cache),
        patch.object(caps, "_redis", return_value=generation),
    )


# --- coverage over the proxy's entry points ------------------------------------------


def test_every_entry_point_of_the_proxy_asks_whether_a_capability_still_serves():
    """A capability reaches the proxy only through ``resolve_host`` (a request
    and a socket alike) and is watched only through ``until_revoked`` (a
    stream and a socket left open), and both ask ``serves``, which ends a
    suspended owner's capability as a revocation does. A new entry that read
    the revocation counter itself would skip that, so none may."""
    proxy_sources = inspect.getsource(proxy_app) + inspect.getsource(proxy_ws)
    assert "is_current" not in proxy_sources
    assert "resolve(" not in proxy_sources.replace("resolve_host(", "")
    assert "serves(capability)" in inspect.getsource(caps.resolve)
    assert "serves(capability)" in inspect.getsource(caps.until_revoked)
    # The artifact origin is the other entry; an owner's grant there ends with
    # the account (``test_account_suspended_gate``), a visitor's is the
    # organization's published content under its public_links policy.
    assert "accounts.read_standing(stored.user_id" in inspect.getsource(proxy_artifacts._owner_file)


async def _request(proxy, scope_type="http"):
    sent: list[dict] = []
    scope = {
        "type": scope_type,
        "method": "GET",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [(b"host", HOST.encode())],
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    try:
        await proxy(scope, receive, send)
    finally:
        await proxy.aclose()
    return sent


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_type", ["http", "websocket"])
async def test_a_suspended_owners_preview_answers_as_a_revoked_one(scope_type):
    dial = AsyncMock()
    stack, _ = _account(SUSPENDED)
    cache, generation = _redis(_record(owner_id="u"))
    with stack, cache, generation, patch.object(PreviewProxy, "_dial", dial):
        sent = await _request(PreviewProxy(BASE), scope_type)
    dial.assert_not_awaited()
    if scope_type == "http":
        assert sent[0]["status"] == 404
    else:
        assert sent == [{"type": "websocket.close", "code": 1008}]


@pytest.mark.asyncio
async def test_an_active_owners_preview_is_served():
    dial = AsyncMock()
    stack, _ = _account(NO_ORG)
    cache, generation = _redis(_record(owner_id="u"))
    with stack, cache, generation, patch.object(PreviewProxy, "_dial", dial):
        await _request(PreviewProxy(BASE))
    dial.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_capability_minted_before_carries_no_owner_and_reads_the_workspaces():
    stack, read = _account(SUSPENDED)
    cache, generation = _redis(_record())
    workspace = AsyncMock(return_value={"user_id": "u"})
    with stack, cache, generation, patch(
        "src.server.database.workspace.get_workspace", workspace
    ):
        assert await caps.resolve(TOKEN) is None
    workspace.assert_awaited_once_with("ws-1")
    read.assert_awaited_once_with("u", unknown_on_cache_error=True)


@pytest.mark.asyncio
async def test_with_the_flag_off_nothing_more_is_read():
    stack, read = _account(SUSPENDED, door=False)
    cache, generation = _redis(_record())
    workspace = AsyncMock()
    with stack, cache, generation, patch(
        "src.server.database.workspace.get_workspace", workspace
    ):
        assert await caps.resolve(TOKEN) is not None
    workspace.assert_not_awaited()
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_open_stream_or_socket_ends_once_its_owner_is_suspended():
    capability = caps.Capability.from_record(_record(owner_id="u"))
    stack, read = _account(NO_ORG)
    read.side_effect = [NO_ORG, SUSPENDED]
    _, generation = _redis(None)
    with stack, generation, patch.object(caps, "RECHECK_S", 0):
        await asyncio.wait_for(caps.until_revoked(capability), 1)
    assert read.await_count == 2


@pytest.mark.asyncio
async def test_a_new_capability_keeps_its_owner_only_with_the_flag_on():
    from ptc_agent.core.sandbox.runtime import PreviewInfo

    info = PreviewInfo(url="https://8080-sb-1.e2b.app", token=None, browser_ready=False)
    for door, owner in ((True, "u"), (False, None)):
        stored = MagicMock(set=AsyncMock(return_value=True))
        pipe = MagicMock(execute=AsyncMock(return_value=[True, True, b"7"]))
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=False)
        client = MagicMock(pipeline=MagicMock(return_value=pipe))
        workspace = AsyncMock(return_value={"user_id": "u"})
        with patch(
            "src.server.services.membership.teams_door_open", return_value=door
        ), patch.object(caps, "_redis", return_value=client), patch.object(
            caps, "get_cache_client", return_value=stored
        ), patch("src.server.database.workspace.get_workspace", workspace):
            await caps.mint(
                info, sandbox_id="sb-1", port=8080, workspace_id="ws-1",
                expires_in=600, base=BASE,
            )
        record = stored.set.await_args.args[1]
        assert record.get("owner_id") == owner
        assert workspace.await_count == (1 if door else 0)
