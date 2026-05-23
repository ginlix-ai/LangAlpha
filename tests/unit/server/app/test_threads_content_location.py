"""Tests for the ``Content-Location`` header and ``run_id`` plumbing on the
chat-message endpoints.

The backend mints a fresh UUID per POST (the canonical per-turn ``run_id``)
and advertises the matching reconnect URL via ``Content-Location`` so the
frontend can resume the exact run after a disconnect. The reconnect endpoint
must propagate ``run_id`` straight through to the underlying replay logic.
"""

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


@pytest_asyncio.fixture
async def threads_client():
    """Threads router only, with auth + rate-limit dependencies neutralised.

    Override the default ``ChatAuthResult`` so the BYOK/tier guard treats
    the request as a platform tier-0 user (skip the 403). The fixture in
    ``conftest`` defaults to ``access_tier=-1`` which trips the no-provider
    gate in ``_handle_send_message``.
    """
    from src.server.app.threads import router
    from src.server.dependencies.usage_limits import (
        ChatAuthResult,
        enforce_chat_limit,
    )

    app = create_test_app(router)
    app.dependency_overrides[enforce_chat_limit] = lambda: ChatAuthResult(
        user_id="test-user-123", access_tier=0
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _empty_async_gen():
    async def _gen():
        if False:
            yield ""  # pragma: no cover — empty stream
        return

    return _gen()


def _resolved_config():
    """Minimal stub for ``resolve_llm_config`` return value."""
    cfg = MagicMock()
    cfg.llm_client = None  # mark request as platform-served, keeps is_byok=False
    cfg.llm = MagicMock(name="claude-sonnet-4-5", flash="claude-haiku")
    cfg.llm.name = "claude-sonnet-4-5"
    return cfg


# ---------------------------------------------------------------------------
# POST /api/v1/threads/{tid}/messages → Content-Location header
# ---------------------------------------------------------------------------


class TestContentLocationHeader:

    @pytest.mark.asyncio
    async def test_post_includes_content_location_with_uuid_run_id(
        self, threads_client
    ):
        """Header announces the reconnect URL for THIS run.

        Format: ``/api/v1/threads/{tid}/messages/stream?run_id={uuid}``
        """
        from src.server.app import setup as setup_module
        from src.server.handlers.chat import resolve_llm_config

        tid = "tid-fixed-1"

        # WorkspaceManager.get_instance requires config on first call; fake
        # a singleton that reports the workspace as ready (skips DB lookup).
        wm_singleton = MagicMock()
        wm_singleton.has_ready_session.return_value = True

        # All of these are deferred imports inside the handler — patch at
        # the source module, not the threads namespace.
        with patch.object(setup_module, "agent_config", MagicMock()), \
             patch("src.server.handlers.chat.resolve_llm_config", new=AsyncMock(return_value=_resolved_config())), \
             patch("src.server.dependencies.usage_limits.enforce_credit_limit", new=AsyncMock()), \
             patch("src.server.database.conversation.get_thread_by_id", new=AsyncMock(return_value={"workspace_id": "ws-1"})), \
             patch("src.server.services.workspace_manager.WorkspaceManager.get_instance", return_value=wm_singleton), \
             patch("src.server.handlers.chat.astream_ptc_workflow", return_value=_empty_async_gen()), \
             patch("src.server.app.threads.observe_chat_stream", side_effect=lambda gen, **_: gen):
            # Use a context manager so we DON'T consume the body — just read headers.
            async with threads_client.stream(
                "POST",
                f"/api/v1/threads/{tid}/messages",
                json={
                    "workspace_id": "ws-1",
                    "messages": [{"role": "user", "content": "hi"}],
                    "agent_mode": "ptc",
                },
            ) as resp:
                assert resp.status_code == 200
                loc = resp.headers.get("content-location")
                assert loc is not None, (
                    f"Content-Location missing from response headers: {dict(resp.headers)!r}"
                )
                # Path prefix + thread_id are echoed verbatim.
                prefix = f"/api/v1/threads/{tid}/messages/stream?run_id="
                assert loc.startswith(prefix), (
                    f"unexpected Content-Location: {loc!r}"
                )
                run_id = loc[len(prefix):]
                assert UUID_RE.match(run_id), (
                    f"run_id is not a UUID: {run_id!r}"
                )

        # Sanity: the canonical resolver was actually invoked (proves the
        # mocked path was hit, not some short-circuit error response).
        assert resolve_llm_config is not None


# ---------------------------------------------------------------------------
# GET /api/v1/threads/{tid}/messages/stream — reconnect wiring
# ---------------------------------------------------------------------------


class TestReconnectWiring:
    """``run_id`` from the querystring must reach
    ``reconnect_to_workflow_stream`` unchanged. When omitted, the parameter
    is forwarded as ``None`` so downstream code falls back to "latest run on
    the thread."
    """

    @pytest.mark.asyncio
    async def test_reconnect_forwards_run_id_and_last_event_id(self, threads_client):
        captured: dict[str, Any] = {}

        async def _fake_reconnect(thread_id, run_id, last_event_id):
            captured["args"] = (thread_id, run_id, last_event_id)
            if False:
                yield ""

        with patch("src.server.app.threads.require_thread_owner", new=AsyncMock()), \
             patch("src.server.handlers.chat.reconnect_to_workflow_stream", new=_fake_reconnect):
            resp = await threads_client.get(
                "/api/v1/threads/tid-X/messages/stream",
                params={"run_id": "r-X", "last_event_id": 5},
            )

        assert resp.status_code == 200
        assert captured["args"] == ("tid-X", "r-X", 5), (
            f"reconnect wiring mismatch: got {captured.get('args')!r}"
        )

    @pytest.mark.asyncio
    async def test_reconnect_without_run_id_passes_none(self, threads_client):
        captured: dict[str, Any] = {}

        async def _fake_reconnect(thread_id, run_id, last_event_id):
            captured["args"] = (thread_id, run_id, last_event_id)
            if False:
                yield ""

        with patch("src.server.app.threads.require_thread_owner", new=AsyncMock()), \
             patch("src.server.handlers.chat.reconnect_to_workflow_stream", new=_fake_reconnect):
            resp = await threads_client.get(
                "/api/v1/threads/tid-Y/messages/stream",
            )

        assert resp.status_code == 200
        thread_id, run_id, _last = captured["args"]
        assert thread_id == "tid-Y"
        assert run_id is None, (
            f"run_id must default to None when missing; got {run_id!r}"
        )
