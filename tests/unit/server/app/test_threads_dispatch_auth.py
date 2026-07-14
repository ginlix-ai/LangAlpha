"""Dispatch auth on ``X-Dispatch: background`` (``is_internal`` in threads.py).

The header is honoured only for authenticated internal service calls: a
matching ``X-Service-Token``, or oss mode with no ``INTERNAL_SERVICE_TOKEN``
configured (nothing to authenticate against — the self-dispatch is trusted).
Anything else is rejected with 403 rather than silently downgraded to a
foreground SSE run that burns credits for an ack the caller can't parse.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.agent import CredentialSource
from tests.conftest import create_test_app

USER = "usr-dispatch-001"
TOKEN = "test-internal-service-token"

_BODY = {
    "workspace_id": "ws-placeholder",
    "messages": [{"role": "user", "content": "test query"}],
    "agent_mode": "ptc",
}


def _make_config():
    """Stub ``AgentConfig`` returned by ``resolve_llm_config`` (platform key)."""
    cfg = MagicMock()
    cfg.credential_source = CredentialSource.PLATFORM
    cfg.llm_client = None
    cfg.llm = MagicMock()
    cfg.llm.name = "claude-sonnet-placeholder"
    return cfg


def _stub_ptc_workflow(**kwargs):
    """Stand-in for ``astream_ptc_workflow``: a dispatched generator yields the
    post-START marker first (v4 2.4c — the handler primes to it before its
    202), a foreground one streams nothing."""

    async def _gen():
        if kwargs.get("dispatched"):
            from src.server.handlers.chat._common import DISPATCH_STARTED_MARKER

            yield DISPATCH_STARTED_MARKER

    return _gen()


def _unset_token(monkeypatch):
    # setenv("") rather than delenv: a lazy import during the request may
    # re-run load_dotenv(), which repopulates a deleted var from .env but
    # never overrides an existing one. Empty and unset are equivalent here.
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "")


def _app():
    from src.server.app.threads import router
    from src.server.dependencies.usage_limits import ChatAuthResult, enforce_chat_limit

    app = create_test_app(router)
    app.dependency_overrides[enforce_chat_limit] = lambda: ChatAuthResult(
        user_id=USER, access_tier=0
    )
    return app


@contextmanager
def _stub_workflow(release_burst=None, mark_active=None):
    """Patch everything past the dispatch-auth check, for both branches.

    The dispatched branch's admission/tracking singletons are mocked so a
    background dispatch returns its JSON ack without real Redis/task work.
    Yields the singleton mocks for call-shape assertions.
    """
    from src.server.app import setup as setup_module

    wm = MagicMock()
    wm.has_ready_session.return_value = True

    btm = MagicMock()
    btm.get_admission_lock = AsyncMock(return_value=asyncio.Lock())
    btm.wait_for_admission = AsyncMock(return_value=("fresh", None))
    btm.pre_register = AsyncMock()

    tracker = MagicMock()
    tracker.mark_active = mark_active if mark_active is not None else AsyncMock()
    tracker.mark_failed = AsyncMock()

    with (
        patch(
            "src.server.app.threads.get_thread_owner_id",
            new=AsyncMock(return_value=USER),
        ),
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(return_value={"user_id": USER, "status": "running"}),
        ),
        patch.object(setup_module, "agent_config", MagicMock()),
        patch(
            "src.server.handlers.chat.resolve_llm_config",
            new=AsyncMock(return_value=_make_config()),
        ),
        patch(
            "src.server.dependencies.usage_limits.enforce_credit_limit",
            new=AsyncMock(),
        ),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=wm,
        ),
        patch(
            "src.server.handlers.chat.astream_ptc_workflow",
            new=_stub_ptc_workflow,
        ),
        patch(
            "src.server.app.threads.observe_chat_stream",
            side_effect=lambda gen, **_: gen,
        ),
        patch(
            "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
            return_value=btm,
        ),
        patch(
            "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
            return_value=tracker,
        ),
        patch("src.server.app.threads._consume_background_gen", new=AsyncMock()),
        patch(
            "src.server.app.threads._assert_stream_transport_ready",
            new=AsyncMock(),
        ),
        # Sync passthrough (patch() would auto-create an AsyncMock for this
        # async function, whose wrapper coroutine returns — never awaits —
        # the inner one, leaking it).
        patch(
            "src.server.app.threads.observe_background_chat_turn",
            new=lambda coro, **_: coro,
        ),
        patch(
            "src.server.dependencies.usage_limits.release_burst_slot",
            new=release_burst if release_burst is not None else AsyncMock(),
        ),
    ):
        yield SimpleNamespace(btm=btm, tracker=tracker)


async def _post(app, *, headers=None, body=None):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        resp = await c.post(
            "/api/v1/threads/tid-dispatch/messages",
            json=body or _BODY,
            headers=headers or {},
        )
    # Drain the dispatched branch's fire-and-forget task so it finishes while
    # the patches (and the test's event loop) are still alive.
    for _ in range(3):
        await asyncio.sleep(0)
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oss_no_token_background_dispatch_returns_ack(monkeypatch):
    """The oss zero-config promise: no token configured, self-dispatch trusted."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    _unset_token(monkeypatch)
    app = _app()
    with _stub_workflow():
        resp = await _post(app, headers={"X-Dispatch": "background"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "dispatched"


@pytest.mark.asyncio
async def test_oss_matching_token_dispatches(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()
    with _stub_workflow():
        resp = await _post(
            app, headers={"X-Dispatch": "background", "X-Service-Token": TOKEN}
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dispatched"


@pytest.mark.asyncio
async def test_oss_configured_token_still_enforced(monkeypatch):
    """A token set in oss mode must be honoured: request without it -> 403."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()
    with _stub_workflow():
        resp = await _post(app, headers={"X-Dispatch": "background"})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_platform_matching_token_dispatches(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()
    with _stub_workflow():
        resp = await _post(
            app, headers={"X-Dispatch": "background", "X-Service-Token": TOKEN}
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dispatched"


@pytest.mark.asyncio
async def test_platform_stray_dispatch_header_is_rejected(monkeypatch):
    """No service token configured -> 403, and the burst slot is released.

    Repins test_threads_authz.py's former internal-dispatch case, which
    asserted the silent downgrade to a foreground 200.
    """
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    _unset_token(monkeypatch)
    release = AsyncMock()
    app = _app()
    with _stub_workflow(release_burst=release):
        resp = await _post(app, headers={"X-Dispatch": "background"})
    assert resp.status_code == 403
    assert "INTERNAL_SERVICE_TOKEN" in resp.json()["detail"]
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_admission_marker_write_aborts_dispatch(monkeypatch):
    """Codex round-9 F1: ``mark_active`` returning False (a swallowed Redis
    write failure) must abort with 503 BEFORE pre_register/create_task — a
    scheduled run with no marker would falsify the dispatcher's gen-scoped
    admission oracle."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    release = AsyncMock()
    app = _app()
    with _stub_workflow(
        release_burst=release, mark_active=AsyncMock(return_value=False)
    ) as stubs:
        resp = await _post(
            app, headers={"X-Dispatch": "background", "X-Service-Token": TOKEN}
        )
    assert resp.status_code == 503
    stubs.btm.pre_register.assert_not_awaited()
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_gen_threads_receipt_gate_into_admission(monkeypatch):
    """Codex round-13 P0: a gen-carrying dispatch admits through the
    receipt-gated marker write (refuse key + its own generation as member),
    so a generation the orphan resolver already receipted as phantom can
    never admit late; a gen-less dispatch has nothing to receipt against
    and skips the gate."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()
    with _stub_workflow() as stubs:
        resp = await _post(
            app,
            body={
                **_BODY,
                "origin_flash_thread_id": "flash-1",
                "origin_dispatch_gen": "g-1",
            },
            headers={"X-Dispatch": "background", "X-Service-Token": TOKEN},
        )
    assert resp.status_code == 200
    kwargs = stubs.tracker.mark_active.await_args.kwargs
    assert kwargs["refuse_receipt_key"] == "ptc_rb_resolved:tid-dispatch"
    assert kwargs["receipt_member"] == "g-1"

    app = _app()
    with _stub_workflow() as stubs:
        resp = await _post(
            app, headers={"X-Dispatch": "background", "X-Service-Token": TOKEN}
        )
    assert resp.status_code == 200
    kwargs = stubs.tracker.mark_active.await_args.kwargs
    assert kwargs["refuse_receipt_key"] is None
    assert kwargs["receipt_member"] is None


@pytest.mark.asyncio
async def test_dispatch_retransmit_race_adopts_existing_run(monkeypatch):
    """A retransmit that slips past the route-level dedup probe (the first
    copy's START had not committed at probe time) surfaces in-generator as
    DuplicateRequestError during priming; the dispatch handler adopts the
    existing run in its ack instead of orphaning the caller (v4 2.4c)."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()

    def _dup_gen(**kwargs):
        async def _gen():
            from src.server.database.turn_lifecycle import DuplicateRequestError

            raise DuplicateRequestError(
                {"conversation_response_id": "run-existing"}
            )
            yield  # unreachable; makes this a generator function

        return _gen()

    with _stub_workflow() as stubs, patch(
        "src.server.handlers.chat.astream_ptc_workflow", new=_dup_gen
    ):
        resp = await _post(
            app, headers={"X-Dispatch": "background", "X-Service-Token": TOKEN}
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "dispatched"
    assert body["run_id"] == "run-existing"
    # The loser's own run never schedules — no placeholder, marker cleaned.
    stubs.btm.pre_register.assert_not_awaited()
    stubs.tracker.mark_failed.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_dispatch_header_foreground_unchanged(monkeypatch):
    """Without X-Dispatch nothing changes: plain foreground SSE."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    _unset_token(monkeypatch)
    app = _app()
    with _stub_workflow():
        resp = await _post(app)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")


@pytest.mark.asyncio
async def test_non_ascii_service_token_rejected_not_500(monkeypatch):
    """A non-ASCII X-Service-Token must 403, not 500. Header values arrive
    latin-1-decoded, and the compare now runs through service_token_matches
    (bytes), so a high byte can't raise TypeError out of hmac.compare_digest."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    app = _app()
    with _stub_workflow():
        # Raw latin-1 byte 0xE5 → Starlette decodes it to a non-ASCII str.
        resp = await _post(
            app,
            headers={"X-Dispatch": "background", "X-Service-Token": b"\xe5-not-ascii"},
        )
    assert resp.status_code == 403
