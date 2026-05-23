"""SSE workspace_status contract for astream_ptc_workflow.

The cold-start block in ``ptc_workflow.py`` emits one to three
``workspace_status`` SSE events depending on the sandbox state observed
by ``PTCSandbox.reconnect`` via the ``on_state_observed`` callback.
This test pins the wire contract — any rewrite that changes event
order, payload shape, or emission count will fail here.

Architecture note: the generator does NOT probe Daytona itself. It
passes a callback into ``workspace_manager.get_session_for_workspace``
which threads it down to ``PTCSandbox.reconnect``. The test exercises
this by mocking ``get_session_for_workspace`` to invoke the callback
with a specific state before returning a session, mirroring what the
real reconnect path does.
"""

from __future__ import annotations

import asyncio
import json
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


PTC = "src.server.handlers.chat.ptc_workflow"


def _parse_ws_status_events(sse_lines: list[str]) -> list[dict]:
    events: list[dict] = []
    for raw in sse_lines:
        if "event: workspace_status" not in raw:
            continue
        match = re.search(r"^data: (.+)$", raw, re.MULTILINE)
        if not match:
            continue
        events.append(json.loads(match.group(1)))
    return events


class _SentinelStop(Exception):
    """Raised from a mocked dep to break out of the generator after the
    workspace_status block has fully emitted its events."""


def _make_request():
    req = MagicMock()
    req.workspace_id = "ws-1"
    req.additional_context = None
    req.hitl_response = None
    req.checkpoint_id = None
    req.messages = [MagicMock(role="user", content="hi")]
    req.plan_mode = False
    req.timezone = "UTC"
    req.locale = "en-US"
    req.subagents_enabled = None
    req.llm_model = None
    return req


def _make_config():
    cfg = MagicMock()
    cfg.llm.name = "claude-test"
    cfg.subagents.enabled = False
    return cfg


def _make_workspace_manager(
    has_ready: bool,
    observed_state: str | None,
    session_delay_s: float = 0.02,
    fire_callback_delay_s: float = 0.005,
):
    """Build a WorkspaceManager mock whose get_session_for_workspace fires
    ``on_state_observed`` with ``observed_state`` after a short delay,
    then returns a Session mock.

    Pass ``observed_state=None`` to simulate the recovery path where the
    reconnect callback is never invoked (new sandbox, no pre-existing
    state to observe).
    """
    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=has_ready)

    async def _session_with_callback(
        workspace_id, *, user_id=None, on_state_observed=None
    ):
        # Fire the callback asynchronously, as the real reconnect does.
        if observed_state is not None and on_state_observed is not None:
            await asyncio.sleep(fire_callback_delay_s)
            on_state_observed(observed_state)
        # Then simulate the rest of session init work.
        await asyncio.sleep(session_delay_s)
        return MagicMock(name="Session")

    wm.get_session_for_workspace = AsyncMock(side_effect=_session_with_callback)
    return wm


async def _run_to_sentinel(request, workspace_manager):
    from src.server.handlers.chat.ptc_workflow import astream_ptc_workflow

    sentinel_registry_store = MagicMock()
    sentinel_registry_store.get_or_create_registry = AsyncMock(
        side_effect=_SentinelStop("stop after workspace_status block")
    )

    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.ensure_thread", new_callable=AsyncMock),
        patch(
            f"{PTC}._setup_fork_and_persistence",
            new_callable=AsyncMock,
            return_value=("Q", False, MagicMock()),
        ),
        patch(f"{PTC}.persist_or_skip_replay", new_callable=AsyncMock),
        patch(f"{PTC}._resolve_timezone", return_value="UTC"),
        patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())),
        patch(f"{PTC}.apply_fetch_override"),
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(f"{PTC}._fire_and_forget"),
        patch(f"{PTC}.update_workspace_activity"),
        patch(f"{PTC}.BackgroundRegistryStore") as mock_reg_store_cls,
        patch(f"{PTC}.ExecutionTracker"),
    ):
        mock_setup.agent_config = MagicMock()
        mock_wm_cls.get_instance.return_value = workspace_manager
        mock_reg_store_cls.get_instance.return_value = sentinel_registry_store

        gen = astream_ptc_workflow(
            request=request,
            thread_id="t-1",
            run_id="r-1",
            user_input="hi",
            user_id="u-1",
            workspace_id="ws-1",
            is_byok=False,
            config=_make_config(),
        )

        collected: list[str] = []
        try:
            async for event in gen:
                collected.append(event)
        except Exception:
            pass
        finally:
            await gen.aclose()
        return collected


@pytest.mark.asyncio
async def test_archived_path_emits_three_events_in_order():
    """Cold start from archived sandbox → starting, starting+archived, ready."""
    req = _make_request()
    wm = _make_workspace_manager(has_ready=False, observed_state="archived")

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    assert len(events) == 3, f"expected 3 workspace_status events, got {events}"
    assert events[0] == {"status": "starting", "workspace_id": "ws-1"}
    assert events[1] == {
        "status": "starting",
        "workspace_id": "ws-1",
        "sandbox_state": "archived",
    }
    assert events[2] == {"status": "ready", "workspace_id": "ws-1"}


@pytest.mark.asyncio
async def test_stopped_path_emits_two_events_no_refinement():
    """Cold start from stopped sandbox → starting, ready (no refinement)."""
    req = _make_request()
    wm = _make_workspace_manager(has_ready=False, observed_state="stopped")

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    assert len(events) == 2, f"expected 2 workspace_status events, got {events}"
    assert events[0] == {"status": "starting", "workspace_id": "ws-1"}
    assert events[1] == {"status": "ready", "workspace_id": "ws-1"}
    assert "sandbox_state" not in events[0]


@pytest.mark.asyncio
async def test_running_cold_path_emits_two_events_no_refinement():
    """Cold start from running sandbox (server-restart case) → starting, ready."""
    req = _make_request()
    wm = _make_workspace_manager(has_ready=False, observed_state="running")

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    assert len(events) == 2
    assert events[0]["status"] == "starting"
    assert events[1]["status"] == "ready"
    for e in events:
        assert "sandbox_state" not in e


@pytest.mark.asyncio
async def test_recovery_path_callback_never_fires_no_refinement(monkeypatch):
    """When reconnect doesn't observe state (e.g. fresh-sandbox recovery),
    the generator must time out, skip refinement, and emit starting + ready."""
    req = _make_request()
    wm = _make_workspace_manager(has_ready=False, observed_state=None)

    # Patch wait_for so the test doesn't actually wait 5s for the timeout.
    import asyncio as _aio

    real_wait_for = _aio.wait_for

    async def _fast_wait_for(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=0.05)

    monkeypatch.setattr(f"{PTC}.asyncio.wait_for", _fast_wait_for)

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    assert len(events) == 2
    assert events[0]["status"] == "starting"
    assert events[1]["status"] == "ready"
    for e in events:
        assert "sandbox_state" not in e


@pytest.mark.asyncio
async def test_warm_path_emits_zero_events():
    """has_ready_session=True → no workspace_status events at all."""
    req = _make_request()
    wm = _make_workspace_manager(has_ready=True, observed_state=None)

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    assert events == []
    # get_session_for_workspace is still called on the warm path for the
    # cached session, but no callback should flow through because the
    # generator doesn't supply one on the warm branch.
    warm_calls = [
        c for c in wm.get_session_for_workspace.await_args_list
        if c.kwargs.get("on_state_observed") is not None
    ]
    assert warm_calls == [], "warm path must not pass on_state_observed"


@pytest.mark.asyncio
async def test_refinement_fires_before_session_completes():
    """Archived refinement must be emitted while session init is still in
    flight — not after the ready event. Proves the callback path actually
    races against session_task correctly."""
    req = _make_request()
    # Session takes longer than the callback fires so refinement must
    # appear between starting and ready.
    wm = _make_workspace_manager(
        has_ready=False,
        observed_state="archived",
        session_delay_s=0.05,
        fire_callback_delay_s=0.002,
    )

    lines = await _run_to_sentinel(req, wm)
    events = _parse_ws_status_events(lines)

    statuses = [e.get("status") for e in events]
    sandbox_states = [e.get("sandbox_state") for e in events]

    # Expected order: starting → starting(archived) → ready
    assert statuses == ["starting", "starting", "ready"]
    assert sandbox_states == [None, "archived", None]
