"""steer_only wiring at the workflow entrypoints.

Both the foreground and dispatched paths now flow through the single
``wait_or_steer`` admission decision; the entrypoint's only job is to hand it
the right knobs. Per mode (PTC and Flash) this pins that wiring:
- the foreground path forwards ``request.steer_only`` with ``can_steer=True``
  and no ``exclude_run_id`` (dropping ``steer_only`` would silently revert the
  gateway-probe fix), and
- the dispatched path (X-Dispatch=background) forwards ``can_steer=False`` and
  its own pre-registered ``exclude_run_id`` so it can never steer.

The admission *behavior* those knobs select (fresh+steer_only → not_running,
non-fresh + can't-steer → 409) lives in ``TestWaitOrSteer``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PTC = "src.server.handlers.chat.ptc_workflow"
FLASH = "src.server.handlers.chat.flash_workflow"


def _make_request(steer_only: bool = True):
    req = MagicMock()
    req.workspace_id = "ws-1"
    req.additional_context = None
    req.hitl_response = None
    req.checkpoint_id = None
    req.messages = [MagicMock(role="user", content="hi")]
    req.plan_mode = False
    req.timezone = "UTC"
    req.locale = None
    req.subagents_enabled = None
    req.llm_model = None
    req.steer_only = steer_only
    # Truthy MagicMock attrs would trip the request_key dedup probe (real
    # DB call) and the retry-chain resolution before wait_or_steer.
    req.request_key = None
    req.retry_of_run_id = None
    return req


def _make_manager(admission_state: str = "fresh"):
    manager = MagicMock()
    manager.get_admission_lock = AsyncMock(return_value=asyncio.Lock())
    manager.wait_for_admission = AsyncMock(return_value=admission_state)
    manager.cancel_stale_workflow = AsyncMock()
    return manager


async def _drain(gen) -> list[str]:
    """Collect events; tolerate the workflow's post-error re-raise (the
    generator yields the SSE error events, then re-raises for the route
    layer — see the ``except Exception`` tail of both astream functions)."""
    collected: list[str] = []
    try:
        async for event in gen:
            collected.append(event)
    except Exception:
        pass
    finally:
        await gen.aclose()
    return collected


# ---------------------------------------------------------------------------
# Foreground: steer_only forwarded, steering permitted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ptc_foreground_forwards_steer_only_and_can_steer():
    from src.server.handlers.chat.ptc_workflow import astream_ptc_workflow

    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=True)

    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.ExecutionTracker"),
        patch(f"{PTC}.BackgroundTaskManager") as mock_btm_cls,
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(f"{PTC}.release_burst_slot", new_callable=AsyncMock),
        patch(
            f"{PTC}.wait_or_steer",
            new_callable=AsyncMock,
            return_value=(False, None),
        ) as mock_wos,
    ):
        mock_setup.agent_config = MagicMock()
        mock_btm_cls.get_instance.return_value = _make_manager()
        mock_wm_cls.get_instance.return_value = wm

        await _drain(
            astream_ptc_workflow(
                request=_make_request(steer_only=True),
                thread_id="t-1",
                run_id="r-1",
                user_input="hi",
                user_id="u-1",
                workspace_id="ws-1",
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is True
    assert kwargs["exclude_run_id"] is None


@pytest.mark.asyncio
async def test_flash_foreground_forwards_steer_only_and_can_steer():
    from src.server.handlers.chat.flash_workflow import astream_flash_workflow

    with (
        patch(f"{FLASH}.setup") as mock_setup,
        patch(f"{FLASH}.ExecutionTracker"),
        patch(f"{FLASH}.BackgroundTaskManager") as mock_btm_cls,
        patch(f"{FLASH}.release_burst_slot", new_callable=AsyncMock),
        patch(
            f"{FLASH}.wait_or_steer",
            new_callable=AsyncMock,
            return_value=(False, None),
        ) as mock_wos,
    ):
        mock_setup.agent_config = MagicMock()
        mock_btm_cls.get_instance.return_value = _make_manager()

        await _drain(
            astream_flash_workflow(
                request=_make_request(steer_only=True),
                thread_id="t-1",
                run_id="r-1",
                user_input="hi",
                user_id="u-1",
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is True
    assert kwargs["exclude_run_id"] is None


# ---------------------------------------------------------------------------
# Dispatched: steering disabled, own placeholder excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ptc_dispatched_forwards_can_steer_false_and_exclude_run_id():
    from src.server.handlers.chat.ptc_workflow import astream_ptc_workflow

    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=True)

    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.ExecutionTracker"),
        patch(f"{PTC}.BackgroundTaskManager") as mock_btm_cls,
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(f"{PTC}.release_burst_slot", new_callable=AsyncMock),
        patch(
            f"{PTC}.wait_or_steer",
            new_callable=AsyncMock,
            return_value=(False, None),
        ) as mock_wos,
    ):
        mock_setup.agent_config = MagicMock()
        mock_btm_cls.get_instance.return_value = _make_manager("fresh")
        mock_wm_cls.get_instance.return_value = wm

        await _drain(
            astream_ptc_workflow(
                request=_make_request(steer_only=True),
                thread_id="t-1",
                run_id="r-1",
                user_input="hi",
                user_id="u-1",
                workspace_id="ws-1",
                dispatched=True,
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is False
    assert kwargs["exclude_run_id"] == "r-1"


@pytest.mark.asyncio
async def test_flash_dispatched_forwards_can_steer_false_and_exclude_run_id():
    from src.server.handlers.chat.flash_workflow import astream_flash_workflow

    with (
        patch(f"{FLASH}.setup") as mock_setup,
        patch(f"{FLASH}.ExecutionTracker"),
        patch(f"{FLASH}.BackgroundTaskManager") as mock_btm_cls,
        patch(f"{FLASH}.release_burst_slot", new_callable=AsyncMock),
        patch(
            f"{FLASH}.wait_or_steer",
            new_callable=AsyncMock,
            return_value=(False, None),
        ) as mock_wos,
    ):
        mock_setup.agent_config = MagicMock()
        mock_btm_cls.get_instance.return_value = _make_manager("fresh")

        await _drain(
            astream_flash_workflow(
                request=_make_request(steer_only=True),
                thread_id="t-1",
                run_id="r-1",
                user_input="hi",
                user_id="u-1",
                dispatched=True,
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is False
    assert kwargs["exclude_run_id"] == "r-1"
