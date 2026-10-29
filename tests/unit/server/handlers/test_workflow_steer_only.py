"""steer_only wiring at the workflow entrypoints.

Both the foreground and dispatched paths now flow through the single
``wait_or_steer`` admission decision; the entrypoint's only job is to hand it
the right knobs. This pins that wiring:
- the foreground path forwards ``request.steer_only`` with ``can_steer=True``
  (dropping ``steer_only`` would silently revert the gateway-probe fix), and
- the dispatched path (X-Dispatch=background) forwards ``can_steer=False`` so
  it can never steer.

The admission *behavior* those knobs select (fresh+steer_only → not_running,
non-fresh + can't-steer → 409) lives in ``TestWaitOrSteer``.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.turn_runtime import TurnRoute

PTC = "src.server.handlers.chat.ptc_run"
# RunScope routes burst releases through the definition site.
RELEASE = "src.server.dependencies.usage_limits.release_burst_slot"


def _make_request(steer_only: bool = True):
    req = MagicMock()
    req.workspace_id = "ws-1"
    req.additional_context = None
    req.hitl_response = None
    req.checkpoint_id = None
    req.messages = [MagicMock(role="user", content="hi")]
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
    layer — see the ``except Exception`` tail of ``astream_ptc_workflow``)."""
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
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=True)

    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.LocalRunExecutor") as mock_btm_cls,
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(RELEASE, new_callable=AsyncMock),
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
                route=TurnRoute("ws-1", "analyst"),
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is True


# ---------------------------------------------------------------------------
# Dispatched: steering disabled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ptc_dispatched_forwards_can_steer_false():
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=True)

    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.LocalRunExecutor") as mock_btm_cls,
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(RELEASE, new_callable=AsyncMock),
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
                route=TurnRoute("ws-1", "analyst"),
                dispatched=True,
            )
        )

    mock_wos.assert_awaited_once()
    kwargs = mock_wos.await_args.kwargs
    assert kwargs["steer_only"] is True
    assert kwargs["can_steer"] is False


@pytest.mark.parametrize(
    "steerable, dispatched, retry_of, allowed",
    [
        (True, False, None, True),
        # An automation's instruction is a turn of its own, never a steer.
        (False, False, None, False),
        (True, True, None, False),
        (True, False, "run-1", False),
    ],
)
def test_only_a_steerable_foreground_turn_steers(steerable, dispatched, retry_of, allowed):
    from src.server.handlers.chat.admission_gate import steer_allowed

    request = MagicMock(
        retry_of_run_id=retry_of, fork_from_turn=None, checkpoint_id=None
    )
    assert steer_allowed(request, dispatched=dispatched, steerable=steerable) is allowed


# ---------------------------------------------------------------------------
# turn_held: a send waiting behind a thread mutation says why
# ---------------------------------------------------------------------------

MUTATION_RUNNER = "src.server.services.thread_mutation.ThreadMutationRunner"


def _frame(event: str) -> tuple[str, dict]:
    """Split one ``id: 0\\nevent: ...\\ndata: ...`` frame into (name, data)."""
    lines = event.strip().split("\n")
    assert lines[0] == "id: 0"
    return lines[1].removeprefix("event: "), json.loads(lines[2].removeprefix("data: "))


async def _run_to_admission(
    holding_verb,
    *,
    steer_only: bool = False,
    dispatched: bool = False,
    wait_side_effect=None,
    wait_seconds: float = 0,
):
    """Drive ``astream_ptc_workflow`` through its early frames.

    ``wait_or_steer`` is stubbed to refuse admission, so the generator ends
    right after it; ``order`` records when the stub ran relative to the frames.
    Returns (frames, wait_or_steer mock, order, errors), ``errors`` being what
    the generator handed its error path.
    """
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    wm = MagicMock()
    wm.has_ready_session = MagicMock(return_value=True)
    runner = MagicMock()
    runner.holding_verb = holding_verb
    order: list[str] = []
    errors: list[BaseException] = []

    async def _error_path(exc, **kwargs):
        errors.append(exc)
        return
        yield  # pragma: no cover

    async def _wait(*args, **kwargs):
        order.append("wait_or_steer")
        await asyncio.sleep(wait_seconds)
        if wait_side_effect is not None:
            raise wait_side_effect
        return (False, None)

    frames: list[str] = []
    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.LocalRunExecutor") as mock_btm_cls,
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(RELEASE, new_callable=AsyncMock),
        patch(f"{PTC}.wait_or_steer", side_effect=_wait) as mock_wos,
        patch(f"{MUTATION_RUNNER}.get_instance", return_value=runner),
        patch(f"{PTC}.handle_workflow_error", _error_path),
        patch(f"{PTC}.get_sse_keepalive_interval", return_value=0.01),
    ):
        mock_setup.agent_config = MagicMock()
        mock_btm_cls.get_instance.return_value = _make_manager()
        mock_wm_cls.get_instance.return_value = wm

        gen = astream_ptc_workflow(
            request=_make_request(steer_only=steer_only),
            thread_id="t-1",
            run_id="r-1",
            user_input="hi",
            user_id="u-1",
            route=TurnRoute("ws-1", "analyst"),
            dispatched=dispatched,
        )
        try:
            async for event in gen:
                order.append(
                    _frame(event)[0] if event.startswith("id: 0")
                    else "keepalive" if event == ":keepalive\n\n"
                    else "other"
                )
                frames.append(event)
        except Exception:
            pass
        finally:
            await gen.aclose()
    return frames, mock_wos, order, errors


@pytest.mark.asyncio
async def test_a_send_held_by_a_mutation_is_told_why_then_told_it_is_released():
    """The held frame is out before the admission wait and the released frame
    after it, ahead of anything else, so the client can show why it waits."""
    holding_verb = AsyncMock(return_value="handoff")

    frames, mock_wos, order, _ = await _run_to_admission(holding_verb)

    assert [_frame(f) for f in frames] == [
        ("turn_held", {"status": "held", "reason": "handoff"}),
        ("turn_held", {"status": "released"}),
    ]
    assert order == ["turn_held", "wait_or_steer", "turn_held"]
    holding_verb.assert_awaited_once_with("t-1")
    mock_wos.assert_called_once()


@pytest.mark.asyncio
async def test_a_send_on_a_free_thread_gets_no_turn_held_frame():
    holding_verb = AsyncMock(return_value=None)

    frames, mock_wos, order, _ = await _run_to_admission(holding_verb)

    assert frames == []
    assert order == ["wait_or_steer"]
    holding_verb.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs", [{"dispatched": True}, {"steer_only": True}], ids=["dispatched", "steer_only"]
)
async def test_a_dispatch_primer_or_steer_probe_never_asks_what_holds_the_thread(kwargs):
    """Both read the first frame as their answer, so a held frame would be
    taken for one."""
    holding_verb = AsyncMock(return_value="compact")

    frames, mock_wos, _, _ = await _run_to_admission(holding_verb, **kwargs)

    holding_verb.assert_not_awaited()
    assert frames == []
    mock_wos.assert_called_once()


@pytest.mark.asyncio
async def test_a_409_from_admission_still_surfaces_after_the_held_frame():
    """The held frame is already on the wire when admission refuses; the
    refusal then travels the usual error path unchanged."""
    from fastapi import HTTPException

    holding_verb = AsyncMock(return_value="compact")
    refusal = HTTPException(status_code=409, detail="busy")

    frames, _, order, errors = await _run_to_admission(
        holding_verb, wait_side_effect=refusal
    )

    assert [_frame(f) for f in frames] == [
        ("turn_held", {"status": "held", "reason": "compact"}),
    ]
    assert order == ["turn_held", "wait_or_steer"]
    assert errors == [refusal]


@pytest.mark.asyncio
async def test_a_long_held_wait_keeps_the_stream_alive_until_released():
    """A hand-off can hold a send for minutes, longer than an edge proxy lets
    a silent stream live, so keepalives flow between held and released."""
    holding_verb = AsyncMock(return_value="handoff")

    frames, _, order, _ = await _run_to_admission(holding_verb, wait_seconds=0.05)

    assert order[:2] == ["turn_held", "wait_or_steer"]
    assert order[-1] == "turn_held"
    assert set(order[2:-1]) == {"keepalive"}
    assert _frame(frames[-1]) == ("turn_held", {"status": "released"})
