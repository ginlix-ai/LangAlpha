"""A retry runs on the surface its attempt ran on.

``/retry`` builds its attempt from the failed run's row, with no surface, so
a retry of a channel turn would otherwise run as a web turn: its tools would
name no conversation while the rules in view still say to reply through the
channel. Each attempt records where it ran on its run row, and the retry
reads it back. The thread's bound surface is never the answer: a web turn on
a channel's thread stays a web turn when retried.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.request_prep import PriorThread, build_turn_context
from src.server.models.chat import ChatRequest

PTC = "src.server.handlers.chat.ptc_run"
PREP = "src.server.handlers.chat.request_prep"

RULES = "Reply through send_message."


class _Admitted(Exception):
    """Raised from begin_run once it has been called, to end the run there."""


def _retry() -> ChatRequest:
    # What /retry builds for a checkpoint replay: no surface of its own.
    return ChatRequest(
        workspace_id="ws-1",
        messages=[],
        checkpoint_id="cp-1",
        retry_of_run_id="run-0",
    )


async def _start(request: ChatRequest, prior: PriorThread, failed_run: dict):
    """Run the turn up to its START write: what it stamped there, and the
    turn context it built."""
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    contexts = []

    def _capture(*args, **kwargs):
        context = build_turn_context(*args, **kwargs)
        contexts.append(context)
        return context

    config = MagicMock()
    config.llm.name = "claude-test"
    config.subagents.enabled = False
    with (
        patch(f"{PTC}.setup"),
        patch(f"{PTC}.WorkspaceManager") as workspaces,
        patch(f"{PTC}.ensure_thread", new=AsyncMock(return_value=prior)),
        patch(f"{PTC}.wait_or_steer", new=AsyncMock(return_value=(True, None))),
        patch(f"{PTC}.read_disk_notice", new=AsyncMock(return_value=(None, False))),
        patch(f"{PTC}.get_user_profile_for_prompt", new=AsyncMock(return_value=None)),
        patch(f"{PTC}.build_turn_context", side_effect=_capture),
        patch(f"{PTC}._resolve_fork", return_value=("Q", None)),
        patch(f"{PTC}._resolve_origin_meta", new=AsyncMock(return_value={})),
        patch(f"{PTC}.carry.carried_pair", new=AsyncMock(return_value={})),
        patch(f"{PTC}.carry.carried_delivery", new=AsyncMock(return_value=None)),
        patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())),
        patch(f"{PTC}.begin_run", new=AsyncMock(side_effect=_Admitted())) as begin,
        patch(f"{PREP}.tl_db.get_run", new=AsyncMock(return_value=failed_run)) as get_run,
    ):
        workspaces.get_instance.return_value.has_ready_session.return_value = True
        gen = astream_ptc_workflow(
            request=request,
            thread_id="t-1",
            run_id="r-1",
            user_input="",
            user_id="u-1",
            workspace_id="ws-1",
            is_byok=False,
            config=config,
        )
        try:
            async for _ in gen:
                pass
        except Exception:
            pass
        finally:
            await gen.aclose()
    begin.assert_awaited_once()
    (context,) = contexts
    return begin.await_args.kwargs["extra_run_metadata"], context, get_run


@pytest.mark.asyncio
async def test_a_channel_turn_records_where_it_ran():
    request = ChatRequest(
        workspace_id="ws-1",
        messages=[{"role": "user", "content": "hi"}],
        platform="slack",
        surface_rules=RULES,
    )

    stamped, _, get_run = await _start(request, PriorThread(), failed_run={})

    assert stamped["surface"] == {"platform": "slack", "rules": RULES}
    # Not a retry: nothing is read back.
    get_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_retry_of_a_channel_turn_runs_on_the_channel():
    failed = {"metadata": {"surface": {"platform": "slack", "rules": RULES}}}

    stamped, context, get_run = await _start(_retry(), PriorThread(), failed)

    get_run.assert_awaited_once_with("run-0")
    assert (context.platform, context.surface_rules) == ("slack", RULES)
    assert context.inherits_rules is False
    # Recorded again, so a retry of the retry runs there too.
    assert stamped["surface"] == {"platform": "slack", "rules": RULES}


@pytest.mark.asyncio
async def test_a_retry_of_a_web_turn_on_a_channel_thread_stays_on_the_web():
    stamped, context, _ = await _start(
        _retry(), PriorThread(platform="slack"), failed_run={"metadata": {}}
    )

    assert (context.platform, context.surface_rules) == (None, None)
    assert "surface" not in stamped


@pytest.mark.asyncio
async def test_a_retry_of_a_notification_runs_under_the_last_rules_stated():
    """A report-back names no surface and runs under the rules in force; its
    retry carries no query type, so the stamp says so for it."""
    failed = {"metadata": {"surface": {"platform": "slack", "inherits_rules": True}}}

    stamped, context, _ = await _start(_retry(), PriorThread(), failed)

    assert context.platform == "slack"
    assert context.inherits_rules is True
    assert stamped["surface"] == {"platform": "slack", "inherits_rules": True}
