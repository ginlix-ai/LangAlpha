"""A resume answers the pause the newest checkpoint holds, so a step the
turn's agent lacks is cancelled first (``cancel_tool_calls_it_lacks``),
whatever the role: on Claude Code a step another model paused on, which its
graph lacks, never by name, so the Chief of Staff's Claude Code turn resumes
as an analyst's does.
"""

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.turn_runtime import TurnRoute

PTC = "src.server.handlers.chat.ptc_run"
CODE = "claude-haiku-5-5-code"


class _Built(Exception):
    """Raised once the graph is built and the resume's cancel decided, to end the run there."""


def _resume():
    request = MagicMock()
    request.workspace_id = "ws-1"
    request.additional_context = None
    request.hitl_response = {"interrupt-1": {"decisions": [{"type": "approve"}]}}
    request.checkpoint_id = None
    request.messages = []
    request.timezone = "UTC"
    request.locale = "en-US"
    request.subagents_enabled = None
    request.llm_model = None
    request.steer_only = False
    request.retry_of_run_id = None
    request.request_key = None
    request.artifact_comment_ids = None
    request.artifact_comments = None
    request.disable_subagents = False
    return request


async def _resume_as(role: str, model: str):
    """Run a resume turn of ``role`` on ``model`` up to the messages it sends;
    the cancel's mock, and the graph it would be given."""
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    config = MagicMock()
    config.llm.name = model
    config.subagents.enabled = False
    graph = MagicMock(name="graph")
    workspaces = MagicMock()
    workspaces.has_ready_session = MagicMock(return_value=True)
    workspaces.get_session_for_workspace = AsyncMock(return_value=MagicMock(name="Session"))
    workspaces.restore_sibling_folders = AsyncMock()
    cancel = AsyncMock(return_value=[])
    built = MagicMock(side_effect=_Built())
    setup, manager, registries = MagicMock(), MagicMock(), MagicMock()
    with ExitStack() as stack:
        for target, value in [
            ("setup", setup),
            ("ensure_thread", AsyncMock()),
            ("_resolve_fork", MagicMock(return_value=("Q", None))),
            ("begin_run", AsyncMock(return_value=MagicMock(turn_index=1, finalized=False))),
            ("wait_or_steer", AsyncMock(return_value=(True, None))),
            ("get_user_profile_for_prompt", AsyncMock(return_value=None)),
            ("process_hitl_response", MagicMock(return_value=SimpleNamespace(
                feedback_action=None, query_content="", metadata={},
            ))),
            ("init_tracking", MagicMock(return_value=(MagicMock(), MagicMock()))),
            ("apply_fetch_override", MagicMock()),
            ("WorkspaceManager", manager),
            ("_fire_and_forget", MagicMock()),
            ("update_workspace_activity", MagicMock()),
            ("BackgroundRegistryStore", registries),
            ("keep_named_model", AsyncMock()),
            ("build_ptc_graph_with_session", AsyncMock(return_value=graph)),
            ("turn_start_checkpoint_id", AsyncMock(return_value=None)),
            ("cancel_tool_calls_it_lacks", cancel),
            ("normalize_request_messages", built),
            ("read_disk_notice", AsyncMock(return_value=(None, False))),
            ("_resolve_origin_meta", AsyncMock(return_value={})),
            ("carry.carried_pair", AsyncMock(return_value={})),
            ("carry.carried_delivery", AsyncMock(return_value=None)),
        ]:
            stack.enter_context(patch(f"{PTC}.{target}", value))
        for target, value in [
            ("src.server.services.user_skills.skills_delivery_signature", MagicMock(return_value="skills")),
            ("src.server.services.thread_agent.AgentInputs.read", AsyncMock(return_value=MagicMock(build_kwargs=dict))),
            ("src.server.services.egress.direct_tools.order_ledger_for_turn", MagicMock()),
            ("src.server.services.wakeups.cancel_notice.ThreadCancelNotices", MagicMock()),
        ]:
            stack.enter_context(patch(target, value))
        setup.agent_config = MagicMock()
        manager.get_instance.return_value = workspaces
        registries.get_instance.return_value.get_or_create_registry = AsyncMock(return_value=MagicMock())
        gen = astream_ptc_workflow(
            request=_resume(),
            thread_id="t-1",
            run_id="r-1",
            user_input="",
            user_id="u-1",
            route=TurnRoute("ws-1", role),
            is_byok=False,
            config=config,
        )
        try:
            async for _ in gen:
                pass
        except _Built:
            pass
        finally:
            await gen.aclose()
    built.assert_called_once()
    return cancel, graph


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["chief_of_staff", "analyst"])
async def test_a_claude_code_resume_cancels_another_models_step_whatever_the_role(role):
    cancel, graph = await _resume_as(role, CODE)
    cancel.assert_awaited_once_with(graph, "t-1", user_id="u-1", by_name=False, claude_code=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(("role", "by_name"), [("chief_of_staff", True), ("analyst", False)])
async def test_any_other_resume_cancels_the_steps_its_agent_lacks_first(role, by_name):
    """Home's by name, since the retired agent it may have paused under named its own."""
    cancel, graph = await _resume_as(role, "gpt-5")
    cancel.assert_awaited_once_with(graph, "t-1", user_id="u-1", by_name=by_name, claude_code=False)
