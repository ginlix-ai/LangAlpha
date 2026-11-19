"""Which account each read a turn makes before its agent is built takes.

A turn carries its sender as ``user_id`` and the workspace's owner on its
route (``TurnRoute.owner_id``). They are one account for every turn that can
run today, so these pass two different ids to show which read takes which:
the person's flags, models and profile are the sender's, and what the
computer is given (connectors, skills) is the owner's.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.shared_workspaces.turn import TurnIdentity
from src.server.services.thread_agent import AgentInputs
from src.server.services.turn_runtime import TurnRoute

SENDER = "user-sender"
OWNER = "user-owner"


@pytest.mark.asyncio
async def test_the_inputs_bind_the_owners_connectors_and_carry_both_accounts():
    view = MagicMock(trading_rule=None)
    manager = MagicMock()
    manager.tool_view.return_value = view
    direct = AsyncMock(return_value=MagicMock(name="direct"))
    with (
        patch("src.server.services.workspace_manager.WorkspaceManager.get_instance", return_value=manager),
        patch("src.server.services.workspace_layout.resolve_project_context", AsyncMock(return_value=None)),
        patch("src.server.services.egress.direct_tools.direct_tools_for_turn", direct),
    ):
        inputs = await AgentInputs.read(
            SimpleNamespace(computer_id="c-1"),
            identity=TurnIdentity(SENDER, OWNER, SENDER),
            workspace_id="ws-1",
            thread_id="t-1",
            role="analyst",
            user_profile=None,
        )

    assert direct.await_args.kwargs["user_id"] == OWNER
    built = inputs.build_kwargs()
    assert (built["user_id"], built["owner_id"]) == (SENDER, OWNER)
    # A Claude Code CLI signs in as the person whose turn it is.
    assert inputs.identity.sender == SENDER


@pytest.mark.asyncio
async def test_the_skills_are_the_owners_and_the_flags_the_senders():
    from src.server.services.llm.config import load_user_context

    config = SimpleNamespace()
    bundle = SimpleNamespace(
        skills=(), disabled_builtins=frozenset(), dir=None, workspace_dir=None, command_overrides={}
    )
    flags = AsyncMock(return_value={})
    skills = AsyncMock(return_value=bundle)
    with (
        patch("src.server.services.llm.config.effective_flags_for_user", flags),
        patch("src.server.services.user_skills.load_user_skill_bundle", skills),
    ):
        await load_user_context(config, SENDER, "ws-1", {}, owner_id=OWNER)
        await load_user_context(config, SENDER, None, {})

    assert [c.args[0] for c in flags.await_args_list] == [SENDER, SENDER]
    # With no workspace named there is no owner but the sender.
    assert [c.args for c in skills.await_args_list] == [(OWNER, "ws-1"), (SENDER, None)]


@pytest.mark.asyncio
async def test_a_threads_resolver_reads_the_routes_owner_for_the_workspace():
    from src.server.services.thread_agent import thread_resolver

    resolve = AsyncMock(return_value="config")
    with (
        patch("src.server.app.setup.agent_config", MagicMock()),
        patch("src.server.services.llm.config.resolve_llm_config", resolve),
    ):
        await thread_resolver(
            SENDER, "t-1", TurnRoute("ws-1", "analyst", OWNER),
            identity=TurnIdentity(SENDER, OWNER, SENDER), thread=None, is_byok=False,
        )(None)

    assert resolve.await_args.args[1] == SENDER
    assert resolve.await_args.kwargs["identity"].owner == OWNER
    assert resolve.await_args.kwargs["workspace_id"] == "ws-1"
