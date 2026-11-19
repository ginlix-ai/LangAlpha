"""Which account each read of the agent build takes.

A turn has a sender and a workspace owner. Today they are always one account,
so only a build handed two different ids shows which read takes which: the
file tools, memory and the workflow store are the workspace owner's, the
commands, wakeups, approvals and thread status are the sender's.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent import agent as agent_module
from ptc_agent.agent.agent import PTCAgent
from ptc_agent.agent.graph import build_ptc_graph_with_session
from ptc_agent.agent.middleware.runtime_context.epoch import Workspace
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.project_context import ProjectContext

SENDER = "user-sender"
OWNER = "user-owner"
WORKSPACE = "00000000-0000-4000-8000-00000000aaaa"


def _build(**kwargs) -> dict[str, MagicMock]:
    """Build the main agent with spies on the reads that take an account."""
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        features={"wakeups": True},
    )
    llm = GenericFakeChatModel(messages=iter([]))

    def fake_create_agent(_model, **_built):
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    spies = {
        "gates": MagicMock(wraps=agent_module.resolve_identity_gates),
        "filesystem": MagicMock(wraps=agent_module.build_filesystem_backend),
        "hand_off": MagicMock(wraps=agent_module.HandOff),
        "thread_status": MagicMock(wraps=agent_module.build_thread_status_tool),
    }
    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=fake_create_agent),
        patch("ptc_agent.agent.agent.resolve_identity_gates", spies["gates"]),
        patch("ptc_agent.agent.agent.build_filesystem_backend", spies["filesystem"]),
        patch("ptc_agent.agent.agent.HandOff", spies["hand_off"]),
        patch("ptc_agent.agent.agent.build_thread_status_tool", spies["thread_status"]),
    ):
        PTCAgent(config).create_agent(
            sandbox=MagicMock(computer_root="/home/workspace", mounts_livefs=True),
            mcp_registry=MagicMock(),
            llm=llm,
            thread_id="00000000-0000-4000-8000-00000000bbbb",
            store=MagicMock(),
            project=ProjectContext(WORKSPACE, "Macro"),
            **kwargs,
        )
    return spies


def test_the_workspace_is_read_for_its_owner_and_the_turn_for_its_sender():
    spies = _build(user_id=SENDER, owner_id=OWNER)

    assert spies["gates"].call_args.kwargs["user_id"] == OWNER
    assert spies["filesystem"].call_args.kwargs["user_id"] == OWNER
    assert spies["hand_off"].call_args.kwargs["user_id"] == SENDER
    assert spies["thread_status"].call_args.kwargs["user_id"] == SENDER


def test_a_build_naming_no_owner_mounts_no_ones_workspace():
    """The owner is named, never taken from the sender: a build that names
    none reads no account's memory or files."""
    spies = _build(user_id=SENDER)

    assert spies["gates"].call_args.kwargs["user_id"] is None
    assert spies["filesystem"].call_args.kwargs["user_id"] is None
    assert spies["hand_off"].call_args.kwargs["user_id"] == SENDER


@pytest.mark.asyncio
async def test_the_graph_redacts_the_owners_vault_and_reads_the_senders_profile_data():
    """The redaction set is every secret the computer can read, which is its
    owner's vault; the portfolio and watchlist counts are the person's."""
    session = MagicMock(mcp_tool_summary="summary")
    agent = MagicMock()
    with (
        patch("ptc_agent.agent.graph.PTCAgent", return_value=agent),
        patch("ptc_agent.agent.graph._read_workspace_naming", AsyncMock(return_value=Workspace("A"))),
        patch("ptc_agent.agent.graph.fetch_user_data_counts", AsyncMock(return_value=None)) as counts,
        patch(
            "src.server.database.user_vault_secrets.get_user_secrets_decrypted",
            AsyncMock(return_value={"KEY": "synthetic-owner-value"}),
        ) as vault,
    ):
        await build_ptc_graph_with_session(
            session=session, config=MagicMock(), user_id=SENDER, owner_id=OWNER,
            project=ProjectContext(WORKSPACE, "a"),
        )
    vault.assert_awaited_once_with(OWNER)
    counts.assert_awaited_once_with(SENDER)
    built = agent.create_agent.call_args.kwargs
    assert (built["user_id"], built["owner_id"]) == (SENDER, OWNER)
    assert built["vault_secrets"] == {"KEY": "synthetic-owner-value"}
