"""A Claude Code turn on a cloud computer, live: the CLI in the provider's sandbox, the tools and session in this process.

Runs the real runtime (``ClaudeTurn``) through ``sandbox_launcher`` on each
requested provider (see this folder's conftest), against a live Messages API
the sandbox can reach: ``CLAUDE_CODE_LIVE_API_KEY``, with
``CLAUDE_CODE_LIVE_BASE_URL`` for an Anthropic-compatible endpoint other than
Anthropic's and ``CLAUDE_CODE_LIVE_MODEL`` for the model. The first turn
installs the pinned CLI in a fresh sandbox; the second resumes the session
from the checkpoint in a new CLI.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(importlib.util.find_spec("claude_agent_sdk") is None, reason="needs the claude-code extra"),
    pytest.mark.skipif(not os.environ.get("CLAUDE_CODE_LIVE_API_KEY"), reason="needs CLAUDE_CODE_LIVE_API_KEY"),
]

KEY = os.environ.get("CLAUDE_CODE_LIVE_API_KEY", "")
MODEL = os.environ.get("CLAUDE_CODE_LIVE_MODEL", "claude-haiku-5-5")
ENV = {"ANTHROPIC_BASE_URL": url} if (url := os.environ.get("CLAUDE_CODE_LIVE_BASE_URL")) else {}


class Store:
    def __init__(self, credential) -> None:
        self.credential, self.reads = credential, 0

    async def read(self):
        self.reads += 1
        return self.credential

    async def refused(self) -> None:  # pragma: no cover - a live key is served
        raise AssertionError("the live key was refused")


@pytest_asyncio.fixture(scope="class", loop_scope="class")
async def cloud_computer(shared_runtime, provider_name):
    if provider_name in ("ssh", "memory"):
        pytest.skip("a cloud computer's sandbox only")
    root = await shared_runtime.fetch_working_dir()
    await shared_runtime.exec(f"mkdir -p {root}")
    return SimpleNamespace(wait_ready=AsyncMock(), runtime=shared_runtime, working_dir=root)


@pytest.mark.asyncio(loop_scope="class")
class TestClaudeCodeInTheSandbox:
    async def test_a_turn_calls_a_tool_here_and_the_next_resumes_with_nothing_left_there(self, cloud_computer, timed):
        from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

        from ptc_agent.agent.claude_code.launcher import Credential
        from ptc_agent.agent.claude_code.sandbox_launcher import sandbox_launcher
        from tests.unit.ptc_agent.agent.claude_code.harness import new_graph, run_turn, spec

        store = Store(Credential(KEY, kind="api_key"))
        graph, thread, sp = new_graph(), "t" + uuid.uuid4().hex[:8], spec(MODEL)
        launch = lambda: sandbox_launcher(cloud_computer, identity="test-user", credential=store, env=ENV)  # noqa: E731
        async with timed("claude_code", "first_turn_with_install"):
            first = await run_turn(sp, launch(), graph, thread, [HumanMessage(
                "FACT: the code word is heron. Use the Bash tool to run: echo spike-42 . Then reply with its output only.")])
        calls = [m for m in first["messages"] if isinstance(m, ToolMessage)]
        assert calls and calls[0].content == "ran: echo spike-42"
        assert isinstance(first["messages"][-1], AIMessage) and "spike-42" in first["messages"][-1].text
        async with timed("claude_code", "resumed_turn"):
            second = await run_turn(sp, launch(), graph, thread, [HumanMessage("What was the code word? One word.")])
        assert second["claude_session"]["session_id"] == first["claude_session"]["session_id"]
        assert "heron" in second["messages"][-1].text.lower()
        assert store.reads == 2
        root = cloud_computer.working_dir
        tail = KEY[-24:]
        left = await cloud_computer.runtime.exec(
            f"pgrep -x claude | wc -l; ls {root}/_internal/claude-code/config | wc -l; "
            f"grep -rlF -- {tail} \"$HOME\" {root} /tmp 2>/dev/null | wc -l; "
            f"grep -lF -- {tail} /proc/[0-9]*/environ 2>/dev/null | wc -l",
            timeout=300,
        )
        assert left.stdout.split() == ["0", "0", "0", "0"], left.stdout
