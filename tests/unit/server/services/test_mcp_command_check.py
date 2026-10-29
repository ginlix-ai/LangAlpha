"""The host half of the command check: which values, which computer, which answer.

The sandbox half is pinned by ``test_mcp_command_check_run``; here the sandbox
is a stub, so these cover what the host decides before and after the run: a
missing value starts nothing, a value the request brings wins without being
stored, the check lands on a computer the server will run on, and every way it
can fail reads as one sentence the user can act on.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from ptc_agent.core.sandbox import mcp_setup
from ptc_agent.core.sandbox.runtime import ExecResult
from ptc_agent.core.tool_generator import ToolFunctionGenerator
from src.server.models.mcp_server import McpServerInput
from src.server.services import mcp_command_check as cc
from src.server.services.mcp_validation import Passed

_USER = "user-1"


def _server(**overrides) -> McpServerInput:
    fields = {
        "name": "acme",
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "@acme/mcp@1.2.3"],
        "env": {"ACME_KEY": "${vault:ACME_KEY}"},
    }
    fields.update(overrides)
    return McpServerInput(**fields)


def _computer(computer_id: str = "comp-1", **overrides) -> dict:
    row = {
        "computer_id": computer_id,
        "user_id": _USER,
        "status": "stopped",
        "provider_ref": None,
        "host_unavailable_since": None,
        "config": {},
    }
    row.update(overrides)
    return row


_OK = {
    "status": "ok",
    "error": "",
    "tools": [
        {"name": "search", "description": "Find things.", "input_schema": {"type": "object"}},
    ],
    "server_info": {"name": "acme", "version": "1.2.3"},
}


@pytest.fixture
def world(monkeypatch):
    """A user with one running computer whose sandbox answers ``_OK``."""
    sandbox = SimpleNamespace(check_user_mcp_server=AsyncMock(return_value=dict(_OK)))
    manager = MagicMock()
    manager.wake_computer = AsyncMock(return_value=SimpleNamespace(sandbox=sandbox))
    state = SimpleNamespace(
        sandbox=sandbox,
        manager=manager,
        stored={"ACME_KEY": "stored-value-123"},
        picked=_computer(status="running", provider_ref="sbx-1"),
    )
    manager.pick_for_user = AsyncMock(side_effect=lambda *_a, **_k: state.picked)

    async def secrets(user_id, names):
        assert user_id == _USER
        return {n: state.stored[n] for n in names if n in state.stored}

    state.secrets = AsyncMock(side_effect=secrets)
    monkeypatch.setattr(cc, "get_user_secrets_decrypted", state.secrets)
    monkeypatch.setattr(cc.ComputerManager, "current", classmethod(lambda cls: manager))
    yield state
    for task in list(cc._wakes):
        task.cancel()


def _vault_sent(world) -> dict:
    return world.sandbox.check_user_mcp_server.await_args.args[1]


class TestValues:
    @pytest.mark.asyncio
    async def test_a_ref_with_no_value_starts_nothing(self, world):
        world.stored = {}

        result = await cc.check_command_server(_USER, _server())

        assert result.verdict == "missing_secrets"
        assert result.missing_secrets == ("ACME_KEY",)
        world.manager.wake_computer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_value_the_request_brings_wins_and_is_not_looked_up(self, world):
        server = _server(
            env={"ACME_KEY": "${vault:ACME_KEY}", "REGION": "${vault:REGION}"},
        )
        world.stored = {"ACME_KEY": "stored-value-123", "REGION": "eu-west"}

        result = await cc.check_command_server(
            _USER, server, extra_secrets={"ACME_KEY": "typed-in-value", "UNUSED": "x"}
        )

        assert result == Passed("acme")
        world.secrets.assert_awaited_once_with(_USER, ["REGION"])
        # Only what the command refers to reaches the computer.
        assert _vault_sent(world) == {"ACME_KEY": "typed-in-value", "REGION": "eu-west"}

    @pytest.mark.asyncio
    async def test_a_ref_in_args_counts_too(self, world):
        server = _server(env={}, args=["-y", "@acme/mcp@1.2.3", "--key=${vault:ACME_KEY}"])

        await cc.check_command_server(_USER, server)

        assert _vault_sent(world) == {"ACME_KEY": "stored-value-123"}

    @pytest.mark.asyncio
    async def test_a_server_with_no_refs_reads_no_vault(self, world):
        await cc.check_command_server(_USER, _server(env={}))

        world.secrets.assert_not_awaited()
        assert _vault_sent(world) == {}


class TestWhichComputer:
    """Which computer is the manager's call (``pick_for_user``); the check
    runs on the one it picks, and says why when it cannot."""

    @pytest.mark.asyncio
    async def test_the_check_runs_on_the_computer_the_manager_picks(self, world):
        world.picked = _computer("ws-comp", status="stopped")

        await cc.check_command_server(_USER, _server(), workspace_id="ws-1")

        world.manager.pick_for_user.assert_awaited_once_with(_USER, workspace_id="ws-1")
        assert world.manager.wake_computer.await_args.args[0] == "ws-comp"

    @pytest.mark.asyncio
    async def test_no_computer_that_would_start(self, world):
        world.picked = None

        result = await cc.check_command_server(_USER, _server())

        assert result.verdict == "command_failed"
        assert result.reason == cc.NO_COMPUTER
        world.manager.wake_computer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_workspace_on_a_broken_computer_says_so(self, world):
        world.picked = _computer("ws-comp", status="error")

        result = await cc.check_command_server(_USER, _server(), workspace_id="ws-1")

        assert result.reason == cc.BROKEN_COMPUTER
        world.manager.wake_computer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_computer_manager(self, world, monkeypatch):
        monkeypatch.setattr(cc.ComputerManager, "current", classmethod(lambda cls: None))

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.NO_MANAGER


class TestWaking:
    @pytest.mark.asyncio
    async def test_the_plans_computer_limit_is_relayed_verbatim(self, world):
        world.manager.wake_computer.side_effect = HTTPException(
            status_code=429, detail={"message": "Your plan runs 1 computer at a time."}
        )

        result = await cc.check_command_server(_USER, _server())

        assert result.verdict == "command_failed"
        assert result.reason == "Your plan runs 1 computer at a time."

    @pytest.mark.asyncio
    async def test_a_start_that_fails(self, world):
        world.manager.wake_computer.side_effect = RuntimeError("provider said no")

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.WAKE_FAILED

    @pytest.mark.asyncio
    async def test_a_slow_start_times_the_check_out_but_keeps_going(self, world, monkeypatch):
        monkeypatch.setattr(cc, "CHECK_TIMEOUT_S", 0.1)
        finished = asyncio.Event()

        async def slow_wake(*_a, **_k):
            await asyncio.sleep(0.3)
            finished.set()
            return SimpleNamespace(sandbox=world.sandbox)

        world.manager.wake_computer.side_effect = slow_wake

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.WAKE_TIMEOUT
        # The start is not cut off halfway; the next check finds it up.
        await asyncio.wait_for(finished.wait(), timeout=2)
        world.sandbox.check_user_mcp_server.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_too_little_time_left_after_the_start_blames_the_start(self, world, monkeypatch):
        monkeypatch.setattr(cc, "CHECK_TIMEOUT_S", cc._MIN_RUN_S + cc._RUN_MARGIN_S - 1)

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.WAKE_TIMEOUT
        world.sandbox.check_user_mcp_server.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_run_gets_its_full_budget_on_a_running_computer(self, world):
        await cc.check_command_server(_USER, _server())

        assert world.sandbox.check_user_mcp_server.await_args.kwargs["budget_s"] == cc._RUN_BUDGET_S


class TestAnswers:
    @pytest.mark.asyncio
    async def test_a_command_that_lists_its_tools_passes(self, world):
        result = await cc.check_command_server(_USER, _server())

        assert result == Passed("acme")

    @pytest.mark.asyncio
    async def test_a_server_that_runs_out_of_time_names_the_budget(self, world):
        world.sandbox.check_user_mcp_server.return_value = {"status": "timeout"}

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.NO_ANSWER.format(seconds=int(cc._RUN_BUDGET_S))

    @pytest.mark.asyncio
    async def test_the_run_is_handed_the_checks_deadline(self, world):
        loop = asyncio.get_running_loop()
        before = loop.time()

        await cc.check_command_server(_USER, _server())

        deadline = world.sandbox.check_user_mcp_server.await_args.kwargs["deadline"]
        assert before + cc.CHECK_TIMEOUT_S <= deadline <= loop.time() + cc.CHECK_TIMEOUT_S

    @pytest.mark.asyncio
    async def test_a_run_ended_at_the_checks_deadline_names_the_budget(self, world):
        # The run bounds itself by the deadline it is handed (pinned in
        # test_mcp_command_check_run) and raises there.
        world.sandbox.check_user_mcp_server.side_effect = TimeoutError

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.NO_ANSWER.format(seconds=int(cc._RUN_BUDGET_S))

    @pytest.mark.asyncio
    async def test_a_run_that_answered_keeps_its_answer_however_slow_the_cleanup(
        self, world, monkeypatch
    ):
        """The run answers just before the deadline and its files take far
        longer to go: the answer stands, the wait for the cleanup is short,
        and the cleanup carries on without the request."""
        monkeypatch.setattr(cc, "CHECK_TIMEOUT_S", 0.6)
        monkeypatch.setattr(cc, "_MIN_RUN_S", 0.05)
        monkeypatch.setattr(cc, "_RUN_MARGIN_S", 0.1)
        monkeypatch.setattr(mcp_setup, "CHECK_CLEANUP_WAIT_S", 0.3)
        loop = asyncio.get_running_loop()
        answer_at = loop.time() + cc.CHECK_TIMEOUT_S - 0.1
        cleanup_finished = asyncio.Event()

        async def runtime_call(func, *args, retry_policy, total_timeout=None, **kwargs):
            if func is runtime.exec and args[0].startswith("rm -f "):
                await asyncio.sleep(1.5)  # retries on a flaky link
                cleanup_finished.set()
            return ExecResult("MCP_CHECK_RC=0\n", "", 0)

        async def download(_path):
            await asyncio.sleep(max(0.0, answer_at - loop.time()))
            return b'{"status": "ok", "tools": [{"name": "t", "description": "d"}]}'

        runtime = SimpleNamespace(exec=AsyncMock(), upload_file=AsyncMock())
        computer = SimpleNamespace(
            runtime=runtime,
            _work_dir="/home/workspace",
            _wait_ready=AsyncMock(),
            _runtime_call=runtime_call,
            tool_generator=ToolFunctionGenerator(),
            adownload_file_bytes=download,
        )

        async def real_run(config, vault, **kwargs):
            return await mcp_setup.check_user_mcp_server(computer, config, vault, **kwargs)

        world.sandbox.check_user_mcp_server.side_effect = real_run
        started = loop.time()

        result = await cc.check_command_server(_USER, _server(env={}))

        assert result == Passed("acme"), result
        assert loop.time() - started < cc.CHECK_TIMEOUT_S + 0.3 + 0.2
        assert not cleanup_finished.is_set()
        await asyncio.wait_for(cleanup_finished.wait(), timeout=3)

    @pytest.mark.asyncio
    async def test_a_run_that_breaks_on_the_computer(self, world):
        world.sandbox.check_user_mcp_server.side_effect = ConnectionError("gone")

        result = await cc.check_command_server(_USER, _server())

        assert result.reason == cc.RUN_FAILED

    @pytest.mark.asyncio
    async def test_a_failure_never_quotes_the_value_it_was_started_with(self, world):
        world.sandbox.check_user_mcp_server.return_value = {
            "status": "error",
            "error": (
                "MCP server acme closed connection\nserver stderr tail:\n"
                "Error: key typed-in-value is invalid"
            ),
        }

        result = await cc.check_command_server(
            _USER, _server(), extra_secrets={"ACME_KEY": "typed-in-value"}
        )

        assert result.verdict == "command_failed"
        assert "typed-in-value" not in result.reason
        assert result.reason.startswith("the command exited before it answered: Error: key ")


_NPM_404 = """MCP server acme closed connection
server stderr tail:
npm error code E404
npm error 404 Not Found - GET https://registry.npmjs.org/@acme%2fmcp - Not found
npm error 404
npm error 404  The requested resource '@acme/mcp@1.2.3' could not be found or you do not have permission to access it.
npm error 404
npm error 404 Note that you can also install from a
npm error 404 tarball, folder, http url, or git url.
npm error A complete log of this run can be found in: /root/.npm/_logs/x-debug-0.log"""

_NPM_NO_VERSION = """MCP server acme closed connection
server stderr tail:
npm error code ETARGET
npm error notarget No matching version found for @acme/mcp@99.0.0.
npm error notarget In most cases you or one of your dependencies are requesting
npm error notarget a package version that doesn't exist.
npm error A complete log of this run can be found in: /root/.npm/_logs/x-debug-0.log"""

_TRACEBACK = """MCP server acme closed connection
server stderr tail:
Traceback (most recent call last):
  File "/root/.cache/uv/acme/server.py", line 3, in <module>
    from mcp.shared.exceptions import McpError
ImportError: cannot import name 'McpError' from 'mcp.shared.exceptions'"""


class TestFailureReason:
    @pytest.mark.parametrize(
        "error, expected",
        [
            (
                _NPM_404,
                "the command exited before it answered: The requested resource "
                "'@acme/mcp@1.2.3' could not be found or you do not have "
                "permission to access it.",
            ),
            (
                _NPM_NO_VERSION,
                "the command exited before it answered: notarget No matching "
                "version found for @acme/mcp@99.0.0.",
            ),
            (
                _TRACEBACK,
                "the command exited before it answered: ImportError: cannot import "
                "name 'McpError' from 'mcp.shared.exceptions'",
            ),
            (
                "[Errno 2] No such file or directory: 'npx'",
                "the command 'npx' was not found on your computer",
            ),
            (
                "[Errno 13] Permission denied: 'npx'",
                "the command 'npx' is not allowed to run on your computer",
            ),
            (
                "MCP server acme timed out after 10s",
                "the command started but did not answer in time",
            ),
            (
                "tools/list error: unauthorized",
                "the server refused to list its tools: unauthorized",
            ),
        ],
    )
    def test_reads_as_one_clause(self, error, expected):
        assert cc.command_failure_reason(error, _server(), {}) == expected

    def test_a_long_reason_is_cut(self):
        error = "MCP server acme closed connection\nserver stderr tail:\nError: " + "x" * 900

        reason = cc.command_failure_reason(error, _server(), {})

        assert len(reason) == cc._REASON_MAX
        assert reason.endswith("...")

    def test_resolved_values_are_redacted(self):
        error = "MCP server acme closed connection\nserver stderr tail:\nbad key sk-live-abcdef123"

        reason = cc.command_failure_reason(error, _server(), {"ACME_KEY": "sk-live-abcdef123"})

        assert "sk-live-abcdef123" not in reason

    @pytest.mark.parametrize(
        "server",
        [
            _server(env={"ACME_KEY": "sk-literal-abcdef123"}),
            _server(env={}, args=["-y", "@acme/mcp@1.2.3", "--api-key=sk-literal-abcdef123"]),
            _server(env={}, args=["-y", "@acme/mcp@1.2.3", "--token", "sk-literal-abcdef123"]),
        ],
    )
    def test_a_key_written_straight_into_the_server_is_redacted_too(self, server):
        error = (
            "MCP server acme closed connection\nserver stderr tail:\n"
            "Error: key sk-literal-abcdef123 is invalid"
        )

        reason = cc.command_failure_reason(error, server, {})

        assert "sk-literal" not in reason
        assert reason.endswith("is invalid")

    def test_plain_config_in_the_env_is_left_readable(self):
        error = "MCP server acme closed connection\nserver stderr tail:\nError: region us-east-1 is closed"

        reason = cc.command_failure_reason(error, _server(env={"REGION": "us-east-1"}), {})

        assert reason.endswith("Error: region us-east-1 is closed")

    @pytest.mark.parametrize(
        "error",
        [
            # The client quotes a stdout line cut to 200 characters.
            "Invalid JSON from MCP server acme: 'Using key sk-live-abcdef12'",
            'Invalid JSON from MCP server acme: "it\'s key sk-live-abcdef12"',
            # Each stderr line is kept to its first 4096 characters.
            "MCP server acme closed connection\nserver stderr tail:\n"
            "Error: invalid key sk-live-abcdef12",
        ],
    )
    def test_the_start_of_a_key_the_clients_cap_cut_off_is_redacted(self, error):
        reason = cc.command_failure_reason(error, _server(), {"ACME_KEY": "sk-live-abcdef123456"})

        assert "sk-live-" not in reason
        assert "[REDACTED:ACME_KEY]" in reason

    def test_a_few_characters_that_merely_start_a_key_are_left_alone(self):
        error = "MCP server acme closed connection\nserver stderr tail:\nError: bad sk-li"

        reason = cc.command_failure_reason(error, _server(), {"ACME_KEY": "sk-live-abcdef123456"})

        assert reason.endswith("Error: bad sk-li")
