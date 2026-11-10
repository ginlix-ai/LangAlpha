"""What the turn adds to every Bash and ExecuteCode job's environment (the
user's chart views, ``PTC_CHART_VIEWS``) rides ``configurable`` as
``JOB_ENV_KEY`` and reaches each job through its ``JobContext``; a name the
job sets itself wins over it."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.agent.tools.command_handoff import JOB_ENV_KEY
from ptc_agent.core.sandbox import execution
from chartkit.protocol import (
    CHART_FILE_ENV,
    CHART_VIEWS_ENV,
    SNAPSHOT_DIR_ENV,
)
from ptc_agent.core.sandbox.jobs import OUTBOX_FILE, JobContext, JobRef
from tests.unit.ptc_agent.agent.job_fakes import outcome, tool_call

_VIEWS = '[{"symbol":"NVDA"}]'


class TestJobEnv:
    def _sandbox(self) -> SimpleNamespace:
        workspace = SimpleNamespace(
            mcp_client_config="/root/ws/.mcp.json", join=lambda rel: f"/root/ws/{rel}"
        )
        return SimpleNamespace(
            workspace=lambda: workspace,
            layout=SimpleNamespace(cli_token="/root/_internal/la/token"),
        )

    def _ref(self) -> JobRef:
        return JobRef(
            command_id="c1",
            state_dir="/root/.jobs/c1",
            log_path="/root/ws/log",
            trace_path="/root/.jobs/c1/trace",
            log_relpath="log",
        )

    def _env(self, context: JobContext) -> dict[str, str]:
        return execution._job_env(self._sandbox(), self._ref(), context)

    def test_the_turn_adds_its_views(self):
        env = self._env(JobContext(thread_id="t1", env={CHART_VIEWS_ENV: _VIEWS}))
        assert env[CHART_VIEWS_ENV] == _VIEWS
        assert env["MCP_TRACE_FILE"] == "/root/.jobs/c1/trace"
        assert env[CHART_FILE_ENV] == f"/root/.jobs/c1/{OUTBOX_FILE}"
        assert env[SNAPSHOT_DIR_ENV] == "/root/ws/.agents/threads/t1/charts"

    def test_a_name_of_the_job_own_wins(self):
        env = self._env(JobContext(env={"MCP_TRACE_FILE": "/elsewhere"}))
        assert env["MCP_TRACE_FILE"] == "/root/.jobs/c1/trace"

    def test_no_views_no_name(self):
        env = self._env(JobContext(thread_id="t1"))
        assert CHART_VIEWS_ENV not in env


class TestTheToolsPassThem:
    """A subagent's call reads the same config, which inherits the turn's."""

    def _backend(self) -> SimpleNamespace:
        return SimpleNamespace(
            start_code_job=AsyncMock(return_value=outcome(output="ok")),
            start_bash_job=AsyncMock(return_value=outcome(output="ok")),
            livefs=None,
            settled_livefs=AsyncMock(return_value=None),
            config_folder=AsyncMock(return_value=None),
            provider="docker",
            channel="docker",
            sandbox=SimpleNamespace(call_filer=None),
        )

    @pytest.mark.asyncio
    async def test_execute_code(self):
        backend = self._backend()
        config = {"configurable": {JOB_ENV_KEY: {CHART_VIEWS_ENV: _VIEWS}}}
        await create_execute_code_tool(backend, None).ainvoke(
            tool_call("ExecuteCode", code="run()"), config
        )
        context = backend.start_code_job.await_args.kwargs["context"]
        assert context.env == {CHART_VIEWS_ENV: _VIEWS}

    @pytest.mark.asyncio
    async def test_bash(self):
        backend = self._backend()
        config = {"configurable": {JOB_ENV_KEY: {CHART_VIEWS_ENV: _VIEWS}}}
        await create_execute_bash_tool(backend).ainvoke(tool_call("Bash", command="ls"), config)
        context = backend.start_bash_job.await_args.kwargs["context"]
        assert context.env == {CHART_VIEWS_ENV: _VIEWS}

    @pytest.mark.asyncio
    async def test_none_without_views(self):
        backend = self._backend()
        await create_execute_bash_tool(backend).ainvoke(tool_call("Bash", command="ls"), {})
        assert backend.start_bash_job.await_args.kwargs["context"].env == {}
