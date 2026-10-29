"""A subagent that runs past its own clock is stopped, and settles as a timeout.

The ask for its answer and the tool stop come first; this is the backstop for
a run that keeps going anyway, as the server's stop is for a turn. It closes
the graph it stops, within a bound, and settles where every surface can see
it: an error to the ledger, so the report-back fires, and a timeout in memory,
so TaskOutput tells the agent to narrow the task.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from langchain_core.messages import AIMessage

from ptc_agent.agent.middleware.background_subagent import run_executor
from ptc_agent.agent.middleware.background_subagent import subagent as subagent_mod
from ptc_agent.agent.middleware.background_subagent.outcomes import (
    Outcome,
    SubagentTimeLimitError,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.tools import _terminal_notice
from src.utils.stream_coalescing import HEARTBEAT_EVENT_TYPE
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

BEAT = ("custom", {"type": HEARTBEAT_EVENT_TYPE})


class _BeatingGraph:
    """A graph whose model call never returns: it only beats."""

    def __init__(self, *, cleanup_s: float = 0.0) -> None:
        self.closed = False
        self.cleanup_s = cleanup_s

    async def astream(self, state, config, stream_mode=None, durability=None):
        try:
            while True:
                await asyncio.sleep(0.01)
                yield BEAT
        finally:
            await asyncio.sleep(self.cleanup_s)
            self.closed = True


class _AnsweringGraph:
    async def astream(self, state, config, stream_mode=None, durability=None):
        yield ("values", {"messages": [AIMessage(content="done")]})


@pytest.fixture
def short_clock(monkeypatch):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0.05)
    monkeypatch.setattr(subagent_mod, "SUBAGENT_STOP_GRACE_S", 0.0)


class TestTheStop:
    @pytest.mark.asyncio
    async def test_a_run_past_its_clock_is_stopped_and_its_graph_closed(self, short_clock):
        graph = _BeatingGraph()
        started = time.monotonic()
        with pytest.raises(SubagentTimeLimitError):
            await subagent_mod.arun_subagent_streaming(
                graph, {"messages": []}, {"configurable": {}}, registry=None
            )
        assert time.monotonic() - started < 2
        assert graph.closed

    @pytest.mark.asyncio
    async def test_the_close_is_bounded(self, short_clock, monkeypatch):
        monkeypatch.setattr(subagent_mod, "SUBAGENT_CLOSE_TIMEOUT_S", 0.05)
        graph = _BeatingGraph(cleanup_s=0.5)
        started = time.monotonic()
        with pytest.raises(SubagentTimeLimitError):
            await subagent_mod.arun_subagent_streaming(
                graph, {"messages": []}, {"configurable": {}}, registry=None
            )
        assert time.monotonic() - started < 0.4
        assert not graph.closed
        # The close is left to finish on its own rather than abandoned.
        await asyncio.sleep(0.6)
        assert graph.closed

    @pytest.mark.asyncio
    async def test_a_run_inside_its_clock_is_left_alone(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
        state = await subagent_mod.arun_subagent_streaming(
            _AnsweringGraph(), {"messages": []}, {"configurable": {}}, registry=None
        )
        assert state["messages"][0].content == "done"

    @pytest.mark.asyncio
    async def test_a_run_nothing_times_is_never_stopped(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0)
        state = await subagent_mod.arun_subagent_streaming(
            _AnsweringGraph(), {"messages": []}, {"configurable": {}}, registry=None
        )
        assert state["messages"][0].content == "done"


class TestTheSettle:
    def test_an_error_to_the_ledger_and_a_timeout_in_memory(self):
        outcome = Outcome.from_exception(SubagentTimeLimitError("Timed out after 6s"))
        assert outcome.status == "error"
        assert outcome.as_result()["status"] == "timeout"
        assert outcome.as_failure() == {
            "error": "Timed out after 6s",
            "error_type": "SubagentTimeLimitError",
        }

    def test_other_failures_keep_one_status(self):
        assert Outcome.from_exception(RuntimeError("x")).as_result()["status"] == "error"

    @pytest.mark.asyncio
    async def test_the_writer_settles_every_surface(self, short_clock):
        class _Ledger:
            def __init__(self) -> None:
                self.statuses: list[str] = []
                self.outcomes: list[str] = []

            async def finalize_task_run(self, task_run_id, status, **kwargs):
                self.statuses.append(status)
                return {"applied": True, "run": {"status": status}}

            async def append_run_end(self, task_run_id, **kwargs):
                self.outcomes.append(kwargs["outcome"])

        registry = BackgroundTaskRegistry()
        ledger = registry.run_ledger = _Ledger()
        task = await registry.register(
            tool_call_id="tc-1",
            description="d",
            prompt="p",
            subagent_type="general-purpose",
        )
        task.task_run_id = "run-1"

        async def handler(_request):
            return await subagent_mod.arun_subagent_streaming(
                _BeatingGraph(),
                {"messages": []},
                {"configurable": {}},
                registry=registry,
                tool_call_id="tc-1",
            )

        writer = asyncio.create_task(
            run_executor._run_background_task(
                task, handler, None, PerCallTokenTracker(), "Subagent", registry=registry
            )
        )
        await asyncio.wait_for(writer, timeout=5)
        task.adopt_writer_outcome(writer)

        # The ledger reads an error, so the report-back fires: a cancel never
        # reports back, and nobody chose this stop.
        assert ledger.statuses == ["error"]
        assert ledger.outcomes == ["error"]
        assert task.terminal_status == "timeout"
        assert "ran past its time limit" in _terminal_notice(task)
