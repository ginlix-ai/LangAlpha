"""ExecuteCode and Bash publish the chart layers their run wrote: the read-back
rides the result's text, the stored layers its artifact, whether the run
succeeded or failed after publishing."""

from __future__ import annotations

import dataclasses
import json
import sys
import types
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.core.sandbox.jobs import Outbox, StoppedJob
from tests.unit.ptc_agent.agent.job_fakes import outcome
from libs.chartkit.tests.samples import sample  # the layer contract's samples, in chartkit's suite

WS = "00000000-0000-4000-8000-00000000aaaa"
_CONFIG = {"configurable": {"workspace_id": WS}}
_ADD = "src.tools.chart_annotation.layers.add_layers"


@pytest.fixture(autouse=True)
def _computer_runs(monkeypatch):
    mod = types.ModuleType("src.server.database.computer_runs")
    mod.record_oom_kill = AsyncMock(return_value=True)
    monkeypatch.setitem(sys.modules, "src.server.database.computer_runs", mod)


@pytest.fixture
def store():
    add = AsyncMock()
    with patch(_ADD, add), patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
        yield add


def _backend(result: Any) -> Any:
    return SimpleNamespace(
        livefs=None,
        settled_livefs=AsyncMock(return_value=None),
        config_folder=AsyncMock(return_value=None),
        provider="docker",
        channel="docker",
        sandbox_id="sbx-1",
        sandbox=SimpleNamespace(resource_tier=None, call_filer=None),
        start_code_job=AsyncMock(return_value=result),
        start_bash_job=AsyncMock(return_value=result),
        stop_job=AsyncMock(return_value=True),
    )


def _ended(exit_code: int = 0, output: str = "done\n", *names: str) -> Any:
    return dataclasses.replace(
        outcome(exit_code=exit_code, output=output),
        outbox=Outbox(tuple(json.dumps(sample(n)) for n in names)),
    )


def _code(result: Any) -> Any:
    return create_execute_code_tool(_backend(result), None)


def _bash(result: Any) -> Any:
    return create_execute_bash_tool(_backend(result))


_CODE_CALL = {"type": "tool_call", "name": "ExecuteCode", "args": {"code": "c.publish()"}, "id": "c1"}
_BASH_CALL = {"type": "tool_call", "name": "Bash", "args": {"command": "python3 x.py"}, "id": "b1"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("build", "call", "head"),
    [(_code, _CODE_CALL, "SUCCESS\ndone\n"), (_bash, _BASH_CALL, "done\n")],
)
async def test_a_run_that_published_returns_the_read_back_and_the_layers(store, build, call, head):
    msg = await build(_ended(0, "done\n", "intraday")).ainvoke(call, _CONFIG)

    assert msg.content.startswith(
        head + "\nPublished chart layer spy-orb 'SPY opening range' on SPY:5min"
    )
    assert [layer["layer_id"] for layer in msg.artifact["chart_layers"]] == ["spy-orb"]
    store.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("build", "call", "head"),
    [
        (_code, _CODE_CALL, "ERROR\nTraceback: boom"),
        (_bash, _BASH_CALL, "ERROR: Command failed (exit code 1)\nTraceback: boom"),
    ],
)
async def test_a_run_that_failed_after_publishing_still_publishes(store, build, call, head):
    msg = await build(_ended(1, "Traceback: boom", "intraday")).ainvoke(call, _CONFIG)

    assert msg.content.startswith(head + "\n\nPublished chart layer spy-orb")
    assert msg.artifact["chart_layers"][0]["chart_id"] == "SPY:5min"


@pytest.mark.asyncio
@pytest.mark.parametrize(("build", "call"), [(_code, _CODE_CALL), (_bash, _BASH_CALL)])
async def test_a_run_that_published_nothing_is_unchanged(store, build, call):
    msg = await build(_ended(0, "done\n")).ainvoke(call, _CONFIG)

    assert "chart layer" not in msg.content
    assert "chart_layers" not in msg.artifact
    store.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_layer_leaves_no_artifact_entry(store):
    bad = sample("intraday")
    bad["timeframe"] = "1d"
    result = dataclasses.replace(outcome(output="ok"), outbox=Outbox((json.dumps(bad),)))

    msg = await _code(result).ainvoke(_CODE_CALL, _CONFIG)

    assert msg.content == (
        "SUCCESS\nok\n\nChart layer 'spy-orb' NOT published (nothing was drawn):\n"
        "  timeframe: must be '1min', '5min', '15min', '30min', '1hour', '4hour' or '1day', got '1d'"
    )
    assert "chart_layers" not in msg.artifact


@pytest.mark.asyncio
async def test_a_layer_the_store_refuses_is_told_and_the_output_kept():
    """A publish never fails the call: the run's output stands and the
    agent is told what was not published."""
    with patch(_ADD, AsyncMock(side_effect=RuntimeError("db down"))):
        msg = await _code(_ended(0, "ok", "intraday")).ainvoke(_CODE_CALL, _CONFIG)
    assert msg.content == (
        "SUCCESS\nok\n\nChart layer 'spy-orb' NOT published: it could not be saved (storage "
        "unavailable), so nothing was drawn. Publish it again in a moment."
    )
    assert "chart_layers" not in msg.artifact


@pytest.mark.asyncio
async def test_a_run_read_at_its_deadline_stop_publishes_too(store):
    """A subagent's run that ended between its last wait and the stop is read
    by the stop and returned as ended, with what it published."""
    backend = _backend(outcome(done=False))
    backend.stop_job_with_trace = AsyncMock(
        return_value=StoppedJob(was_running=False, had_ended=True, mcp_trace=[])
    )
    backend.read_ended_job = AsyncMock(return_value=_ended(0, "late\n", "intraday"))

    msg = await create_execute_code_tool(backend, None).ainvoke(_CODE_CALL, _CONFIG)

    assert msg.content.startswith("SUCCESS\nlate\n\nPublished chart layer spy-orb")
    assert msg.artifact["chart_layers"][0]["layer_id"] == "spy-orb"


@pytest.mark.asyncio
async def test_a_run_stopped_at_its_deadline_publishes_what_it_wrote_once(store):
    """A run still going when its clock runs out is stopped; what it
    published before the stop rides the deadline result, published once."""
    backend = _backend(outcome(done=False))
    backend.stop_job_with_trace = AsyncMock(
        return_value=StoppedJob(
            was_running=True,
            had_ended=False,
            mcp_trace=[],
            outbox=Outbox((json.dumps(sample("intraday")),)),
        )
    )

    msg = await create_execute_code_tool(backend, None).ainvoke(_CODE_CALL, _CONFIG)

    assert msg.content.startswith("ERROR: Stopped after")
    assert "\n\nPublished chart layer spy-orb" in msg.content
    assert [layer["layer_id"] for layer in msg.artifact["chart_layers"]] == ["spy-orb"]
    store.assert_awaited_once()
