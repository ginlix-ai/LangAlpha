"""Settled contract of the user's subagent switch inside the agent.

The switch never touches the bound tools or the prompt, so the two things here
are all it does: a durable row whenever the switch differs from the last one
the model can read, and a refusal for any launch while it is off. Both read
the switch fresh on every hook, and a read that does not answer changes
nothing: no row, and the launch goes through.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction.summary_request import _summarizable
from ptc_agent.agent.middleware.runtime_context import (
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    SUBAGENTS_ROW_KIND,
    render_update_row,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.baseline import _retained_change_rows
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
)
from ptc_agent.agent.middleware import subagent_switch
from ptc_agent.agent.middleware.subagent_switch import (
    SUBAGENTS_REFUSAL,
    SubagentSwitchMiddleware,
    last_announced,
)

OFF_TEXT = (
    "The user turned subagents off for this thread. Do the work yourself rather "
    "than delegating it: starting, resuming, or redirecting a subagent will be "
    "refused until the user turns them back on. Subagents already running keep "
    "going, and their results can still be collected."
)
ON_TEXT = (
    "The user turned subagents back on for this thread. Delegate to them when it helps."
)


def _reader(allowed: bool | None = True) -> AsyncMock:
    return AsyncMock(return_value=allowed)


def _switch_row(allowed: bool) -> HumanMessage:
    return build_update_message(
        DurableUpdate(
            kind=SUBAGENTS_ROW_KIND,
            schema_version=1,
            text="",
            provenance={"source": "user", "allowed": allowed},
        )
    )


def _state(*messages, cutoff_index: int | None = None) -> dict:
    state: dict = {"messages": [HumanMessage(content="hi", id="u-0"), *messages]}
    if cutoff_index is not None:
        state["_summarization_event"] = {"cutoff_index": cutoff_index}
    return state


async def _notice(reader: AsyncMock, state: dict) -> list:
    written = await SubagentSwitchMiddleware(reader).abefore_model(state)
    return [] if written is None else written["messages"]


class TestTheNotice:
    @pytest.mark.asyncio
    async def test_a_thread_never_switched_says_nothing(self):
        """On is the default, so a thread that never moved writes no row."""
        assert await _notice(_reader(True), _state()) == []

    @pytest.mark.asyncio
    async def test_off_writes_one_durable_row(self):
        rows = await _notice(_reader(False), _state())

        assert len(rows) == 1
        row = rows[0]
        assert isinstance(row, HumanMessage)
        assert row.additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE
        meta = row.additional_kwargs[RUNTIME_UPDATE_KEY]
        assert meta["kind"] == SUBAGENTS_ROW_KIND
        assert meta["provenance"] == {"source": "user", "allowed": False}
        assert row.content == OFF_TEXT
        # The Pregel path mints ids for a row a hook returns.
        assert row.id is None

    @pytest.mark.asyncio
    async def test_off_already_announced_is_not_repeated(self):
        state = _state(_switch_row(False), AIMessage(content="working"))
        assert await _notice(_reader(False), state) == []

    @pytest.mark.asyncio
    async def test_back_on_writes_one_on_row(self):
        rows = await _notice(_reader(True), _state(_switch_row(False)))

        assert len(rows) == 1
        assert rows[0].content == ON_TEXT
        assert (
            rows[0].additional_kwargs[RUNTIME_UPDATE_KEY]["provenance"]["allowed"]
            is True
        )

    @pytest.mark.asyncio
    async def test_on_already_announced_is_not_repeated(self):
        state = _state(_switch_row(False), _switch_row(True))
        assert await _notice(_reader(True), state) == []

    @pytest.mark.asyncio
    async def test_the_newest_row_decides(self):
        state = _state(_switch_row(False), _switch_row(True), _switch_row(False))
        assert await _notice(_reader(False), state) == []
        assert len(await _notice(_reader(True), state)) == 1

    @pytest.mark.asyncio
    async def test_a_flip_mid_turn_lands_after_the_tool_result(self):
        """The hook runs before every model call, so a flip between two calls
        of one turn reaches the next one."""
        state = _state(
            AIMessage(
                content="",
                tool_calls=[{"id": "c-1", "name": "Bash", "args": {}}],
            ),
            ToolMessage(content="ok", tool_call_id="c-1"),
        )
        rows = await _notice(_reader(False), state)
        assert [r.content for r in rows] == [OFF_TEXT]

    @pytest.mark.asyncio
    async def test_an_off_row_compaction_took_from_view_is_restated(self):
        """The summary drops the row, so the model no longer reads it."""
        state = _state(_switch_row(False), AIMessage(content="later"), cutoff_index=2)
        rows = await _notice(_reader(False), state)
        assert [r.content for r in rows] == [OFF_TEXT]

    @pytest.mark.asyncio
    async def test_on_after_compaction_took_every_row_is_restated(self):
        """The summary leaves the rows out but can keep what a refusal said."""
        state = _state(_switch_row(False), AIMessage(content="later"), cutoff_index=2)
        rows = await _notice(_reader(True), state)
        assert [r.content for r in rows] == [ON_TEXT]

    @pytest.mark.asyncio
    async def test_a_restated_row_is_not_repeated(self):
        state = _state(
            _switch_row(False),
            AIMessage(content="later"),
            _switch_row(True),
            cutoff_index=2,
        )
        assert await _notice(_reader(True), state) == []

    @pytest.mark.asyncio
    async def test_an_unknown_switch_is_not_a_change(self):
        assert await _notice(_reader(None), _state()) == []
        assert await _notice(_reader(None), _state(_switch_row(False))) == []

    @pytest.mark.asyncio
    async def test_a_failed_read_costs_the_row_never_the_turn(self):
        reader = AsyncMock(side_effect=RuntimeError("db down"))
        assert await _notice(reader, _state()) == []

    @pytest.mark.asyncio
    async def test_a_stalled_read_is_given_up_on(self, monkeypatch):
        monkeypatch.setattr(subagent_switch, "_READ_TIMEOUT_S", 0.01)

        async def stalled() -> bool:
            await asyncio.sleep(10)
            return False

        assert await _notice(stalled, _state()) == []

    @pytest.mark.asyncio
    async def test_every_call_reads_the_switch_afresh(self):
        reader = _reader(True)
        middleware = SubagentSwitchMiddleware(reader)
        await middleware.abefore_model(_state())
        await middleware.abefore_model(_state())
        assert reader.await_count == 2

    def test_other_rows_do_not_count(self):
        other = build_update_message(
            DurableUpdate(
                kind="agent_md_changed",
                schema_version=1,
                text="diff",
                provenance={"source": "sandbox", "allowed": False},
            )
        )
        assert last_announced(_state(other)) is True


def _request(name: str, **args) -> MagicMock:
    request = MagicMock()
    request.tool_call = {"id": f"call-{name}", "name": name, "args": args}
    return request


async def _call(reader: AsyncMock, request: MagicMock):
    handler = AsyncMock(return_value=ToolMessage(content="ran", tool_call_id="x"))
    result = await SubagentSwitchMiddleware(reader).awrap_tool_call(request, handler)
    return result, handler


class TestTheGate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "request_",
        [
            _request("Task", action="init", subagent_type="research", description="d"),
            _request("Task", action="update", task_id="k", description="more"),
            _request("Task", action="resume", task_id="k", description="go on"),
            _request("RunWorkflow", script="return 1"),
        ],
        ids=["task-init", "task-update", "task-resume", "run-workflow"],
    )
    async def test_off_refuses_every_launch(self, request_):
        result, handler = await _call(_reader(False), request_)

        handler.assert_not_awaited()
        assert isinstance(result, ToolMessage)
        assert result.status == "error"
        assert result.content == SUBAGENTS_REFUSAL
        assert result.name == request_.tool_call["name"]
        assert result.tool_call_id == request_.tool_call["id"]

    @pytest.mark.asyncio
    async def test_off_still_collects_what_ran(self):
        reader = _reader(False)
        result, handler = await _call(reader, _request("TaskOutput", task_id="k"))

        handler.assert_awaited_once()
        assert result.content == "ran"
        # Only a launch is worth a read.
        reader.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_other_tools_pass_without_a_read(self):
        reader = _reader(False)
        result, handler = await _call(reader, _request("Bash", command="ls"))

        handler.assert_awaited_once()
        reader.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_on_lets_a_launch_through(self):
        result, handler = await _call(_reader(True), _request("Task", action="init"))
        handler.assert_awaited_once()
        assert result.content == "ran"

    @pytest.mark.asyncio
    async def test_an_unknown_switch_fails_open(self):
        result, handler = await _call(_reader(None), _request("RunWorkflow"))
        handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_read_that_raises_fails_open_and_says_so(self, caplog):
        reader = AsyncMock(side_effect=RuntimeError("db down"))
        with caplog.at_level(logging.WARNING):
            result, handler = await _call(reader, _request("Task", action="init"))
        handler.assert_awaited_once()
        assert "switch unread" in caplog.text

    @pytest.mark.asyncio
    async def test_every_launch_reads_the_switch_afresh(self):
        """A flip mid-turn holds the very next launch."""
        reader = AsyncMock(side_effect=[True, False])
        middleware = SubagentSwitchMiddleware(reader)
        handler = AsyncMock(return_value=ToolMessage(content="ran", tool_call_id="x"))

        first = await middleware.awrap_tool_call(
            _request("Task", action="init"), handler
        )
        second = await middleware.awrap_tool_call(
            _request("Task", action="init"), handler
        )

        assert first.content == "ran"
        assert second.content == SUBAGENTS_REFUSAL
        assert handler.await_count == 1

    def test_the_sync_path_never_starts_a_launch(self):
        """The Task tool's sync body runs a subagent, and the switch can only be
        read asynchronously, so a sync launch must not reach it."""
        handler = MagicMock()
        with pytest.raises(NotImplementedError):
            SubagentSwitchMiddleware(_reader(True)).wrap_tool_call(
                _request("Task", action="init"), handler
            )
        handler.assert_not_called()


class TestTheRowText:
    @pytest.mark.parametrize("allowed", [False, True])
    def test_it_names_no_tool(self, allowed):
        """Tool names belong to the bound tools; the row speaks of subagents."""
        rendered = render_update_row(
            DurableUpdate(
                kind=SUBAGENTS_ROW_KIND,
                schema_version=1,
                text="",
                provenance={"source": "user", "allowed": allowed},
            )
        )
        for name in ("Task", "RunWorkflow", "TaskOutput"):
            assert name not in rendered

    def test_it_round_trips_through_history(self):
        update = runtime_update_from_message(_switch_row(False))
        assert update is not None
        assert update.kind == SUBAGENTS_ROW_KIND
        assert update.provenance["allowed"] is False


class TestCompactionLeavesItToRestate:
    def test_the_summarizer_never_reads_it(self):
        """A summary saying "off" would outlive the switch going back on."""
        kept = _summarizable([HumanMessage(content="hi"), _switch_row(False)])
        assert [m.content for m in kept] == ["hi"]

    def test_a_baseline_rebuild_does_not_count_it_as_a_change_row(self):
        change = build_update_message(
            DurableUpdate(
                kind="agent_md_changed",
                schema_version=1,
                text="diff",
                provenance={"source": "sandbox"},
            )
        )
        state = _state(change, _switch_row(False))
        assert _retained_change_rows(state) == ("agent_md_changed",)
