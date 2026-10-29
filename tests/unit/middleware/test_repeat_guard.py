"""The repeat guard refuses a call once it has answered alike three times in a row,
and a status read once three rounds in a row found the work still running, and
ends the run once three rounds in a row had every call refused."""

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from ptc_agent.agent.limits import (
    REFUSAL_KEY,
    is_pending_read,
    is_refusal,
    pending_read,
)
from ptc_agent.agent.middleware import repeat_guard
from ptc_agent.agent.middleware.repeat_guard import (
    END_LIMIT,
    POLL_LIMIT,
    POLL_REFUSAL,
    REPEAT_LIMIT,
    REPEAT_REFUSAL,
    RepeatCallGuardMiddleware,
    _call_key,
    poll_streak,
    refused_streak,
    repeat_streak,
)

POLL = {"name": "agent_output", "args": {"thread": "t1"}}
KEY = _call_key(POLL["name"], POLL["args"])

# A repeat refusal as rows written before refusals were tagged carry it.
UNTAGGED_REFUSAL = (
    "Not run: this exact call has returned the same result 3 times in a row, so "
    "another call will not change it. If you are waiting on work that reports "
    "back when it finishes, end your turn now and it will. Otherwise do "
    "something different, or answer with what you have."
)


def _round(call_id, result, *, call=POLL, status="success"):
    return [
        AIMessage(content="", tool_calls=[{**call, "id": call_id}]),
        ToolMessage(content=result, tool_call_id=call_id, status=status),
    ]


def _polls(results):
    messages = [HumanMessage("go")]
    for i, result in enumerate(results):
        messages += _round(f"c{i}", result)
    return messages


def _read(call_id, *, call=POLL, text=None):
    """A status read that found its work still running; its text differs every
    time, as a live read's does (elapsed time, growing output)."""
    return ToolMessage(
        content=text or f"running, elapsed {call_id}",
        tool_call_id=call_id,
        artifact=pending_read(),
    )


def _read_rounds(n, *, calls=(POLL,)):
    messages = [HumanMessage("go")]
    for i in range(n):
        ids = [f"r{i}_{j}" for j in range(len(calls))]
        messages.append(
            AIMessage(content="", tool_calls=[{**c, "id": id_} for c, id_ in zip(calls, ids)])
        )
        messages += [_read(id_, call=c) for c, id_ in zip(calls, ids)]
    return messages


def _refused_round(call_id, kind="poll", call=POLL):
    return [
        AIMessage(content="", tool_calls=[{**call, "id": call_id}]),
        ToolMessage(
            content="any words",
            tool_call_id=call_id,
            status="error",
            additional_kwargs={REFUSAL_KEY: kind},
        ),
    ]


class _Request:
    def __init__(self, messages, call=POLL):
        self.tool_call = {**call, "id": "next"}
        self.state = {"messages": messages}


async def _ran(request):
    return ToolMessage(content="ran", tool_call_id="next")


class TestRepeatStreak:
    def test_counts_identical_results_back_to_back(self):
        assert repeat_streak(_polls(["running"] * 3), KEY) == 3

    def test_a_different_result_ends_the_streak(self):
        assert repeat_streak(_polls(["running", "running", "done"]), KEY) == 1

    def test_another_call_ends_the_streak(self):
        other = {"name": "bash", "args": {"command": "ls"}}
        messages = _polls(["running"] * 2) + _round("o", "running", call=other)
        messages += _round("c9", "running")
        assert repeat_streak(messages, KEY) == 1

    def test_a_message_from_outside_the_loop_ends_it(self):
        assert repeat_streak(_polls(["running"] * 3) + [HumanMessage("row")], KEY) == 0

    def test_a_refusal_neither_counts_nor_ends_it(self):
        messages = _polls(["running"] * 3)
        messages += [
            AIMessage(content="", tool_calls=[{**POLL, "id": "r"}]),
            ToolMessage(
                content="any words",
                tool_call_id="r",
                status="error",
                additional_kwargs={REFUSAL_KEY: "time_up"},
            ),
        ]
        assert repeat_streak(messages, KEY) == 3

    def test_an_untagged_refusal_from_before_the_tag_still_reads_as_one(self):
        messages = _polls(["running"] * 3)
        messages += _round("r", UNTAGGED_REFUSAL, status="error")
        assert repeat_streak(messages, KEY) == 3

    def test_output_that_only_reads_like_a_refusal_is_a_result(self):
        """Tool output is anyone's text: a page or a command's stdout that opens
        with the refusal's words must not hide its own loop from the guard."""
        assert not is_refusal(ToolMessage(content=UNTAGGED_REFUSAL, tool_call_id="x"))
        assert repeat_streak(_polls([UNTAGGED_REFUSAL] * 3), KEY) == 3


class TestPollStreak:
    def test_counts_rounds_of_pending_reads_however_their_text_reads(self):
        rounds, reads = poll_streak(_read_rounds(3))
        assert rounds == 3
        assert {_call_key(c["name"], c["args"]) for c in reads} == {KEY}

    def test_reads_batched_in_one_message_count_once(self):
        other = {"name": "TaskOutput", "args": {"task_id": "b"}}
        assert poll_streak(_read_rounds(2, calls=(POLL, other)))[0] == 2

    def test_a_round_with_any_other_result_ends_it(self):
        messages = _read_rounds(2)
        work = {"name": "bash", "args": {"command": "ls"}}
        messages += [
            AIMessage(content="", tool_calls=[{**POLL, "id": "m1"}, {**work, "id": "m2"}]),
            _read("m1"),
            ToolMessage(content="a.txt", tool_call_id="m2"),
        ]
        messages += _read_rounds(1)[1:]
        assert poll_streak(messages)[0] == 1

    def test_a_read_that_found_the_work_done_ends_it(self):
        messages = _read_rounds(3) + _round("d", "completed: the answer")
        assert poll_streak(messages)[0] == 0

    def test_a_refused_round_neither_counts_nor_ends_it(self):
        messages = _read_rounds(3) + _refused_round("x", "time_up")
        assert poll_streak(messages)[0] == 3

    def test_the_round_being_run_does_not_count(self):
        """Its calls are checked one by one before any of its results land."""
        messages = _read_rounds(3) + [AIMessage(content="", tool_calls=[{**POLL, "id": "n"}])]
        assert poll_streak(messages)[0] == 3

    def test_the_marks_survive_the_checkpoint(self):
        """The guard reads them off a reloaded thread too, so they must come back."""
        serde = JsonPlusSerializer()
        refusal = _refused_round("x", "poll")[1]
        for message in (_read("c1"), refusal):
            back = serde.loads_typed(serde.dumps_typed(message))
            assert back.artifact == message.artifact
            assert back.additional_kwargs == message.additional_kwargs
        assert is_pending_read(serde.loads_typed(serde.dumps_typed(_read("c1"))))
        assert is_refusal(serde.loads_typed(serde.dumps_typed(refusal)))


class TestCost:
    def test_serializes_only_the_calls_the_walk_reaches(self, monkeypatch):
        """With no step cap the tail is the whole turn, and the walk runs on the
        event loop before every call: it must stop where the streak does."""
        keyed = []
        call_key = repeat_guard._call_key
        monkeypatch.setattr(
            repeat_guard, "_call_key", lambda *a: keyed.append(a) or call_key(*a)
        )
        other = {"name": "bash", "args": {"command": "x" * 3000}}
        messages = [HumanMessage("go")]
        for i in range(5000):
            messages += _round(f"o{i}", "out", call=other)
        for i in range(3):
            messages += _round(f"c{i}", "running")

        assert repeat_streak(messages, KEY) == 3
        assert len(keyed) == 4


class TestGuard:
    @pytest.mark.asyncio
    async def test_runs_until_the_limit(self):
        guard = RepeatCallGuardMiddleware()
        result = await guard.awrap_tool_call(_Request(_polls(["running"] * (REPEAT_LIMIT - 1))), _ran)
        assert result.content == "ran"

    @pytest.mark.asyncio
    async def test_refuses_the_next_identical_call(self):
        guard = RepeatCallGuardMiddleware()
        result = await guard.awrap_tool_call(_Request(_polls(["running"] * REPEAT_LIMIT)), _ran)
        assert result.status == "error"
        assert result.content == REPEAT_REFUSAL.format(count=REPEAT_LIMIT)
        assert result.tool_call_id == "next"
        assert result.additional_kwargs == {REFUSAL_KEY: "repeat"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("history_args", "request_args"),
        [
            # Live state: the parser rewrote the AIMessage's args in place, and
            # the request reaching the guard still holds what the model wrote.
            ({"items": ["a", "b"]}, {"items": '["a","b"]'}),
            # A thread reloaded from its checkpoint holds what the model wrote.
            ({"items": '["a","b"]'}, {"items": ["a", "b"]}),
            # Write's string-valued payload, encoded rather than decoded.
            ({"content": '{"k": 1}'}, {"content": {"k": 1}}),
        ],
    )
    async def test_args_match_whichever_side_was_parsed(self, history_args, request_args):
        name = "Write" if "content" in history_args else "pick"
        messages = [HumanMessage("go")]
        for i in range(REPEAT_LIMIT):
            messages += _round(f"c{i}", "same", call={"name": name, "args": history_args})
        request = _Request(messages, call={"name": name, "args": request_args})
        result = await RepeatCallGuardMiddleware().awrap_tool_call(request, _ran)
        assert result.content == REPEAT_REFUSAL.format(count=REPEAT_LIMIT)

    @pytest.mark.asyncio
    async def test_other_args_still_run(self):
        guard = RepeatCallGuardMiddleware()
        other = {"name": "agent_output", "args": {"thread": "t2"}}
        result = await guard.awrap_tool_call(
            _Request(_polls(["running"] * REPEAT_LIMIT), call=other), _ran
        )
        assert result.content == "ran"


class TestPollGuard:
    @pytest.mark.asyncio
    async def test_runs_until_the_limit(self):
        guard = RepeatCallGuardMiddleware()
        result = await guard.awrap_tool_call(_Request(_read_rounds(POLL_LIMIT - 1)), _ran)
        assert result.content == "ran"

    @pytest.mark.asyncio
    async def test_refuses_the_next_read_of_that_work(self):
        guard = RepeatCallGuardMiddleware()
        result = await guard.awrap_tool_call(_Request(_read_rounds(POLL_LIMIT)), _ran)
        assert result.status == "error"
        assert result.content == POLL_REFUSAL.format(count=POLL_LIMIT)
        assert result.tool_call_id == "next"
        assert result.additional_kwargs == {REFUSAL_KEY: "poll"}

    @pytest.mark.asyncio
    async def test_keeps_refusing_until_the_agent_does_something_else(self):
        guard = RepeatCallGuardMiddleware()
        messages = _read_rounds(POLL_LIMIT) + _refused_round("x")
        refused = await guard.awrap_tool_call(_Request(messages), _ran)
        assert refused.additional_kwargs == {REFUSAL_KEY: "poll"}

        messages += _round("w", "a.txt", call={"name": "bash", "args": {"command": "ls"}})
        assert (await guard.awrap_tool_call(_Request(messages), _ran)).content == "ran"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "call",
        [
            # A read of other work has not been found running yet.
            {"name": "agent_output", "args": {"thread": "t2"}},
            # Stopping the work, or anything else, is the different thing the
            # refusal asks for.
            {"name": "manage_threads", "args": {"action": "stop", "thread_id": "t1"}},
        ],
        ids=["other work", "a stop"],
    )
    async def test_any_other_call_still_runs(self, call):
        guard = RepeatCallGuardMiddleware()
        result = await guard.awrap_tool_call(_Request(_read_rounds(POLL_LIMIT), call=call), _ran)
        assert result.content == "ran"


class TestEndOfRun:
    def test_counts_rounds_where_every_call_was_refused_of_any_kind(self):
        messages = [HumanMessage("go")]
        for i, kind in enumerate(["repeat", "poll", "time_up"]):
            messages += _refused_round(f"x{i}", kind)
        assert refused_streak(messages) == 3

    def test_a_round_with_a_call_that_ran_ends_it(self):
        work = {"name": "bash", "args": {"command": "ls"}}
        messages = [HumanMessage("go"), *_refused_round("x0"), *_refused_round("x1")]
        messages += [
            AIMessage(content="", tool_calls=[{**POLL, "id": "m1"}, {**work, "id": "m2"}]),
            _refused_round("m1")[1],
            ToolMessage(content="a.txt", tool_call_id="m2"),
        ]
        assert refused_streak(messages) == 0

    @pytest.mark.asyncio
    async def test_ends_before_the_next_model_call_at_the_limit(self):
        guard = RepeatCallGuardMiddleware()
        messages = [HumanMessage("go")]
        for i in range(END_LIMIT - 1):
            messages += _refused_round(f"x{i}")
        assert await guard.abefore_model({"messages": messages}) is None
        messages += _refused_round("last", "time_up")
        assert await guard.abefore_model({"messages": messages}) == {"jump_to": "end"}
        # A user's message, a steer or a report, starts the count over.
        assert await guard.abefore_model({"messages": [*messages, HumanMessage("and?")]}) is None

    @pytest.mark.asyncio
    async def test_the_run_completes_with_every_call_answered(self):
        """Through create_agent: the jump is its own exit, so the after_agent
        hooks run, nothing is raised, and the thread can take its next turn."""

        @tool("lookup")
        def lookup(query: str) -> str:
            """Look something up."""
            return "42"

        class Repeats(BaseChatModel):
            calls: int = 0

            @property
            def _llm_type(self):
                return "repeats"

            def bind_tools(self, tools, **kwargs):
                return self

            def _generate(self, *args, **kwargs):
                raise NotImplementedError

            async def _agenerate(self, messages, **kwargs):
                self.calls += 1
                if self.calls > 20:
                    message = AIMessage(content="never stopped")
                elif isinstance(messages[-1], HumanMessage) and messages[-1].content == "and?":
                    message = AIMessage(content="It is 42.")
                else:
                    call = {"name": "lookup", "args": {"query": "q"}, "id": f"c{self.calls}"}
                    message = AIMessage(content="", tool_calls=[{**call, "type": "tool_call"}])
                return ChatResult(generations=[ChatGeneration(message=message)])

        ended = []

        class AfterAgent(AgentMiddleware):
            async def aafter_agent(self, state, runtime):
                ended.append(len(state["messages"]))

        model = Repeats()
        agent = create_agent(
            model,
            tools=[lookup],
            middleware=[RepeatCallGuardMiddleware(), AfterAgent()],
            checkpointer=InMemorySaver(),
        )
        config = {"configurable": {"thread_id": "t"}}
        state = await agent.ainvoke({"messages": [HumanMessage("go")]}, config)

        messages = state["messages"]
        # Three calls ran, then three were refused, and no seventh model call.
        assert model.calls == REPEAT_LIMIT + END_LIMIT
        assert sum(is_refusal(m) for m in messages) == END_LIMIT
        assert isinstance(messages[-1], ToolMessage)
        answered = {m.tool_call_id for m in messages if isinstance(m, ToolMessage)}
        assert all(c["id"] in answered for m in messages if isinstance(m, AIMessage) for c in m.tool_calls)
        assert ended == [len(messages)]
        assert (await agent.aget_state(config)).next == ()

        state = await agent.ainvoke({"messages": [HumanMessage("and?")]}, config)
        assert state["messages"][-1].content == "It is 42."
