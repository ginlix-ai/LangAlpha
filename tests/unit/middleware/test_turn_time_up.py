"""A run near its wall clock is asked for its answer, then its tools stop."""

import time
from contextlib import contextmanager

import pytest
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.runnables.config import var_child_runnable_config

from ptc_agent.agent.limits import REFUSAL_KEY
from ptc_agent.agent.middleware.background_subagent.subagent import _with_own_deadline
from ptc_agent.agent.middleware.runtime_context.durable import (
    last_stated,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import (
    NON_CHANGE_ROW_KINDS,
    TIME_UP_REFUSAL,
    TIME_UP_ROW_KIND,
    TIME_UP_WITHDRAWN_ROW_KIND,
    TURN_DEADLINE_KEY,
    TURN_ROW_KIND,
    TURN_TIME_LIMIT_KEY,
    TimeUpToolStopMiddleware,
    TurnContextMiddleware,
)


@contextmanager
def _deadline(epoch, limit=None):
    configurable = {} if epoch is None else {TURN_DEADLINE_KEY: epoch}
    if limit is not None:
        configurable[TURN_TIME_LIMIT_KEY] = limit
    token = var_child_runnable_config.set({"configurable": configurable})
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)


def _minutes_out(minutes):
    return time.time() + minutes * 60


class _Request:
    tool_call = {"id": "t1", "name": "bash", "args": {"command": "ls"}}
    state: dict = {}


async def _ran(request):
    return ToolMessage(content="ran", tool_call_id="t1")


@pytest.fixture(params=[False, True], ids=["main", "subagent"])
def middleware(request):
    return TurnContextMiddleware(timezone="UTC", is_subagent=request.param)


class TestTimeUpRow:
    def test_none_before_the_window(self, middleware):
        with _deadline(_minutes_out(30)):
            assert middleware._time_up({"messages": [HumanMessage("x")]}) is None

    def test_none_without_a_deadline(self, middleware):
        with _deadline(None):
            assert middleware._time_up({"messages": [HumanMessage("x")]}) is None

    def test_one_row_in_the_window_and_again_after_compaction(self, middleware):
        with _deadline(_minutes_out(15)):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
            in_view = {"messages": [HumanMessage("x"), row]}
            assert last_stated(in_view, TIME_UP_ROW_KIND, "deadline") is not None
            assert middleware._time_up({"messages": [HumanMessage("x"), row]}) is None
            assert middleware._time_up({"messages": [HumanMessage("summary")]}) is not None

    def test_the_text_says_whether_tools_have_stopped(self, middleware):
        with _deadline(_minutes_out(15.5)):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
        assert "your tools stop running 10 minutes before that" in str(row.content)
        with _deadline(_minutes_out(6.7)):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
        assert "It ends in 7 minutes, and your tools have stopped running." in str(row.content)
        with _deadline(_minutes_out(1.2)):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
        assert "It ends in 1 minute, and your tools have stopped running." in str(row.content)

    def test_it_is_a_non_change_row(self):
        assert TIME_UP_ROW_KIND in NON_CHANGE_ROW_KINDS


def _kinds(written):
    return [runtime_update_from_message(m).kind for m in (written or {}).get("messages", [])]


class TestAStaleAskIsWithdrawn:
    """A time-up row outlives its run; the next run's clock takes it back."""

    @staticmethod
    def _stale_row(middleware):
        with _deadline(_minutes_out(5)):
            return middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]

    def test_a_new_clock_takes_the_ask_back_once(self, middleware):
        stale = self._stale_row(middleware)
        thread = [HumanMessage("x"), stale, HumanMessage("go on")]
        with _deadline(_minutes_out(6 * 60), limit=6 * 3600):
            written = middleware._time_up({"messages": thread})
            assert _kinds(written) == [TIME_UP_WITHDRAWN_ROW_KIND]
            row = written["messages"][0]
            assert str(row.content).strip() == (
                "The earlier time limit no longer applies\n\n"
                "This turn has a new time limit, so the note above about it "
                "running out is withdrawn, and your tools run again."
            )
            assert middleware._time_up({"messages": [*thread, row]}) is None

    def test_the_next_ask_on_the_new_clock_still_lands(self, middleware):
        stale = self._stale_row(middleware)
        thread = [HumanMessage("x"), stale, HumanMessage("go on")]
        later = _minutes_out(6 * 60)
        with _deadline(later, limit=6 * 3600):
            withdrawn = middleware._time_up({"messages": thread})["messages"][0]
        thread = [*thread, withdrawn]
        with _deadline(_minutes_out(15), limit=6 * 3600):
            assert _kinds(middleware._time_up({"messages": thread})) == [TIME_UP_ROW_KIND]

    def test_a_new_clock_already_near_its_end_asks_instead(self, middleware):
        stale = self._stale_row(middleware)
        with _deadline(_minutes_out(15)):
            written = middleware._time_up({"messages": [HumanMessage("x"), stale]})
        assert _kinds(written) == [TIME_UP_ROW_KIND]

    def test_nothing_to_take_back_without_an_ask_in_view(self, middleware):
        stale = self._stale_row(middleware)
        with _deadline(_minutes_out(6 * 60), limit=6 * 3600):
            assert middleware._time_up({"messages": [HumanMessage("x")]}) is None
            # A compaction took the ask out of view along with what it was about.
            assert middleware._time_up({"messages": [HumanMessage("summary")]}) is None
        with _deadline(None):
            assert middleware._time_up({"messages": [HumanMessage("x"), stale]}) is None

    def test_the_same_clock_has_nothing_to_take_back(self, middleware):
        with _deadline(_minutes_out(5)):
            stale = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
            assert middleware._time_up({"messages": [HumanMessage("x"), stale]}) is None

    def test_it_is_a_non_change_row(self):
        assert TIME_UP_WITHDRAWN_ROW_KIND in NON_CHANGE_ROW_KINDS

    @pytest.mark.asyncio
    async def test_a_resumed_run_reads_the_ask_on_its_first_call(self):
        """A resumed interrupt skips the entry hook, so the first model call
        opens the turn; it carries the withdrawal with the anchor."""
        stale = self._stale_row(TurnContextMiddleware(timezone="UTC"))
        thread = {"messages": [HumanMessage("x"), stale]}
        with _deadline(_minutes_out(6 * 60), limit=6 * 3600):
            written = await TurnContextMiddleware(timezone="UTC").abefore_model(thread)
        assert _kinds(written) == [TURN_ROW_KIND, TIME_UP_WITHDRAWN_ROW_KIND]
        with _deadline(_minutes_out(15)):
            written = await TurnContextMiddleware(timezone="UTC").abefore_model(
                {"messages": [HumanMessage("x")]}
            )
        assert _kinds(written) == [TURN_ROW_KIND, TIME_UP_ROW_KIND]


@pytest.fixture
def stop():
    return TimeUpToolStopMiddleware()


class TestToolStop:
    @pytest.mark.asyncio
    async def test_tools_run_before_the_stop(self, stop):
        with _deadline(_minutes_out(15)):
            assert (await stop.awrap_tool_call(_Request(), _ran)).content == "ran"

    @pytest.mark.asyncio
    async def test_tools_run_without_a_clock(self, stop):
        with _deadline(None):
            assert (await stop.awrap_tool_call(_Request(), _ran)).content == "ran"

    @pytest.mark.asyncio
    async def test_tools_refused_in_the_last_minutes(self, stop):
        async def _never(request):
            raise AssertionError("a refused call must not reach the handler")

        with _deadline(_minutes_out(5)):
            result = await stop.awrap_tool_call(_Request(), _never)
        assert result.status == "error"
        assert result.content == TIME_UP_REFUSAL
        assert result.tool_call_id == "t1"
        assert result.name == "bash"
        assert result.additional_kwargs == {REFUSAL_KEY: "time_up"}

    def test_the_turn_row_no_longer_wraps_tools(self):
        """The stop left the row middleware, which sits inside the Task
        interceptor and the file-op stream in every stack."""
        assert "awrap_tool_call" not in vars(TurnContextMiddleware)


class TestShortClock:
    def test_a_twelve_minute_clock_asks_at_four_and_stops_at_two(self, middleware):
        with _deadline(_minutes_out(5), limit=720):
            assert middleware._time_up({"messages": [HumanMessage("x")]}) is None
        with _deadline(_minutes_out(3.5), limit=720):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
        assert "your tools stop running 2 minutes before that" in str(row.content)

    @pytest.mark.asyncio
    async def test_its_tools_run_until_the_last_two_minutes(self, stop):
        with _deadline(_minutes_out(3), limit=720):
            assert (await stop.awrap_tool_call(_Request(), _ran)).content == "ran"
        with _deadline(_minutes_out(1.5), limit=720):
            assert (await stop.awrap_tool_call(_Request(), _ran)).status == "error"

    def test_a_long_clock_keeps_the_fixed_windows(self, middleware):
        with _deadline(_minutes_out(19), limit=6 * 3600):
            row = middleware._time_up({"messages": [HumanMessage("x")]})["messages"][0]
        assert "your tools stop running 10 minutes before that" in str(row.content)


class TestSubagentClock:
    def test_replaces_the_launchers_deadline(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
        before = time.time()
        config = _with_own_deadline({"configurable": {TURN_DEADLINE_KEY: 1.0, "thread_id": "t"}})
        assert config["configurable"]["thread_id"] == "t"
        assert config["configurable"][TURN_TIME_LIMIT_KEY] == 600
        assert before + 600 <= config["configurable"][TURN_DEADLINE_KEY] <= time.time() + 600

    def test_no_clock_when_timing_is_off(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0)
        config = _with_own_deadline({"configurable": {TURN_DEADLINE_KEY: 1.0}})
        assert TURN_DEADLINE_KEY not in config["configurable"]

    def test_an_owner_that_kills_sooner_shortens_it(self, monkeypatch):
        """A workflow child's clock is its driver's kill, so the short-clock
        scaling asks for its answer in time for it."""
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 6 * 3600)
        config = _with_own_deadline({"configurable": {}}, 1800)
        assert config["configurable"][TURN_TIME_LIMIT_KEY] == 1800
        assert config["configurable"][TURN_DEADLINE_KEY] <= time.time() + 1800

    def test_the_turns_clock_still_caps_a_longer_owner(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
        config = _with_own_deadline({"configurable": {}}, 1800)
        assert config["configurable"][TURN_TIME_LIMIT_KEY] == 600

    def test_an_owner_clock_applies_with_timing_off(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0)
        config = _with_own_deadline({"configurable": {}}, 1800)
        assert config["configurable"][TURN_TIME_LIMIT_KEY] == 1800

    def test_a_spent_owner_clock_is_ignored(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
        config = _with_own_deadline({"configurable": {}}, -5)
        assert config["configurable"][TURN_TIME_LIMIT_KEY] == 600
