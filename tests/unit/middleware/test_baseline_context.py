"""Settled contracts of the per-thread runtime-context baseline.

Four things here are load-bearing enough to pin.

The block must be byte-identical across the calls of one turn, because that is
the whole reason a cache breakpoint can sit on it; a mid-turn agent.md write
must not move it. The epoch must rebuild on compaction and only on compaction,
because compaction is the one event that can drop the block out of the model's
retained history. A cross-writer change must reach the model as a durable row
with a diff, written into history since the block itself never gets edited. And
the wire must carry exactly four Anthropic breakpoints, which is the whole
budget, with the fourth on the last row in history rather than on anything
this call will drop.

The fifth is a regression: the ``<user_profile>`` component carries steering
(the locale language rule, the HTML output routing, the profile file table),
not just identity, and it has to keep rendering now that the middleware that
used to own it is gone.

The sixth is the same four contracts reached from a different kind of source.
The MCP roster and the skills manifest are harness-authored text rather than
files, and they were appended to the system message per call until they moved
in here, so they are pinned to freeze and to file rows the way a file does.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic.chat_models import ChatAnthropic
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool

from ptc_agent.agent.middleware.runtime_context import (
    ENVELOPE_OPEN,
    MAX_AGENT_MD_SIZE,
    RUNTIME_UPDATE_KEY,
    BaselineContextMiddleware,
    BaselineEpoch,
    BaselineSources,
    MemoSource,
    MemoryTierSource,
    Observations,
    TailEnvelopeMiddleware,
    TurnContextMiddleware,
    advance_epoch,
    is_runtime_update_message,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.baseline import _workspace_block
from ptc_agent.agent.middleware.runtime_context.changes import render_diff, sha256_text
from ptc_agent.agent.middleware.runtime_context.epoch import Workspace
from ptc_agent.core.paths import (
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    WorkspaceLayout,
)

STATE_BASELINE = "runtime_baseline"

NOW = datetime(2026, 4, 5, 19, 42, tzinfo=UTC)


def _rows(update: dict) -> list:
    """The durable rows a turn-boundary read wrote into history."""
    return [m for m in update.get("messages") or [] if is_runtime_update_message(m)]


async def _turn_row() -> list:
    """The turn's anchor, written by the middleware that runs before this one."""
    written = await TurnContextMiddleware(
        now=datetime(2026, 4, 5, 19, 42, tzinfo=UTC), preferred_market="US"
    ).abefore_agent({})
    return _rows(written or {})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _session(agent_md: str | None = "# Notes\nalpha", writer: dict | None = None):
    """A session whose sandbox answers one agent.md read; no cache in between."""
    session = MagicMock()
    _set_agent_md(session, agent_md)
    session.take_agent_md_writer = MagicMock(side_effect=[writer, None, None, None])
    session.conversation_id = "ws-test"
    return session


def _set_agent_md(session, agent_md, *, error: Exception | None = None) -> None:
    session.sandbox.normalize_path = MagicMock(side_effect=lambda p: f"/home/workspace/{p}")
    if error is not None:
        session.sandbox.aread_file_text = AsyncMock(side_effect=error)
    else:
        session.sandbox.aread_file_text = AsyncMock(return_value=agent_md)


def _middleware(session=None, **kwargs) -> BaselineContextMiddleware:
    kwargs.setdefault("guidance", "lean")
    return BaselineContextMiddleware(session=session, **kwargs)


_MEMORY_DISPLAY = {
    "user": f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}",
    "workspace": f"{WorkspaceLayout.MEMORY_DIR}/{MEMORY_INDEX_FILENAME}",
}


def _sources(store, *, memory: dict | None = None, memo=None) -> BaselineSources:
    """The store-backed tiers, wired the way the agent build wires them."""
    return BaselineSources(
        store=store,
        memory={
            tier: MemoryTierSource(
                namespace_factory=(lambda ns=ns: ns),
                display_path=_MEMORY_DISPLAY[tier],
            )
            for tier, ns in (memory or {}).items()
        },
        memo=MemoSource(namespace_factory=(lambda: memo)) if memo else None,
    )


def _request(state: dict, model=None, tools=None, rows=None) -> ModelRequest:
    return ModelRequest(
        model=model if model is not None else MagicMock(spec=ChatAnthropic),
        messages=[HumanMessage(content="What moved today?"), *(rows or [])],
        system_prompt="Static system prompt.",
        state=state,
        tools=tools or [],
    )


async def _render(mw: BaselineContextMiddleware, state: dict) -> SystemMessage:
    captured: dict = {}

    async def handler(request: ModelRequest) -> ModelResponse:
        captured["request"] = request
        return MagicMock()

    await mw.awrap_model_call(_request(state), handler)
    return captured["request"].system_message


def _text(system_message: SystemMessage) -> str:
    """The system message's blocks as one string, not as a repr of the list."""
    return "\n".join(b["text"] for b in system_message.content)


def _compaction_event(anchor: str, cutoff: int = 12) -> dict:
    summary = HumanMessage(content="[Context Summary] ...", id=f"sum-{anchor}")
    return {
        "cutoff_index": cutoff,
        "anchor_message_id": anchor,
        "summary_message": summary,
        "file_path": None,
    }


# ---------------------------------------------------------------------------
# The block is frozen for the turn
# ---------------------------------------------------------------------------


class TestTheBlockIsFrozenForTheTurn:
    @pytest.mark.asyncio
    async def test_byte_identical_across_calls_when_agent_md_changes_mid_turn(self):
        """A write between two calls of one turn must not move the block.

        This is the contract the cache breakpoint rests on. The old middleware
        re-read agent.md per call, so a write mid-turn re-wrote the whole
        message history behind it.
        """
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)

        first = await _render(mw, state)

        # The agent writes agent.md between the two model calls of this turn.
        _set_agent_md(session, "# Notes\nalpha\nbeta\ngamma")
        second = await _render(mw, state)

        assert first.content == second.content
        assert "beta" not in _text(first)

    @pytest.mark.asyncio
    async def test_the_frozen_block_carries_every_tier_in_a_fixed_order(self):
        store = _store(
            {("u", "memory"): "user index", ("u", "ws", "memory"): "ws index"}
        )
        mw = _middleware(
            _session("# Notes"),
            workspace_name="Q3 Semis",
            sources=_sources(
                store,
                memory={
                    "user": ("u", "memory"),
                    "workspace": ("u", "ws", "memory"),
                },
            ),
        )
        state = await mw.abefore_agent({}, None)
        text = _text(await _render(mw, state))

        order = [
            text.index("<workspace"),
            text.index("<user_identity"),
            text.index("<agentmd"),
            text.index("user index"),
            text.index("ws index"),
        ]
        assert order == sorted(order)


def _store(values: dict[tuple[str, ...], str]):
    store = MagicMock()

    async def aget(namespace, key):
        content = values.get(tuple(namespace))
        if content is None:
            return None
        item = MagicMock()
        item.value = {"content": content, "modified_at": "2026-04-05T00:00:00+00:00"}
        return item

    store.aget = AsyncMock(side_effect=aget)
    return store


# ---------------------------------------------------------------------------
# The epoch
# ---------------------------------------------------------------------------


class TestTheEpoch:
    @pytest.mark.asyncio
    async def test_a_compaction_event_rebuilds_exactly_once(self):
        """The epoch follows compaction, and does not fire again on the same event."""
        mw = _middleware(_session("# Notes"))
        first = await mw.abefore_agent({}, None)
        assert first[STATE_BASELINE]["epoch"] == 1

        event = _compaction_event("msg-42")
        second = await mw.abefore_agent({**first, "_summarization_event": event}, None)
        assert second[STATE_BASELINE]["epoch"] == 2
        assert second[STATE_BASELINE]["reason"] == "compaction"

        third = await mw.abefore_agent({**second, "_summarization_event": event}, None)
        assert third is None, "the same compaction event must not rebuild again"

        fourth = await mw.abefore_agent(
            {**second, "_summarization_event": _compaction_event("msg-99")}, None
        )
        assert fourth[STATE_BASELINE]["epoch"] == 3

    @pytest.mark.asyncio
    async def test_accumulated_drift_folds_back_into_the_block(self):
        """The safety valve: a thread that never compacts still folds eventually."""
        mw = _middleware(_session("# Notes"), rebuild_after_updates=2)
        state = await mw.abefore_agent({}, None)
        baseline = {**state[STATE_BASELINE], "drift_updates": 2}

        rebuilt = await mw.abefore_agent({STATE_BASELINE: baseline}, None)

        assert rebuilt[STATE_BASELINE]["epoch"] == 2
        assert rebuilt[STATE_BASELINE]["reason"] == "drift"
        assert rebuilt[STATE_BASELINE]["drift_updates"] == 0

    @pytest.mark.asyncio
    async def test_a_failed_read_keeps_the_previous_text_and_retries_next_turn(self):
        """ "The sandbox did not answer" must never render as "you have no notes"."""
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        first = await mw.abefore_agent({}, None)

        _set_agent_md(session, None, error=RuntimeError("sandbox down"))
        second = await mw.abefore_agent(
            {**first, "_summarization_event": _compaction_event("msg-1")}, None
        )

        baseline = second[STATE_BASELINE]
        assert baseline["agent_md"]["text"] == "# Notes\nalpha"
        assert baseline["incomplete"] is True

        # The hole forces one more rebuild rather than being frozen in.
        _set_agent_md(session, "# Notes\nrecovered")
        third = await mw.abefore_agent({STATE_BASELINE: baseline}, None)
        assert third[STATE_BASELINE]["agent_md"]["text"] == "# Notes\nrecovered"
        assert third[STATE_BASELINE]["incomplete"] is False

    @pytest.mark.asyncio
    async def test_a_build_that_no_longer_reads_a_source_rebuilds_without_it(self):
        """A PTC thread continued without a sandbox must not keep rendering agent.md."""
        with_sandbox = _middleware(_session("# Notes\nalpha"))
        state = await with_sandbox.abefore_agent({}, None)
        assert state[STATE_BASELINE]["agent_md"]["text"] == "# Notes\nalpha"

        without = _middleware(None)
        second = await without.abefore_agent(state, None)
        assert second[STATE_BASELINE]["reason"] == "source_removed"
        assert "agent_md" not in second[STATE_BASELINE]
        assert "<agentmd>" not in _text(await _render(without, {**state, **second}))

        # Settled: the next turn without the source neither rebuilds nor files.
        assert await without.abefore_agent({**state, **second}, None) is None

    @pytest.mark.asyncio
    async def test_a_build_with_no_workspace_drops_the_one_the_ptc_epoch_froze(self):
        """Flash reads no workspace at all, which is not a read that failed."""
        ptc = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="Alpha Desk", workspace_description="desk",
        )
        state = await ptc.abefore_agent({}, None)
        assert state[STATE_BASELINE]["workspace"]["name"] == "Alpha Desk"

        flash = _middleware(None)
        second = await flash.abefore_agent(state, None)
        assert second[STATE_BASELINE]["reason"] == "source_removed"
        assert second[STATE_BASELINE]["workspace"]["name"] == ""
        assert second[STATE_BASELINE]["incomplete"] is False
        assert "Alpha Desk" not in _text(await _render(flash, {**state, **second}))

    @pytest.mark.asyncio
    async def test_the_first_model_call_opens_the_turn_when_the_entry_hook_did_not_run(self):
        """A resumed interrupt skips the entry node; the boundary read runs on the first model hook instead."""
        mw = _middleware(_session("# Notes"))
        written = await mw.abefore_model({}, None)
        assert written[STATE_BASELINE]["reason"] == "first_turn"
        assert await mw.abefore_model(written, None) is None

        opened = _middleware(_session("# Notes"))
        await opened.abefore_agent({}, None)
        assert await opened.abefore_model({}, None) is None

    @pytest.mark.asyncio
    async def test_a_rebuild_blind_to_the_profile_still_retires_rows_about_other_sources(self):
        """Only a row about the carried source is still right after the rebuild."""
        session = _session("# Notes\nalpha")
        profile = {"name": "Ada", "timezone": "UTC", "locale": "en-US"}
        counts = {"portfolio_count": 2}
        first = _middleware(session, user_profile=profile, user_data_counts=counts, sandbox_enabled=True)
        state = await first.abefore_agent({}, None)
        _set_agent_md(session, "# Notes\nalpha\nbeta")
        second = await first.abefore_agent(state, None)
        assert [runtime_update_from_message(m).kind for m in _rows(second)] == ["agent_md_changed"]

        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        blind = _middleware(session, user_profile=None, user_data_counts=None, sandbox_enabled=True)
        third = await blind.abefore_agent({**state, **second, "_summarization_event": event}, None)
        assert third[STATE_BASELINE]["incomplete"] is False
        rows = [runtime_update_from_message(m) for m in _rows(third)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert "still_in_force" not in rows[0].provenance
        assert "The exception" not in _rows(third)[0].content

    @pytest.mark.asyncio
    async def test_a_blind_rebuild_retires_the_other_rows_and_names_the_exception(self):
        """An agent.md row and a profile row in view; only the profile read failed."""
        session = _session("# Notes\nalpha")
        profile = {"name": "Ada", "timezone": "Asia/Hong_Kong", "locale": "en-US"}
        first = _middleware(session, user_profile=profile, user_data_counts={"portfolio_count": 2}, sandbox_enabled=True)
        state = await first.abefore_agent({}, None)
        _set_agent_md(session, "# Notes\nalpha\nbeta")
        moved = _middleware(session, user_profile=profile, user_data_counts={"portfolio_count": 7}, sandbox_enabled=True)
        second = await moved.abefore_agent(state, None)
        assert sorted(runtime_update_from_message(m).kind for m in _rows(second)) == [
            "agent_md_changed", "profile_changed",
        ]

        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        blind = _middleware(session, user_profile=None, user_data_counts=None, sandbox_enabled=True)
        third = await blind.abefore_agent({**state, **second, "_summarization_event": event}, None)
        rows = [runtime_update_from_message(m) for m in _rows(third)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert "1 earlier change row(s) folded in" in rows[0].text
        assert rows[0].provenance["still_in_force"] == ["<user_profile> and <user_identity>"]
        assert "The exception is a row about <user_profile>" in _rows(third)[0].content
        # The identity block rides the same read, so it is carried too.
        assert third[STATE_BASELINE]["identity"]["timezone"] == "Asia/Hong_Kong"

    @pytest.mark.asyncio
    async def test_an_incomplete_rebuild_still_retires_rows_about_the_sources_it_read(self):
        """agent.md is down but the profile was read: the profile row is folded in now,
        and waiting for the retry would keep it in force for as long as agent.md stays down."""
        session = _session("# Notes\nalpha")
        profile = {"name": "Ada", "timezone": "UTC", "locale": "en-US"}
        first = _middleware(session, user_profile=profile, user_data_counts={"portfolio_count": 2}, sandbox_enabled=True)
        state = await first.abefore_agent({}, None)
        moved = _middleware(session, user_profile=profile, user_data_counts={"portfolio_count": 7}, sandbox_enabled=True)
        second = await moved.abefore_agent(state, None)
        assert [runtime_update_from_message(m).kind for m in _rows(second)] == ["profile_changed"]

        _set_agent_md(session, None, error=RuntimeError("sandbox down"))
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        third = await moved.abefore_agent({**state, **second, "_summarization_event": event}, None)
        assert third[STATE_BASELINE]["incomplete"] is True
        rows = [runtime_update_from_message(m) for m in _rows(third)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert "still_in_force" not in rows[0].provenance

    @pytest.mark.asyncio
    async def test_an_incomplete_rebuild_leaves_the_retained_rows_in_force(self):
        """The old copy was carried forward, so the row describing the file is still right."""
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        first = await mw.abefore_agent({}, None)
        _set_agent_md(session, "# Notes\nalpha\nbeta")
        second = await mw.abefore_agent(first, None)
        assert len(_rows(second)) == 1

        _set_agent_md(session, None, error=RuntimeError("sandbox down"))
        event = {**_compaction_event("msg-1"), "cutoff_index": 0, "anchor_message_id": None}
        third = await mw.abefore_agent({**first, **second, "_summarization_event": event}, None)
        assert third[STATE_BASELINE]["incomplete"] is True
        assert _rows(third) == []

        _set_agent_md(session, "# Notes\nalpha\nbeta")
        fourth = await mw.abefore_agent(
            {**first, **second, "_summarization_event": event, STATE_BASELINE: third[STATE_BASELINE]},
            None,
        )
        assert fourth[STATE_BASELINE]["incomplete"] is False
        assert [runtime_update_from_message(m).kind for m in _rows(fourth)] == ["baseline_rebuilt"]


# ---------------------------------------------------------------------------
# Cross-writer change detection
# ---------------------------------------------------------------------------


class TestCrossWriterChanges:
    @pytest.mark.asyncio
    async def test_a_changed_agent_md_writes_one_row_into_history(self):
        """The row is a message, so it keeps the place in time it was observed."""
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)

        _set_agent_md(session, "# Notes\nalpha\nbeta")
        session.take_agent_md_writer = MagicMock(
            side_effect=[{"writer": "subagent:research", "at": "2026-04-05"}, None]
        )
        second = await mw.abefore_agent(state, None)

        rows = _rows(second)
        assert len(rows) == 1
        message = rows[0]
        # No id: the Pregel path stamps one, and a pre-stamped uuid re-rolls
        # on every replay.
        assert message.id is None
        assert "+beta" in message.content

        stamped = message.additional_kwargs[RUNTIME_UPDATE_KEY]
        assert stamped["kind"] == "agent_md_changed"
        assert stamped["schema_version"] == 1
        assert stamped["provenance"]["source"] == "sandbox"
        assert stamped["provenance"]["path"] == "/agent.md"
        assert stamped["provenance"]["writer"] == "subagent:research"
        datetime.fromisoformat(stamped["created_at"])

        # And it reads back as the row it was written from.
        row = runtime_update_from_message(message)
        assert row.kind == "agent_md_changed"
        assert "+beta" in row.text

        # The frozen block is untouched; only history grew.
        assert second[STATE_BASELINE]["agent_md"]["text"] == "# Notes\nalpha"
        assert second[STATE_BASELINE]["epoch"] == 1
        assert second[STATE_BASELINE]["drift_updates"] == 1

    @pytest.mark.asyncio
    async def test_an_unchanged_source_emits_nothing_on_the_next_turn(self):
        mw = _middleware(_session("# Notes\nalpha"))
        state = await mw.abefore_agent({}, None)
        assert await mw.abefore_agent(state, None) is None

    @pytest.mark.asyncio
    async def test_the_same_change_is_not_re_emitted_every_turn(self):
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)

        _set_agent_md(session, "# Notes\nalpha\nbeta")
        second = await mw.abefore_agent(state, None)
        assert len(_rows(second)) == 1

        third = await mw.abefore_agent({**state, **second}, None)
        assert third is None

    @pytest.mark.asyncio
    async def test_a_memory_tier_change_names_its_tier_and_the_store(self):
        values = {("u", "memory"): "index v1"}
        store = _store(values)
        mw = _middleware(
            None,
            sources=_sources(store, memory={"user": ("u", "memory")}),
        )
        state = await mw.abefore_agent({}, None)

        values[("u", "memory")] = "index v2"
        second = await mw.abefore_agent(state, None)

        row = runtime_update_from_message(_rows(second)[0])
        assert row.kind == "memory_changed:user"
        assert row.provenance == {
            "source": "store",
            "tier": "user",
            "modified_at": "2026-04-05T00:00:00+00:00",
        }

    @pytest.mark.asyncio
    async def test_a_rebuild_that_folds_one_memory_tier_names_the_other_as_the_exception(self):
        """Both tiers share one tag; the row that keeps the failed tier's update
        in force has to say which tier, or the folded one reads as still open."""
        values = {("u", "memory"): "user v1", ("w", "memory"): "workspace v1"}
        down: set = set()
        store = MagicMock()

        async def aget(namespace, key):
            if tuple(namespace) in down:
                raise RuntimeError("store down")
            item = MagicMock()
            item.value = {"content": values[tuple(namespace)], "modified_at": "2026-04-05T00:00:00+00:00"}
            return item

        store.aget = AsyncMock(side_effect=aget)
        mw = _middleware(
            None,
            sources=_sources(store, memory={"user": ("u", "memory"), "workspace": ("w", "memory")}),
        )
        state = await mw.abefore_agent({}, None)
        values[("u", "memory")] = "user v2"
        values[("w", "memory")] = "workspace v2"
        second = await mw.abefore_agent(state, None)
        assert sorted(runtime_update_from_message(m).kind for m in _rows(second)) == [
            "memory_changed:user",
            "memory_changed:workspace",
        ]

        down.add(("w", "memory"))
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        third = await mw.abefore_agent({**state, **second, "_summarization_event": event}, None)
        rows = [runtime_update_from_message(m) for m in _rows(third)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert rows[0].provenance["still_in_force"] == ["<memory> for the workspace tier"]

    @pytest.mark.asyncio
    async def test_a_carried_exception_outlives_the_compaction_that_took_its_row(self):
        """A compaction that keeps the rebuilt row but takes the change row it
        kept in force leaves that row as the only word on the unread tier; the
        next rebuild that still cannot read it has to carry the exception on."""
        values = {("u", "memory"): "user v1", ("w", "memory"): "workspace v1"}
        down: set = set()
        store = MagicMock()

        async def aget(namespace, key):
            if tuple(namespace) in down:
                raise RuntimeError("store down")
            item = MagicMock()
            item.value = {"content": values[tuple(namespace)], "modified_at": "2026-04-05T00:00:00+00:00"}
            return item

        store.aget = AsyncMock(side_effect=aget)
        mw = _middleware(
            None,
            sources=_sources(store, memory={"user": ("u", "memory"), "workspace": ("w", "memory")}),
        )
        state = await mw.abefore_agent({}, None)
        values[("u", "memory")] = "user v2"
        values[("w", "memory")] = "workspace v2"
        second = await mw.abefore_agent(state, None)
        assert sorted(runtime_update_from_message(m).kind for m in _rows(second)) == [
            "memory_changed:user",
            "memory_changed:workspace",
        ]

        down.add(("w", "memory"))
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        third = await mw.abefore_agent({**state, **second, "_summarization_event": event}, None)
        rebuilt = [runtime_update_from_message(m) for m in _rows(third)]
        assert rebuilt[0].provenance["carried"] == ["memory_changed:workspace"]

        # The next compaction takes both change rows and keeps the rebuilt row.
        event = {**_compaction_event("a2"), "cutoff_index": 2, "anchor_message_id": None}
        history = {"messages": [*_rows(second), *_rows(third)], "_summarization_event": event}
        fourth = await mw.abefore_agent({**state, **third, **history}, None)
        rows = [runtime_update_from_message(m) for m in _rows(fourth)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert rows[0].provenance["still_in_force"] == ["<memory> for the workspace tier"]

        # Once the tier reads again the exception is folded and the row says so.
        down.clear()
        event = {**_compaction_event("a3"), "cutoff_index": 2, "anchor_message_id": None}
        history = {"messages": [*_rows(second), *_rows(fourth)], "_summarization_event": event}
        fifth = await mw.abefore_agent({**state, **fourth, **history}, None)
        rows = [runtime_update_from_message(m) for m in _rows(fifth)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert "still_in_force" not in rows[0].provenance

    @pytest.mark.asyncio
    async def test_a_memo_upload_mid_thread_files_a_row_rather_than_waiting(self):
        """The memo pointer is a count, so the row carries the number, not a diff."""
        counts = {"memo_count": 2}
        store = MagicMock()

        async def aget(namespace, key):
            item = MagicMock()
            item.value = dict(counts)
            return item

        store.aget = AsyncMock(side_effect=aget)
        mw = _middleware(
            None, sources=_sources(store, memo=("u", "memos"))
        )
        state = await mw.abefore_agent({}, None)
        assert state[STATE_BASELINE]["memo"]["display"] == "2"

        counts["memo_count"] = 5
        second = await mw.abefore_agent(state, None)

        row = runtime_update_from_message(_rows(second)[0])
        assert row.kind == "memo_changed"
        assert "5 memo(s)" in row.text
        assert row.provenance == {"source": "store", "tier": "memo"}

        # Once observed, it does not re-fire every turn.
        assert await mw.abefore_agent({**state, **second}, None) is None

    @pytest.mark.asyncio
    async def test_a_memo_count_that_did_not_answer_marks_the_epoch_incomplete(self):
        """A frozen None would leave every later count with nothing to compare against."""
        counts: dict = {}
        store = MagicMock()

        async def aget(namespace, key):
            if not counts:
                raise RuntimeError("store down")
            item = MagicMock()
            item.value = dict(counts)
            return item

        store.aget = AsyncMock(side_effect=aget)
        mw = _middleware(
            None, sources=_sources(store, memo=("u", "memos"))
        )
        state = await mw.abefore_agent({}, None)
        assert state[STATE_BASELINE]["incomplete"] is True

        counts["memo_count"] = 3
        second = await mw.abefore_agent(state, None)
        assert second[STATE_BASELINE]["reason"] == "incomplete"
        assert second[STATE_BASELINE]["memo"]["count"] == 3
        assert second[STATE_BASELINE]["incomplete"] is False

    @pytest.mark.asyncio
    async def test_the_writer_stamp_labels_one_change_only(self):
        """A local stamp left in place would label a later foreign write too."""
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)

        _set_agent_md(session, "# Notes\nalpha\nbeta")
        session.take_agent_md_writer = MagicMock(side_effect=[{"writer": "user"}, None])
        first = await mw.abefore_agent(state, None)
        assert runtime_update_from_message(_rows(first)[0]).provenance["writer"] == "user"

        _set_agent_md(session, "# Notes\nalpha\nbeta\ngamma")
        second = await mw.abefore_agent({**state, **first}, None)
        assert "writer" not in runtime_update_from_message(_rows(second)[0]).provenance

    def test_a_wholesale_rewrite_says_so_instead_of_shipping_a_second_copy(self):
        old = "\n".join(f"old line {i}" for i in range(200))
        new = "\n".join(f"new line {i}" for i in range(200))
        assert "rewritten" in render_diff(old, new, "/agent.md")

    def test_a_wide_diff_falls_back_to_the_rewrite_notice(self):
        """A one-line file diffs as three lines that carry the whole file twice."""
        old = "x" * 3000
        new = "x" * 2999 + "y"
        text = render_diff(old, new, "/agent.md")
        assert "rewritten" in text
        assert len(text) < 200

    def test_a_long_diff_is_capped_with_a_note(self):
        old = "\n".join(f"line {i}" for i in range(60))
        new = "\n".join(f"line {i}" + (" edited" if i % 2 else "") for i in range(60))
        text = render_diff(old, new, "/agent.md")
        assert "more diff lines" in text
        assert len(text.splitlines()) <= 61

    def test_returning_to_the_baseline_value_still_files_a_row(self):
        """Otherwise a now-wrong "changed" notice stands for the rest of the epoch."""
        from ptc_agent.agent.middleware.runtime_context.changes import SourceRead

        epoch = BaselineEpoch.from_state(
            {
                "agent_md": {"text": "original", "sha256": sha256_text("original")},
                "observed": {"agent_md": sha256_text("edited")},
            }
        )
        read = SourceRead(
            kind="agent_md",
            update_kind="agent_md_changed",
            path="/agent.md",
            text="original",
        )
        advanced, updates = advance_epoch(
            epoch, Observations(now=NOW, reads=(read,))
        )

        assert len(updates) == 1
        assert "matches its frozen copy again" in updates[0].text
        assert advanced is not None
        assert advanced.cursor.observed["agent_md"] == sha256_text("original")


# ---------------------------------------------------------------------------
# Supersession is chronology
# ---------------------------------------------------------------------------


class TestProfileFollowsTheGoldenRule:
    """The profile is frozen into the epoch; a change is a row, never a re-render.

    Holdings, watchlists and preferences move underneath a thread from the
    platform side. If the block re-rendered them per turn, every change would
    rewrite the cached prefix. So the block keeps the epoch's snapshot and a
    `profile_changed` row carries the delta until compaction catches the block
    up.
    """

    PROFILE = {
        "name": "Ada",
        "timezone": "Europe/Paris",
        "locale": "fr-FR",
        "agent_preference": {"output_format": "html"},
    }
    COUNTS = {"portfolio_count": 2, "watchlist_summary": "1:5", "prefs_set": True}

    def _turn(self, profile: dict, counts: dict) -> BaselineContextMiddleware:
        # A new instance per turn, as the server builds one per request with
        # that request's snapshot of the profile.
        return _middleware(
            _session("# Notes"),
            user_profile=profile,
            user_data_counts=counts,
            sandbox_enabled=True,
        )

    @pytest.mark.asyncio
    async def test_the_block_keeps_the_frozen_profile_when_the_next_turn_differs(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        assert state[STATE_BASELINE]["profile"]["user_data_counts"]["portfolio_count"] == 2

        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        merged = {**state, **second}
        text = _text(await _render(later, merged))

        assert "Portfolio: 2 holding(s)" in text
        assert "7 holding(s)" not in text
        assert second[STATE_BASELINE]["epoch"] == 1

    @pytest.mark.asyncio
    async def test_a_moved_profile_writes_one_row_with_the_delta(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)

        later = self._turn(
            {**self.PROFILE, "timezone": "America/New_York"},
            {**self.COUNTS, "portfolio_count": 7, "watchlist_summary": "2:9"},
        )
        second = await later.abefore_agent(state, None)

        rows = _rows(second)
        assert len(rows) == 1
        row = runtime_update_from_message(rows[0])
        assert row.kind == "profile_changed"
        assert row.provenance == {"source": "platform", "tier": "profile"}
        assert "Timezone: America/New_York (the frozen block says Europe/Paris)" in row.text
        assert "Portfolio holdings: 7 (the frozen block says 2)" in row.text
        assert "Watchlists: 2 list(s) with 9 symbol(s) (the frozen block says 1 list(s) with 5 symbol(s))" in row.text
        assert "Name:" not in row.text
        assert (
            "The user's profile changed after the copies in <user_profile> and <user_identity> were frozen"
            in rows[0].content
        )

        # Reported once: the same profile on the following turn is silent.
        merged = {**state, **second}
        assert await later.abefore_agent(merged, None) is None

    @pytest.mark.asyncio
    async def test_a_preference_change_names_only_that_key(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(
            {**self.PROFILE, "agent_preference": {"output_format": "markdown", "tone": "terse"}},
            self.COUNTS,
        )
        row = runtime_update_from_message(_rows(await later.abefore_agent(state, None))[0])
        assert "Preference output_format: markdown (the frozen block says html)" in row.text
        assert "Preference tone: terse (not in the frozen block)" in row.text
        assert "Timezone" not in row.text

    @pytest.mark.asyncio
    async def test_compaction_catches_the_block_up(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        merged = {**state, **second, "_summarization_event": _compaction_event("a1")}

        third = await later.abefore_agent(merged, None)
        assert third[STATE_BASELINE]["epoch"] == 2
        assert third[STATE_BASELINE]["profile"]["user_data_counts"]["portfolio_count"] == 7
        assert _rows(third) == []
        text = _text(await _render(later, {**merged, **third}))
        assert "Portfolio: 7 holding(s)" in text

    @pytest.mark.asyncio
    async def test_a_rebuild_with_a_row_still_in_view_files_the_row_that_retires_it(self):
        """The old row says to trust it over the block; after the rebuild only a later row can say the block is current."""
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        assert len(_rows(second)) == 1
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        merged = {**state, **second, "_summarization_event": event}

        third = await later.abefore_agent(merged, None)
        assert third[STATE_BASELINE]["reason"] == "compaction"
        rows = [runtime_update_from_message(m) for m in _rows(third)]
        assert [r.kind for r in rows] == ["baseline_rebuilt"]
        assert "1 earlier change row(s) folded in" in rows[0].text
        assert "folded into the blocks now" in _rows(third)[0].content

    @pytest.mark.asyncio
    async def test_a_rebuild_with_no_row_in_view_files_nothing(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        event = {**_compaction_event("a1"), "cutoff_index": 1, "anchor_message_id": None}
        merged = {**state, **second, "_summarization_event": event}

        third = await later.abefore_agent(merged, None)
        assert third[STATE_BASELINE]["reason"] == "compaction"
        assert _rows(third) == []


    @pytest.mark.asyncio
    async def test_a_profile_read_that_did_not_answer_files_no_row(self):
        """Both platform helpers answer None on failure; that is not a cleared profile."""
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        blind = _middleware(_session("# Notes"), user_profile=None, user_data_counts=None, sandbox_enabled=True)

        assert await blind.abefore_agent(state, None) is None

        rebuilt = await blind.abefore_agent(
            {**state, "_summarization_event": _compaction_event("a1")}, None
        )
        assert rebuilt[STATE_BASELINE]["profile"]["user_profile"] == self.PROFILE

        # The read that answers again against the kept copy files nothing.
        again = self._turn(self.PROFILE, self.COUNTS)
        assert await again.abefore_agent({**state, **rebuilt}, None) is None

    @pytest.mark.asyncio
    async def test_a_rebuild_blind_to_the_profile_leaves_the_profile_row_in_force(self):
        """The rebuilt block carries the old profile, so the row that says otherwise stays the last word."""
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        assert len(_rows(second)) == 1
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        merged = {**state, **second, "_summarization_event": event}

        blind = _middleware(
            _session("# Notes"), user_profile=None, user_data_counts=None, sandbox_enabled=True
        )
        third = await blind.abefore_agent(merged, None)
        assert third[STATE_BASELINE]["reason"] == "compaction"
        assert third[STATE_BASELINE]["incomplete"] is False
        assert third[STATE_BASELINE]["profile"]["user_data_counts"]["portfolio_count"] == 2
        assert _rows(third) == []

        # The read that answers again measures against the row still in
        # force, which already says 7, so there is nothing new to state.
        fourth = await later.abefore_agent({**merged, **third}, None)
        assert fourth is None or _rows(fourth) == []

        # A return to the frozen values during the outage is the one change
        # the surviving row cannot describe, so it files the row that takes
        # that row back.
        back = self._turn(self.PROFILE, self.COUNTS)
        fifth = await back.abefore_agent({**merged, **third}, None)
        row = runtime_update_from_message(_rows(fifth)[0])
        assert row.kind == "profile_changed"
        assert row.text.endswith("Every field is back to the value in <user_profile>.")

    @pytest.mark.asyncio
    async def test_a_blind_rebuild_whose_row_was_compacted_away_measures_from_the_block(self):
        """The cutoff took the row, so the model reads the frozen copy alone."""
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await later.abefore_agent(state, None)
        rows = _rows(second)
        event = {**_compaction_event("a1"), "cutoff_index": len(rows), "anchor_message_id": None}
        merged = {**state, **second, "_summarization_event": event}

        blind = _middleware(
            _session("# Notes"), user_profile=None, user_data_counts=None, sandbox_enabled=True
        )
        third = await blind.abefore_agent(merged, None)
        fourth = await later.abefore_agent({**merged, **third}, None)
        row = runtime_update_from_message(_rows(fourth)[0])
        assert row.kind == "profile_changed"
        assert "Portfolio holdings: 7 (the frozen block says 2)" in row.text

    @pytest.mark.asyncio
    async def test_a_profile_that_returns_to_the_frozen_values_still_files_a_row(self):
        """A to B to A: the B row is now wrong, and only a newer row can say so."""
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)

        moved = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        second = await moved.abefore_agent(state, None)
        assert "Portfolio holdings: 7" in runtime_update_from_message(_rows(second)[0]).text

        back = self._turn(self.PROFILE, self.COUNTS)
        third = await back.abefore_agent({**state, **second}, None)
        row = runtime_update_from_message(_rows(third)[0])
        assert row.kind == "profile_changed"
        assert row.text.endswith("Every field is back to the value in <user_profile>.")

        # And it is quiet from there.
        assert await self._turn(self.PROFILE, self.COUNTS).abefore_agent(
            {**state, **second, **third}, None
        ) is None

    @pytest.mark.asyncio
    async def test_a_moved_preferred_market_is_a_row(self):
        """The market <user_identity> states is derived from inputs that move
        (an explicit choice, the watchlist); the block stays, the row says."""
        first = _middleware(
            _session("# Notes"), user_profile=self.PROFILE, user_data_counts=self.COUNTS,
            sandbox_enabled=True, preferred_market="US",
        )
        state = await first.abefore_agent({}, None)
        later = _middleware(
            _session("# Notes"), user_profile=self.PROFILE, user_data_counts=self.COUNTS,
            sandbox_enabled=True, preferred_market="CN",
        )
        second = await later.abefore_agent(state, None)

        rows = [runtime_update_from_message(m) for m in _rows(second)]
        assert [r.kind for r in rows] == ["profile_changed"]
        assert "Preferred market: CN (the frozen block says US)" in rows[0].text
        assert "Preferred market: US" in _text(await _render(later, {**state, **second}))

    @pytest.mark.asyncio
    async def test_a_field_the_block_never_shows_cannot_move_it(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        later = self._turn({**self.PROFILE, "email": "ada@example.com"}, self.COUNTS)
        assert await later.abefore_agent(state, None) is None

    @pytest.mark.asyncio
    async def test_an_epoch_frozen_before_profiles_falls_back_to_this_turn(self):
        state = await self._turn(self.PROFILE, self.COUNTS).abefore_agent({}, None)
        state[STATE_BASELINE].pop("profile")
        state[STATE_BASELINE]["observed"].pop("profile", None)

        later = self._turn(self.PROFILE, {**self.COUNTS, "portfolio_count": 7})
        assert await later.abefore_agent(state, None) is None
        assert "Portfolio: 7 holding(s)" in _text(await _render(later, state))


class TestSupersession:
    @pytest.mark.asyncio
    async def test_a_second_change_of_a_kind_is_a_second_row_further_down(self):
        """No stub, no collapse: the newer row simply sits later in history.

        The older row is what the model was told at the time, and where it sits
        is the evidence for how long that was true. Rewriting it into "1 earlier
        update was superseded" threw that away and cost a re-render besides.
        """
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)

        _set_agent_md(session, "# Notes\nalpha\nbeta")
        first = await mw.abefore_agent(state, None)
        _set_agent_md(session, "# Notes\nalpha\nbeta\ngamma")
        second = await mw.abefore_agent({**state, **first}, None)

        history = [*_rows(first), *_rows(second)]
        assert len(history) == 2
        kinds = [runtime_update_from_message(m).kind for m in history]
        assert kinds == ["agent_md_changed", "agent_md_changed"]

        # Both survive in full, oldest first, and nothing says "superseded".
        assert "+beta" in history[0].content
        assert "+gamma" in history[1].content
        assert "superseded" not in "".join(m.content for m in history)

        stamps = [
            datetime.fromisoformat(
                m.additional_kwargs[RUNTIME_UPDATE_KEY]["created_at"]
            )
            for m in history
        ]
        assert stamps[0] <= stamps[1]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRendering:
    @pytest.mark.asyncio
    async def test_the_user_profile_steering_renders_for_a_sandbox_agent(self):
        """The regression: the profile component carries steering, not just identity."""
        mw = _middleware(
            _session("# Notes"),
            user_profile={
                "name": "Ada",
                "locale": "fr-FR",
                "agent_preference": {"output_format": "html", "tone": "terse"},
            },
            user_data_counts={"portfolio_count": 2, "watchlist_summary": "1:5"},
            sandbox_enabled=True,
        )
        state = await mw.abefore_agent({}, None)
        text = _text(await _render(mw, state))

        assert "Output Format: Styled HTML" in text
        assert "Default to the user's locale (fr-FR)" in text
        assert "**tone**: terse" in text
        assert ".agents/user/profile/portfolio.json" in text
        assert "Portfolio: 2 holding(s)" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("locale", ["en-US", None])
    async def test_the_language_rule_holds_for_every_locale(self, locale):
        """Only the locale default is conditional; the rule to answer in the
        user's own language is not, since most users never set a locale."""
        mw = _middleware(_session("# Notes"), user_profile={"name": "Ada", "locale": locale})
        state = await mw.abefore_agent({}, None)
        text = _text(await _render(mw, state))

        assert "Answer in the language the user wrote in." in text
        assert "Default to the user's locale" not in text
        assert "**Locale**: en-US" in text

    @pytest.mark.asyncio
    async def test_a_non_english_locale_is_the_default_for_a_message_nobody_wrote(self):
        """An automation's instruction or a parent agent's brief is often in
        English by accident; the person who reads the result is the user."""
        mw = _middleware(_session("# Notes"), user_profile={"name": "Ada", "locale": "zh-CN"})
        state = await mw.abefore_agent({}, None)
        text = _text(await _render(mw, state))

        assert "Default to the user's locale (zh-CN) when the message gives no signal." in text
        assert "automation or another agent" in text
        assert "the user's locale is the safe choice" in text

    @pytest.mark.asyncio
    async def test_a_flash_style_agent_gets_no_html_routing_and_no_agentmd(self):
        mw = _middleware(
            None,
            user_profile={"agent_preference": {"output_format": "html"}},
            sandbox_enabled=False,
        )
        state = await mw.abefore_agent({}, None)
        text = _text(await _render(mw, state))

        assert "Output Format: Styled HTML" not in text
        assert "<agentmd" not in text
        assert "<user_identity>" in text

    @pytest.mark.asyncio
    async def test_a_missing_agent_md_keeps_the_create_it_placeholder(self):
        mw = _middleware(_session(None))
        state = await mw.abefore_agent({}, None)
        assert "No agent.md exists yet" in _text(await _render(mw, state))

    @pytest.mark.asyncio
    async def test_a_large_agent_md_is_truncated(self):
        mw = _middleware(_session("x" * (MAX_AGENT_MD_SIZE + 1000)))
        state = await mw.abefore_agent({}, None)
        assert "[... truncated ...]" in _text(await _render(mw, state))

    @pytest.mark.asyncio
    async def test_a_renamed_workspace_is_a_row_and_the_block_stays(self):
        """The static prompt calls <workspace> the one authoritative place for
        the name, so a rename has to reach the model without a rebuild."""
        first = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="Old Name", workspace_description="old desc",
        )
        state = await first.abefore_agent({}, None)
        later = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="New Name", workspace_description="old desc",
        )
        second = await later.abefore_agent(state, None)

        rows = [runtime_update_from_message(m) for m in _rows(second)]
        assert [r.kind for r in rows] == ["workspace_changed"]
        assert rows[0].text.endswith("Name: New Name (the frozen block says Old Name)")
        assert second[STATE_BASELINE]["epoch"] == state[STATE_BASELINE]["epoch"]
        assert "Old Name" in _text(await _render(later, {**state, **second}))

        # Seen once: the same name on the next turn is not a second row.
        merged = {**state, **second}
        assert await later.abefore_agent(merged, None) is None

        # Back to the frozen name: one row that supersedes the notice.
        back = await first.abefore_agent(merged, None)
        assert runtime_update_from_message(_rows(back)[0]).text.endswith(
            "Name and description are back to what <workspace> says."
        )

    @pytest.mark.asyncio
    async def test_a_workspace_read_that_did_not_answer_files_no_row(self):
        """A DB blip is not a workspace that lost its name."""
        first = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="Old Name", workspace_description="old desc",
        )
        state = await first.abefore_agent({}, None)
        blind = _middleware(_session("# Notes"), sandbox_enabled=True)

        assert await blind.abefore_agent(state, None) is None

        # A rebuild while blind keeps the frozen pair.
        rebuilt = await blind.abefore_agent(
            {**state, "_summarization_event": _compaction_event("a1")}, None
        )
        assert rebuilt[STATE_BASELINE]["workspace"]["name"] == "Old Name"
        assert "Old Name" in _text(await _render(blind, {**state, **rebuilt}))

    @pytest.mark.asyncio
    async def test_a_rebuild_blind_to_the_workspace_leaves_the_rename_row_in_force(self):
        first = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="Old Name", workspace_description="old desc",
        )
        state = await first.abefore_agent({}, None)
        later = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="New Name", workspace_description="old desc",
        )
        second = await later.abefore_agent(state, None)
        assert len(_rows(second)) == 1
        event = {**_compaction_event("a1"), "cutoff_index": 0, "anchor_message_id": None}
        merged = {**state, **second, "_summarization_event": event}

        blind = _middleware(_session("# Notes"), sandbox_enabled=True)
        third = await blind.abefore_agent(merged, None)
        assert third[STATE_BASELINE]["workspace"]["name"] == "Old Name"
        assert _rows(third) == []

        # The rename row survived the cutoff and still says New Name.
        fourth = await later.abefore_agent({**merged, **third}, None)
        assert fourth is None or _rows(fourth) == []

        renamed_back = _middleware(
            _session("# Notes"), sandbox_enabled=True,
            workspace_name="Old Name", workspace_description="old desc",
        )
        fifth = await renamed_back.abefore_agent({**merged, **third}, None)
        row = runtime_update_from_message(_rows(fifth)[0])
        assert row.kind == "workspace_changed"
        assert row.text.endswith("Name and description are back to what <workspace> says.")

    @pytest.mark.asyncio
    async def test_a_flash_build_drops_the_memo_pointer_a_ptc_epoch_froze(self):
        store = MagicMock()

        async def aget(namespace, key):
            item = MagicMock()
            item.value = {"memo_count": 3}
            return item

        store.aget = AsyncMock(side_effect=aget)
        ptc = _middleware(
            _session("# Notes"), sources=_sources(store, memo=("u", "memos")), sandbox_enabled=True
        )
        state = await ptc.abefore_agent({}, None)
        assert "<memo-index" in _text(await _render(ptc, state))

        flash = _middleware(None)
        second = await flash.abefore_agent(state, None)
        assert second[STATE_BASELINE]["reason"] == "source_removed"
        assert "<memo-index" not in _text(await _render(flash, {**state, **second}))

    def test_the_workspace_name_is_escaped_as_element_text(self):
        block = _workspace_block(
            Workspace(name="A & B <lab>", description="O'Brien")
        )
        assert "A &amp; B &lt;lab&gt;" in block
        assert "O'Brien" in block

    def test_a_workspace_with_no_name_leaves_no_empty_tag(self):
        assert _workspace_block(Workspace(description="x")) == ""


# ---------------------------------------------------------------------------
# The harness-authored blocks
# ---------------------------------------------------------------------------


class TestTheHarnessBlocks:
    """The MCP roster and the skills manifest obey the same rule as agent.md.

    Both used to be appended to the system message per call, which put text in
    front of the history that moved whenever a server or a skill was
    discovered. They freeze into the epoch instead, and a change arrives as a
    row.
    """

    ROSTER = "- yfinance: quotes and fundamentals"
    MANIFEST = "## Available Skills\n\n- **pdf**: read and write PDFs"

    def _middleware(self, texts: dict):
        """A middleware whose two block sources read a dict the test can move."""
        return _middleware(
            _session("# Notes"),
            blocks={
                "mcp_servers": lambda _state: texts["mcp_servers"],
                "skills": lambda _state: texts["skills"],
            },
        )

    @pytest.mark.asyncio
    async def test_both_sources_freeze_into_the_epoch_and_render_in_the_block(self):
        texts = {"mcp_servers": self.ROSTER, "skills": self.MANIFEST}
        mw = self._middleware(texts)
        state = await mw.abefore_agent({}, None)

        frozen = state[STATE_BASELINE]["blocks"]
        assert frozen["mcp_servers"]["text"] == self.ROSTER
        assert frozen["skills"]["sha256"] == sha256_text(self.MANIFEST)

        text = _text(await _render(mw, state))
        assert self.ROSTER in text
        assert f"<skills>\n{self.MANIFEST}\n</skills>" in text
        order = [
            text.index("<user_identity"),
            text.index("<mcp-servers>"),
            text.index("<skills>"),
            text.index("<agentmd"),
        ]
        assert order == sorted(order)

        # Frozen for the turn, like every other source: a roster that grows
        # between two calls of one turn must not move the block.
        texts["mcp_servers"] = f"{self.ROSTER}\n- polygon: market data"
        assert _text(await _render(mw, state)) == text

    @pytest.mark.asyncio
    async def test_a_source_that_is_not_configured_leaves_no_block(self):
        """Flash has no MCP roster to state, and no hole to rebuild for."""
        mw = _middleware(
            _session("# Notes"), blocks={"skills": lambda _state: self.MANIFEST}
        )
        state = await mw.abefore_agent({}, None)

        assert set(state[STATE_BASELINE]["blocks"]) == {"skills"}
        assert state[STATE_BASELINE]["incomplete"] is False
        assert "<mcp-servers>" not in _text(await _render(mw, state))

    @pytest.mark.asyncio
    async def test_a_changed_roster_writes_one_row_and_leaves_the_block_frozen(self):
        texts = {"mcp_servers": self.ROSTER, "skills": self.MANIFEST}
        mw = self._middleware(texts)
        state = await mw.abefore_agent({}, None)

        texts["mcp_servers"] = f"{self.ROSTER}\n- polygon: market data"
        second = await mw.abefore_agent(state, None)

        rows = _rows(second)
        assert len(rows) == 1
        row = runtime_update_from_message(rows[0])
        assert row.kind == "mcp_servers_changed"
        assert "+- polygon: market data" in row.text
        assert "<mcp-servers> (frozen copy)" in row.text
        assert row.provenance == {"source": "harness"}

        # The epoch did not rebuild; only history grew.
        assert second[STATE_BASELINE]["epoch"] == 1
        assert second[STATE_BASELINE]["blocks"]["mcp_servers"]["text"] == self.ROSTER
        assert second[STATE_BASELINE]["drift_updates"] == 1

        # And the same change is not re-emitted on the turn after.
        assert await mw.abefore_agent({**state, **second}, None) is None

    @pytest.mark.asyncio
    async def test_a_changed_manifest_writes_one_skills_row(self):
        texts = {"mcp_servers": self.ROSTER, "skills": self.MANIFEST}
        mw = self._middleware(texts)
        state = await mw.abefore_agent({}, None)

        texts["skills"] = f"{self.MANIFEST}\n- **xlsx**: read and write workbooks"
        second = await mw.abefore_agent(state, None)

        rows = _rows(second)
        assert len(rows) == 1
        row = runtime_update_from_message(rows[0])
        assert row.kind == "skills_changed"
        assert "+- **xlsx**: read and write workbooks" in row.text
        assert row.provenance == {"source": "harness"}

    @pytest.mark.asyncio
    async def test_a_manifest_with_no_skills_in_it_is_an_answer_not_a_hole(self):
        """An empty manifest is a real state; only a None means "not answered"."""
        mw = self._middleware({"mcp_servers": self.ROSTER, "skills": ""})
        state = await mw.abefore_agent({}, None)

        assert state[STATE_BASELINE]["blocks"]["skills"]["text"] == ""
        assert state[STATE_BASELINE]["incomplete"] is False
        assert "<skills>" not in _text(await _render(mw, state))

    @pytest.mark.asyncio
    async def test_a_roster_that_has_not_answered_marks_the_epoch_incomplete(self):
        """Discovery that has not returned must not freeze in as "no servers"."""
        texts: dict = {"mcp_servers": None, "skills": self.MANIFEST}
        mw = self._middleware(texts)
        state = await mw.abefore_agent({}, None)

        assert "mcp_servers" not in state[STATE_BASELINE]["blocks"]
        assert state[STATE_BASELINE]["incomplete"] is True
        assert "<mcp-servers>" not in _text(await _render(mw, state))

        # The hole forces one more rebuild, and exactly one.
        texts["mcp_servers"] = self.ROSTER
        second = await mw.abefore_agent(state, None)
        assert second[STATE_BASELINE]["reason"] == "incomplete"
        assert second[STATE_BASELINE]["blocks"]["mcp_servers"]["text"] == self.ROSTER
        assert second[STATE_BASELINE]["incomplete"] is False
        assert await mw.abefore_agent({**state, **second}, None) is None

    @pytest.mark.asyncio
    async def test_a_thread_that_predates_the_blocks_rebuilds_once_to_take_them(self):
        """A stored epoch with no `blocks` is a thread from before this deploy.

        Nothing else would move it: it is stored, complete, uncompacted and
        under drift. Without a rebuild the model would see neither element
        and no row could describe them, since a row measures against a
        frozen copy.
        """
        texts = {"mcp_servers": self.ROSTER, "skills": self.MANIFEST}
        mw = self._middleware(texts)
        stored = (await mw.abefore_agent({}, None))[STATE_BASELINE]
        old = {k: v for k, v in stored.items() if k != "blocks"}
        for kind in ("mcp_servers", "skills"):
            old["observed"] = {k: v for k, v in old["observed"].items() if k != kind}

        second = await mw.abefore_agent({STATE_BASELINE: old}, None)

        assert second[STATE_BASELINE]["reason"] == "new_source"
        assert second[STATE_BASELINE]["blocks"]["mcp_servers"]["text"] == self.ROSTER
        assert second[STATE_BASELINE]["blocks"]["skills"]["text"] == self.MANIFEST
        assert _rows(second) == []
        assert "<mcp-servers>" in _text(await _render(mw, second))
        assert await mw.abefore_agent({STATE_BASELINE: second[STATE_BASELINE]}, None) is None

    @pytest.mark.asyncio
    async def test_a_source_that_raises_is_a_hole_rather_than_an_empty_block(self):
        def _boom(_state):
            raise RuntimeError("discovery blew up")

        mw = _middleware(_session("# Notes"), blocks={"mcp_servers": _boom})
        state = await mw.abefore_agent({}, None)

        assert state[STATE_BASELINE]["blocks"] == {}
        assert state[STATE_BASELINE]["incomplete"] is True

    @pytest.mark.asyncio
    async def test_the_blocks_survive_the_state_round_trip(self):
        """They are checkpointed, so a reload has to hand back the same bytes."""
        mw = self._middleware({"mcp_servers": self.ROSTER, "skills": self.MANIFEST})
        stored = (await mw.abefore_agent({}, None))[STATE_BASELINE]

        epoch = BaselineEpoch.from_state(stored)
        assert epoch.blocks["mcp_servers"].text == self.ROSTER
        assert epoch.blocks["skills"].text == self.MANIFEST
        assert epoch.to_state()["blocks"] == stored["blocks"]
        assert epoch.cursor.observed["skills"] == sha256_text(self.MANIFEST)


# ---------------------------------------------------------------------------
# Breakpoint budget
# ---------------------------------------------------------------------------


@tool
def _probe(query: str) -> str:
    """A stand-in tool so the tools array has something to tag."""
    return query


class TestBreakpointBudget:
    @pytest.mark.asyncio
    async def test_the_wire_carries_exactly_four_anthropic_breakpoints(self):
        """tools, static system, baseline, turn row: the whole Anthropic budget."""
        mw = _middleware(_session("# Notes"))
        state = await mw.abefore_agent({}, None)

        captured: dict = {}

        async def capture(request: ModelRequest) -> ModelResponse:
            captured["request"] = request
            return MagicMock()

        chain = _compose(
            [
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                mw,
                TailEnvelopeMiddleware(
                    now=datetime(2026, 4, 5, 19, 42, tzinfo=UTC), guidance="lean"
                ),
            ],
            capture,
        )
        await chain(_request(state, tools=[_probe], rows=await _turn_row()))

        request = captured["request"]
        tool_marks = sum(
            1
            for t in request.tools
            if "cache_control" in (getattr(t, "extras", None) or {})
        )
        system_marks = sum(
            1 for b in request.system_message.content if "cache_control" in b
        )
        message_marks = sum(
            1
            for m in request.messages
            if isinstance(m.content, list)
            for b in m.content
            if isinstance(b, dict) and "cache_control" in b
        )

        assert (tool_marks, system_marks, message_marks) == (1, 2, 1)
        assert tool_marks + system_marks + message_marks == 4

        # The message breakpoint sits on the turn row, the newest history here:
        # a boundary at the very end of the request is one the next call can
        # never read back, and the row is what it still carries.
        blocks = request.messages[-1].content
        assert blocks[-2]["text"] == "What moved today?"
        assert "cache_control" not in blocks[-2]
        assert ENVELOPE_OPEN in blocks[-1]["text"]
        assert "7:42 PM UTC, Sunday, April 5, 2026" in blocks[-1]["text"]
        assert blocks[-1]["cache_control"] == {"type": "ephemeral"}

        # The baseline is the second system breakpoint, behind the static
        # prefix, which is now the last block anything appends before it.
        blocks = request.system_message.content
        assert len(blocks) == 2
        assert blocks[0]["text"] == "Static system prompt."
        assert "agentmd" in blocks[1]["text"]
        assert blocks[1]["cache_control"] == {"type": "ephemeral"}

    @pytest.mark.asyncio
    async def test_the_last_row_of_the_turn_takes_the_message_breakpoint(self):
        """The turn writes its anchor first and the change rows after it, so
        the marker lands on the last one rather than on the anchor."""
        session = _session("# Notes\nalpha")
        mw = _middleware(session)
        state = await mw.abefore_agent({}, None)
        _set_agent_md(session, "# Notes\nalpha\nbeta")
        second = await mw.abefore_agent(state, None)

        captured: dict = {}

        async def capture(request: ModelRequest) -> ModelResponse:
            captured["request"] = request
            return MagicMock()

        tail = TailEnvelopeMiddleware(
            now=datetime(2026, 4, 5, 19, 42, tzinfo=UTC), guidance="lean"
        )
        request = _request({**state, **second}, rows=await _turn_row())
        request = request.override(messages=[*request.messages, *_rows(second)])
        await tail.awrap_model_call(request, capture)

        blocks = captured["request"].messages[-1].content
        assert len(blocks) == 3
        assert blocks[0]["text"] == "What moved today?"
        assert "7:42 PM UTC, Sunday, April 5, 2026" in blocks[1]["text"]
        assert "+beta" in blocks[2]["text"]
        assert blocks[2]["cache_control"] == {"type": "ephemeral"}
        assert "cache_control" not in blocks[0]
        assert "cache_control" not in blocks[1]


def _compose(middlewares, final_handler):
    async def chain(request: ModelRequest) -> ModelResponse:
        return await final_handler(request)

    for mw in reversed(middlewares):
        outer = chain

        async def wrapper(req, *, _mw=mw, _h=outer):
            return await _mw.awrap_model_call(req, _h)

        chain = wrapper

    return chain
