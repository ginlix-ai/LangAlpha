"""Settled contract of the file mount's notice inside the agent.

A turn does not wait for the mount to start, so a prompt can say it serves
when it does not. The notice is the correction: a durable row whenever the
mount differs from the last word the model can read on it, and nothing while
a start has yet to answer or under a prompt that already says it is down.
"""

from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.compaction.summary_request import _summarizable
from ptc_agent.agent.middleware.file_mount_notice import FileMountNoticeMiddleware
from ptc_agent.agent.middleware.runtime_context import (
    FILE_MOUNT_ROW_KIND,
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    STATE_BASELINE,
)
from ptc_agent.agent.middleware.runtime_context.baseline import _retained_change_rows
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
)

# The wording is prompt surface.
DOWN_TEXT = (
    "The file mount is not serving on this computer for now. Bash and code "
    "cannot open the files kept on the server (memory, memos, profile, "
    "automations, workflows); use Read, Edit and Write for them. Transcripts "
    "of past conversations are out of reach until it serves again."
)
UP_TEXT = "The file mount is serving again on this computer."


def _mount_row(serving: bool) -> HumanMessage:
    return build_update_message(
        DurableUpdate(
            kind=FILE_MOUNT_ROW_KIND,
            schema_version=1,
            text="",
            provenance={"source": "harness", "serving": serving},
        )
    )


def _state(*messages, frozen: bool | None = True, cutoff_index: int | None = None) -> dict:
    state: dict = {"messages": [HumanMessage(content="hi", id="u-0"), *messages]}
    if frozen is not None:
        state[STATE_BASELINE] = {"epoch": 1, "files_mounted": frozen}
    if cutoff_index is not None:
        state["_summarization_event"] = {"cutoff_index": cutoff_index}
    return state


async def _notice(
    serving: bool | None, state: dict, *, built: bool = True, mounts: bool = True
) -> list:
    """``serving`` False is a sandbox handed no mount; None, one handed the
    mount ahead of a start still to answer."""
    sandbox = SimpleNamespace(
        livefs=None if serving is False else SimpleNamespace(serving=serving),
        mounts_livefs=mounts,
    )
    written = await FileMountNoticeMiddleware(sandbox, files_mounted=built).abefore_model(state)
    return [] if written is None else written["messages"]


@pytest.mark.asyncio
class TestTheNotice:
    async def test_a_mount_that_serves_says_nothing(self):
        assert await _notice(True, _state()) == []

    async def test_a_start_on_its_way_says_nothing(self):
        """Neither side: a row saying it is down would be a guess, and so
        would one taking back an earlier row."""
        assert await _notice(None, _state()) == []
        assert await _notice(None, _state(_mount_row(False))) == []

    async def test_a_mount_found_down_writes_one_durable_row(self):
        rows = await _notice(False, _state())

        assert len(rows) == 1
        row = rows[0]
        assert row.additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE
        meta = row.additional_kwargs[RUNTIME_UPDATE_KEY]
        assert meta["kind"] == FILE_MOUNT_ROW_KIND
        assert meta["provenance"] == {"source": "harness", "serving": False}
        assert row.content == DOWN_TEXT
        # The Pregel path mints ids for a row a hook returns.
        assert row.id is None

    async def test_down_already_said_is_not_repeated(self):
        state = _state(_mount_row(False), AIMessage(content="working"))
        assert await _notice(False, state) == []

    async def test_serving_again_writes_one_row(self):
        rows = await _notice(True, _state(_mount_row(False)))

        assert [row.content for row in rows] == [UP_TEXT]
        assert rows[0].additional_kwargs[RUNTIME_UPDATE_KEY]["provenance"]["serving"] is True

    async def test_the_newest_row_decides(self):
        state = _state(_mount_row(False), _mount_row(True))
        assert await _notice(True, state) == []
        assert [row.content for row in await _notice(False, state)] == [DOWN_TEXT]

    async def test_a_down_row_a_summary_took_is_said_again_while_it_is_down(self):
        state = _state(_mount_row(False), AIMessage(content="kept"), cutoff_index=2)
        assert [row.content for row in await _notice(False, state)] == [DOWN_TEXT]
        # Up again, the prompt is the word left, and it already says so.
        assert await _notice(True, state) == []

    async def test_a_mount_handed_but_found_down_since_is_said_down(self):
        sandbox = SimpleNamespace(livefs=SimpleNamespace(serving=False), mounts_livefs=True)
        written = await FileMountNoticeMiddleware(sandbox, files_mounted=True).abefore_model(
            _state()
        )
        assert [row.content for row in written["messages"]] == [DOWN_TEXT]

    async def test_a_prompt_that_says_it_is_down_gets_no_row(self):
        """It already says what the row would, and nothing in it would give
        a row that the mount is back anything to refer to."""
        assert await _notice(False, _state(frozen=False)) == []
        # A rebuilt epoch under a row from the one before it.
        assert await _notice(True, _state(_mount_row(False), frozen=False)) == []

    async def test_a_machine_that_never_mounts_gets_no_row(self):
        """A thread moved there keeps a prompt that says mounted, and its
        file tools reach the transcripts the row would say are out of reach."""
        assert await _notice(False, _state(), mounts=False) == []

    @pytest.mark.parametrize(("built", "rows"), [(True, 1), (False, 0)])
    async def test_a_thread_with_no_epoch_yet_goes_by_the_built_prompt(self, built, rows):
        assert len(await _notice(False, _state(frozen=None), built=built)) == rows


@pytest.mark.asyncio
class TestAClaudeCodeTurn:
    """It runs the notice once, as the turn opens, and waits there for a
    start still to answer: nothing tells it later."""

    @pytest.mark.parametrize(
        ("ends_serving", "rows"), [(True, []), (False, [DOWN_TEXT])], ids=["serves", "fails"]
    )
    async def test_it_waits_for_the_start_and_says_how_it_ended(self, ends_serving, rows):
        sandbox = SimpleNamespace(mounts_livefs=True)

        async def started() -> None:
            # The host takes the mount back where the start ended without one.
            sandbox.livefs = SimpleNamespace(serving=True) if ends_serving else None

        sandbox.livefs = SimpleNamespace(serving=None, started=started)
        notice = FileMountNoticeMiddleware(sandbox, files_mounted=True, waits_for_start=True)

        written = await notice.abefore_model(_state())

        assert [row.content for row in (written or {}).get("messages", [])] == rows


class TestTheRowAmongTheOthers:
    def test_a_summary_does_not_carry_it(self):
        """The notice states it again while it holds."""
        kept = _summarizable([HumanMessage(content="hi"), _mount_row(False)])
        assert [message.content for message in kept] == ["hi"]

    def test_a_rebuild_does_not_count_it_as_a_change_row(self):
        assert _retained_change_rows(_state(_mount_row(False))) == ()
