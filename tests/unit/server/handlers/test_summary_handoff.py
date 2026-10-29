"""The hand-off at a model switch.

A thread whose provider compacted it into a summary only that provider can
read (OpenAI's encrypted item) reads a digest on any other model. When the
user moves it to such a model, the model it was on writes the summary out as
text, in the background, in place of the digest.
"""

import asyncio
from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from ptc_agent.agent.middleware.compaction.utils import build_summary_message, with_summary_text
from src.llms.provider_compaction import COMPACTION_BETA

HANDLER = "src.server.handlers.thread_maintenance"
LLM_HANDLER = "src.server.services.llm.config"
ASK = "ptc_agent.agent.middleware.compaction.ask_between_turns"
FIRE = "src.server.handlers.chat.detached.fire_and_forget"
COMPACTION_TIMEOUT = "src.config.settings.get_compaction_timeout"
RUNNER_GET_INSTANCE = "src.server.services.thread_mutation.ThreadMutationRunner.get_instance"

_ITEM = {"type": "compaction", "encrypted_content": "opaque"}
_BLOCK = {"type": "compaction", "content": "the provider's summary", "signature": "sig-1"}


@pytest.fixture(autouse=True)
def _official_hosts(monkeypatch):
    for var in ("OPENAI_BASE_URL", "OPENAI_API_BASE", "ANTHROPIC_BASE_URL", "ANTHROPIC_API_URL"):
        monkeypatch.delenv(var, raising=False)


def _gpt() -> ChatOpenAI:
    return ChatOpenAI(
        model="gpt-test",
        api_key="x",
        output_version="responses/v1",
        metadata={"provider_route": "openai"},
    )


def _claude() -> ChatAnthropic:
    return ChatAnthropic(
        model="claude-test",
        api_key="x",
        betas=[COMPACTION_BETA],
        metadata={"provider_route": "anthropic"},
    )


def _opaque():
    return build_summary_message(
        "a digest", None, source="provider", native={"items": [_ITEM]}, origin="openai"
    )


def _readable():
    return build_summary_message(
        "the provider's summary",
        None,
        source="provider",
        native={"block": _BLOCK},
        origin="anthropic",
    )


def _event(summary):
    return {"summary_message": summary, "cutoff_index": 4, "anchor_message_id": "h2"}


class _Thread:
    """The thread's checkpoint where its next turn starts, its row and its
    workspace; ``models`` maps a model name (None for the default) to the
    client a resolve of it runs, or to an exception it raises."""

    def __init__(self, summary, models, *, llm_model="m-new", interrupts=()):
        self.values = {"_summarization_event": _event(summary) if summary else None}
        self.models = models
        self.interrupts = list(interrupts)
        self.meta = {
            "workspace_id": "ws-1",
            "llm_model": llm_model,
            "reasoning_effort": "high",
            "fast_mode": None,
        }
        self.saver = MagicMock()
        self.saver.aget_tuple = AsyncMock(
            side_effect=lambda _cfg: SimpleNamespace(
                checkpoint={"channel_values": self.values},
                pending_writes=[
                    ("task-1", "__interrupt__", [SimpleNamespace(id=f"i{n}", value={})])
                    for n, _ in enumerate(self.interrupts)
                ],
            )
        )
        self.resolved: list = []

    async def _resolve(self, base_cfg, user_id, model, is_byok, **kwargs):
        self.resolved.append((model, kwargs))
        client = self.models[model]
        if isinstance(client, Exception):
            raise client
        config = MagicMock(name=f"config-{model}")
        config.get_llm_client.return_value = client
        return config

    def patches(self):
        return (
            patch(f"{HANDLER}.get_checkpointer", return_value=self.saver),
            patch(f"{HANDLER}.turn_base_checkpoint_id", new=AsyncMock(return_value="cp-base")),
            patch(
                "src.server.database.conversation.get_thread_auth_meta",
                new=AsyncMock(return_value=self.meta),
            ),
            patch(
                "src.server.database.workspace.get_workspace",
                new=AsyncMock(return_value={"workspace_id": "ws-1", "user_id": "user-1"}),
            ),
            patch(
                "src.server.database.api_keys.is_byok_active",
                new=AsyncMock(return_value=False),
            ),
            patch(f"{LLM_HANDLER}.resolve_llm_config", new=self._resolve),
        )


def _runner():
    from src.server.services.thread_mutation import MutationSession

    runner = MagicMock()
    runner.held = []
    runner.released = []

    @asynccontextmanager
    async def _exclusive(thread_id, verb):
        runner.held.append((thread_id, verb))
        try:
            yield MutationSession(conn=None, saver="fence-saver")
        finally:
            runner.released.append((thread_id, verb))

    runner.exclusive = _exclusive
    return runner


@asynccontextmanager
async def _refused(_thread_id, _verb):
    raise HTTPException(status_code=409, detail={"code": "thread_busy"})
    yield  # pragma: no cover


async def _switch(
    thread: _Thread,
    written=None,
    previous="m-old",
    *,
    refused=False,
    timeout=None,
    ask=None,
):
    """``hand_off_on_switch`` on ``thread``, its background hand-off run to the
    end, the ask answering ``written``; what it said and what was built and
    written."""
    from src.server.handlers.chat.detached import _background_tasks
    from src.server.handlers.thread_maintenance import ThreadAgent, hand_off_on_switch

    graph, state = MagicMock(), MagicMock(values=thread.values)
    build = AsyncMock(
        return_value=ThreadAgent("thread-1", graph, state, [], "ws-1", None, None)
    )
    update = AsyncMock()
    ask = ask or AsyncMock(return_value=written)
    runner = _runner()
    with ExitStack() as stack:
        for p in thread.patches():
            stack.enter_context(p)
        stack.enter_context(patch(RUNNER_GET_INSTANCE, return_value=runner))
        stack.enter_context(patch(f"{HANDLER}._thread_agent", new=build))
        stack.enter_context(patch(f"{HANDLER}._update_graph_state", new=update))
        stack.enter_context(patch(ASK, new=ask))
        if refused:
            stack.enter_context(patch(f"{HANDLER}._hold_thread_mutation", new=_refused))
        if timeout is not None:
            stack.enter_context(patch(COMPACTION_TIMEOUT, return_value=timeout))
        said = await hand_off_on_switch("thread-1", "user-1", previous)
        # What ``fire_and_forget`` swallows is not the test's to see.
        await asyncio.gather(*list(_background_tasks))
    return SimpleNamespace(said=said, build=build, update=update, ask=ask, runner=runner)


class TestHandOffOnSwitch:
    @pytest.mark.asyncio
    async def test_a_move_off_the_items_provider_hands_the_summary_off(self):
        from ptc_agent.agent.middleware.compaction import HandOff

        opaque = _opaque()
        written = with_summary_text(opaque, "Goal: chart AAPL.")
        thread = _Thread(opaque, {"m-new": _claude(), "m-old": _gpt()})

        run = await _switch(thread, written)

        assert run.said is True
        # The new model is the thread's, resolved as its next turn; the model it
        # was on writes the summary out, not the one it moved to. Asked before
        # the fence and again under it.
        assert [name for name, _ in thread.resolved] == ["m-new", "m-old"] * 2
        (_, kwargs), _ = thread.resolved[-2:]
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["workspace_id"] == "ws-1"
        assert kwargs["role"] == "analyst"
        # Read where the thread's next turn starts.
        thread.saver.aget_tuple.assert_awaited()
        assert (
            thread.saver.aget_tuple.await_args.args[0]["configurable"]["checkpoint_id"]
            == "cp-base"
        )
        assert run.runner.held == [("thread-1", "handoff")]
        assert run.runner.released == [("thread-1", "handoff")]
        build = run.build.await_args.kwargs
        assert build["checkpointer"] == "fence-saver"
        assert build["role"] == "analyst"
        assert build["config"].get_llm_client() is thread.models["m-old"]
        assert run.ask.await_args.args[2] == HandOff()
        values = run.update.await_args.args[2]
        assert values == {"_summarization_event": {**_event(opaque), "summary_message": written}}

    @pytest.mark.asyncio
    async def test_a_move_to_a_model_that_replays_the_item_needs_none(self):
        thread = _Thread(_opaque(), {"m-new": _gpt(), "m-old": _gpt()})

        run = await _switch(thread, with_summary_text(_opaque(), "x"))

        assert run.said is False
        run.build.assert_not_awaited()
        run.update.assert_not_awaited()
        # Asked before the fence, so the switch never held the thread.
        assert run.runner.held == []

    @pytest.mark.asyncio
    async def test_a_new_model_that_cannot_run_now_is_handed_the_text(self):
        """It reads the text when it can run."""
        opaque = _opaque()
        written = with_summary_text(opaque, "written out")
        thread = _Thread(opaque, {"m-new": RuntimeError("oauth lapsed"), "m-old": _gpt()})

        run = await _switch(thread, written)

        assert run.said is True
        assert run.update.await_args.args[2]["_summarization_event"]["summary_message"] is written

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "summary",
        [
            None,
            pytest.param(_readable(), id="readable"),
            pytest.param(build_summary_message("the summarizer's", None), id="plain"),
            pytest.param(with_summary_text(_opaque(), "written out"), id="handed-off"),
        ],
    )
    async def test_a_thread_with_no_summary_only_its_provider_can_read_needs_none(
        self, summary
    ):
        thread = _Thread(summary, {"m-new": _claude(), "m-old": _gpt()})

        run = await _switch(thread)

        assert run.said is False
        assert thread.resolved == []
        run.build.assert_not_awaited()
        assert run.runner.held == []

    @pytest.mark.asyncio
    async def test_a_failed_read_says_no_hand_off(self):
        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": _gpt()})
        thread.saver.aget_tuple = AsyncMock(side_effect=ConnectionError("pool closed"))

        run = await _switch(thread)

        assert run.said is False
        run.build.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_step_waiting_on_the_user_needs_none(self):
        """A write ends the step, and the user's answer would have nothing to
        resume: no hand-off is started, nor the thread taken."""
        from src.server.handlers.thread_maintenance import _summary_to_hand_off

        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": _gpt()}, interrupts=[{"id": "i"}])
        fire = MagicMock()
        with ExitStack() as stack:
            for p in thread.patches():
                stack.enter_context(p)
            assert await _summary_to_hand_off("thread-1") is None
            stack.enter_context(patch(FIRE, new=fire))
            from src.server.handlers.thread_maintenance import hand_off_on_switch

            assert await hand_off_on_switch("thread-1", "user-1", "m-old") is False
        fire.assert_not_called()

        run = await _switch(thread)
        assert run.said is False
        assert run.runner.held == []

    @pytest.mark.asyncio
    async def test_no_step_waiting_hands_the_summary_over(self):
        from src.server.handlers.thread_maintenance import _summary_to_hand_off

        opaque = _opaque()
        thread = _Thread(opaque, {})
        with ExitStack() as stack:
            for p in thread.patches():
                stack.enter_context(p)
            assert await _summary_to_hand_off("thread-1") is opaque

    @pytest.mark.asyncio
    async def test_a_thread_the_fence_refuses_says_no_hand_off(self):
        """A turn or background tasks still hold it: the new model reads the
        digest, and nothing is built or written."""
        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": _gpt()})

        run = await _switch(thread, with_summary_text(_opaque(), "x"), refused=True)

        assert run.said is False
        run.build.assert_not_awaited()
        run.ask.assert_not_awaited()
        run.update.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "old", [pytest.param(_claude(), id="cannot-read"), RuntimeError("oauth lapsed")]
    )
    async def test_a_previous_model_that_cannot_write_it_leaves_the_digest(self, old):
        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": old})

        run = await _switch(thread)

        assert run.said is False
        run.build.assert_not_awaited()
        run.update.assert_not_awaited()
        assert run.runner.held == []

    @pytest.mark.asyncio
    async def test_nothing_written_leaves_the_digest(self):
        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": _gpt()})

        run = await _switch(thread, None)

        assert run.said is True
        run.ask.assert_awaited_once()
        run.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_hand_off_that_outlasts_the_compaction_timeout_is_given_up(self):
        """True still: it held the thread and had a summary to write. A send
        waiting on it is not held past the timeout, and the digest stays."""

        async def _slow(*_args, **_kwargs):
            await asyncio.sleep(10)

        thread = _Thread(_opaque(), {"m-new": _claude(), "m-old": _gpt()})

        run = await _switch(thread, timeout=0.05, ask=AsyncMock(side_effect=_slow))

        assert run.said is True
        run.ask.assert_awaited_once()
        run.update.assert_not_awaited()
        assert run.runner.released == [("thread-1", "handoff")]
