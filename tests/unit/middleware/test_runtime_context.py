"""Settled contracts of the runtime-context tail envelope and its carrier.

Three things here are load-bearing enough to pin. The hard cap decides what the
model does NOT see when the one live block runs long. The carrier decides how
harness text reaches the model at all: a block inside the message it belongs to
where the provider has no operator channel, a message of its own where it has
one, and never inside a tool result. And breakpoint 4 has to land on content the
next call still carries, which is the only reason the pin is worth placing.

The durable rows go through that same carrier. They are persisted history, so
the shape is applied to a request-only copy and the checkpoint keeps the
provider-neutral message.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from ptc_agent.agent.middleware.runtime_context import (
    ENVELOPE_CLOSE,
    ENVELOPE_HARD_CAP_TOKENS,
    ENVELOPE_OPEN,
    RUNTIME_CONTEXT_SOURCE,
    RUNTIME_UPDATE_SOURCE,
    DurableUpdate,
    PinSite,
    apply_breakpoint,
    build_update_message,
    carry_durable_updates,
    compose_request,
    count_tokens,
    frame_reminder,
    render_call_updates,
)
from ptc_agent.agent.middleware.runtime_context.carrier import resolve_carrier_shape

NOW = datetime(2026, 4, 5, 12, 0, tzinfo=UTC)


def _padded(target_tokens: int, word: str) -> str:
    """Text of roughly ``target_tokens`` tokens, built from one repeated word."""
    text = word
    while count_tokens(text) < target_tokens:
        text = f"{text} {word}"
    return text


def _stamp(text: str) -> DurableUpdate:
    """A per-call row, in the one kind the envelope renders verbatim."""
    return DurableUpdate(
        kind="market_watch", schema_version=1, text=text, created_at=NOW
    )


class TestTheHardCap:
    """The block is dropped whole, never trimmed: half a stamp is worse than none."""

    def test_a_block_under_the_cap_is_rendered_with_its_header(self):
        block = render_call_updates([_stamp("AAPL 232.10 (+1.2%)")], NOW, "lean")

        assert "AAPL 232.10 (+1.2%)" in block
        assert "Runtime context for you" in block
        assert count_tokens(block) <= ENVELOPE_HARD_CAP_TOKENS

    def test_a_block_over_the_cap_is_dropped(self):
        block = render_call_updates([_stamp(_padded(2600, "alpha"))], NOW, "lean")

        assert block == ""

    def test_nothing_to_say_renders_nothing(self):
        assert render_call_updates([], NOW, "lean") == ""
        assert render_call_updates([_stamp("")], NOW, "lean") == ""

    def test_rows_render_oldest_first(self):
        older = _stamp("older")
        newer = DurableUpdate(
            kind="market_watch",
            schema_version=1,
            text="newer",
            created_at=datetime(2026, 4, 5, 12, 5, tzinfo=UTC),
        )

        block = render_call_updates([newer, older], NOW, "lean")

        assert block.index("older") < block.index("newer")


class TestCarrierShapes:
    """One resolver, three shapes; the reminder shape is what ships by default."""

    ENVELOPE = "context"
    FRAMED = "<system-reminder>\ncontext\n</system-reminder>"

    def _carried(self, messages: list, shape: str = "reminder") -> list:
        """The composed request, which is the only door onto a shape."""
        return compose_request(messages, self.ENVELOPE, shape).messages

    def test_shape_is_reminder_without_an_operator_channel(self):
        assert resolve_carrier_shape(None) == "reminder"
        assert resolve_carrier_shape(MagicMock()) == "reminder"

    def test_shape_falls_back_to_reminder_when_resolution_raises(self, monkeypatch):
        """A carrier fault costs a role, never a turn."""
        import src.llms.operator_channel as oc

        def _boom(model):
            raise RuntimeError("manifest is gone")

        monkeypatch.setattr(oc, "resolve_operator_channel", _boom)
        assert resolve_carrier_shape(MagicMock()) == "reminder"

    def test_reminder_merges_into_a_trailing_human_message(self):
        """The Claude Code shape: one message, the user's text first.

        The block carries no tag of its own. It reaches the endpoint verbatim on
        the OpenAI Chat Completions layout, and the carrier is innermost and
        request-only, so nothing downstream would ever have read one.
        """
        original = HumanMessage(content="What moved today?")

        result = self._carried([original])

        assert len(result) == 1
        blocks = result[0].content
        assert blocks == [
            {"type": "text", "text": "What moved today?"},
            {"type": "text", "text": self.FRAMED},
        ]

    def test_the_merged_block_leaves_the_message_kwargs_alone(self):
        """``additional_kwargs`` describe the message, and the message is the user's."""
        original = HumanMessage(
            content="hi", additional_kwargs={"lc_source": "user_upload"}
        )

        result = self._carried([original])

        assert result[0].additional_kwargs == {"lc_source": "user_upload"}

    def test_reminder_appends_to_an_existing_block_list(self):
        """An image-bearing message keeps its blocks; the envelope goes after them."""
        image = {"type": "image", "source": {"type": "url", "url": "x://y"}}
        original = HumanMessage(content=[image, {"type": "text", "text": "this one?"}])

        blocks = self._carried([original])[0].content

        assert blocks[:2] == [image, {"type": "text", "text": "this one?"}]
        assert blocks[2] == {"type": "text", "text": self.FRAMED}

    def test_reminder_stands_alone_after_a_tool_batch(self):
        """Harness text inside a tool result would sit under an untrusted label."""
        messages = [
            HumanMessage(content="q"),
            AIMessage(content=""),
            ToolMessage(content="result", tool_call_id="call_a"),
        ]

        result = self._carried(messages)

        assert len(result) == 4
        assert isinstance(result[-1], HumanMessage)
        assert result[-1].content == self.FRAMED
        assert result[-1].additional_kwargs["lc_source"] == RUNTIME_CONTEXT_SOURCE
        assert result[2].content == "result"

    def test_reminder_stands_alone_after_an_assistant_turn(self):
        result = self._carried([AIMessage(content="done")])

        assert len(result) == 2
        assert isinstance(result[-1], HumanMessage)

    def test_reminder_stands_alone_on_an_empty_history(self):
        result = self._carried([])

        assert len(result) == 1
        assert result[0].additional_kwargs["lc_source"] == RUNTIME_CONTEXT_SOURCE

    def test_never_mutates_the_user_message_or_the_input_list(self):
        original = HumanMessage(content="What moved today?")
        messages = [original]

        result = self._carried(messages)

        assert messages == [original], "input list must not be mutated"
        assert original.content == "What moved today?"
        assert result is not messages
        assert result[0] is not original

    def test_empty_envelope_appends_nothing(self):
        messages = [HumanMessage(content="hi")]
        assert compose_request(messages, "", "reminder").messages == messages

    def test_operator_shapes_stand_alone_and_win_over_the_reminder(self, monkeypatch):
        """A resolved channel is its own message, even behind a user turn."""
        import src.llms.operator_channel as oc

        for channel, kwarg in (
            ("developer", "__openai_role__"),
            ("system", "lc_operator_channel"),
        ):
            monkeypatch.setattr(
                oc, "resolve_operator_channel", lambda model, c=channel: c
            )
            user = HumanMessage(content="hi")

            shape = resolve_carrier_shape(MagicMock())
            result = self._carried([user], shape)

            assert len(result) == 2
            assert result[0] is user
            carried = result[-1]
            assert isinstance(carried, SystemMessage)
            assert carried.additional_kwargs[kwarg] in (channel, "developer")
            assert carried.additional_kwargs["lc_source"] == RUNTIME_CONTEXT_SOURCE
            # The role is the framing: an operator message carries the bare text.
            assert carried.content == self.ENVELOPE


class TestDurableCarrier:
    """A persisted row takes the model's shape at render time, not at write time."""

    ENVELOPE = "context"
    FRAMED = "<system-reminder>\ncontext\n</system-reminder>"

    def _row(self, text: str, kind: str = "agent_md_changed"):
        return build_update_message(
            DurableUpdate(
                kind=kind,
                schema_version=1,
                text=text,
                provenance={"source": "sandbox"},
                created_at=datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
            )
        )

    def _history(self):
        return [
            HumanMessage(content="What moved today?"),
            self._row("first body"),
            self._row("second body", kind="memory_changed:user"),
        ]

    def test_reminder_merges_every_row_into_the_user_message(self):
        """The Claude Code shape: harness attachments ride inside the message
        that precedes them, so a turn is one user message however many rows it
        collected."""
        result = carry_durable_updates(self._history(), "reminder")

        assert len(result) == 1
        blocks = result[0].content
        assert blocks[0] == {"type": "text", "text": "What moved today?"}
        assert [b["text"].startswith(ENVELOPE_OPEN) for b in blocks[1:]] == [True, True]
        assert [b["text"].endswith(ENVELOPE_CLOSE) for b in blocks[1:]] == [True, True]
        assert "first body" in blocks[1]["text"]
        assert "second body" in blocks[2]["text"]

    def test_reminder_stands_alone_with_no_preceding_human_message(self):
        """After an assistant turn there is nothing to ride inside."""
        result = carry_durable_updates([AIMessage(content="done"), self._row("body")], "reminder")

        assert len(result) == 2
        carried = result[-1]
        assert isinstance(carried, HumanMessage)
        assert carried.content.startswith(ENVELOPE_OPEN)
        assert "body" in carried.content
        assert carried.additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE

    @pytest.mark.parametrize("shape", ["system", "developer"])
    def test_operator_shapes_coalesce_the_rows_into_one_message(self, shape, monkeypatch):
        """Two adjacent operator entries are unproven on the Anthropic wire
        form, so consecutive rows become one message with a block each."""
        import src.llms.operator_channel as oc

        monkeypatch.setattr(oc, "resolve_operator_channel", lambda model: shape)
        result = carry_durable_updates(self._history(), shape)

        assert len(result) == 2
        assert isinstance(result[0], HumanMessage)
        carried = result[1]
        assert isinstance(carried, SystemMessage)
        assert carried.additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE
        assert [b["type"] for b in carried.content] == ["text", "text"]
        assert "first body" in carried.content[0]["text"]
        assert "second body" in carried.content[1]["text"]
        # No reminder framing: the role is the framing.
        assert ENVELOPE_OPEN not in carried.content[0]["text"]

    @pytest.mark.parametrize("shape", ["system", "developer"])
    def test_the_envelope_coalesces_into_the_trailing_row_message(self, shape):
        """A row followed by the envelope is still one operator message."""
        result = compose_request(self._history(), self.ENVELOPE, shape).messages

        assert len(result) == 2
        blocks = result[-1].content
        assert len(blocks) == 3
        assert blocks[-1]["text"] == self.ENVELOPE

    def test_a_row_that_renders_empty_is_dropped(self):
        """An empty message is a malformed request on several providers."""
        row = self._row("body").model_copy(update={"content": "   "})
        result = carry_durable_updates([HumanMessage(content="hi"), row], "reminder")

        assert len(result) == 1
        assert result[0].content == "hi"

    def test_nothing_is_mutated(self):
        """The persisted rows stay provider-neutral; only the request copy moves."""
        messages = self._history()
        snapshot = [(m, m.content) for m in messages]

        for shape in ("reminder", "system", "developer"):
            result = carry_durable_updates(messages, shape)
            assert result is not messages
            assert [(m, m.content) for m in messages] == snapshot

    @pytest.mark.parametrize("shape", ["reminder", "system", "developer"])
    def test_a_row_after_a_tool_result_stands_apart_from_it(self, shape):
        """Harness text under a tool result would wear its untrusted label."""
        tool_result = ToolMessage(content="ok", tool_call_id="c1")
        messages = [
            HumanMessage(content="go"),
            AIMessage(content="", tool_calls=[{"name": "Bash", "args": {}, "id": "c1"}]),
            tool_result,
            self._row("write your notes", kind="notes_due"),
        ]

        result = carry_durable_updates(messages, shape)

        assert len(result) == 4
        assert result[2] == tool_result
        assert result[2].content == "ok"
        assert not isinstance(result[3], ToolMessage)
        assert "write your notes" in str(result[3].content)
        assert result[3].additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE

    def test_a_history_without_rows_is_left_alone(self):
        messages = [HumanMessage(content="hi"), AIMessage(content="ok")]
        assert carry_durable_updates(messages, "reminder") == messages


class TestHistoryBreakpoint:
    """Breakpoint 4 goes on the newest content the next call still carries.

    The composer places the envelope and names the site in one pass, so the pin
    is where the carrier says it is rather than where a second reading of the
    finished list guesses it should be.
    """

    MARK = ("cache_control", {"type": "ephemeral"})

    def _composed(self, messages: list, shape: str = "reminder"):
        """The composed request with the marker applied at the site it named."""
        composed = compose_request(messages, "<envelope>", shape)
        if composed.pin is not None:
            apply_breakpoint(composed.messages, composed.pin, *self.MARK)
        return composed

    def test_merged_shape_pins_the_users_own_block(self):
        """The envelope block trails the marker, exactly as a transient should."""
        composed = self._composed([HumanMessage(content="hello")])

        assert composed.pin == PinSite(0, 0)
        blocks = composed.messages[0].content
        assert blocks[0]["cache_control"] == {"type": "ephemeral"}
        assert blocks[1]["text"] == frame_reminder("<envelope>")
        assert "cache_control" not in blocks[1]

    def test_merged_shape_pins_the_last_user_block_of_several(self):
        composed = self._composed(
            [
                HumanMessage(
                    content=[
                        {"type": "text", "text": "a"},
                        {"type": "text", "text": "b"},
                    ]
                )
            ]
        )

        blocks = composed.messages[0].content
        assert "cache_control" not in blocks[0]
        assert blocks[1]["cache_control"] == {"type": "ephemeral"}
        assert "cache_control" not in blocks[2]

    def test_merged_shape_walks_back_when_the_message_has_no_other_text(self):
        """An image-only user message cannot carry a marker, so the pin moves to
        the last message that can rather than onto the envelope."""
        image = {"type": "image", "source": {"type": "url", "url": "x://y"}}
        composed = self._composed(
            [AIMessage(content="earlier"), HumanMessage(content=[image])]
        )

        assert composed.pin == PinSite(0, None)
        messages = composed.messages
        assert messages[0].content[-1]["cache_control"] == {"type": "ephemeral"}
        assert messages[1].content[-1]["text"] == frame_reminder("<envelope>")
        assert "cache_control" not in messages[1].content[-1]

    def test_standalone_carrier_pins_the_message_before_the_envelope(self):
        """After a tool batch the envelope is a message of its own, so the
        marker goes on the last history message instead of inside one."""
        composed = self._composed(
            [
                HumanMessage(content="hello"),
                AIMessage(content=""),
                ToolMessage(content="result", tool_call_id="call_a"),
            ]
        )

        messages = composed.messages
        assert len(messages) == 4
        assert composed.pin == PinSite(2, None)
        assert messages[2].content[-1]["cache_control"] == {"type": "ephemeral"}
        assert messages[3].content == frame_reminder("<envelope>")
        assert messages[3].additional_kwargs["lc_source"] == RUNTIME_CONTEXT_SOURCE

    def test_a_standalone_row_can_take_the_marker(self):
        """A row is history the carrier only re-shaped, not transient text."""
        row = build_update_message(
            DurableUpdate(kind="memo_changed", schema_version=1, text="3 memos now")
        )
        composed = self._composed([AIMessage(content="done"), row])

        # The envelope merged into the standalone row, so the row's block takes it.
        assert composed.pin == PinSite(1, 0)
        blocks = composed.messages[1].content
        assert "3 memos now" in blocks[0]["text"]
        assert blocks[0]["cache_control"] == {"type": "ephemeral"}
        assert "cache_control" not in blocks[1]

    @pytest.mark.parametrize("shape", ["system", "developer"])
    def test_an_operator_message_is_skipped_rather_than_pinned(self, shape):
        """A breakpoint on that role is unproven, so the pin walks past it."""
        row = build_update_message(
            DurableUpdate(kind="memo_changed", schema_version=1, text="3 memos now")
        )
        composed = self._composed([HumanMessage(content="hello"), row], shape)

        assert composed.pin == PinSite(0, None)
        messages = composed.messages
        assert messages[0].content[-1]["cache_control"] == {"type": "ephemeral"}
        assert all("cache_control" not in b for b in messages[1].content)

    def test_nothing_is_pinned_when_only_the_envelope_could_take_it(self):
        """A boundary ending in the envelope is one the next call never reads
        back, so it is worth less than the breakpoint it spends."""
        composed = self._composed([])

        assert composed.pin is None
        assert composed.messages[0].content == frame_reminder("<envelope>")


class TestOperatorWindow:
    """The system shape is honored on the Anthropic wire only inside the window
    the tail opens around the one call it composed; the rest of the process
    sees the stock hoist."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("channel", "open_during_call"),
        [("system", True), ("developer", False), (None, False)],
        ids=["system", "developer", "reminder"],
    )
    async def test_window_matches_the_composed_shape(
        self, monkeypatch, channel, open_during_call
    ):
        import src.llms.operator_channel as oc
        from langchain.agents.middleware.types import ModelRequest
        from ptc_agent.agent.middleware.runtime_context.tail import TailEnvelopeMiddleware

        monkeypatch.setattr(oc, "resolve_operator_channel", lambda model: channel)
        seen: dict = {}

        async def handler(request):
            seen["open"] = oc._in_operator_request.get()
            return MagicMock()

        row = build_update_message(
            DurableUpdate(
                kind="turn_opened",
                schema_version=1,
                text="9:46 PM EDT",
                provenance={"source": "harness"},
                created_at=datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
            )
        )
        await TailEnvelopeMiddleware(
            now=datetime(2026, 4, 5, 12, 0, tzinfo=UTC), guidance="lean"
        ).awrap_model_call(
            ModelRequest(
                model=MagicMock(),
                messages=[HumanMessage(content="hi"), row],
                system_prompt="static",
                state={},
                tools=[],
            ),
            handler,
        )
        assert seen["open"] is open_during_call
        assert oc._in_operator_request.get() is False


class TestRequestLevelMarker:
    """Anthropic's direct API turns the caching middleware's top-level
    ``cache_control`` into a breakpoint of its own on the last block. With the
    tail's pin placed that is a fifth, which the API refuses outright, so the
    request-level marker is dropped exactly when the pin is placed."""

    @staticmethod
    def _request(model, settings):
        from langchain.agents.middleware.types import ModelRequest

        row = build_update_message(
            DurableUpdate(
                kind="turn_opened",
                schema_version=1,
                text="9:46 PM EDT",
                provenance={"source": "harness"},
                created_at=datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
            )
        )
        return ModelRequest(
            model=model,
            messages=[HumanMessage(content="hi"), row],
            system_prompt="static",
            state={},
            tools=[],
            model_settings=settings,
        )

    @pytest.mark.asyncio
    async def test_anthropic_request_marker_is_dropped_when_the_pin_is_placed(self, monkeypatch):
        import src.llms.operator_channel as oc
        from langchain_anthropic import ChatAnthropic
        from ptc_agent.agent.middleware.runtime_context.tail import TailEnvelopeMiddleware

        monkeypatch.setattr(oc, "resolve_operator_channel", lambda model: None)
        seen: dict = {}

        async def handler(request):
            seen["settings"] = request.model_settings
            seen["messages"] = request.messages
            return MagicMock()

        model = ChatAnthropic(model="claude-opus-5", api_key="x")
        await TailEnvelopeMiddleware(
            now=datetime(2026, 4, 5, 12, 0, tzinfo=UTC), guidance="lean"
        ).awrap_model_call(
            self._request(model, {"cache_control": {"type": "ephemeral"}, "max_tokens": 9}),
            handler,
        )
        assert seen["settings"] == {"max_tokens": 9}
        pinned = [
            b
            for m in seen["messages"]
            if isinstance(m.content, list)
            for b in m.content
            if isinstance(b, dict) and "cache_control" in b
        ]
        assert len(pinned) == 1

    @pytest.mark.asyncio
    async def test_settings_pass_through_without_a_marker(self, monkeypatch):
        import src.llms.operator_channel as oc
        from ptc_agent.agent.middleware.runtime_context.tail import TailEnvelopeMiddleware

        monkeypatch.setattr(oc, "resolve_operator_channel", lambda model: None)
        seen: dict = {}

        async def handler(request):
            seen["settings"] = request.model_settings
            return MagicMock()

        settings = {"cache_control": {"type": "ephemeral"}}
        await TailEnvelopeMiddleware(
            now=datetime(2026, 4, 5, 12, 0, tzinfo=UTC), guidance="lean"
        ).awrap_model_call(self._request(MagicMock(), settings), handler)
        assert seen["settings"] == settings
