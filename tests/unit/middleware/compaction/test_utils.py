"""Tests for compaction reconstruction: orphan-strip + id-anchored boundary.

These cover the production "orphaned tool_result" brick: a positional
``cutoff_index`` that drifts onto a ``ToolMessage`` reconstructs into a summary
turn whose first content block is an orphaned ``tool_result`` (Anthropic 400).
The fixes are (1) strip any leading ``ToolMessage`` from the reconstructed tail
and (2) track the boundary by the first preserved message's id so list
perturbation can't silently shift it.
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction.types import CompactionEvent
from ptc_agent.agent.middleware.compaction.utils import (
    build_compaction_event,
    compute_absolute_cutoff,
    get_effective_messages,
)


def _summary() -> HumanMessage:
    return HumanMessage(content="[Context Summary] ...", id="summary")


def _conversation() -> list:
    """[H0, A1, T2, H3, A4, T5] — a tool call/result pair straddling the cutoff."""
    return [
        HumanMessage(content="q0", id="0"),
        AIMessage(content="a1", id="1"),
        ToolMessage(content="r2", id="2", tool_call_id="tc2"),
        HumanMessage(content="q3", id="3"),
        AIMessage(content="a4", id="4"),
        ToolMessage(content="r5", id="5", tool_call_id="tc5"),
    ]


class TestBuildCompactionEvent:
    def test_grounds_cutoff_at_anchor_position(self):
        raw = _conversation()
        preserved = raw[3:]
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=preserved,
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=3,
            previous_event=None,
        )
        assert event["anchor_message_id"] == "3"
        assert event["cutoff_index"] == 3
        # cutoff_index must point at the anchor message in the raw list
        assert raw[event["cutoff_index"]].id == event["anchor_message_id"]

    def test_empty_preserved_falls_back_to_arithmetic(self):
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=[],
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=6,
            previous_event=None,
        )
        assert event["anchor_message_id"] is None
        assert event["cutoff_index"] == 6  # compute_absolute_cutoff(6, None)

    def test_empty_preserved_chained_uses_previous_event(self):
        raw = _conversation()
        prev: CompactionEvent = {
            "cutoff_index": 2,
            "summary_message": _summary(),
            "file_path": None,
        }
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=[],
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=4,
            previous_event=prev,
        )
        assert event["anchor_message_id"] is None
        # compute_absolute_cutoff(4, prev) == 2 + 4 - 1 == 5
        assert event["cutoff_index"] == compute_absolute_cutoff(4, prev) == 5


class TestGetEffectiveMessages:
    def test_none_event_returns_messages_unchanged(self):
        raw = _conversation()
        assert get_effective_messages(raw, None) is raw

    def test_happy_path_positional_still_valid(self):
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=raw[3:],
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=3,
            previous_event=None,
        )
        result = get_effective_messages(raw, event)
        assert result[0] is event["summary_message"]
        assert [m.id for m in result[1:]] == ["3", "4", "5"]

    def test_id_anchor_overrides_drifted_positional_index(self):
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=raw[3:],
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=3,
            previous_event=None,
        )
        # Perturb: insert a message before the cutoff, shifting the tail right.
        drifted = list(raw)
        drifted.insert(3, HumanMessage(content="injected", id="x"))
        result = get_effective_messages(drifted, event)
        # Boundary must follow the anchor message, not the stale index 3.
        assert [m.id for m in result[1:]] == ["3", "4", "5"]
        assert all(m.id != "x" for m in result[1:])

    def test_orphan_strip_on_legacy_event(self):
        """Legacy event (no anchor) whose positional cutoff lands on a ToolMessage."""
        raw = _conversation()
        legacy: CompactionEvent = {
            "cutoff_index": 2,  # points at ToolMessage T2
            "summary_message": _summary(),
            "file_path": None,
        }
        result = get_effective_messages(raw, legacy)
        assert result[0] is legacy["summary_message"]
        # The orphaned leading ToolMessage must be stripped.
        assert not isinstance(result[1], ToolMessage)
        assert [m.id for m in result[1:]] == ["3", "4", "5"]

    def test_anchor_not_found_falls_back_to_positional_and_strips(self):
        raw = _conversation()
        event: CompactionEvent = {
            "cutoff_index": 2,  # ToolMessage
            "summary_message": _summary(),
            "file_path": None,
            "anchor_message_id": "missing",
        }
        result = get_effective_messages(raw, event)
        # Anchor unresolvable -> positional fallback -> still strips the orphan.
        assert not isinstance(result[1], ToolMessage)
        assert [m.id for m in result[1:]] == ["3", "4", "5"]

    def test_reconstruction_never_starts_with_tool_message(self):
        """Exact brick reproduction: summary + leading orphaned tool_result."""
        raw = [
            HumanMessage(content="q0", id="0"),
            AIMessage(content="a1", id="1"),
            ToolMessage(content="orphan", id="2", tool_call_id="tc2"),
            HumanMessage(content="q3", id="3"),
        ]
        # Drifted positional cutoff onto the ToolMessage with a stale/absent anchor.
        event: CompactionEvent = {
            "cutoff_index": 2,
            "summary_message": _summary(),
            "file_path": None,
            "anchor_message_id": None,
        }
        result = get_effective_messages(raw, event)
        assert len(result) >= 1
        assert not any(
            isinstance(m, ToolMessage) for m in result[1:2]
        ), "reconstruction must not start with an orphaned tool_result"

    def test_orphan_strip_matches_dict_shaped_tool_result(self):
        """Backstop is format-agnostic: a dict-shaped tool result is stripped too.

        The checkpoint reducer coerces writes to typed messages, so this can't
        occur on the persisted path today — but the orphan-strip is the crash
        backstop and must not rely on that invariant holding forever.
        """
        raw = [
            {"role": "tool", "content": "orphan", "tool_call_id": "tc2", "id": "2"},
            HumanMessage(content="q3", id="3"),
            AIMessage(content="a4", id="4"),
        ]
        legacy: CompactionEvent = {
            "cutoff_index": 0,  # lands on the dict-shaped tool result
            "summary_message": _summary(),
            "file_path": None,
        }
        result = get_effective_messages(raw, legacy)
        assert result[0] is legacy["summary_message"]
        # The leading dict-shaped tool result must be stripped, not passed through.
        assert result[1:] == [raw[1], raw[2]]

    def test_orphan_strip_covers_injected_tool_pair(self):
        """Cutoff landing inside a tool-call/tool-result pair strips the orphan.

        When a compaction cutoff falls between an AIMessage(tool_calls=[...])
        and its matching ToolMessage, the reconstructed tail must not start
        with the orphaned ToolMessage (Anthropic 400: tool_result with no
        tool_use).
        """
        raw = [
            HumanMessage(content="q0", id="0"),
            AIMessage(content="a1", id="1"),
            AIMessage(
                content="",
                id="2",
                tool_calls=[
                    {
                        "name": "web_search",
                        "args": {"query": "NVDA"},
                        "id": "call_abc",
                    }
                ],
            ),
            ToolMessage(
                content="NVDA  $233.45",
                id="3",
                tool_call_id="call_abc",
                name="web_search",
            ),
        ]
        event: CompactionEvent = {
            "cutoff_index": 3,  # between the pair, on the ToolMessage
            "summary_message": _summary(),
            "file_path": None,
            "anchor_message_id": None,
        }
        result = get_effective_messages(raw, event)
        assert result[0] is event["summary_message"]
        # The orphaned tool-result ToolMessage must be stripped.
        assert not any(isinstance(m, ToolMessage) for m in result[1:2])

    def test_anchor_resolves_after_left_shift_removal(self):
        """A removed pre-cutoff message left-shifts the tail; anchor still finds it."""
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=raw[3:],
            summary_message=_summary(),
            file_path=None,
            effective_cutoff=3,
            previous_event=None,
        )
        # Remove a pre-cutoff message: anchor "3" now sits at index 2, not 3.
        drifted = [m for m in raw if m.id != "1"]
        result = get_effective_messages(drifted, event)
        assert [m.id for m in result[1:]] == ["3", "4", "5"]
