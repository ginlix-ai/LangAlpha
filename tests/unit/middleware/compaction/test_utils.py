"""Tests for compaction reconstruction: orphan-strip + id-anchored boundary.

These cover the production "orphaned tool_result" brick: a positional
``cutoff_index`` that drifts onto a ``ToolMessage`` reconstructs into a summary
turn whose first content block is an orphaned ``tool_result`` (Anthropic 400).
The fixes are (1) strip any leading ``ToolMessage`` from the reconstructed tail
and (2) track the boundary by the first preserved message's id so list
perturbation can't silently shift it.
"""

from __future__ import annotations

import itertools

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction import utils
from ptc_agent.agent.middleware.compaction.types import CompactionEvent
from ptc_agent.agent.middleware.compaction.utils import (
    build_compaction_event,
    declared_tool_call_ids,
    find_group_safe_cutoff,
    get_effective_messages,
    partition_at_cutoff,
    strip_orphan_tool_messages,
)


def _summary() -> HumanMessage:
    return HumanMessage(content="[Context Summary] ...", id="summary")


def _call(tool_call_id: str) -> dict:
    return {"name": "web_search", "args": {"query": "q"}, "id": tool_call_id}


def _conversation() -> list:
    """[H0, A1, T2, H3, A4, T5] — a tool call/result pair straddling the cutoff.

    Each assistant turn really declares the call its ToolMessage answers. An
    earlier version did not, which made every tool result in the fixture an
    orphan and left the strip untestable: it could only ever be exercised on
    results that happened to be leading.
    """
    return [
        HumanMessage(content="q0", id="0"),
        AIMessage(content="a1", id="1", tool_calls=[_call("tc2")]),
        ToolMessage(content="r2", id="2", tool_call_id="tc2"),
        HumanMessage(content="q3", id="3"),
        AIMessage(content="a4", id="4", tool_calls=[_call("tc5")]),
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
        )
        assert event["anchor_message_id"] == "3"
        assert event["cutoff_index"] == 3
        # cutoff_index must point at the anchor message in the raw list
        assert raw[event["cutoff_index"]].id == event["anchor_message_id"]

    def test_empty_preserved_is_the_end_of_the_raw_list(self):
        """Nothing survived, so the boundary is exact and needs no arithmetic."""
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=[],
            summary_message=_summary(),
            file_path=None,
        )
        assert event["anchor_message_id"] is None
        assert event["cutoff_index"] == len(raw) == 6

    def test_empty_preserved_chained_does_not_resurrect_summarized_messages(self):
        """The chained arithmetic this replaces returned ``prev + cutoff - 1``,
        which assumed the effective tail was a 1:1 suffix of the raw list. Any
        filtering breaks that, and the drift reconstructs messages the previous
        pass had already summarized."""
        raw = _conversation()
        event = build_compaction_event(
            raw_messages=raw,
            preserved_messages=[],
            summary_message=_summary(),
            file_path=None,
        )
        assert event["cutoff_index"] == len(raw)
        # The whole point: reconstruction yields the summary and nothing else.
        assert get_effective_messages(raw, event) == [event["summary_message"]]


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
        )
        # Remove a pre-cutoff message: anchor "3" now sits at index 2, not 3.
        drifted = [m for m in raw if m.id != "1"]
        result = get_effective_messages(drifted, event)
        assert [m.id for m in result[1:]] == ["3", "4", "5"]


class TestFindGroupSafeCutoff:
    """The boundary is found by tool-call group, not by message type.

    Advancing only past ToolMessages was enough while every tool result was a
    ToolMessage. A visual Read used to answer with a HumanMessage carrying the
    image among the results, so the walk stopped on the carrier and left the
    rest of the batch orphaned behind a deleted parent. Those histories are in
    checkpoints, so the shape still has to be handled.
    """

    @staticmethod
    def _batch() -> list:
        """[H0, A1(tc2,tc3), T2, T3, A4, H5] — one assistant turn, two results."""
        return [
            HumanMessage(content="q0", id="0"),
            AIMessage(content="", id="1", tool_calls=[_call("tc2"), _call("tc3")]),
            ToolMessage(content="r2", id="2", tool_call_id="tc2"),
            ToolMessage(content="r3", id="3", tool_call_id="tc3"),
            AIMessage(content="a4", id="4"),
            HumanMessage(content="q5", id="5"),
        ]

    def test_a_cut_inside_a_batch_snaps_to_the_next_assistant_turn(self):
        messages = self._batch()
        assert find_group_safe_cutoff(messages, 3) == 4

    def test_a_clean_cut_is_left_where_it_is(self):
        messages = self._batch()
        assert find_group_safe_cutoff(messages, 4) == 4

    def test_a_run_that_ends_before_the_cut_is_not_a_split(self):
        """Nothing of that turn's is on the preserved side, so nothing orphans."""
        messages = self._batch()
        assert find_group_safe_cutoff(messages, 5) == 5

    def test_the_media_carrier_between_two_results_is_dropped_with_them(self):
        """The legacy shape: a HumanMessage sitting inside the tool-result run."""
        messages = [
            HumanMessage(content="q0", id="0"),
            AIMessage(content="", id="1", tool_calls=[_call("tc2"), _call("tc3")]),
            ToolMessage(content="r2", id="2", tool_call_id="tc2"),
            HumanMessage(content="[Viewing image]", id="media"),
            ToolMessage(content="r3", id="3", tool_call_id="tc3"),
            AIMessage(content="a5", id="5"),
        ]
        assert find_group_safe_cutoff(messages, 3) == 5

    def test_a_group_running_to_the_end_snaps_back_to_its_owner(self):
        """There is nothing forward to snap to, and preserving nothing would
        cost the turn the work it just did."""
        messages = [
            HumanMessage(content="q0", id="0"),
            AIMessage(content="", id="1", tool_calls=[_call("tc2")]),
            ToolMessage(content="r2", id="2", tool_call_id="tc2"),
        ]
        assert find_group_safe_cutoff(messages, 2) == 1

    def test_a_cutoff_outside_the_list_is_returned_untouched(self):
        messages = self._batch()
        assert find_group_safe_cutoff(messages, 0) == 0
        assert find_group_safe_cutoff(messages, len(messages)) == len(messages)


class TestStripOrphanToolMessages:
    def test_a_result_whose_parent_was_summarized_away_is_dropped(self):
        tail = [
            ToolMessage(content="r2", id="2", tool_call_id="tc2"),
            HumanMessage(content="q3", id="3"),
        ]
        assert strip_orphan_tool_messages(tail) == tail[1:]

    def test_an_answered_call_keeps_its_result_and_the_list_identity(self):
        """Returning the original list is what keeps this free on the read path,
        which runs on every model call."""
        tail = [
            AIMessage(content="", id="1", tool_calls=[_call("tc2")]),
            ToolMessage(content="r2", id="2", tool_call_id="tc2"),
        ]
        assert strip_orphan_tool_messages(tail) is tail

    def test_ownership_has_to_precede_the_result(self):
        """A later turn reusing an id must not vouch for an already stranded one."""
        tail = [
            ToolMessage(content="stranded", id="0", tool_call_id="tc9"),
            AIMessage(content="", id="1", tool_calls=[_call("tc9")]),
            ToolMessage(content="answered", id="2", tool_call_id="tc9"),
        ]
        assert [m.id for m in strip_orphan_tool_messages(tail)] == ["1", "2"]

    def test_a_result_answering_an_unparseable_call_is_kept(self):
        """PatchToolCallsMiddleware answers invalid_tool_calls with a ToolMessage;
        a predicate reading only tool_calls deletes the failure record."""
        tail = [
            AIMessage(
                content="",
                id="1",
                invalid_tool_calls=[
                    {
                        "name": "web_search",
                        "args": "{unparseable",
                        "id": "tc7",
                        "error": "invalid json",
                        "type": "invalid_tool_call",
                    }
                ],
            ),
            ToolMessage(content="r7", id="7", tool_call_id="tc7"),
        ]
        assert strip_orphan_tool_messages(tail) is tail

    def test_a_call_declared_only_in_native_blocks_still_owns_its_result(self):
        """A message whose parsed lists were never populated still declares its
        calls in the provider's own blocks."""
        responses = [
            AIMessage(
                content=[{"type": "function_call", "call_id": "tc8", "name": "Read"}],
                id="1",
            ),
            ToolMessage(content="r8", id="8", tool_call_id="tc8"),
        ]
        anthropic = [
            AIMessage(
                content=[{"type": "tool_use", "id": "tc9", "name": "Read", "input": {}}],
                id="1",
            ),
            ToolMessage(content="r9", id="9", tool_call_id="tc9"),
        ]
        assert strip_orphan_tool_messages(responses) is responses
        assert strip_orphan_tool_messages(anthropic) is anthropic

    def test_a_result_with_no_id_at_all_is_kept(self):
        """It cannot be matched either way, and dropping it loses content on a
        shape we do not recognise."""
        tail = [{"role": "tool", "content": "unidentified"}]
        assert strip_orphan_tool_messages(tail) is tail


class TestInheritedOrphansAreMatchedByIdentity:
    """An inherited orphan is an allowance for itself, not for one more orphan.

    ``find_group_safe_cutoff`` forgives orphans the incoming history already
    held, so a thread carrying one stays compactable. Deciding that on a count
    made the allowance fungible: with one old orphan in the list, a cutoff that
    stranded a healthy result also left exactly one orphan, passed the check,
    and ``partition_at_cutoff`` then deleted the healthy result. It landed in
    neither the summarized half nor the preserved tail.
    """

    @staticmethod
    def _history() -> list:
        return [
            ToolMessage(content="inherited orphan", tool_call_id="GONE"),
            AIMessage(content="", tool_calls=[_call("X")]),
            ToolMessage(content="healthy result", tool_call_id="X"),
        ]

    def test_no_cutoff_loses_the_healthy_result(self) -> None:
        history = self._history()
        for cutoff in range(1, len(history)):
            snapped = find_group_safe_cutoff(history, cutoff)
            head, tail = partition_at_cutoff(history, snapped)
            kept = {m.content for m in head} | {m.content for m in tail}
            assert "healthy result" in kept, (
                f"cutoff {cutoff} snapped to {snapped} and dropped the result "
                "of a call that is still in the history"
            )

    def test_the_group_is_never_split(self) -> None:
        history = self._history()
        # Index 2 is inside the group: the parent is behind it, the result on
        # it. The count test accepted this; identity moves back past the parent.
        assert find_group_safe_cutoff(history, 2) == 1

    def test_an_orphan_free_history_is_unaffected(self) -> None:
        history = [
            AIMessage(content="", tool_calls=[_call("A")]),
            ToolMessage(content="a", tool_call_id="A"),
        ]
        assert find_group_safe_cutoff(history, 1) == 0


class TestBothHalvesReadTheSameShapes:
    """Ownership is decided on dict-shaped messages too, or it deletes them.

    ``is_tool_message`` documents why it refuses to assume the reducer coerced
    everything: this rule backs the crash backstop, so a dict-shaped result
    slipping in still has to be caught. ``declared_tool_call_ids`` read only
    attributes, so a dict-shaped assistant turn declared nothing, its own
    answered result was counted an orphan, and the strip deleted it. A mismatch
    between the two halves always fails in the deleting direction.
    """

    HISTORY = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "X", "name": "f", "args": {}}],
        },
        {"role": "tool", "content": "healthy result", "tool_call_id": "X"},
    ]

    def test_a_dict_assistant_declares_its_calls(self) -> None:
        assert declared_tool_call_ids(self.HISTORY[0]) == {"X"}

    def test_its_answered_result_is_not_an_orphan(self) -> None:
        assert strip_orphan_tool_messages(list(self.HISTORY)) == self.HISTORY

    def test_a_genuinely_unowned_dict_result_is_still_dropped(self) -> None:
        # The backstop still has to work, or the fix would just disable it.
        unowned = [{"role": "tool", "content": "no parent", "tool_call_id": "GONE"}]
        assert strip_orphan_tool_messages(unowned) == []

    def test_provider_native_blocks_on_a_dict_message(self) -> None:
        history = [
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "Y", "name": "f", "input": {}}],
            },
            {"role": "tool", "content": "answer", "tool_call_id": "Y"},
        ]
        assert strip_orphan_tool_messages(history) == history


class TestTheCutoffSearchIsOnePass:
    """The search answers every candidate from one derivation of ownership.

    It used to re-derive the orphans of ``messages[start:]`` for each candidate,
    which is the same question asked once per position and quadratic in the
    length of the history. A cut strands a result exactly when the result
    survives it and its owner does not, so the earliest owner among the results
    from each position on decides every candidate, and one backward pass fills
    that in.
    """

    ALPHABET = (
        lambda: HumanMessage(content="u"),
        lambda: AIMessage(content="a"),
        lambda: AIMessage(content="", tool_calls=[_call("X")]),
        lambda: AIMessage(content="", tool_calls=[_call("Y")]),
        lambda: AIMessage(content="", tool_calls=[_call("X"), _call("Y")]),
        lambda: ToolMessage(content="rx", tool_call_id="X"),
        lambda: ToolMessage(content="ry", tool_call_id="Y"),
        lambda: ToolMessage(content="r?", tool_call_id=""),
    )

    @staticmethod
    def _rescan_orphans(tail: list) -> list[int]:
        """The reference: accumulate declarations, flag a result nothing asked for."""
        declared: set[str] = set()
        orphans: list[int] = []
        for i, msg in enumerate(tail):
            if isinstance(msg, ToolMessage):
                if msg.tool_call_id and msg.tool_call_id not in declared:
                    orphans.append(i)
            else:
                declared |= declared_tool_call_ids(msg)
        return orphans

    @classmethod
    def _rescan_cutoff(cls, messages: list, cutoff_index: int) -> int:
        """The reference search, re-deriving the tail's orphans per candidate."""
        if cutoff_index <= 0 or cutoff_index >= len(messages):
            return cutoff_index
        inherited = set(cls._rescan_orphans(messages))

        def adds_no_orphan(start: int) -> bool:
            return all(
                start + i in inherited for i in cls._rescan_orphans(messages[start:])
            )

        for i in range(cutoff_index, len(messages)):
            if adds_no_orphan(i):
                return i
        for i in range(cutoff_index - 1, -1, -1):
            if adds_no_orphan(i):
                return i
        return 0

    def test_it_agrees_with_the_rescan_on_every_short_history(self) -> None:
        for length in range(1, 5):
            for combo in itertools.product(self.ALPHABET, repeat=length):
                history = [make() for make in combo]
                for cutoff in range(-1, length + 2):
                    assert find_group_safe_cutoff(history, cutoff) == self._rescan_cutoff(
                        history, cutoff
                    ), f"history={[type(m).__name__ for m in history]} cutoff={cutoff}"

    def test_ownership_is_derived_once_per_search(self, monkeypatch) -> None:
        """The linearity, pinned where a timing assertion would only flake."""
        calls = 0
        original = utils._result_owners

        def counting(messages):
            nonlocal calls
            calls += 1
            return original(messages)

        monkeypatch.setattr(utils, "_result_owners", counting)
        history = [AIMessage(content="", tool_calls=[_call("X")])]
        history += [ToolMessage(content=f"r{i}", tool_call_id="X") for i in range(40)]
        assert utils.find_group_safe_cutoff(history, 20) == 0
        assert calls == 1



class TestStripBase64PreservesSiblingKeys:
    """Redacting a data URI must not strip the block's other keys."""

    def test_text_block_keys_survive_the_redaction(self) -> None:
        block = {
            "type": "text",
            "text": f"see data:image/png;base64,{'A' * 120}",
            "phase": "final_answer",
            "index": 0,
        }
        stripped = utils.strip_base64_from_content([block])
        assert stripped == [
            {
                "type": "text",
                "text": "see [base64 data removed]",
                "phase": "final_answer",
                "index": 0,
            }
        ]

    def test_clean_text_block_is_returned_untouched(self) -> None:
        content = [{"type": "text", "text": "no data uri", "phase": "commentary"}]
        assert utils.strip_base64_from_content(content) is content


def test_stale_reads_tolerate_args_read_never_accepted():
    # Tier 1 runs at turn start: one malformed Read call in the history must
    # not fail every turn after an idle gap.
    messages = [
        AIMessage("", id="a1", tool_calls=[
            {"name": "Read", "id": "r1", "args": {"file_path": None, "offset": [1]}},
            {"name": "Read", "id": "r2", "args": {"file_path": "notes.md"}},
        ]),
        ToolMessage("ERROR: bad args", tool_call_id="r1", id="t1"),
        ToolMessage("v1", tool_call_id="r2", id="t2"),
        AIMessage("", id="a2", tool_calls=[
            {"name": "Read", "id": "r3", "args": {"file_path": "notes.md"}},
        ]),
        ToolMessage("v2", tool_call_id="r3", id="t3"),
    ]

    assert utils.stale_read_ids(messages, len(messages)) == {"r2"}
