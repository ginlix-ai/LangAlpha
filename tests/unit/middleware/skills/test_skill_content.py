"""Tests for the skill content layer (``ptc_agent`` skills middleware).

Covers two pure functions that the SkillsMiddleware composes:

- ``build_skill_content`` — the already-loaded dedup that keeps a re-sent skill's
  SKILL.md body from being re-injected every turn while still refreshing its
  (per-turn) instruction.
- ``compute_already_loaded`` — which skills' bodies are still live in the
  effective (post-compaction) message window, so the body can be skipped.

Import + patch targets use the bare ``ptc_agent.X`` namespace — the package's
one module identity (a ``src.``-prefixed import would create a distinct module
object whose patches never reach production code; ruff TID251 bans it).
"""

from unittest.mock import MagicMock, patch

import pytest

from ptc_agent.agent.middleware.skills.content import (
    SkillRequest,
    build_skill_content,
    compute_already_loaded,
)

MOD = "ptc_agent.agent.middleware.skills.content"


def _ctx(name: str, instruction: str | None = None) -> SkillRequest:
    return SkillRequest(name=name, instruction=instruction)


@pytest.fixture(autouse=True)
def _skill_exposed_in_mode():
    """The already-loaded branch calls ``get_skill(name, mode)`` to confirm the
    skill is still exposed in the current mode. Default it truthy so the dedup
    tests exercise the skip path; the mode-mismatch test overrides it to None.
    """
    with patch(f"{MOD}.get_skill", return_value=MagicMock()):
        yield


# ---------------------------------------------------------------------------
# build_skill_content
# ---------------------------------------------------------------------------


def test_fresh_skill_injects_full_body_and_instruction():
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content([_ctx("chart-annotation", "draw on AAPL:1day")])

    assert result is not None
    assert result.loaded_skill_names == ["chart-annotation"]
    assert '<loaded-skill name="chart-annotation">' in result.content
    assert "SKILL BODY" in result.content
    assert "[Instruction: draw on AAPL:1day]" in result.content


def test_fresh_skill_appends_tool_descriptions_block():
    """When a skill has tools, the body block carries an **Available tools:**
    section plus the 'call directly' note — the text the model uses to invoke
    the skill's tools without a separate LoadSkill round-trip."""
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(
            f"{MOD}.build_tool_descriptions",
            return_value="- draw_chart_annotation: draw on the chart",
        ),
    ):
        result = build_skill_content([_ctx("chart-annotation")])

    assert result is not None
    assert "**Available tools:**" in result.content
    assert "- draw_chart_annotation: draw on the chart" in result.content
    assert "without needing to call LoadSkill" in result.content


def test_fresh_skill_marker_carries_message_id():
    """The emitted marker binds to the target message's id so the dedup scanner
    can verify it later (defeats content-forged 'already loaded' matches)."""
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content([_ctx("chart-annotation")], message_id="msg-7")

    assert result is not None
    assert '<loaded-skill name="chart-annotation" mid="msg-7">' in result.content


def test_same_skill_twice_in_one_request_emits_body_once():
    """Intra-request dedup: a duplicate name (e.g. duplicate additional_context
    entries) must not paste the body or count the skill as loaded twice."""
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY") as load,
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [_ctx("chart-annotation", "AAPL:1d"), _ctx("chart-annotation", "TSLA:4h")],
            message_id="m1",
        )

    assert result is not None
    assert result.loaded_skill_names == ["chart-annotation"]
    assert result.content.count('<loaded-skill name="chart-annotation"') == 1
    load.assert_called_once()  # the second occurrence is skipped before disk I/O


def test_same_already_loaded_skill_twice_refreshes_instruction_once():
    """A duplicate name on the already-loaded path must not double the instruction."""
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [_ctx("chart-annotation", "AAPL:1d"), _ctx("chart-annotation", "TSLA:4h")],
            already_loaded={"chart-annotation"},
        )

    assert result is not None
    # First occurrence wins; the duplicate is dropped before it can re-emit.
    assert result.content.count("AAPL:1d") == 1
    assert "TSLA:4h" not in result.content


def test_already_loaded_skill_skips_body_keeps_instruction():
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY") as load,
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [_ctx("chart-annotation", "draw on NVDA:1hour")],
            already_loaded={"chart-annotation"},
        )

    # Body is not re-pasted and the skill is not re-counted as freshly loaded...
    assert result is not None
    assert result.loaded_skill_names == []
    assert "<loaded-skill" not in result.content
    assert "SKILL BODY" not in result.content
    load.assert_not_called()
    # ...but the fresh instruction (current symbol/timeframe) still rides.
    assert "[Instruction: draw on NVDA:1hour]" in result.content


def test_already_loaded_skill_without_instruction_returns_none():
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [_ctx("chart-annotation")],
            already_loaded={"chart-annotation"},
        )

    # Nothing to inject: body skipped, no instruction to refresh.
    assert result is None


def test_mixed_fresh_and_already_loaded():
    def fake_load(name, skill_dirs=None, mode=None):
        return f"BODY:{name}"

    with (
        patch(f"{MOD}.load_skill_content", side_effect=fake_load),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [
                _ctx("chart-annotation", "draw on AAPL:1day"),
                _ctx("research", "find news"),
            ],
            already_loaded={"chart-annotation"},
        )

    assert result is not None
    # Only the not-yet-loaded skill gets a body and a fresh-name entry.
    assert result.loaded_skill_names == ["research"]
    assert "BODY:research" in result.content
    assert "BODY:chart-annotation" not in result.content
    assert '<loaded-skill name="research">' in result.content
    # Both instructions are listed (multi-instruction format).
    assert "[Instructions]" in result.content
    assert "- chart-annotation: draw on AAPL:1day" in result.content
    assert "- research: find news" in result.content


def test_single_instruction_across_multiple_skills_uses_named_format():
    """A fresh skill body plus a *different* already-loaded skill's instruction
    means two skills are represented, so the lone instruction must be named —
    the bare ``[Instruction: ...]`` form would read as belonging to the fresh
    skill's body block instead of its real owner.
    """
    with (
        patch(f"{MOD}.load_skill_content", return_value="SKILL BODY"),
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [
                _ctx("research"),  # fresh, no instruction -> body block only
                _ctx("chart-annotation", "draw on NVDA:1day"),  # already loaded -> instruction only
            ],
            already_loaded={"chart-annotation"},
        )

    assert result is not None
    assert result.loaded_skill_names == ["research"]
    assert '<loaded-skill name="research">' in result.content
    # Two skills represented -> named list, never the bare shorthand.
    assert "[Instructions]" in result.content
    assert "- chart-annotation: draw on NVDA:1day" in result.content
    assert "[Instruction: draw on NVDA:1day]" not in result.content


def test_already_loaded_but_mode_mismatch_drops_instruction():
    """A stale loaded_skills entry for a skill no longer exposed in the current
    mode must NOT short-circuit to an instruction-only inject — the mode re-check
    fails, the skill falls through to the fresh path, and load_skill_content
    (which also mode-gates) rejects it. Net: nothing is injected, so a caller
    can't smuggle an instruction past the mode gate via a stale loaded set.
    """
    with (
        patch(f"{MOD}.get_skill", return_value=None),  # not exposed in this mode
        patch(f"{MOD}.load_skill_content", return_value=None),  # fresh path also rejects
        patch(f"{MOD}.build_tool_descriptions", return_value=None),
    ):
        result = build_skill_content(
            [_ctx("chart-annotation", "draw on NVDA:1day")],
            already_loaded={"chart-annotation"},
            mode="flash",
        )

    assert result is None


# ---------------------------------------------------------------------------
# compute_already_loaded
# ---------------------------------------------------------------------------


def test_compute_filters_non_str_names():
    """Only string skill names survive."""
    result = compute_already_loaded(
        ["chart-annotation", None, 7, "research"], [], None
    )
    assert result == {"chart-annotation", "research"}


def test_compute_empty_loaded_returns_empty():
    assert compute_already_loaded([], [{"content": "hi"}], None) == set()
    assert compute_already_loaded(None, None, None) == set()


def test_compute_no_compaction_trusts_loaded_set():
    """Without a compaction event the full history is in the model's view, so
    every loaded skill's body still survives — no marker scan needed."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [{"content": "hi"}],  # marker not present, but no compaction
        None,
    )
    assert result == {"chart-annotation"}


def test_compute_event_without_cutoff_trusts_loaded_set():
    """A summarization event with no/zero cutoff_index means nothing was actually
    summarized away, so trust the loaded set as-is."""
    assert compute_already_loaded(
        ["chart-annotation"], [{"content": "hi"}], {"cutoff_index": 0}
    ) == {"chart-annotation"}


def test_compute_compaction_drops_body_before_cutoff():
    """After compaction, a skill whose injected body now sits *before* the
    cutoff is dropped so the body gets re-injected."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [
            {
                "content": '<loaded-skill name="chart-annotation" mid="m0">body</loaded-skill>',
                "id": "m0",
            },
            {"content": "assistant reply", "id": "m1"},
            {"content": "later user turn", "id": "m2"},  # messages[2:] — no marker
        ],
        {"cutoff_index": 2, "summary_message": {"content": "compacted summary"}},
    )
    assert result == set()


def test_compute_compaction_keeps_body_after_cutoff():
    """A skill whose body marker still appears in the surviving tail
    (messages[cutoff:]) — with ``mid`` matching that message's own id —
    stays in the dedup set; no need to re-inject."""
    result = compute_already_loaded(
        ["chart-annotation", "research"],
        [
            {"content": "pre-cutoff summarized turn", "id": "m0"},
            {
                "content": '<loaded-skill name="chart-annotation" mid="m1">body</loaded-skill>',
                "id": "m1",
            },
            {"content": "another turn", "id": "m2"},
        ],
        {"cutoff_index": 1, "summary_message": {"content": "compacted summary"}},
    )
    # Only chart-annotation's marker survives the cutoff; research's body is gone.
    assert result == {"chart-annotation"}


def test_compute_compaction_ignores_marker_inside_summary():
    """A body that survives only inside the (lossy) summary message is gone
    verbatim — get_effective_messages prepends the summary at index 0 and we
    scan from index 1, so such a skill is dropped and re-injected."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [
            {
                "content": '<loaded-skill name="chart-annotation" mid="m0">body</loaded-skill>',
                "id": "m0",
            },
            {"content": "assistant reply", "id": "m1"},
            {"content": "later user turn", "id": "m2"},  # surviving tail — no marker
        ],
        {
            "cutoff_index": 2,
            # Marker present in the summary, but the summary is lossy prose.
            "summary_message": {
                "content": 'mentions <loaded-skill name="chart-annotation"> once'
            },
        },
    )
    assert result == set()


def test_compute_compaction_scans_multimodal_list_content():
    """Attachments produce list-content user turns, and the body is appended as a
    ``{"type": "text"}`` part — so the marker scan must read list content, not
    just plain strings, or a multimodal turn would falsely re-inject every time."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [
            {"content": "pre-cutoff summarized turn", "id": "m0"},
            {
                "id": "m1",
                "content": [
                    {"type": "image_url", "image_url": {"url": "data:..."}},
                    {
                        "type": "text",
                        "text": '<loaded-skill name="chart-annotation" mid="m1">body</loaded-skill>',
                    },
                ],
            },
        ],
        {"cutoff_index": 1, "summary_message": {"content": "compacted summary"}},
    )
    assert result == {"chart-annotation"}


def test_compute_compaction_ignores_foreign_mid():
    """Identity binding: a marker whose ``mid`` is some *other* message's id (e.g.
    a user copied a real marker from earlier in the thread into a new turn) does
    NOT count — the mid must equal the scanned message's own id."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [
            {"content": "pre-cutoff summarized turn", "id": "m0"},
            {
                # mid points at a different message than the one it lives in.
                "content": '<loaded-skill name="chart-annotation" mid="m0">body</loaded-skill>',
                "id": "m_self",
            },
        ],
        {"cutoff_index": 1, "summary_message": {"content": "compacted summary"}},
    )
    assert result == set()


def test_compute_compaction_ignores_documented_block_in_other_skill_body():
    """Finding #2: a SKILL.md body that *documents* the format with a full
    ``<loaded-skill name="research" ...>...</loaded-skill>`` example must not make
    ``research`` look loaded — the documented mid won't equal the host message id."""
    result = compute_already_loaded(
        ["research"],
        [
            {"content": "pre-cutoff summarized turn", "id": "m0"},
            {
                # chart-annotation's own body, which happens to document the format.
                "content": (
                    '<loaded-skill name="chart-annotation" mid="a1">\n'
                    'To load a skill the server emits '
                    '<loaded-skill name="research" mid="example">...</loaded-skill>\n'
                    "</loaded-skill>"
                ),
                "id": "a1",
            },
        ],
        {"cutoff_index": 1, "summary_message": {"content": "compacted summary"}},
    )
    assert result == set()


def test_compute_compaction_ignores_bare_legacy_marker():
    """A bare ``<loaded-skill name="X">`` with no ``mid`` (legacy injection or a
    user typing the old format) no longer matches — the scanner requires the
    mid-bound form, so such a turn re-injects rather than falsely deduping."""
    result = compute_already_loaded(
        ["chart-annotation"],
        [
            {"content": "pre-cutoff summarized turn", "id": "m0"},
            {
                "content": '<loaded-skill name="chart-annotation">body</loaded-skill>',
                "id": "m1",
            },
        ],
        {"cutoff_index": 1, "summary_message": {"content": "compacted summary"}},
    )
    assert result == set()
