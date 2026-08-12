"""Tests for the server's skill request-parsing helpers.

Body loading + inline injection moved into ``ptc_agent`` (SkillsMiddleware); this
module now only resolves *which* skills a request activated:
``parse_skill_contexts`` (from ``additional_context``) and ``detect_slash_commands``
(leading ``/command`` fallback).
"""

from unittest.mock import patch

from src.server.models.additional_context import SkillContext
from src.server.utils.skill_context import (
    detect_slash_commands,
    parse_skill_contexts,
)

MOD = "src.server.utils.skill_context"


# ---------------------------------------------------------------------------
# parse_skill_contexts
# ---------------------------------------------------------------------------


def test_parse_none_and_empty_returns_empty():
    assert parse_skill_contexts(None) == []
    assert parse_skill_contexts([]) == []


def test_parse_dict_skill_item():
    result = parse_skill_contexts(
        [{"type": "skills", "name": "chart-annotation", "instruction": "AAPL:1d"}]
    )
    assert len(result) == 1
    assert result[0].name == "chart-annotation"
    assert result[0].instruction == "AAPL:1d"


def test_parse_passes_through_skillcontext_instances():
    ctx = SkillContext(type="skills", name="research", instruction="news")
    assert parse_skill_contexts([ctx]) == [ctx]


def test_parse_filters_non_skill_items():
    result = parse_skill_contexts(
        [
            {"type": "directive", "content": "be terse"},
            {"type": "skills", "name": "research"},
        ]
    )
    assert [s.name for s in result] == ["research"]


# ---------------------------------------------------------------------------
# detect_slash_commands
# ---------------------------------------------------------------------------


def test_detect_non_slash_text_is_unchanged():
    text, detected = detect_slash_commands("hello world")
    assert text == "hello world"
    assert detected == []


def test_detect_matched_command_strips_prefix():
    with patch(f"{MOD}.get_command_to_skill_map", return_value={"research": "research"}):
        text, detected = detect_slash_commands("/research market analysis")
    assert text == "market analysis"
    assert [s.name for s in detected] == ["research"]


def test_detect_command_only_keeps_original_text():
    """A bare ``/command`` with no body keeps the original text so the agent at
    least knows what was asked."""
    with patch(f"{MOD}.get_command_to_skill_map", return_value={"research": "research"}):
        text, detected = detect_slash_commands("/research")
    assert text == "/research"
    assert [s.name for s in detected] == ["research"]


def test_detect_unregistered_command_is_unchanged():
    with patch(f"{MOD}.get_command_to_skill_map", return_value={"research": "research"}):
        text, detected = detect_slash_commands("/unknown do thing")
    assert text == "/unknown do thing"
    assert detected == []
