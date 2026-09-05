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


def test_detect_extra_command_triggers():
    with patch(f"{MOD}.get_command_to_skill_map", return_value={"research": "research"}):
        text, detected = detect_slash_commands(
            "/pb do thing", extra_commands={"pb": "probe"}
        )
    assert text == "do thing"
    assert [s.name for s in detected] == ["probe"]


def test_detect_extra_command_replaces_builtin_trigger():
    """A rename in ``extra_commands`` REPLACES the builtin trigger for the
    same skill: the old command must stop answering."""
    builtin = {"market-watch": "market-watch", "research": "research"}
    extra = {"mw": "market-watch"}
    with patch(f"{MOD}.get_command_to_skill_map", return_value=builtin):
        old_text, old_detected = detect_slash_commands(
            "/market-watch AAPL", extra_commands=extra
        )
        new_text, new_detected = detect_slash_commands(
            "/mw AAPL", extra_commands=extra
        )
        other_text, other_detected = detect_slash_commands(
            "/research news", extra_commands=extra
        )
    assert old_detected == [] and old_text == "/market-watch AAPL"
    assert [s.name for s in new_detected] == ["market-watch"] and new_text == "AAPL"
    assert [s.name for s in other_detected] == ["research"]


def test_detect_gates_on_the_builds_own_skill_set():
    """A command the build will refuse must not match at all.

    Detection strips the prefix and records a skill context; if the effective
    registry then rejects the skill, nothing loads and the model silently
    receives the shortened message. The gate covers both an alias and a
    default trigger, since a disabled skill has both.
    """
    builtin = {"market-watch": "market-watch", "research": "research"}
    extra = {"mw": "market-watch"}
    allowed = {"research"}  # market-watch disabled for this user
    with patch(f"{MOD}.get_command_to_skill_map", return_value=builtin):
        alias_text, alias_detected = detect_slash_commands(
            "/mw AAPL", extra_commands=extra, allowed_skills=allowed
        )
        plain_text, plain_detected = detect_slash_commands(
            "/market-watch AAPL", allowed_skills=allowed
        )
        live_text, live_detected = detect_slash_commands(
            "/research news", extra_commands=extra, allowed_skills=allowed
        )
    assert alias_detected == [] and alias_text == "/mw AAPL"
    assert plain_detected == [] and plain_text == "/market-watch AAPL"
    assert [s.name for s in live_detected] == ["research"] and live_text == "news"


def test_gating_a_renamed_skill_does_not_revive_its_builtin_trigger():
    """The filter runs after the merge. Applied before it, dropping the alias
    would leave the builtin trigger the rename was supposed to replace."""
    builtin = {"market-watch": "market-watch"}
    with patch(f"{MOD}.get_command_to_skill_map", return_value=builtin):
        text, detected = detect_slash_commands(
            "/market-watch AAPL",
            extra_commands={"mw": "market-watch"},
            allowed_skills=set(),
        )
    assert detected == [] and text == "/market-watch AAPL"


def test_no_allowed_set_means_no_gating():
    """The CLI and tests have no user, so None keeps today's behavior."""
    with patch(f"{MOD}.get_command_to_skill_map", return_value={"research": "research"}):
        text, detected = detect_slash_commands("/research news", allowed_skills=None)
    assert [s.name for s in detected] == ["research"] and text == "news"
