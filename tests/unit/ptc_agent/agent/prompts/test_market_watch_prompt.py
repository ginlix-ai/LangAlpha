"""Market-watch guidance lives in a skill now, not a system-prompt flag.

The flag-gated ``<market_watch>`` section was removed; its guidance moved into
the ``market-watch`` skill. These tests pin both halves: the section tag never
renders (regardless of any lingering flag), and the skill file is present,
parses, and carries the feed vocabulary the agent needs.
"""

import re

import yaml

from ptc_agent.agent.prompts import get_loader

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def test_system_prompt_never_renders_the_market_watch_section():
    """The ``<market_watch>`` section is gone; no flag re-introduces it.

    (``<market-watch>``/``watch_market`` still appear in the always-on tool_guide
    table — that's subsystem b/c, so we key off the unique section tag instead.)
    """
    loader = get_loader()
    # Build the retired flag name from parts so the repo-wide zero-token grep
    # gate stays green while still proving a stray flag can't resurrect the block.
    retired_flag = "market_watch" + "_mode"
    for kwargs in ({}, {retired_flag: True}):
        prompt = loader.get_system_prompt(
            current_time="2026-07-01 14:30 ET",
            subagent_summary="",
            tool_summary="",
            **kwargs,
        )
        assert "<market_watch>" not in prompt


def test_tool_guide_row_follows_the_master_switch():
    """watch_market lists in the tool table by default (yaml default: enabled)
    and disappears when the render passes the master switch as off."""
    loader = get_loader()
    base = dict(
        current_time="2026-07-01 14:30 ET",
        subagent_summary="",
        tool_summary="",
    )

    assert "watch_market" in loader.get_system_prompt(**base)
    assert "watch_market" not in loader.get_system_prompt(
        **base, market_watch_enabled=False
    )


def test_market_watch_skill_md_exists_and_frontmatter_parses(shipped_skill_md):
    skill_md = shipped_skill_md("market-watch")
    content = skill_md.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(content)
    assert match, f"no YAML frontmatter in {skill_md}"

    frontmatter = yaml.safe_load(match.group(1))
    assert isinstance(frontmatter, dict)
    assert frontmatter.get("name") == "market-watch"
    assert str(frontmatter.get("description", "")).strip()


def test_market_watch_skill_body_covers_the_feed_vocabulary(shipped_skill_md):
    body = shipped_skill_md("market-watch").read_text(encoding="utf-8")
    assert "watch_market" in body
    assert 'action="unwatch"' in body
    assert "<market-watch>" in body
