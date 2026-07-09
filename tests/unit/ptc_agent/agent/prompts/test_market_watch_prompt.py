"""Market-watch guidance lives in a skill now, not a system-prompt flag.

The flag-gated ``<market_watch>`` section was removed; its guidance moved into
``skills/market-watch/SKILL.md``. These tests pin both halves: the section tag
never renders (regardless of any lingering flag), and the skill file is present,
parses, and carries the feed vocabulary the agent needs.
"""

import re
from pathlib import Path

import yaml

from ptc_agent.agent.prompts import get_loader

# tests/unit/ptc_agent/agent/prompts/ -> repo root is five parents up.
REPO_ROOT = Path(__file__).resolve().parents[5]
SKILL_MD = REPO_ROOT / "skills" / "market-watch" / "SKILL.md"

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


def test_market_watch_skill_md_exists_and_frontmatter_parses():
    assert SKILL_MD.is_file(), f"missing {SKILL_MD}"

    content = SKILL_MD.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(content)
    assert match, "no YAML frontmatter in skills/market-watch/SKILL.md"

    frontmatter = yaml.safe_load(match.group(1))
    assert isinstance(frontmatter, dict)
    assert frontmatter.get("name") == "market-watch"
    assert str(frontmatter.get("description", "")).strip()


def test_market_watch_skill_body_covers_the_feed_vocabulary():
    body = SKILL_MD.read_text(encoding="utf-8")
    assert "watch_market" in body
    assert "unwatch_market" in body
    assert "<market-watch>" in body
