"""The tile each skill row carries in ``GET /api/v1/skills``.

A shipped skill's mark comes from its bundle's manifest; an installed one's
from the manifest stored with its package, matched by the directory the
package carried it in, because the row's name is the SKILL.md's and may
differ. An installed package gets glyphs only: a site would have the
unauthenticated icon route fetch a host a user's manifest named, which the
package's own mark is not allowed to do either.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.server.app import skills as skills_app
from src.server.database import plugins as plugins_db
from src.server.models.plugin import SkillMark

PLUGIN_ID = "22222222-2222-2222-2222-222222222222"


def _stored(skills: object) -> dict:
    return {
        "user_plugin_id": PLUGIN_ID,
        "manifest": {"name": "pack", "extensions": {"ai.langalpha": {"skills": skills}}},
    }


def _row(name: str, *, skill_dir: str | None = None, plugin_id: str | None = PLUGIN_ID) -> dict:
    return {"name": name, "plugin_id": plugin_id, "plugin_skill_dir": skill_dir}


async def _marks_for(monkeypatch, stored: list[dict], rows: list[dict]):
    listed = AsyncMock(return_value=stored)
    monkeypatch.setattr(plugins_db, "list_plugins", listed)
    marks = await skills_app._installed_marks("u-1", rows)
    return {r["name"]: skills_app._installed_mark(r, marks) for r in rows}, listed


class TestInstalledMarks:
    @pytest.mark.asyncio
    async def test_a_glyph_is_drawn_and_a_site_is_not(self, monkeypatch):
        by_row, _ = await _marks_for(
            monkeypatch,
            [_stored({"dcf": {"icon": "calculator"}, "feed": {"icon": "x.com"}})],
            [_row("dcf", skill_dir="dcf"), _row("feed", skill_dir="feed")],
        )
        assert by_row == {"dcf": SkillMark(icon_glyph="calculator"), "feed": None}

    @pytest.mark.asyncio
    async def test_matched_by_the_directory_not_the_row_name(self, monkeypatch):
        by_row, _ = await _marks_for(
            monkeypatch,
            [_stored({"dcf": {"icon": "calculator"}})],
            [_row("valuation", skill_dir="dcf"), _row("dcf", skill_dir="other")],
        )
        assert by_row == {
            "valuation": SkillMark(icon_glyph="calculator"),
            "dcf": None,
        }

    @pytest.mark.asyncio
    async def test_a_namespace_that_will_not_parse_draws_nothing(self, monkeypatch):
        by_row, _ = await _marks_for(
            monkeypatch,
            [_stored({"dcf": {"icon": "calculator", "colour": "red"}})],
            [_row("dcf", skill_dir="dcf")],
        )
        assert by_row == {"dcf": None}

    @pytest.mark.asyncio
    async def test_a_listing_with_no_package_rows_reads_no_packages(self, monkeypatch):
        by_row, listed = await _marks_for(
            monkeypatch, [], [_row("mine", plugin_id=None)]
        )
        assert by_row == {"mine": None}
        listed.assert_not_awaited()


class TestTheRowWearsItsMark:
    def test_a_shipped_skill_takes_its_bundles_mark(self):
        state = skills_app._BundleState(
            owners={"dcf": "pack"},
            disabled=(),
            marks={"dcf": SkillMark(icon_url="/api/v1/plugins/pack/skills/dcf/icon")},
        )
        entry = {"name": "dcf", "description": "d", "tool_count": 0, "tools": [], "command": None}
        info = skills_app._platform_info(entry, bundles=state)
        assert (info.icon_url, info.icon_glyph) == (
            "/api/v1/plugins/pack/skills/dcf/icon", None,
        )

    def test_a_skill_with_no_mark_answers_both_fields_empty(self):
        state = skills_app._BundleState(owners={}, disabled=(), marks={})
        entry = {"name": "dcf", "description": "d", "tool_count": 0, "tools": [], "command": None}
        dumped = skills_app._platform_info(entry, bundles=state).model_dump()
        assert (dumped["icon_url"], dumped["icon_glyph"]) == (None, None)
