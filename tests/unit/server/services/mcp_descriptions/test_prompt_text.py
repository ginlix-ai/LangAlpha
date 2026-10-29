"""What the agent reads about a server: the user's words, then the generated
description, then the shipped text, which on a brokerage row is nobody's edit."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions import prompt_text
from src.server.services.mcp_descriptions.prompt_text import effective_description

WEBULL = brokerage_by_name("webull")
PROFILE = {"description": "Generated description."}


def _row(url, description=""):
    return {"url": url, "description": description}


def test_a_brokerage_row_holding_the_shipped_text_reads_the_profile():
    assert effective_description(_row(WEBULL.url, WEBULL.description), PROFILE) == (
        PROFILE["description"]
    )


def test_with_no_profile_a_brokerage_reads_the_current_shipped_text():
    assert effective_description(_row(WEBULL.url, ""), None) == WEBULL.description


def test_another_brokerages_shipped_text_on_a_repointed_row_is_nobodys_edit():
    row = _row("https://mcp.example.com/mcp", brokerage_by_name("robinhood").description)
    assert effective_description(row, PROFILE) == PROFILE["description"]


def test_the_users_own_words_win_over_everything():
    row = _row(WEBULL.url, "My Webull, use for options.")
    assert effective_description(row, PROFILE) == "My Webull, use for options."


def test_a_users_server_falls_back_to_the_profile_and_then_to_nothing():
    url = "https://mcp.example.com/mcp"
    assert effective_description(_row(url), PROFILE) == PROFILE["description"]
    assert effective_description(_row(url), None) == ""
    assert effective_description(_row(url, "Mine."), PROFILE) == "Mine."


@pytest.mark.asyncio
async def test_a_row_that_does_not_parse_is_skipped_alone(monkeypatch):
    monkeypatch.setattr(
        prompt_text,
        "get_mcp_descriptions_config",
        lambda: SimpleNamespace(enabled=True, user_servers=True),
    )
    read = AsyncMock(return_value={})
    monkeypatch.setattr(prompt_text.db, "current_descriptions", read)
    rows = [
        {"name": "bad", "url": "https://bad.example.com/mcp", "transport": "carrier-pigeon"},
        {"name": "good", "url": "https://good.example.com/mcp", "transport": "http"},
    ]

    await prompt_text.profiles_by_server("user-1", rows, {})

    assert len(read.await_args.args[0]) == 1
