"""A bot's allow/deny lists decide what its main agent can see."""

from __future__ import annotations

from types import SimpleNamespace

from src.tools.bots.scope import narrow_tools

TOOLS = [SimpleNamespace(name=n) for n in ("Bash", "ExecuteCode", "send_bot_message", "get_quote")]


def _names(tools) -> list[str]:
    return [t.name for t in tools]


def test_no_lists_keep_the_whole_surface():
    assert _names(narrow_tools(TOOLS, {})) == _names(TOOLS)


def test_an_allow_list_narrows_to_it():
    bot = {"tools_allow": ["Bash", "get_quote"]}
    assert _names(narrow_tools(TOOLS, bot)) == ["Bash", "get_quote"]


def test_deny_wins_and_removes_from_allow():
    bot = {"tools_allow": ["Bash", "ExecuteCode"], "tools_deny": ["ExecuteCode"]}
    assert _names(narrow_tools(TOOLS, bot)) == ["Bash"]


def test_a_denied_tool_disappears_without_an_allow_list():
    bot = {"tools_deny": ["send_bot_message"]}
    assert "send_bot_message" not in _names(narrow_tools(TOOLS, bot))


def test_allow_names_the_build_never_bound_are_ignored():
    bot = {"tools_allow": ["a_tool_from_a_plugin_not_installed"]}
    assert narrow_tools(TOOLS, bot) == []
