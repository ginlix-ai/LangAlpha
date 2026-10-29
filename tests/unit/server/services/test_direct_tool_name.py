"""The name a direct MCP tool is bound under, as the chat UI parses it back.

``direct_tool_name`` is the encode half of a pair whose decode half is
``parseDirectToolName`` in ``web/src/pages/ChatAgent/utils/directTools.ts``,
which splits on the first ``__`` after the prefix and hands the rest to the
tool detail panel and the approval card. ``_parse`` below mirrors it, so a
change to the Python half that the frontend could not read fails here.
"""

from __future__ import annotations

import re

from src.server.services.egress.direct_tools import (
    _MAX_TOOL_NAME,
    direct_tool_name,
)

PREFIX = "mcp__"


def _parse(name: str) -> tuple[str, str]:
    assert name.startswith(PREFIX)
    rest = name[len(PREFIX) :]
    sep = rest.index("__")
    return rest[:sep], rest[sep + 2 :]


def test_name_is_the_prefix_the_frontend_splits_on():
    name = direct_tool_name("moomoo", "sim_trade_account_list")
    assert name == "mcp__moomoo__sim_trade_account_list"
    assert _parse(name) == ("moomoo", "sim_trade_account_list")


def test_a_long_server_is_truncated_and_the_tool_survives_verbatim():
    server = "a" * 80
    name = direct_tool_name(server, "trading_order_place")
    assert len(name) <= _MAX_TOOL_NAME
    parsed_server, parsed_tool = _parse(name)
    assert parsed_tool == "trading_order_place"
    assert server.startswith(parsed_server.rsplit("_", 1)[0])


def test_two_long_server_names_do_not_collide():
    assert direct_tool_name("a" * 80, "read_order") != direct_tool_name(
        "a" * 79 + "b", "read_order"
    )


def test_two_tools_on_one_long_server_do_not_collide():
    server = "a" * 80
    read = direct_tool_name(server, "read_order")
    cancel = direct_tool_name(server, "cancel_order")
    assert read != cancel
    assert _parse(read)[1] == "read_order"
    assert _parse(cancel)[1] == "cancel_order"


def test_a_tool_name_the_provider_would_reject_is_made_legal():
    # Discovery accepts any tool name up to 128 chars; the model-facing
    # function name has to survive a provider that enforces [A-Za-z0-9_-].
    name = direct_tool_name("moomoo", "foo.bar")
    assert re.fullmatch(r"[A-Za-z0-9_-]+", name)


def test_two_tools_differing_only_where_the_charset_folds_do_not_collide():
    assert direct_tool_name("moomoo", "foo.bar") != direct_tool_name(
        "moomoo", "foo_bar"
    )


def test_a_separator_bearing_server_cannot_alias_another_pair():
    # ("a__b", "c") and ("a", "b__c") both name mcp__a__b__c unless the
    # ambiguous one is disambiguated: the tools node keeps whichever came
    # last, and the policy middleware then gates against the wrong connection.
    assert direct_tool_name("a__b", "c") != direct_tool_name("a", "b__c")


def test_two_long_tools_sharing_a_prefix_do_not_collide():
    stem = "get_account_positions_with_realtime_market_quotes_and_pnl_v2_"
    alpha = direct_tool_name("moomoo", stem + "alpha")
    beta = direct_tool_name("moomoo", stem + "beta")
    assert len(alpha) <= _MAX_TOOL_NAME and len(beta) <= _MAX_TOOL_NAME
    assert alpha != beta
