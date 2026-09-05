"""The capability groups a brokerage connection can carry, and the tools in each.

A brokerage publishes one flat tool list, and connecting one hands the agent all
of it. These groups are the unit of consent instead: the user picks which ones a
connection carries, the choice is stored on the connection, and the egress relay
refuses a call to anything outside it.

The assignment is hand-made and has to stay that way, because a prefix rule gets
it wrong in both directions. moomoo's ``quote_modify_user_security`` writes the
watchlist despite reading like a quote, and IBKR's ``provide_customer_feedback``
sends a message to the broker despite reading like nothing at all.

The policy regulates what is curated and does not block what is not: a tool in
no capability group is permitted, so a vendor publishing one between our
releases reaches the agent rather than being refused until a deploy catches up.
That is a deliberate trade. It is also why ``UNCURATED`` is a list rather than
an omission -- the second tool above is one we read and chose to keep away, and
that decision has to be written down somewhere the denial can see it, or "we
never classified this" and "we classified this and said no" become the same
state and the second one wins the wrong way.

The rungs below ``trading`` are three different things, not one, which is why
they are three groups. moomoo's ``sim_trade_*`` is **paper trading**: a parallel
simulated account with its own cash, positions and order history, unable to
touch real money. Robinhood's ``review_*_order`` is an **order preview**: a
computation against the real account that returns pre-trade alerts and persists
nothing, which is why it has no list or delete counterpart. IBKR's
``*_order_instruction`` is a **staged order**: a full create/list/delete triple
that writes an object into the real account, one human click from being live --
IBKR's own description draws the line for us, saying an instruction "is not a
live order".

Folding them into one group would cost the display its vocabulary. The badge on
the row and the switch in the consent dialog read the same key, so a single
group would have to be labelled something true of all three and precise about
none of them.

Asset class is not one of these axes. A vendor's crypto order tools file under
the same rungs as its equity ones, because a group answers what an action
costs rather than what it trades, and ``trading`` has always carried equities,
options and option exercise together. Granting crypto separately would need a
second axis that neither the ladder nor the badges have.

Keys are facts and the words for them belong to the client, the same contract
``brokerages.py`` keeps.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.server.services.brokerages import brokerage_by_name, brokerage_for_url
from src.server.services.egress import fold_tool_name


def vendor_for_url(server_url: str | None) -> str | None:
    """The brokerage whose rules a connection is bound by, from its address.

    Every function below takes a *vendor*, and this is the only sanctioned way
    to get one. Enforcement used to key on the catalog row's name, which is the
    user's to choose and to edit: a row named ``my_ibkr`` at IBKR's host got the
    IBKR consent dialog and then no policy at all, and a row named ``robinhood``
    repointed elsewhere got a denial computed against the wrong vendor's tool
    names. Both looked healthy and both passed live-order tools. The address is
    what the token was issued for and what the relay dials, so it is the only
    identity a policy may be derived from.
    """
    vendor = brokerage_for_url(server_url)
    return vendor.name if vendor else None


@dataclass(frozen=True)
class CapabilityGroup:
    """One consent toggle.

    ``tone`` tells the client how loudly to draw the row without deciding the
    words: ``neutral`` is public or personal data, ``caution`` is the user's own
    positions and money, ``danger`` places real orders.

    ``rung`` marks a group as one of the steps between reading and placing.
    Which of them a broker has is the first thing someone wants off a row --
    paper, preview, staged, live -- and it is a fact about the group rather than
    a reading of its key, so it travels rather than being inferred client-side.
    """

    key: str
    order: int
    tone: str
    rung: bool = False


GROUPS: tuple[CapabilityGroup, ...] = (
    CapabilityGroup(key="market_data", order=10, tone="neutral"),
    CapabilityGroup(key="watchlists", order=20, tone="neutral"),
    CapabilityGroup(key="scanners", order=30, tone="neutral"),
    CapabilityGroup(key="alerts", order=40, tone="neutral"),
    CapabilityGroup(key="account", order=50, tone="caution"),
    CapabilityGroup(key="paper_trading", order=60, tone="neutral", rung=True),
    CapabilityGroup(key="order_preview", order=70, tone="neutral", rung=True),
    CapabilityGroup(key="staged_orders", order=80, tone="caution", rung=True),
    CapabilityGroup(key="trading", order=90, tone="danger", rung=True),
)

_BY_KEY: dict[str, CapabilityGroup] = {g.key: g for g in GROUPS}

# Tool names exactly as the vendor publishes them, which is what the relay
# compares against. Counted against live discovery: moomoo 88, Robinhood 67,
# IBKR 34, Webull 71, one of IBKR's deliberately left out.
_CURATION: dict[str, dict[str, tuple[str, ...]]] = {
    "moomoo": {
        "market_data": (
            "quote_capital_distribution",
            "quote_capital_flow",
            "quote_capital_flow_history",
            "quote_community_search",
            "quote_company_executive_background",
            "quote_company_executives",
            "quote_company_operational_efficiency",
            "quote_company_profile",
            "quote_corporate_actions_buybacks",
            "quote_corporate_actions_dividends",
            "quote_corporate_actions_rehab",
            "quote_corporate_actions_stock_splits",
            "quote_cur_kline",
            "quote_daily_short_volume",
            "quote_economic_calendar_hot",
            "quote_economic_calendar_search",
            "quote_financials_earnings_price_history",
            "quote_financials_earnings_price_move",
            "quote_financials_revenue_breakdown",
            "quote_financials_statements",
            "quote_future_info",
            "quote_history_kline",
            "quote_insider_holder_list",
            "quote_insider_trade_list",
            "quote_ipo_list_cn",
            "quote_ipo_list_hk",
            "quote_ipo_list_my",
            "quote_ipo_list_sg",
            "quote_ipo_list_us",
            "quote_market_snapshot",
            "quote_market_state",
            "quote_news_search",
            "quote_option_chain",
            "quote_option_exercise_probability",
            "quote_option_expiration_date",
            "quote_option_screen",
            "quote_option_volatility",
            "quote_order_book",
            "quote_owner_plate",
            "quote_plate_list",
            "quote_plate_stock",
            "quote_referencefuture_list",
            "quote_research_analyst_consensus",
            "quote_research_morningstar_report",
            "quote_research_rating_summary",
            "quote_rt_data",
            "quote_rt_ticker",
            "quote_shareholders_holder_detail",
            "quote_shareholders_holding_changes",
            "quote_shareholders_institutional",
            "quote_shareholders_overview",
            "quote_short_interest",
            "quote_stock_basicinfo",
            "quote_stock_feed",
            "quote_stock_quote",
            "quote_stock_screen",
            "quote_top_ten_brokers",
            "quote_top_ten_brokers_history",
            "quote_trading_days",
            "quote_valuation_detail",
            "quote_valuation_index_component_stock_list",
            "quote_valuation_index_stock_plate_list",
            "quote_valuation_plate_stock_list",
            "quote_warrant_screen",
        ),
        "watchlists": (
            "quote_modify_user_security",
            "quote_user_security",
            "quote_user_security_group",
        ),
        "account": (
            "account_authorized_trd_accs",
            "account_fills_history",
            "account_funds",
            "account_order_fills_today",
            "account_orders_active",
            "account_orders_detail",
            "account_orders_history",
            "account_positions",
            "account_trading_info",
        ),
        "paper_trading": (
            "sim_trade_account_list",
            "sim_trade_cancel_order",
            "sim_trade_cash_info",
            "sim_trade_history_order_list",
            "sim_trade_input_order",
            "sim_trade_max_buy_sell",
            "sim_trade_modify_order",
            "sim_trade_position_list",
        ),
        "trading": (
            "trading_order_cancel",
            "trading_order_confirm",
            "trading_order_place",
            "trading_order_replace",
        ),
    },
    "robinhood": {
        "market_data": (
            "get_crypto_quotes",
            "get_currency_pairs",
            "get_earnings_calendar",
            "get_earnings_results",
            "get_equity_fundamentals",
            "get_equity_historicals",
            "get_equity_news",
            "get_equity_price_book",
            "get_equity_quotes",
            "get_equity_technical_indicators",
            "get_equity_tradability",
            "get_financials",
            "get_index_historicals",
            "get_index_quotes",
            "get_indexes",
            "get_option_chains",
            "get_option_historicals",
            "get_option_instruments",
            "get_option_quotes",
            "get_sec_filing",
            "get_sec_filing_facts",
            "get_sec_filing_facts_catalog",
            "get_sec_filing_index",
            "search",
        ),
        "watchlists": (
            "add_option_to_watchlist",
            "add_to_watchlist",
            "create_watchlist",
            "follow_watchlist",
            "get_option_watchlist",
            "get_popular_watchlists",
            "get_watchlist_items",
            "get_watchlists",
            "remove_from_watchlist",
            "remove_option_from_watchlist",
            "unfollow_watchlist",
            "update_watchlist",
        ),
        "scanners": (
            "create_scan",
            "get_scanner_filter_specs",
            "get_scans",
            "run_scan",
            "update_scan_config",
            "update_scan_filters",
        ),
        "account": (
            "get_accounts",
            "get_crypto_account_onboarding_info",
            "get_crypto_orders",
            "get_crypto_positions",
            "get_equity_orders",
            "get_equity_positions",
            "get_equity_tax_lots",
            "get_limited_margin_upgrade_info",
            "get_option_level_upgrade_info",
            "get_option_orders",
            "get_option_positions",
            "get_pnl_trade_history",
            "get_portfolio",
            "get_realized_pnl",
        ),
        "order_preview": (
            "preview_crypto_order",
            "review_equity_order",
            "review_option_order",
        ),
        "trading": (
            "cancel_crypto_order",
            "cancel_equity_order",
            "cancel_option_exercise",
            "cancel_option_order",
            "exercise_option",
            "place_crypto_order",
            "place_equity_order",
            "place_option_order",
        ),
    },
    "ibkr": {
        "market_data": (
            "get_combo_identifier",
            "get_company_connections",
            "get_company_themes",
            "get_option_data",
            "get_option_parameters",
            "get_price_history",
            "get_price_snapshot",
            "get_theme_details",
            "search_contracts",
            "search_futures",
            "search_investment_topics",
            "whats_new",
        ),
        "watchlists": (
            "create_watchlist",
            "delete_watchlist",
            "edit_watchlist",
            "get_watchlist",
            "get_watchlists",
        ),
        "alerts": (
            "create_alert",
            "delete_alert",
            "get_alert",
            "get_alerts",
            "set_alert_status",
            "update_alert",
        ),
        "account": (
            "get_account_balances",
            "get_account_orders",
            "get_account_positions",
            "get_account_summary",
            "get_account_trades",
            "get_pa_allocation",
            "get_pa_performance_all_periods",
        ),
        "staged_orders": (
            "create_order_instruction",
            "delete_order_instruction",
            "get_order_instructions",
        ),
    },
    # No rung of any kind here, and the line is the vendor's rather than ours.
    # Webull's consent screen offers exactly four capabilities -- account infos,
    # order query, market data, security infos -- and no trading checkbox, so the
    # write scope is not grantable at all and nothing published places, previews
    # or stages an order. The order tools below read.
    "webull": {
        "market_data": (
            "get_52_week_high_low",
            "get_analyst_rating",
            "get_analyst_target_price",
            "get_balance_sheet",
            "get_cash_flow",
            "get_company_profile",
            "get_crypto_bars",
            "get_crypto_instruments",
            "get_crypto_snapshot",
            "get_event_bars",
            "get_event_categories",
            "get_event_depth",
            "get_event_events",
            "get_event_instruments",
            "get_event_series",
            "get_event_snapshot",
            "get_event_tick",
            # Risk indicators published about a security, not an alert the user
            # owns, which is why this is data rather than the ``alerts`` group.
            "get_financial_alert",
            "get_financial_indicators",
            "get_fund_allocation",
            "get_fund_brief",
            "get_fund_dividends",
            "get_fund_files",
            "get_fund_holdings",
            "get_fund_net_value",
            "get_fund_performance",
            "get_fund_rating",
            "get_fund_splits",
            "get_futures_bars",
            "get_futures_depth",
            "get_futures_footprint",
            "get_futures_instruments",
            "get_futures_product_class",
            "get_futures_products",
            "get_futures_snapshot",
            "get_futures_tick",
            "get_gainers_losers",
            "get_high_dividend",
            "get_income_statement",
            "get_instruments",
            "get_market_sectors",
            "get_market_sectors_detail",
            "get_most_active",
            "get_stock_bars",
            "get_stock_bars_single",
            "get_stock_capital_flow",
            "get_stock_dividend_calendar",
            "get_stock_earnings_calendar",
            "get_stock_filings",
            "get_stock_footprint",
            "get_stock_forecast_eps",
            "get_stock_industry_comparison",
            "get_stock_noii_bars",
            "get_stock_noii_snapshot",
            "get_stock_quotes",
            "get_stock_snapshot",
            "get_stock_tick",
        ),
        "watchlists": (
            "add_watchlist_instruments",
            "create_watchlist",
            "delete_watchlist",
            "get_watchlist_instruments",
            "get_watchlists",
            "remove_watchlist_instruments",
            "update_watchlist",
            "update_watchlist_instruments",
        ),
        "account": (
            "get_account_balance",
            "get_account_list",
            "get_account_positions",
            "get_open_orders",
            "get_order_detail",
            "get_order_history",
        ),
    },
}

# Named so a reader can see it was a decision, not an omission: this submits a
# feature request to IBKR in the user's name, which is not a thing an analysis
# turn should be able to do on its own. ``denied_tools`` refuses these whatever
# the user granted, which is the only place the policy is stricter than the
# groups -- everything else it blocks, a capability toggle can unblock.
UNCURATED: dict[str, tuple[str, ...]] = {
    "ibkr": ("provide_customer_feedback",),
}


def groups_for(brokerage: str | None) -> tuple[CapabilityGroup, ...]:
    """The consent toggles to offer for a brokerage, in display order.

    Only groups the vendor actually has tools for: IBKR publishes nothing that
    places an order, so it is never asked about trading.
    """
    curated = _CURATION.get(brokerage) if brokerage else None
    if not curated:
        return ()
    return tuple(sorted((_BY_KEY[k] for k in curated), key=lambda g: g.order))


def group_keys_for(brokerage: str | None) -> tuple[str, ...]:
    """Every group key a brokerage offers, in display order."""
    return tuple(g.key for g in groups_for(brokerage))


def tools_for(brokerage: str | None, granted: Iterable[str]) -> frozenset[str] | None:
    """The tools a grant of these groups permits, or None if we curate no policy.

    The affirmative view of a grant, for display and for reasoning about one.
    **Not the gate.** Enforcement reads ``denied_tools``, and the difference is
    the whole policy: this answers "which curated tools did they say yes to",
    which says nothing about a tool that is in no group at all.

    Reaching for this at a gate would turn every uncurated tool into a refusal,
    including the ones a vendor publishes between our releases. That is the
    behaviour the denial is here to avoid, so the two must not be swapped.

    None is not "allow nothing" and not "allow everything": it means this server
    is not one we have a map for, and the caller decides.
    """
    if brokerage is None:
        return None
    curated = _CURATION.get(brokerage)
    if curated is None:
        return frozenset() if brokerage_by_name(brokerage) else None
    wanted = set(granted)
    return frozenset(
        tool for key, tools in curated.items() if key in wanted for tool in tools
    )


def denied_tools(brokerage: str | None, granted: Iterable[str]) -> frozenset[str] | None:
    """The curated tools this grant refuses. What enforcement actually reads.

    The complement of ``tools_for``, and the two are not interchangeable at a
    gate: this regulates what we know and never blocks what we do not. A tool
    in no group is in no denial, so a tool the vendor publishes after we curated
    them is reachable rather than refused. That is the deliberate line -- every
    other harness a user could connect these brokers to grants the whole tool
    list, and a connector that silently stops working when the vendor ships an
    update is a worse failure in practice than one that carries a tool we have
    not classified yet.

    The consequence to know when reading a bug report: a mistake here fails
    permissive. An allowlist that came out empty served nothing and was obvious;
    a denial that comes out empty serves everything and looks healthy. That is
    why ``policy_required`` still rides along -- it can no longer prove the
    policy is right, only that it was computed at all.

    ``UNCURATED`` is denied unconditionally, and that is the line between the
    two kinds of absence. A tool we never looked at is unknown, and passes. A
    tool we read, understood, and deliberately put in no group is *known* and
    refused, whatever groups the user granted. Collapsing them would turn the
    one list we wrote down to keep a tool away into the mechanism that lets it
    through, which is the opposite of what it says on it.

    None only for a server we hold no map for, matching ``tools_for``. A shipped
    brokerage we have not curated denies nothing, because every one of its tools
    is one we have not classified.
    """
    if brokerage is None:
        return None
    curated = _CURATION.get(brokerage)
    if curated is None:
        return frozenset() if brokerage_by_name(brokerage) else None
    wanted = set(granted)
    return frozenset(
        tool for key, tools in curated.items() if key not in wanted for tool in tools
    ) | frozenset(UNCURATED.get(brokerage, ()))


def group_of_tool(brokerage: str | None, tool: str) -> str | None:
    """The group a tool belongs to, or None if no group names it.

    None does not say whether the tool is reachable, and a display must not read
    it that way: ask :func:`is_always_denied` for that. The two ungrouped cases
    are opposites -- an ``UNCURATED`` tool is refused at every grant, one we
    never classified passes at every grant -- and the old docstring here claimed
    they looked the same to a reader, which was the bug: the detail view labelled
    both "the agent never sees them" while half of them were callable.

    ``tool`` comes from discovery, so it is matched folded, the way the relay
    and the registry filter match it. Otherwise a vendor that recases or pads a
    name has it refused at the relay and stripped from the composite while this
    endpoint still reports it as unclassified, and the detail view tells the
    user a declined tool is callable.
    """
    return _BY_TOOL.get(brokerage or "", {}).get(fold_tool_name(tool))


def is_always_denied(brokerage: str | None, tool: str) -> bool:
    """Whether a tool is refused whatever the user granted.

    True only for ``UNCURATED``: a tool we read and deliberately put in no
    group. A tool we simply have not classified is not one of these, and saying
    so is the difference between describing the policy and inverting it. Folded
    for the reason :func:`group_of_tool` gives.
    """
    return fold_tool_name(tool) in _UNCURATED_FOLDED.get(brokerage or "", frozenset())


# Built once rather than scanned per tool: the tools endpoint annotates a whole
# vendor list in one pass, and moomoo's is 88 long. Keyed folded because the
# names looked up here are the vendor's, not ours.
_BY_TOOL: dict[str, dict[str, str]] = {
    brokerage: {
        fold_tool_name(tool): key
        for key, tools in curated.items()
        for tool in tools
    }
    for brokerage, curated in _CURATION.items()
}

_UNCURATED_FOLDED: dict[str, frozenset[str]] = {
    brokerage: frozenset(fold_tool_name(t) for t in tools)
    for brokerage, tools in UNCURATED.items()
}
