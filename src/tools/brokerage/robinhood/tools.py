"""
LangChain tool wrappers for the Robinhood trading MCP server.

All tools retrieve the user's Robinhood Bearer token from user_oauth_tokens at
call time and forward the request to the remote MCP server via client.call_tool.

review_equity_order is NOT exposed as a tool — it is called internally by
place_equity_order before submitting the order.
"""
from __future__ import annotations

import uuid
from typing import Any

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.server.services.robinhood_oauth import get_valid_token

from .client import RobinhoodMCPError, call_tool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _get_token(config: RunnableConfig) -> str:
    """Fetch a valid Robinhood access token for the calling user."""
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        raise ValueError("user_id missing from RunnableConfig")
    result = await get_valid_token(user_id)
    if not result:
        raise RobinhoodMCPError(
            "Robinhood account not connected. "
            "Please connect your Robinhood account first."
        )
    return result["access_token"]


def _args(**kwargs: Any) -> dict[str, Any]:
    """Build an MCP arguments dict, dropping keys whose value is None."""
    return {k: v for k, v in kwargs.items() if v is not None}


# ---------------------------------------------------------------------------
# Discovery / quotes
# ---------------------------------------------------------------------------


@tool
async def search(
    query: str,
    config: RunnableConfig,
    asset_type: str | None = None,
    limit: int | None = None,
) -> Any:
    """
    Resolve a natural-language query to Robinhood instruments (stocks/ETFs), crypto
    pairs, or market indexes. Use when the user names an asset by name (or partial
    name) instead of a ticker/pair/index symbol, or when you need an instrument_id /
    currency-pair UUID / market-index id for a downstream tool. Defaults to instrument
    search; pass asset_type="currency_pair" for crypto or asset_type="market_index"
    for indexes (SPX, NDX, DJI, etc.). Instrument results carry symbol + instrument_id
    (use with get_equity_quotes / get_equity_tradability / place_equity_order or any
    instrument_id-based tool). Crypto results carry hyphenated symbol (e.g. BTC-USD) +
    id — the symbol routes to crypto quote/order tools, the id routes to watchlist
    tools as currency_pair_ids. Market-index results carry symbol + id — pass id to
    index quote tools for current values, or in the index_ids array of watchlist tools.

    Args:
        query: Natural-language search query: company name, partial name, or ticker
            (e.g. "apple", "tesla motors", "AAPL").
        asset_type: Asset category to search. Supported: "instrument" (US-listed
            stocks/ETFs), "currency_pair" (crypto pairs like BTC-USD), and
            "market_index" (e.g. SPX, NDX, DJI). Defaults to "instrument" when
            omitted.
        limit: Max results to return. Defaults to 10; clamped to 20.
    """
    token = await _get_token(config)
    return await call_tool(token, "search", _args(query=query, asset_type=asset_type, limit=limit))


@tool
async def get_accounts(config: RunnableConfig) -> Any:
    """
    List the user's brokerage accounts. Use this to look up account_number values
    needed by other tools. If the user has multiple accounts and hasn't specified
    which one, present the list and ask them to choose. Does NOT return reliable
    buying power — route buying-power questions through get_portfolio.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_accounts", {})


@tool
async def get_portfolio(account_number: str, config: RunnableConfig) -> Any:
    """
    Get the account's portfolio market value breakdown by asset type and buying
    power. Use for "how much is my account worth?", "what's my portfolio
    breakdown?", "how much do I have in options?", and "how much can I spend /
    afford?" questions.

    Args:
        account_number: Brokerage account number. Obtain from get_accounts.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_portfolio", {"account_number": account_number})


@tool
async def get_equity_quotes(
    symbols: list[str],
    config: RunnableConfig,
) -> Any:
    """
    Get real-time stock quotes and the official last-completed-session close for
    one or more symbols.

    Args:
        symbols: One or more stock symbols. Above 20 symbols, quotes still return
            but closes is omitted with closes_error set.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_equity_quotes", {"symbols": symbols})


@tool
async def get_equity_positions(
    account_number: str,
    config: RunnableConfig,
    cursor: str | None = None,
) -> Any:
    """
    List open equity positions for a specific brokerage account. Returns symbol,
    quantity, average cost, and per-position hold breakdowns.

    Args:
        account_number: Brokerage account number. Must come from the user or be
            clearly implied — never default from get_accounts.
        cursor: Pagination cursor. Omit for the first page; for the next page,
            pass the cursor query param from the prior response's next URL.
    """
    token = await _get_token(config)
    return await call_tool(
        token, "get_equity_positions", _args(account_number=account_number, cursor=cursor)
    )


@tool
async def get_equity_orders(
    account_number: str,
    config: RunnableConfig,
    order_id: str | None = None,
    state: str | None = None,
    symbol: str | None = None,
    created_at_gte: str | None = None,
    placed_agent: str | None = None,
    cursor: str | None = None,
) -> Any:
    """
    Fetch equity orders for an account — list mode (newest first; open and closed,
    including fills, cancellations, rejections) or single-order mode by passing
    order_id. When the user asks for "orders" without specifying equity or options,
    call both get_equity_orders and get_option_orders in parallel.

    Filtering tips:
    - Prefer narrow queries: combine state, symbol, and/or created_at_gte for
      specific questions — the per-page cap is fixed.
    - created_at_gte: interpret relative times in the user's timezone, convert to
      UTC before sending.
    - symbol forces a symbol→instrument lookup; omit it if you don't need it.

    Args:
        account_number: Brokerage account number. Must come from the user or be
            clearly implied — never default from get_accounts.
        order_id: Filter to a single order by UUID. The response shape is unchanged
            (orders[] with at most one entry); empty when the order does not belong
            to account_number.
        state: Filter by single state: new, queued, confirmed, unconfirmed,
            partially_filled, filled, cancelled, rejected, failed, voided.
        symbol: Filter to one symbol (triggers a symbol→instrument lookup before
            the orders call).
        created_at_gte: Lower bound (inclusive). ISO 8601 UTC or YYYY-MM-DD; naive
            values are interpreted as UTC.
        placed_agent: Filter to one source: 'user', 'agentic' (MCP), 'recurring',
            'drip', etc.
        cursor: Pagination cursor. Omit for the first page; for the next page,
            pass the cursor query param from the prior response's next URL.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "get_equity_orders",
        _args(
            account_number=account_number,
            order_id=order_id,
            state=state,
            symbol=symbol,
            created_at_gte=created_at_gte,
            placed_agent=placed_agent,
            cursor=cursor,
        ),
    )


@tool
async def get_equity_tradability(
    account_number: str,
    symbols: list[str],
    config: RunnableConfig,
) -> Any:
    """
    Check tradability for up to 10 equity symbols on a given account: per-session
    eligibility and fractional. Call before placing an order to surface
    restrictions. Exact-ticker match — no name or partial-ticker resolution.

    Args:
        account_number: Brokerage account number. Must come from the user or be
            clearly implied — never default from get_accounts.
        symbols: Stock symbols (max 10 per call). Exact-ticker match only.
    """
    token = await _get_token(config)
    return await call_tool(
        token, "get_equity_tradability", {"account_number": account_number, "symbols": symbols}
    )


# ---------------------------------------------------------------------------
# Watchlists
# ---------------------------------------------------------------------------


@tool
async def get_watchlists(config: RunnableConfig) -> Any:
    """
    List the user's watchlists, including both user-created custom lists and
    Robinhood-curated lists the user follows. Use to look up list_id values for
    other watchlist tools.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_watchlists", {})


@tool
async def get_watchlist_items(list_id: str, config: RunnableConfig) -> Any:
    """
    List the items in a watchlist. Items may be stocks/ETFs, crypto pairs, futures,
    indexes — distinguished by object_type. For the options watchlist, use
    get_options_watchlist instead — this tool returns a generic shape that drops
    the strategy-specific fields and the upstream rejects it with 400 anyway. Does
    not return live prices; call get_equity_quotes with the symbol(s) for that.

    Args:
        list_id: UUID of the watchlist whose items to fetch. Obtain from
            get_watchlists or get_popular_lists.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_watchlist_items", {"list_id": list_id})


@tool
async def create_watchlist(
    display_name: str,
    config: RunnableConfig,
    display_description: str | None = None,
    icon_emoji: str | None = None,
) -> Any:
    """
    Create a new custom watchlist for the user. Confirm the name with the user
    before calling — this is a real write. Do not use this to follow a
    Robinhood-curated list (use follow_list).

    Args:
        display_name: Name for the new watchlist (e.g. 'Tech Stocks'). Must be
            unique among the user's watchlists.
        display_description: Short description shown under the name.
        icon_emoji: Emoji shown next to the name (one character).
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "create_watchlist",
        _args(
            display_name=display_name,
            display_description=display_description,
            icon_emoji=icon_emoji,
        ),
    )


@tool
async def update_watchlist(
    list_id: str,
    config: RunnableConfig,
    display_name: str | None = None,
    display_description: str | None = None,
    icon_emoji: str | None = None,
) -> Any:
    """
    Rename a custom watchlist or change its icon/description. Robinhood-curated
    lists cannot be renamed; the call will fail with 404. Provide at least one of
    display_name, icon_emoji, display_description.

    Args:
        list_id: UUID of the watchlist to update. Obtain from get_watchlists.
        display_name: New name for the watchlist.
        display_description: New description.
        icon_emoji: New emoji.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "update_watchlist",
        _args(
            list_id=list_id,
            display_name=display_name,
            display_description=display_description,
            icon_emoji=icon_emoji,
        ),
    )


@tool
async def add_to_watchlist(
    list_id: str,
    config: RunnableConfig,
    symbols: list[str] | None = None,
    currency_pair_ids: list[str] | None = None,
    index_ids: list[str] | None = None,
) -> Any:
    """
    Add items to a watchlist. Exactly one of symbols (stocks/ETFs),
    currency_pair_ids (crypto), or index_ids (market indexes like SPX, NDX) is
    required — mutually exclusive. For options use add_option_to_watchlist
    (separate dedicated watchlist). Futures still require the Robinhood app.
    Already-present items are no-ops. Confirm with the user before calling.

    Args:
        list_id: UUID of the watchlist to add items to.
        symbols: Stock symbols to add (e.g. ['AAPL', 'NVDA']). US stocks and ETFs
            only. Mutually exclusive with currency_pair_ids and index_ids.
        currency_pair_ids: Currency-pair UUIDs to add (e.g. the object_id from
            get_watchlist_items where object_type=currency_pair, or the id from
            list_currency_pairs). Mutually exclusive with symbols and index_ids.
        index_ids: Market-index UUIDs to add (the id field from get_indexes; SPX,
            NDX, DJI, etc.). Mutually exclusive with symbols and currency_pair_ids.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "add_to_watchlist",
        _args(
            list_id=list_id,
            symbols=symbols,
            currency_pair_ids=currency_pair_ids,
            index_ids=index_ids,
        ),
    )


@tool
async def remove_from_watchlist(
    list_id: str,
    config: RunnableConfig,
    symbols: list[str] | None = None,
    currency_pair_ids: list[str] | None = None,
    index_ids: list[str] | None = None,
) -> Any:
    """
    Remove items from a watchlist. Exactly one of symbols (stocks/ETFs),
    currency_pair_ids (crypto), or index_ids (market indexes) is required —
    mutually exclusive. For options use remove_option_from_watchlist. Items not on
    the list are no-ops (not errors). Confirm with the user before calling.

    Args:
        list_id: UUID of the watchlist to remove items from.
        symbols: Stock symbols to remove (e.g. ['AAPL']). Mutually exclusive with
            currency_pair_ids and index_ids.
        currency_pair_ids: Currency-pair UUIDs to remove. Mutually exclusive with
            symbols and index_ids.
        index_ids: Index UUIDs to remove. Mutually exclusive with symbols and
            currency_pair_ids.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "remove_from_watchlist",
        _args(
            list_id=list_id,
            symbols=symbols,
            currency_pair_ids=currency_pair_ids,
            index_ids=index_ids,
        ),
    )


@tool
async def get_popular_lists(config: RunnableConfig) -> Any:
    """
    Discover Robinhood-curated lists the user can follow (e.g. '100 Most Popular',
    'Daily Movers'). Use to find a list_id, then pass it to follow_list.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_popular_lists", {})


@tool
async def follow_list(list_id: str, config: RunnableConfig) -> Any:
    """
    Follow a Robinhood-curated list so it appears in the user's watchlists.
    Confirm with the user before calling. Use only for curated lists; the user
    already owns their custom lists.

    Args:
        list_id: UUID of the Robinhood-curated list to follow. Obtain from
            get_popular_lists.
    """
    token = await _get_token(config)
    return await call_tool(token, "follow_list", {"list_id": list_id})


@tool
async def unfollow_list(list_id: str, config: RunnableConfig) -> Any:
    """
    Stop following a Robinhood-curated list. The list itself is unchanged — it
    just no longer appears in the user's watchlists. Confirm with the user before
    calling.

    Args:
        list_id: UUID of the Robinhood-curated list to unfollow.
    """
    token = await _get_token(config)
    return await call_tool(token, "unfollow_list", {"list_id": list_id})


# ---------------------------------------------------------------------------
# Options watchlist
# ---------------------------------------------------------------------------


@tool
async def get_options_watchlist(config: RunnableConfig) -> Any:
    """
    List the single-leg option contracts on the user's options watchlist. Use this
    instead of get_watchlist_items for the options watchlist — get_watchlist_items
    returns a generic shape that drops the option-specific title and the upstream
    rejects it with 400 anyway. Works for both equity options (AAPL, NVDA) and
    index options (SPX, NDX, RUT). Multi-leg strategies (verticals, condors, etc.)
    that may exist in the user's watchlist from app-side order placement are not
    shown — direct the user to the Robinhood app to view those.
    """
    token = await _get_token(config)
    return await call_tool(token, "get_options_watchlist", {})


@tool
async def add_option_to_watchlist(
    option_ids: list[str],
    config: RunnableConfig,
    position_type: str | None = None,
) -> Any:
    """
    Add option contracts to the user's options watchlist. Works for both equity
    options (AAPL, NVDA) and index options (SPX, NDX, RUT). Source option_ids from
    get_option_instruments. Confirm with the user before calling — this is a real
    write.

    Args:
        option_ids: Option contract UUIDs to add. Each becomes a single-leg
            position on the user's options watchlist. Source from
            get_option_instruments.
        position_type: "long" (default) or "short". Applies to every option_id in
            this call. For mixed long/short adds, issue two calls.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "add_option_to_watchlist",
        _args(option_ids=option_ids, position_type=position_type),
    )


@tool
async def remove_option_from_watchlist(
    option_ids: list[str],
    config: RunnableConfig,
    position_type: str | None = None,
) -> Any:
    """
    Remove option contracts from the user's options watchlist. Specify the same
    position_type used when the contract was added (defaults to "long"). Contracts
    not on the list are no-ops. Confirm with the user before calling.

    Args:
        option_ids: Option contract UUIDs to remove. The position_type must match
            how each contract was added (most likely "long").
        position_type: "long" (default) or "short". Must match how the contract was
            originally added.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "remove_option_from_watchlist",
        _args(option_ids=option_ids, position_type=position_type),
    )


# ---------------------------------------------------------------------------
# Order simulation and placement
# ---------------------------------------------------------------------------


@tool
async def review_equity_order(
    account_number: str,
    symbol: str,
    side: str,
    order_type: str,
    config: RunnableConfig,
    quantity: str | None = None,
    dollar_amount: str | None = None,
    limit_price: str | None = None,
    stop_price: str | None = None,
    time_in_force: str = "gfd",
    market_hours: str = "regular_hours",
) -> Any:
    """
    Simulate a stock order without placing it. Returns the current quote plus
    pre-trade alerts (buying power, PDT, instrument halt, etc.). Call this by
    default before place_equity_order unless the user has very explicitly asked
    to skip review. Requires an agentic_allowed=true account; non-agentic accounts
    are rejected — do not call.

    Parameter rules:
    - If the user has not specified order_type, ask. For immediate fills with price
      protection, prefer a marketable limit at the current ask over a plain market.
    - Provide exactly one of quantity or dollar_amount; dollar_amount requires
      order_type=market (server computes shares from last_trade_price).
    - Fractional shares: only on order_type=market with market_hours=regular_hours,
      eligible accounts, up to 6 decimal places, no short sells.
    - limit_price required for limit/stop_limit; stop_price required for
      stop_market/stop_limit.
    - Fractional and dollar-based orders only place in regular_hours; the tool
      rejects them in other sessions.

    Args:
        account_number: Brokerage account number. Must come from the user or be
            clearly implied. Must be agentic_allowed=true.
        symbol: Stock symbol.
        side: 'buy' or 'sell'.
        order_type: 'market', 'limit', 'stop_market', or 'stop_limit'.
        quantity: Number of shares. Decimals (fractional) allowed for market +
            regular_hours only.
        dollar_amount: USD notional (e.g. '100.00'). Only valid with
            order_type=market.
        limit_price: Limit price; required for limit or stop_limit.
        stop_price: Stop trigger price; required for stop_market or stop_limit.
        time_in_force: 'gfd' (good for day) or 'gtc' (good till cancelled).
            Default: gfd.
        market_hours: 'regular_hours' (default), 'extended_hours', or
            'all_day_hours'.
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "review_equity_order",
        _args(
            account_number=account_number,
            symbol=symbol,
            side=side,
            type=order_type,
            quantity=quantity,
            dollar_amount=dollar_amount,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            market_hours=market_hours,
        ),
    )


@tool
async def place_equity_order(
    account_number: str,
    symbol: str,
    side: str,
    order_type: str,
    config: RunnableConfig,
    quantity: str | None = None,
    dollar_amount: str | None = None,
    limit_price: str | None = None,
    stop_price: str | None = None,
    time_in_force: str = "gfd",
    market_hours: str = "regular_hours",
    ref_id: str | None = None,
) -> Any:
    """
    Place a real equity order with real money. Parameters mirror review_equity_order
    plus the optional ref_id. Requires an agentic_allowed=true account; non-agentic
    accounts are rejected — do not call.

    Workflow: by default call review_equity_order first, present the estimated cost
    and any alerts, and get explicit user confirmation before calling this tool. Skip
    review only when the user has very explicitly asked to bypass it (e.g. "skip the
    review", "just place it, don't review") — a generic "place this order" is NOT a
    bypass.

    Idempotency: pass a fresh UUID as ref_id on the first call for each logical
    order, and re-send the SAME ref_id on retries of transient transport failures.
    Use a new ref_id only when the user wants a new order.

    Args:
        account_number: Brokerage account number. Must come from the user or be
            clearly implied. Must be agentic_allowed=true.
        symbol: Stock symbol.
        side: 'buy' or 'sell'.
        order_type: 'market', 'limit', 'stop_market', or 'stop_limit'.
        quantity: Number of shares. Decimals (fractional) allowed for market +
            regular_hours only.
        dollar_amount: USD notional (e.g. '100.00'). Only valid with
            order_type=market.
        limit_price: Limit price; required for limit or stop_limit.
        stop_price: Stop trigger price; required for stop_market or stop_limit.
        time_in_force: 'gfd' (good for day) or 'gtc' (good till cancelled).
            Default: gfd.
        market_hours: 'regular_hours' (default), 'extended_hours', or
            'all_day_hours'.
        ref_id: Idempotency key (UUID). Generate once per logical order and
            re-send on retry. Omitting falls back to a server-generated key
            (loses client↔gateway idempotency).
    """
    token = await _get_token(config)
    return await call_tool(
        token,
        "place_equity_order",
        _args(
            account_number=account_number,
            symbol=symbol,
            side=side,
            type=order_type,
            quantity=quantity,
            dollar_amount=dollar_amount,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            market_hours=market_hours,
            ref_id=ref_id or str(uuid.uuid4()),
        ),
    )


@tool
async def cancel_equity_order(
    account_number: str,
    order_id: str,
    config: RunnableConfig,
) -> Any:
    """
    Cancel an open equity order by order_id. Always confirm with the user before
    calling. Resolve order_id via get_equity_orders if the user refers to it by
    symbol or description; pass the same account_number. Requires an
    agentic_allowed=true account; non-agentic accounts are rejected — do not call.
    Cancellation may be rejected if the order has already filled, was already
    cancelled, or is otherwise ineligible.

    Args:
        account_number: Brokerage account that owns the order. Must come from the
            user or be clearly implied. Must be agentic_allowed=true. The upstream
            rejects mismatches against the order's owning account.
        order_id: Order UUID from get_equity_orders. Must live in account_number.
    """
    token = await _get_token(config)
    return await call_tool(
        token, "cancel_equity_order", {"account_number": account_number, "order_id": order_id}
    )
