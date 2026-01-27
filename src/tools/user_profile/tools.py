"""
Unified user profile tools for managing watchlists, portfolio, and preferences.

These tools run on the HOST (not in sandbox) and have direct database access.
They are designed to be dynamically loaded via the load_skill mechanism.

- get_user_data: Read user data (profile, preferences, watchlists, portfolio)
- update_user_data: Create or update user data (upsert semantics)
- remove_user_data: Delete user data
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.database import portfolio as portfolio_db

logger = logging.getLogger(__name__)


# ==================== Helpers ====================


def _get_user_id(config: RunnableConfig) -> str:
    """Extract user_id from the runnable config."""
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        raise ValueError("user_id not found in config. Ensure it's passed in configurable.")
    return user_id


def _get_workspace_id(config: RunnableConfig) -> str | None:
    """Extract workspace_id from the runnable config."""
    configurable = config.get("configurable", {})
    return configurable.get("workspace_id")


async def _sync_user_file_async(entity: str, user_id: str, workspace_id: str) -> None:
    """
    Sync user data file to sandbox (fire-and-forget).

    Args:
        entity: Entity type that was changed
        user_id: User ID
        workspace_id: Workspace ID to get sandbox from
    """
    try:
        from src.server.services.workspace_manager import WorkspaceManager
        from src.server.services.sync_user_data import sync_single_file

        workspace_manager = WorkspaceManager.get_instance()
        # Get session without user_id to avoid re-syncing all files
        session = await workspace_manager.get_session_for_workspace(workspace_id)

        if session and session.sandbox:
            await sync_single_file(session.sandbox, entity, user_id)
            logger.debug(f"[user_profile] Synced {entity} for user {user_id}")
    except Exception as e:
        # Non-blocking - don't fail the tool if sync fails
        logger.warning(f"[user_profile] Failed to sync {entity}: {e}")


def _schedule_sync(entity: str, config: RunnableConfig) -> None:
    """
    Schedule a background sync task for the affected file.

    Args:
        entity: Entity type that was changed
        config: RunnableConfig with user_id and workspace_id
    """
    user_id = _get_user_id(config)
    workspace_id = _get_workspace_id(config)

    if not workspace_id:
        logger.debug("[user_profile] No workspace_id in config, skipping sync")
        return

    # Fire-and-forget: schedule sync without awaiting
    asyncio.create_task(_sync_user_file_async(entity, user_id, workspace_id))


def validate_enum_field(
    value: str | None, valid_values: list[str], field_name: str
) -> str | dict[str, str]:
    """Validate and normalize an enum field value.

    Args:
        value: The value to validate (may be None)
        valid_values: List of valid lowercase values
        field_name: Name of the field for error messages

    Returns:
        Normalized lowercase value if valid, or error dict if invalid
    """
    if not value:
        return {"error": f"{field_name} is required"}
    normalized = value.lower()
    if normalized not in valid_values:
        return {"error": f"Invalid {field_name}. Must be one of: {', '.join(valid_values)}"}
    return normalized


# ==================== GET Handlers ====================


async def _get_profile(config: RunnableConfig, entity_id: str | None = None) -> dict[str, Any]:
    """Get user profile info (excludes sensitive data like email/phone)."""
    user_id = _get_user_id(config)
    user = await user_db.get_user(user_id)
    if not user:
        return {"error": "User not found"}
    return {
        "name": user.get("name"),
        "timezone": user.get("timezone"),
        "locale": user.get("locale"),
    }


async def _get_preferences(config: RunnableConfig, entity_id: str | None = None) -> dict[str, Any]:
    """Get all user preferences."""
    user_id = _get_user_id(config)
    prefs = await user_db.get_user_preferences(user_id)
    if not prefs:
        return {"message": "No preferences set yet."}
    return {
        "risk_preference": prefs.get("risk_preference"),
        "investment_preference": prefs.get("investment_preference"),
        "agent_preference": prefs.get("agent_preference"),
    }


async def _get_watchlists(config: RunnableConfig, entity_id: str | None = None) -> list[dict[str, Any]]:
    """Get all watchlists for the user."""
    user_id = _get_user_id(config)
    watchlists = await watchlist_db.get_user_watchlists(user_id)
    return watchlists


async def _get_watchlist_items(config: RunnableConfig, entity_id: str | None = None) -> list[dict[str, Any]]:
    """Get items in a specific watchlist."""
    user_id = _get_user_id(config)

    # Get or create default watchlist if none specified
    if not entity_id:
        default_watchlist = await watchlist_db.get_or_create_default_watchlist(user_id)
        entity_id = default_watchlist["watchlist_id"]

    items = await watchlist_db.get_watchlist_items(entity_id, user_id)
    return items


async def _get_portfolio(config: RunnableConfig, entity_id: str | None = None) -> list[dict[str, Any]]:
    """Get all portfolio holdings."""
    user_id = _get_user_id(config)
    holdings = await portfolio_db.get_user_portfolio(user_id)
    return holdings


async def _get_all(config: RunnableConfig, entity_id: str | None = None) -> dict[str, Any]:
    """Get complete user data including profile, preferences, watchlists, and portfolio."""
    user_id = _get_user_id(config)

    # Fetch all data in parallel
    profile, preferences, watchlists, portfolio = await asyncio.gather(
        _get_profile(config, entity_id),
        _get_preferences(config, entity_id),
        _get_watchlists(config, entity_id),
        _get_portfolio(config, entity_id),
    )

    # Get items for each watchlist in parallel
    async def get_watchlist_with_items(wl: dict[str, Any]) -> dict[str, Any]:
        items = await watchlist_db.get_watchlist_items(wl["watchlist_id"], user_id)
        return {**wl, "items": items}

    watchlists_with_items = await asyncio.gather(
        *[get_watchlist_with_items(wl) for wl in watchlists]
    )

    return {
        "profile": profile,
        "preferences": preferences,
        "watchlists": list(watchlists_with_items),
        "portfolio": portfolio,
    }


# ==================== UPDATE Handlers ====================


async def _update_profile(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Update user profile (name, timezone, locale, onboarding_completed)."""
    user_id = _get_user_id(config)

    updated = await user_db.update_user(
        user_id=user_id,
        name=data.get("name"),
        timezone=data.get("timezone"),
        locale=data.get("locale"),
        onboarding_completed=data.get("onboarding_completed"),
    )

    if not updated:
        return {"error": "Failed to update profile"}

    return {
        "success": True,
        "profile": {
            "name": updated.get("name"),
            "timezone": updated.get("timezone"),
            "locale": updated.get("locale"),
        },
    }


async def _update_risk_preference(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Set risk tolerance settings."""
    user_id = _get_user_id(config)

    risk_pref: dict[str, Any] = {}

    # Validate risk_tolerance if provided
    risk_tolerance = data.get("risk_tolerance")
    if risk_tolerance:
        valid_values = ["low", "medium", "high", "long_term_focus"]
        result = validate_enum_field(risk_tolerance, valid_values, "risk_tolerance")
        if isinstance(result, dict):
            return result
        risk_pref["risk_tolerance"] = result

    # Add any extra fields
    for key, value in data.items():
        if key != "risk_tolerance" and value is not None:
            risk_pref[key] = value

    # When replacing, require the main field
    if replace and "risk_tolerance" not in risk_pref:
        return {"error": "risk_tolerance is required when replace=True"}

    if not risk_pref:
        return {"error": "At least one field is required"}

    prefs = await user_db.upsert_user_preferences(
        user_id=user_id, risk_preference=risk_pref, replace=replace
    )
    return {"success": True, "risk_preference": prefs.get("risk_preference", {})}


async def _update_investment_preference(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Set investment style settings."""
    user_id = _get_user_id(config)

    investment_pref: dict[str, Any] = {}
    enum_fields = {"company_interest", "holding_period", "analysis_focus"}

    # Validate enum fields if provided
    company_interest = data.get("company_interest")
    if company_interest:
        valid_values = ["growth", "stable", "value", "esg"]
        result = validate_enum_field(company_interest, valid_values, "company_interest")
        if isinstance(result, dict):
            return result
        investment_pref["company_interest"] = result

    holding_period = data.get("holding_period")
    if holding_period:
        valid_values = ["short_term", "mid_term", "long_term", "flexible"]
        result = validate_enum_field(holding_period, valid_values, "holding_period")
        if isinstance(result, dict):
            return result
        investment_pref["holding_period"] = result

    analysis_focus = data.get("analysis_focus")
    if analysis_focus:
        valid_values = ["growth", "valuation", "moat", "risk"]
        result = validate_enum_field(analysis_focus, valid_values, "analysis_focus")
        if isinstance(result, dict):
            return result
        investment_pref["analysis_focus"] = result

    # Add any extra fields (notes, avoid_sectors, focus_sectors, etc.)
    for key, value in data.items():
        if key not in enum_fields and value is not None:
            investment_pref[key] = value

    if not investment_pref:
        return {"error": "At least one field is required"}

    prefs = await user_db.upsert_user_preferences(
        user_id=user_id, investment_preference=investment_pref, replace=replace
    )
    return {"success": True, "investment_preference": prefs.get("investment_preference", {})}


async def _update_agent_preference(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Set agent behavior settings."""
    user_id = _get_user_id(config)

    agent_pref: dict[str, Any] = {}

    # Validate output_style if provided
    output_style = data.get("output_style")
    if output_style:
        valid_values = ["summary", "data", "deep_dive", "quick"]
        result = validate_enum_field(output_style, valid_values, "output_style")
        if isinstance(result, dict):
            return result
        agent_pref["output_style"] = result

    # Add any extra fields (notes, instruction, data_visualization, etc.)
    for key, value in data.items():
        if key != "output_style" and value is not None:
            agent_pref[key] = value

    # When replacing, require the main field
    if replace and "output_style" not in agent_pref:
        return {"error": "output_style is required when replace=True"}

    if not agent_pref:
        return {"error": "At least one field is required"}

    prefs = await user_db.upsert_user_preferences(
        user_id=user_id, agent_preference=agent_pref, replace=replace
    )
    return {"success": True, "agent_preference": prefs.get("agent_preference", {})}


async def _upsert_watchlist(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Create or update a watchlist."""
    user_id = _get_user_id(config)

    name = data.get("name")
    if not name:
        return {"error": "name is required"}

    try:
        watchlist = await watchlist_db.create_watchlist(
            user_id=user_id,
            name=name,
            description=data.get("description"),
            is_default=data.get("is_default", False),
        )
        return {"success": True, "watchlist": watchlist}
    except ValueError as e:
        return {"error": str(e)}


async def _upsert_watchlist_item(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Add or update an item in a watchlist."""
    user_id = _get_user_id(config)

    symbol = data.get("symbol")
    if not symbol:
        return {"error": "symbol is required"}

    watchlist_id = data.get("watchlist_id")
    if not watchlist_id:
        default_watchlist = await watchlist_db.get_or_create_default_watchlist(user_id)
        watchlist_id = default_watchlist["watchlist_id"]

    try:
        item = await watchlist_db.create_watchlist_item(
            user_id=user_id,
            watchlist_id=watchlist_id,
            symbol=symbol.upper(),
            instrument_type=data.get("instrument_type", "stock"),
            exchange=data.get("exchange"),
            name=data.get("name"),
            notes=data.get("notes"),
        )
        return {"success": True, "watchlist_item": item}
    except ValueError as e:
        return {"error": str(e)}


async def _upsert_portfolio_holding(config: RunnableConfig, data: dict[str, Any], replace: bool = False) -> dict[str, Any]:
    """Add or update a portfolio holding."""
    user_id = _get_user_id(config)

    symbol = data.get("symbol")
    quantity = data.get("quantity")
    if not symbol:
        return {"error": "symbol is required"}
    if quantity is None:
        return {"error": "quantity is required"}

    # Parse date if provided
    purchase_date = None
    first_purchased_at = data.get("first_purchased_at")
    if first_purchased_at:
        try:
            purchase_date = datetime.fromisoformat(first_purchased_at)
        except ValueError:
            return {"error": f"Invalid date format: {first_purchased_at}. Use ISO format (YYYY-MM-DD)"}

    average_cost = data.get("average_cost")

    try:
        holding = await portfolio_db.create_portfolio_holding(
            user_id=user_id,
            symbol=symbol.upper(),
            instrument_type=data.get("instrument_type", "stock"),
            quantity=Decimal(str(quantity)),
            average_cost=Decimal(str(average_cost)) if average_cost else None,
            currency=data.get("currency", "USD"),
            exchange=data.get("exchange"),
            name=data.get("name"),
            account_name=data.get("account_name"),
            notes=data.get("notes"),
            first_purchased_at=purchase_date,
        )
        return {"success": True, "portfolio_holding": holding}
    except ValueError as e:
        return {"error": str(e)}


# ==================== REMOVE Handlers ====================


async def _remove_watchlist(config: RunnableConfig, identifier: dict[str, Any]) -> dict[str, Any]:
    """Delete an entire watchlist."""
    user_id = _get_user_id(config)

    watchlist_id = identifier.get("watchlist_id")
    name = identifier.get("name")

    if not watchlist_id and not name:
        return {"error": "Either watchlist_id or name is required"}

    # If name provided, find the watchlist first
    if not watchlist_id and name:
        watchlists = await watchlist_db.get_user_watchlists(user_id)
        for wl in watchlists:
            if wl["name"].lower() == name.lower():
                watchlist_id = wl["watchlist_id"]
                break
        if not watchlist_id:
            return {"error": f"Watchlist '{name}' not found"}

    deleted = await watchlist_db.delete_watchlist(watchlist_id, user_id)
    if deleted:
        return {"success": True, "deleted": watchlist_id}
    return {"success": False, "error": "Failed to delete watchlist"}


async def _remove_watchlist_item(config: RunnableConfig, identifier: dict[str, Any]) -> dict[str, Any]:
    """Remove an item from a watchlist."""
    user_id = _get_user_id(config)

    symbol = identifier.get("symbol")
    if not symbol:
        return {"error": "symbol is required"}

    watchlist_id = identifier.get("watchlist_id")
    if not watchlist_id:
        default_watchlist = await watchlist_db.get_or_create_default_watchlist(user_id)
        watchlist_id = default_watchlist["watchlist_id"]

    # Find the item by symbol
    items = await watchlist_db.get_watchlist_items(watchlist_id, user_id)
    symbol_upper = symbol.upper()

    for item in items:
        if item["symbol"].upper() == symbol_upper:
            deleted = await watchlist_db.delete_watchlist_item(item["item_id"], user_id)
            if deleted:
                return {"success": True, "symbol": symbol_upper}
            return {"success": False, "error": "Failed to delete item"}

    return {"success": False, "error": f"Symbol {symbol_upper} not found in watchlist"}


async def _remove_portfolio_holding(config: RunnableConfig, identifier: dict[str, Any]) -> dict[str, Any]:
    """Remove a portfolio holding."""
    user_id = _get_user_id(config)

    symbol = identifier.get("symbol")
    if not symbol:
        return {"error": "symbol is required"}

    account_name = identifier.get("account_name")

    # Find the holding by symbol
    holdings = await portfolio_db.get_user_portfolio(user_id)
    symbol_upper = symbol.upper()

    matching_holding = None
    for holding in holdings:
        if holding["symbol"].upper() == symbol_upper:
            if account_name and holding.get("account_name") != account_name:
                continue
            matching_holding = holding
            break

    if not matching_holding:
        return {"success": False, "error": f"Holding for {symbol_upper} not found in portfolio"}

    deleted = await portfolio_db.delete_portfolio_holding(matching_holding["holding_id"], user_id)
    if deleted:
        return {"success": True, "symbol": symbol_upper}
    return {"success": False, "error": "Failed to delete holding"}


# ==================== Entity Handler Registry ====================


GET_HANDLERS = {
    "all": _get_all,
    "profile": _get_profile,
    "preferences": _get_preferences,
    "watchlists": _get_watchlists,
    "watchlist_items": _get_watchlist_items,
    "portfolio": _get_portfolio,
}

UPDATE_HANDLERS = {
    "profile": _update_profile,
    "risk_preference": _update_risk_preference,
    "investment_preference": _update_investment_preference,
    "agent_preference": _update_agent_preference,
    "watchlist": _upsert_watchlist,
    "watchlist_item": _upsert_watchlist_item,
    "portfolio_holding": _upsert_portfolio_holding,
}

REMOVE_HANDLERS = {
    "watchlist": _remove_watchlist,
    "watchlist_item": _remove_watchlist_item,
    "portfolio_holding": _remove_portfolio_holding,
}


# ==================== Unified Tools ====================


@tool
async def get_user_data(
    entity: str,
    config: RunnableConfig,
    entity_id: str | None = None,
) -> dict[str, Any]:
    """Get user data.

    Args:
        entity: What to retrieve. One of:
            - "all": Complete user data (profile, preferences, watchlists with items, portfolio)
            - "profile": User info (name, timezone, locale)
            - "preferences": All preferences (risk, investment, agent)
            - "watchlists": List of all watchlists
            - "watchlist_items": Items in a specific watchlist
            - "portfolio": All portfolio holdings
        entity_id: Required for "watchlist_items" (watchlist_id).
                   If omitted, uses default watchlist.

    Returns:
        The requested data as a dictionary.
    """
    handler = GET_HANDLERS.get(entity)
    if not handler:
        return {"error": f"Unknown entity: {entity}. Valid entities: {list(GET_HANDLERS.keys())}"}
    return await handler(config, entity_id)


@tool
async def update_user_data(
    entity: str,
    data: dict[str, Any],
    config: RunnableConfig,
    replace: bool = False,
) -> dict[str, Any]:
    """Create or update user data.

    Args:
        entity: What to update. One of:
            - "profile": Update user info or mark onboarding complete
            - "risk_preference": Set risk tolerance settings
            - "investment_preference": Set investment style settings
            - "agent_preference": Set agent behavior settings
            - "watchlist": Create or update a watchlist
            - "watchlist_item": Add or update item in watchlist
            - "portfolio_holding": Add or update a portfolio holding
        data: Fields to set (see SKILL.md for entity-specific fields).
              Extra fields like notes, instruction, avoid_sectors are allowed.
        replace: If True, completely replace the preference instead of merging.
                 Only applies to preference entities (risk, investment, agent).

    Returns:
        The created/updated record.
    """
    handler = UPDATE_HANDLERS.get(entity)
    if not handler:
        return {"error": f"Unknown entity: {entity}. Valid entities: {list(UPDATE_HANDLERS.keys())}"}

    result = await handler(config, data, replace)

    # Schedule sync if operation succeeded
    if result.get("success"):
        _schedule_sync(entity, config)

    return result


@tool
async def remove_user_data(
    entity: str,
    identifier: dict[str, Any],
    config: RunnableConfig,
) -> dict[str, Any]:
    """Remove user data.

    Args:
        entity: What to remove. One of:
            - "watchlist": Delete an entire watchlist
            - "watchlist_item": Remove item from watchlist
            - "portfolio_holding": Remove a portfolio holding
        identifier: How to identify what to remove (see SKILL.md)

    Returns:
        Confirmation of deletion.
    """
    handler = REMOVE_HANDLERS.get(entity)
    if not handler:
        return {"error": f"Unknown entity: {entity}. Valid entities: {list(REMOVE_HANDLERS.keys())}"}

    result = await handler(config, identifier)

    # Schedule sync if operation succeeded
    if result.get("success"):
        _schedule_sync(entity, config)

    return result
