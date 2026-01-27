"""
User Data Sync Service.

Syncs user data (preferences, watchlists, portfolio) to sandbox as read-only
markdown files. These files provide context to the agent but cannot be modified
directly - agents must use the user-profile skill instead.

Directory structure in sandbox:
    /home/daytona/user/
    ├── preference.md      # User preferences (risk, investment, agent settings)
    ├── watchlist.md       # All watchlists with symbols
    └── portfolio.md       # Holdings (symbol, quantity, cost basis)
"""

import asyncio
import logging
from typing import Any

from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.database import portfolio as portfolio_db

logger = logging.getLogger(__name__)

# Sandbox directory for user data files
USER_DATA_DIR = "user"
USER_DATA_SANDBOX_PATH = f"/home/daytona/{USER_DATA_DIR}"

# File names
PREFERENCE_FILE = "preference.md"
WATCHLIST_FILE = "watchlist.md"
PORTFOLIO_FILE = "portfolio.md"


# =============================================================================
# Data Fetching
# =============================================================================


def _handle_result(result: Any, default: Any, name: str) -> Any:
    """Handle asyncio.gather result that may be an exception."""
    if isinstance(result, Exception):
        logger.warning(f"Failed to fetch {name}: {result}")
        return default
    return result


async def fetch_all_user_data(user_id: str) -> dict[str, Any]:
    """
    Fetch all user data from database in parallel.

    Args:
        user_id: User ID

    Returns:
        Dict with profile, preferences, watchlists, portfolio
    """
    # Fetch all data in parallel
    user_data, preferences, watchlists, portfolio = await asyncio.gather(
        user_db.get_user(user_id),
        user_db.get_user_preferences(user_id),
        watchlist_db.get_user_watchlists(user_id),
        portfolio_db.get_user_portfolio(user_id),
        return_exceptions=True,
    )

    # Handle exceptions gracefully
    user_data = _handle_result(user_data, None, "user data")
    preferences = _handle_result(preferences, None, "preferences")
    watchlists = _handle_result(watchlists, [], "watchlists")
    portfolio = _handle_result(portfolio, [], "portfolio")

    # Fetch watchlist items for each watchlist
    watchlists_with_items = []
    if watchlists:
        async def get_watchlist_with_items(wl: dict[str, Any]) -> dict[str, Any]:
            try:
                items = await watchlist_db.get_watchlist_items(wl["watchlist_id"], user_id)
                return {**wl, "items": items}
            except Exception as e:
                logger.warning(f"Failed to fetch watchlist items: {e}")
                return {**wl, "items": []}

        watchlists_with_items = await asyncio.gather(
            *[get_watchlist_with_items(wl) for wl in watchlists]
        )

    return {
        "profile": user_data,
        "preferences": preferences,
        "watchlists": list(watchlists_with_items),
        "portfolio": portfolio,
    }


# =============================================================================
# Markdown Formatters
# =============================================================================


def format_preferences_md(data: dict[str, Any]) -> str:
    """
    Format user preferences as markdown.

    Args:
        data: Dict with 'preferences' key containing preference data

    Returns:
        Markdown string
    """
    preferences = data.get("preferences")
    if not preferences:
        return "# User Preferences\n\nNo preferences set yet.\n"

    lines = ["# User Preferences", ""]

    # Risk preference
    risk_pref = preferences.get("risk_preference")
    if risk_pref:
        lines.append("## Risk Tolerance")
        risk_tolerance = risk_pref.get("risk_tolerance", "not set")
        lines.append(f"- Level: {risk_tolerance}")
        # Add any extra fields
        for key, value in risk_pref.items():
            if key != "risk_tolerance" and value:
                lines.append(f"- {key.replace('_', ' ').title()}: {value}")
        lines.append("")

    # Investment preference
    inv_pref = preferences.get("investment_preference")
    if inv_pref:
        lines.append("## Investment Style")
        for key, value in inv_pref.items():
            if value:
                lines.append(f"- {key.replace('_', ' ').title()}: {value}")
        lines.append("")

    # Agent preference
    agent_pref = preferences.get("agent_preference")
    if agent_pref:
        lines.append("## Agent Settings")
        for key, value in agent_pref.items():
            if value:
                lines.append(f"- {key.replace('_', ' ').title()}: {value}")
        lines.append("")

    return "\n".join(lines)


def format_watchlist_md(data: dict[str, Any]) -> str:
    """
    Format watchlists as markdown.

    Args:
        data: Dict with 'watchlists' key containing watchlist data

    Returns:
        Markdown string
    """
    watchlists = data.get("watchlists", [])
    if not watchlists:
        return "# Watchlists\n\nNo watchlists yet.\n"

    lines = ["# Watchlists", ""]

    for wl in watchlists:
        name = wl.get("name", "Unnamed")
        is_default = wl.get("is_default", False)
        description = wl.get("description")

        header = f"## {name}"
        if is_default:
            header += " (Default)"
        lines.append(header)

        if description:
            lines.append(f"_{description}_")
            lines.append("")

        items = wl.get("items", [])
        if items:
            lines.append("| Symbol | Type | Notes |")
            lines.append("|--------|------|-------|")
            for item in items:
                symbol = item.get("symbol", "")
                inst_type = item.get("instrument_type", "stock")
                notes = item.get("notes", "")
                # Escape pipe characters in notes
                notes = notes.replace("|", "\\|") if notes else ""
                lines.append(f"| {symbol} | {inst_type} | {notes} |")
        else:
            lines.append("_No items in this watchlist._")

        lines.append("")

    return "\n".join(lines)


def format_portfolio_md(data: dict[str, Any]) -> str:
    """
    Format portfolio holdings as markdown.

    Args:
        data: Dict with 'portfolio' key containing holding data

    Returns:
        Markdown string
    """
    portfolio = data.get("portfolio", [])
    if not portfolio:
        return "# Portfolio Holdings\n\nNo holdings yet.\n"

    lines = ["# Portfolio Holdings", ""]
    lines.append("| Symbol | Type | Quantity | Avg Cost | Account |")
    lines.append("|--------|------|----------|----------|---------|")

    for holding in portfolio:
        symbol = holding.get("symbol", "")
        inst_type = holding.get("instrument_type", "stock")
        quantity = holding.get("quantity", 0)
        avg_cost = holding.get("average_cost")
        account = holding.get("account_name", "")
        currency = holding.get("currency", "USD")

        # Format quantity (remove trailing zeros)
        if isinstance(quantity, (int, float)):
            qty_str = f"{quantity:g}"
        else:
            qty_str = str(quantity)

        # Format cost with currency symbol
        if avg_cost is not None:
            currency_symbol = "$" if currency == "USD" else currency
            cost_str = f"{currency_symbol}{avg_cost:,.2f}"
        else:
            cost_str = "-"

        lines.append(f"| {symbol} | {inst_type} | {qty_str} | {cost_str} | {account} |")

    lines.append("")
    return "\n".join(lines)


# =============================================================================
# Sync Functions
# =============================================================================


async def _sync_file_if_changed(sandbox: Any, path: str, new_content: str) -> bool:
    """
    Write file only if content differs from existing.

    Args:
        sandbox: PTCSandbox instance
        path: File path in sandbox
        new_content: New content to write

    Returns:
        True if file was written, False if unchanged
    """
    try:
        existing = await sandbox.aread_file_text(path)
        if existing == new_content:
            logger.debug(f"[sync_user_data] File unchanged: {path}")
            return False
    except Exception as e:
        # File doesn't exist or error reading - will write
        logger.debug(f"[sync_user_data] File read failed (will create): {path} - {e}")

    logger.info(f"[sync_user_data] Writing file: {path} ({len(new_content)} bytes)")
    success = await sandbox.awrite_file_text(path, new_content)
    logger.info(f"[sync_user_data] Write result for {path}: {success}")
    return success if success is not None else True


async def sync_user_data_to_sandbox(
    sandbox: Any,
    user_id: str,
) -> dict[str, bool]:
    """
    Sync all user data to sandbox as markdown files.

    Creates/updates files only if content has changed.

    Args:
        sandbox: PTCSandbox instance
        user_id: User ID

    Returns:
        Dict with file names as keys and bool (True if updated) as values
    """
    logger.info(f"[sync_user_data] Starting sync for user_id={user_id}")

    # Fetch all data
    data = await fetch_all_user_data(user_id)
    logger.info(f"[sync_user_data] Fetched data: profile={data.get('profile') is not None}, "
                f"preferences={data.get('preferences') is not None}, "
                f"watchlists={len(data.get('watchlists', []))}, "
                f"portfolio={len(data.get('portfolio', []))}")

    # Format markdown
    preference_md = format_preferences_md(data)
    watchlist_md = format_watchlist_md(data)
    portfolio_md = format_portfolio_md(data)

    # Ensure directory exists
    try:
        logger.info(f"[sync_user_data] Creating directory: {USER_DATA_SANDBOX_PATH}")
        await sandbox.execute_bash_command(f"mkdir -p {USER_DATA_SANDBOX_PATH}")
    except Exception as e:
        logger.warning(f"Failed to create user data directory: {e}")

    # Sync all files in parallel
    results = await asyncio.gather(
        _sync_file_if_changed(
            sandbox,
            f"{USER_DATA_SANDBOX_PATH}/{PREFERENCE_FILE}",
            preference_md,
        ),
        _sync_file_if_changed(
            sandbox,
            f"{USER_DATA_SANDBOX_PATH}/{WATCHLIST_FILE}",
            watchlist_md,
        ),
        _sync_file_if_changed(
            sandbox,
            f"{USER_DATA_SANDBOX_PATH}/{PORTFOLIO_FILE}",
            portfolio_md,
        ),
        return_exceptions=True,
    )

    # Build result dict
    files = [PREFERENCE_FILE, WATCHLIST_FILE, PORTFOLIO_FILE]
    sync_results = {}
    for file_name, result in zip(files, results):
        if isinstance(result, Exception):
            logger.warning(f"Failed to sync {file_name}: {result}")
            sync_results[file_name] = False
        else:
            sync_results[file_name] = result

    updated_count = sum(1 for v in sync_results.values() if v)
    if updated_count > 0:
        logger.info(
            f"[sync_user_data] Synced {updated_count} files for user {user_id}"
        )
    else:
        logger.debug(f"[sync_user_data] No changes for user {user_id}")

    return sync_results


async def sync_single_file(
    sandbox: Any,
    entity: str,
    user_id: str,
) -> bool:
    """
    Sync a single user data file (for partial updates after mutations).

    Args:
        sandbox: PTCSandbox instance
        entity: Entity type that was changed (maps to file)
        user_id: User ID

    Returns:
        True if file was updated, False otherwise
    """
    # Map entity to file
    entity_to_file = {
        # Preference entities -> preference.md
        "profile": PREFERENCE_FILE,
        "risk_preference": PREFERENCE_FILE,
        "investment_preference": PREFERENCE_FILE,
        "agent_preference": PREFERENCE_FILE,
        # Watchlist entities -> watchlist.md
        "watchlist": WATCHLIST_FILE,
        "watchlist_item": WATCHLIST_FILE,
        "watchlists": WATCHLIST_FILE,
        "watchlist_items": WATCHLIST_FILE,
        # Portfolio entities -> portfolio.md
        "portfolio": PORTFOLIO_FILE,
        "portfolio_holding": PORTFOLIO_FILE,
    }

    file_name = entity_to_file.get(entity)
    if not file_name:
        logger.warning(f"Unknown entity type for sync: {entity}")
        return False

    # Map file to formatter
    formatters = {
        PREFERENCE_FILE: format_preferences_md,
        WATCHLIST_FILE: format_watchlist_md,
        PORTFOLIO_FILE: format_portfolio_md,
    }
    formatter = formatters.get(file_name)
    if not formatter:
        return False

    # Fetch and format the specific data
    data = await fetch_all_user_data(user_id)
    content = formatter(data)

    try:
        updated = await _sync_file_if_changed(
            sandbox,
            f"{USER_DATA_SANDBOX_PATH}/{file_name}",
            content,
        )
        if updated:
            logger.info(f"[sync_user_data] Updated {file_name} for user {user_id}")
        return updated
    except Exception as e:
        logger.warning(f"Failed to sync {file_name}: {e}")
        return False
