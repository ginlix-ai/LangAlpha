"""
Onboarding and personalization completion service.

Two separate completion flows:

1. **Personalization** (BYOK wizard): auto-sets personalization_completed
   when the user has at least one API key or OAuth connection.

2. **Onboarding** (investment preferences): auto-sets onboarding_completed
   when the user submits the profile form, which always sets
   ``risk_preference.risk_level``, or, for a user who onboarded before it,
   when any risk preference is set and they hold or watch at least one stock.
"""

import logging

from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.database import portfolio as portfolio_db
from src.server.models.profile_vocabulary import RISK_LEVELS

logger = logging.getLogger(__name__)


def _has_risk_preference(prefs: dict | None) -> bool:
    """Check if user has any risk preference set (flexible field names)."""
    if not prefs:
        return False
    risk = prefs.get("risk_preference") or {}
    if not isinstance(risk, dict):
        return False
    # Accept risk_tolerance, tolerance, or any non-empty value
    for key in ("risk_tolerance", "tolerance"):
        val = risk.get(key)
        if val and isinstance(val, str) and val.strip():
            return True
    # Fallback: any truthy value in risk_preference
    for v in risk.values():
        if v and isinstance(v, str) and v.strip():
            return True
    return False


def _submitted_risk_level(prefs: dict | None) -> bool:
    """Whether the profile form was submitted: it always sends a risk level,
    and a value from its vocabulary is one the form or a checked write set."""
    risk = (prefs or {}).get("risk_preference")
    return isinstance(risk, dict) and risk.get("risk_level") in RISK_LEVELS


async def maybe_complete_personalization(user_id: str) -> bool:
    """
    If personalization requirements are met, set personalization_completed=True.

    Requirements: at least one API key configured (BYOK) or one OAuth connection.

    Returns:
        True if personalization was just completed, False otherwise.
    """
    try:
        user = await user_db.get_user(user_id)
        if not user:
            return False
        if user.get("personalization_completed"):
            return False

        # get_user() already returns has_api_key and has_oauth_token as
        # computed columns — use them instead of issuing separate queries.
        if user.get("has_api_key") or user.get("has_oauth_token"):
            await user_db.update_user(
                user_id=user_id, personalization_completed=True
            )
            logger.info(
                f"[onboarding] Auto-completed personalization for user {user_id}"
            )
            return True
    except Exception as e:
        logger.warning(
            f"[onboarding] Failed to check/complete personalization: {e}"
        )
    return False


async def maybe_complete_onboarding(user_id: str) -> bool:
    """
    If onboarding requirements are met, set onboarding_completed=True.

    Requirements, either of:
    - ``risk_preference.risk_level`` set: the profile form was submitted,
      which is the whole of onboarding now, stocks or not.
    - Any risk preference set and at least one stock (watchlist item OR
      portfolio holding): the rule from before the form, kept so a user who
      onboarded through the agent still completes the same way.

    Returns:
        True if onboarding was completed, False otherwise.
    """
    try:
        user = await user_db.get_user(user_id)
        if not user:
            return False
        if user.get("onboarding_completed"):
            return False

        prefs = await user_db.get_user_preferences(user_id)
        if _submitted_risk_level(prefs):
            await user_db.update_user(user_id=user_id, onboarding_completed=True)
            logger.info(f"[onboarding] Auto-completed onboarding for user {user_id} (profile form)")
            return True
        if not _has_risk_preference(prefs):
            return False

        # Check watchlist items across all watchlists
        watchlists = await watchlist_db.get_user_watchlists(user_id)
        total_items = 0
        for wl in watchlists or []:
            items = await watchlist_db.get_watchlist_items(wl["watchlist_id"], user_id)
            total_items += len(items or [])
        has_watchlist_stock = total_items > 0

        holdings = await portfolio_db.get_user_portfolio(user_id)
        has_portfolio_stock = len(holdings or []) > 0

        if has_watchlist_stock or has_portfolio_stock:
            await user_db.update_user(user_id=user_id, onboarding_completed=True)
            logger.info(f"[onboarding] Auto-completed onboarding for user {user_id}")
            return True
    except Exception as e:
        logger.warning(f"[onboarding] Failed to check/complete onboarding: {e}")
    return False
