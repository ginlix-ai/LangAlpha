"""When ``onboarding_completed`` flips on its own.

Two rules: the profile form was submitted (it always sets
``risk_preference.risk_level``), or the rule from before the form, any risk
preference plus a stock held or watched. Every writer of those rows calls the
check after it commits: the preferences PUT, the watchlist and portfolio
endpoints, and the agent's profile files.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services import onboarding

USER = "user-1"


@contextmanager
def _account(prefs, *, completed=False, watchlist_items=0, holdings=0):
    """The rows the check reads, yielding the ``update_user`` mock."""
    watchlists = [{"watchlist_id": "wl-1"}]
    with (
        patch.object(
            onboarding.user_db,
            "get_user",
            new=AsyncMock(return_value={"user_id": USER, "onboarding_completed": completed}),
        ),
        patch.object(onboarding.user_db, "get_user_preferences", new=AsyncMock(return_value=prefs)),
        patch.object(onboarding.user_db, "update_user", new=AsyncMock()) as update,
        patch.object(
            onboarding.watchlist_db, "get_user_watchlists", new=AsyncMock(return_value=watchlists)
        ),
        patch.object(
            onboarding.watchlist_db,
            "get_watchlist_items",
            new=AsyncMock(return_value=[{}] * watchlist_items),
        ),
        patch.object(
            onboarding.portfolio_db, "get_user_portfolio", new=AsyncMock(return_value=[{}] * holdings)
        ),
    ):
        yield update


@pytest.mark.asyncio
async def test_a_submitted_form_completes_onboarding_without_a_stock():
    with _account({"risk_preference": {"risk_level": "moderate"}}) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is True
    update.assert_awaited_once_with(user_id=USER, onboarding_completed=True)


@pytest.mark.asyncio
async def test_the_rule_from_before_the_form_still_completes_with_a_stock():
    with _account({"risk_preference": {"risk_tolerance": "moderate"}}, holdings=1) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is True
    update.assert_awaited_once_with(user_id=USER, onboarding_completed=True)


@pytest.mark.asyncio
async def test_the_rule_from_before_the_form_still_needs_a_stock():
    with _account({"risk_preference": {"risk_tolerance": "moderate"}}) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is False
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_risk_level_the_form_never_offers_is_not_a_submitted_form():
    """A ``risk_level`` from before the key had a vocabulary falls to the old
    rule rather than standing in for the form."""
    with _account({"risk_preference": {"risk_level": "medium"}}) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is False
    update.assert_not_awaited()

    with _account({"risk_preference": {"risk_level": "medium"}}, watchlist_items=2) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is True
    update.assert_awaited_once_with(user_id=USER, onboarding_completed=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefs",
    [None, {}, {"risk_preference": {}}, {"investment_preference": {"markets": ["us"]}}],
)
async def test_no_risk_preference_completes_nothing(prefs):
    with _account(prefs, holdings=3) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is False
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_completed_account_is_left_alone():
    with _account({"risk_preference": {"risk_level": "growth"}}, completed=True) as update:
        assert await onboarding.maybe_complete_onboarding(USER) is False
    update.assert_not_awaited()
