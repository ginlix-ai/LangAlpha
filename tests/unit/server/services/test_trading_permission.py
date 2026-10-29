"""A stored level grants only what its agreement still covers."""

from __future__ import annotations

import pytest

from src.server.services.trading_permission import (
    DEFAULT_TRADING_PERMISSION,
    TRADING_AGREEMENT_VERSION,
    TradingPermission,
    effective_permission,
)


@pytest.mark.parametrize("level", ["plan_first", "autonomous"])
def test_a_level_that_skips_approval_holds_only_under_the_current_agreement(level):
    assert effective_permission(level, TRADING_AGREEMENT_VERSION) is TradingPermission(level)
    assert effective_permission(level, TRADING_AGREEMENT_VERSION - 1) is DEFAULT_TRADING_PERMISSION
    assert effective_permission(level, None) is DEFAULT_TRADING_PERMISSION


@pytest.mark.parametrize("level", ["no_trading", "approve_each"])
def test_a_level_that_asks_needs_no_agreement(level):
    assert effective_permission(level, None) is TradingPermission(level)


@pytest.mark.parametrize("stored", [None, "", "yolo", 3, {}])
def test_anything_unreadable_asks(stored):
    assert effective_permission(stored, TRADING_AGREEMENT_VERSION) is DEFAULT_TRADING_PERMISSION
    assert DEFAULT_TRADING_PERMISSION.asks
