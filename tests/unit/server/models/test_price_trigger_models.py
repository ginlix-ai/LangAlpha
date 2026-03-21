"""Tests for price trigger Pydantic models.

Covers PriceCondition, RetriggerConfig, and PriceTriggerConfig in
src/server/models/automation.py including field constraints, enum values,
and defaults.
"""

import pytest
from pydantic import ValidationError

from src.server.models.automation import (
    MarketType,
    PriceCondition,
    PriceConditionType,
    PriceTriggerConfig,
    RetriggerConfig,
    RetriggerMode,
)


# ---------------------------------------------------------------------------
# PriceCondition
# ---------------------------------------------------------------------------


class TestPriceCondition:
    """PriceCondition construction and validation."""

    def test_valid_price_above(self):
        c = PriceCondition(type=PriceConditionType.PRICE_ABOVE, value=150.0)
        assert c.type == PriceConditionType.PRICE_ABOVE
        assert c.value == 150.0
        assert c.reference == "previous_close"

    def test_valid_pct_change_below_with_day_open(self):
        c = PriceCondition(
            type=PriceConditionType.PCT_CHANGE_BELOW,
            value=5.0,
            reference="day_open",
        )
        assert c.type == PriceConditionType.PCT_CHANGE_BELOW
        assert c.value == 5.0
        assert c.reference == "day_open"

    def test_value_must_be_positive_zero(self):
        with pytest.raises(ValidationError):
            PriceCondition(type=PriceConditionType.PRICE_ABOVE, value=0)

    def test_value_must_be_positive_negative(self):
        with pytest.raises(ValidationError):
            PriceCondition(type=PriceConditionType.PRICE_ABOVE, value=-10.0)

    def test_default_reference_is_previous_close(self):
        c = PriceCondition(type=PriceConditionType.PRICE_BELOW, value=100.0)
        assert c.reference == "previous_close"

    def test_invalid_reference_rejected(self):
        with pytest.raises(ValidationError):
            PriceCondition(
                type=PriceConditionType.PRICE_ABOVE,
                value=100.0,
                reference="week_high",
            )

    def test_invalid_condition_type_rejected(self):
        with pytest.raises(ValidationError):
            PriceCondition(type="volume_above", value=100.0)


# ---------------------------------------------------------------------------
# RetriggerConfig
# ---------------------------------------------------------------------------


class TestRetriggerConfig:
    """RetriggerConfig defaults and constraints."""

    def test_default_mode_is_one_shot(self):
        r = RetriggerConfig()
        assert r.mode == RetriggerMode.ONE_SHOT

    def test_default_cooldown_is_none(self):
        r = RetriggerConfig()
        assert r.cooldown_seconds is None

    def test_cooldown_minimum_4h_for_recurring(self):
        with pytest.raises(ValidationError):
            RetriggerConfig(mode="recurring", cooldown_seconds=3599)

    def test_cooldown_4h_ok_for_recurring(self):
        r = RetriggerConfig(mode="recurring", cooldown_seconds=14400)
        assert r.cooldown_seconds == 14400

    def test_all_modes_accepted(self):
        for mode in RetriggerMode:
            r = RetriggerConfig(mode=mode)
            assert r.mode == mode

    def test_cooldown_normalized_to_recurring(self):
        r = RetriggerConfig(mode="cooldown", cooldown_seconds=14400)
        assert r.mode == RetriggerMode.RECURRING
        assert r.cooldown_seconds == 14400

    def test_one_shot_ignores_cooldown(self):
        r = RetriggerConfig(mode="one_shot", cooldown_seconds=14400)
        assert r.cooldown_seconds is None


# ---------------------------------------------------------------------------
# PriceTriggerConfig
# ---------------------------------------------------------------------------


class TestPriceTriggerConfig:
    """PriceTriggerConfig construction and validation."""

    def _condition(self, **overrides):
        defaults = {"type": PriceConditionType.PRICE_ABOVE, "value": 150.0}
        defaults.update(overrides)
        return PriceCondition(**defaults)

    def test_valid_single_condition(self):
        cfg = PriceTriggerConfig(symbol="AAPL", conditions=[self._condition()])
        assert cfg.symbol == "AAPL"
        assert len(cfg.conditions) == 1

    def test_valid_multiple_conditions(self):
        cfg = PriceTriggerConfig(
            symbol="TSLA",
            conditions=[
                self._condition(type=PriceConditionType.PRICE_ABOVE, value=200.0),
                self._condition(type=PriceConditionType.PCT_CHANGE_ABOVE, value=5.0),
            ],
        )
        assert len(cfg.conditions) == 2

    def test_symbol_required(self):
        with pytest.raises(ValidationError):
            PriceTriggerConfig(conditions=[self._condition()])

    def test_symbol_non_empty(self):
        with pytest.raises(ValidationError):
            PriceTriggerConfig(symbol="", conditions=[self._condition()])

    def test_symbol_max_length(self):
        with pytest.raises(ValidationError):
            PriceTriggerConfig(symbol="A" * 11, conditions=[self._condition()])

    def test_conditions_must_have_at_least_one(self):
        with pytest.raises(ValidationError):
            PriceTriggerConfig(symbol="AAPL", conditions=[])

    def test_default_retrigger_config(self):
        cfg = PriceTriggerConfig(symbol="AAPL", conditions=[self._condition()])
        assert cfg.retrigger.mode == RetriggerMode.ONE_SHOT
        assert cfg.retrigger.cooldown_seconds is None

    def test_full_config_with_custom_retrigger(self):
        cfg = PriceTriggerConfig(
            symbol="GOOG",
            conditions=[self._condition(value=3000.0)],
            retrigger=RetriggerConfig(
                mode=RetriggerMode.RECURRING,
                cooldown_seconds=14400,
            ),
        )
        assert cfg.retrigger.mode == RetriggerMode.RECURRING
        assert cfg.retrigger.cooldown_seconds == 14400

    # -- Symbol validation (validate_bare_symbol) --

    def test_rejects_i_prefix(self):
        with pytest.raises(ValidationError, match="bare symbol"):
            PriceTriggerConfig(symbol="I:SPX", conditions=[self._condition()])

    def test_rejects_caret_prefix(self):
        with pytest.raises(ValidationError, match="bare symbol"):
            PriceTriggerConfig(symbol="^SPX", conditions=[self._condition()])

    def test_normalizes_gspc_to_spx(self):
        cfg = PriceTriggerConfig(symbol="GSPC", conditions=[self._condition()])
        assert cfg.symbol == "SPX"

    def test_normalizes_ixic_to_comp(self):
        cfg = PriceTriggerConfig(symbol="IXIC", conditions=[self._condition()])
        assert cfg.symbol == "COMP"

    def test_uppercases_symbol(self):
        cfg = PriceTriggerConfig(symbol="aapl", conditions=[self._condition()])
        assert cfg.symbol == "AAPL"

    # -- Market auto-detection (infer_market_from_symbol) --

    def test_auto_detects_index_market_for_spx(self):
        cfg = PriceTriggerConfig(symbol="SPX", conditions=[self._condition()])
        assert cfg.market == MarketType.INDEX

    def test_auto_detects_index_market_from_gspc_alias(self):
        cfg = PriceTriggerConfig(symbol="GSPC", conditions=[self._condition()])
        assert cfg.symbol == "SPX"
        assert cfg.market == MarketType.INDEX

    def test_auto_detects_index_for_all_known(self):
        for sym in ("SPX", "DJI", "COMP", "NDX", "RUT", "VIX"):
            cfg = PriceTriggerConfig(symbol=sym, conditions=[self._condition()])
            assert cfg.market == MarketType.INDEX, f"{sym} should auto-detect as index"

    def test_stock_market_default_for_unknown(self):
        cfg = PriceTriggerConfig(symbol="AAPL", conditions=[self._condition()])
        assert cfg.market == MarketType.STOCK

    def test_explicit_market_overridden_by_auto_detection(self):
        """Even if market=stock is passed, known index symbols get corrected."""
        cfg = PriceTriggerConfig(symbol="SPX", market="stock", conditions=[self._condition()])
        assert cfg.market == MarketType.INDEX
