"""Unit tests for ConditionEvaluator — price condition evaluation logic."""

import pytest

from src.server.models.automation import PriceConditionType
from src.server.services.price_monitor import ConditionEvaluator


class TestPriceAbove:
    def test_returns_true_when_price_exceeds_threshold(self):
        ev = ConditionEvaluator()
        assert ev.evaluate(PriceConditionType.PRICE_ABOVE, 150.0, "previous_close", 151.0, "AAPL")

    def test_returns_false_when_price_below_threshold(self):
        ev = ConditionEvaluator()
        assert not ev.evaluate(PriceConditionType.PRICE_ABOVE, 150.0, "previous_close", 149.99, "AAPL")

    def test_returns_false_when_price_equals_threshold(self):
        ev = ConditionEvaluator()
        assert not ev.evaluate(PriceConditionType.PRICE_ABOVE, 150.0, "previous_close", 150.0, "AAPL")


class TestPriceBelow:
    def test_returns_true_when_price_below_threshold(self):
        ev = ConditionEvaluator()
        assert ev.evaluate(PriceConditionType.PRICE_BELOW, 150.0, "previous_close", 149.0, "AAPL")

    def test_returns_false_when_price_above_threshold(self):
        ev = ConditionEvaluator()
        assert not ev.evaluate(PriceConditionType.PRICE_BELOW, 150.0, "previous_close", 150.01, "AAPL")

    def test_returns_false_when_price_equals_threshold(self):
        ev = ConditionEvaluator()
        assert not ev.evaluate(PriceConditionType.PRICE_BELOW, 150.0, "previous_close", 150.0, "AAPL")


class TestPctChangeAbove:
    def test_returns_true_when_pct_change_exceeds_threshold(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=100.0, day_open=102.0)
        # 6% up from previous_close of 100
        assert ev.evaluate(PriceConditionType.PCT_CHANGE_ABOVE, 5.0, "previous_close", 106.0, "AAPL")

    def test_returns_false_when_pct_change_below_threshold(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=100.0, day_open=102.0)
        # 3% up from previous_close
        assert not ev.evaluate(PriceConditionType.PCT_CHANGE_ABOVE, 5.0, "previous_close", 103.0, "AAPL")

    def test_uses_day_open_reference(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=100.0, day_open=102.0)
        # 6% up from day_open of 102
        assert ev.evaluate(PriceConditionType.PCT_CHANGE_ABOVE, 5.0, "day_open", 108.12, "AAPL")

    def test_returns_false_when_no_reference_available(self):
        ev = ConditionEvaluator()
        # No reference set for AAPL
        assert not ev.evaluate(PriceConditionType.PCT_CHANGE_ABOVE, 5.0, "previous_close", 200.0, "AAPL")

    def test_returns_false_when_reference_price_is_zero(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=0, day_open=0)
        assert not ev.evaluate(PriceConditionType.PCT_CHANGE_ABOVE, 5.0, "previous_close", 100.0, "AAPL")


class TestPctChangeBelow:
    def test_returns_true_when_price_drops_beyond_threshold(self):
        ev = ConditionEvaluator()
        ev.set_reference("TSLA", previous_close=200.0, day_open=198.0)
        # Price at 188 is -6% from previous_close of 200
        assert ev.evaluate(PriceConditionType.PCT_CHANGE_BELOW, 5.0, "previous_close", 188.0, "TSLA")

    def test_returns_false_when_drop_within_threshold(self):
        ev = ConditionEvaluator()
        ev.set_reference("TSLA", previous_close=200.0, day_open=198.0)
        # Price at 196 is -2% from previous_close
        assert not ev.evaluate(PriceConditionType.PCT_CHANGE_BELOW, 5.0, "previous_close", 196.0, "TSLA")

    def test_returns_false_when_price_is_up(self):
        ev = ConditionEvaluator()
        ev.set_reference("TSLA", previous_close=200.0, day_open=198.0)
        # Price at 210 is +5% — not a drop
        assert not ev.evaluate(PriceConditionType.PCT_CHANGE_BELOW, 5.0, "previous_close", 210.0, "TSLA")


class TestSetReference:
    def test_set_and_overwrite_reference(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=100.0, day_open=101.0)
        assert ev._reference_prices["AAPL"]["previous_close"] == 100.0

        ev.set_reference("AAPL", previous_close=150.0, day_open=151.0)
        assert ev._reference_prices["AAPL"]["previous_close"] == 150.0

    def test_multiple_symbols(self):
        ev = ConditionEvaluator()
        ev.set_reference("AAPL", previous_close=100.0, day_open=101.0)
        ev.set_reference("TSLA", previous_close=200.0, day_open=201.0)
        assert ev._reference_prices["AAPL"]["previous_close"] == 100.0
        assert ev._reference_prices["TSLA"]["previous_close"] == 200.0
