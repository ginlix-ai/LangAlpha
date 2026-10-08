"""Unit tests for PriceMonitorService — price monitoring and automation triggering."""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.server.models.automation import MarketType, PriceTriggerConfig
from src.server.services.price_monitor import (
    ConditionEvaluator,
    PriceMonitorService,
    _SERVICE_USER_ID,
    _from_display_symbol,
    _from_ws_symbol,
    _seconds_until_next_market_open,
    _to_ws_symbol,
)
from src.server.services.market_data_feed import MarketDataFeed

ET = ZoneInfo("America/New_York")


def _make_automation(
    symbol="AAPL",
    condition_type="price_below",
    value=150.0,
    reference="previous_close",
    retrigger_mode="one_shot",
    cooldown_seconds=None,
    market="stock",
    **overrides,
):
    """Factory for automation dicts with price trigger_config."""
    auto_id = overrides.pop("automation_id", str(uuid.uuid4()))
    retrigger = {"mode": retrigger_mode}
    if cooldown_seconds is not None:
        retrigger["cooldown_seconds"] = cooldown_seconds
    return {
        "automation_id": auto_id,
        "user_id": "test-user",
        "name": f"Test {symbol} alert",
        "trigger_type": "price",
        "trigger_config": {
            "symbol": symbol,
            "market": market,
            "conditions": [
                {"type": condition_type, "value": value, "reference": reference}
            ],
            "retrigger": retrigger,
        },
        "status": "active",
        "agent_mode": "flash",
        "instruction": "Analyze the price movement",
        "workspace_id": None,
        "cron_expression": None,
        "timezone": "UTC",
        "next_run_at": None,
        "last_run_at": None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "max_failures": 3,
        "failure_count": 0,
        "delivery_config": {},
        "metadata": {},
        **overrides,
    }


class TestSymbolNormalization:
    """Test module-level symbol normalization utilities."""

    def test_to_ws_symbol_stock(self):
        """Stock symbols pass through uppercased."""
        assert _to_ws_symbol("aapl", MarketType.STOCK) == "AAPL"
        assert _to_ws_symbol("TSLA", MarketType.STOCK) == "TSLA"

    def test_to_ws_symbol_index(self):
        """Bare index symbols get I: prefix."""
        assert _to_ws_symbol("SPX", MarketType.INDEX) == "I:SPX"

    def test_to_ws_symbol_index_known(self):
        """Known display symbols use _INDEX_SYMBOL_MAP for mapping."""
        # GSPC is in _INDEX_SYMBOL_MAP and maps to I:SPX
        assert _to_ws_symbol("GSPC", MarketType.INDEX) == "I:SPX"
        # DJI maps to I:DJI
        assert _to_ws_symbol("DJI", MarketType.INDEX) == "I:DJI"
        # IXIC maps to I:COMP
        assert _to_ws_symbol("IXIC", MarketType.INDEX) == "I:COMP"

    def test_to_ws_symbol_index_unknown(self):
        """Unknown index symbols get a generic I: prefix."""
        assert _to_ws_symbol("NYFANG", MarketType.INDEX) == "I:NYFANG"

    def test_from_ws_symbol_strips_prefix(self):
        """Wire format I:SPX becomes bare SPX."""
        assert _from_ws_symbol("I:SPX") == "SPX"
        assert _from_ws_symbol("I:DJI") == "DJI"
        assert _from_ws_symbol("I:COMP") == "COMP"

    def test_from_ws_symbol_stock_passthrough(self):
        """Stock symbols pass through unchanged."""
        assert _from_ws_symbol("AAPL") == "AAPL"
        assert _from_ws_symbol("TSLA") == "TSLA"

    def test_from_display_symbol_known(self):
        """Known display symbols are mapped to bare symbols."""
        assert _from_display_symbol("GSPC") == "SPX"
        assert _from_display_symbol("IXIC") == "COMP"

    def test_from_display_symbol_passthrough(self):
        """Symbols already bare pass through unchanged."""
        assert _from_display_symbol("DJI") == "DJI"
        assert _from_display_symbol("AAPL") == "AAPL"


class TestOnMessage:
    """Test _on_message dispatches to _evaluate_and_trigger correctly."""

    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    @pytest.mark.asyncio
    async def test_evaluates_matching_symbol(self):
        svc = PriceMonitorService()
        auto = _make_automation(symbol="AAPL", condition_type="price_below", value=150.0)
        svc._symbol_automations = {"AAPL": [auto]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            bar = {"symbol": "AAPL", "close": 149.0, "open": 150.0, "high": 150.5, "low": 148.5, "volume": 1000, "time": 1710000000000}
            await svc._on_message('{"ev":"AM"}', bar)
            mock_eval.assert_called_once_with(auto, 149.0)

    @pytest.mark.asyncio
    async def test_skips_unmonitored_symbol(self):
        svc = PriceMonitorService()
        svc._symbol_automations = {"AAPL": [_make_automation()]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            bar = {"symbol": "TSLA", "close": 200.0, "open": 201.0, "high": 202.0, "low": 199.0, "volume": 500, "time": 1710000000000}
            await svc._on_message('{"ev":"AM"}', bar)
            mock_eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_none_bar(self):
        svc = PriceMonitorService()
        svc._symbol_automations = {"AAPL": [_make_automation()]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            await svc._on_message('{"type":"keepalive"}', None)
            mock_eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_normalizes_index_ws_symbol(self):
        """_on_message normalizes I:SPX to SPX for lookup in _symbol_automations."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="SPX", market="index", condition_type="price_below", value=5000.0)
        svc._symbol_automations = {"SPX": [auto]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            bar = {"symbol": "I:SPX", "close": 4900.0, "open": 5000.0, "high": 5010.0, "low": 4890.0, "volume": 0, "time": 1710000000000}
            await svc._on_message('{"ev":"AM"}', bar)
            mock_eval.assert_called_once_with(auto, 4900.0)

    @pytest.mark.asyncio
    async def test_stock_symbol_no_prefix_still_works(self):
        """Stock symbols without I: prefix still match correctly."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="AAPL", market="stock")
        svc._symbol_automations = {"AAPL": [auto]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            bar = {"symbol": "AAPL", "close": 149.0, "open": 150.0, "high": 150.5, "low": 148.5, "volume": 1000, "time": 1710000000000}
            await svc._on_message('{"ev":"AM"}', bar)
            mock_eval.assert_called_once_with(auto, 149.0)

    @pytest.mark.asyncio
    async def test_index_symbol_not_in_automations_skipped(self):
        """An index bar for a symbol not in _symbol_automations is skipped."""
        svc = PriceMonitorService()
        svc._symbol_automations = {"SPX": [_make_automation(symbol="SPX", market="index")]}

        with patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval:
            bar = {"symbol": "I:DJI", "close": 40000.0, "open": 39500.0, "high": 40100.0, "low": 39400.0, "volume": 0, "time": 1710000000000}
            await svc._on_message('{"ev":"AM"}', bar)
            mock_eval.assert_not_called()


class TestEvaluateAndTrigger:
    """Test _evaluate_and_trigger calls _try_trigger only when conditions are met."""

    def setup_method(self):
        PriceMonitorService._instance = None

    @pytest.mark.asyncio
    async def test_triggers_when_condition_met(self):
        svc = PriceMonitorService()
        auto = _make_automation(condition_type="price_below", value=150.0)

        with patch.object(svc, "_try_trigger", new_callable=AsyncMock) as mock_trigger:
            await svc._evaluate_and_trigger(auto, 149.0)
            mock_trigger.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_trigger_when_condition_not_met(self):
        svc = PriceMonitorService()
        auto = _make_automation(condition_type="price_below", value=150.0)

        with patch.object(svc, "_try_trigger", new_callable=AsyncMock) as mock_trigger:
            await svc._evaluate_and_trigger(auto, 155.0)
            mock_trigger.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_invalid_trigger_config(self):
        svc = PriceMonitorService()
        auto = _make_automation()
        auto["trigger_config"] = {"invalid": True}  # missing required fields

        with patch.object(svc, "_try_trigger", new_callable=AsyncMock) as mock_trigger:
            await svc._evaluate_and_trigger(auto, 149.0)
            mock_trigger.assert_not_called()


class TestTryTrigger:
    """Test _try_trigger acquires Redis lock and dispatches execution."""

    def setup_method(self):
        PriceMonitorService._instance = None

    @pytest.mark.asyncio
    async def test_acquires_lock_and_creates_execution(self):
        svc = PriceMonitorService()
        auto = _make_automation(retrigger_mode="one_shot")

        from src.server.models.automation import PriceTriggerConfig
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_redis_client = AsyncMock()
        mock_redis_client.set = AsyncMock(return_value=True)  # lock acquired

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.client = mock_redis_client

        mock_scheduler = MagicMock()
        mock_scheduler.server_id = "test-server"

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value="exec-123") as mock_claim,
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=mock_scheduler),
        ):
            await svc._try_trigger(auto, config, 149.0)

            # Lock was acquired with NX
            mock_redis_client.set.assert_called_once()
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["nx"] is True

            # Claimed 'executing', with its execution, before dispatch
            mock_claim.assert_called_once_with(auto["automation_id"], "test-server")

            # Dispatched through the scheduler, which holds it for shutdown
            mock_scheduler.dispatch.assert_called_once()
            assert mock_scheduler.dispatch.call_args.args == (auto, "exec-123")

    @pytest.mark.asyncio
    async def test_skips_when_lock_not_acquired(self):
        svc = PriceMonitorService()
        auto = _make_automation(retrigger_mode="recurring")

        from src.server.models.automation import PriceTriggerConfig
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_redis_client = AsyncMock()
        mock_redis_client.set = AsyncMock(return_value=False)  # lock NOT acquired

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.client = mock_redis_client

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache):
            await svc._try_trigger(auto, config, 149.0)
            # If lock wasn't acquired, we should have returned early
            # (no exception means success)

    @pytest.mark.asyncio
    async def test_an_alert_paused_since_the_load_is_not_dispatched(self):
        svc = PriceMonitorService()
        auto = _make_automation()
        config = PriceTriggerConfig(**auto["trigger_config"])
        mock_scheduler = MagicMock(server_id="s1")

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=MagicMock(enabled=False, client=None)),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value=None),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=mock_scheduler),
        ):
            await svc._try_trigger(auto, config, 149.0)

        mock_scheduler.dispatch.assert_not_called()
        # Nothing fired, so a resume is heard from the next reload on.
        from src.server.services.price_monitor import _REFRESH_INTERVAL
        held = svc._local_locks[auto["automation_id"]] - time.monotonic()
        assert 0 < held <= _REFRESH_INTERVAL

    @pytest.mark.asyncio
    async def test_falls_back_to_in_memory_lock_when_redis_unavailable(self):
        svc = PriceMonitorService()
        auto = _make_automation()

        from src.server.models.automation import PriceTriggerConfig
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_cache = MagicMock()
        mock_cache.enabled = False
        mock_cache.client = None

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
        ):
            await svc._try_trigger(auto, config, 149.0)
            assert auto["automation_id"] in svc._local_locks


class TestLoadAutomations:
    """Test _load_automations loads from DB and updates subscriptions."""

    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    @pytest.mark.asyncio
    async def test_loads_and_subscribes_stocks(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle

        autos = [
            _make_automation(symbol="AAPL"),
            _make_automation(symbol="TSLA"),
        ]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock):
                await svc._load_automations()

        assert "AAPL" in svc._monitored_symbols
        assert "TSLA" in svc._monitored_symbols
        assert len(svc._symbol_automations["AAPL"]) == 1
        assert len(svc._symbol_automations["TSLA"]) == 1
        mock_stock_handle.subscribe.assert_called_once()
        mock_index_handle.subscribe.assert_not_called()

    @pytest.mark.asyncio
    async def test_loads_and_subscribes_indices(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle

        autos = [
            _make_automation(symbol="SPX", market="index"),
            _make_automation(symbol="DJI", market="index"),
        ]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock) as mock_refresh:
                await svc._load_automations()

        assert "SPX" in svc._monitored_symbols
        assert "DJI" in svc._monitored_symbols
        assert svc._symbol_markets["SPX"] == MarketType.INDEX
        assert svc._symbol_markets["DJI"] == MarketType.INDEX
        # Index symbols subscribe on the index handle
        mock_index_handle.subscribe.assert_called_once()
        mock_stock_handle.subscribe.assert_not_called()
        # WS symbols should be the wire format
        assert "I:SPX" in svc._index_ws_symbols
        assert "I:DJI" in svc._index_ws_symbols
        # refresh_references called with correct symbol_markets
        mock_refresh.assert_called_once()
        call_args = mock_refresh.call_args
        assert set(call_args.args[0]) == {"SPX", "DJI"}
        assert call_args.kwargs["symbol_markets"] == {"SPX": MarketType.INDEX, "DJI": MarketType.INDEX}

    @pytest.mark.asyncio
    async def test_loads_mixed_stock_and_index(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle

        autos = [
            _make_automation(symbol="AAPL", market="stock"),
            _make_automation(symbol="SPX", market="index"),
        ]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock) as mock_refresh:
                await svc._load_automations()

        assert "AAPL" in svc._monitored_symbols
        assert "SPX" in svc._monitored_symbols
        assert svc._symbol_markets["AAPL"] == MarketType.STOCK
        assert svc._symbol_markets["SPX"] == MarketType.INDEX
        mock_stock_handle.subscribe.assert_called_once()
        mock_index_handle.subscribe.assert_called_once()
        assert "AAPL" in svc._stock_ws_symbols
        assert "I:SPX" in svc._index_ws_symbols
        # refresh_references called with both markets
        mock_refresh.assert_called_once()
        call_args = mock_refresh.call_args
        assert set(call_args.args[0]) == {"AAPL", "SPX"}
        assert call_args.kwargs["symbol_markets"] == {"AAPL": MarketType.STOCK, "SPX": MarketType.INDEX}

    @pytest.mark.asyncio
    async def test_removes_stale_stock_subscriptions(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle
        # Pre-populate with AAPL and TSLA as existing stock subscriptions
        svc._stock_ws_symbols = {"AAPL", "TSLA"}
        svc._symbol_automations = {
            "AAPL": [_make_automation(symbol="AAPL")],
            "TSLA": [_make_automation(symbol="TSLA")],
        }
        svc._symbol_markets = {"AAPL": MarketType.STOCK, "TSLA": MarketType.STOCK}

        # Only AAPL remains active
        autos = [_make_automation(symbol="AAPL")]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock):
                await svc._load_automations()

        assert "AAPL" in svc._monitored_symbols
        assert "TSLA" not in svc._monitored_symbols
        mock_stock_handle.unsubscribe.assert_called_once_with(["TSLA"])

    @pytest.mark.asyncio
    async def test_removes_stale_index_subscriptions(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle
        # Pre-populate with SPX and DJI as existing index subscriptions
        svc._index_ws_symbols = {"I:SPX", "I:DJI"}
        svc._symbol_automations = {
            "SPX": [_make_automation(symbol="SPX", market="index")],
            "DJI": [_make_automation(symbol="DJI", market="index")],
        }
        svc._symbol_markets = {"SPX": MarketType.INDEX, "DJI": MarketType.INDEX}

        # Only SPX remains active
        autos = [_make_automation(symbol="SPX", market="index")]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock):
                await svc._load_automations()

        assert "SPX" in svc._monitored_symbols
        assert "DJI" not in svc._monitored_symbols
        mock_index_handle.unsubscribe.assert_called_once_with(["I:DJI"])

    @pytest.mark.asyncio
    async def test_skips_invalid_trigger_config(self):
        svc = PriceMonitorService()
        mock_stock_handle = AsyncMock()
        mock_index_handle = AsyncMock()
        svc._stock_handle = mock_stock_handle
        svc._index_handle = mock_index_handle

        auto = _make_automation(symbol="AAPL")
        auto["trigger_config"] = {"bad": "config"}

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=[auto])):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock):
                await svc._load_automations()

        assert len(svc._monitored_symbols) == 0

    @pytest.mark.asyncio
    async def test_only_us_listings_subscribe_on_the_us_feeds(self):
        """The feeds carry the US tape: a CN stock or the Hang Seng is still
        monitored, by the poll, but never subscribed there."""
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()

        autos = [
            _make_automation(symbol="600519.SH"),
            _make_automation(symbol="HSI", market="index"),
            _make_automation(symbol="BRK.B"),
        ]

        with patch("src.server.database.automation.get_active_price_automations", AsyncMock(return_value=autos)):
            with patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock):
                await svc._load_automations()

        assert svc._monitored_symbols == {"600519.SH", "HSI", "BRK.B"}
        assert svc._stock_ws_symbols == {"BRK.B"}
        assert svc._index_ws_symbols == set()
        svc._stock_handle.subscribe.assert_called_once_with(["BRK.B"])
        svc._index_handle.subscribe.assert_not_called()


class TestPollSnapshots:
    """Test _poll_snapshots REST fallback path."""

    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    @pytest.mark.asyncio
    async def test_stock_snapshot_evaluates(self):
        """Stock snapshots are looked up by bare symbol and trigger evaluation."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="AAPL", condition_type="price_below", value=150.0)
        svc._symbol_automations = {"AAPL": [auto]}
        svc._symbol_markets = {"AAPL": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 149.0, "previous_close": 151.0, "open": 150.5},
        ])

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)

        mock_provider.get_snapshots.assert_called_once_with(
            ["AAPL"], asset_type="stocks", user_id="langalpha-service"
        )
        mock_eval.assert_called_once_with(auto, 149.0)

    @pytest.mark.asyncio
    async def test_index_snapshot_normalizes_display_to_bare(self):
        """Index snapshots normalize display symbols (GSPC→SPX) before lookup."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="SPX", market="index", condition_type="price_above", value=5000.0)
        svc._symbol_automations = {"SPX": [auto]}
        svc._symbol_markets = {"SPX": MarketType.INDEX}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "GSPC", "price": 5100.0, "previous_close": 5050.0, "open": 5060.0},
        ])

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=True)

        mock_provider.get_snapshots.assert_called_once_with(
            ["SPX"], asset_type="indices", user_id="langalpha-service"
        )
        mock_eval.assert_called_once_with(auto, 5100.0)

    @pytest.mark.asyncio
    async def test_skips_zero_price(self):
        """Snapshots with price <= 0 are skipped."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="AAPL")
        svc._symbol_automations = {"AAPL": [auto]}
        svc._symbol_markets = {"AAPL": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 0, "previous_close": 151.0},
        ])

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)

        mock_eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_null_price_row_does_not_abort_the_pass(self):
        """A row with data but a null price is skipped alone: the rows after it
        are still evaluated, and its null reference fails no compare."""
        svc = PriceMonitorService()
        aapl = _make_automation(symbol="AAPL", condition_type="pct_change_below", value=1.0)
        tsla = _make_automation(symbol="TSLA", condition_type="price_below", value=300.0)
        svc._symbol_automations = {"AAPL": [aapl], "TSLA": [tsla]}
        svc._symbol_markets = {"AAPL": MarketType.STOCK, "TSLA": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": None, "previous_close": None, "volume": 1200},
            {"symbol": "TSLA", "price": 250.0, "previous_close": 260.0, "open": None},
        ])

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_try_trigger", new_callable=AsyncMock) as mock_trigger,
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)

        assert [c.args[0] for c in mock_trigger.call_args_list] == [tsla]
        assert svc._evaluator._reference_prices["TSLA"] == {"previous_close": 260.0, "day_open": 0.0}

    @pytest.mark.asyncio
    async def test_null_references_read_as_missing(self):
        """refresh_references stores a null previous_close or open as no
        reference, so a pct_change alert stays quiet instead of raising."""
        evaluator = ConditionEvaluator()
        provider = AsyncMock(get_snapshots=AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 150.0, "previous_close": None, "open": None},
        ]))
        with patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)):
            await evaluator.refresh_references(["AAPL"], symbol_markets={"AAPL": MarketType.STOCK})

        assert evaluator._reference_prices["AAPL"] == {"previous_close": 0.0, "day_open": 0.0}
        assert evaluator.evaluate("pct_change_above", 1.0, "previous_close", 150.0, "AAPL") is False

    @pytest.mark.asyncio
    async def test_a_failed_stock_fetch_keeps_the_index_references(self):
        """The stock and index calls are independent, so one failing must not
        cost the other its reference prices."""
        evaluator = ConditionEvaluator()

        async def _snapshots(symbols, asset_type, user_id):
            if asset_type == "stocks":
                raise RuntimeError("upstream down")
            return [{"symbol": "GSPC", "previous_close": 5050.0, "open": 5060.0}]

        provider = AsyncMock(get_snapshots=AsyncMock(side_effect=_snapshots))
        with patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)):
            await evaluator.refresh_references(
                ["AAPL", "SPX"],
                symbol_markets={"AAPL": MarketType.STOCK, "SPX": MarketType.INDEX},
            )

        assert evaluator._reference_prices == {"SPX": {"previous_close": 5050.0, "day_open": 5060.0}}


class TestSnapshotAuthAttribution:
    """Regression: background REST snapshot calls must carry the service-account
    X-User-Id. ginlix-data rejects service-token calls without a non-empty user_id
    (401), which silently breaks reference prices and the poll fallback. See
    market_data_feed.py for the matching WS-path principal.
    """

    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    def test_service_user_id_matches_ws_path(self):
        """The REST principal must equal the WS principal so both attribute alike."""
        assert _SERVICE_USER_ID == "langalpha-service"

    @pytest.mark.asyncio
    async def test_refresh_references_passes_service_user_id(self):
        """ConditionEvaluator.refresh_references attributes both markets to the service account."""
        evaluator = ConditionEvaluator()
        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[])

        with patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)):
            await evaluator.refresh_references(
                ["AAPL", "SPX"],
                symbol_markets={"AAPL": MarketType.STOCK, "SPX": MarketType.INDEX},
            )

        for call in mock_provider.get_snapshots.call_args_list:
            assert call.kwargs["user_id"] == _SERVICE_USER_ID
        asset_types = {c.kwargs["asset_type"] for c in mock_provider.get_snapshots.call_args_list}
        assert asset_types == {"stocks", "indices"}

    @pytest.mark.asyncio
    async def test_poll_snapshots_passes_service_user_id(self):
        """_poll_snapshots attributes both markets to the service account."""
        svc = PriceMonitorService()
        svc._symbol_automations = {
            "AAPL": [_make_automation(symbol="AAPL", market="stock")],
            "SPX": [_make_automation(symbol="SPX", market="index")],
        }
        svc._symbol_markets = {"AAPL": MarketType.STOCK, "SPX": MarketType.INDEX}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[])

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock),
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=True)

        assert mock_provider.get_snapshots.call_count == 2
        for call in mock_provider.get_snapshots.call_args_list:
            assert call.kwargs["user_id"] == _SERVICE_USER_ID


class TestSecondsUntilNextMarketOpen:
    """A recurring lock runs to the next XNYS session open: the session in
    progress, weekends and holidays are skipped, a morning before pre-market
    is not."""

    @pytest.mark.parametrize("now, next_open", [
        # Mid-session Monday: Tuesday's open.
        (datetime(2026, 3, 16, 14, 0, tzinfo=ET), datetime(2026, 3, 17, 9, 30, tzinfo=ET)),
        (datetime(2026, 3, 17, 10, 0, tzinfo=ET), datetime(2026, 3, 18, 9, 30, tzinfo=ET)),
        # Friday afternoon and Saturday: Monday's open.
        (datetime(2026, 3, 20, 15, 0, tzinfo=ET), datetime(2026, 3, 23, 9, 30, tzinfo=ET)),
        (datetime(2026, 3, 21, 10, 0, tzinfo=ET), datetime(2026, 3, 23, 9, 30, tzinfo=ET)),
        # Pre-market is part of today's session: tomorrow's open.
        (datetime(2026, 3, 17, 9, 29, 50, tzinfo=ET), datetime(2026, 3, 18, 9, 30, tzinfo=ET)),
        # Before pre-market, today's session is still ahead.
        (datetime(2026, 3, 17, 3, 0, tzinfo=ET), datetime(2026, 3, 17, 9, 30, tzinfo=ET)),
        # Thanksgiving: Wednesday's lock runs to Friday.
        (datetime(2026, 11, 25, 14, 0, tzinfo=ET), datetime(2026, 11, 27, 9, 30, tzinfo=ET)),
    ])
    def test_runs_to_the_next_session_open(self, now, next_open):
        expected = int((next_open - now).total_seconds())
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            assert _seconds_until_next_market_open() == expected
            assert _seconds_until_next_market_open("AAPL") == expected


class TestTryTriggerLockTTL:
    """Test that lock TTL matches the new retrigger strategy."""

    def setup_method(self):
        PriceMonitorService._instance = None

    @pytest.mark.asyncio
    async def test_one_shot_uses_short_dedup_ttl(self):
        """one_shot: 300s dedup lock — must exceed refresh interval to prevent re-trigger."""
        svc = PriceMonitorService()
        auto = _make_automation(retrigger_mode="one_shot")
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_redis_client = AsyncMock()
        mock_redis_client.set = AsyncMock(return_value=True)
        mock_cache = MagicMock(enabled=True, client=mock_redis_client)

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
        ):
            await svc._try_trigger(auto, config, 149.0)
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["ex"] == 300

    @pytest.mark.asyncio
    async def test_recurring_no_cooldown_uses_trading_day(self):
        """recurring with no cooldown_seconds: lock until next market open."""
        svc = PriceMonitorService()
        auto = _make_automation(retrigger_mode="recurring")
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_redis_client = AsyncMock()
        mock_redis_client.set = AsyncMock(return_value=True)
        mock_cache = MagicMock(enabled=True, client=mock_redis_client)

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.services.price_monitor._seconds_until_next_market_open", return_value=70200),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
        ):
            await svc._try_trigger(auto, config, 149.0)
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["ex"] == 70200

    @pytest.mark.asyncio
    async def test_recurring_with_explicit_cooldown(self):
        """recurring with explicit cooldown_seconds: use that value."""
        svc = PriceMonitorService()
        auto = _make_automation(retrigger_mode="recurring", cooldown_seconds=14400)
        config = PriceTriggerConfig(**auto["trigger_config"])

        mock_redis_client = AsyncMock()
        mock_redis_client.set = AsyncMock(return_value=True)
        mock_cache = MagicMock(enabled=True, client=mock_redis_client)

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.database.automation.claim_price_firing", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
        ):
            await svc._try_trigger(auto, config, 149.0)
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["ex"] == 14400


SH = ZoneInfo("Asia/Shanghai")
HK = ZoneInfo("Asia/Hong_Kong")
# Wednesday 2026-09-09: both venues trade it.
CN_SESSION = datetime(2026, 9, 9, 10, 0, tzinfo=SH)
HK_SESSION = datetime(2026, 9, 9, 10, 0, tzinfo=HK)


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def _at(now: datetime):
    """Pin the monitor's clock, which the session gate reads."""
    return patch(
        "src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)
    )


class TestNonUsSymbolsPollInSession:
    """The live-data WebSocket carries US venues only, so a CN/HK automation
    has no tick source unless the REST poll runs for it, which it does while
    the venue's regular session runs.
    """

    def setup_method(self):
        PriceMonitorService._instance = None

    @pytest.mark.parametrize("symbol, market, us", [
        ("AAPL", MarketType.STOCK, True),
        # A dotted class share parses to no known venue, but trades on the US tape.
        ("BRK.B", MarketType.STOCK, True),
        ("BF.B", MarketType.STOCK, True),
        # Any other suffix the protocol does not know is a foreign venue, polled.
        ("NOVO-B.CO", MarketType.STOCK, False),
        ("PETR4.SA", MarketType.STOCK, False),
        ("600519.SS", MarketType.STOCK, False),
        ("0700.HK", MarketType.STOCK, False),
        ("VOD.L", MarketType.STOCK, False),
        ("SPX", MarketType.INDEX, True),
        # A bare foreign index family is the index the alert names, not a US ticker.
        ("HSI", MarketType.INDEX, False),
        ("N225", MarketType.INDEX, False),
        ("STOXX50E", MarketType.INDEX, False),
        ("000300.SH", MarketType.INDEX, False),
    ])
    def test_is_us_symbol_classification(self, symbol, market, us):
        from src.server.services.price_monitor import _is_us_symbol

        assert _is_us_symbol(symbol, market) is us

    @pytest.mark.asyncio
    async def test_poll_snapshots_includes_non_us_when_ws_is_healthy(self):
        """poll_stock=False models a connected WS; the CN symbol is polled
        anyway, the US symbol is not."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="600519.SS", condition_type="price_above", value=1000.0)
        svc._symbol_automations = {"600519.SH": [auto], "AAPL": [_make_automation()]}
        svc._symbol_markets = {"600519.SH": MarketType.STOCK, "AAPL": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "600519.SH", "price": 1712.4, "previous_close": 1700.0, "as_of": _ms(CN_SESSION)},
        ])
        triggered = []

        with (
            _at(CN_SESSION),
            patch("src.data_client.get_market_data_provider", new_callable=AsyncMock, return_value=mock_provider),
            patch.object(svc, "_try_trigger", new=AsyncMock(side_effect=lambda a, c, p: triggered.append(c.symbol))),
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        assert mock_provider.get_snapshots.call_args.args[0] == ["600519.SH"]
        assert triggered == ["600519.SH"]

    @pytest.mark.asyncio
    async def test_an_hsi_index_alert_is_polled_with_the_feeds_up(self):
        """HSI on an index alert is the Hang Seng: the WS never carries it, so
        the poll must, though a bare spelling reads as a US ticker elsewhere."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="HSI", market="index", condition_type="price_above", value=25000.0)
        svc._symbol_automations = {"HSI": [auto]}
        svc._symbol_markets = {"HSI": MarketType.INDEX}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "HSI", "price": 25400.0, "previous_close": 25100.0, "as_of": _ms(HK_SESSION)},
        ])

        with (
            _at(HK_SESSION),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        mock_provider.get_snapshots.assert_called_once_with(
            ["HSI"], asset_type="indices", user_id=_SERVICE_USER_ID
        )
        mock_eval.assert_called_once_with(auto, 25400.0)

    @pytest.mark.asyncio
    async def test_a_calendar_error_skips_only_its_own_listing(self):
        """One venue's calendar raising must not abort the pass: every other
        venue's alerts are still polled and evaluated."""
        import src.server.services.price_monitor as pm

        svc = PriceMonitorService()
        cn = _make_automation(symbol="600519.SH", condition_type="price_above", value=1000.0)
        hk = _make_automation(symbol="0700.HK", condition_type="price_above", value=300.0)
        svc._symbol_automations = {"600519.SH": [cn], "0700.HK": [hk]}
        svc._symbol_markets = {"600519.SH": MarketType.STOCK, "0700.HK": MarketType.STOCK}

        real_clock_for_ref = pm.clock_for_ref

        def _clock(ref):
            if ref.calendar_id == "XSHG":
                raise ValueError("calendar out of range")
            return real_clock_for_ref(ref)

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "0700.HK", "price": 512.0, "previous_close": 505.0, "as_of": _ms(HK_SESSION)},
        ])

        with (
            _at(HK_SESSION),
            patch.object(pm, "clock_for_ref", side_effect=_clock),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        mock_provider.get_snapshots.assert_called_once_with(
            ["0700.HK"], asset_type="stocks", user_id=_SERVICE_USER_ID
        )
        mock_eval.assert_called_once_with(hk, 512.0)

    @pytest.mark.parametrize("now", [
        datetime(2026, 9, 9, 16, 0, tzinfo=SH),  # after the close
        datetime(2026, 9, 9, 12, 0, tzinfo=SH),  # lunch break: the price is frozen
        datetime(2026, 9, 9, 9, 20, tzinfo=SH),  # opening auction, before the open
        datetime(2026, 9, 12, 10, 0, tzinfo=SH),  # Saturday
    ], ids=["after-close", "lunch", "pre-open", "weekend"])
    @pytest.mark.asyncio
    async def test_a_cn_symbol_is_not_polled_while_xshg_is_closed(self, now):
        """Outside the session the quote cannot move, so neither an upstream
        call nor an evaluation is spent on it."""
        svc = PriceMonitorService()
        svc._symbol_automations = {"600519.SH": [_make_automation(symbol="600519.SH")]}
        svc._symbol_markets = {"600519.SH": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "600519.SH", "price": 1712.4, "previous_close": 1700.0},
        ])

        with (
            _at(now),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        mock_provider.get_snapshots.assert_not_called()
        mock_eval.assert_not_called()

    @pytest.mark.parametrize("as_of, evaluated", [
        # Yesterday's close: the vendor has not rolled the row to today.
        (datetime(2026, 9, 8, 15, 0, tzinfo=SH), False),
        # Today's opening auction prints before the open and is today's price.
        (datetime(2026, 9, 9, 9, 25, tzinfo=SH), True),
        (datetime(2026, 9, 9, 9, 30, 5, tzinfo=SH), True),
        # No print time: nothing shows the row rolled to today.
        (None, False),
    ], ids=["previous-session", "opening-auction", "first-trade", "no-print-time"])
    @pytest.mark.asyncio
    async def test_a_row_printed_before_the_session_day_is_skipped(self, as_of, evaluated):
        """At the open a recurring alert's lock has just expired; a row that
        still holds yesterday's move, or cannot show it does not, must not fire it."""
        svc = PriceMonitorService()
        auto = _make_automation(
            symbol="600519.SH", condition_type="pct_change_above", value=2.0,
            retrigger_mode="recurring",
        )
        svc._symbol_automations = {"600519.SH": [auto]}
        svc._symbol_markets = {"600519.SH": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "600519.SH", "price": 1750.0, "previous_close": 1700.0,
            "open": 1705.0, "as_of": None if as_of is None else _ms(as_of),
        }])

        with (
            _at(datetime(2026, 9, 9, 9, 31, tzinfo=SH)),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        assert mock_eval.called is evaluated
        assert ("600519.SH" in svc._evaluator._reference_prices) is evaluated

    @pytest.mark.asyncio
    async def test_a_fallback_vendors_spelling_reaches_the_alert(self):
        """FMP answers Shanghai as .SS; the alert is keyed on .SH."""
        svc = PriceMonitorService()
        auto = _make_automation(symbol="600519.SH", condition_type="price_above", value=1000.0)
        svc._symbol_automations = {"600519.SH": [auto]}
        svc._symbol_markets = {"600519.SH": MarketType.STOCK}

        mock_provider = AsyncMock()
        mock_provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "600519.SS", "price": 1712.4, "previous_close": 1700.0, "as_of": _ms(CN_SESSION)},
        ])

        with (
            _at(CN_SESSION),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=mock_provider)),
            patch.object(svc, "_evaluate_and_trigger", new_callable=AsyncMock) as mock_eval,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        mock_eval.assert_called_once_with(auto, 1712.4)

    @pytest.mark.asyncio
    async def test_poll_loop_runs_for_non_us_with_both_feeds_connected(self, monkeypatch):
        import src.server.services.price_monitor as pm

        monkeypatch.setattr(pm, "_POLL_INTERVAL", 0.01)
        svc = PriceMonitorService()
        svc._symbol_automations = {"600519.SS": [_make_automation(symbol="600519.SS")]}
        svc._symbol_markets = {"600519.SS": MarketType.STOCK}
        svc._stock_ws = MagicMock(is_connected=True)
        svc._index_ws = MagicMock(is_connected=True)

        polled = []

        async def _poll(poll_stock, poll_index):
            polled.append((poll_stock, poll_index))
            svc._shutdown_event.set()

        with patch.object(svc, "_poll_snapshots", new=_poll):
            await asyncio.wait_for(svc._poll_fallback_loop(), timeout=2)

        # Both feeds healthy, yet the poll still ran: it decides per symbol.
        assert polled == [(False, False)]

    @pytest.mark.parametrize("symbol, market, next_open", [
        # A CN automation waits for Shanghai to reopen, not New York.
        ("600519.SH", MarketType.STOCK, datetime(2026, 9, 10, 9, 30, tzinfo=SH)),
        # HSI on an index alert is the Hang Seng; on a stock alert it is the
        # US ticker, which reopens in New York that morning.
        ("HSI", MarketType.INDEX, datetime(2026, 9, 10, 9, 30, tzinfo=HK)),
        ("HSI", MarketType.STOCK, datetime(2026, 9, 9, 9, 30, tzinfo=ET)),
    ])
    def test_cooldown_uses_the_alerts_own_listing(self, symbol, market, next_open):
        at = datetime(2026, 9, 9, 14, 0, tzinfo=SH)  # Shanghai and HK trading, 02:00 in New York
        with _at(at):
            assert _seconds_until_next_market_open(symbol, market) == int((next_open - at).total_seconds())

    def test_cn_lock_during_the_session_runs_to_the_next_open(self):
        """Mid-session Shanghai: the lock reaches tomorrow's 09:30, not the floor."""
        from src.data_client.instrument_clock import clock_for

        at = datetime(2024, 1, 10, 2, 0, tzinfo=timezone.utc)  # Wed 10:00 Shanghai, open
        secs = clock_for("600519.SS").seconds_until_next_session_open(at)
        assert 3600 * 20 < secs <= 3600 * 24
        # Friday session → Monday's open.
        at = datetime(2024, 1, 12, 2, 0, tzinfo=timezone.utc)
        assert clock_for("600519.SS").seconds_until_next_session_open(at) > 3600 * 48
