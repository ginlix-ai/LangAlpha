"""Unit tests for PriceMonitorService — price monitoring and automation triggering."""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.server.models.automation import MarketType, PriceConditionType, PriceTriggerConfig, RetriggerMode
from src.server.services.price_monitor import (
    PriceMonitorService,
    _from_display_symbol,
    _from_ws_symbol,
    _seconds_until_next_market_open,
    _to_ws_symbol,
)
from src.server.services.shared_ws_manager import SharedWSConnectionManager

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
        SharedWSConnectionManager._instances.clear()

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

        mock_executor = AsyncMock()

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=mock_cache),
            patch("src.server.database.automation.create_execution", new_callable=AsyncMock, return_value="exec-123") as mock_create_exec,
            patch("src.server.database.automation.update_automation_next_run", new_callable=AsyncMock) as mock_update,
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=mock_scheduler),
            patch("src.server.services.automation_executor.AutomationExecutor.get_instance", return_value=mock_executor),
        ):
            await svc._try_trigger(auto, config, 149.0)

            # Lock was acquired with NX
            mock_redis_client.set.assert_called_once()
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["nx"] is True

            # Execution was created
            mock_create_exec.assert_called_once()

            # Status was set to 'executing' before dispatch
            mock_update.assert_called_once_with(
                auto["automation_id"], next_run_at=None, status="executing",
            )

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
            patch("src.server.database.automation.create_execution", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.database.automation.update_automation_next_run", new_callable=AsyncMock),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
            patch("src.server.services.automation_executor.AutomationExecutor.get_instance", return_value=AsyncMock()),
        ):
            await svc._try_trigger(auto, config, 149.0)
            assert auto["automation_id"] in svc._local_locks


class TestLoadAutomations:
    """Test _load_automations loads from DB and updates subscriptions."""

    def setup_method(self):
        PriceMonitorService._instance = None
        SharedWSConnectionManager._instances.clear()

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


class TestPollSnapshots:
    """Test _poll_snapshots REST fallback path."""

    def setup_method(self):
        PriceMonitorService._instance = None
        SharedWSConnectionManager._instances.clear()

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

        mock_provider.get_snapshots.assert_called_once_with(["AAPL"], asset_type="stocks")
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

        mock_provider.get_snapshots.assert_called_once_with(["SPX"], asset_type="indices")
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


class TestSecondsUntilNextMarketOpen:
    """Test _seconds_until_next_market_open returns correct TTL."""

    def test_monday_2pm_et_returns_next_morning(self):
        now = datetime(2026, 3, 16, 14, 0, tzinfo=ET)
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            result = _seconds_until_next_market_open()
        assert result == 70200

    def test_friday_3pm_et_returns_monday_morning(self):
        now = datetime(2026, 3, 20, 15, 0, tzinfo=ET)
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            result = _seconds_until_next_market_open()
        assert result == 239400

    def test_saturday_returns_monday_morning(self):
        now = datetime(2026, 3, 21, 10, 0, tzinfo=ET)
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            result = _seconds_until_next_market_open()
        assert result == 171000

    def test_tuesday_10am_et_returns_next_morning(self):
        now = datetime(2026, 3, 17, 10, 0, tzinfo=ET)
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            result = _seconds_until_next_market_open()
        assert result == 84600

    def test_just_before_open_returns_next_day(self):
        now = datetime(2026, 3, 17, 9, 29, 50, tzinfo=ET)
        with patch("src.server.services.price_monitor._now_utc", return_value=now.astimezone(timezone.utc)):
            result = _seconds_until_next_market_open()
        assert result == 86410


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
            patch("src.server.database.automation.create_execution", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.database.automation.update_automation_next_run", new_callable=AsyncMock),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
            patch("src.server.services.automation_executor.AutomationExecutor.get_instance", return_value=AsyncMock()),
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
            patch("src.server.database.automation.create_execution", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.database.automation.update_automation_next_run", new_callable=AsyncMock),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
            patch("src.server.services.automation_executor.AutomationExecutor.get_instance", return_value=AsyncMock()),
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
            patch("src.server.database.automation.create_execution", new_callable=AsyncMock, return_value="exec-1"),
            patch("src.server.database.automation.update_automation_next_run", new_callable=AsyncMock),
            patch("src.server.services.automation_scheduler.AutomationScheduler.get_instance", return_value=MagicMock(server_id="s1")),
            patch("src.server.services.automation_executor.AutomationExecutor.get_instance", return_value=AsyncMock()),
        ):
            await svc._try_trigger(auto, config, 149.0)
            call_kwargs = mock_redis_client.set.call_args
            assert call_kwargs.kwargs["ex"] == 14400
