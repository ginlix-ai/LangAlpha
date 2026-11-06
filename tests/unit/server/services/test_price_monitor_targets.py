"""Unit tests for market-watch targets in PriceMonitorService: evaluation,
the database claim on a hit, loading, the sweep and the change nudge."""

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pytest

import src.server.services.price_monitor as pm
from src.server.models.automation import MarketType, PriceConditionType
from src.server.services.market_data_feed import MarketDataFeed
from src.server.services.price_monitor import ConditionEvaluator, PriceMonitorService

_DB = "src.server.database.market_targets"
_NUDGE = "src.server.services.price_monitor._nudge_hook_drainer"
_AAPL = ("AAPL", MarketType.STOCK)
_SPX = ("SPX", MarketType.INDEX)


def _target(
    symbol="AAPL",
    kind="above",
    value=200.0,
    pct_from=None,
    armed_price=None,
    market="stock",
    expires_in=3600,
    **overrides,
):
    """A row as list_armed_targets returns it."""
    now = datetime.now(timezone.utc)
    return {
        "target_id": str(uuid.uuid4()),
        "conversation_thread_id": str(uuid.uuid4()),
        "user_id": "test-user",
        "symbol": symbol,
        "market": market,
        "kind": kind,
        "value": value,
        "pct_from": pct_from,
        "armed_price": armed_price,
        "status": "armed",
        "expires_at": now + timedelta(seconds=expires_in),
        "warn_at": now + timedelta(seconds=expires_in - 60),
        "warned_at": None,
        "fired_at": None,
        "fired_price": None,
        "created_at": now,
        "updated_at": now,
        **overrides,
    }


@pytest.fixture(autouse=True)
def _no_price_wakeups():
    """The monitor reads price wakeups beside targets; these tests hold none."""
    with patch(
        "src.server.database.wakeups.read.list_armed_price_wakeups",
        AsyncMock(return_value=[]),
    ):
        yield


def _now_ms():
    return int(time.time() * 1000)


def _bar(symbol, close, time_ms=None):
    """A tick printed now unless *time_ms* says when."""
    return {"symbol": symbol, "close": close, "open": close, "high": close,
            "low": close, "volume": 0,
            "time": _now_ms() if time_ms is None else time_ms}


def _ms_datetime(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def _load(svc, rows, thread_ids=None):
    with (
        patch(f"{_DB}.list_armed_targets", AsyncMock(return_value=rows)) as listed,
        patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock),
    ):
        await svc._load_targets(thread_ids)
    return listed


class TestEvaluateTarget:
    @pytest.mark.parametrize("kind, value, price, hit", [
        ("above", 200.0, 200.01, True),
        ("above", 200.0, 200.0, True),  # a touch, where an automation needs a cross
        ("above", 200.0, 199.99, False),
        ("below", 150.0, 149.99, True),
        ("below", 150.0, 150.0, True),
        ("below", 150.0, 150.01, False),
    ])
    def test_levels(self, kind, value, price, hit):
        ev = ConditionEvaluator()
        assert ev.evaluate_target(_target(kind=kind, value=value), price, _AAPL) is hit

    @pytest.mark.parametrize("kind, price, hit", [
        ("pct_up", 103.5, True),
        ("pct_up", 103.0, True),
        ("pct_up", 102.9, False),
        ("pct_down", 96.9, True),
        ("pct_down", 97.0, True),
        ("pct_down", 97.5, False),
    ])
    def test_pct_from_previous_close(self, kind, price, hit):
        ev = ConditionEvaluator()
        ev.set_reference(_AAPL, previous_close=100.0, day_open=101.0)
        target = _target(kind=kind, value=3.0, pct_from="previous_close")
        assert ev.evaluate_target(target, price, _AAPL) is hit

    def test_pct_from_armed_measures_from_the_armed_price(self):
        ev = ConditionEvaluator()
        # Down 48% on the day, yet up 4% since the target was set.
        ev.set_reference(_AAPL, previous_close=100.0, day_open=100.0)
        target = _target(kind="pct_up", value=3.0, pct_from="armed", armed_price=50.0)
        assert ev.evaluate_target(target, 52.0, _AAPL)
        assert not ev.evaluate_target(
            _target(kind="pct_down", value=3.0, pct_from="armed", armed_price=50.0),
            52.0, _AAPL,
        )

    def test_no_reference_never_fires(self):
        ev = ConditionEvaluator()
        assert not ev.evaluate_target(
            _target(kind="pct_down", value=3.0, pct_from="previous_close"), 1.0, _AAPL
        )
        assert not ev.evaluate_target(
            _target(kind="pct_up", value=3.0, pct_from="armed", armed_price=None),
            1000.0, _AAPL,
        )

    def test_an_automation_still_needs_a_cross(self):
        ev = ConditionEvaluator()
        ev.set_reference(_AAPL, previous_close=100.0, day_open=100.0)
        assert not ev.evaluate(
            PriceConditionType.PRICE_ABOVE, 200.0, "previous_close", 200.0, _AAPL
        )
        assert not ev.evaluate(
            PriceConditionType.PCT_CHANGE_ABOVE, 3.0, "previous_close", 103.0, _AAPL
        )


class TestTargetFire:
    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    @pytest.mark.asyncio
    async def test_a_hit_is_claimed_once_and_leaves_memory(self):
        svc = PriceMonitorService()
        target = _target(kind="above", value=200.0)
        await _load(svc, [target])
        fired_row = {**target, "status": "fired", "fired_price": 201.0}

        printed = _now_ms()
        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[fired_row])) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._on_message("{}", _bar("AAPL", 201.0, printed))
            await svc._on_message("{}", _bar("AAPL", 202.0))

        # Stamped with when the price traded, not when the claim ran.
        fire.assert_awaited_once_with(
            [target["target_id"]], 201.0, printed_at=_ms_datetime(printed)
        )
        nudge.assert_called_once()
        assert _AAPL not in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_a_lost_race_drops_the_target_without_a_nudge(self):
        svc = PriceMonitorService()
        await _load(svc, [_target(kind="above", value=200.0)])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._on_message("{}", _bar("AAPL", 201.0))
            await svc._on_message("{}", _bar("AAPL", 202.0))

        fire.assert_awaited_once()
        nudge.assert_not_called()
        assert _AAPL not in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_a_price_crossing_several_targets_claims_them_in_one_call(self):
        """The feed waits on the claim, so one tick is one transaction
        however many targets it crosses."""
        svc = PriceMonitorService()
        first = _target(kind="above", value=200.0)
        second = _target(kind="above", value=190.0)
        missed = _target(kind="below", value=150.0)
        await _load(svc, [first, second, missed])
        won = [{**first, "status": "fired"}, {**second, "status": "fired"}]

        printed = _now_ms()
        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=won)) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._on_message("{}", _bar("AAPL", 205.0, printed))

        fire.assert_awaited_once_with(
            [first["target_id"], second["target_id"]], 205.0,
            printed_at=_ms_datetime(printed),
        )
        nudge.assert_called_once()
        assert [t["target_id"] for t in svc._symbol_targets[_AAPL]] == [missed["target_id"]]

    @pytest.mark.asyncio
    async def test_a_batch_partly_lost_drops_every_hit_and_nudges_once(self):
        svc = PriceMonitorService()
        won = _target(kind="above", value=200.0)
        lost = _target(kind="above", value=190.0)
        await _load(svc, [won, lost])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**won}])) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._on_message("{}", _bar("AAPL", 205.0))
            await svc._on_message("{}", _bar("AAPL", 206.0))

        fire.assert_awaited_once()
        nudge.assert_called_once()
        assert _AAPL not in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_a_target_that_fails_to_evaluate_leaves_the_others_claimed(self):
        svc = PriceMonitorService()
        broken = _target(kind="above", value=200.0)
        good = _target(kind="above", value=190.0)
        await _load(svc, [broken, good])
        real = svc._evaluator.evaluate_target

        def _evaluate(target, *args):
            if target["target_id"] == broken["target_id"]:
                raise RuntimeError("bad row")
            return real(target, *args)

        with (
            patch.object(svc._evaluator, "evaluate_target", side_effect=_evaluate),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**good}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("AAPL", 205.0))

        fire.assert_awaited_once_with([good["target_id"]], 205.0, printed_at=ANY)
        assert [t["target_id"] for t in svc._symbol_targets[_AAPL]] == [broken["target_id"]]

    @pytest.mark.asyncio
    async def test_a_failed_claim_drops_until_the_next_load(self):
        svc = PriceMonitorService()
        target = _target(kind="above", value=200.0)
        await _load(svc, [target])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(side_effect=RuntimeError("db down"))) as fire,
            patch(_NUDGE) as nudge,
            patch.object(svc, "request_targets_reload"),
        ):
            await svc._on_message("{}", _bar("AAPL", 201.0))
            await svc._on_message("{}", _bar("AAPL", 202.0))

        fire.assert_awaited_once()
        nudge.assert_not_called()
        # Still armed in the table, so the next load brings it back.
        await _load(svc, [target])
        assert [t["target_id"] for t in svc._symbol_targets[_AAPL]] == [target["target_id"]]

    @pytest.mark.asyncio
    async def test_a_failed_claim_asks_for_its_threads_to_be_re_read(self):
        """A claim can fail on a deadlock with a thread delete's cascade; the
        batch's targets still armed come back after the debounce, not at the
        timed reload."""
        svc = PriceMonitorService()
        thread = str(uuid.uuid4())
        first = _target(kind="above", value=200.0, conversation_thread_id=thread)
        second = _target(kind="above", value=190.0, conversation_thread_id=thread)
        other = _target(kind="above", value=180.0)
        await _load(svc, [first, second, other])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(side_effect=RuntimeError("deadlock"))),
            patch(_NUDGE),
            patch.object(svc, "request_targets_reload") as reload,
        ):
            await svc._on_message("{}", _bar("AAPL", 205.0))

        assert sorted(c.args[0] for c in reload.call_args_list) == sorted(
            [thread, other["conversation_thread_id"]]
        )
        assert _AAPL not in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_a_claim_that_ran_asks_for_no_re_read(self):
        svc = PriceMonitorService()
        target = _target(kind="above", value=200.0)
        await _load(svc, [target])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])),
            patch(_NUDGE),
            patch.object(svc, "request_targets_reload") as reload,
        ):
            await svc._on_message("{}", _bar("AAPL", 201.0))

        reload.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_miss_keeps_the_target_and_other_targets_on_the_symbol(self):
        svc = PriceMonitorService()
        above = _target(kind="above", value=200.0)
        below = _target(kind="below", value=150.0)
        await _load(svc, [above, below])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**below}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("AAPL", 180.0))
            fire.assert_not_awaited()
            await svc._on_message("{}", _bar("AAPL", 149.0))

        fire.assert_awaited_once_with([below["target_id"]], 149.0, printed_at=ANY)
        assert [t["target_id"] for t in svc._symbol_targets[_AAPL]] == [above["target_id"]]

    @pytest.mark.asyncio
    async def test_an_index_tick_fires_an_index_target(self):
        svc = PriceMonitorService()
        target = _target(symbol="SPX", market="index", kind="below", value=5000.0)
        await _load(svc, [target])

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("I:SPX", 4900.0))

        fire.assert_awaited_once_with([target["target_id"]], 4900.0, printed_at=ANY)

    @pytest.mark.asyncio
    async def test_a_tick_printed_before_the_target_was_set_does_not_fire_it(self):
        """The index feed runs about 15 minutes behind the claim."""
        svc = PriceMonitorService()
        target = _target(symbol="SPX", market="index", kind="below", value=5000.0)
        await _load(svc, [target])
        created_ms = int(target["created_at"].timestamp() * 1000)

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("I:SPX", 4900.0, created_ms - 15 * 60_000))
            fire.assert_not_awaited()
            assert ("SPX", MarketType.INDEX) in svc._symbol_targets
            await svc._on_message("{}", _bar("I:SPX", 4900.0, created_ms + 1))

        fire.assert_awaited_once_with(
            [target["target_id"]], 4900.0, printed_at=_ms_datetime(created_ms + 1)
        )

    @pytest.mark.asyncio
    async def test_the_poll_fires_a_target_only_on_a_row_printed_since_it_was_set(self):
        """A closed US venue's row still carries the last session's move, and
        a row with no print time cannot show it is newer. Automations on the
        symbol skip the last session's print, which a row that has rolled
        still holds, and are evaluated on the row with no print time."""
        svc = PriceMonitorService()
        target = _target(kind="pct_up", value=2.0, pct_from="previous_close")
        await _load(svc, [target])
        svc._symbol_automations = {_AAPL: [{"automation_id": "a1"}]}
        created_ms = int(target["created_at"].timestamp() * 1000)
        provider = AsyncMock()
        rows = (
            {"as_of": created_ms - 2 * 86_400_000},  # the last session's print
            {},  # no print time
            {"as_of": created_ms + 1000},
        )

        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch.object(svc, "_evaluate_and_trigger", AsyncMock()) as automation_eval,
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            for stamp in rows:
                provider.get_snapshots = AsyncMock(return_value=[{
                    "symbol": "AAPL", "price": 110.0, "previous_close": 100.0,
                    "open": 100.0, **stamp,
                }])
                await svc._poll_snapshots(poll_stock=True, poll_index=False)
                if stamp is not rows[-1]:
                    fire.assert_not_awaited()

        assert automation_eval.await_count == 2
        fire.assert_awaited_once_with(
            [target["target_id"]], 110.0, printed_at=_ms_datetime(created_ms + 1000)
        )

    @pytest.mark.asyncio
    async def test_the_poll_fires_on_a_pre_market_trade(self):
        """Before the open a US row has no session price, and after the close
        its session price is the close; the poll reads the trades the feed
        fires on, so a hit does not depend on the feed being up."""
        svc = PriceMonitorService()
        target = _target(kind="below", value=330.0)
        await _load(svc, [target])
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "AAPL", "price": None, "last_trade_price": 329.5,
            "previous_close": 344.17, "as_of": _now_ms(),
        }])
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)
        fire.assert_awaited_once_with([target["target_id"]], 329.5, printed_at=ANY)

    @pytest.mark.asyncio
    async def test_a_row_with_an_unreadable_print_time_is_skipped_alone(self):
        """A hit on it would be claimed before its print time is read, which
        fails; the rest of the pass still fires."""
        svc = PriceMonitorService()
        aapl = _target(kind="below", value=330.0)
        msft = _target(symbol="MSFT", kind="below", value=400.0)
        await _load(svc, [aapl, msft])
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": None, "last_trade_price": 329.5,
             "previous_close": 344.17, "as_of": 1e30},
            {"symbol": "MSFT", "price": None, "last_trade_price": 390.0,
             "previous_close": 410.0, "as_of": _now_ms()},
        ])
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**msft}])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)
        fire.assert_awaited_once_with([msft["target_id"]], 390.0, printed_at=ANY)
        assert [t["target_id"] for t in svc._symbol_targets[_AAPL]] == [aapl["target_id"]]

    @pytest.mark.asyncio
    async def test_a_fresh_quote_is_not_a_new_trade(self):
        """At the pre-market open a quote can move before a thin name trades;
        the row's price is still the last session's."""
        svc = PriceMonitorService()
        target = _target(kind="below", value=330.0)
        await _load(svc, [target])
        yesterday = _now_ms() - 86_400_000
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "AAPL", "price": None, "last_trade_price": 329.5,
            "last_trade_at": yesterday, "previous_close": 344.17, "as_of": _now_ms(),
        }])
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)
        fire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_closed_venue_row_is_not_measured_from_the_next_session_base(self):
        """After the close the base is the session's own close, which the
        next session measures from; the row's after-hours print was measured
        from the session's base when it printed."""
        svc = PriceMonitorService()
        target = _target(kind="pct_down", value=2.0, pct_from="previous_close",
                         created_at=datetime(2026, 11, 5, 20, 0, tzinfo=timezone.utc))
        await _load(svc, [target])
        print_ms = int(datetime(2026, 11, 6, 0, 59, tzinfo=timezone.utc).timestamp() * 1000)
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "AAPL", "price": 100.0, "regular_close": 100.0, "last_trade_price": 97.5,
            "last_trade_at": print_ms, "previous_close": 95.0, "as_of": print_ms,
        }])
        night = datetime(2026, 11, 6, 2, 0, tzinfo=timezone.utc)  # 21:00 ET
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch("src.server.services.price_monitor._now_utc", return_value=night),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=True, poll_index=False)
        fire.assert_not_awaited()
        assert svc._evaluator._reference_prices[_AAPL]["previous_close"] == 100.0

    @pytest.mark.asyncio
    async def test_a_delayed_closing_value_is_measured_from_its_own_session(self):
        """The index settles its close just after 16:00 and the feed shows it
        about 15 minutes later, with the venue already closed. Within the
        grace it is measured from the session's base, for watchers set before
        it printed; after the grace the closed row is skipped."""
        svc = PriceMonitorService()
        before = datetime(2026, 11, 5, 14, 0, tzinfo=timezone.utc)
        target = _target(symbol="SPX", market="index", kind="pct_down", value=2.0,
                         pct_from="previous_close", created_at=before)
        await _load(svc, [target])
        down_2 = {"symbol": "SPX", "market": "index", "conditions": [
            {"type": "pct_change_below", "value": 2.0, "reference": "previous_close"}]}
        after = datetime(2026, 11, 5, 21, 10, tzinfo=timezone.utc)
        early = {"automation_id": "early", "trigger_config": down_2, "created_at": before}
        late = {"automation_id": "late", "trigger_config": down_2, "created_at": after}
        edited = {"automation_id": "edited", "trigger_config": down_2,
                  "created_at": before, "updated_at": after}
        svc._symbol_automations = {_SPX: [early, late, edited]}
        provider = AsyncMock()

        def _row(price, stamp):
            return [{"symbol": "GSPC", "price": price, "regular_close": price,
                     "previous_close": 5000.0, "as_of": int(stamp.timestamp() * 1000)}]

        closing = _row(4890.0, datetime(2026, 11, 5, 21, 2, 20, tzinfo=timezone.utc))
        passes = (
            # 16:14:50 ET: the delayed row is at 15:59, down 1.8%.
            (datetime(2026, 11, 5, 21, 14, 50, tzinfo=timezone.utc),
             _row(4910.0, datetime(2026, 11, 5, 20, 59, tzinfo=timezone.utc))),
            # 16:17:20 ET: the closing value, stamped 16:02:20, down 2.2%.
            (datetime(2026, 11, 5, 21, 17, 20, tzinfo=timezone.utc), closing),
            # 16:31 ET: past the grace, the closed row is skipped.
            (datetime(2026, 11, 5, 21, 31, tzinfo=timezone.utc), closing),
        )
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch.object(svc, "_try_trigger", AsyncMock()) as trigger,
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            for now, rows in passes:
                provider.get_snapshots = AsyncMock(return_value=rows)
                with patch("src.server.services.price_monitor._now_utc", return_value=now):
                    await svc._poll_snapshots(poll_stock=False, poll_index=True)

        fire.assert_awaited_once_with([target["target_id"]], 4890.0, printed_at=ANY)
        assert trigger.await_args_list == [call(early, ANY, 4890.0)]
        assert svc._evaluator._reference_prices[_SPX]["previous_close"] == 4890.0

    @pytest.mark.asyncio
    async def test_the_reference_refresh_keeps_the_ended_session_base_for_the_closing_value(self):
        """The delayed feed can deliver the closing bar after a refresh has read
        the closing row, so the refresh holds the session's base through the
        grace and the close after it."""
        evaluator = ConditionEvaluator()
        provider = AsyncMock()
        stamp = datetime(2026, 11, 5, 21, 2, 20, tzinfo=timezone.utc)
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "GSPC", "price": 4890.0, "regular_close": 4890.0,
            "previous_close": 5000.0, "as_of": int(stamp.timestamp() * 1000),
        }])
        expected = (
            (datetime(2026, 11, 5, 21, 17, 20, tzinfo=timezone.utc), 5000.0),
            (datetime(2026, 11, 5, 21, 31, tzinfo=timezone.utc), 4890.0),
        )
        with patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)):
            for now, base in expected:
                with patch("src.server.services.price_monitor._now_utc", return_value=now):
                    await evaluator.refresh_references([_SPX])
                assert evaluator._reference_prices[_SPX]["previous_close"] == base

    @pytest.mark.asyncio
    async def test_a_closing_tick_reaches_only_a_watcher_set_before_it(self):
        """On the feed too, a delayed closing value is not news to an
        automation set or edited since it printed."""
        svc = PriceMonitorService()
        before = datetime(2026, 11, 5, 14, 0, tzinfo=timezone.utc)
        after = datetime(2026, 11, 5, 21, 20, tzinfo=timezone.utc)
        early = {"automation_id": "early", "created_at": before}
        late = {"automation_id": "late", "created_at": after}
        edited = {"automation_id": "edited", "created_at": before, "updated_at": after}
        svc._symbol_automations = {_SPX: [early, late, edited]}
        stamp = int(datetime(2026, 11, 5, 21, 2, 20, tzinfo=timezone.utc).timestamp() * 1000)
        closed = datetime(2026, 11, 5, 21, 21, tzinfo=timezone.utc)
        with (
            patch.object(svc, "_evaluate_and_trigger", AsyncMock()) as automation_eval,
            patch("src.server.services.price_monitor._now_utc", return_value=closed),
        ):
            await svc._on_message("{}", _bar("I:SPX", 4890.0, stamp))
        assert automation_eval.await_args_list == [call(early, 4890.0)]

    @pytest.mark.asyncio
    async def test_a_row_with_an_unreadable_stamp_keeps_its_previous_close(self):
        """One bad row does not stop the refresh for the rest."""
        evaluator = ConditionEvaluator()
        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "AAPL", "price": 100.0, "regular_close": 100.0,
             "previous_close": 95.0, "as_of": 1e30},
            {"symbol": "MSFT", "price": 400.0, "previous_close": 390.0},
        ])
        night = datetime(2026, 11, 6, 2, 0, tzinfo=timezone.utc)
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch("src.server.services.price_monitor._now_utc", return_value=night),
        ):
            await evaluator.refresh_references([_AAPL, ("MSFT", MarketType.STOCK)])
        assert evaluator._reference_prices[_AAPL]["previous_close"] == 95.0
        assert evaluator._reference_prices[("MSFT", MarketType.STOCK)]["previous_close"] == 390.0

    @pytest.mark.asyncio
    async def test_a_caret_index_meets_its_tick_and_its_snapshot_row(self):
        """Stored in the watch list's spelling (^GSPC), read back as SPX."""
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()
        tick = _target(symbol="^GSPC", market="index", kind="above", value=5000.0)
        polled = _target(symbol="^GSPC", market="index", kind="below", value=4000.0)
        await _load(svc, [tick, polled])
        assert svc._index_ws_symbols == {"I:SPX"}

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**tick}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("I:SPX", 5100.0))
        fire.assert_awaited_once_with([tick["target_id"]], 5100.0, printed_at=ANY)

        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[
            {"symbol": "GSPC", "price": 3900.0, "previous_close": 4100.0, "open": 4090.0,
             "as_of": _now_ms()},
        ])
        with (
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**polled}])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=True)
        provider.get_snapshots.assert_awaited_once_with(
            ["GSPC"], asset_type="indices", user_id="langalpha-service"
        )
        fire.assert_awaited_once_with([polled["target_id"]], 3900.0, printed_at=ANY)

    @pytest.mark.asyncio
    async def test_a_price_from_the_other_market_never_fires(self):
        """HSI on an index target is the Hang Seng; a US ticker's HSI tick is not its price."""
        svc = PriceMonitorService()
        await _load(svc, [_target(symbol="HSI", market="index", kind="below", value=19000.0)])

        with patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire:
            await svc._on_message("{}", _bar("HSI", 12.0))

        fire.assert_not_awaited()
        assert ("HSI", MarketType.INDEX) in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_a_bare_symbol_in_two_markets_is_two_listings(self):
        """HSI the US ticker and ^HSI the Hang Seng are both stored as HSI:
        each is read in its own market and fires on its own price, measured
        from its own previous close."""
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()
        ticker = _target(symbol="HSI", kind="pct_up", value=5.0, pct_from="previous_close")
        hang_seng = _target(
            symbol="HSI", market="index", kind="pct_up", value=1.0, pct_from="previous_close"
        )
        await _load(svc, [ticker, hang_seng])
        # The ticker rides the US feed; the Hang Seng has none, so the poll reads it.
        assert svc._stock_ws_symbols == {"HSI"}
        assert svc._listings == {("HSI", MarketType.STOCK), ("HSI", MarketType.INDEX)}
        svc._evaluator.set_reference(("HSI", MarketType.STOCK), previous_close=10.0, day_open=10.0)

        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "HSI", "price": 20300.0, "previous_close": 20000.0,
            "open": 20050.0, "as_of": _now_ms(),
        }])
        with (
            patch.object(pm, "_session_day_start_ms", return_value=0),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**hang_seng}])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)
        provider.get_snapshots.assert_awaited_once_with(
            ["HSI"], asset_type="indices", user_id="langalpha-service"
        )
        fire.assert_awaited_once_with([hang_seng["target_id"]], 20300.0, printed_at=ANY)

        with (
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**ticker}])) as fire,
            patch(_NUDGE),
        ):
            await svc._on_message("{}", _bar("HSI", 10.6))
        # Up 6% from its own close; from the Hang Seng's it would be no move at all.
        fire.assert_awaited_once_with([ticker["target_id"]], 10.6, printed_at=ANY)

    @pytest.mark.asyncio
    async def test_a_lapsed_target_is_not_claimed(self):
        svc = PriceMonitorService()
        await _load(svc, [_target(kind="above", value=200.0, expires_in=-1)])

        with patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire:
            await svc._on_message("{}", _bar("AAPL", 250.0))

        fire.assert_not_awaited()
        assert _AAPL not in svc._symbol_targets

    @pytest.mark.asyncio
    async def test_the_rest_poll_fires_targets(self):
        svc = PriceMonitorService()
        target = _target(symbol="600519.SH", kind="pct_up", value=2.0, pct_from="previous_close")
        await _load(svc, [target])
        provider = AsyncMock()
        printed = _now_ms()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "600519.SH", "price": 1529.0, "previous_close": 1500.0,
            "open": 1505.0, "as_of": printed,
        }])

        with (
            patch.object(pm, "_session_day_start_ms", return_value=0),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        # +1.93% is short of 2; the snapshot's previous close is the reference.
        fire.assert_not_awaited()

        provider.get_snapshots.return_value[0]["price"] = 1530.0
        with (
            patch.object(pm, "_session_day_start_ms", return_value=0),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE) as nudge,
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)

        # Exactly +2% touches the target.
        fire.assert_awaited_once_with(
            [target["target_id"]], 1530.0, printed_at=_ms_datetime(printed)
        )
        nudge.assert_called_once()


class TestLoadTargets:
    def setup_method(self):
        PriceMonitorService._instance = None
        MarketDataFeed._instances.clear()

    @pytest.mark.asyncio
    async def test_targets_subscribe_on_the_feed_of_their_market(self):
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()

        await _load(svc, [
            _target(symbol="NVDA"),
            _target(symbol="SPX", market="index"),
            _target(symbol="600519.SH"),
        ])

        assert svc._stock_ws_symbols == {"NVDA"}
        assert svc._index_ws_symbols == {"I:SPX"}
        # A CN listing has no WS feed; the REST poll covers it in session.
        assert svc._monitored_listings == {
            ("NVDA", MarketType.STOCK), ("SPX", MarketType.INDEX), ("600519.SH", MarketType.STOCK),
        }
        assert ("600519.SH", MarketType.STOCK) in svc._listings

    @pytest.mark.asyncio
    async def test_a_target_reload_keeps_automation_subscriptions(self):
        automation = {
            "automation_id": str(uuid.uuid4()),
            "trigger_config": {
                "symbol": "AAPL",
                "conditions": [{"type": "price_above", "value": 250.0}],
            },
        }
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()
        with (
            patch(
                "src.server.database.automation.get_active_price_automations",
                AsyncMock(return_value=[automation]),
            ),
            patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock),
        ):
            await svc._load_automations()

        await _load(svc, [_target(symbol="TSLA")])
        assert svc._stock_ws_symbols == {"AAPL", "TSLA"}

        # The target is gone (fired, cancelled or lapsed): only its symbol goes.
        await _load(svc, [])
        assert svc._stock_ws_symbols == {"AAPL"}
        svc._stock_handle.unsubscribe.assert_awaited_once_with(["TSLA"])
        assert svc._monitored_listings == {_AAPL}

    @pytest.mark.asyncio
    async def test_a_thread_reload_replaces_that_threads_targets_only(self):
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()
        mine, theirs = str(uuid.uuid4()), str(uuid.uuid4())
        kept = _target(symbol="AAPL", conversation_thread_id=theirs)
        dropped = _target(symbol="AAPL", kind="below", value=150.0, conversation_thread_id=theirs)
        replaced = _target(symbol="AAPL", conversation_thread_id=mine)
        gone = _target(symbol="TSLA", conversation_thread_id=mine)
        await _load(svc, [kept, dropped, replaced, gone])
        # Claimed by this worker since the load: a reload of another thread
        # must not bring it back.
        svc._drop_target(dropped)
        added = _target(symbol="NVDA", conversation_thread_id=mine)

        listed = await _load(svc, [added], thread_ids={mine})

        listed.assert_awaited_once_with(thread_ids=[mine])
        assert {
            listing: [t["target_id"] for t in targets]
            for listing, targets in svc._symbol_targets.items()
        } == {_AAPL: [kept["target_id"]], ("NVDA", MarketType.STOCK): [added["target_id"]]}
        # The untouched thread's row is the one already held, not a re-read.
        assert svc._symbol_targets[_AAPL][0] is kept
        assert svc._stock_ws_symbols == {"AAPL", "NVDA"}
        assert svc._target_listings == {_AAPL, ("NVDA", MarketType.STOCK)}
        svc._stock_handle.unsubscribe.assert_awaited_once_with(["TSLA"])

    @pytest.mark.asyncio
    async def test_a_claim_during_a_thread_reload_stays_dropped(self):
        """The reload merges after its awaits, so a target another thread
        lost to a claim meanwhile is not restored from a stale copy."""
        svc = PriceMonitorService()
        mine, theirs = str(uuid.uuid4()), str(uuid.uuid4())
        claimed = _target(symbol="AAPL", conversation_thread_id=theirs)
        await _load(svc, [claimed])
        added = _target(symbol="NVDA", conversation_thread_id=mine)

        async def _claim_meanwhile(_listings):
            svc._drop_target(claimed)

        with (
            patch(f"{_DB}.list_armed_targets", AsyncMock(return_value=[added])),
            patch.object(svc._evaluator, "refresh_references", side_effect=_claim_meanwhile),
        ):
            await svc._load_targets({mine})

        assert list(svc._symbol_targets) == [("NVDA", MarketType.STOCK)]

    @pytest.mark.asyncio
    async def test_a_full_reload_reads_every_thread(self):
        svc = PriceMonitorService()
        stale = _target(symbol="AAPL")
        await _load(svc, [stale])
        fresh = _target(symbol="NVDA")

        listed = await _load(svc, [fresh])

        listed.assert_awaited_once_with()
        assert list(svc._symbol_targets) == [("NVDA", MarketType.STOCK)]

    @pytest.mark.asyncio
    async def test_an_alert_and_a_target_in_two_markets_each_read_their_own(self):
        """An alert on HSI, the US ticker, and a target on ^HSI, the Hang
        Seng: neither market wins the symbol, and neither reads the other's
        price."""
        automation = {
            "automation_id": str(uuid.uuid4()),
            "trigger_config": {
                "symbol": "HSI",
                "conditions": [{"type": "price_below", "value": 25000.0}],
            },
        }
        target = _target(symbol="HSI", market="index", kind="below", value=19000.0)
        svc = PriceMonitorService()
        svc._stock_handle = AsyncMock()
        svc._index_handle = AsyncMock()
        with (
            patch(
                "src.server.database.automation.get_active_price_automations",
                AsyncMock(return_value=[automation]),
            ),
            patch.object(svc._evaluator, "refresh_references", new_callable=AsyncMock),
        ):
            await svc._load_automations()
        await _load(svc, [target])

        assert svc._stock_ws_symbols == {"HSI"}
        assert svc._listings == {("HSI", MarketType.STOCK), ("HSI", MarketType.INDEX)}

        with (
            patch.object(svc, "_try_trigger", AsyncMock()) as alert_fire,
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[])) as fire,
        ):
            await svc._on_message("{}", _bar("HSI", 12.0))
        alert_fire.assert_awaited_once()
        fire.assert_not_awaited()

        provider = AsyncMock()
        provider.get_snapshots = AsyncMock(return_value=[{
            "symbol": "HSI", "price": 18000.0, "previous_close": 19500.0, "as_of": _now_ms(),
        }])
        with (
            patch.object(pm, "_session_day_start_ms", return_value=0),
            patch("src.data_client.get_market_data_provider", AsyncMock(return_value=provider)),
            patch.object(svc, "_try_trigger", AsyncMock()) as alert_fire,
            patch(f"{_DB}.fire_targets", AsyncMock(return_value=[{**target}])) as fire,
            patch(_NUDGE),
        ):
            await svc._poll_snapshots(poll_stock=False, poll_index=False)
        # The alert's price_below 25000 would hold at 18000, the Hang Seng's price.
        alert_fire.assert_not_awaited()
        fire.assert_awaited_once_with([target["target_id"]], 18000.0, printed_at=ANY)


class TestSweep:
    def setup_method(self):
        PriceMonitorService._instance = None

    @pytest.mark.asyncio
    async def test_claims_warnings_in_batches_then_expires(self, monkeypatch):
        monkeypatch.setattr(pm, "_TARGET_WARNING_BATCH", 2)
        svc = PriceMonitorService()
        claim = AsyncMock(side_effect=[[_target(), _target()], [_target()]])

        with (
            patch(f"{_DB}.claim_due_warnings", claim),
            patch(f"{_DB}.expire_due_targets", AsyncMock(return_value=3)) as expire,
            patch(_NUDGE) as nudge,
        ):
            await svc._sweep_targets()

        assert claim.await_count == 2
        nudge.assert_called_once()
        expire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_warning_due_is_no_nudge_and_a_failed_claim_still_expires(self):
        svc = PriceMonitorService()
        with (
            patch(f"{_DB}.claim_due_warnings", AsyncMock(return_value=[])),
            patch(f"{_DB}.expire_due_targets", AsyncMock(return_value=0)),
            patch(_NUDGE) as nudge,
        ):
            await svc._sweep_targets()
        nudge.assert_not_called()

        with (
            patch(f"{_DB}.claim_due_warnings", AsyncMock(side_effect=RuntimeError("db"))),
            patch(f"{_DB}.expire_due_targets", AsyncMock(return_value=1)) as expire,
            patch(_NUDGE) as nudge,
        ):
            await svc._sweep_targets()
        expire.assert_awaited_once()
        nudge.assert_not_called()


class TestChangeNudge:
    def setup_method(self):
        PriceMonitorService._instance = None

    def teardown_method(self):
        PriceMonitorService._instance = None

    async def _reload_after(self, monkeypatch, *requests):
        """What one debounced reload loads after *requests* in a burst."""
        monkeypatch.setattr(pm, "_TARGET_RELOAD_DEBOUNCE", 0.05)
        svc = PriceMonitorService()
        load = AsyncMock()
        with patch.object(svc, "_load_targets", load):
            task = asyncio.create_task(svc._target_reload_loop())
            for thread_id in requests:
                svc.request_targets_reload(thread_id)
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.15)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        load.assert_awaited_once()
        return load.await_args.args[0]

    @pytest.mark.asyncio
    async def test_a_burst_of_requests_is_one_reload_of_the_threads_named(self, monkeypatch):
        first, second = uuid.uuid4(), uuid.uuid4()
        # Spelled as a row spells it, whichever way the request did.
        scope = await self._reload_after(
            monkeypatch, str(first).upper(), str(second).encode(), str(first)
        )
        assert scope == {str(first), str(second)}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("unnamed", [None, b"1", "not-a-thread"])
    async def test_a_request_naming_no_thread_reloads_every_thread(self, monkeypatch, unnamed):
        """A fresh subscription, an older publisher's payload, or one that
        cannot be read."""
        assert await self._reload_after(monkeypatch, str(uuid.uuid4()), unnamed) is None

    @pytest.mark.asyncio
    async def test_a_change_names_its_thread_on_the_channel_and_in_process(self):
        from src.server.services.market_targets import changes
        from src.server.services.market_targets.signals import TARGETS_CHANGED_CHANNEL

        svc = PriceMonitorService.get_instance()
        thread_id = str(uuid.uuid4())
        cache = MagicMock(enabled=True, client=MagicMock(publish=AsyncMock()))
        with patch(
            "src.server.services.market_targets.signals.get_cache_client",
            return_value=cache,
        ):
            await changes.targets_changed(thread_id)

        cache.client.publish.assert_awaited_once_with(TARGETS_CHANGED_CHANNEL, thread_id)
        assert svc._targets_reload_threads == {thread_id}
        assert not svc._targets_full_reload

    @pytest.mark.asyncio
    async def test_a_message_on_the_channel_requests_a_reload(self, monkeypatch):
        from src.server.services.market_targets.signals import TARGETS_CHANGED_CHANNEL

        svc = PriceMonitorService()
        thread_id = str(uuid.uuid4()).encode()
        messages = [{"type": "message", "data": thread_id}]

        async def _get_message(**_kw):
            if messages:
                return messages.pop()
            svc._shutdown_event.set()
            return None

        pubsub = MagicMock()
        pubsub.subscribe = AsyncMock()
        pubsub.get_message = AsyncMock(side_effect=_get_message)
        pubsub.aclose = AsyncMock()
        client = MagicMock(pubsub=MagicMock(return_value=pubsub))

        with (
            patch(
                "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
                AsyncMock(return_value=client),
            ),
            patch.object(svc, "request_targets_reload") as request,
        ):
            await asyncio.wait_for(svc._target_signal_loop(), timeout=2)

        pubsub.subscribe.assert_awaited_once_with(TARGETS_CHANGED_CHANNEL)
        # A full re-read for the fresh subscription, the thread's for the message.
        assert request.call_args_list == [call(), call(thread_id)]
        pubsub.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_pubsub_retries_instead_of_retiring(self, monkeypatch):
        monkeypatch.setattr(pm, "_TARGET_SIGNAL_RETRY", 0.01)
        svc = PriceMonitorService()
        calls = 0

        async def _client():
            nonlocal calls
            calls += 1
            if calls == 3:
                svc._shutdown_event.set()
            return None

        with patch(
            "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
            _client,
        ):
            await asyncio.wait_for(svc._target_signal_loop(), timeout=2)
        assert calls == 3
