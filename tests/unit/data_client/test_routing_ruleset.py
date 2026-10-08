"""Tests for the generated routing ruleset: policy, table lookup, and IO."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from market_protocol.routing import (
    RULESET_VERSION,
    Cell,
    CellKey,
    ProbedProvider,
    ProviderInfo,
    Ruleset,
    RoutingTable,
    Surface,
    load_ruleset,
    order_providers,
)
from scripts.data_probe.store import merge_rulesets, save_ruleset
from src.data_client.registry import default_ruleset_path

NOW = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)


def _probe(name: str, **kw) -> ProbedProvider:
    kw.setdefault("coverage_hit", 2)
    kw.setdefault("coverage_total", 2)
    return ProbedProvider(name=name, **kw)


def _cell(key: CellKey, providers: list[ProbedProvider]) -> Cell:
    return Cell(key=key, probed_at=NOW, providers=order_providers(key.surface, providers))


INTRADAY_1M = CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY, interval="1m")


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

class TestOrderProviders:
    def test_not_entitled_is_excluded(self):
        out = order_providers(Surface.DAILY, [_probe("a", entitled=False), _probe("b")])
        assert [p.name for p in out] == ["b", "a"]
        assert out[1].excluded_reason == "not_entitled"

    def test_no_coverage_is_excluded(self):
        out = order_providers(Surface.DAILY, [_probe("a", coverage_hit=0), _probe("b")])
        assert {p.name: p.excluded_reason for p in out} == {"a": "no_coverage", "b": None}

    def test_incomplete_session_excluded_for_bars_only(self):
        bars = order_providers(Surface.INTRADAY, [_probe("a", session_complete=False)])
        assert bars[0].excluded_reason == "incomplete_session"
        snaps = order_providers(Surface.SNAPSHOT, [_probe("a", session_complete=False)])
        assert snaps[0].excluded_reason is None

    def test_bad_anchors_excluded(self):
        out = order_providers(Surface.INTRADAY, [_probe("a", anchors_ok=False)])
        assert out[0].excluded_reason == "bad_anchors"

    def test_quota_below_floor_excluded_for_bars_only(self):
        bars = order_providers(Surface.INTRADAY, [_probe("a", calls_per_minute=1)])
        assert bars[0].excluded_reason == "quota_below_floor"
        snaps = order_providers(Surface.SNAPSHOT, [_probe("a", calls_per_minute=1)])
        assert snaps[0].excluded_reason is None

    def test_measured_lag_outranks_declared_tier(self):
        out = order_providers(
            Surface.INTRADAY,
            [_probe("slow", lag_s=600, tier="realtime"), _probe("fast", lag_s=10, tier="delayed_15m")],
        )
        assert [p.name for p in out] == ["fast", "slow"]

    def test_ties_keep_probe_order(self):
        out = order_providers(Surface.DAILY, [_probe("a", tier="eod"), _probe("b", tier="eod")])
        assert [p.name for p in out] == ["a", "b"]

    def test_excluded_always_rank_last(self):
        out = order_providers(
            Surface.DAILY, [_probe("dead", entitled=False, lag_s=0), _probe("live", lag_s=300)]
        )
        assert [p.name for p in out] == ["live", "dead"]

    def test_input_is_not_mutated(self):
        original = _probe("a", entitled=False)
        order_providers(Surface.DAILY, [original])
        assert original.excluded_reason is None


# ---------------------------------------------------------------------------
# RoutingTable
# ---------------------------------------------------------------------------

class TestRoutingTable:
    def test_returns_routable_names_in_order(self):
        rs = Ruleset(
            generated_at=NOW,
            cells=[_cell(INTRADAY_1M, [_probe("yfinance", session_complete=False), _probe("fmp")])],
        )
        assert RoutingTable(rs).order_for("intraday", "cn", "equity", "1m") == ["fmp"]

    def test_unknown_cell_is_none(self):
        rs = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp")])])
        table = RoutingTable(rs)
        assert table.order_for("intraday", "us", "equity", "1m") is None
        assert table.order_for("daily", "cn", "equity") is None

    def test_interval_falls_back_to_interval_less_cell(self):
        key = CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY)
        rs = Ruleset(generated_at=NOW, cells=[_cell(key, [_probe("fmp")])])
        assert RoutingTable(rs).order_for("intraday", "cn", "equity", "15m") == ["fmp"]

    def test_exact_interval_wins_over_fallback(self):
        generic = CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY)
        rs = Ruleset(
            generated_at=NOW,
            cells=[_cell(generic, [_probe("fmp")]), _cell(INTRADAY_1M, [_probe("tushare")])],
        )
        table = RoutingTable(rs)
        assert table.order_for("intraday", "cn", "equity", "1m") == ["tushare"]
        assert table.order_for("intraday", "cn", "equity", "5m") == ["fmp"]

    def test_unprobed_interval_takes_the_nearest_probed_cell(self):
        five = CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY, interval="5m")
        rs = Ruleset(
            generated_at=NOW,
            cells=[_cell(INTRADAY_1M, [_probe("tushare")]), _cell(five, [_probe("fmp")])],
        )
        table = RoutingTable(rs)
        # 4h: widest probed bar no wider than it is 5m.
        assert table.order_for("intraday", "cn", "equity", "4h") == ["fmp"]
        # 1s: readable but narrower than every probed cell, so the narrowest governs.
        assert table.order_for("intraday", "cn", "equity", "1s") == ["tushare"]
        # 30s is not a schema width: unreadable, so no order.
        assert table.order_for("intraday", "cn", "equity", "30s") is None
        assert table.order_for("intraday", "hk", "equity", "4h") is None

    def test_cell_with_everything_excluded_is_an_empty_list(self):
        rs = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp", entitled=False)])])
        assert RoutingTable(rs).order_for("intraday", "cn", "equity", "1m") == []

    def test_surface_accepts_a_plain_string(self):
        rs = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp")])])
        assert RoutingTable(rs).order_for(Surface.INTRADAY, "cn", "equity", "1m") == ["fmp"]


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------

class TestRulesetIO:
    def test_save_load_roundtrip(self, tmp_path):
        rs = Ruleset(
            generated_at=NOW,
            providers={"fmp": ProviderInfo(fingerprint="abcd1234")},
            cells=[_cell(INTRADAY_1M, [_probe("fmp", lag_s=12, tier="realtime")])],
        )
        path = save_ruleset(rs, tmp_path / "data_routing.yaml")
        loaded = load_ruleset(path)
        assert loaded is not None
        assert loaded.providers["fmp"].fingerprint == "abcd1234"
        assert loaded.cells[0].key == INTRADAY_1M
        assert loaded.cells[0].providers[0].lag_s == 12

    def test_missing_file_is_none(self, tmp_path):
        assert load_ruleset(tmp_path / "nope.yaml") is None

    def test_unreadable_file_is_none(self, tmp_path):
        path = tmp_path / "data_routing.yaml"
        path.write_text("cells: [ this is not a ruleset\n")
        assert load_ruleset(path) is None

    def test_wrong_shape_is_none(self, tmp_path):
        path = tmp_path / "data_routing.yaml"
        path.write_text("cells: 7\n")
        assert load_ruleset(path) is None

    def test_version_mismatch_is_none(self, tmp_path):
        rs = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp")])])
        path = save_ruleset(rs, tmp_path / "data_routing.yaml")
        path.write_text(path.read_text().replace(f"version: {RULESET_VERSION}", "version: 99", 1))
        assert load_ruleset(path) is None

    def test_env_override_selects_the_path(self, tmp_path, monkeypatch):
        rs = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp")])])
        path = save_ruleset(rs, tmp_path / "elsewhere.yaml")
        monkeypatch.setenv("DATA_ROUTING_RULESET", str(path))
        loaded = load_ruleset(default_ruleset_path())
        assert loaded is not None and loaded.cells[0].key == INTRADAY_1M

    def test_absent_ruleset_says_static_routing_is_live(self, tmp_path, monkeypatch, caplog):
        from src.data_client.registry import _load_routing_table

        monkeypatch.setenv("DATA_ROUTING_RULESET", str(tmp_path / "absent.yaml"))
        with caplog.at_level("INFO", logger="src.data_client.registry"):
            assert _load_routing_table() is None
        (record,) = [r for r in caplog.records if "no_ruleset" in r.getMessage()]
        assert record.levelname == "INFO" and "static" in record.getMessage()

    def test_interval_is_stored_as_the_schema_id(self):
        assert INTRADAY_1M.interval == "ohlcv-1m"
        assert CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY,
                       interval="1min") == INTRADAY_1M

    def test_version_1_file_migrates_its_bare_intervals(self, tmp_path):
        path = tmp_path / "data_routing.yaml"
        path.write_text(
            "version: 1\n"
            "generated_at: '2026-09-09T12:00:00Z'\n"
            "cells:\n"
            "- key: {market: cn, asset_class: equity, surface: intraday, interval: 5m}\n"
            "  probed_at: '2026-09-09T12:00:00Z'\n"
            "  providers:\n"
            "  - {name: fmp, coverage_hit: 2, coverage_total: 2}\n"
        )
        loaded = load_ruleset(path)
        assert loaded is not None and loaded.version == RULESET_VERSION
        assert loaded.cells[0].key.interval == "ohlcv-5m"
        assert RoutingTable(loaded).order_for("intraday", "cn", "equity", "5min") == ["fmp"]

    def test_merge_combines_matching_cells_and_keeps_the_rest(self):
        hk = CellKey(market="hk", asset_class="index", surface=Surface.SNAPSHOT)
        base = Ruleset(
            generated_at=NOW,
            providers={"fmp": ProviderInfo()},
            cells=[_cell(INTRADAY_1M, [_probe("fmp")]), _cell(hk, [_probe("fmp")])],
        )
        update = Ruleset(
            generated_at=NOW,
            providers={"tushare": ProviderInfo()},
            cells=[_cell(INTRADAY_1M, [_probe("tushare")])],
        )
        merged = merge_rulesets(base, update)
        table = RoutingTable(merged)
        # fmp was not measured by the update, so its verdict in the cell stands.
        assert table.order_for("intraday", "cn", "equity", "1m") == ["tushare", "fmp"]
        assert table.order_for("snapshot", "hk", "index") == ["fmp"]
        assert set(merged.providers) == {"fmp", "tushare"}

    def test_merge_onto_nothing_returns_the_update(self):
        update = Ruleset(generated_at=NOW, cells=[_cell(INTRADAY_1M, [_probe("fmp")])])
        assert merge_rulesets(None, update) is update

    def test_saved_file_carries_the_do_not_edit_header(self, tmp_path):
        path = save_ruleset(Ruleset(generated_at=NOW), tmp_path / "data_routing.yaml")
        assert path.read_text().startswith("# Generated by")


@pytest.mark.parametrize(
    "surface,interval",
    [(Surface.INTRADAY, "ohlcv-1m"), (Surface.DAILY, None), (Surface.SNAPSHOT, None)],
)
def test_cell_key_as_str(surface, interval):
    key = CellKey(market="cn", asset_class="fund", surface=surface, interval=interval)
    tail = f"/{interval}" if interval else ""
    assert key.as_str() == f"cn/fund/{surface.value}{tail}"


class TestPolicyAdjustmentAndQuota:
    def test_daily_ranks_adjusted_series_above_raw(self):
        from market_protocol.routing import ProbedProvider, Surface, order_providers

        raw = ProbedProvider(name="fmp", coverage_hit=3, coverage_total=3, session_complete=True,
                             tier="delayed_15m", price_treatment="raw", cost_calls=1)
        qfq = ProbedProvider(name="tushare", coverage_hit=3, coverage_total=3, session_complete=True,
                             tier="eod", price_treatment="dividend_adjusted", cost_calls=4)
        assert [p.name for p in order_providers(Surface.DAILY, [raw, qfq])] == ["tushare", "fmp"]
        # Intraday does not weigh adjustment: the cheaper, fresher raw feed wins.
        assert [p.name for p in order_providers(Surface.INTRADAY, [raw, qfq])] == ["fmp", "tushare"]

    def test_throttled_to_zero_rows_reads_quota_not_coverage(self):
        from market_protocol.routing import ProbedProvider, Surface, order_providers

        throttled = ProbedProvider(name="tushare", coverage_hit=0, coverage_total=3, calls_per_minute=1)
        (p,) = order_providers(Surface.INTRADAY, [throttled])
        assert p.excluded_reason == "quota_below_floor"
