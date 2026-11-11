"""Symbol search ranking: relevance tiers, liquidity inside a tier, and the
quote fill's failure and timeout falling back to tier order."""

import asyncio
from types import SimpleNamespace

import pytest

from src.server.services import symbol_search as ss


def _row(symbol, name="", **extra):
    return {"symbol": symbol, "name": name, **extra}


class TestRelevanceTier:
    @pytest.mark.parametrize(
        "row, query, tier",
        [
            (_row("NVDA", "NVIDIA Corporation"), "nvda", ss.EXACT),
            (_row("BRK-B", "Berkshire Hathaway"), "brk.b", ss.EXACT),
            (_row("600519.SH", "贵州茅台"), "600519", ss.EXACT),
            (_row("0700.HK", "Tencent"), "700", ss.EXACT),
            (_row("0700.HK", "Tencent"), "00700", ss.EXACT),
            (_row("000700.SZ", "Jiangnan Mould"), "700", ss.OTHER),
            (_row("0700.HK", "Tencent"), "²", ss.OTHER),
            (_row("0700.HK", "Tencent"), "7" * 5000, ss.OTHER),
            (_row("NVDA", "NVIDIA Corporation"), "nv", ss.SYMBOL_PREFIX),
            (_row("GOOGL", "Alphabet Inc."), "alpha", ss.NAME_WORD),
            (_row("BAC", "Bank of America Corporation"), "america", ss.NAME_WORD),
            (_row("BAC", "Bank of America Corporation"), "bank of am", ss.NAME_WORD),
            (_row("600519.SH", "Kweichow Moutai", nameLocal="贵州茅台"), "茅台", ss.NAME_WORD),
            (_row("XYZ", "Unvisited Corp"), "nv", ss.OTHER),
            (_row("BRK.L", "Brickability"), "brk.b", ss.OTHER),
        ],
    )
    def test_tiers(self, row, query, tier):
        assert ss.matcher(query)(row) == tier

    def test_a_word_inside_a_name_word_is_not_a_word_start(self):
        assert ss.matcher("vidia")(_row("ABC", "Panvidia")) == ss.OTHER


def _matched(rows, query):
    match = ss.matcher(query)
    return [{**row, "match": match(row)} for row in rows]


class TestRankRows:
    def test_tier_first_then_dollar_volume(self):
        rows = [_row("NVS", "Novartis"), _row("NVO", "Novo"), _row("NVDA", "NVIDIA"), _row("ANV", "Nv Holdings"), _row("NV")]
        figures = {"NVS": 3e8, "NVO": 5e8, "NVDA": 8e10, "ANV": 9e12}
        ranked = ss.rank_rows(_matched(rows, "nv"), figures)
        assert [r["symbol"] for r in ranked] == ["NV", "NVDA", "NVO", "NVS", "ANV"]

    def test_rows_without_a_figure_follow_in_the_sources_order(self):
        rows = [_row("NVA"), _row("NVB"), _row("NVC"), _row("NVD")]
        ranked = ss.rank_rows(_matched(rows, "nv"), {"NVC": 1e6, "NVA": 0.0})
        assert [r["symbol"] for r in ranked] == ["NVC", "NVA", "NVB", "NVD"]

    def test_no_figures_is_tier_order_only(self):
        rows = [_row("XNV", "Nv Corp"), _row("NVB"), _row("NVA")]
        assert [r["symbol"] for r in ss.rank_rows(_matched(rows, "nv"), None)] == ["NVB", "NVA", "XNV"]


class TestContested:
    def test_only_the_tiers_reaching_the_cut(self):
        rows = _matched([_row("NVA"), _row("NVB"), _row("XA", "Nv one"), _row("ZZZ")], "nv")
        assert ss.contested(rows, 2) == ["NVA", "NVB"]
        assert ss.contested(rows, 3) == ["NVA", "NVB", "XA"]

    def test_a_single_row_has_nothing_to_order(self):
        assert ss.contested(_matched([_row("600519.SH")], "茅台"), 12) == []


class TestDollarVolume:
    NOW = 1_800_000_000_000

    def test_converts_to_dollars(self):
        row = {"price": 100.0, "volume": 1000}
        assert ss.dollar_volume(row, "USD", self.NOW) == 100_000
        assert ss.dollar_volume(row, "INR", self.NOW) == pytest.approx(1150)

    def test_the_rows_own_currency_wins(self):
        assert ss.dollar_volume({"price": 10, "volume": 10, "currency": "JPY"}, "USD", self.NOW) == pytest.approx(0.67)

    def test_unknown_currency_stale_print_or_no_volume_has_no_figure(self):
        assert ss.dollar_volume({"price": 10, "volume": 10}, None, self.NOW) is None
        stale = {"price": 76400, "volume": 3_730_500, "as_of": self.NOW - 30 * 86_400_000}
        assert ss.dollar_volume(stale, "USD", self.NOW) is None
        assert ss.dollar_volume({"price": 10, "volume": None}, "USD", self.NOW) is None
        assert ss.dollar_volume({"price": 10, "volume": 0}, "USD", self.NOW) is None


class _Quotes:
    """QuoteCacheService.fetch_quotes, answering after *delay* or raising
    *error*; the symbols in *unreached* failed to fill."""

    def __init__(self, rows, *, delay=0.0, error=None, unreached=()):
        self.rows, self.delay, self.error, self.unreached = rows, delay, error, set(unreached)
        self.asked = []
        self.finished = asyncio.Event()

    def get_instance(self):
        return self

    async def fetch_quotes(self, refs, user_id=None):
        self.asked.append([ref.instrument_key for ref in refs])
        await asyncio.sleep(self.delay)
        self.finished.set()
        if self.error:
            raise self.error
        failed = [ref for ref in refs if ref.symbol in self.unreached]
        failure = SimpleNamespace(refs=failed) if failed else None
        return [(ref, self.rows[ref.symbol]) for ref in refs if ref.symbol in self.rows], failure


@pytest.fixture
def quotes(monkeypatch):
    def install(rows, **kwargs):
        fake = _Quotes(rows, **kwargs)
        from src.server.services.cache import quote_cache_service

        monkeypatch.setattr(quote_cache_service, "QuoteCacheService", fake)
        return fake

    return install


class TestQuoteTradedValue:
    @pytest.mark.asyncio
    async def test_figures_per_symbol(self, quotes):
        quotes({"NVDA": {"price": 100, "volume": 10}, "NVO": {"price": 50, "volume": 2}})
        figures = await ss.QuoteTradedValue().figures(["NVDA", "NVO", "NVX"], "u1", 1.0)
        assert figures == ss.Figures({"NVDA": 1000.0, "NVO": 100.0}, True)

    @pytest.mark.asyncio
    async def test_a_fill_that_failed_for_some_is_incomplete(self, quotes):
        # The cached NVO answers while NVDA's fill fails: an order without
        # NVDA is not one to keep for the full TTL.
        quotes({"NVO": {"price": 50, "volume": 2}}, unreached={"NVDA"})
        figures = await ss.QuoteTradedValue().figures(["NVDA", "NVO"], "u1", 1.0)
        assert figures == ss.Figures({"NVO": 100.0}, False)

    @pytest.mark.asyncio
    async def test_a_failed_fill_is_no_figures(self, quotes):
        quotes({}, error=RuntimeError("down"))
        assert await ss.QuoteTradedValue().figures(["NVDA"], "u1", 1.0) == ss.Figures({}, False)

    @pytest.mark.asyncio
    async def test_a_slow_fill_is_no_figures_and_is_left_to_finish(self, quotes):
        fake = quotes({"NVDA": {"price": 100, "volume": 10}}, delay=0.2)
        assert await ss.QuoteTradedValue().figures(["NVDA"], "u1", 0.01) == ss.Figures({}, False)
        await asyncio.wait_for(fake.finished.wait(), 1)
        await asyncio.sleep(0)
        assert not ss._draining


class _Cache:
    def __init__(self, hit=None):
        self.hit, self.writes = hit, []

    async def get(self, key):
        return self.hit

    async def set(self, key, value, ttl=None):
        self.writes.append((key, value, ttl))


@pytest.fixture
def sources(monkeypatch):
    import src.data_client as data_client
    from src.utils.cache import redis_cache

    def install(rows, *, partial=False, hit=None):
        from src.data_client.financial_data_provider import PartialSearch

        class _Financial:
            asked = []

            async def search_stocks(self, query, limit):
                self.asked.append(limit)
                if partial:
                    raise PartialSearch(rows)
                return rows

        class _Provider:
            financial = _Financial()

        async def provider():
            return _Provider()

        cache = _Cache(hit)
        monkeypatch.setattr(redis_cache, "get_cache_client", lambda: cache)
        monkeypatch.setattr(ss, "traded_value", lambda: ss.QuoteTradedValue())
        monkeypatch.setattr(data_client, "get_financial_data_provider", provider)
        return _Provider.financial, cache

    return install


NV_ROWS = [_row("NVS", "Novartis"), _row("NVO", "Novo Nordisk"), _row("NVDA", "NVIDIA"), _row("XNV", "Nv Bank")]


class TestSearchSymbols:
    @pytest.mark.asyncio
    async def test_ranks_a_wide_candidate_set_and_caches_it(self, sources, quotes):
        financial, cache = sources(NV_ROWS)
        fake = quotes({"NVDA": {"price": 180, "volume": 4e8}, "NVO": {"price": 40, "volume": 1e7}, "NVS": {"price": 160, "volume": 2e6}})

        rows = await ss.search_symbols(" nv ", 12, "u1")

        assert [(r["symbol"], r["match"]) for r in rows] == [
            ("NVDA", ss.SYMBOL_PREFIX), ("NVO", ss.SYMBOL_PREFIX), ("NVS", ss.SYMBOL_PREFIX), ("XNV", ss.NAME_WORD),
        ]
        assert financial.asked == [ss.MIN_CANDIDATES]
        assert len(fake.asked) == 1 and len(fake.asked[0]) == 4  # one fill; every row can reach the first 12
        assert cache.writes == [("search:v2:nv:12:", {"results": rows}, 300)]

    @pytest.mark.asyncio
    async def test_a_timed_out_fill_answers_in_tier_order_cached_briefly(self, sources, quotes, monkeypatch):
        monkeypatch.setattr(ss, "LIQUIDITY_TIMEOUT_S", 0.01)
        _, cache = sources([_row("XNV", "Nv Bank"), *NV_ROWS[:3]])
        fake = quotes({"NVDA": {"price": 180, "volume": 4e8}}, delay=0.3)

        rows = await ss.search_symbols("nv", 12, "u1")

        assert [r["symbol"] for r in rows] == ["NVS", "NVO", "NVDA", "XNV"]
        assert cache.writes[0][2] == ss._UNRANKED_TTL_S
        await asyncio.wait_for(fake.finished.wait(), 1)

    @pytest.mark.asyncio
    async def test_only_the_tiers_that_reach_the_cut_are_quoted(self, sources, quotes):
        sources(NV_ROWS)
        fake = quotes({})
        await ss.search_symbols("nv", 2, "u1")
        assert sorted(fake.asked[0]) == ["NVDA.XNAS", "NVO.XNYS", "NVS.XNYS"]

    @pytest.mark.asyncio
    async def test_a_failed_fill_still_answers(self, sources, quotes):
        sources(NV_ROWS)
        quotes({}, error=RuntimeError("down"))
        rows = await ss.search_symbols("nv", 12, "u1")
        assert [r["symbol"] for r in rows] == ["NVS", "NVO", "NVDA", "XNV"]

    @pytest.mark.asyncio
    async def test_a_partial_answer_is_ranked_but_not_cached(self, sources, quotes):
        _, cache = sources(NV_ROWS, partial=True)
        quotes({"NVDA": {"price": 180, "volume": 4e8}})
        rows = await ss.search_symbols("nv", 12, "u1")
        assert rows[0]["symbol"] == "NVDA"
        assert cache.writes == []

    @pytest.mark.asyncio
    async def test_a_cached_answer_is_served_without_asking(self, sources, quotes):
        financial, _ = sources(NV_ROWS, hit={"results": [_row("NVDA")]})
        fake = quotes({})
        assert await ss.search_symbols("nv", 12, "u1") == [_row("NVDA")]
        assert financial.asked == [] and fake.asked == []


class TestRoute:
    @pytest.mark.asyncio
    async def test_filters_by_exchange_before_the_cut(self, sources, quotes):
        from src.server.app import market_data

        sources([
            _row("NVDA", "NVIDIA", exchangeShortName="NASDAQ"),
            _row("NVO", "Novo", exchangeShortName="NYSE"),
            _row("NVS", "Novartis", exchangeShortName="NYSE"),
        ])
        quotes({"NVDA": {"price": 180, "volume": 4e8}, "NVO": {"price": 40, "volume": 1e7}, "NVS": {"price": 160, "volume": 2e6}})

        resp = await market_data.search_stocks(user_id="u1", query="nv", limit=1, exchange=["nyse"])

        assert [(r.symbol, r.match) for r in resp.results] == [("NVO", "symbol_prefix")]

    @pytest.mark.asyncio
    async def test_a_row_with_no_name_answers_with_an_empty_one(self, sources, quotes):
        # A row past the limit is never validated; one within it reads as nameless.
        from src.server.app import market_data

        sources([_row("NVDA", None), _row("NVO", "Novo"), _row("NVS", None)])
        quotes({})

        resp = await market_data.search_stocks(user_id="u1", query="nv", limit=2, exchange=[])

        assert [(r.symbol, r.name) for r in resp.results] == [("NVDA", ""), ("NVO", "Novo")]

    @pytest.mark.asyncio
    async def test_a_filtered_out_row_takes_no_place_in_the_ranking(self, sources, quotes):
        # The exact NYSE "A" would decide the cut at one row, leaving the
        # NASDAQ rows unquoted and in the provider's order.
        from src.server.app import market_data

        _, cache = sources([
            _row("A", "Agilent", exchangeShortName="NYSE"),
            _row("AMD", "AMD", exchangeShortName="NASDAQ"),
            _row("AAPL", "Apple", exchangeShortName="NASDAQ"),
        ])
        fake = quotes({"AAPL": {"price": 230, "volume": 5e7}, "AMD": {"price": 150, "volume": 3e7}})

        resp = await market_data.search_stocks(user_id="u1", query="a", limit=1, exchange=["nasdaq"])

        assert [r.symbol for r in resp.results] == ["AAPL"]
        assert sorted(fake.asked[0]) == ["AAPL.XNAS", "AMD.XNAS"]
        assert cache.writes[0][0] == "search:v2:a:1:NASDAQ"


class _Tables:
    """LiquidityTables, holding fixed tables by market."""

    def __init__(self, **tables):
        self.tables, self.asked = tables, []

    async def table(self, market):
        self.asked.append(market)
        return self.tables.get(market)


class _Fallback:
    def __init__(self, values=None, complete=True):
        self.values, self.complete, self.asked, self.timeouts = values or {}, complete, [], []

    async def figures(self, symbols, user_id, timeout):
        self.asked.append(sorted(symbols))
        self.timeouts.append(timeout)
        return ss.Figures({s: v for s, v in self.values.items() if s in symbols}, self.complete)


def _table(market, currency, adv, as_of="2026-11-09"):
    from src.server.services.cache.liquidity_tables import LiquidityTable

    return LiquidityTable.of(market, currency, as_of, 0.0, adv)


US_TABLE = _table("us", "USD", {"NVDA.XNAS": 3.0e10, "NVO.XNYS": 5.0e8, "BRK-B.XNYS": 1.5e9})
CN_TABLE = _table("cn", "CNY", {"600519.XSHG": 7.0e9})


class TestTableTradedValue:
    @pytest.mark.asyncio
    async def test_figures_from_the_tables_in_dollars_without_quotes(self):
        fallback = _Fallback()
        source = ss.TableTradedValue(_Tables(us=US_TABLE, cn=CN_TABLE), fallback)

        figures = await source.figures(["NVDA", "NVO", "NVQX", "600519.SH"], "u1", 1.0)

        assert figures.complete
        assert figures.values == {"NVDA": 3.0e10, "NVO": 5.0e8, "600519.SH": pytest.approx(7.0e9 * 0.14)}
        assert fallback.asked == []  # NVQX is not in the table: no figure, and no quote

    @pytest.mark.asyncio
    async def test_spellings_of_one_listing_join_on_its_instrument_key(self):
        source = ss.TableTradedValue(_Tables(us=US_TABLE), _Fallback())
        figures = await source.figures(["BRK.B", "BRK-B"], "u1", 1.0)
        assert figures.values == {"BRK.B": 1.5e9, "BRK-B": 1.5e9}

    @pytest.mark.asyncio
    async def test_a_market_without_a_table_falls_back_to_quotes(self):
        fallback = _Fallback({"NVDA": 9.0, "0700.HK": 4.0}, complete=False)
        tables = _Tables(cn=CN_TABLE)  # the US table is missing; HK has none
        figures = await ss.TableTradedValue(tables, fallback).figures(["NVDA", "0700.HK", "600519.SH"], "u1", 1.0)

        assert fallback.asked == [["0700.HK", "NVDA"]]
        assert figures.values == {"NVDA": 9.0, "0700.HK": 4.0, "600519.SH": pytest.approx(9.8e8)}
        assert not figures.complete
        assert sorted(tables.asked) == ["cn", "us"]  # HK is never asked of ginlix-data

    @pytest.mark.asyncio
    async def test_a_stale_table_falls_back_to_quotes(self):
        from src.server.services.cache.liquidity_tables import LiquidityTables

        async def route(market):
            return {"currency": "USD", "as_of": "2025-01-02", "rows": [{"instrument_key": "NVDA.XNAS", "adv": 1e10}]}

        class _NoRedis:
            async def get(self, key):
                return None

            async def set(self, key, value, ttl=None):
                pass

        fallback = _Fallback({"NVDA": 7.0})
        tables = LiquidityTables(route, cache=_NoRedis)
        figures = await ss.TableTradedValue(tables, fallback).figures(["NVDA"], "u1", 1.0)
        assert figures.values == {"NVDA": 7.0} and fallback.asked == [["NVDA"]]

    @pytest.mark.asyncio
    async def test_a_table_slow_to_arrive_leaves_the_fallback_what_is_left_of_one_deadline(self):
        class _Slow:
            async def table(self, market):
                await asyncio.sleep(1)

        fallback = _Fallback({"NVDA": 7.0})
        figures = await ss.TableTradedValue(_Slow(), fallback).figures(["NVDA"], "u1", 0.05)
        assert figures.values == {"NVDA": 7.0}
        assert fallback.timeouts[0] < 0.01  # the table spent the whole budget; the fill gets none of its own


    @pytest.mark.asyncio
    async def test_a_market_no_table_covers_waits_briefly_and_a_tableless_one_the_rest(self):
        fallback = _Fallback({"0700.HK": 9.0})
        tables = _Tables(us=US_TABLE)
        await ss.TableTradedValue(tables, fallback).figures(["NVDA", "0700.HK"], "u1", 1.5)
        assert fallback.asked == [["0700.HK"]] and fallback.timeouts[0] <= ss.UNCOVERED_QUOTE_WAIT_S

        fallback = _Fallback({})
        await ss.TableTradedValue(_Tables(), fallback).figures(["NVDA", "0700.HK"], "u1", 1.5)
        assert fallback.timeouts[0] > 1.0  # no US table to be had: quotes get what is left


class TestSourceChoice:
    def test_ginlix_data_configured_ranks_by_its_tables(self, monkeypatch):
        import src.config.settings as settings

        monkeypatch.setattr(settings, "GINLIX_DATA_ENABLED", True)
        assert isinstance(ss.traded_value(), ss.TableTradedValue)
        monkeypatch.setattr(settings, "GINLIX_DATA_ENABLED", False)
        assert isinstance(ss.traded_value(), ss.QuoteTradedValue)

    @pytest.mark.asyncio
    async def test_a_search_ranked_from_the_tables_fetches_no_quote(self, sources, quotes, monkeypatch):
        _, cache = sources(NV_ROWS)
        fake = quotes({})
        monkeypatch.setattr(ss, "traded_value", lambda: ss.TableTradedValue(_Tables(us=US_TABLE), ss.QuoteTradedValue()))

        rows = await ss.search_symbols("nv", 12, "u1")

        assert [r["symbol"] for r in rows] == ["NVDA", "NVO", "NVS", "XNV"]
        assert fake.asked == []
        assert cache.writes[0][2] == 300
