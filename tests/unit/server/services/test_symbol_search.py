"""Symbol search ranking: relevance tiers, liquidity inside a tier, and the
quote fill's failure and timeout falling back to tier order."""

import asyncio

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
    """QuoteCacheService.get_quotes, answering after *delay* or raising *error*."""

    def __init__(self, rows, *, delay=0.0, error=None):
        self.rows, self.delay, self.error = rows, delay, error
        self.asked = []
        self.finished = asyncio.Event()

    def get_instance(self):
        return self

    async def get_quotes(self, refs, user_id=None):
        self.asked.append([ref.instrument_key for ref in refs])
        await asyncio.sleep(self.delay)
        self.finished.set()
        if self.error:
            raise self.error
        return [(ref, self.rows[ref.symbol]) for ref in refs if ref.symbol in self.rows]


@pytest.fixture
def quotes(monkeypatch):
    def install(rows, **kwargs):
        fake = _Quotes(rows, **kwargs)
        from src.server.services.cache import quote_cache_service

        monkeypatch.setattr(quote_cache_service, "QuoteCacheService", fake)
        return fake

    return install


class TestLiquidity:
    @pytest.mark.asyncio
    async def test_figures_per_symbol(self, quotes):
        quotes({"NVDA": {"price": 100, "volume": 10}, "NVO": {"price": 50, "volume": 2}})
        assert await ss.liquidity(["NVDA", "NVO", "NVX"], "u1", 1.0) == {"NVDA": 1000.0, "NVO": 100.0}

    @pytest.mark.asyncio
    async def test_a_failed_fill_is_no_figures(self, quotes):
        quotes({}, error=RuntimeError("down"))
        assert await ss.liquidity(["NVDA"], "u1", 1.0) is None

    @pytest.mark.asyncio
    async def test_a_slow_fill_is_no_figures_and_is_left_to_finish(self, quotes):
        fake = quotes({"NVDA": {"price": 100, "volume": 10}}, delay=0.2)
        assert await ss.liquidity(["NVDA"], "u1", 0.01) is None
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
        assert cache.writes == [("search:v2:nv:12", {"results": rows}, 300)]

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
