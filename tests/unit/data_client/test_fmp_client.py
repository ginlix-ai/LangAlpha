"""Auth-mechanism and key-leak guards for the FMP client.

The API key travels ONLY in the ``apikey`` request header — never the URL.
Query-string auth is forbidden because Daytona substitutes sandbox secret
placeholders exclusively in HTTPS request headers, and because httpx bakes
request URLs into exception messages. ``FMPClient._make_request``
additionally never stringifies the underlying httpx error into
``FMPRequestError`` (defense in depth): it surfaces only the status code or
a static message.
"""

from unittest.mock import AsyncMock

import httpx
import pytest

from src.data_client.fmp.fmp_client import FMPClient, FMPRequestError

_SECRET = "TESTSECRET"  # neutral placeholder — stands in for a real apikey


def _client_with_mocked_http(get_mock: AsyncMock) -> FMPClient:
    client = FMPClient(api_key=_SECRET)
    mock_http = AsyncMock()
    mock_http.is_closed = False  # else _get_client rebuilds a real client
    mock_http.get = get_mock
    client._client = mock_http
    return client


class TestFmpErrorKeyLeakGuard:
    @pytest.mark.asyncio
    async def test_http_status_error_does_not_leak_apikey(self):
        leaky_url = (
            f"https://financialmodelingprep.com/stable/profile?symbol=AAPL&apikey={_SECRET}"
        )
        request = httpx.Request("GET", leaky_url)
        response = httpx.Response(403, request=request, text="Forbidden")

        # Sanity: the RAW httpx error genuinely embeds the key via the URL, so
        # the guard below is load-bearing rather than testing a no-op.
        with pytest.raises(httpx.HTTPStatusError) as raw:
            response.raise_for_status()
        assert _SECRET in str(raw.value)

        client = _client_with_mocked_http(AsyncMock(return_value=response))
        try:
            with pytest.raises(FMPRequestError) as exc:
                await client.get_profile("AAPL")
        finally:
            await client.close()

        assert _SECRET not in str(exc.value)
        assert exc.value.status_code == 403
        assert str(exc.value) == "FMP API request failed (403)"

    @pytest.mark.asyncio
    async def test_request_error_does_not_leak_apikey(self):
        # A transport-level error (ConnectError) whose message embeds the key.
        leaky = f"cannot connect to https://financialmodelingprep.com/stable/quote?apikey={_SECRET}"
        client = _client_with_mocked_http(AsyncMock(side_effect=httpx.ConnectError(leaky)))
        try:
            with pytest.raises(FMPRequestError) as exc:
                await client.get_quote("AAPL")
        finally:
            await client.close()

        assert _SECRET not in str(exc.value)
        assert str(exc.value) == "FMP API request failed"
        assert exc.value.status_code is None


class TestFmpHeaderAuth:
    """The key must travel ONLY in the `apikey` request header.

    Query-string auth is forbidden: Daytona sandbox placeholders are
    substituted exclusively in HTTPS request headers, so a query-param key
    breaks hosted sandboxes (placeholder egresses verbatim -> FMP 401).
    """

    @pytest.mark.asyncio
    async def test_key_sent_in_header_not_query_or_cache(self):
        request = httpx.Request(
            "GET", "https://financialmodelingprep.com/stable/profile?symbol=AAPL"
        )
        response = httpx.Response(200, request=request, json=[{"symbol": "AAPL"}])
        get_mock = AsyncMock(return_value=response)
        client = _client_with_mocked_http(get_mock)
        try:
            await client.get_profile("AAPL")
        finally:
            await client.close()

        assert get_mock.await_count == 1
        kwargs = get_mock.await_args.kwargs
        assert kwargs["headers"] == {"apikey": _SECRET}
        assert "apikey" not in kwargs["params"]
        assert client._cache
        assert all(_SECRET not in key for key in client._cache)


class TestFmpVendorSpelling:
    """FMP resolves Shanghai only as ``.SS``; a ``.SH`` caller still lands."""

    @pytest.mark.asyncio
    async def test_display_spelling_reaches_fmp_in_its_own_form(self):
        request = httpx.Request("GET", "https://financialmodelingprep.com/stable/quote")
        response = httpx.Response(200, request=request, json=[{"symbol": "600519.SS"}])
        get_mock = AsyncMock(return_value=response)
        client = _client_with_mocked_http(get_mock)
        try:
            await client._make_request("quote", {"symbol": "600519.SH"})
            await client._make_request("batch-quote", {"symbols": "600519.SH,AAPL"})
        finally:
            await client.close()

        sent = [call.kwargs["params"] for call in get_mock.await_args_list]
        assert sent[0]["symbol"] == "600519.SS"
        assert sent[1]["symbols"] == "600519.SS,AAPL"

    @pytest.mark.asyncio
    async def test_rows_come_back_in_our_spelling(self):
        request = httpx.Request("GET", "https://financialmodelingprep.com/stable/batch-quote")
        response = httpx.Response(
            200, request=request, json=[{"symbol": "600519.SS"}, {"symbol": "^GSPC"}]
        )
        get_mock = AsyncMock(return_value=response)
        client = _client_with_mocked_http(get_mock)
        try:
            fresh = await client._make_request("batch-quote", {"symbols": "600519.SH,^GSPC"})
            cached = await client._make_request("batch-quote", {"symbols": "600519.SH,^GSPC"})
        finally:
            await client.close()

        assert get_mock.await_count == 1
        assert [r["symbol"] for r in fresh] == ["600519.SH", "^GSPC"]
        assert [r["symbol"] for r in cached] == ["600519.SH", "^GSPC"]

    @pytest.mark.asyncio
    async def test_blank_entries_drop_and_an_unspellable_symbol_never_reaches_fmp(self):
        request = httpx.Request("GET", "https://financialmodelingprep.com/stable/batch-quote")
        get_mock = AsyncMock(return_value=httpx.Response(200, request=request, json=[]))
        client = _client_with_mocked_http(get_mock)
        try:
            await client._make_request("batch-quote", {"symbols": "600519.SH,, AAPL,"})
            with pytest.raises(FMPRequestError) as exc:
                await client._make_request("batch-quote", {"symbols": "AAPL,EUR/USD"})
        finally:
            await client.close()

        assert get_mock.await_count == 1
        assert get_mock.await_args.kwargs["params"]["symbols"] == "600519.SS,AAPL"
        # A status, so the sandbox tools report it as a bad argument, with the reason.
        assert exc.value.status_code == 400
        assert "EUR/USD" in str(exc.value)

    @pytest.mark.asyncio
    async def test_a_snapshot_batch_skips_only_the_symbol_fmp_cannot_be_asked_for(self):
        from unittest.mock import patch

        from src.data_client.fmp.data_source import FMPDataSource

        asked: list[list[str]] = []

        class _FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def get_batch_quotes(self, symbols):
                asked.append(symbols)
                return [{"symbol": s, "price": 1.0} for s in symbols]

        with patch("src.data_client.fmp.data_source.FMPClient", return_value=_FakeClient()):
            snaps = await FMPDataSource().get_snapshots(["AAPL", "EUR/USD", "MSFT"])
            # Nothing left to ask: the refusal itself is the answer.
            with pytest.raises(ValueError, match="EUR/USD"):
                await FMPDataSource().get_snapshots(["EUR/USD"])

        assert [s["symbol"] for s in snaps] == ["AAPL", "MSFT"]
        assert asked == [["AAPL", "MSFT"]]


class TestEconomicIndicatorWindowWalk:
    """Stable ignores ``limit`` and answers at most a stretch of days ending at
    ``to`` (a year here, as measured); a read learns that span from a decade
    asked first and never asks wider."""

    @staticmethod
    def _client(rows_for, clamp_days=365, failing_to=None):
        from datetime import date, timedelta

        async def fmp_get(url, params, headers):
            request = httpx.Request("GET", url)
            if params["to"] == failing_to:
                return httpx.Response(403, request=request, json={})
            hi = date.fromisoformat(params["to"])
            lo = max(date.fromisoformat(params["from"]), hi - timedelta(days=clamp_days - 1))
            return httpx.Response(200, request=request, json=rows_for(lo, hi))

        get_mock = AsyncMock(side_effect=fmp_get)
        return get_mock, _client_with_mocked_http(get_mock)

    @staticmethod
    def _quarters(newest):
        from datetime import date

        quarters = [date(y, m, 1) for y in range(2026, 1999, -1) for m in (10, 7, 4, 1)]
        return [q for q in quarters if q <= newest]

    @pytest.mark.asyncio
    async def test_the_window_is_the_span_the_first_answer_held(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client
        from src.data_client.fmp import walk

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 19))
        quarters = self._quarters(date(2026, 4, 1))
        get_mock, client = self._client(lambda lo, hi: [
            {"name": "GDP", "date": q.isoformat(), "value": q.year} for q in quarters if lo <= q <= hi
        ])
        try:
            rows = await client.get_economic_indicators("GDP", limit=10)
        finally:
            await client.close()

        assert [r["date"] for r in rows] == [q.isoformat() for q in quarters[:10]]
        sent = [call.kwargs["params"] for call in get_mock.await_args_list]
        assert all("limit" not in p for p in sent)
        days = [
            (date.fromisoformat(p["to"]) - date.fromisoformat(p["from"])).days + 1 for p in sent
        ]
        # A decade first, which held 2026-01-01 onward: 292 days, never wider
        # than the year the answer is clamped to; four windows for eight rows.
        assert days == [walk.INDICATOR_PROBE_DAYS, 292, 292, 292, 292]
        tos = [date.fromisoformat(p["to"]) for p in sent]
        assert tos[1] == date(2025, 12, 31)
        assert all((a - b).days == 292 for a, b in zip(tos[1:], tos[2:]))

    @pytest.mark.asyncio
    async def test_a_first_answer_with_nothing_falls_back_to_91_day_windows(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 19))
        quarters = self._quarters(date(2024, 4, 1))
        get_mock, client = self._client(lambda lo, hi: [
            {"name": "GDP", "date": q.isoformat(), "value": q.year} for q in quarters if lo <= q <= hi
        ])
        try:
            rows = await client.get_economic_indicators("GDP", limit=2)
        finally:
            await client.close()

        assert [r["date"] for r in rows] == ["2024-04-01", "2024-01-01"]
        second = get_mock.await_args_list[1].kwargs["params"]
        assert (second["from"], second["to"]) == ("2026-04-21", "2026-07-20")

    @pytest.mark.asyncio
    async def test_a_window_not_needed_may_fail(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 19))
        quarters = self._quarters(date(2026, 4, 1))
        # Two rows held, five wanted: a batch of two windows, the first of
        # which fills the five, so the second's refusal is not needed.
        _, client = self._client(
            lambda lo, hi: [{"date": q.isoformat(), "value": 1} for q in quarters if lo <= q <= hi],
            failing_to="2025-03-14",
        )
        try:
            rows = await client.get_economic_indicators("GDP", limit=5)
        finally:
            await client.close()
        assert [r["date"] for r in rows] == [q.isoformat() for q in quarters[:5]]

    @pytest.mark.asyncio
    async def test_a_sparse_series_walks_windows_no_narrower_than_91_days(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 1, 15))
        years = [date(y, 1, 1) for y in range(2026, 1999, -1)]
        get_mock, client = self._client(lambda lo, hi: [
            {"date": y.isoformat(), "value": y.year} for y in years if lo <= y <= hi
        ])
        try:
            rows = await client.get_economic_indicators("inflation", limit=10)
        finally:
            await client.close()
        # The decade held only 2026-01-01, a 15-day span; 91-day windows still
        # reach ten yearly rows well inside the window cap.
        assert [r["date"] for r in rows] == [y.isoformat() for y in years[:10]]
        assert not rows.truncated and get_mock.await_count < 64

    @pytest.mark.asyncio
    async def test_a_window_refused_once_with_a_429_is_asked_again(self, monkeypatch):
        from datetime import date, timedelta

        import src.data_client.fmp.fmp_client as fmp_client
        from src.data_client.fmp import walk

        async def sleep(seconds):
            return None

        monkeypatch.setattr(walk.asyncio, "sleep", sleep)
        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 19))
        quarters = self._quarters(date(2026, 4, 1))
        left = {"n": 1}

        async def fmp_get(url, params, headers):
            request = httpx.Request("GET", url)
            if params["to"] == "2025-12-31" and left["n"]:
                left["n"] -= 1
                return httpx.Response(429, request=request, json={})
            hi = date.fromisoformat(params["to"])
            lo = max(date.fromisoformat(params["from"]), hi - timedelta(days=364))
            rows = [{"date": q.isoformat(), "value": 1} for q in quarters if lo <= q <= hi]
            return httpx.Response(200, request=request, json=rows)

        client = _client_with_mocked_http(AsyncMock(side_effect=fmp_get))
        try:
            rows = await client.get_economic_indicators("GDP", limit=3)
        finally:
            await client.close()
        assert [r["date"] for r in rows] == [q.isoformat() for q in quarters[:3]]

    @pytest.mark.asyncio
    async def test_a_needed_window_s_failure_fails_the_read(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 19))
        quarters = self._quarters(date(2026, 4, 1))
        _, client = self._client(
            lambda lo, hi: [{"date": q.isoformat(), "value": 1} for q in quarters if lo <= q <= hi],
            failing_to="2025-12-31",
        )
        try:
            with pytest.raises(fmp_client.FMPRequestError):
                await client.get_economic_indicators("GDP", limit=3)
        finally:
            await client.close()


class TestRangeWalk:
    """Treasury rates and both calendars answer at most the 90 days ending at
    ``to``, and an earnings answer at most 4,000 rows, the rest a page away.
    Rates keep the end of their range, a calendar its start."""

    @staticmethod
    def _client(rows_for):
        async def fmp_get(url, params, headers):
            return httpx.Response(200, request=httpx.Request("GET", url), json=rows_for(params))

        get_mock = AsyncMock(side_effect=fmp_get)
        return get_mock, _client_with_mocked_http(get_mock)

    @pytest.mark.asyncio
    async def test_earnings_are_read_in_fortnights_oldest_first_and_a_row_read_twice_is_kept_once(
        self, monkeypatch
    ):
        from src.data_client.fmp import walk

        monkeypatch.setattr(walk, "EARNINGS_PAGE_ROWS", 2)
        a = {"symbol": "A", "date": "2026-10-14", "epsActual": 1.0}
        answers = {
            ("2026-10-01", "2026-10-14", None): [{"symbol": "B", "date": "2026-10-14"}, a],
            # A row landed between the calls and pushed A onto the next page,
            # beside a second A of FMP's own that differs.
            ("2026-10-01", "2026-10-14", 1): [dict(a), {**a, "epsActual": 2.0}],
            ("2026-10-01", "2026-10-14", 2): [],
            ("2026-10-15", "2026-10-16", None): [{"symbol": "C", "date": "2026-10-16"}],
        }
        get_mock, client = self._client(
            lambda p: answers[(p["from"], p["to"], p.get("page"))]
        )
        try:
            rows = await client.get_earnings_calendar_by_date("2026-10-01", "2026-10-16")
        finally:
            await client.close()

        assert [(r["symbol"], r.get("epsActual")) for r in rows] == [
            ("B", None), ("A", 1.0), ("A", 2.0), ("C", None)
        ]
        assert not rows.truncated and get_mock.await_count == 4

    @pytest.mark.asyncio
    async def test_a_lone_earnings_bound_is_one_read_sorted_oldest_first(self):
        from src.data_client.fmp.walk import Walk

        get_mock, client = self._client(
            lambda p: [{"symbol": "C", "date": "2026-10-16"}, {"symbol": "A", "date": "2026-10-02"}]
        )
        try:
            rows = await client.get_earnings_calendar_by_date("2026-10-01")
        finally:
            await client.close()

        assert isinstance(rows, Walk) and not rows.truncated
        assert [r["symbol"] for r in rows] == ["A", "C"]
        assert get_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_a_read_the_call_cap_stops_keeps_the_end_its_reader_wants_and_says_so(self):
        from src.data_client.fmp import walk

        get_mock, client = self._client(lambda p: [{"date": p["to"]}, {"date": p["from"]}])
        try:
            rates = await client.get_treasury_rates("2020-01-01", "2026-10-16")
            # A calendar is read for its nearest dates, so it keeps its start.
            events = await client.get_economic_calendar("2020-01-01", "2026-10-16")
        finally:
            await client.close()

        assert get_mock.await_count == 2 * walk.RANGE_MAX_CALLS
        assert rates.truncated and rates[0]["date"] == "2026-10-16"
        dates = [r["date"] for r in events]
        assert events.truncated and dates == sorted(dates) and dates[0] == "2020-01-01"

    @pytest.mark.asyncio
    async def test_a_lone_treasury_start_runs_to_today(self, monkeypatch):
        from datetime import date

        import src.data_client.fmp.fmp_client as fmp_client

        monkeypatch.setattr(fmp_client, "_today_utc", lambda: date(2026, 10, 16))
        get_mock, client = self._client(lambda p: [])
        try:
            await client.get_treasury_rates("2026-10-01")
        finally:
            await client.close()

        params = get_mock.await_args.kwargs["params"]
        assert (params["from"], params["to"]) == ("2026-10-01", "2026-10-16")

    @pytest.mark.asyncio
    async def test_an_indicator_the_window_cap_stops_short_of_limit_says_so(self, monkeypatch):
        from src.data_client.fmp import walk

        monkeypatch.setattr(walk, "INDICATOR_MAX_WINDOWS", 2)
        get_mock, client = self._client(lambda p: [{"date": p["to"], "value": 1.0}])
        try:
            short = await client.get_economic_indicators("CPI", limit=5)
            whole = await client.get_economic_indicators("CPI", limit=2)
        finally:
            await client.close()

        assert (len(short), short.truncated) == (2, True)
        assert (len(whole), whole.truncated) == (2, False)

    @pytest.mark.asyncio
    async def test_a_refusal_part_way_fails_the_read(self):
        def rows_for(params):
            if params["to"] != "2026-10-16":
                raise FMPRequestError("FMP API request failed (403)", status_code=403)
            return [{"date": "2026-10-16", "year10": 4.0}]

        get_mock, client = self._client(rows_for)
        try:
            # The newest window answers and the next is refused: ``truncated``
            # means a size cap, so the read fails rather than coming back cut.
            with pytest.raises(FMPRequestError):
                await client.get_treasury_rates("2026-01-01", "2026-10-16")
        finally:
            await client.close()

        assert get_mock.await_count == 2

    @staticmethod
    def _rated(refusals, retry_after):
        """Treasury rates whose second window is refused with a 429 ``refusals`` times."""
        left = {"n": refusals}

        def rows_for(params):
            if params["to"] != "2026-10-16" and left["n"]:
                left["n"] -= 1
                raise FMPRequestError(
                    "FMP API request failed (429)", status_code=429, retry_after=retry_after
                )
            return [{"date": params["to"], "year10": 4.0}]

        return rows_for

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("refusals", "retry_after", "calls", "waits"),
        [
            # Asked again after the wait FMP named, then answered.
            (1, 0.5, 3, [0.5]),
            # No wait named: a doubling backoff, inside the total wait.
            (2, None, 4, [1.0, 2.0]),
        ],
    )
    async def test_a_429_is_asked_again_within_the_bounds(
        self, monkeypatch, refusals, retry_after, calls, waits
    ):
        from src.data_client.fmp import walk

        slept = []

        async def sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr(walk.asyncio, "sleep", sleep)
        get_mock, client = self._client(self._rated(refusals, retry_after))
        try:
            rows = await client.get_treasury_rates("2026-07-01", "2026-10-16")
        finally:
            await client.close()
        assert (len(rows), rows.truncated, get_mock.await_count, slept) == (2, False, calls, waits)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("refusals", "retry_after", "calls", "waits"),
        [
            # Refused a third time: the two retries are spent.
            (3, 0.5, 4, [0.5, 0.5]),
            # A wait past the total cap is not waited for.
            (1, 30.0, 2, []),
        ],
    )
    async def test_a_429_past_the_bounds_fails_the_read(
        self, monkeypatch, refusals, retry_after, calls, waits
    ):
        from src.data_client.fmp import walk

        slept = []

        async def sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr(walk.asyncio, "sleep", sleep)
        get_mock, client = self._client(self._rated(refusals, retry_after))
        try:
            with pytest.raises(FMPRequestError):
                await client.get_treasury_rates("2026-07-01", "2026-10-16")
        finally:
            await client.close()
        assert (get_mock.await_count, slept) == (calls, waits)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("header", "wait"),
        [
            ("2", 2.0),
            ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),
            ("Wed, 21 Oct 2015 07:28:00", 0.0),
            ("Wed, 21 Oct 2099 07:28:00 GMT", "future"),
            ("nan", None),
            ("soon", None),
        ],
    )
    async def test_a_429_carries_the_retry_after_fmp_sent(self, header, wait):
        response = httpx.Response(
            429, request=httpx.Request("GET", "https://fmp.test/x"), headers={"Retry-After": header}
        )
        client = _client_with_mocked_http(AsyncMock(return_value=response))
        try:
            with pytest.raises(FMPRequestError) as exc:
                await client.get_treasury_rates()
        finally:
            await client.close()
        assert exc.value.status_code == 429
        if wait == "future":
            assert exc.value.retry_after > 4.0
        else:
            assert exc.value.retry_after == wait

    @pytest.mark.asyncio
    async def test_a_timeout_is_not_tried_again(self):
        get_mock = AsyncMock(side_effect=httpx.ReadTimeout("slow"))
        client = _client_with_mocked_http(get_mock)
        try:
            with pytest.raises(FMPRequestError, match="timed out"):
                await client.get_treasury_rates()
        finally:
            await client.close()
        assert get_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_a_dropped_connection_is_tried_again(self, monkeypatch):
        import src.data_client.fmp.fmp_client as fmp_client

        async def sleep(seconds):
            return None

        monkeypatch.setattr(fmp_client.asyncio, "sleep", sleep)
        ok = httpx.Response(200, request=httpx.Request("GET", "https://fmp.test/x"), json=[{"a": 1}])
        get_mock = AsyncMock(side_effect=[httpx.RemoteProtocolError("goaway"), ok])
        client = _client_with_mocked_http(get_mock)
        try:
            assert await client.get_treasury_rates() == [{"a": 1}]
        finally:
            await client.close()
        assert get_mock.await_count == 2


class TestSearchMerge:
    @pytest.mark.asyncio
    async def test_name_matches_survive_a_full_symbol_answer(self):
        """``search-symbol`` answering a whole page must not cut the name
        matches: on ``nv`` they are what holds NVDA."""
        by_endpoint = {
            "search-symbol": [{"symbol": f"NV{i:02d}"} for i in range(3)],
            "search-name": [{"symbol": "NVDA"}, {"symbol": "NV01"}],
        }
        client = FMPClient(api_key=_SECRET)
        client._make_request = AsyncMock(side_effect=lambda endpoint, params: by_endpoint[endpoint])
        rows = await client.search_stocks(query="nv", limit=3)
        assert [r["symbol"] for r in rows] == ["NV00", "NV01", "NV02", "NVDA"]


class TestPriceReadCaps:
    """An EOD answer holds at most 5,000 bars and an intraday one a fixed span
    of days ending at ``to``, the newest kept; a read that reaches either says so."""

    @staticmethod
    def _client(rows):
        async def fmp_get(url, params, headers):
            return httpx.Response(200, request=httpx.Request("GET", url), json=rows)

        return _client_with_mocked_http(AsyncMock(side_effect=fmp_get))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("count", "start", "truncated"),
        [(5000, "2000-01-01", True), (4999, "2000-01-01", False), (5000, "2010-01-01", False)],
    )
    async def test_an_eod_answer_at_the_record_cap_short_of_the_start_says_so(
        self, count, start, truncated
    ):
        from datetime import date

        newest = date(2026, 10, 16)
        oldest = date(2010, 1, 1)
        step = (newest - oldest) / (count - 1)
        rows = [{"date": (oldest + step * i).isoformat()[:10]} for i in range(count)][::-1]
        client = self._client(rows)
        try:
            out = await client.get_stock_price("0700.HK", start, "2026-10-16")
        finally:
            await client.close()
        assert out.truncated is truncated

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("interval", "start", "truncated"),
        [("5min", "2026-10-06", True), ("5min", "2026-10-07", False), ("1hour", "2026-07-01", True)],
    )
    async def test_an_intraday_window_past_fmps_span_says_so(self, interval, start, truncated):
        client = self._client([{"date": "2026-10-16 15:55:00"}])
        try:
            out = await client.get_intraday_chart("0700.HK", interval, start, "2026-10-16")
        finally:
            await client.close()
        assert out.truncated is truncated


class TestEconomicIndicatorUnknownName:
    @pytest.mark.asyncio
    async def test_a_name_fmp_lacks_is_refused_as_invalid_input(self):
        import src.data_client.fmp.fmp_client as fmp_client

        async def fmp_get(url, params, headers):
            # FMP answers a name it lacks with a 200 whose body is the text "Invalid name".
            return httpx.Response(200, request=httpx.Request("GET", url), text="Invalid name")

        client = _client_with_mocked_http(AsyncMock(side_effect=fmp_get))
        try:
            with pytest.raises(fmp_client.FMPRequestError) as info:
                await client.get_economic_indicators("PPI", limit=3)
        finally:
            await client.close()

        assert info.value.status_code == 422
        assert str(info.value) == "FMP has no economic indicator named 'PPI'"

    @pytest.mark.asyncio
    async def test_any_other_non_json_answer_stays_an_upstream_failure(self):
        async def fmp_get(url, params, headers):
            return httpx.Response(200, request=httpx.Request("GET", url), text="<html>busy</html>")

        client = _client_with_mocked_http(AsyncMock(side_effect=fmp_get))
        try:
            with pytest.raises(ValueError) as info:
                await client.get_economic_indicators("CPI", limit=3)
        finally:
            await client.close()

        assert not hasattr(info.value, "status_code")
