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
