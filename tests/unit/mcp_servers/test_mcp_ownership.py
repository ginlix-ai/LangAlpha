"""The 13F tools: the shared answers (mcp_servers/_ownership.py), the reader without ginlix-data
(mcp_servers/_ownership_direct.py) and the tools over them (mcp_servers/_ownership_tools.py)."""

from __future__ import annotations

import pathlib
import sys
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from mcp_servers import _ownership as own
from mcp_servers import _ownership_direct as direct
from mcp_servers._envelope import page, page_error

_TOOLS = "mcp_servers._ownership_tools"
_DIRECT = "mcp_servers._ownership_direct"


# -- periods ------------------------------------------------------------------


def test_a_period_must_end_a_calendar_quarter():
    assert own.quarter_end("2026-06-30") == "2026-06-30"
    assert own.quarter_end(None) is None
    for bad in ("2026-05-31", "2026-06-29", "June 2026"):
        with pytest.raises(ValueError):
            own.quarter_end(bad)


def test_the_default_period_is_the_engines_expected_period_in_new_york():
    with patch(f"{_DIRECT}._today", return_value=date(2026, 11, 18)):
        assert direct._default_period() == "2026-06-30"  # two days past the deadline: late filings still land
    with patch(f"{_DIRECT}._today", return_value=date(2026, 11, 19)):
        assert direct._default_period() == "2026-09-30"


@pytest.mark.asyncio
async def test_without_edgartools_holders_still_take_the_default_period():
    from mcp_servers._ownership_tools import get_institutional_holders

    holders = AsyncMock(return_value=[])
    with _no_ginlix(), patch.dict(sys.modules, {"edgar": None}), \
            patch(f"{_DIRECT}.get_fmp_client", AsyncMock(return_value=AsyncMock())), \
            patch(f"{_DIRECT}.fmp_ownership.holders", holders), \
            patch(f"{_DIRECT}._today", return_value=date(2026, 11, 19)):
        for name in [n for n in sys.modules if n.startswith(("edgar.", "edgar_engine."))]:
            del sys.modules[name]
        got = await get_institutional_holders("AAPL")
        assert (got["period"], got["holders_total"]) == ("2026-09-30", 0)
        assert holders.await_args.args[1:] == ("AAPL", "2026-09-30")


def test_the_mirrored_answers_name_their_package_on_one_import_line():
    """LangAlpha Data's sync copies _ownership.py verbatim but for that line, and refuses any other mention."""
    source = pathlib.Path(own.__file__).read_text()
    assert source.count("mcp_servers") == 1
    assert [line for line in source.splitlines() if "mcp_servers" in line][0].startswith(
        "from mcp_servers._envelope import ")


# -- paging -------------------------------------------------------------------


def test_a_limit_over_the_cap_is_refused_not_clamped():
    assert page_error(500, 0, 500) is None
    refused = page_error(501, 0, 500, symbol="AAPL")
    assert (refused["error"], refused["symbol"]) == ("invalid_argument", "AAPL")
    assert "500" in refused["detail"] and "limit" in refused["detail"]
    assert page_error(0, 0, 500)["error"] == "invalid_argument"
    assert page_error(1, -1, 500)["error"] == "invalid_argument"


def test_pages_say_where_the_next_starts_and_null_on_the_last():
    rows = list(range(7))
    assert page(rows, 0, 3) == ([0, 1, 2], 3)
    assert page(rows, 6, 3) == ([6], None)
    assert page(rows, 3, 4) == ([3, 4, 5, 6], None)
    assert page(rows, 20, 3) == ([], None)


# -- LangAlpha Data's routes ---------------------------------------------------


def _status(code: int, detail: str = "nope", **body) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://gd.test/x")
    return httpx.HTTPStatusError("x", request=request, response=httpx.Response(code, json={"detail": detail, **body},
                                                                                request=request))


def _reader(**kw) -> AsyncMock:
    reader = AsyncMock()
    for name, value in kw.items():
        getattr(reader, name).side_effect = value if isinstance(value, Exception) else None
        if not isinstance(value, Exception):
            getattr(reader, name).return_value = value
    return reader


_HOLDERS = {"rows": [{"holder": "A", "filer_cik": "0000000001", "shares": 10.0, "value": 5.0, "pctheld": 0.1,
                      "pctchange": None, "value_status": "as_filed", "filing_date": "2026-08-01",
                      "accession": "a", "url": "u-a", "cusip": "x", "date_reported": "2026-06-30"}],
            "period": "2026-06-30", "total": 7, "offset": 0, "next_offset": 1, "build": "g1", "publisher": "sec"}


@pytest.mark.asyncio
async def test_holders_from_the_route_carry_the_paging_echo_and_the_documented_fields():
    reader = _reader(get_holders_page_v2=_HOLDERS)
    got = await own.holders_from_route(reader, "AAPL.XNAS", "AAPL", period=None, limit=1, offset=0, build=None)
    assert {k: got[k] for k in ("symbol", "period", "build", "holders_total", "offset", "next_offset", "count",
                                "source")} == {"symbol": "AAPL", "period": "2026-06-30", "build": "g1",
                                               "holders_total": 7, "offset": 0, "next_offset": 1, "count": 1,
                                               "source": "sec"}
    assert list(got["data"][0]) == list(own.HOLDER_FIELDS) and got["data"][0]["shares"] == 10
    assert got["data"][0]["url"] == "u-a"
    assert "truncated" not in got
    reader.get_holders_page_v2.assert_awaited_once_with("AAPL.XNAS", period=None, limit=1, offset=0, build=None)


@pytest.mark.asyncio
async def test_a_page_of_a_replaced_build_is_the_rebuilt_error():
    reader = _reader(get_holders_page_v2=_status(409, "build g1 is no longer served"))
    got = await own.holders_from_route(reader, "AAPL.XNAS", "AAPL", period="2026-06-30", limit=5, offset=5,
                                       build="g1")
    assert (got["error"], got["symbol"]) == ("rebuilt", "AAPL")
    assert got["detail"] == ("the build passed was replaced since the last page; start again at offset 0 with the "
                             "same period and no build")
    unknown = await own.holders_from_route(_reader(get_holders_page_v2=_status(422, "bad")), "K", "K", period=None,
                                           limit=5, offset=0, build=None)
    assert (unknown["error"], unknown["detail"]) == ("invalid_argument", "bad")
    assert (await own.holders_from_route(_reader(get_holders_page_v2=httpx.ConnectError("x")), "K", "K",
                                         period=None, limit=5, offset=0, build=None))["error"] == "upstream_error"
    assert (await own.holders_from_route(AsyncMock(), "K", "K", period="2026-05-31", limit=5, offset=0,
                                         build=None))["error"] == "invalid_argument"
    none = await own.holders_from_route(_reader(get_holders_page_v2=None), "K", "K", period="2026-09-30", limit=5,
                                        offset=0, build=None)
    assert (none["error"], none["symbol"]) == ("not_found", "K")
    assert none["detail"] == ("no 13F holders are stored for K for 2026-09-30; pass another quarter end as period, "
                              "or check that K is a US-listed ticker")
    latest = await own.holders_from_route(_reader(get_holders_page_v2=None), "K", "K", period=None, limit=5, offset=0,
                                          build=None)
    assert latest["detail"] == "no 13F holders are stored for K; check that it is a US-listed ticker"


@pytest.mark.asyncio
async def test_only_a_503_the_route_marks_not_ready_reaches_the_agent_word_for_word():
    hint = ("13F period 2026-06-30, the default, is still loading and is usually complete within a day. "
            "Pass period=2026-03-31 for the latest complete quarter.")

    async def detail(failure: Exception) -> str:
        got = await own.holders_from_route(_reader(get_holders_page_v2=failure), "K", "K", period=None, limit=5,
                                           offset=0, build=None)
        assert got["error"] == "upstream_error"
        return got["detail"]

    assert await detail(_status(503, hint, reason="not_ready")) == hint
    assert await detail(_status(503, "Every provider failed for institutional_holders: sec: UpstreamError")) == \
        "ownership data request failed (503); a retry may succeed"
    request = httpx.Request("GET", "http://gd.test/x")
    bare = httpx.HTTPStatusError("x", request=request, response=httpx.Response(502, text="Bad Gateway",
                                                                               request=request))
    assert await detail(bare) == "ownership data request failed (502); a retry may succeed"


_FILER = {"rows": [{"ticker": "AAPL", "issuer": "APPLE", "cusip": "037833100", "title_class": "COM", "sh_prn": "SH",
                    "put_call": None, "shares": 5.0, "value": 9.0, "value_status": "as_filed", "weight": 1.0,
                    "change": "new", "shares_prior": 0, "accession": "a", "filing_date": "2026-08-01"}],
          "cik": "0001067983", "period": "2026-06-30", "filer_name": "BERKSHIRE HATHAWAY INC",
          "filing_date": "2026-08-14", "url": "u-r", "prior_url": "u-p", "total": 1, "offset": 0, "next_offset": None,
          "build": "g2", "publisher": "sec"}


@pytest.mark.asyncio
@pytest.mark.parametrize("manager", ["1067983", "0001067983", " 1067983 "])
async def test_a_manager_by_cik_reads_its_positions(manager):
    reader = _reader(get_filer_holdings_v2=_FILER)
    got = await own.manager_from_route(reader, manager, period=None, limit=500, offset=0, build=None)
    assert {k: got[k] for k in ("manager", "filer_cik", "period", "build", "filing_date", "url", "prior_url",
                                "positions_total", "offset", "next_offset", "count")} == {
        "manager": "BERKSHIRE HATHAWAY INC", "filer_cik": "0001067983", "period": "2026-06-30", "build": "g2",
        "filing_date": "2026-08-14", "url": "u-r", "prior_url": "u-p", "positions_total": 1, "offset": 0,
        "next_offset": None, "count": 1}
    assert list(got["data"][0]) == list(own.POSITION_FIELDS) and got["data"][0]["accession"] == "a"
    reader.get_filer_holdings_v2.assert_awaited_once_with(1067983, period=None, limit=500, offset=0, build=None)
    reader.find_filers_v2.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_matching_several_managers_lists_them():
    many = {"rows": [{"filer_cik": f"{i:010d}", "filer_name": f"Alpha {i}", "period": "2026-06-30"}
                     for i in range(20)], "total": 25}
    got = await own.manager_from_route(_reader(find_filers_v2=many), "Alpha", period=None, limit=500, offset=0,
                                       build=None)
    assert got["error"] == "invalid_argument" and got["manager"] == "Alpha"
    assert len(got["candidates"]) == 20 and got["candidates"][0] == {"manager": "Alpha 0", "filer_cik": "0000000000"}
    assert "25" in got["detail"]
    one = {"rows": [{"filer_cik": "0001067983", "filer_name": "BERKSHIRE HATHAWAY INC"}], "total": 1}
    reader = _reader(find_filers_v2=one, get_filer_holdings_v2=_FILER)
    assert (await own.manager_from_route(reader, "Berkshire Hathaway", period="2019-12-31", limit=5, offset=0,
                                         build=None))["filer_cik"] == "0001067983"
    reader.find_filers_v2.assert_awaited_once_with("Berkshire Hathaway", period="2019-12-31")
    none = await own.manager_from_route(_reader(find_filers_v2={"rows": [], "total": 0}), "Nobody", period=None,
                                        limit=5, offset=0, build=None)
    assert none["error"] == "not_found"


@pytest.mark.asyncio
async def test_a_manager_whose_only_report_could_not_be_read_is_told_so_with_the_filing():
    unread = {**_FILER, "rows": [], "total": 0, "unreadable_url": "u-unread"}
    got = await own.manager_from_route(_reader(get_filer_holdings_v2=unread), "21", period="2026-06-30", limit=5,
                                       offset=0, build=None)
    assert got == {"error": "upstream_error", "manager": "21",
                   "detail": "the 13F holdings report for 2026-06-30 could not be read, so its positions are unknown "
                             "and a retry now will not help; the filing is at u-unread"}


@pytest.mark.asyncio
async def test_a_manager_with_no_report_for_the_period_is_not_found_and_caps_are_refused():
    empty = {**_FILER, "rows": [], "total": 0, "filer_name": None, "filing_date": None, "url": None, "prior_url": None}
    got = await own.manager_from_route(_reader(get_filer_holdings_v2=empty), "1", period="2026-06-30", limit=5,
                                       offset=0, build=None)
    assert (got["error"], got["manager"]) == ("not_found", "1")
    assert (await own.manager_from_route(AsyncMock(), "1", period=None, limit=5001, offset=0,
                                         build=None))["error"] == "invalid_argument"
    rebuilt = await own.manager_from_route(_reader(get_filer_holdings_v2=_status(409)), "1", period=None, limit=5,
                                           offset=5, build="g1")
    assert rebuilt["error"] == "rebuilt"
    unstored = await own.manager_from_route(_reader(get_filer_holdings_v2=None), "1", period="2025-12-31", limit=5,
                                            offset=0, build=None)
    assert (unstored["error"], unstored["detail"]) == ("not_found", "no 13F holdings are stored for 2025-12-31")


@pytest.mark.asyncio
async def test_a_filed_report_listing_no_positions_is_an_empty_page_with_its_filing():
    filed = {**_FILER, "rows": [], "total": 0, "prior_url": None}
    got = await own.manager_from_route(_reader(get_filer_holdings_v2=filed), "1067983", period="2026-06-30", limit=5,
                                       offset=0, build=None)
    assert {k: got.get(k) for k in ("error", "manager", "url", "filing_date", "positions_total", "next_offset",
                                    "count", "data")} == {
        "error": None, "manager": "BERKSHIRE HATHAWAY INC", "url": "u-r", "filing_date": "2026-08-14",
        "positions_total": 0, "next_offset": None, "count": 0, "data": []}
    book = {"filer_name": "MGR", "filing_date": "2026-11-14", "url": "u-empty", "prior_url": None, "rows": []}
    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=True), \
            patch(f"{_DIRECT}.edgar_13f.manager_portfolio", AsyncMock(return_value=book)):
        from mcp_servers._ownership_tools import get_manager_holdings

        got = await get_manager_holdings("1067983", period="2026-06-30")
        assert (got.get("error"), got["url"], got["positions_total"], got["data"]) == (None, "u-empty", 0, [])


# -- the tools without LangAlpha Data -------------------------------------------


def _no_ginlix():
    client = AsyncMock()
    client.ensure.return_value = False
    return patch(f"{_TOOLS}.get_ginlix_mcp_client", return_value=client)


class _FMPError(Exception):
    status_code = 429


@pytest.mark.asyncio
async def test_without_langalpha_data_holders_come_from_fmp_ranked_and_paged():
    from mcp_servers._ownership_tools import get_institutional_holders

    ranked = [{"holder": f"H{i}", "filer_cik": f"{i:010d}", "shares": 100 - i} for i in range(5)]
    with _no_ginlix(), patch(f"{_DIRECT}.get_fmp_client", AsyncMock(return_value=AsyncMock())), \
            patch(f"{_DIRECT}.fmp_ownership.holders", AsyncMock(return_value=ranked)) as holders, \
            patch(f"{_DIRECT}._today", return_value=date(2026, 11, 26)):
        got = await get_institutional_holders("AAPL", limit=2, offset=2)
        assert (got["period"], got["holders_total"], got["next_offset"], got["build"], got["source"]) == (
            "2026-09-30", 5, 4, None, "fmp")
        assert [r["holder"] for r in got["data"]] == ["H2", "H3"]
        assert holders.await_args.args[1:] == ("AAPL", "2026-09-30")
        assert (await get_institutional_holders("BRK.B"))["symbol"] == "BRK-B"
        assert holders.await_args.args[1] == "BRK-B"
        assert (await get_institutional_holders("AAPL", limit=600))["error"] == "invalid_argument"
        assert (await get_institutional_holders("AAPL", period="2026-08-31"))["error"] == "invalid_argument"
        holders.side_effect = _FMPError("FMP rate limit (429)")
        assert (await get_institutional_holders("AAPL")) == {
            "error": "rate_limited", "detail": "FMP rate limit (429)", "symbol": "AAPL"}
    with _no_ginlix(), patch(f"{_DIRECT}.get_fmp_client", AsyncMock(side_effect=ValueError("no key"))):
        assert (await get_institutional_holders("AAPL")) == {
            "error": "client_unavailable", "detail": "FMP client is unavailable", "symbol": "AAPL"}


@pytest.mark.asyncio
async def test_a_symbol_that_does_not_resolve_is_refused_before_any_source_is_read():
    from mcp_servers import _ownership_tools as tools

    with _no_ginlix() as client, patch(f"{_TOOLS}._issuer", return_value=("BAD!", None)):
        for tool in (tools.get_institutional_holders,):
            got = await tool("BAD!")
            assert got == {"error": "invalid_argument", "detail": "unknown symbol 'BAD!'; pass a US ticker, e.g. \"AAPL\"",
                           "symbol": "BAD!"}
    client.assert_not_called()


@pytest.mark.asyncio
async def test_a_listing_on_a_known_non_us_venue_is_refused_before_any_source_is_read():
    from mcp_servers import _ownership_tools as tools

    with _no_ginlix() as client:
        for tool in (t for t in tools.TOOLS if t is not tools.get_manager_holdings):
            for symbol in ("0700.HK", "SHOP.TO"):
                assert (await tool(symbol)) == {
                    "error": "invalid_argument", "detail": f"unknown symbol {symbol!r}; pass a US ticker, e.g. \"AAPL\"",
                    "symbol": symbol}
    client.assert_not_called()
    assert tools._issuer("TCEHY") == ("TCEHY", "TCEHY.XNYS")


@pytest.mark.asyncio
async def test_without_langalpha_data_a_later_page_asked_with_a_build_is_rebuilt():
    from mcp_servers._ownership_tools import get_institutional_holders, get_manager_holdings

    rebuilt = "the build passed was replaced since the last page; start again at offset 0 with the same period and no build"
    ranked = [{"holder": f"H{i}", "filer_cik": f"{i:010d}", "shares": 100 - i} for i in range(5)]
    with _no_ginlix(), patch(f"{_DIRECT}.get_fmp_client", AsyncMock(return_value=AsyncMock())), \
            patch(f"{_DIRECT}.fmp_ownership.holders", AsyncMock(return_value=ranked)) as holders:
        assert (await get_institutional_holders("AAPL", period="2026-06-30", offset=2, build="g1")) == {
            "error": "rebuilt", "detail": rebuilt, "symbol": "AAPL"}
        holders.assert_not_awaited()
        first = await get_institutional_holders("AAPL", period="2026-06-30", limit=2, build="g1")
        assert (first["holders_total"], first["build"]) == (5, None)
    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=True), \
            patch(f"{_DIRECT}.edgar_13f.manager_portfolio", AsyncMock()) as read:
        assert (await get_manager_holdings("1067983", period="2026-06-30", offset=500, build="g1")) == {
            "error": "rebuilt", "detail": rebuilt, "manager": "1067983"}
        read.assert_not_awaited()
    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=True), \
            patch(f"{_DIRECT}.edgar_13f_filers.find_managers", AsyncMock(return_value={"rows": [], "total": 0})) as find:
        assert (await get_manager_holdings("Berkshire Hathaway", period="2026-06-30", offset=500, build="g1")) == {
            "error": "rebuilt", "detail": rebuilt, "manager": "Berkshire Hathaway"}
        find.assert_not_awaited()
        assert (await get_manager_holdings("Berkshire Hathaway", limit=5001, offset=500, build="g1"))["error"] == (
            "invalid_argument")


@pytest.mark.asyncio
async def test_without_langalpha_data_a_manager_comes_from_edgar():
    from mcp_servers._ownership_tools import get_manager_holdings

    rows = [{"ticker": "AAPL", "shares": 10.0, "change": "new"}, {"ticker": None, "shares": 0.0, "change": "sold_out"}]
    book = {"filer_name": "BERKSHIRE HATHAWAY INC", "filing_date": "2026-11-14", "url": "u-brk", "prior_url": "u-q",
            "rows": rows}
    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=True), \
            patch(f"{_DIRECT}.edgar_13f.manager_portfolio", AsyncMock(return_value=book)) as read, \
            patch(f"{_DIRECT}.edgar_13f_filers.find_managers", AsyncMock(return_value={
                "rows": [{"filer_cik": "0001067983", "filer_name": "BERKSHIRE HATHAWAY INC"}], "total": 1})) as find, \
            patch(f"{_DIRECT}._today", return_value=date(2026, 11, 26)):
        await get_manager_holdings("Berkshire Hathaway", period="2019-12-31")
        find.assert_awaited_with("Berkshire Hathaway", "2019-12-31")  # an older period's filers are searched too
        got = await get_manager_holdings("Berkshire Hathaway", limit=1)
        find.assert_awaited_with("Berkshire Hathaway", None)
        assert (got["url"], got["prior_url"]) == ("u-brk", "u-q")
        assert (got["manager"], got["filer_cik"], got["period"], got["positions_total"], got["next_offset"],
                got["build"], got["source"]) == ("BERKSHIRE HATHAWAY INC", "0001067983", "2026-09-30", 2, 1, None,
                                                 "sec")
        assert got["data"][0]["shares"] == 10 and isinstance(got["data"][0]["shares"], int)
        read.assert_awaited_with(1067983, "2026-09-30", "2026-06-30")
        read.return_value = None  # no report for the quarter, or a CIK EDGAR does not know
        assert (await get_manager_holdings("9999999999")) == {
            "error": "not_found", "manager": "9999999999",
            "detail": "manager '9999999999' reported no 13F holdings for 2026-09-30: it filed no report, or one "
                      "listing no positions; pass another quarter end as period, or check that the manager files "
                      "Form 13F"}
        read.side_effect = RuntimeError("boom")
        assert (await get_manager_holdings("1067983")) == {
            "error": "upstream_error", "detail": "EDGAR request failed; a retry may succeed", "manager": "1067983"}
        read.side_effect = direct.edgar_13f.UnreadableReports("u-unread")
        assert (await get_manager_holdings("1067983")) == {
            "error": "upstream_error", "manager": "1067983",
            "detail": "the 13F holdings report for 2026-09-30 could not be read, so its positions are unknown and a "
                      "retry now will not help; the filing is at u-unread"}


@pytest.mark.asyncio
async def test_without_langalpha_data_a_name_search_lists_its_real_total():
    from mcp_servers._ownership_tools import get_manager_holdings

    found = {"rows": [{"filer_cik": f"{i:010d}", "filer_name": f"TWO SIGMA {i}"} for i in range(25)], "total": 25}
    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=True), \
            patch(f"{_DIRECT}.edgar_13f_filers.find_managers", AsyncMock(return_value=found)):
        got = await get_manager_holdings("Two Sigma")
        assert (got["error"], len(got["candidates"])) == ("invalid_argument", 20)
        assert got["detail"] == ("manager 'Two Sigma' matches 25 13F filers, the first 20 listed; pass one's filer_cik "
                                 "as manager")


@pytest.mark.asyncio
async def test_without_langalpha_data_or_the_engine_edgar_tools_are_client_unavailable():
    from mcp_servers._ownership_tools import get_manager_holdings

    with _no_ginlix(), patch(f"{_DIRECT}.engine_available", return_value=False):
        for manager in ("1067983", "Berkshire"):
            got = await get_manager_holdings(manager)
            assert (got["error"], got["manager"], got["detail"]) == (
                "client_unavailable", manager,
                "manager holdings are not available in this deployment, so a retry will not help")
