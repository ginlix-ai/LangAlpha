"""The open-source build's 13F sources: FMP's holders and EDGAR's portfolios."""

from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest

from data_client import edgar_13f, edgar_13f_filers
from data_client.edgar_access import DEFAULT_IDENTITY, edgar_identity, engine_available, unknown_company
from data_client.fmp import ownership
from data_client.ttl_cache import TtlCache


def _row(cik, shares, *, kind="Share", sold=False, new=False, last=None, ownership_pct=1.0, value=None,
         discretion="SOLE", security="COM", name="APPLE INC"):
    return {"cik": cik, "investorName": f"Mgr {cik}", "sharesNumber": shares, "putCallShare": kind,
            "isSoldOut": sold, "isNew": new, "lastSharesNumber": shares if last is None else last,
            "ownership": ownership_pct, "marketValue": shares * 2 if value is None else value,
            "investmentDiscretion": discretion, "securityCusip": "037833100", "filingDate": "2026-08-14 00:00:00",
            "typeOfSecurity": security, "securityName": name}


def test_fmp_rows_rank_held_share_positions_one_per_filer():
    rows = [_row("0000000002", 50, last=40), _row("0000000001", 50, new=True, last=0), _row("0000000003", 70),
            _row("0000000003", 70),  # FMP repeats a row
            _row("0000000004", 30, discretion="DFND"), _row("0000000004", 20, discretion="SOLE", last=25),
            _row("0000000005", 99, kind="PUT"), _row("0000000006", 0, sold=True)]
    got = ownership.rank(rows, pctheld=True)
    assert [(h["filer_cik"], h["shares"]) for h in got] == [
        ("0000000003", 70), ("0000000001", 50), ("0000000002", 50), ("0000000004", 50)]
    by = {h["filer_cik"]: h for h in got}
    assert by["0000000002"]["pctchange"] == 0.25 and by["0000000001"]["pctchange"] is None
    # 70 shares are 1% (the largest holder's ownership), so 50 shares are 50/7000 whatever FMP rounded theirs to.
    assert by["0000000003"]["pctheld"] == 0.01 and by["0000000004"]["pctheld"] == round(50 / 7000, 6)
    assert by["0000000003"]["filing_date"] == "2026-08-14"
    assert {h["accession"] for h in got} == {None} and {h["value_status"] for h in got} == {None}


def test_pctheld_is_null_for_a_multi_class_issuer_and_a_depositary_listing():
    assert ownership.rank([_row("1", 5)], pctheld=False)[0]["pctheld"] is None
    adr = _row("1", 5, security="SPONSORED ADS", name="TAIWAN SEMICONDUCTOR MFG LTD")
    assert ownership.rank([adr], pctheld=True)[0]["pctheld"] is None


@pytest.mark.asyncio
async def test_fmp_holders_read_every_page_once_and_keep_the_ranking_an_hour(monkeypatch):
    pages = {0: [_row(f"{i:010d}", 1000 - i) for i in range(100)], 1: [_row("0000009999", 1)]}
    client = AsyncMock()
    client.get_institutional_holders.side_effect = lambda s, y, q, page, limit: pages.get(page, [])
    monkeypatch.setattr(ownership, "_ranked", TtlCache(size=2, ttl=3600.0))
    with patch.object(ownership, "_share_classes", AsyncMock(return_value=1)):
        got = await ownership.holders(client, "AAPL", "2026-06-30")
        again = await ownership.holders(client, "AAPL", "2026-06-30")
    assert len(got) == 101 and got is again
    assert {c.args[1:3] for c in client.get_institutional_holders.await_args_list} == {(2026, 2)}
    assert client.get_institutional_holders.await_count == 5  # one batch: page 1 came back short


@pytest.mark.asyncio
async def test_a_page_failing_past_the_lists_end_fails_nothing_but_one_before_it_fails_the_read():
    def answer(full):
        def page(s, y, q, page, limit):
            if page == 1:
                raise RuntimeError("FMP rate limit (429)")
            return [_row(f"{i:010d}", 1000 - i) for i in range(100 if full else 3)] if page == 0 else []
        return page

    client = AsyncMock()
    client.get_institutional_holders.side_effect = answer(full=False)
    assert len(await ownership._all_rows(client, "AAPL", 2026, 2)) == 3
    client.get_institutional_holders.side_effect = answer(full=True)
    with pytest.raises(RuntimeError):
        await ownership._all_rows(client, "AAPL", 2026, 2)

    cancelled = []

    async def stalls(s, y, q, page, limit):
        if page == 0:
            return [_row("0000000001", 10)]
        if page == 1:
            raise RuntimeError("FMP rate limit (429)")
        try:
            await asyncio.sleep(3600)  # a response that keeps streaming
        except asyncio.CancelledError:
            cancelled.append(page)
            raise

    client.get_institutional_holders.side_effect = stalls
    assert len(await asyncio.wait_for(ownership._all_rows(client, "AAPL", 2026, 2), timeout=2)) == 1
    assert sorted(cancelled) == [2, 3, 4]


@dataclass
class _Unit:
    unit: str
    ambiguous: bool = False


@dataclass
class _Holding:
    issuer: str
    title_class: str
    cusip: str
    sh_prn: str
    put_call: str | None
    shares: float
    value: float | None


@dataclass
class _Filing:
    accession: str
    period: str
    schema_version: str
    value_unit: _Unit
    holdings: tuple


def test_edgar_lines_put_values_in_dollars_by_the_reports_unit():
    pytest.importorskip("edgar_engine")
    old = _Filing("a1", "2022-06-30", "X0202", _Unit("thousands"),
                  (_Holding("APPLE", "COM", "037833100", "SH", None, 10.0, 2.0),))
    new = _Filing("a2", "2026-06-30", "X0202", _Unit("dollars"),
                  (_Holding("APPLE", "COM", "037833100", "SH", None, 10.0, 2000.0),
                   _Holding("APPLE", "COM", "037833100", "SH", "CALL", 1.0, None)))
    got = edgar_13f._lines([old, new])
    assert [(r["value"], r["value_status"]) for r in got] == [(2000.0, "rescaled"), (2000.0, "as_filed"),
                                                              (None, "as_filed")]
    assert got[0]["ticker"] == "AAPL"


def test_edgar_lines_keep_a_blank_share_count_unknown():
    pytest.importorskip("edgar_engine")
    f = _Filing("a1", "2026-06-30", "X0202", _Unit("dollars"),
                (_Holding("APPLE", "COM", "037833100", "SH", None, None, 2000.0),))
    assert edgar_13f._lines([f])[0]["shares"] is None


_FORM_IDX = """Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
10-K             BERKSHIRE HATHAWAY INC                                        1067983     2026-08-01  edgar/data/1067983/0000000000-26-000001.txt
13F-HR           BERKSHIRE HATHAWAY INC                                        1067983     2026-08-14  edgar/data/1067983/0000950123-26-000002.txt
13F-HR           BERKSHIRE HATHAWAY HOMESTATE                                  42          2026-08-10  edgar/data/42/0000000042-26-000001.txt
13F-HR/A         BERKSHIRE HATHAWAY INC.                                       1067983     2026-09-02  edgar/data/1067983/0000950123-26-000003.txt
13F-NT           BERKSHIRE NOTICE ONLY LLC                                     77          2026-08-12  edgar/data/77/0000000077-26-000001.txt
"""


def test_the_full_index_lists_every_name_a_13f_hr_filer_used_latest_first(monkeypatch):
    import gzip

    import httpx

    asked = []

    def handler(request):
        asked.append((str(request.url), request.headers["user-agent"]))
        if "QTR4" in str(request.url):
            return httpx.Response(404)
        return httpx.Response(200, content=gzip.compress(_FORM_IDX.encode()))

    real = httpx.Client
    monkeypatch.setattr(edgar_13f_filers.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    monkeypatch.setenv("EDGAR_IDENTITY", "Jane Doe jane@example.com")
    assert edgar_13f_filers._fetch_filers(2026, 3, current=False) == [  # a name changed within the quarter keeps both
        ["1067983", "BERKSHIRE HATHAWAY INC."], ["1067983", "BERKSHIRE HATHAWAY INC"], ["42", "BERKSHIRE HATHAWAY HOMESTATE"]]
    assert asked[-1] == ("https://www.sec.gov/Archives/edgar/full-index/2026/QTR3/form.gz", "Jane Doe jane@example.com")
    assert edgar_13f_filers._fetch_filers(2026, 4, current=True) == []  # EDGAR has not listed the quarter's index yet
    with pytest.raises(httpx.HTTPStatusError):
        edgar_13f_filers._fetch_filers(2026, 4, current=False)


def test_the_filer_list_is_read_once_per_quarter_and_the_current_one_daily(monkeypatch, tmp_path):
    import os
    from datetime import date

    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: tmp_path / "full-index")
    monkeypatch.setattr(edgar_13f_filers.time, "time", lambda: _ny(2026, 2, 10, 12))
    fetched, slept = [], []
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers",
                        lambda y, q, current: fetched.append((y, q, current)) or [[str(q), f"FILER Q{q}"]])
    monkeypatch.setattr(edgar_13f_filers.time, "sleep", slept.append)
    (tmp_path / "full-index").mkdir()
    (tmp_path / "full-index" / "13f-hr-2025q1.json").write_text("{}")
    today = date(2026, 2, 10)
    assert edgar_13f_filers._quarters(today) == [(2026, 1), (2025, 4), (2025, 3), (2025, 2)]
    assert edgar_13f_filers._filers(today) == [("1", "FILER Q1"), ("4", "FILER Q4"), ("3", "FILER Q3"), ("2", "FILER Q2")]
    assert fetched == [(2025, 2, False), (2025, 3, False), (2025, 4, False), (2026, 1, True)]
    assert slept == [edgar_13f_filers._REQUEST_GAP_S] * 3  # 2 requests a second at most
    assert sorted(f.name for f in (tmp_path / "full-index").iterdir()) == [  # 2025q1 kept as an older quarter
        "13f-hr-2025q1.json", "13f-hr-2025q2.json", "13f-hr-2025q3.json", "13f-hr-2025q4.json", "13f-hr-2026q1.json"]
    fetched.clear()
    edgar_13f_filers._filers(today)
    assert fetched == []
    current = tmp_path / "full-index" / "13f-hr-2026q1.json"
    stale = _ny(2026, 2, 10, 12) - edgar_13f_filers._CURRENT_TTL_S - 1
    os.utime(current, (stale, stale))
    edgar_13f_filers._filers(today)
    assert fetched == [(2026, 1, True)]


def test_a_kept_filer_list_not_in_the_current_shape_is_fetched_again(monkeypatch, tmp_path):
    from datetime import date

    folder = tmp_path / "full-index"
    folder.mkdir()
    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: folder)
    monkeypatch.setattr(edgar_13f_filers.time, "time", lambda: _ny(2026, 2, 10, 12))
    monkeypatch.setattr(edgar_13f_filers.time, "sleep", lambda s: None)
    fetched = []
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers",
                        lambda y, q, current: fetched.append((y, q)) or [[str(q), f"FILER Q{q}"]])
    kept = {"2025q2": '{"1": "OLD SHAPE"}', "2025q3": '[{"cik": "3"}]', "2025q4": '[["4", "BROKEN', "2026q1": '[["1", "KEPT"]]'}
    for name, text in kept.items():
        (folder / f"13f-hr-{name}.json").write_text(text)
    assert edgar_13f_filers._filers(date(2026, 2, 10)) == [("1", "KEPT"), ("4", "FILER Q4"), ("3", "FILER Q3"),
                                                          ("2", "FILER Q2")]
    assert fetched == [(2025, 2), (2025, 3), (2025, 4)]
    assert json.loads((folder / "13f-hr-2025q2.json").read_text()) == [["2", "FILER Q2"]]


def _ny(year, month, day, hour):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime(year, month, day, hour, tzinfo=ZoneInfo("America/New_York")).timestamp()


def test_a_quarter_read_before_its_index_closed_is_read_again_until_it_has_closed(monkeypatch, tmp_path):
    import os
    from datetime import date

    folder = tmp_path / "full-index"
    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: folder)
    monkeypatch.setattr(edgar_13f_filers.time, "sleep", lambda s: None)
    fetched, clock = [], [0.0]
    monkeypatch.setattr(edgar_13f_filers.time, "time", lambda: clock[0])
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers", lambda y, q, current: fetched.append((y, q)) or [])

    def at(when, today):
        clock[0] = when
        fetched.clear()
        edgar_13f_filers._filers(today)
        for year, quarter in fetched:  # written at this call's time
            os.utime(folder / f"13f-hr-{year}q{quarter}.json", (when, when))
        return list(fetched)

    assert at(_ny(2026, 12, 31, 10), date(2026, 12, 31))[-1] == (2026, 4)
    assert at(_ny(2027, 1, 1, 9), date(2027, 1, 1)) == [(2027, 1)]  # Q4 read within the day
    assert at(_ny(2027, 1, 1, 11), date(2027, 1, 1)) == [(2026, 4)]  # Q4's index may still grow
    assert at(_ny(2027, 1, 2, 12), date(2027, 1, 2)) == [(2026, 4), (2027, 1)]  # read once the index closed
    assert at(_ny(2027, 2, 1, 12), date(2027, 2, 1)) == [(2027, 1)]  # and never again


def test_a_name_asked_for_an_older_period_is_matched_over_that_periods_filers_too(monkeypatch, tmp_path):
    from datetime import date

    folder = tmp_path / "full-index"
    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: folder)
    monkeypatch.setattr(edgar_13f_filers.time, "sleep", lambda s: None)
    fetched = []
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers",
                        lambda y, q, current: fetched.append((y, q)) or [[f"{y}{q}", f"FILER {y}Q{q}"]])
    today = date(2026, 11, 27)
    assert dict(edgar_13f_filers._filers(today, "2019-12-31"))["20201"] == "FILER 2020Q1"  # due in the quarter after it ends
    assert fetched[0] == (2020, 1)
    assert edgar_13f_filers._filing_quarter("2020-06-30") == (2020, 3)
    fetched.clear()
    assert "20201" not in dict(edgar_13f_filers._filers(today, "2026-06-30"))  # a recent period's quarter is already listed
    assert fetched == []


def test_a_manager_is_found_by_a_name_it_filed_under_in_the_quarter_asked(monkeypatch, tmp_path):
    from datetime import date

    pytest.importorskip("edgar_engine")
    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: tmp_path)
    monkeypatch.setattr(edgar_13f_filers, "QUARTERS", 1)
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers", lambda y, q, current: [
        ["123", "NEW NAME CAPITAL" if y == 2026 else "OLD NAME ADVISERS"], ["456", "OTHER NAME PARTNERS"]])
    filers = edgar_13f_filers._filers(date(2026, 11, 26), "2020-03-31")
    assert filers == [("123", "NEW NAME CAPITAL"), ("456", "OTHER NAME PARTNERS"), ("123", "OLD NAME ADVISERS")]
    monkeypatch.setattr(edgar_13f_filers, "_filers", lambda today, period: filers)
    assert edgar_13f_filers._search("Old Name Advisers", "2020-03-31") == {
        "rows": [{"filer_cik": "0000000123", "filer_name": "OLD NAME ADVISERS"}], "total": 1}
    assert edgar_13f_filers._search("Name", "2020-03-31")["rows"] == [  # a CIK once, under its latest matching name
        {"filer_cik": "0000000123", "filer_name": "NEW NAME CAPITAL"},
        {"filer_cik": "0000000456", "filer_name": "OTHER NAME PARTNERS"}]


def test_older_quarters_lists_are_kept_by_last_use_and_survive_another_process_deleting_them(monkeypatch, tmp_path):
    import os
    from datetime import date

    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: tmp_path)
    monkeypatch.setattr(edgar_13f_filers, "QUARTERS", 1)
    monkeypatch.setattr(edgar_13f_filers.time, "sleep", lambda s: None)
    fetched = []
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers", lambda y, q, current: fetched.append((y, q)) or [[f"{y}{q}", "F"]])
    today = date(2026, 11, 26)
    provisional = tmp_path / "13f-hr-2015q1.json"  # read while its quarter was open, and least recently used
    provisional.write_text("{}")
    os.utime(provisional, (_ny(2015, 2, 1, 12), _ny(2015, 2, 1, 12)))
    for period in ("2020-03-31", "2020-06-30", "2020-03-31", "2020-06-30", "2020-03-31"):
        edgar_13f_filers._filers(today, period)
    assert [f for f in fetched if f[0] == 2020] == [(2020, 2), (2020, 3)]  # alternating periods download once each
    for period in ("2018-03-31", "2018-06-30", "2018-09-30", "2018-12-31"):
        edgar_13f_filers._filers(today, period)
    assert sorted(p.name for p in tmp_path.glob("13f-hr-*.json")) == [  # the HISTORY most recently used, and now
        "13f-hr-2018q2.json", "13f-hr-2018q3.json", "13f-hr-2018q4.json", "13f-hr-2019q1.json", "13f-hr-2020q2.json",
        "13f-hr-2026q4.json"]
    fetched.clear()
    real = pathlib.Path.read_text

    def deleted_meanwhile(path, *args, **kwargs):
        if path.name == "13f-hr-2019q1.json":
            path.unlink()
        return real(path, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "read_text", deleted_meanwhile)
    assert ("20191", "F") in edgar_13f_filers._filers(today, "2018-12-31")  # read again rather than failing
    assert fetched == [(2019, 1)]


def test_a_name_typed_past_edgars_60_character_cut_still_matches(monkeypatch):
    pytest.importorskip("edgar_engine")
    full = "SAMPLE STATE TEACHERS RETIREMENT SYSTEM AND SAVINGS PLAN TRUST FUND"
    listed = full[:60]
    assert len(listed) == 60 and listed.endswith("PLAN TRU")
    monkeypatch.setattr(edgar_13f_filers, "_filers", lambda today, period: [("77", listed), ("78", "SAMPLE STATE BANK")])
    assert edgar_13f_filers._search(full, None) == {"rows": [{"filer_cik": "0000000077", "filer_name": listed}], "total": 1}
    assert edgar_13f_filers._search(full.replace("TEACHERS", "POLICE"), None)["total"] == 0  # a word before the cut differs
    assert edgar_13f_filers._search("Sample State", None)["total"] == 2  # names shorter than the cut compare as before
    assert edgar_13f_filers._search("Sample State Bank Trust", None)["total"] == 0


def test_two_processes_filling_the_filer_list_at_once_both_get_it(monkeypatch, tmp_path):
    import contextlib
    import os
    import threading
    from datetime import date

    monkeypatch.setattr(edgar_13f_filers, "_index_dir", lambda: tmp_path)
    monkeypatch.setattr(edgar_13f_filers, "_quarters", lambda today: [(2026, 4)])
    monkeypatch.setattr(edgar_13f_filers, "_fetch_filers", lambda y, q, current: [["123", "SAMPLE ADVISERS"]])
    monkeypatch.setattr(edgar_13f_filers, "_indexing", contextlib.nullcontext())  # as two processes, each with its own lock
    both_written, real = threading.Barrier(2, timeout=5), os.replace

    def replace(src, dst):
        both_written.wait()
        return real(src, dst)

    monkeypatch.setattr(os, "replace", replace)
    got = []

    def fill():
        try:
            got.append(edgar_13f_filers._filers(date(2026, 11, 26)))
        except Exception as exc:  # noqa: BLE001
            got.append(type(exc).__name__)

    workers = [threading.Thread(target=fill) for _ in range(2)]
    for w in workers:
        w.start()
    for w in workers:
        w.join(10)
    assert got == [[("123", "SAMPLE ADVISERS")]] * 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["13f-hr-2026q4.json"]


def test_a_managers_name_is_matched_over_every_13f_hr_filer(monkeypatch):
    pytest.importorskip("edgar_engine")
    filers = [("1067983", "BERKSHIRE HATHAWAY INC"), ("42", "BERKSHIRE HATHAWAY HOMESTATE"),
              ("720005", "RAYMOND JAMES FINANCIAL INC"), ("1462284", "RAYMOND JAMES FINANCIAL SERVICES ADVISORS, INC.")]
    asked = []
    monkeypatch.setattr(edgar_13f_filers, "_filers", lambda today, period: asked.append(period) or filers)
    assert edgar_13f_filers._search("berkshire hathaway", "2019-12-31") == {
        "rows": [{"filer_cik": "0001067983", "filer_name": "BERKSHIRE HATHAWAY INC"}], "total": 1}
    assert asked == ["2019-12-31"]
    assert edgar_13f_filers._search("Raymond James Financial", None)["rows"] == [
        {"filer_cik": "0000720005", "filer_name": "RAYMOND JAMES FINANCIAL INC"}]
    assert edgar_13f_filers._search("Berkshire", None)["total"] == 2
    monkeypatch.setattr(edgar_13f_filers, "_filers", lambda today, period: (_ for _ in ()).throw(OSError("index unreadable")))
    with pytest.raises(OSError):  # a list that could not be read is never "no filer"
        edgar_13f_filers._search("Berkshire", None)

@dataclass
class _Report:
    accession: str
    filer_name: str
    filed: str
    period: str
    schema_version: str
    value_unit: _Unit
    holdings: tuple
    confidential_omitted: bool | None = None


def _book(monkeypatch, by_period):
    """edgar_13f._read over canned reports: by_period maps a period to its reports, to the accession of an unreadable
    one, to (that accession, the readable reports), or to an exception."""
    pytest.importorskip("edgar_engine")
    from edgar_engine import ThirteenFReports

    monkeypatch.setattr(edgar_13f, "_lines", lambda filings: [
        {"accession": f.accession, "issuer": h.issuer, "title_class": h.title_class, "cusip": h.cusip,
         "sh_prn": h.sh_prn, "put_call": h.put_call, "shares": h.shares, "value": h.value, "value_status": "as_filed",
         "ticker": None} for f in filings for h in f.holdings])

    def reports(cik, period, submissions=None):
        got = by_period.get(period, [])
        if isinstance(got, Exception):
            raise got
        if isinstance(got, str):
            return ThirteenFReports(cik, (), (got,), ())
        if isinstance(got, tuple):
            return ThirteenFReports(cik, tuple(got[1]), (got[0],), ())
        return ThirteenFReports(cik, tuple(got), (), ())

    monkeypatch.setattr("edgar_engine.get_13f_reports", reports)


def _report(period, shares):
    return _Report("a-" + period, "MGR", "2026-11-14", period, "X0202", _Unit("dollars"),
                   (_Holding("APPLE", "COM", "037833100", "SH", None, shares, shares * 2.0),))


def test_an_unreadable_prior_quarter_leaves_change_unknown_not_new(monkeypatch):
    _book(monkeypatch, {"2026-09-30": [_report("2026-09-30", 10.0)], "2026-06-30": [_report("2026-06-30", 4.0)]})
    got = edgar_13f._read(1, "2026-09-30", "2026-06-30")
    row, = got["rows"]
    assert (row["change"], row["shares_prior"]) == ("increased", 4.0)
    assert got["url"] == "https://www.sec.gov/Archives/edgar/data/1/a20260930/a-2026-09-30-index.htm"
    assert got["prior_url"] == "https://www.sec.gov/Archives/edgar/data/1/a20260630/a-2026-06-30-index.htm"
    assert row["accession"] == "a-2026-09-30"
    _book(monkeypatch, {"2026-09-30": [_report("2026-09-30", 10.0)]})
    row, = edgar_13f._read(1, "2026-09-30", "2026-06-30")["rows"]
    assert (row["change"], row["shares_prior"]) == ("new", 0.0)
    _book(monkeypatch, {"2026-09-30": [_report("2026-09-30", 10.0)],
                        "2026-06-30": "0000000001-26-000001"})
    got = edgar_13f._read(1, "2026-09-30", "2026-06-30")
    row, = got["rows"]
    assert (row["change"], row["shares_prior"], got["prior_url"]) == (None, None, None)
    _book(monkeypatch, {"2026-09-30": [_report("2026-09-30", 10.0)],
                        "2026-06-30": ("0000000001-26-000001", [_report("2026-06-30", 4.0)])})
    assert edgar_13f._read(1, "2026-09-30", "2026-06-30")["prior_url"] is None  # a partly read quarter is not held


class _EdgarHTTPError(Exception):
    def __init__(self, status, url):
        self.response = type("R", (), {"status_code": status})()
        self.request = type("Q", (), {"url": url})()


def test_a_cik_edgar_does_not_know_reads_as_no_report(monkeypatch):
    missing = _EdgarHTTPError(404, "https://data.sec.gov/submissions/CIK9999999999.json")
    _book(monkeypatch, {"2026-09-30": missing})
    assert edgar_13f._read(9999999999, "2026-09-30", "2026-06-30") is None
    _book(monkeypatch, {"2026-09-30": _EdgarHTTPError(503, "https://data.sec.gov/submissions/CIK1.json")})
    with pytest.raises(_EdgarHTTPError):
        edgar_13f._read(1, "2026-09-30", "2026-06-30")
    assert not unknown_company(_EdgarHTTPError(404, "https://www.sec.gov/Archives/edgar/data/1/x.txt"))
    from edgar.exceptions import CompanyNotFoundError

    assert unknown_company(CompanyNotFoundError("ZZZZQX"))


@pytest.mark.asyncio
async def test_portfolios_are_kept_a_few_at_a_time_and_a_missing_one_never(monkeypatch):
    monkeypatch.setattr(edgar_13f, "_portfolios", TtlCache(size=2, ttl=3600.0))
    reads = []

    def read(cik, period, prior):
        reads.append(cik)
        return None if cik == 0 else {"filer_name": "M", "filing_date": None, "rows": []}

    monkeypatch.setattr(edgar_13f, "_read", read)
    for cik in (0, 0, 1, 1, 2, 3, 1):
        await edgar_13f.manager_portfolio(cik, "2026-09-30", "2026-06-30")
    assert reads == [0, 0, 1, 2, 3, 1]
    assert len(edgar_13f._portfolios) == 2

def test_a_ttl_cache_is_bounded_expires_and_refuses_none():
    now = [0.0]
    cache = TtlCache(size=2, ttl=10.0, clock=lambda: now[0])
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1
    cache.put("c", 3)  # b is the least recently read
    assert (cache.get("b"), cache.get("a"), cache.get("c")) == (None, 1, 3)
    now[0] = 11.0
    assert cache.get("a") is None and len(cache) == 1
    cache.put("d", 4)
    assert len(cache) == 1
    with pytest.raises(ValueError):
        cache.put("e", None)


def test_the_edgar_identity_defaults_once_with_a_warning(monkeypatch, caplog):
    monkeypatch.setenv("EDGAR_IDENTITY", "")
    assert edgar_identity() == DEFAULT_IDENTITY
    assert "EDGAR_IDENTITY is not set" in caplog.text
    monkeypatch.setenv("EDGAR_IDENTITY", "Jane Doe jane@example.com")
    assert edgar_identity() == "Jane Doe jane@example.com"


def test_an_unreadable_report_of_the_quarter_asked_leaves_the_readable_ones_served_without_change(monkeypatch):
    prior = _Report("a-prior", "MGR", "2026-08-14", "2026-06-30", "X0202", _Unit("dollars"), (
        _Holding("APPLE", "COM", "037833100", "SH", None, 10.0, 20.0),
        _Holding("MICROSOFT", "COM", "594918104", "SH", None, 100.0, 200.0)))
    _book(monkeypatch, {"2026-09-30": ("0000000001-26-000009", [_report("2026-09-30", 10.0)]),
                        "2026-06-30": [prior]})
    row, = edgar_13f._read(1, "2026-09-30", "2026-06-30")["rows"]  # the unread amendment may still hold MICROSOFT
    assert (row["cusip"], row["shares"], row["change"], row["shares_prior"]) == ("037833100", 10.0, None, None)


def test_a_report_leaving_holdings_out_as_confidential_leaves_change_unknown(monkeypatch):
    from dataclasses import replace

    prior = _Report("a-prior", "MGR", "2026-08-14", "2026-06-30", "X0202", _Unit("dollars"), (
        _Holding("APPLE", "COM", "037833100", "SH", None, 4.0, 8.0),
        _Holding("MICROSOFT", "COM", "594918104", "SH", None, 100.0, 200.0)))
    now = _report("2026-09-30", 10.0)
    for flagged_now, flagged_prior in ((True, False), (False, True)):
        _book(monkeypatch, {"2026-09-30": [replace(now, confidential_omitted=flagged_now)],
                            "2026-06-30": [replace(prior, confidential_omitted=flagged_prior)]})
        got = edgar_13f._read(1, "2026-09-30", "2026-06-30")
        row, = got["rows"]  # MICROSOFT may be held under confidentiality
        assert (row["cusip"], row["change"], row["shares_prior"], got["prior_url"]) == ("037833100", None, None, None)
    _book(monkeypatch, {"2026-09-30": [now], "2026-06-30": [prior]})
    got = edgar_13f._read(1, "2026-09-30", "2026-06-30")
    assert [(r["change"], r["accession"] is None) for r in got["rows"]] == [("increased", False), ("sold_out", True)]
    assert got["prior_url"] == "https://www.sec.gov/Archives/edgar/data/1/aprior/a-prior-index.htm"


def test_a_quarter_whose_every_report_is_unreadable_says_so_with_the_filing(monkeypatch):
    _book(monkeypatch, {"2026-09-30": "0000000001-26-000009", "2026-06-30": [_report("2026-06-30", 10.0)]})
    with pytest.raises(edgar_13f.UnreadableReports) as raised:
        edgar_13f._read(1, "2026-09-30", "2026-06-30")
    assert raised.value.url == (
        "https://www.sec.gov/Archives/edgar/data/1/000000000126000009/0000000001-26-000009-index.htm")
    empty = _Report("a-empty", "MGR", "2026-11-14", "2026-09-30", "X0202", _Unit("dollars"), ())
    _book(monkeypatch, {"2026-09-30": ("0000000001-26-000009", [empty]), "2026-06-30": [_report("2026-06-30", 10.0)]})
    with pytest.raises(edgar_13f.UnreadableReports):  # a readable report listing nothing beside an unread one
        edgar_13f._read(1, "2026-09-30", "2026-06-30")
    _book(monkeypatch, {"2026-09-30": [empty], "2026-06-30": [_report("2026-06-30", 10.0)]})
    assert edgar_13f._read(1, "2026-09-30", "2026-06-30")["rows"][0]["change"] == "sold_out"  # a valid empty report
    _book(monkeypatch, {"2026-09-30": [empty]})
    got = edgar_13f._read(1, "2026-09-30", "2026-06-30")  # and one with nothing held the quarter before
    assert (got["rows"], got["url"]) == ([], "https://www.sec.gov/Archives/edgar/data/1/aempty/a-empty-index.htm")
    _book(monkeypatch, {"2026-06-30": [_report("2026-06-30", 10.0)]})
    assert edgar_13f._read(1, "2026-09-30", "2026-06-30") is None  # no report at all


def test_the_engine_counts_as_available_only_with_edgartools_beside_it():
    assert engine_available()
    with patch.dict(sys.modules, {"edgar": None}):
        assert not engine_available()  # edgar_engine's root imports without edgartools
    with patch.dict(sys.modules, {"edgar_engine": None}):
        assert not engine_available()
