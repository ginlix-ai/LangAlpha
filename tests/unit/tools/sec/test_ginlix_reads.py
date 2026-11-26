import gzip
from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from edgar_engine.filings import labels_from_companyfacts

from src.config import settings
from src.data_client.ginlix_data import sec_filings
from src.data_client.ginlix_data.sec_filings import FilingCalendar, OwedReport, SecFilingsClient
from src.tools.sec import edgar, indexed, tool
from tests.unit.tools.sec.fakes import ACC, INCOME, ORIGINAL, Backend, filing


def client_answering(handler) -> SecFilingsClient:
    c = SecFilingsClient("http://data.test")
    c.http = httpx.Client(base_url="http://data.test/api/v2/data/sec", transport=httpx.MockTransport(handler))
    return c


@pytest.mark.parametrize("status", [404, 503])
def test_a_miss_or_a_503_reads_as_none(status):
    c = client_answering(lambda request: httpx.Response(status, json={"detail": "no"}))
    assert c.company("AAPL") is None and c.filings("1", ["10-K"]) is None and c.fiscal_labels("AAPL") is None
    assert c.calendar("AAPL") is None and c.parsed("sections/v1/a.json.gz", 1) is None


def test_a_filing_list_cut_short_reads_as_none():
    c = client_answering(lambda request: httpx.Response(200, json={"truncated": True, "filings": [{}]}))
    assert c.filings("1", ["10-K"]) is None


def test_a_company_resolved_through_a_former_ticker_names_it():
    body = {**COMPANY, "tickers": ["OLD-B"], "former_ticker": {"ticker": "OLD-B", "last_seen": 1, "tickers": ["NEW"]}}
    c = client_answering(lambda request: httpx.Response(200, json=body))
    assert (c.company("old.b").former_ticker, c.company("old.b").tickers) == ("OLD-B", ("NEW",))
    assert client_answering(lambda request: httpx.Response(200, json=COMPANY)).company("ACME").former_ticker is None


def test_any_other_failure_raises():
    c = client_answering(lambda request: httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        c.company("AAPL")


def test_a_parsed_blob_comes_back_gzipped_as_the_engine_caches_store_it():
    blob = gzip.compress(b'{"accession": "a"}', mtime=0)
    asked = []

    def handler(request):
        asked.append((request.url.path, request.url.params["cik"], request.headers["accept-encoding"]))
        return httpx.Response(200, stream=httpx.ByteStream(blob), headers={"Content-Encoding": "gzip"})

    assert client_answering(handler).parsed("sections/v1/a.json.gz", 320193) == blob
    assert asked == [("/api/v2/data/sec/parsed/sections/v1/a.json.gz", "320193", "gzip")]
    plain = client_answering(lambda request: httpx.Response(200, stream=httpx.ByteStream(b'{"accession": "a"}')))
    assert gzip.decompress(plain.parsed("sections/v1/a.json.gz", 1)) == b'{"accession": "a"}'


def test_a_parsed_store_on_another_parser_version_is_reported_once(caplog):
    sec_filings.note_skew.cache_clear()
    body = {"detail": "x", "reason": "other_version", "versions": ["v3/edgartools-6.3.0"], "queued": False}
    c = client_answering(lambda request: httpx.Response(404, json=body))
    with caplog.at_level("WARNING"):
        assert c.parsed("statements/v3/edgartools-5.62.1/a.json.gz", 1) is None
        assert c.parsed("statements/v3/edgartools-5.62.1/b.json.gz", 1) is None
    assert [r.message.count("v3/edgartools-6.3.0") for r in caplog.records] == [1]


def row(accession, form, filed, report="", items=None, accepted=None):
    return {"accession_number": accession, "form": form, "filing_date": filed, "report_date": report or None,
            "accepted": accepted, "items": items}


INDEX = [row("0000000001-25-000003", "8-K", "2025-11-01", items="2.02,9.01", accepted="2025-11-01T20:00:00.000Z"),
         row("0000000001-25-000002", "10-Q", "2025-08-01", "2025-06-30"),
         row("0000000001-25-000001", "10-K", "2025-02-20", "2024-12-31")]
COMPANY = {"cik": 1, "name": "Acme Corp", "fiscal_year_end": "1231", "tickers": ["ACME", "ACME-P"]}
FACTS = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
    {"accn": "0000000001-25-000001", "form": "10-K", "fy": 2024, "fp": "FY"}]}}}}}


class Ginlix:
    """ginlix-data's SEC routes over a mock transport, answering from what the test gives them; any of them can be
    made to fail."""

    def __init__(self, company=None, filings=None, facts=None, engine=None, calendar=None, parsed=None, down=False):
        self.row, self.rows, self.facts, self.engine, self.cal = company, filings, facts, engine, calendar
        self.blobs, self.down, self.asked, self.facts_read = parsed or {}, down, [], 0
        self.answers = {}

    def handle(self, request):
        route, _, rest = request.url.path.removeprefix("/api/v2/data/sec/").partition("/")
        self.asked.append(route)
        if self.down:
            raise httpx.ConnectError("down", request=request)
        if route in self.answers:
            return httpx.Response(200, json=self.answers[route])
        if route == "parsed":
            blob = self.blobs.get(rest)
            return (httpx.Response(200, stream=httpx.ByteStream(blob), headers={"Content-Encoding": "gzip"})
                    if blob is not None else httpx.Response(404, json={"detail": "none"}))
        body = {"companies": lambda: self.row,
                "filings": lambda: self.row and self.rows is not None and {"cik": self.row["cik"], "filings": self.rows},
                "fiscal-labels": self.fiscal_labels, "calendar": lambda: self.cal,
                "companyfacts": self.companyfacts}[route]()
        return httpx.Response(200, json=body) if body else httpx.Response(404, json={"detail": "none"})

    def fiscal_labels(self):
        if self.facts is None or self.row is None:
            return None
        found = labels_from_companyfacts(self.facts)
        return {"cik": self.row["cik"], "engine": self.engine or indexed.labels_engine(),
                "labels": sorted([a, f, y, p] for (a, f), (y, p) in found.items())}

    def companyfacts(self):
        self.facts_read += 1
        return self.facts


def use(monkeypatch, ginlix):
    """Configures ginlix-data as the process's, or none."""
    monkeypatch.setattr(settings, "GINLIX_DATA_URL", "http://data.test" if ginlix else "")
    monkeypatch.setattr(settings, "GINLIX_DATA_SEC_ENABLED", True)
    monkeypatch.setattr(sec_filings, "_client", client_answering(ginlix.handle) if ginlix else None)


class Cache(dict):
    def get(self, key):
        return super().get(key)

    def put(self, key, data):
        self[key] = data

    def delete(self, key):
        self.pop(key, None)


def test_a_parsed_filing_the_cache_misses_is_read_from_ginlix_data_and_writes_go_to_the_cache(monkeypatch):
    ginlix = Ginlix(parsed={"sections/v1/a.json.gz": b"stored", "sections/v1/b.json.gz": b"stored b"})
    use(monkeypatch, ginlix)
    cache = Cache({"sections/v1/b.json.gz": b"cached b"})
    store = indexed.CacheThenGinlixStore(cache, 7)

    assert store.get("sections/v1/a.json.gz") == b"stored"
    assert store.get("sections/v1/b.json.gz") == b"cached b"
    assert store.get("submissions/a.txt.gz") is None
    store.put("statements/v4/x/a.json.gz", b"new")
    assert ginlix.asked == ["parsed"] and cache["statements/v4/x/a.json.gz"] == b"new"


def test_a_ginlix_data_failure_reads_as_a_miss(monkeypatch):
    use(monkeypatch, Ginlix(down=True))
    assert indexed.CacheThenGinlixStore(Cache(), 7).get("sections/v1/a.json.gz") is None


def test_without_ginlix_data_filings_are_read_through_the_cache_alone(monkeypatch):
    use(monkeypatch, None)
    assert edgar.filing_store(7) is edgar.store()


def test_once_ginlix_data_is_unreachable_the_rest_of_the_call_skips_it(monkeypatch):
    ginlix = Ginlix(down=True)
    use(monkeypatch, ginlix)
    store = indexed.CacheThenGinlixStore(Cache(), 7)
    with indexed.call_scope():
        assert store.get("sections/v1/a.json.gz") is None and store.get("sections/v1/b.json.gz") is None
    assert ginlix.asked == ["parsed"]
    with indexed.call_scope():
        store.get("sections/v1/a.json.gz")
    assert ginlix.asked == ["parsed", "parsed"]


def test_a_failure_other_than_ginlix_data_being_unreachable_keeps_the_rest_of_the_call_on_it(monkeypatch):
    ginlix, asked = Ginlix(), []
    ginlix.handle = lambda request: asked.append(request.url.path) or httpx.Response(500)
    use(monkeypatch, ginlix)
    store = indexed.CacheThenGinlixStore(Cache(), 7)
    with indexed.call_scope():
        store.get("sections/v1/a.json.gz")
        store.get("sections/v1/b.json.gz")
    assert len(asked) == 2


@pytest.fixture
def ginlix(monkeypatch):
    found = Ginlix(company=COMPANY, filings=[dict(r) for r in INDEX], facts=FACTS)
    use(monkeypatch, found)
    return found


def run(coro):
    import asyncio
    return asyncio.run(coro)


def test_a_company_ginlix_data_indexes_selects_from_its_whole_history(ginlix):
    found = run(edgar.filer("acme-p"))
    co = found.co
    assert (co.symbol, co.name, co.cik, co.fiscal_year_end) == ("ACME", "Acme Corp", 1, "1231")
    assert dict(found.labels) == {("0000000001-25-000001", "10-K"): (2024, "FY")} and ginlix.facts_read == 0
    assert co.rows(["10-K"], False) == co.rows(["10-K"], True) == [co.filings[2]]

    ref = edgar.select(co, "10-K", found.labels, 2024, None)
    assert (ref.accession, ref.fiscal_source) == ("0000000001-25-000001", "xbrl")
    assert [r.accession for r in edgar.periodic_reports(co, found.labels)] == ["0000000001-25-000002",
                                                                               "0000000001-25-000001"]


def test_recent_8ks_of_an_indexed_company_come_from_its_rows(ginlix):
    recent = (date.today() - timedelta(days=3)).isoformat()
    ginlix.rows[0]["filing_date"] = recent
    ginlix.rows[1:] = [row(f"0000000002-25-{n:06d}", "424B2", recent) for n in range(edgar.RECENT_PAGE_MIN)]
    [k] = edgar.recent_8ks(run(edgar.company("ACME")))
    assert (k.ref.accession, k.items) == ("0000000001-25-000003", ("2.02", "9.01"))


def edgar_company(monkeypatch, cik=7, name="From EDGAR", ticker="edg", rows=()):
    import edgar as edgartools

    table = SimpleNamespace(data=SimpleNamespace(to_pylist=lambda: list(rows)))
    found = SimpleNamespace(cik=cik, name=name, not_found=False, get_ticker=lambda: ticker, tickers=[ticker],
                            data=SimpleNamespace(entity_type="operating"), get_filings=lambda **kw: table)
    monkeypatch.setattr(edgartools, "Company", lambda symbol: found)
    return found


@pytest.mark.parametrize("answer", [Ginlix(), Ginlix(down=True), None])
def test_without_an_answer_from_ginlix_data_the_company_and_labels_come_from_edgar(monkeypatch, answer):
    use(monkeypatch, answer)
    edgar_company(monkeypatch, rows=[{"accession_number": "a"}])
    monkeypatch.setattr(edgar, "companyfacts_labels", lambda cik: {("a", "10-K"): (2020, "FY")})

    found = run(edgar.filer("EDG"))
    assert (found.co.symbol, found.co.cik, found.co.filings) == ("EDG", 7, [{"accession_number": "a"}])
    assert dict(found.labels) == {("a", "10-K"): (2020, "FY")} and found.calendar is None


def test_with_its_sec_reads_switched_off_ginlix_data_is_never_asked(ginlix, monkeypatch):
    monkeypatch.setattr(settings, "GINLIX_DATA_SEC_ENABLED", False)
    edgar_company(monkeypatch, rows=[{"accession_number": "a"}])
    monkeypatch.setattr(edgar, "companyfacts_labels", lambda cik: {("a", "10-K"): (2020, "FY")})

    found = run(edgar.filer("EDG"))
    assert (found.co.cik, found.calendar) == (7, None) and edgar.filing_store(7) is edgar.store()
    assert ginlix.asked == []


def test_a_company_row_ginlix_data_holds_no_name_for_reads_from_edgar(ginlix, monkeypatch):
    ginlix.row = {"cik": 1, "name": None, "fiscal_year_end": None, "tickers": []}
    edgar_company(monkeypatch, cik=1, name="Acme Corp", ticker="ACME", rows=[{"accession_number": "from-edgar"}])
    assert run(edgar.company("acme")).filings == [{"accession_number": "from-edgar"}]


@pytest.mark.parametrize(("route", "answer"), [
    ("companies", {**COMPANY, "tickers": [None]}),
    ("companies", {**COMPANY, "fiscal_year_end": 1231}),
    ("companies", {**COMPANY, "fiscal_year_end": "9999"}),
    ("companies", {**COMPANY, "tickers": "ACME"}),
    ("filings", {"cik": 1, "filings": [row("0000000001-25-000001", "10-K", "2025-02-20", items=["2.02"])]}),
    ("filings", {"cik": 1, "filings": {}}),
    ("filings", {"cik": 1, "filings": [row("0000000001-25-000001", "10-K", "not-a-date")]}),
    ("filings", {"cik": 1, "filings": [row("0000000001-25-000001", "10-K", "2025-02-20", "2024-13-31")]}),
    ("filings", {"cik": 1, "filings": [row("0000000001-25-000001", 10, "2025-02-20")]}),
])
def test_a_company_row_or_filing_rows_of_another_shape_read_from_edgar(ginlix, monkeypatch, route, answer):
    ginlix.answers[route] = answer
    edgar_company(monkeypatch, cik=1, name="Acme Corp", ticker="ACME", rows=[{"accession_number": "from-edgar"}])
    assert run(edgar.company("acme")).filings == [{"accession_number": "from-edgar"}]


def test_filing_dates_are_read_in_one_spelling(ginlix):
    ginlix.answers["filings"] = {"cik": 1, "filings": [row("0000000001-25-000001", "10-K", "20250220", "20241231")]}
    [found] = run(edgar.company("acme")).filings
    assert (found["filing_date"], found["reportDate"]) == ("2025-02-20", "2024-12-31")


@pytest.mark.parametrize("change", [{"deadline": None}, {"window": [123, "2026-02-20"]}, {"notice": []}])
def test_a_calendar_of_another_shape_is_left_out(ginlix, change):
    ginlix.cal = calendar_body("current")
    ginlix.cal["next"].update(change)
    assert run(edgar.filer("ACME")).calendar is None


def test_a_filing_list_of_another_filer_than_the_company_row_reads_from_edgar(ginlix, monkeypatch):
    ginlix.answers["filings"] = {"cik": 2, "filings": []}
    edgar_company(monkeypatch, cik=1, name="Acme Corp", ticker="ACME", rows=[{"accession_number": "from-edgar"}])
    assert run(edgar.company("acme")).filings == [{"accession_number": "from-edgar"}]


def test_labels_and_a_calendar_of_another_filer_than_the_company_read_are_not_used(ginlix, monkeypatch):
    ginlix.answers["companies"] = {**COMPANY, "cik": 2}
    ginlix.answers["filings"] = {"cik": 2, "filings": []}
    ginlix.cal = calendar_body("current")
    monkeypatch.setattr(edgar, "companyfacts_labels", lambda cik: {})
    found = run(edgar.filer("ACME"))
    assert found.co.cik == 2 and found.calendar is None and ginlix.facts_read == 1


def test_fiscal_labels_from_another_engine_are_computed_here_from_the_company_facts(ginlix, caplog):
    sec_filings.note_skew.cache_clear()
    ginlix.engine = "labels-000000000000"
    with caplog.at_level("WARNING"):
        first, second = run(edgar.filer("ACME")), run(edgar.filer("ACME"))
    assert [r.message.count("labels-000000000000") for r in caplog.records] == [1]
    assert dict(first.labels) == dict(second.labels) == {("0000000001-25-000001", "10-K"): (2024, "FY")}
    assert ginlix.facts_read == 2


@pytest.mark.parametrize("broken", ["engine", [["a", "10-K"]], [["0000000001-25-000001", "10-K", 2024, None]],
                                    [["0000000001-25-000001", "10-K", 2024, ["FY"]]],
                                    [["0000000001-25-000001", "10-K", 2024.5, "FY"]], {}])
def test_labels_that_cannot_be_checked_or_read_are_computed_from_the_company_facts(ginlix, monkeypatch, broken):
    def unreadable():
        raise OSError("gone")

    if broken == "engine":
        monkeypatch.setattr(indexed, "labels_engine", unreadable)
    else:
        ginlix.answers["fiscal-labels"] = {"cik": 1, "engine": indexed.labels_engine(), "labels": broken}
    assert dict(run(edgar.filer("ACME")).labels) == {("0000000001-25-000001", "10-K"): (2024, "FY")}
    assert ginlix.facts_read == 1


def calendar_body(verdict, notice=None, termination=None, latest="0000000001-25-000002"):
    return {"cik": 1, "calendar_as_of": "2025-08-02", "verdict": verdict, "termination": termination,
            "latest": {"accession": latest}, "next": {
                "form": "10-K", "fiscal_period": "FY", "period_end": "2025-12-31", "window": ["2026-02-10", "2026-02-20"],
                "deadline": "2026-03-02", "latest_deadline": "2026-03-17", "notice": notice}}


def test_a_calendar_is_read_with_the_dates_of_the_notice_and_form_15_it_names(ginlix):
    ginlix.rows += [row("0000000001-26-000007", "NT 10-K", "2026-03-03"), row("0000000001-26-000008", "15-12G", "2026-03-20")]
    ginlix.cal = calendar_body("extended", notice="0000000001-26-000007", termination="0000000001-26-000008")
    calendar = run(edgar.filer("ACME")).calendar
    assert calendar.served == FilingCalendar(
        1, "extended", "0000000001-25-000002", "0000000001-26-000008",
        OwedReport("10-K", "FY", "2025-12-31", ("2026-02-10", "2026-02-20"), "2026-03-02", "2026-03-17",
                   "0000000001-26-000007"))
    assert calendar.filed == {"0000000001-26-000007": ("NT 10-K", "2026-03-03"),
                              "0000000001-26-000008": ("15-12G", "2026-03-20")}


@pytest.mark.asyncio
async def test_the_filers_calendar_reaches_the_recent_reports_and_one_of_another_shape_is_left_out(ginlix, monkeypatch):
    monkeypatch.setattr(edgar, "sections", AsyncMock(side_effect=lambda co, ref: filing(
        *(replace(s, source=None) for s in ORIGINAL.items.values()), accession=ref.accession, form=ref.form)))
    monkeypatch.setattr(edgar, "financials", AsyncMock(return_value=edgar.Financials()))
    monkeypatch.setattr(edgar, "amendments", AsyncMock(return_value=([], [])))
    monkeypatch.setattr(edgar, "recent_8ks", lambda co, after=None: [])
    monkeypatch.setattr(tool, "fetch_matching_earnings_call", AsyncMock(return_value=None))
    ginlix.cal = calendar_body("current")
    ahead = [(date.today() + timedelta(days=n)).isoformat() for n in (30, 40, 50, 65)]
    ginlix.cal["next"].update(window=ahead[:2], deadline=ahead[2], latest_deadline=ahead[3])
    content, _ = await tool.get_sec_filing_async("ACME", "10-Q", backend=Backend())
    assert "This is the latest periodic report on file. The next is the 10-K for fiscal year 2025" in content

    del ginlix.cal["next"]["window"]
    content, _ = await tool.get_sec_filing_async("ACME", "10-Q", backend=Backend())
    assert "This is the latest periodic report on file.\n" in content and "The next is" not in content


@pytest.mark.asyncio
@pytest.mark.parametrize("delisted", [False, True])
async def test_a_filing_reads_the_same_from_ginlix_data_as_from_edgar(monkeypatch, delisted):
    """The tool's whole output over one filing table, once as ginlix-data serves it and once as edgartools does;
    delisted, the company has no ticker today and ACME is the ticker it last had."""
    import edgar as edgartools

    day = [(date.today() - timedelta(days=n)).isoformat() for n in (5, 10)]
    native = [row("0000000001-26-000009", "8-K", day[0], items="2.02,9.01", accepted=f"{day[0]}T21:05:00.000Z"),
              row("0000000001-26-000004", "8-K", day[1], items="7.01", accepted=f"{day[1]}T12:00:00.000Z"),
              row("0000000001-26-000003", "10-Q", day[1], "2026-09-30", accepted=f"{day[1]}T20:00:00.000Z"),
              row(ACC, "10-K", "2026-02-01", "2025-12-31"),
              row("0000000001-25-000001", "10-K", "2025-02-03", "2024-12-31"),
              row("0000000001-24-000001", "10-K", "2024-02-02", "")]
    if delisted:
        native[:0] = [row("0000000001-26-000021", "15-12G", day[0]), row("0000000001-26-000020", "25-NSE", day[1])]
    facts = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
        {"accn": ACC, "form": "10-K", "fy": 2025, "fp": "FY"},
        {"accn": "0000000001-26-000003", "form": "10-Q", "fy": 2026, "fp": "Q3"}]}}}}}

    def table(**kw):
        rows = [{"accession_number": r["accession_number"], "form": r["form"],
                 "filing_date": date.fromisoformat(r["filing_date"]), "reportDate": r["report_date"] or "",
                 "acceptanceDateTime": r["accepted"] and datetime.fromisoformat(r["accepted"].removesuffix("Z")),
                 "items": r["items"] or ""} for r in native if "form" not in kw or r["form"] in kw["form"]]
        return SimpleNamespace(data=SimpleNamespace(num_rows=len(rows), to_pylist=lambda: rows))

    edgar_co = SimpleNamespace(cik=1, name="Acme Corp", not_found=False, fiscal_year_end="1231",
                               get_ticker=lambda: None if delisted else "ACME", tickers=[] if delisted else ["ACME"],
                               data=SimpleNamespace(entity_type="operating"), get_filings=table)
    monkeypatch.setattr(edgartools, "Company", lambda symbol: edgar_co)
    monkeypatch.setattr(edgar, "companyfacts_labels", lambda cik: labels_from_companyfacts(facts))

    def read(ref):
        return filing(*(replace(s, source=None) for s in ORIGINAL.items.values()), accession=ref.accession,
                      form=ref.form, filed=ref.filing_date)

    monkeypatch.setattr(edgar, "sections", AsyncMock(side_effect=lambda co, ref: read(ref)))
    monkeypatch.setattr(edgar, "financials", AsyncMock(side_effect=lambda ref: edgar.Financials(
        {"revenue": 2e9}, "| Revenue | 2 |", "USD", held=INCOME)))
    monkeypatch.setattr(edgar, "amendments", AsyncMock(side_effect=lambda co, labels, ref: ([], [])))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (read(k.ref), [])))
    monkeypatch.setattr(tool, "fetch_matching_earnings_call", AsyncMock(return_value=None))
    company = {"cik": 1, "name": "Acme Corp", "fiscal_year_end": "1231", "entity_type": "operating",
               "tickers": ["ACME"]}
    if delisted:  # a company row read before SEC's map dropped the ticker still lists it
        company["former_ticker"] = {"ticker": "ACME", "last_seen": 1, "tickers": []}
    remote = Ginlix(company=company, filings=native, facts=facts)
    listing = (f"ACME is a former ticker of Acme Corp, which has no listed ticker today: a Form 25-NSE to delist it "
               f"was filed on {day[1]} (source: {edgar.index_url(1, '0000000001-26-000020')}), and it filed Form 15-12G "
               f"on {day[0]} to stop reporting (source: {edgar.index_url(1, '0000000001-26-000021')}).")

    async def both(**kw):
        out = []
        for source in (None, remote):
            use(monkeypatch, source)
            backend = Backend()
            out.append((*await tool.get_sec_filing_async("ACME", backend=backend, **kw), backend.files))
        return out

    for kw in ({"filing_type": "10-K"}, {"filing_type": "10-K", "fiscal_year": 2024}, {"filing_type": "10-Q"},
               {"filing_type": "8-K"}, {"filing_type": "10-K", "fiscal_year": 2030}):
        from_edgar, from_ginlix = await both(**kw)
        assert from_ginlix == from_edgar, kw
        assert (listing in from_edgar[0]) == delisted, kw
    assert remote.facts_read == 0 and "fiscal-labels" in remote.asked


@pytest.mark.parametrize("facts", [[], ["not", "companyfacts"]])
def test_company_facts_of_another_shape_read_as_a_miss_and_the_labels_come_from_edgar(ginlix, monkeypatch, facts):
    ginlix.engine = "labels-v0"
    ginlix.answers["companyfacts"] = facts
    monkeypatch.setattr(edgar, "companyfacts_labels", lambda cik: {("a", "10-K"): (2020, "FY")})
    assert dict(run(edgar.filer("ACME")).labels) == {("a", "10-K"): (2020, "FY")}


def test_a_read_joined_across_calls_skips_ginlix_data_only_for_calls_that_found_it_unreachable(monkeypatch):
    import asyncio
    import threading

    use(monkeypatch, Ginlix())
    started, release, reads = threading.Event(), threading.Event(), []

    @edgar._shared(lambda name: f"probe:{name}")
    def probe(name):
        reads.append(indexed.cut_off())
        started.set()
        release.wait(5)
        return name

    async def call(cut):
        with indexed.call_scope():
            if cut:
                indexed._unreachable.get().set()
            return await probe("x")

    async def main():
        first = asyncio.create_task(call(cut=True))
        await asyncio.to_thread(started.wait, 5)
        second = asyncio.create_task(call(cut=False))
        await asyncio.sleep(0.05)
        release.set()
        return await asyncio.gather(first, second)

    assert run(main()) == ["x", "x"] and sorted(reads) == [False, True]
