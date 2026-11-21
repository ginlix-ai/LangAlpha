from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest
from edgar_engine.filings import (LABELS_VERSION, FilingRef, get_filing, labels_engine, labels_from_companyfacts,
                                  period_rule, pick_ref, refs_for_form, refs_from_rows, select_filing, select_ref)

CIK = 1234567


def row(accession, form, filed, report):
    return {"accession_number": accession, "form": form, "filing_date": date.fromisoformat(filed), "reportDate": report}


ROWS = [
    row("0001234567-25-000010", "10-K", "2025-02-20", "2024-12-31"),
    row("0001234567-24-000011", "10-K", "2024-02-21", "2023-12-31"),
    row("0001234567-24-000050", "10-K", "2024-03-30", "2023-12-31"),
    row("0001234567-24-000060", "10-K/A", "2024-04-30", "2023-12-31"),
    row("0001234567-23-000012", "10-K405", "2023-02-22", "2022-12-31"),
    row("0001234567-24-000020", "10-Q", "2024-05-02", "2024-03-31"),
    row("0001234567-24-000030", "10-Q", "2024-08-01", "2024-06-30"),
    row("0001234567-24-000040", "10-Q", "2024-11-01", "2024-09-30"),
    row("0001234567-24-000099", "8-K", "2024-11-02", ""),
]
LABELS = {
    ("0001234567-25-000010", "10-K"): (2024, "FY"),
    ("0001234567-24-000011", "10-K"): (2023, "FY"),
    ("0001234567-24-000050", "10-K"): (2023, "FY"),
    ("0001234567-24-000060", "10-K/A"): (2023, "FY"),
    ("0001234567-24-000020", "10-Q"): (2024, "Q1"),
    ("0001234567-24-000030", "10-Q"): (2024, "Q2"),
    ("0001234567-24-000040", "10-Q"): (2024, "Q3"),
}


def refs(form="10-K", forms=("10-K", "10-K405"), **kw):
    return refs_from_rows(ROWS, CIK, forms, fiscal_labels=LABELS, fiscal_year_end="1231", **kw)


def test_rows_to_refs_filter_and_order():
    got = refs()
    assert [r.accession for r in got] == ["0001234567-25-000010", "0001234567-24-000050", "0001234567-24-000011", "0001234567-23-000012"]
    assert got[0] == FilingRef(CIK, "0001234567-25-000010", "10-K", "2025-02-20", "2024-12-31", 2024, "FY", "xbrl")


def test_amendments_are_opt_in():
    assert "10-K/A" not in {r.form for r in refs()}
    assert "10-K/A" in {r.form for r in refs(amendments=True)}


def test_amendment_carries_its_status_and_the_original_it_amends():
    amendment = next(r for r in refs(amendments=True) if r.form == "10-K/A")
    assert (amendment.filing_status, amendment.amends) == ("amendment", "0001234567-24-000050")
    assert {r.filing_status for r in refs()} == {"original"}


def test_filed_on_or_before():
    assert [r.accession for r in refs(filed_on_or_before="2024-03-30")][0] == "0001234567-24-000050"
    assert refs(filed_on_or_before="2022-01-01") == []


def test_unlabelled_filing_falls_back_to_period_rule():
    old = refs()[-1]
    assert (old.fiscal_year, old.fiscal_period, old.fiscal_source) == (2022, "FY", "period_rule")


def test_unlabelled_years_follow_the_filers_own_xbrl_convention():
    rows = [row("0001234567-11-000001", "10-K", "2011-03-25", "2011-01-29"),
            row("0001234567-10-000001", "10-K", "2010-03-26", "2010-01-30"),
            row("0001234567-09-000001", "10-K", "2009-03-27", "2009-01-31")]
    labels = {("0001234567-11-000001", "10-K"): (2010, "FY"), ("0001234567-10-000001", "10-K"): (2009, "FY")}
    got = refs_from_rows(rows, CIK, ["10-K"], fiscal_labels=labels, fiscal_year_end="0131")
    assert [(r.fiscal_year, r.fiscal_source) for r in got] == [(2010, "xbrl"), (2009, "xbrl"), (2008, "period_rule")]
    assert pick_ref(got, fiscal_year=2008).accession == "0001234567-09-000001"


def test_a_moved_year_end_shifts_only_unlabelled_years_with_the_same_year_end_month():
    rows = [row("0001234567-11-000001", "10-K", "2011-03-25", "2011-01-29"),
            row("0001234567-10-000001", "10-K", "2010-03-26", "2010-01-30"),
            row("0001234567-01-000001", "10-K", "2001-03-27", "2000-12-31")]
    labels = {("0001234567-11-000001", "10-K"): (2010, "FY"), ("0001234567-10-000001", "10-K"): (2009, "FY")}
    got = refs_from_rows(rows, CIK, ["10-K"], fiscal_labels=labels, fiscal_year_end="0131")
    assert [(r.fiscal_year, r.fiscal_source) for r in got] == [(2010, "xbrl"), (2009, "xbrl"), (2000, "period_rule")]


def test_an_old_quarter_counts_from_the_year_end_of_its_own_annual_report():
    rows = [row("0001234567-09-000001", "10-K", "2008-09-10", "2008-06-30"),
            row("0001234567-06-000001", "10-K", "2006-03-10", "2005-12-31"),
            row("0001234567-05-000002", "10-Q", "2005-05-10", "2005-03-31")]
    got = refs_from_rows(rows, CIK, ["10-Q"], fiscal_year_end="0630")
    assert [(r.accession, r.fiscal_year, r.fiscal_period) for r in got] == [("0001234567-05-000002", 2005, "Q1")]


def test_same_day_filings_are_ordered_by_acceptance_time_not_accession():
    from datetime import datetime
    rows = [dict(row("0009999999-24-000001", "10-K", "2024-03-01", "2023-12-31"), acceptanceDateTime=datetime(2024, 3, 1, 9)),
            dict(row("0001111111-24-000002", "10-K", "2024-03-01", "2023-12-31"), acceptanceDateTime=datetime(2024, 3, 1, 16))]
    got = refs_from_rows(rows, CIK, ["10-K"], fiscal_year_end="1231")
    assert pick_ref(got, fiscal_year=2023).accession == "0001111111-24-000002"


def test_annual_report_without_report_date_covers_the_last_fiscal_year_end_before_it():
    got = refs_from_rows([row("0001234567-09-000001", "40-F", "2009-03-30", ""),
                          row("0001234567-09-000002", "40-F/A", "2009-04-15", "2008-12-31")],
                         CIK, ["40-F"], amendments=True, fiscal_year_end="1231")
    assert [(r.report_date, r.fiscal_year, r.amends) for r in got] == [
        ("2008-12-31", 2008, "0001234567-09-000001"), ("2008-12-31", 2008, None)]


def test_undated_annual_report_takes_the_year_end_of_the_annual_report_filed_nearest_it():
    got = refs_from_rows([row("0001234567-01-000001", "10-K", "2001-03-20", ""),
                          row("0001234567-02-000001", "10-K", "2002-03-25", "2001-12-31"),
                          row("0001234567-10-000001", "10-K", "2010-09-10", "2010-06-30")],
                         CIK, ["10-K"], fiscal_year_end="0630")
    assert [r.report_date for r in got] == ["2010-06-30", "2001-12-31", "2000-12-31"]


def test_filing_without_report_date_has_no_label():
    got = refs_from_rows([row("0001234567-24-000099", "8-K", "2024-11-02", "")], CIK, ["8-K"])
    assert got[0].report_date is None
    assert (got[0].fiscal_year, got[0].fiscal_period, got[0].fiscal_source) == (None, None, None)


def test_pick_latest_without_a_period():
    assert pick_ref(refs()).accession == "0001234567-25-000010"
    assert pick_ref([]) is None


def test_pick_by_fiscal_year_takes_the_refile_its_amendments_link_to():
    got = refs(amendments=True)
    assert pick_ref(refs(), fiscal_year=2023).accession == "0001234567-24-000050"
    assert next(r for r in got if r.form == "10-K/A").amends == "0001234567-24-000050"


def test_pick_amendment_never_beats_the_original():
    assert pick_ref(refs(amendments=True), fiscal_year=2023).accession == "0001234567-24-000050"


def test_pick_by_fiscal_period():
    quarters = refs_from_rows(ROWS, CIK, ["10-Q"], fiscal_labels=LABELS)
    assert pick_ref(quarters, fiscal_year=2024, fiscal_period="Q2").accession == "0001234567-24-000030"
    assert pick_ref(quarters, fiscal_year=2024, fiscal_period="Q4") is None


@pytest.mark.parametrize(
    "period_end, accession",
    [("2024-09-30", "0001234567-24-000040"), ("2024-06", "0001234567-24-000030"), ("2030-01", None)],
)
def test_pick_by_period_end(period_end, accession):
    quarters = refs_from_rows(ROWS, CIK, ["10-Q"], fiscal_labels=LABELS)
    got = pick_ref(quarters, period_end=period_end)
    assert (got.accession if got else None) == accession


@pytest.mark.parametrize(
    "form, report_date, fye, expected",
    [
        ("10-K", "2021-12-31", "1231", (2021, "FY")),
        ("10-K405", "1999-06-30", "0630", (1999, "FY")),
        ("10-K", "2021-01-02", "1231", (2020, "FY")),
        ("10-K", "2021-01-30", "0130", (2021, "FY")),
        ("10-K", "2021-09-25", None, (2021, "FY")),
        ("10-Q", "2021-03-31", "1231", (2021, "Q1")),
        ("10-Q", "2021-06-30", "1231", (2021, "Q2")),
        ("10-Q", "2021-09-30", "1231", (2021, "Q3")),
        ("10-Q", "2021-03-27", "1231", (2021, "Q1")),
        ("10-Q", "2021-10-02", "1231", (2021, "Q3")),
        ("10-Q", "2020-12-26", "0930", (2021, "Q1")),
        ("10-Q", "2021-03-27", "0930", (2021, "Q2")),
        ("10-Q", "2021-06-26", "0930", (2021, "Q3")),
        ("10-Q", "2021-01-02", "0630", (2021, "Q2")),
        ("10-Q", "2021-09-30", None, (2021, None)),
        ("10-Q/A", "2021-03-31", "1231", (2021, "Q1")),
    ],
)
def test_period_rule(form, report_date, fye, expected):
    assert period_rule(form, report_date, fye) == expected


def test_period_rule_leap_day_year_end():
    assert period_rule("10-Q", "2024-05-31", "0229") == (2025, "Q1")


def test_companyfacts_labels_are_read_only_because_callers_share_them(monkeypatch):
    from edgar.entity import entity_facts
    from edgar_engine.filings import companyfacts_labels
    fact = {"accn": "0001234567-24-000011", "form": "10-K", "fy": 2023, "fp": "FY"}
    monkeypatch.setattr(entity_facts, "download_company_facts_from_sec",
                        lambda cik: {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [fact]}}}}})
    labels = companyfacts_labels(CIK)
    assert labels == {("0001234567-24-000011", "10-K"): (2023, "FY")}
    with pytest.raises(TypeError):
        labels[("0001234567-24-000011", "10-K")] = (2024, "FY")


def test_companyfacts_labels_are_read_again_on_every_call(monkeypatch):
    from edgar.entity import entity_facts
    from edgar.exceptions import CompanyFactsNotFoundError
    from edgar_engine.filings import companyfacts_labels
    calls = []

    def download(cik):
        calls.append(cik)
        if len(calls) == 1:
            raise CompanyFactsNotFoundError(cik)
        return {"facts": {"dei": {"X": {"units": {"USD": [{"accn": "A-1", "form": "10-K", "fy": len(calls), "fp": "FY"}]}}}}}
    monkeypatch.setattr(entity_facts, "download_company_facts_from_sec", download)
    assert companyfacts_labels(CIK) == {}
    assert companyfacts_labels(CIK) == {("A-1", "10-K"): (2, "FY")}
    assert companyfacts_labels(CIK) == {("A-1", "10-K"): (3, "FY")}


def test_list_filings_takes_labels_a_caller_already_read(monkeypatch):
    from edgar_engine import filings as filings_mod

    monkeypatch.setattr(filings_mod, "companyfacts_labels", lambda cik: pytest.fail("labels read again"))
    rows = [{"accession_number": "0001234567-25-000040", "form": "10-K", "filing_date": "2025-02-01",
             "reportDate": "2024-12-31", "acceptanceDateTime": "2025-02-01T06:00:00"}]
    company = SimpleNamespace(cik=CIK, fiscal_year_end="1231",
                              get_filings=lambda **kw: SimpleNamespace(data=SimpleNamespace(num_rows=1, to_pylist=lambda: rows)))
    monkeypatch.setattr(filings_mod, "_company", lambda c: c)
    labels = {("0001234567-25-000040", "10-K"): (2025, "FY")}
    (ref,) = filings_mod.list_filings(company, "10-K", fiscal_labels=labels)
    assert (ref.fiscal_year, ref.fiscal_source) == (2025, "xbrl")


def test_get_filing_builds_edgartools_filing():
    ref = FilingRef(CIK, "0001234567-24-000011", "10-K", "2024-02-21")
    filing = get_filing(ref)
    assert (filing.cik, filing.form, filing.accession_no) == (CIK, "10-K", "0001234567-24-000011")


@pytest.mark.network
def test_select_filing_live_uses_companyfacts():
    from edgar import set_identity

    set_identity("LangAlpha EdgarEngine user@example.com")
    ref = select_filing(320193, "10-K", fiscal_year=2023)
    assert ref is not None
    assert (ref.accession, ref.fiscal_year, ref.fiscal_period, ref.fiscal_source) == ("0000320193-23-000106", 2023, "FY", "xbrl")
    assert ref.report_date == "2023-09-30"


def test_the_year_shift_is_learned_from_originals_before_amendments():
    rows = [row("0001234567-11-000001", "10-K", "2011-03-25", "2011-01-29"),
            row("0001234567-11-000002", "10-K/A", "2011-06-01", "2011-01-29"),
            row("0001234567-10-000001", "10-K", "2010-03-26", "2010-01-30"),
            row("0001234567-10-000002", "10-K/A", "2010-06-01", "2010-01-30"),
            row("0001234567-09-000001", "10-K", "2009-03-27", "2009-01-31")]
    labels = {("0001234567-11-000001", "10-K"): (2010, "FY"), ("0001234567-11-000002", "10-K/A"): (2011, "FY"),
              ("0001234567-10-000002", "10-K/A"): (2010, "FY")}
    got = refs_from_rows(rows, CIK, ["10-K"], amendments=True, fiscal_labels=labels, fiscal_year_end="0131")
    assert next(r for r in got if r.accession == "0001234567-09-000001").fiscal_year == 2008


def test_an_amendment_with_no_period_of_report_is_left_for_its_text_to_place():
    got = refs_from_rows([row("0001234567-12-000001", "10-K", "2012-03-01", "2011-12-31"),
                          row("0001234567-12-000009", "10-K/A", "2012-09-01", "")],
                         CIK, ["10-K"], amendments=True, fiscal_year_end="1231")
    amendment = next(r for r in got if r.form == "10-K/A")
    assert (amendment.report_date, amendment.amends) == (None, None)


def test_a_same_day_amendment_links_to_the_original_received_before_it():
    got = refs_from_rows([row("0001234567-12-000001", "10-K", "2012-03-01", "2011-12-31"),
                          row("0001234567-12-000002", "10-K/A", "2012-03-01", "2011-12-31"),
                          row("0001234567-12-000003", "10-K", "2012-03-01", "2011-12-31")],
                         CIK, ["10-K"], amendments=True, fiscal_year_end="1231")
    assert next(r for r in got if r.form == "10-K/A").amends == "0001234567-12-000001"


def test_select_filing_reads_the_whole_history_only_when_the_filing_can_be_older(monkeypatch):
    from edgar_engine import filings as filings_mod

    recent = [r for r in refs() if r.filing_date >= "2024-01-01"]
    reads = []

    def listing(company, form, *, full_history=True, **kw):
        reads.append(full_history)
        return recent if not full_history else refs()

    monkeypatch.setattr(filings_mod, "_company", lambda c: c)
    monkeypatch.setattr(filings_mod, "list_filings", listing)

    def select(**period):
        reads.clear()
        return select_filing(CIK, "10-K", fiscal_labels=LABELS, **period), list(reads)

    assert select() == (recent[0], [False])
    assert select(fiscal_year=2030) == (None, [False])
    ref, read = select(fiscal_year=2022)
    assert ref.accession == "0001234567-23-000012" and read == [False, True]

    recent[-1] = replace(recent[-1], fiscal_year=2019, fiscal_source="period_rule")
    assert select(fiscal_year=2019)[1] == [False, True]
    assert select(fiscal_year=2025) == (None, [False])


def test_a_submission_that_does_not_parse_is_dropped_from_the_cache_and_read_once_more(monkeypatch):
    from edgar_engine import submission
    from edgar_engine.submission import TruncatedSubmission, parse_submission

    dropped, reads = [], iter(["cut", "whole", "cut", "cut"])
    monkeypatch.setattr(submission, "_drop_cached", lambda filing: dropped.append(filing.accession_no))
    filing = SimpleNamespace(accession_no="A-1", full_text_submission=lambda: next(reads))

    def parse(raw):
        if raw == "cut":
            raise ValueError("truncated")
        return raw

    assert parse_submission(filing, parse) == "whole"
    with pytest.raises(TruncatedSubmission) as e:
        parse_submission(filing, parse)
    assert dropped == ["A-1", "A-1"] and e.value.accession == "A-1"


@pytest.mark.parametrize(("form", "amendments"), [("10-K", False), ("10-K", True), ("10-Q", False), ("20-F", False)])
def test_rows_from_any_source_select_as_list_filings_does(monkeypatch, form, amendments):
    from edgar_engine import filings as filings_mod
    from edgar_engine.filings import list_filings, refs_for_form

    rows = [*ROWS, row("0001234567-21-000001", "10-K", "2021-03-01", ""),
            row("0001234567-20-000002", "20-F", "2020-10-15", "2020-06-30")]

    def get_filings(form, trigger_full_load):
        kept = [r for r in rows if r["form"] in form]
        return SimpleNamespace(data=SimpleNamespace(num_rows=len(kept), to_pylist=lambda: kept))

    company = SimpleNamespace(cik=CIK, fiscal_year_end="1231", get_filings=get_filings)
    monkeypatch.setattr(filings_mod, "_company", lambda c: c)
    assert refs_for_form(rows, CIK, form, amendments=amendments, fiscal_labels=LABELS, fiscal_year_end="1231") == \
        list_filings(company, form, amendments=amendments, fiscal_labels=LABELS)


def test_fiscal_labels_are_the_majority_label_of_each_filings_facts():
    from edgar_engine.filings import labels_from_companyfacts

    def fact(fy, fp):
        return {"accn": "a-1", "form": "10-K", "fy": fy, "fp": fp}

    body = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [fact(2024, "FY"), fact(2024, "FY"), fact(2023, "FY")]}}}}}
    assert dict(labels_from_companyfacts(body)) == {("a-1", "10-K"): (2024, "FY")}
    assert dict(labels_from_companyfacts(None)) == {}


def test_select_ref_reads_the_whole_history_only_when_the_recent_page_cannot_hold_the_period():
    asked = []

    def listed(full_history):
        asked.append(full_history)
        rows = ROWS if full_history else ROWS[:1]
        return refs_for_form(rows, CIK, "10-K", fiscal_labels=LABELS)

    assert select_ref(listed).accession == "0001234567-25-000010" and asked == [False]
    assert select_ref(listed, fiscal_year=2025) is None and asked == [False, False]
    assert select_ref(listed, fiscal_year=2023).accession == "0001234567-24-000050" and asked == [False, False, False, True]
    assert select_ref(listed, fiscal_year=2023, full_history=False) is None


def test_the_labels_version_moves_with_the_labels_function():
    """Fails when labels_from_companyfacts changes: bump LABELS_VERSION if what it returns changes, then record its new
    source digest here."""
    import hashlib
    import inspect

    digest = hashlib.sha256(inspect.getsource(labels_from_companyfacts).encode()).hexdigest()[:12]
    assert (LABELS_VERSION, digest) == (1, "d7aeb0e3613a")
    assert labels_engine() == "labels-v1"
