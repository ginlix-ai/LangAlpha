from datetime import date

import pytest

from edgar_engine.filings import FilingRef, get_filing, period_rule, pick_ref, refs_from_rows, select_filing

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


def test_filing_without_report_date_has_no_label():
    got = refs_from_rows([row("0001234567-24-000099", "8-K", "2024-11-02", "")], CIK, ["8-K"])
    assert got[0].report_date is None
    assert (got[0].fiscal_year, got[0].fiscal_period, got[0].fiscal_source) == (None, None, None)


def test_pick_latest_without_a_period():
    assert pick_ref(refs()).accession == "0001234567-25-000010"
    assert pick_ref([]) is None


def test_pick_by_fiscal_year_takes_earliest_filed_original():
    assert pick_ref(refs(), fiscal_year=2023).accession == "0001234567-24-000011"


def test_pick_amendment_never_beats_the_original():
    assert pick_ref(refs(amendments=True), fiscal_year=2023).accession == "0001234567-24-000011"


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

    from edgar_engine.filings import _companyfacts_labels, companyfacts_labels
    fact = {"accn": "0001234567-24-000011", "form": "10-K", "fy": 2023, "fp": "FY"}
    monkeypatch.setattr(entity_facts, "download_company_facts_from_sec",
                        lambda cik: {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [fact]}}}}})
    _companyfacts_labels.cache_clear()
    labels = companyfacts_labels(CIK)
    assert labels == {("0001234567-24-000011", "10-K"): (2023, "FY")}
    with pytest.raises(TypeError):
        labels[("0001234567-24-000011", "10-K")] = (2024, "FY")
    _companyfacts_labels.cache_clear()


def test_companyfacts_labels_do_not_keep_a_company_that_has_none_yet(monkeypatch):
    from edgar.entity import entity_facts
    from edgar.exceptions import CompanyFactsNotFoundError

    from edgar_engine.filings import _companyfacts_labels, companyfacts_labels
    calls = []

    def download(cik):
        calls.append(cik)
        if len(calls) == 1:
            raise CompanyFactsNotFoundError(cik)
        return {"facts": {"dei": {"X": {"units": {"USD": [{"accn": "A-1", "form": "10-K", "fy": 2024, "fp": "FY"}]}}}}}
    monkeypatch.setattr(entity_facts, "download_company_facts_from_sec", download)
    _companyfacts_labels.cache_clear()
    assert companyfacts_labels(CIK) == {}
    assert companyfacts_labels(CIK) == {("A-1", "10-K"): (2024, "FY")}
    _companyfacts_labels.cache_clear()


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
