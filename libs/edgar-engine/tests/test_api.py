from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from edgar_engine import api


@dataclass(frozen=True)
class _Report:
    accession: str
    filer_cik: int | None
    form: str
    filed: str
    amendment_type: str | None = None
    amendment_no: int | None = None
    period: str = "2024-06-30"
    has_information_table: bool = True
    declared_entries: int = 1


def test_13f_reports_resolve_and_return_under_the_filer_asked_for_when_a_report_names_no_cik(monkeypatch):
    reports = {"a1": _Report("a1", None, "13F-HR", "2024-08-14"),
               "a2": _Report("a2", 1067983, "13F-HR/A", "2024-09-01", "RESTATEMENT", 1)}
    listed = SimpleNamespace(data=SimpleNamespace(to_pylist=lambda: [
        {"accession_number": a, "reportDate": "2024-06-30"} for a in shown]))
    monkeypatch.setattr(api, "_company", lambda filer: SimpleNamespace(cik=1067983, get_filings=lambda form: listed))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: accession)
    monkeypatch.setattr(api, "parse_13f", lambda body, accession: reports[accession])
    monkeypatch.setattr(api, "holding_rows", lambda f: [])
    shown = ["a1", "a2"]
    assert [f.accession for f in api.get_13f_holdings(1067983, "2024-06-30")] == ["a2"]
    shown = ["a1"]
    assert [(f.accession, f.filer_cik) for f in api.get_13f_holdings(1067983, "2024-06-30")] == [("a1", 1067983)]
    reports["a3"] = _Report("a3", 999, "13F-HR", "2024-08-14")
    shown = ["a1", "a3"]
    with pytest.warns(UserWarning, match="a3: filed under CIK 999"):
        assert [f.accession for f in api.get_13f_holdings(1067983, "2024-06-30")] == ["a1"]


def test_insider_filings_are_listed_from_the_xml_era_when_start_is_omitted(monkeypatch):
    asked = []
    company = SimpleNamespace(cik=320193, get_filings=lambda form, filing_date: asked.append(filing_date))
    monkeypatch.setattr(api, "_company", lambda c: company)
    assert api.get_insider_filings(320193, end="2024-10-31") == []
    assert asked == [f"{api.OWNERSHIP_XML_FROM}:2024-10-31"]


def test_a_13f_report_that_does_not_parse_is_left_out_with_a_warning(monkeypatch):
    listed = SimpleNamespace(data=SimpleNamespace(to_pylist=lambda: [{"accession_number": "a1", "reportDate": ""}]))
    monkeypatch.setattr(api, "_company", lambda filer: SimpleNamespace(cik=1, get_filings=lambda form: listed))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: b"")
    monkeypatch.setattr(api, "parse_13f", lambda body, accession: None)
    with pytest.warns(UserWarning, match="a1: no readable 13F document"):
        assert api.get_13f_holdings(1, "2024-06-30") == []
