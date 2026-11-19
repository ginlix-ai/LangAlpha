"""Statements and key metrics: what is kept, what is named as unread, and the submission path."""
import gzip
from types import SimpleNamespace

import pytest

from edgar_engine import statements
from edgar_engine.cache import LocalDirStore, StatementsCache
from edgar_engine.filings import FilingRef
from edgar_engine.statements import Financials

REF = FilingRef(1, "0000000001-26-000001", "10-K", "2026-02-01", "2025-12-31", 2025, "FY")


def test_the_reporting_currency_is_the_one_most_monetary_facts_use():
    def fin(*refs):
        units = {"usd": {"type": "simple", "measure": "iso4217:USD\n"},  # XML leaves a measure's whitespace in
                 "cny": {"type": "simple", "measure": " iso4217:CNY"},
                 "shares": {"type": "simple", "measure": "xbrli:shares"}}
        facts = {i: SimpleNamespace(unit_ref=r) for i, r in enumerate(refs)}
        return SimpleNamespace(xb=SimpleNamespace(units=units, _facts=facts))

    assert statements._currency(fin("usd", "usd", "cny", "shares", "shares", "shares")) == "USD"
    assert statements._currency(fin("usd", "cny")) is None and statements._currency(fin("shares")) is None


def test_a_statement_the_filing_lacks_is_not_listed_and_one_that_failed_is_named(monkeypatch):
    from edgar.financials import Financials as Extracted

    def broken():
        raise ValueError("bad")

    fake = SimpleNamespace(income_statement=lambda: SimpleNamespace(render=lambda: "income"),
                           balance_sheet=lambda: SimpleNamespace(render=lambda: None), cash_flow_statement=broken,
                           get_financial_metrics=lambda: {"revenue": 1.0})
    monkeypatch.setattr(Extracted, "extract", staticmethod(lambda filing: fake))
    monkeypatch.setattr(statements, "_currency", lambda fin: "USD")
    _, parts, _, unread = statements._read(None)
    assert parts == {"income statement": "income"} and unread == ["cash flow statement"]


def test_statements_that_all_failed_read_as_failed_and_only_complete_ones_are_kept(monkeypatch, tmp_path):
    from edgar_engine import filings

    cache = StatementsCache(LocalDirStore(tmp_path))
    monkeypatch.setattr(filings, "get_filing", lambda ref: None)
    monkeypatch.setattr(statements, "_read", lambda f: ({}, {}, None, ["income statement", "key metrics"]))
    assert statements.read(REF, cache) == Financials(failed=True)
    monkeypatch.setattr(statements, "_read", lambda f: ({"revenue": 1.0}, {}, "USD", ["key metrics"]))
    assert statements.read(REF, cache).unread == ("key metrics",) and not list(tmp_path.rglob("*.gz"))
    monkeypatch.setattr(statements, "_read", lambda f: ({"revenue": 1.0}, {"income statement": "i"}, "USD", []))
    kept = statements.read(REF, cache)
    assert kept.held == ("income statement",) and cache.get(REF.accession) == kept


@pytest.mark.parametrize("fin", [Financials(failed=True), Financials({"revenue": 1.0}, unread=("balance sheet",))])
def test_the_cache_refuses_a_read_that_is_not_complete(tmp_path, fin):
    cache = StatementsCache(LocalDirStore(tmp_path))
    with pytest.raises(ValueError):
        cache.put(REF.accession, fin)
    assert not list(tmp_path.rglob("*.gz"))


def test_xbrl_whose_instance_is_missing_reads_as_failed_so_it_is_never_kept(monkeypatch):
    from edgar.financials import Financials as Extracted

    linkbases_only = SimpleNamespace(xb=SimpleNamespace(_facts={}), income_statement=lambda: None,
                                     balance_sheet=lambda: None, cash_flow_statement=lambda: None,
                                     get_financial_metrics=lambda: {})
    monkeypatch.setattr(Extracted, "extract", staticmethod(lambda filing: linkbases_only))
    assert statements._financials(None, REF.accession) == Financials(failed=True)
    monkeypatch.setattr(Extracted, "extract", staticmethod(lambda filing: SimpleNamespace(
        xb=None, income_statement=lambda: None, balance_sheet=lambda: None, cash_flow_statement=lambda: None,
        get_financial_metrics=lambda: {})))
    assert statements._financials(None, REF.accession).complete  # no XBRL at all: complete, and kept


def test_a_cache_entry_that_cannot_be_decompressed_reads_as_a_miss(tmp_path):
    store = LocalDirStore(tmp_path)
    cache = StatementsCache(store)
    store.put(cache._key(REF.accession), gzip.compress(b"x")[:12] + b"\x00" * 20)
    assert cache.get(REF.accession) is None


@pytest.mark.network
def test_the_submission_text_gives_the_same_statements_as_the_filing():
    from edgar_engine.filings import get_filing, select_filing

    ref = select_filing(320193, "10-K", fiscal_year=2024)
    text = get_filing(ref).full_text_submission()
    from_text, read = statements.from_submission(text, ref), statements.read(ref)
    assert from_text.complete and from_text.statements and from_text == read
