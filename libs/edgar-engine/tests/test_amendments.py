"""Amendments: linking to the original, scope, the as-amended merge and get_sections views, on synthetic filings."""
from dataclasses import replace
from datetime import date

import pytest

import edgar_engine.amendments as amendments_mod
import edgar_engine.api as api_mod
import edgar_engine.filings as filings_mod
import edgar_engine.sections.extract as extract_mod
import edgar_engine.submission as submission_mod
from edgar_engine import get_sections
from edgar_engine.amendments import (amendment_scope, cover_period, merge, nth_amendment, original_filed_on,
                                     resolve_amends)
from edgar_engine.cache import LocalDirStore, SectionsCache
from edgar_engine.filings import FilingRef, base_form, link_amendments, refs_from_rows, select_filing
from edgar_engine.sections import extract_sections
from edgar_engine.sections.model import DocumentInfo, FilingSections, Section, Span, Target
from edgar_engine.sections.version import SECTIONS_VERSION

CIK = 1234567


def row(accession, form, filed, report):
    return {"accession_number": accession, "form": form, "filing_date": date.fromisoformat(filed), "reportDate": report}


def refs(rows, forms=("10-K", "10-K405")):
    return refs_from_rows(rows, CIK, forms, amendments=True, fiscal_year_end="1231")


def by_accession(refs):
    return {r.accession: r for r in refs}


# Filing list: which original an amendment amends.

def test_amendment_links_to_the_original_of_its_period():
    got = by_accession(refs([
        row("A-23", "10-K", "2023-02-20", "2022-12-31"),
        row("A-24", "10-K", "2024-02-20", "2023-12-31"),
        row("A-24a", "10-K/A", "2024-04-29", "2023-12-31"),
    ]))
    assert got["A-24a"].amends == "A-24"
    assert (got["A-24a"].filing_status, got["A-24"].filing_status) == ("amendment", "original")
    assert got["A-24"].amends is None


def test_ten_k405_original_and_ten_k405_amendment_share_the_base_form():
    got = by_accession(refs([
        row("B-98", "10-K405", "1998-03-30", "1997-12-31"),
        row("B-98a", "10-K/A", "1998-04-30", "1997-12-31"),
        row("B-98b", "10-K405/A", "1998-09-14", "1997-12-31"),
    ]))
    assert got["B-98a"].amends == got["B-98b"].amends == "B-98"
    assert base_form("10-K405/A") == base_form("10-K/A") == "10-K"


def test_two_amendments_and_one_filed_years_later_are_listed_in_filing_order():
    rows = [
        row("C-14", "10-K", "2014-03-26", "2013-12-31"),
        row("C-15", "10-K", "2015-03-20", "2014-12-31"),
        row("C-19a", "10-K/A", "2019-04-09", "2013-12-31"),
        row("C-14a", "10-K/A", "2014-04-30", "2013-12-31"),
    ]
    listed = refs(rows)
    original = by_accession(listed)["C-14"]
    assert [r.accession for r in sorted(listed, key=lambda r: r.filing_date) if r.amends == original.accession] == [
        "C-14a", "C-19a"]


def test_amendment_whose_period_matches_no_original_is_left_unlinked():
    got = by_accession(refs([
        row("D-21", "10-K", "2021-09-28", "2021-06-30"),
        row("D-21a", "10-K/A", "2021-12-14", "2021-09-30"),
        row("D-21b", "10-K/A", "2021-12-15", ""),
    ]))
    assert got["D-21a"].amends is None
    assert got["D-21b"].amends is None


def test_amendment_never_links_to_an_original_filed_after_it_but_may_share_its_day():
    got = by_accession(refs([
        row("E-06a", "10-Q/A", "2006-08-07", "2006-06-30"),
        row("E-06", "10-Q", "2006-08-07", "2006-06-30"),
        row("E-07a", "10-Q/A", "2007-05-01", "2007-03-31"),
        row("E-07", "10-Q", "2007-05-10", "2007-03-31"),
    ], forms=("10-Q",)))
    assert got["E-06a"].amends == "E-06"
    assert got["E-07a"].amends is None


def test_link_amendments_takes_the_latest_original_filed_before_a_refile():
    linked = by_accession(link_amendments([
        FilingRef(CIK, "F-1", "10-K", "2024-02-21", "2023-12-31"),
        FilingRef(CIK, "F-2", "10-K", "2024-03-30", "2023-12-31"),
        FilingRef(CIK, "F-2a", "10-K/A", "2024-04-30", "2023-12-31"),
    ]))
    assert linked["F-2a"].amends == "F-2"


def test_nth_amendment_counts_from_one_or_from_the_end():
    assert [nth_amendment(["a", "b"], n) for n in (1, 2, 3, -1, -2, -3)] == ["a", "b", None, "b", "a", None]
    with pytest.raises(ValueError):
        nth_amendment(["a"], 0)


ROWS = [
    row("0001234567-24-000010", "10-K", "2024-02-20", "2023-12-31"),
    row("0001234567-24-000020", "10-K/A", "2024-04-29", "2023-12-31"),
    row("0001234567-25-000030", "10-K/A", "2025-06-02", "2023-12-31"),
    row("0001234567-25-000040", "10-K", "2025-02-20", "2024-12-31"),
]


@pytest.fixture
def filer(monkeypatch):
    """list_filings over ROWS, honoring amendments and filed_on_or_before, with no network."""
    def fake(company, form, *, amendments=False, filed_on_or_before=None, full_history=True, fiscal_labels=None):
        return refs_from_rows(ROWS, CIK, ("10-K", "10-K405"), amendments=amendments,
                              filed_on_or_before=filed_on_or_before, fiscal_year_end="1231")
    monkeypatch.setattr(filings_mod, "list_filings", fake)
    monkeypatch.setattr(api_mod, "list_filings", fake)
    return fake


def test_select_filing_picks_originals_only(filer):
    assert select_filing(CIK, "10-K").accession == "0001234567-25-000040"
    assert select_filing(CIK, "10-K", fiscal_year=2023).accession == "0001234567-24-000010"
    assert select_filing(CIK, "10-K", fiscal_year=2023, filed_on_or_before="2024-01-01") is None


# Reading an amendment's own text.

def test_cover_period_reads_the_cover_line():
    assert cover_period("FORM 10-K/A\n(Mark One)\nFor the fiscal year ended December 31, 2008\n") == "2008-12-31"
    assert cover_period("FORM 10-Q/A\nFor the Quarterly Period Ended June 30, 2006\n") == "2006-06-30"
    assert cover_period("FORM 20-F/A\nFor the fiscal year ended 31 December 2022\n") == "2022-12-31"
    assert cover_period("FORM 10-K/A\nFor the fiscal year ended: Sept. 30, 2019\n") == "2019-09-30"
    assert cover_period("FORM 10-K/A\nAmendment No. 1\n") is None


def test_original_filed_on_keeps_only_dates_for_the_report_being_amended():
    text = ("EXPLANATORY NOTE\n"
            "This Amendment No. 1 amends our Annual Report on Form 10-K for the year ended December 31, 2008, "
            "originally filed with the U.S. Securities and Exchange Commission on March 26, 2009 (the Original Filing).\n"
            "As previously reported in our current report on Form 8-K filed with the SEC on August 4, 2009, we changed auditors.\n")
    assert original_filed_on(text) == ["2009-03-26"]


def test_original_filed_on_takes_the_report_named_last_before_the_date():
    assert original_filed_on("This Form 10-K/A discusses our Form 8-K filed on March 1, 2020.\n") == []
    assert original_filed_on("This Form 10-K/A discusses our Form 8-K originally filed on February 20, 2024.\n") == []
    assert original_filed_on("This Amendment to Annual Report on Form 20-F/A is being filed solely to update Item 16 "
                             "of the Form 20-F filed on April 7, 2000.\n") == ["2000-04-07"]
    assert original_filed_on("This Amendment No. 1 amends our Annual Report on Form 10\u2011K for the fiscal year ended "
                             "December 31, 2019, as filed with the SEC on March 26, 2020.\n") == ["2020-03-26"]


def test_original_filed_on_reads_a_date_across_hard_line_wraps():
    text = ("EXPLANATORY NOTE\n"
            "This Amendment No. 1 amends our Annual Report on Form 10-K for the year ended December 31, 2008, originally\n"
            "filed with the Securities and Exchange Commission on\n"
            "March 26, 2009. As previously reported in our current report on Form 8-K filed with the\n"
            "SEC on August 4, 2009, we changed auditors.\n")
    assert original_filed_on(text) == ["2009-03-26"]


def amendment_text(form, filed, text):
    return FilingSections(accession="X-a", form=form, filing_date=filed, items={}, text=text,
                          engine_version=SECTIONS_VERSION)


def test_resolve_amends_by_cover_period_then_by_original_filing_date():
    listed = [
        FilingRef(CIK, "G-1", "10-K", "2009-03-26", "2009-03-26"),
        FilingRef(CIK, "G-2", "10-K", "2010-03-30", "2009-12-31"),
        FilingRef(CIK, "G-3", "10-Q", "2009-05-12", "2009-03-31"),
    ]
    by_cover = amendment_text("10-K/A", "2010-06-01", "For the fiscal year ended December 31, 2009\n")
    assert resolve_amends(by_cover, listed) == "G-2"
    by_note = amendment_text("10-K/A", "2009-10-09", "For the fiscal year ended December 31, 2008\n"
                             "This amends our Form 10-K originally filed with the SEC on March 26, 2009.\n")
    assert resolve_amends(by_note, listed) == "G-1"
    neither = amendment_text("10-K/A", "2009-10-09", "For the fiscal year ended December 31, 2005\n")
    assert resolve_amends(neither, listed) is None
    later_date = amendment_text("10-K/A", "2010-06-01", "For the fiscal year ended December 31, 2005\n"
                                "This amends our Form 10-K originally filed with the SEC on March 20, 2006.\n"
                                "Our Annual Report on Form 10-K was filed on March 30, 2010.\n")
    assert resolve_amends(later_date, listed) is None
    same_day = [FilingRef(CIK, "Z-5", "10-K", "2010-06-01", "2009-12-31", accepted="2010-06-01T09:00:00"),
                FilingRef(CIK, "B-5", "10-K", "2010-06-01", "2009-12-31", accepted="2010-06-01T11:00:00"),
                FilingRef(CIK, "X-a", "10-K/A", "2010-06-01", "2009-12-31", accepted="2010-06-01T10:00:00")]
    assert resolve_amends(amendment_text("10-K/A", "2010-06-01", "For the fiscal year ended December 31, 2009\n"), same_day) == "Z-5"


def test_the_explanatory_note_date_comes_before_dates_in_the_exhibit_index():
    listed = [FilingRef(CIK, "H-1", "10-K", "2007-04-02", "2006-12-31"),
              FilingRef(CIK, "H-2", "10-K", "2009-03-26", "2009-03-26")]
    text = ("For the fiscal year ended December 31, 2008\n"
            "This amends our Annual Report on Form 10-K originally filed with the SEC on March 26, 2009.\n"
            "3.1 Bylaws, incorporated by reference to our Form 10-K filed with the SEC on April 2, 2007.\n")
    assert resolve_amends(amendment_text("10-K/A", "2009-10-09", text), listed) == "H-2"


# Scope and the as-amended merge.

def section(item, text, start=0, heading=None, target=None):
    return Section(item=item, status="present", text=text, start=start, end=start + len(text),
                   heading=heading if heading is not None else text.split("\n")[0], target=target)


def filing(accession, form, filed, items, scope=None, head=""):
    text, out = head, {}
    for iid, body in items.items():
        out[iid] = section(iid, body, start=len(text))
        text += body + "\n"
    fs = FilingSections(accession=accession, form=form, filing_date=filed, items=out, text=text,
                        engine_version=SECTIONS_VERSION, documents=[DocumentInfo(1, form, "d.htm", 0, text.count("\n"))])
    fs.scope = scope or (amendment_scope(fs) if fs.filing_status == "amendment" else None)
    return fs


ORIGINAL = filing("O", "10-K", "2024-02-20", {
    "1": "Item 1. Business\nwidgets", "1A": "Item 1A. Risk Factors\nrisks", "7": "Item 7. MD&A\nsales rose",
    "8": "Item 8. Financial Statements\nnet income 10", "10": "Item 10. Directors\nsee proxy",
    "11": "Item 11. Executive Compensation\nsee proxy", "15": "Item 15. Exhibits and Financial Statement Schedules\n3.1 4.1 21 23",
})
PART_III = filing("P", "10-K/A", "2024-04-29", {
    "10": "Item 10. Directors\nthe board", "11": "Item 11. Executive Compensation\npay table",
    "15": "Item 15. Exhibits\n31.1 31.2",
})
FULL_ITEMS = {iid: f"Item {iid}. Restated\nas restated" for iid in ["1", "2", "3", "4", "5", "7", "7A", "9", "9A", "9B", "12"]}
FULL = filing("F", "10-K/A", "2024-06-01", {
    "1": "Item 1. Business\nwidgets", **FULL_ITEMS, "7": "Item 7. MD&A\nsales fell",
    "8": "Item 8. Financial Statements\nnet income 7", "10": "Item 10. Directors\nthe board",
    "15": "Item 15. Exhibits\n3.1 4.1 21 23 31.1",
})


def test_scope_by_parts_printed():
    assert PART_III.scope == "partial"
    assert FULL.scope == "complete"
    assert filing("E", "10-K/A", "2024-05-01", {"15": "Item 15. Exhibits\n10.1"}).scope == "exhibits"
    assert filing("C", "10-K/A", "2024-05-01", {}).scope == "cover"
    assert filing("Q", "10-Q/A", "2024-05-01", {"I-1": "Item 1. Financial Statements\nx", "I-2": "Item 2. MD&A\ny",
                                                 "II-6": "Item 6. Exhibits\n31.1"}).scope == "partial"
    assert filing("Q2", "10-Q/A", "2024-05-01", {iid: f"Item {iid[-1]}. x\ny" for iid in
                                                 ["I-1", "I-2", "I-3", "I-4", "II-1", "II-1A"]}).scope == "complete"
    assert filing("H", "40-F/A", "2024-05-01", {"FEES": "Principal Accountant Fees\nx"}).scope == "partial"
    assert filing("H2", "40-F/A", "2024-05-01", {"AIF": "Annual Information Form\nx", "AFS": "Financial Statements\nx",
                                                 "MDA": "MD&A\nx", "FEES": "Fees\ny"}).scope == "complete"


def test_an_amendment_printing_one_item_per_part_is_partial():
    assert filing("Q3", "10-Q/A", "2024-05-01", {iid: f"Item {iid[-1]}. x\ny" for iid in
                                                 ["I-1", "I-2", "I-4", "II-1A"]}).scope == "partial"
    assert filing("T", "20-F/A", "2024-05-01", {iid: f"Item {iid}. x\ny" for iid in ["5", "15", "18", "19"]}).scope == "partial"
    assert filing("K", "10-K/A", "2024-05-01", {iid: f"Item {iid}. x\ny" for iid in ["1A", "9A", "10"]}).scope == "partial"
    afs_only = filing("A", "40-F/A", "2024-05-01", {"UNDERTAKING": "Undertaking\nx", "CONSENT": "Consent\nx",
                                                    "AFS": "Audited Financial Statements\nrestated"})
    assert afs_only.scope == "partial"


def test_an_amendment_carrying_a_plan_annual_report_changes_no_item():
    plan = filing("K11", "10-K/A", "2024-06-20", {"4": "Item 4. Financial Statements and Supplemental Schedules\nplan assets"},
                  head="This Amendment is filed to furnish the annual report on Form 11-K of the Savings Plan.\n")
    assert plan.scope == "cover"
    m = merge(ORIGINAL, [plan])
    assert m.amendments == ["K11"] and "4" not in m.items


def test_a_plan_report_named_only_in_the_exhibit_list_leaves_a_part_three_amendment_partial():
    part3 = filing("P3", "10-K/A", "2024-04-29", {"10": "Item 10. Directors\nboard", "11": "Item 11. Compensation\npay",
                                               "15": "Item 15. Exhibits\n99.1 Annual report on Form 11-K of the Savings Plan"})
    assert part3.scope == "partial"


def test_old_ten_k_exhibit_item_is_known_by_its_heading():
    old = filing("Old", "10-K/A", "2001-04-30", {"14": "Item 14. Exhibits, Financial Statement Schedules, and Reports on Form 8-K\n27"})
    assert old.scope == "exhibits"


def test_partial_amendment_replaces_the_items_it_prints_its_exhibit_list_included():
    m = merge(ORIGINAL, [PART_III])
    assert (m.view, m.filing_status, m.accession, m.amendments) == ("as_amended", "original", "O", ["P"])
    assert {k: s.source for k, s in m.items.items()} == {"1": "O", "1A": "O", "7": "O", "8": "O", "10": "P", "11": "P",
                                                          "15": "P"}
    assert m["10"].text.endswith("the board")
    assert not m["15"].text.endswith("3.1 4.1 21 23")


def test_complete_amendment_replaces_the_item_set_and_later_amendments_apply_on_top():
    later_part_iii = replace(PART_III, accession="P2", filing_date="2024-07-01")
    m = merge(ORIGINAL, [later_part_iii, FULL])
    assert m.amendments == ["F", "P2"]
    assert list(m.items) == ["1", "2", "3", "4", "5", "7", "7A", "8", "9", "9A", "9B", "10", "11", "12", "15"]
    assert {k: s.source for k, s in m.items.items() if s.source != "F"} == {"10": "P2", "11": "P2", "15": "P2"}
    assert "1A" not in m.items


def test_an_exhibits_amendment_replaces_the_exhibit_list_and_a_cover_amendment_changes_no_item():
    exhibits = filing("E", "10-K/A", "2024-05-01", {"15": "Item 15. Exhibits\n10.1"})
    cover = filing("C", "10-K/A", "2024-05-02", {})
    m = merge(ORIGINAL, [exhibits, cover])
    assert m.amendments == ["E", "C"]
    assert {k: s.source for k, s in m.items.items() if s.source != "O"} == {"15": "E"}


def test_merged_offsets_and_target_spans_index_the_merged_text():
    stub = section("12", "Item 12. Ownership\nsee below", heading="Item 12. Ownership",
                   target=Target(kind="in_document", spans=[Span(0, 18)], text="Item 12. Ownership"))
    amendment = FilingSections(accession="S", form="10-K/A", filing_date="2024-05-01", items={"12": stub},
                               text="Item 12. Ownership\nsee below\n", engine_version=SECTIONS_VERSION,
                               documents=[DocumentInfo(1, "10-K/A", "s.htm", 0, 2)])
    amendment.scope = "partial"
    m = merge(ORIGINAL, [PART_III, amendment])
    for s in m.items.values():
        assert m.text[s.start:s.end] == s.text
    span = m["12"].target.spans[0]
    assert m.text[span.start:span.end] == "Item 12. Ownership"
    assert [d.accession for d in m.documents] == ["O", "P", "S"]
    assert m.text.count("\n") == m.documents[-1].line_end
    assert ORIGINAL["10"].source is None and PART_III["10"].start == 0


def test_merge_with_no_amendments_is_the_original_labeled():
    m = merge(ORIGINAL, [])
    assert (m.view, m.amendments, m.text) == ("as_amended", [], ORIGINAL.text)
    assert {s.source for s in m.items.values()} == {"O"}


def test_forty_f_amendment_reads_the_documents_it_refiles_as_exhibits():
    from test_sections import BODY, html, p, submission
    wrapper = html(p("FORM 40-F/A"), p("(Amendment No. 2)"), p("EXPLANATORY NOTE"),
                   p("This amendment files the restated management's discussion and analysis as Exhibit 99.2."))
    mda = html(p("Exhibit 99.2"), p("AMENDED AND RESTATED"), p("MANAGEMENT'S DISCUSSION AND ANALYSIS"),
               p("For the years ended December 31, 2022 and 2021"), p(BODY))
    cert = html(p("Exhibit 99.4"), p("Certification of Principal Executive Officer"), p("I have reviewed this amendment."))
    fs = extract_sections(submission("40-F/A", "2024-04-02", ("40-F/A", wrapper), ("EX-99.2", mda), ("EX-99.4", cert)))
    assert list(fs.items) == ["MDA"]
    assert fs["MDA"].text.startswith("MANAGEMENT'S DISCUSSION AND ANALYSIS") and BODY in fs["MDA"].text
    assert "Certification" not in fs["MDA"].text
    assert amendment_scope(fs) == "partial"
    original = extract_sections(submission("40-F", "2023-03-31", ("40-F", wrapper.replace("40-F/A", "40-F")),
                                           ("EX-99.2", mda)))
    assert "MDA" not in original.items


def test_an_amendment_lead_in_opens_the_item_it_amends():
    from test_sections import BODY, html, p, submission
    lead = "Item {} of the Annual Report for the fiscal year ended June 30, 2024 is hereby amended in full as follows:"
    body = html(p("FORM 10-K/A"), p("PART III"), p(lead.format(10)), p("ITEM 10. DIRECTORS AND EXECUTIVE OFFICERS"), p(BODY),
                p(lead.format(11)), p("ITEM 11. EXECUTIVE COMPENSATION"), p(BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-K/A", "2024-10-28", ("10-K/A", body)))
    assert "hereby amended" not in fs["10"].text.split("ITEM 10.", 1)[1]
    assert fs["11"].text.startswith("ITEM 11. EXECUTIVE COMPENSATION")


def test_forty_f_amendment_printing_its_annual_information_form_keeps_its_headings_out_of_the_wrapper():
    from test_sections import BODY, html, p, submission
    body = html(p("FORM 40-F/A"), p("EXPLANATORY NOTE"), p("This amendment is filed to include the annual information form."),
                p("UNDERTAKING"), p("The Registrant undertakes to make available representatives to respond to inquiries."),
                p("FORWARD-LOOKING INFORMATION"), p(BODY),
                p("2023 Annual information form"), p("February 15, 2024"), *[p(BODY)] * 3, p("AUDIT COMMITTEE"),
                *[p(BODY)] * 3, p("PRINCIPAL ACCOUNTANT FEES AND SERVICES"), *[p(BODY)] * 3,
                p("CODE OF BUSINESS CONDUCT AND ETHICS"), *[p(BODY)] * 3)
    fs = extract_sections(submission("40-F/A", "2024-03-01", ("40-F/A", body)))
    assert fs["UNDERTAKING"].text.startswith("UNDERTAKING")
    assert fs["AIF"].text.startswith("2023 Annual information form") and "CODE OF BUSINESS CONDUCT" in fs["AIF"].text
    assert not {"AC", "FEES", "CODE"} & set(fs.items)


def test_part_iii_amendment_extracts_only_what_it_prints():
    html = ("<html><body><p>FORM 10-K/A</p><p>For the fiscal year ended December 31, 2023</p>"
            "<p>EXPLANATORY NOTE</p><p>This Amendment No. 1 amends our Annual Report on Form 10-K originally filed on "
            "February 20, 2024 to include Part III information.</p><p>PART III</p>"
            "<p>ITEM 10. DIRECTORS, EXECUTIVE OFFICERS AND CORPORATE GOVERNANCE</p>"
            "<p>Our board of directors has seven members who are elected each year by the stockholders.</p>"
            "<p>ITEM 11. EXECUTIVE COMPENSATION</p>"
            "<p>The compensation committee sets the pay of our executive officers every year in March.</p>"
            "<p>PART IV</p><p>ITEM 15. EXHIBITS AND FINANCIAL STATEMENT SCHEDULES</p><p>31.1 Certification</p>"
            "<p>SIGNATURES</p><p>Pursuant to the requirements of the Securities Exchange Act of 1934, the registrant has "
            "duly caused this report to be signed.</p></body></html>")
    sub = ("<SEC-HEADER>\nACCESSION NUMBER:\t\t0001234567-24-000020\nCONFORMED SUBMISSION TYPE:\t10-K/A\n"
           "FILED AS OF DATE:\t\t20240429\n</SEC-HEADER>\n<DOCUMENT>\n<TYPE>10-K/A\n<SEQUENCE>1\n<FILENAME>a.htm\n"
           f"<TEXT>\n{html}\n</TEXT>\n</DOCUMENT>")
    fs = extract_sections(sub)
    assert set(fs.items) == {"10", "11", "15"}
    assert (fs.filing_status, amendment_scope(fs)) == ("amendment", "partial")
    assert cover_period(fs.text) == "2023-12-31"
    assert original_filed_on(fs.text) == ["2024-02-20"]


# get_sections views over a fake filer.

TEXTS = {
    "0001234567-24-000010": ("10-K", "2024-02-20", ORIGINAL),
    "0001234567-24-000020": ("10-K/A", "2024-04-29", PART_III),
    "0001234567-25-000030": ("10-K/A", "2025-06-02", filing("x", "10-K/A", "2025-06-02", {
        "11": "Item 11. Executive Compensation\nrevised pay table"})),
    "0001234567-25-000040": ("10-K", "2025-02-20", ORIGINAL),
}


class FakeFiling:
    def __init__(self, ref):
        self.form, self.filing_date, self.accession_no = ref.form, ref.filing_date, ref.accession

    def full_text_submission(self):
        FETCHED.append(self.accession_no)
        return "unused"


FETCHED: list[str] = []


@pytest.fixture
def engine(monkeypatch, filer):
    """get_filing and extract_sections replaced by the synthetic filings above."""
    def extract(submission, *, form, filing_date, accession):
        fs = TEXTS[accession][2]
        return replace(fs, accession=accession, form=form, filing_date=filing_date,
                       items={k: replace(s) for k, s in fs.items.items()}, filing_status=None, scope=None)

    FETCHED.clear()
    monkeypatch.setattr(amendments_mod, "get_filing", FakeFiling)
    monkeypatch.setattr(extract_mod, "extract_sections", extract)


def test_default_is_the_original_as_filed(engine):
    fs = get_sections(CIK, "10-K", fiscal_year=2023)
    assert (fs.accession, fs.filing_status, fs.amends, fs.view, fs.scope) == (
        "0001234567-24-000010", "original", None, "as_filed", None)
    assert {s.source for s in fs.items.values()} == {"0001234567-24-000010"}
    assert FETCHED == ["0001234567-24-000010"]


def test_amendment_zero_is_refused_before_anything_is_read(engine):
    with pytest.raises(ValueError):
        get_sections(CIK, "10-K", fiscal_year=1999, amendment=0)
    assert FETCHED == []


def test_an_amendment_as_filed(engine):
    fs = get_sections(CIK, "10-K", fiscal_year=2023, amendment=1)
    assert (fs.accession, fs.filing_status, fs.amends, fs.scope) == (
        "0001234567-24-000020", "amendment", "0001234567-24-000010", "partial")
    assert set(fs.items) == {"10", "11", "15"}
    assert get_sections(CIK, "10-K", fiscal_year=2023, amendment=-1).accession == "0001234567-25-000030"
    assert get_sections(CIK, "10-K", fiscal_year=2023, amendment=3) is None


def test_the_first_amendment_is_read_without_reading_later_ones(engine):
    assert get_sections(CIK, "10-K", fiscal_year=2023, amendment=1).accession == "0001234567-24-000020"
    assert get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended", amendment=1).amendments == ["0001234567-24-000020"]
    assert "0001234567-25-000030" not in FETCHED


def test_as_amended_merges_every_amendment_in_filing_order(engine):
    fs = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended")
    assert fs.amendments == ["0001234567-24-000020", "0001234567-25-000030"]
    assert {k: s.source[-6:] for k, s in fs.items.items()} == {
        "1": "000010", "1A": "000010", "7": "000010", "8": "000010", "10": "000020", "11": "000030", "15": "000020"}
    through_first = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended", amendment=1)
    assert through_first.amendments == ["0001234567-24-000020"]
    assert through_first["11"].source == "0001234567-24-000020"
    as_of = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended", filed_on_or_before="2024-12-31")
    assert as_of.amendments == ["0001234567-24-000020"]


def test_as_amended_reads_through_the_per_accession_cache(engine, tmp_path):
    cache = SectionsCache(LocalDirStore(tmp_path))
    first = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended", cache=cache)
    fetched = list(FETCHED)
    again = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended", cache=cache)
    assert FETCHED == fetched and sorted(fetched) == sorted(set(fetched))
    assert again.items == first.items
    stored = sorted(p.name for p in (tmp_path / "sections" / f"v{SECTIONS_VERSION}").iterdir())
    assert stored == ["0001234567-24-000010.json.gz", "0001234567-24-000020.json.gz", "0001234567-25-000030.json.gz"]
    assert cache.get("0001234567-24-000010").view == "as_filed"


def test_unknown_view_is_refused(engine):
    with pytest.raises(ValueError):
        get_sections(CIK, "10-K", fiscal_year=2023, view="as_restated")


def test_amendment_the_list_cannot_place_is_placed_by_its_cover(engine, monkeypatch):
    orphan = "0001234567-24-000025"
    monkeypatch.setitem(TEXTS, orphan, ("10-K/A", "2024-05-01", filing(
        "y", "10-K/A", "2024-05-01", {"12": "Item 12. Security Ownership\ntable"},
        head="For the fiscal year ended December 31, 2023\n")))
    rows = [*ROWS, row(orphan, "10-K/A", "2024-05-01", "2024-05-01")]

    def fake(company, form, *, amendments=False, filed_on_or_before=None, full_history=True, fiscal_labels=None):
        return refs_from_rows(rows, CIK, ("10-K", "10-K405"), amendments=amendments,
                              filed_on_or_before=filed_on_or_before, fiscal_year_end="1231")
    monkeypatch.setattr(api_mod, "list_filings", fake)
    fs = get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended")
    assert fs.amendments == ["0001234567-24-000020", orphan, "0001234567-25-000030"]
    assert fs["12"].source == orphan
    assert get_sections(CIK, "10-K", fiscal_year=2023, amendment=2).amends == "0001234567-24-000010"


def test_amendment_whose_cover_names_another_year_is_not_merged_into_this_one(engine, monkeypatch):
    stray = "0001234567-25-000035"
    monkeypatch.setitem(TEXTS, stray, ("10-K/A", "2025-06-02", filing(
        "z", "10-K/A", "2025-06-02", {"8": "Item 8. Financial Statements\nrestated"},
        head="For the fiscal year ended December 31, 2024\n")))
    rows = [*ROWS, row(stray, "10-K/A", "2025-06-02", "2023-12-31")]

    def fake(company, form, *, amendments=False, filed_on_or_before=None, full_history=True, fiscal_labels=None):
        return refs_from_rows(rows, CIK, ("10-K", "10-K405"), amendments=amendments,
                              filed_on_or_before=filed_on_or_before, fiscal_year_end="1231")
    monkeypatch.setattr(api_mod, "list_filings", fake)
    assert stray not in get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended").amendments


def test_amendment_the_list_links_to_another_year_is_placed_by_its_cover(engine, monkeypatch):
    mistyped = "0001234567-25-000045"
    monkeypatch.setitem(TEXTS, mistyped, ("10-K/A", "2025-03-01", filing(
        "w", "10-K/A", "2025-03-01", {"9A": "Item 9A. Controls and Procedures\nmaterial weakness"},
        head="For the fiscal year ended December 31, 2023\n")))
    rows = [*ROWS, row(mistyped, "10-K/A", "2025-03-01", "2024-12-31")]

    def fake(company, form, *, amendments=False, filed_on_or_before=None, full_history=True, fiscal_labels=None):
        return refs_from_rows(rows, CIK, ("10-K", "10-K405"), amendments=amendments,
                              filed_on_or_before=filed_on_or_before, fiscal_year_end="1231")
    monkeypatch.setattr(api_mod, "list_filings", fake)
    assert get_sections(CIK, "10-K", fiscal_year=2023, view="as_amended")["9A"].source == mistyped
    assert mistyped not in get_sections(CIK, "10-K", fiscal_year=2024, view="as_amended").amendments

def test_an_amendment_that_cannot_be_read_is_set_aside_when_asked_and_the_rest_still_read(engine, monkeypatch):
    read_one = extract_mod.extract_sections

    def extract(submission, *, form, filing_date, accession):
        if accession == "0001234567-24-000020":
            raise ValueError("truncated")
        return read_one(submission, form=form, filing_date=filing_date, accession=accession)

    monkeypatch.setattr(extract_mod, "extract_sections", extract)
    monkeypatch.setattr(submission_mod, "_drop_cached", lambda filing: None)
    refs = filings_mod.list_filings(CIK, "10-K", amendments=True)
    original = next(r for r in refs if r.accession == "0001234567-24-000010")
    unreadable = []

    found = amendments_mod.amended_sections(original, refs, unreadable=unreadable)

    assert [a.accession for a in found] == ["0001234567-25-000030"]
    assert [r.accession for r in unreadable] == ["0001234567-24-000020"]
    with pytest.raises(ValueError):
        amendments_mod.amended_sections(original, refs)


def test_an_amendment_restates_the_statements_when_it_prints_their_item_or_refiles_the_report():
    part_iii = filing("a", "10-K/A", "2024-04-29", {"10": "Item 10. Directors\nThe board.",
                                                     "11": "Item 11. Executive Compensation\nPay."})
    restated = filing("b", "10-K/A", "2024-04-29", {"8": "Item 8. Financial Statements\nRestated balance sheet."})
    assert not amendments_mod.restates_statements(part_iii) and amendments_mod.restates_statements(restated)
    assert amendments_mod.restates_statements(replace(part_iii, scope="complete"))


# Live: an original with a Part III amendment and a second, narrower one, pinned by accession.

LIVE_ORIGINAL = "0001171843-24-001382"
LIVE_AMENDMENTS = ["0001193125-24-108802", "0001193125-24-127331"]


@pytest.mark.network
def test_real_amendments_merge_by_source():
    from edgar import find, set_identity

    from edgar_engine.filings import list_filings

    set_identity("LangAlpha EdgarEngine user@example.com")
    cik = find(LIVE_ORIGINAL).cik
    listed = list_filings(cik, "10-K", amendments=True, filed_on_or_before="2024-12-31")
    original = by_accession(listed)[LIVE_ORIGINAL]
    assert sorted(r.accession for r in listed if r.amends == original.accession) == LIVE_AMENDMENTS
    fs = get_sections(cik, "10-K", period_end=original.report_date, filed_on_or_before="2024-12-31",
                      view="as_amended")
    assert fs.amendments == LIVE_AMENDMENTS
    sources = {k: s.source for k, s in fs.items.items()}
    assert sources["7"] == sources["15"] == LIVE_ORIGINAL
    assert sources["10"] == LIVE_AMENDMENTS[1]
    assert sources["11"] == LIVE_AMENDMENTS[0]
