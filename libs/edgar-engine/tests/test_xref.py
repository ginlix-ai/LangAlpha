"""Cross-reference indexes: items mapped to printed pages instead of headed in the body."""
from edgar_engine.sections import extract_sections, xref
from edgar_engine.sections.vocab import spec_for

from test_sections import BODY, html, p, row, submission


def pages(*bodies: str) -> list[str]:
    """Printed pages 1..n, each its text then its page number alone, as the footer."""
    return [p(b) + p(str(k)) for k, b in enumerate(bodies, 1)]


GUIDE = html(
    p("ANNUAL REPORT ON FORM 20-F"), p("SEC Form 20-F cross reference guide"),
    "<table>", row("Item", "Item Caption", "Pages"),
    row("1", "Identity of Directors, Senior Management and Advisers", "Not applicable"),
    row("3", "Key Information", "2, 5"), row("4", "Information on the Company", "3-4"),
    row("5", "Operating and Financial Review and Prospects", "4"), row("18", "Financial Statements", "6"), "</table>",
    *pages("Chairman's letter. " + BODY, "Selected financial data. " + BODY, "Business overview. " + BODY,
           "Operating results. " + BODY, "Risk factors. " + BODY, "Consolidated income statement. " + BODY),
)


def test_literal_guide_maps_each_item_to_its_pages_in_document_order():
    fs = extract_sections(submission("20-F", "2013-03-28", ("20-F", GUIDE)))
    key = fs["3"]
    assert key.status == "xref" and len(key.target.spans) == 2
    assert key.text.startswith("Selected financial data") and "Risk factors" in key.text
    assert "Business overview" not in key.text
    assert fs["4"].text.startswith("Business overview") and "Operating results" in fs["4"].text
    assert fs["18"].text.startswith("Consolidated income statement")
    assert "1" not in fs.items and "2" not in fs.items


INDEX = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
    "<table>", row("PART I", "", "PAGE"), row("ITEM 1", "Business", "1"),
    row("ITEM 7", "Management's Discussion and Analysis", "2"), row("ITEM 8", "Financial Statements and Supplementary Data", ""),
    row("", "Consolidated Balance Sheets", "3"), row("", "Notes to Consolidated Financial Statements", "5"),
    row("ITEM 9", "Changes in and Disagreements With Accountants", "None"), row("ITEM 9A", "Controls and Procedures", "4"),
    row("ITEM 10", "Directors and Executive Officers", "*"), row("ITEM 15", "Exhibits", "6"), "</table>",
    *pages("ITEM 1. BUSINESS</p><p>" + BODY, "Results of operations. " + BODY, "Balance sheet. " + BODY,
           "ITEM 9A. CONTROLS AND PROCEDURES</p><p>Our disclosure controls are effective.",
           "Notes to the statements. " + BODY, "ITEM 15. EXHIBITS</p><p>The exhibits are listed below."),
)


def test_contents_index_runs_items_to_the_next_row_and_cuts_out_headed_items():
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", INDEX)))
    assert fs["1"].status == "present" and "Results of operations" not in fs["1"].text
    assert fs["7"].status == "xref" and fs["7"].text.startswith("Results of operations")
    eight = fs["8"]
    assert eight.status == "xref" and len(eight.target.spans) == 2
    assert eight.text.startswith("Balance sheet") and "Notes to the statements" in eight.text
    assert "disclosure controls" not in eight.text
    assert fs["9A"].status == "present" and "Notes to the statements" not in fs["9A"].text
    assert "9" not in fs.items and "10" not in fs.items


def test_an_item_the_index_cuts_down_to_a_pointer_is_a_stub():
    doc = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "PAGE"), row("ITEM 1", "Business", "1"),
        row("ITEM 7", "Management's Discussion and Analysis", "2"), row("ITEM 8", "Financial Statements and Supplementary Data", "3"),
        row("ITEM 9A", "Controls and Procedures", "4"), "</table>",
        *pages("ITEM 1. BUSINESS</p><p>" + BODY, "ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</p><p>See Financial Review on page 3.",
               "Financial Review. " + " ".join([BODY] * 30), "ITEM 9A. CONTROLS AND PROCEDURES</p><p>Our disclosure controls are effective."))
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", doc)))
    assert fs["7"].status == "stub" and fs["8"].status == "xref"


def test_item_headings_with_numbers_are_not_an_index_without_a_title():
    lines = ["ITEM 16C. Principal Accountant Fees 77, 864", "Audit fees were paid.", "ITEM 16D. Exemptions 3, 4",
             "None.", "ITEM 16E. Purchases 5-6", "None.", "ITEM 16F. Change in accountant 7, 9", "None."]
    assert xref.find_index(lines, list(spec_for("20-F", "2016-04-01").order)) is None


def test_a_row_answering_an_earlier_page_is_only_that_page():
    rows = [xref.IndexRow(("7",), 1, [("10", "10")]), xref.IndexRow(("8",), 2, [("20", "20")]),
            xref.IndexRow(("9A",), 3, [("12", "12")]), xref.IndexRow(("15",), 4, [("40", "40")])]
    got = xref.item_pages(xref.Index(rows, (1, 5), literal=False))
    assert got == {"7": [("10", "19")], "8": [("20", "39")], "9A": [("12", "12")], "15": [("40", "40")]}


def test_an_item_citing_ordinary_and_f_pages_keeps_both():
    rows = [xref.IndexRow(("7",), 1, [("10", "10")]), xref.IndexRow(("8",), 2, [("20", "20"), ("F-1", "F-3")]),
            xref.IndexRow(("9A",), 3, [("30", "30")]), xref.IndexRow(("15",), 4, [("40", "40")])]
    got = xref.item_pages(xref.Index(rows, (1, 5), literal=False))
    assert got["8"] == [("20", "29"), ("F-1", "F-3")]


ANNUAL_ROW = html(
    p("TABLE OF CONTENTS"), "<table>", row("Item 1.", "Business", "1"),
    row("Item 5.", "Market for Registrant's Common Equity, in the Annual Report to Shareholders", "2"),
    row("Item 6.", "Selected Financial Data", "4"), row("Item 7.", "Management's Discussion and Analysis", "5"), "</table>",
    p("PART I"), p("ITEM 1. BUSINESS"), p(BODY), p("1"), p("Stock prices. " + BODY), p("2"), p("Dividends. " + BODY), p("3"),
    p("ITEM 6. SELECTED FINANCIAL DATA"), p(BODY), p("4"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY), p("5"),
    p("SIGNATURES"),
)


def test_a_contents_row_pointing_outside_the_filing_maps_no_local_pages():
    fs = extract_sections(submission("10-K", "1999-03-30", ("10-K", ANNUAL_ROW)))
    assert "5" not in fs.items
    assert fs["6"].status == "present"


def test_an_index_row_citing_the_annual_report_maps_no_local_pages_without_exhibit_13():
    doc = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "PAGE"), row("ITEM 1", "Business", "1"),
        row("ITEM 5", "Market for Registrant's Common Equity", "Annual Report to Shareholders 2"),
        row("ITEM 7", "Management's Discussion and Analysis", "3"),
        row("ITEM 9A", "Controls and Procedures", "4"), "</table>",
        *pages("ITEM 1. BUSINESS</p><p>" + BODY, "Properties. " + BODY, "Results of operations. " + BODY,
               "ITEM 9A. CONTROLS AND PROCEDURES</p><p>Our disclosure controls are effective."))
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", doc)))
    assert fs["7"].status == "xref"
    assert "5" not in fs.items or not fs["5"].target.spans


def test_annual_report_rows_read_exhibit_13_when_both_documents_carry_the_pages():
    main = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "PAGE"), row("ITEM 1", "Business", "Annual Report to Shareholders 1"),
        row("ITEM 5", "Market for Registrant's Common Equity", "Annual Report to Shareholders 2"),
        row("ITEM 7", "Management's Discussion and Analysis", "Annual Report to Shareholders 3"),
        row("ITEM 8", "Financial Statements", "Annual Report to Shareholders 4"), "</table>",
        *pages("Cover. " + BODY, "Signatures. " + BODY, "Exhibit list. " + BODY, "Certifications. " + BODY))
    ex13 = html(*pages("Our business. " + BODY, "Stock prices. " + BODY, "Results of operations. " + BODY, "Balance sheet. " + BODY))
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", main), ("EX-13", ex13)))
    assert fs["7"].status == "xref" and fs["7"].text.startswith("Results of operations")


def test_each_index_row_reads_the_document_its_location_names():
    main = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "PAGE"), row("ITEM 1", "Business", "1"), row("ITEM 2", "Properties", "2"),
        row("ITEM 3", "Legal Proceedings", "3"),
        row("ITEM 7", "Management's Discussion and Analysis", "Annual Report to Shareholders 3"), "</table>",
        *pages("Our business. " + BODY, "Properties we own. " + BODY, "Legal matters. " + BODY, "Exhibit list. " + BODY))
    ex13 = html(*pages("Letter to shareholders. " + BODY, "Stock prices. " + BODY, "Results of operations. " + BODY,
                       "Balance sheet. " + BODY))
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", main), ("EX-13", ex13)))
    assert "Our business" in fs["1"].text and "Letter to shareholders" not in fs["1"].text
    assert fs["2"].text.startswith("Properties we own")
    assert fs["7"].status == "xref" and fs["7"].text.startswith("Results of operations")


def test_a_proxy_named_in_its_own_column_maps_no_local_pages():
    doc = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "", "PAGE"), row("ITEM 1", "Business", "", "1"),
        row("ITEM 7", "Management's Discussion and Analysis", "", "2"),
        row("ITEM 9A", "Controls and Procedures", "", "4"),
        row("ITEM 10", "Directors and Executive Officers", "Proxy Statement", "2-3"), "</table>",
        *pages("ITEM 1. BUSINESS</p><p>" + BODY, "Results of operations. " + BODY, "Liquidity. " + BODY,
               "ITEM 9A. CONTROLS AND PROCEDURES</p><p>Our disclosure controls are effective."))
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", doc)))
    assert fs["7"].status == "xref"
    assert "10" not in fs.items or fs["10"].status != "xref"


LOCATED = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("FORM 10-K CROSS REFERENCE INDEX"),
    "<table>", row("Item of Form 10-K", "", "Location"),
    row("Item 1.", "Business", "\u201cOur Business\u201d on pages 1 and 2."),
    row("Item 2.", "Properties", "\u201cProperties\u201d on page 3."),
    row("Item 4.", "Submission of Matters to a Vote of Security Holders", "Not applicable."),
    row("Item 7.", "Management's Discussion and Analysis", "Pages 4 through 5."),
    row("Item 10.", "Directors and Executive Officers", "Information is included in the Proxy Statement on pages 6 through 9."),
    row("Item 16.", "Form 10-K Summary", "Not applicable"), row("Signatures", "", "6"), "</table>",
    *pages("Our business. " + BODY, "More business. " + BODY, "Properties we own. " + BODY,
           "Results of operations. " + BODY, "Liquidity. " + BODY, "Signatures follow."),
)


def test_location_sentences_map_pages_and_answer_only_rows_leave_the_item_absent():
    fs = extract_sections(submission("10-K", "2008-03-01", ("10-K", LOCATED)))
    assert fs["1"].status == "xref" and "Our business" in fs["1"].text and "More business" in fs["1"].text
    assert "Properties we own" not in fs["1"].text
    assert fs["2"].text.startswith("Properties we own") and "Results of operations" not in fs["2"].text
    assert fs["7"].text.startswith("Results of operations") and "Liquidity" in fs["7"].text
    assert not {"4", "10", "16"} & set(fs.items)


FOOTNOTED = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
    "<table>", row("Item 1", "Business", "1"), row("Item 2", "Properties", "2"), row("Item 7", "Management's Discussion", "3"),
    row("Item 8", "Financial Statements", "4"), row("Item 10", "Directors and Executive Officers", "*"),
    row("Item 11", "Executive Compensation", "*"), "</table>",
    p("* Information called for by Part III is incorporated by reference to the Proxy Statement."),
    p("Item 10\u2014See information that appears under the heading \u201cElection of Directors\u201d."),
    p("Item 11\u2014See information that appears under the heading \u201cExecutive Compensation\u201d."),
    p("TABLE OF CONTENTS"),
    *pages("Our business. " + BODY, "Properties we own. " + BODY, "Results of operations. " + BODY, "Balance sheet. " + BODY),
)


def test_items_the_index_footnotes_send_to_the_proxy_are_absent():
    fs = extract_sections(submission("10-K", "2005-03-01", ("10-K", FOOTNOTED)))
    assert fs["7"].status == "xref" and fs["7"].text.startswith("Results of operations")
    assert "10" not in fs.items and "11" not in fs.items


def statement_pages(*bodies: str) -> list[str]:
    """Printed financial-statement pages F-1..F-n."""
    return [p(b) + p(f"F-{k}") for k, b in enumerate(bodies, 1)]


def test_financial_statement_page_ranges_are_read_whole():
    for cited in ("F-1 - F-3", "F-1-F-3", "F-1-3"):
        guide = html(
            p("ANNUAL REPORT ON FORM 20-F"), p("SEC Form 20-F cross reference guide"),
            "<table>", row("Item", "Item Caption", "Pages"), row("3", "Key Information", "1"),
            row("4", "Information on the Company", "2"), row("5", "Operating and Financial Review", "3"),
            row("18", "Financial Statements", cited), "</table>",
            *pages("Selected data. " + BODY, "Business overview. " + BODY, "Operating results. " + BODY),
            *statement_pages("Consolidated income statement. " + BODY, "Balance sheet. " + BODY, "Notes. " + BODY),
        )
        fs = extract_sections(submission("20-F", "2013-03-28", ("20-F", guide)))
        assert fs["18"].status == "xref" and fs["18"].text.startswith("Consolidated income statement"), cited
        assert "Notes." in fs["18"].text


POINTED = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("FORM 10-K CROSS REFERENCE INDEX"),
    "<table>", row("Item of Form 10-K", "", "Location"),
    row("Item 1.", "Business", "\u201cOur Business\u201d on pages 1 and 2."), row("Item 2.", "Properties", "\u201cProperties\u201d on page 3."),
    row("Item 7.", "Management's Discussion and Analysis", "Pages 4 through 5."),
    row("Item 10.", "Directors and Executive Officers", "Proxy Statement pages 2-3"),
    row("Item 15.", "Exhibits", "See the exhibit list that follows the signatures."), "</table>",
    *pages("Our business. " + BODY, "More business. " + BODY, "Properties we own. " + BODY,
           "Results of operations. " + BODY, "Liquidity. " + BODY),
)


def test_proxy_pages_give_no_pages_and_a_pointer_row_is_not_a_heading():
    fs = extract_sections(submission("10-K", "2008-03-01", ("10-K", POINTED)))
    assert fs["7"].text.startswith("Results of operations")
    assert "10" not in fs.items and "15" not in fs.items


def test_a_contents_row_keeps_its_own_range_and_statement_pages_run_to_the_last_one():
    rows = [xref.IndexRow(("7",), 1, [("10", "10")]), xref.IndexRow(("8",), 2, [("20", "25")]),
            xref.IndexRow(("9A",), 3, [("30", "30")]), xref.IndexRow(("15",), 4, [("F-1", "F-1")])]
    got = xref.item_pages(xref.Index(rows, (1, 5), literal=False))
    assert got == {"7": [("10", "19")], "8": [("20", "25")], "9A": [("30", "30")], "15": [("F-1", xref.LAST_F)]}


WRAPPED = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
    "<table>", row("ITEM 1", "Business", "1"), row("ITEM 2", "Properties", "2"), row("ITEM 7", "Management's Discussion", "3"),
    row("ITEM 8", "Financial Statements", "4"), row("ITEM 10", "Directors and Executive Officers", ""),
    row("", "Proxy statement 4", ""), "</table>",
    *pages("Our business. " + BODY, "Properties we own. " + BODY, "Results of operations. " + BODY, "Balance sheet. " + BODY),
)


def test_a_wrapped_row_naming_the_proxy_gives_no_filing_pages():
    fs = extract_sections(submission("10-K", "2005-03-01", ("10-K", WRAPPED)))
    assert fs["8"].text.startswith("Balance sheet")
    assert "10" not in fs.items or "Balance sheet" not in fs["10"].text


RETARGET = html(
    p("ANNUAL REPORT ON FORM 10-K"), p("CROSS REFERENCE INDEX"),
    "<table>", row("ITEM 1", "Business", "1"), row("ITEM 7", "Management's Discussion", "2"),
    row("ITEM 8", "Financial Statements", "3"), row("ITEM 9A", "Controls and Procedures", "4"), row("ITEM 15", "Exhibits", "5"),
    "</table>", *[p(f"Forward-looking statement {k}. " + BODY) for k in range(5)],
    *pages("ITEM 1. BUSINESS</p><p>" + BODY + "</p><p>ITEM 2. PROPERTIES</p><p>See Item 7.",
           "ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</p><p>Results of operations. " + BODY,
           "Balance sheet. " + BODY, "ITEM 9A. CONTROLS AND PROCEDURES</p><p>Our disclosure controls are effective.",
           "ITEM 15. EXHIBITS</p><p>The exhibits are listed below."),
)


def test_a_stub_target_follows_the_item_as_the_index_cuts_it():
    fs = extract_sections(submission("10-K", "2006-03-01", ("10-K", RETARGET)))
    assert fs["8"].status == "xref" and fs["8"].text.startswith("Balance sheet")
    assert "Balance sheet" not in fs["7"].text
    assert fs["2"].status == "stub" and "Results of operations" in fs["2"].target.text
    assert "Balance sheet" not in fs["2"].target.text

def test_pages_numbered_before_a_running_text_that_opens_with_a_year_are_found():
    footer = ("2025 ANNUAL REPORT {}", "{} 2025 ANNUAL REPORT")
    doc = html(
        p("ANNUAL REPORT ON FORM 10-K"), p("FORM 10-K CROSS REFERENCE INDEX"),
        "<table>", row("PART I", "", "Page(s)"), row("Item 1.", "Business", "2-3, 5"),
        row("Item 1A.", "Risk Factors", "4"), row("Item 7.", "Management's Discussion and Analysis", "3-4"),
        row("Item 8.", "Financial Statements and Supplementary Data", "6"), "</table>",
        *(p(b) + p(footer[k % 2].format(k)) for k, b in enumerate(
            ["Contents. " + BODY, "About the company. " + BODY, "Segments. " + BODY, "Risks. " + BODY,
             "Legal matters. " + BODY, "Consolidated balance sheet. " + BODY], 1)))
    fs = extract_sections(submission("10-K", "2026-01-29", ("10-K", doc)))
    business = fs["1"]
    assert business.status == "xref"
    assert business.text.startswith("About the company") and "Legal matters" in business.text
    assert fs["1A"].status == "xref" and fs["1A"].text.startswith("Risks")
