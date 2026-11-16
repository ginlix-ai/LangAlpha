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


def test_item_headings_with_numbers_are_not_an_index_without_a_title():
    lines = ["ITEM 16C. Principal Accountant Fees 77, 864", "Audit fees were paid.", "ITEM 16D. Exemptions 3, 4",
             "None.", "ITEM 16E. Purchases 5-6", "None.", "ITEM 16F. Change in accountant 7, 9", "None."]
    assert xref.find_index(lines, list(spec_for("20-F", "2016-04-01").order)) is None


def test_a_row_answering_an_earlier_page_is_only_that_page():
    rows = [xref.IndexRow(("7",), 1, [("10", "10")]), xref.IndexRow(("8",), 2, [("20", "20")]),
            xref.IndexRow(("9A",), 3, [("12", "12")]), xref.IndexRow(("15",), 4, [("40", "40")])]
    got = xref.item_pages(xref.Index(rows, (1, 5), literal=False))
    assert got == {"7": [("10", "19")], "8": [("20", "39")], "9A": [("12", "12")], "15": [("40", "40")]}
