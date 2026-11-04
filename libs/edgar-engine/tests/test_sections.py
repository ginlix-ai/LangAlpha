"""Settled behavior of the sections engine on small synthetic filings shaped like real ones."""
import pytest

from edgar_engine.sections import extract_sections


def submission(form: str, date: str, *docs: tuple[str, str]) -> str:
    """A full-text submission: SEC header plus one <DOCUMENT> per (type, body); .htm when the body is HTML."""
    head = (f"<SEC-HEADER>\nACCESSION NUMBER:\t\t0000000000-00-000001\nCONFORMED SUBMISSION TYPE:\t{form}\n"
            f"FILED AS OF DATE:\t\t{date.replace('-', '')}\n</SEC-HEADER>\n")
    parts = []
    for seq, (typ, body) in enumerate(docs, 1):
        name = f"doc{seq}.htm" if "<" in body.strip()[:1] else f"doc{seq}.txt"
        parts.append(f"<DOCUMENT>\n<TYPE>{typ}\n<SEQUENCE>{seq}\n<FILENAME>{name}\n<TEXT>\n{body}\n</TEXT>\n</DOCUMENT>")
    return head + "\n".join(parts)


def html(*blocks: str) -> str:
    return "<html><body>" + "".join(blocks) + "</body></html>"


def p(text: str) -> str:
    return f"<p>{text}</p>"


def row(*cells: str) -> str:
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


BODY = "The company designs and sells widgets to customers in many markets around the world every year."

TENK = html(
    p("TABLE OF CONTENTS"),
    "<table>", row("Item 1.", "Business", "3"), row("Item 1A.", "Risk Factors", "5"), row("Item 2.", "Properties", "9"),
    row("Item 7.", "Management's Discussion and Analysis", "12"), row("Item 8.", "Financial Statements", "20"), "</table>",
    p("PART I"), p("ITEM 1. BUSINESS"), p(BODY), p("ITEM 1A. RISK FACTORS"), p(BODY), p("12"),
    p("ITEM 2. PROPERTIES"), p("We lease our headquarters."), p("PART II"),
    p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY),
    "<table>", row("Revenue", "$", "1,234", "$", "987"), row("Net income", "(", "56", ")", "43"), "</table>", p("2024"),
    p("ITEM 8. FINANCIAL STATEMENTS"), p("See the statements below."), p(BODY),
    p("SIGNATURES"), p("Pursuant to the requirements of the Securities Exchange Act of 1934, the registrant has duly caused this report to be signed."),
)


def test_body_heading_outvotes_table_of_contents():
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", TENK)))
    assert fs["1"].text.startswith("ITEM 1. BUSINESS")
    assert fs["1A"].text.startswith("ITEM 1A. RISK FACTORS")
    assert "Risk Factors | 5" not in fs["1"].text


def test_part_heading_and_signatures_end_items():
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", TENK)))
    assert fs["2"].text.endswith("We lease our headquarters.")
    assert "SIGNATURES" not in fs["8"].text


def test_number_only_lines_and_table_cells_are_kept():
    item7 = extract_sections(submission("10-K", "2024-02-01", ("10-K", TENK)))["7"].text
    assert "Revenue | $ 1,234 | $ 987" in item7
    assert "Net income | (56) | 43" in item7
    assert item7.splitlines()[-1] == "2024"


def test_plain_text_filing():
    text = "\n".join([
        "                         PART I", "ITEM 1.  BUSINESS", BODY, "ITEM 2.  PROPERTIES", "The company owns a plant.",
        "                         PART II", "ITEM 5.  MARKET FOR REGISTRANT'S COMMON EQUITY", BODY, "<PAGE>",
        "ITEM 14. EXHIBITS, FINANCIAL STATEMENT SCHEDULES AND REPORTS ON FORM 8-K", "(a) Exhibits", "SIGNATURES"])
    fs = extract_sections(submission("10-K405", "1997-03-30", ("10-K405", text)))
    assert set(fs.items) == {"1", "2", "5", "14"}
    assert fs["2"].text.rstrip().endswith("The company owns a plant.")
    assert fs["14"].text.endswith("(a) Exhibits")


def test_sentence_that_mentions_an_item_is_not_a_heading():
    text = "\n".join(["ITEM 1. BUSINESS", "The discussion in", "Item 7 of this report covers results.",
                     "Items 3 and 4 are not applicable.", "ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS", BODY])
    fs = extract_sections(submission("10-K", "1999-03-30", ("10-K", text)))
    assert set(fs.items) == {"1", "7"}
    assert "Item 7 of this report" in fs["1"].text


def test_combined_heading_shares_one_span():
    text = "\n".join(["ITEMS 1 AND 2. BUSINESS AND PROPERTIES", BODY, "ITEM 3. LEGAL PROCEEDINGS", "None."])
    fs = extract_sections(submission("10-K", "1999-03-30", ("10-K", text)))
    assert fs["1"].text == fs["2"].text
    assert fs["1"].combined_with == ["2"]


def test_ten_q_items_are_keyed_by_part():
    doc = html(p("PART I. FINANCIAL INFORMATION"), p("Item 1. Financial Statements"), p(BODY),
               p("Item 2. Management's Discussion and Analysis"), p(BODY), p("PART II. OTHER INFORMATION"),
               p("Item 1. Legal Proceedings"), p("None."), p("Item 6. Exhibits"), p("31.1 Certification"), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2015-05-01", ("10-Q", doc)))
    assert set(fs.items) == {"I-1", "I-2", "II-1", "II-6"}
    assert fs["II-1"].text == "Item 1. Legal Proceedings\nNone."


def test_eight_k_numbering_follows_the_filing_date():
    new = extract_sections(submission("8-K", "2020-01-10", ("8-K", html(
        p("Item 2.02 Results of Operations and Financial Condition."), p(BODY),
        p("Item 9.01 Financial Statements and Exhibits."), p("(d) Exhibits"), p("SIGNATURE")))))
    old = extract_sections(submission("8-K", "2001-01-10", ("8-K", "\n".join([
        "Item 5. Other Events.", BODY, "Item 7. Financial Statements and Exhibits.", "(c) Exhibits", "SIGNATURES"]))))
    assert set(new.items) == {"2.02", "9.01"}
    assert set(old.items) == {"5", "7"}
    assert new["9.01"].text.endswith("(d) Exhibits")


def test_sentence_addressing_part_three_items_is_a_combined_proxy_stub():
    text = "\n".join(["ITEM 9. CHANGES IN AND DISAGREEMENTS WITH ACCOUNTANTS", "None.", "PART III",
                     "The information required by Items 10, 11, 12 and 13 will be contained in the registrant's",
                     "definitive proxy statement and is incorporated herein by reference.", "PART IV",
                     "ITEM 14. EXHIBITS", "(a) Exhibits"])
    fs = extract_sections(submission("10-K", "2002-03-30", ("10-K", text)))
    assert fs["9"].text == "ITEM 9. CHANGES IN AND DISAGREEMENTS WITH ACCOUNTANTS\nNone."
    for item in ("10", "11", "12", "13"):
        assert fs[item].status == "stub"
        assert fs[item].target.kind == "proxy"
        assert fs[item].target.spans == []
    assert fs["10"].combined_with == ["11", "12", "13"]


def test_pointer_to_a_note_resolves_inside_the_filing():
    doc = html(p("PART I. FINANCIAL INFORMATION"), p("Item 1. Financial Statements"), p(BODY),
               p("NOTES TO CONDENSED CONSOLIDATED FINANCIAL STATEMENTS"), p("Note 6. Debt"), p("We have a revolving credit facility."),
               p("Note 7. Commitments and Contingencies"), p("We are a defendant in a patent lawsuit filed in 2014."),
               p("Note 8. Subsequent Events"), p("None."),
               p("Item 2. Management's Discussion and Analysis"), p(BODY), p("PART II. OTHER INFORMATION"),
               p("Item 1. Legal Proceedings"), p("See Note 7, Commitments and Contingencies, of the notes to the financial statements."),
               p("Item 6. Exhibits"), p("31.1 Certification"), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2015-05-01", ("10-Q", doc)))
    legal = fs["II-1"]
    assert legal.status == "stub"
    assert legal.target.kind == "in_document"
    assert legal.target.text == "Note 7. Commitments and Contingencies\nWe are a defendant in a patent lawsuit filed in 2014."


def test_pointer_into_exhibit_13_resolves_by_caption_and_pages():
    main = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"),
                p("The information required by this item is set forth under the caption \u201cManagement's Discussion and "
                  "Analysis\u201d on pages 2 through 3 of the Annual Report to Shareholders and is incorporated herein by reference."),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("SIGNATURES"))
    ex13 = html(p("Letter to Shareholders"), p(BODY), p("1"), p("Management's Discussion and Analysis"), p(BODY), p("2"),
                p("Results of operations improved."), p("3"), p("Report of Independent Auditors"), p(BODY), p("4"))
    fs = extract_sections(submission("10-K", "1998-03-30", ("10-K", main), ("EX-13", ex13)))
    md = fs["7"]
    assert md.status == "stub"
    assert md.target.text.startswith("Management's Discussion and Analysis")
    assert md.target.text.endswith("Results of operations improved.\n3")
    assert "Report of Independent Auditors" not in md.target.text


def test_twenty_f_lettered_sub_items_without_the_word_item():
    doc = html(p("ITEM 15. CONTROLS AND PROCEDURES"), p(BODY), p("ITEM 16."), p("16.A. AUDIT COMMITTEE FINANCIAL EXPERT"),
               p("Our board has determined that one member is an expert."), p("16.B. CODE OF ETHICS"), p("We have adopted a code."),
               p("ITEM 17. FINANCIAL STATEMENTS"), p("Not applicable."), p("ITEM 18. FINANCIAL STATEMENTS"), p(BODY),
               p("ITEM 19. EXHIBITS"), p("1.1 Articles of association"), p("SIGNATURES"))
    fs = extract_sections(submission("20-F", "2021-03-17", ("20-F", doc)))
    assert fs["16A"].text.startswith("ITEM 16.\n16.A. AUDIT COMMITTEE FINANCIAL EXPERT")
    assert fs["16B"].text == "16.B. CODE OF ETHICS\nWe have adopted a code."
    assert fs["19"].text.endswith("1.1 Articles of association")


def test_twenty_f_sub_items_printed_as_letters_or_bare_titles():
    doc = html(p("ITEM 15. CONTROLS AND PROCEDURES"), p(BODY), p("Item 16 [Reserved]"),
               p("Audit Committee Financial Expert"), p("The board has determined that one member is an expert."),
               p("B. CODE OF ETHICS"), p("We have adopted a code."), p("Corporate Governance"), p("We follow home rules."),
               p("ITEM 17. FINANCIAL STATEMENTS"), p("Not applicable."), p("ITEM 18. FINANCIAL STATEMENTS"), p(BODY),
               p("ITEM 19. EXHIBITS"), p("1.1 Articles of association"), p("SIGNATURES"))
    fs = extract_sections(submission("20-F", "2013-04-30", ("20-F", doc)))
    assert fs["16"].text == "Item 16 [Reserved]"
    assert fs["16A"].text.startswith("Audit Committee Financial Expert")
    assert fs["16B"].text == "B. CODE OF ETHICS\nWe have adopted a code."
    assert fs["16G"].text == "Corporate Governance\nWe follow home rules."


def test_forty_f_unlisted_heading_ends_a_section_and_group_heading_splits():
    doc = html(p("DISCLOSURE CONTROLS AND PROCEDURES"), p("Our disclosure controls are effective."),
               p("MANAGEMENT'S ANNUAL REPORT ON INTERNAL CONTROL OVER FINANCIAL REPORTING"), p("Control is effective."),
               p("NOTICES PURSUANT TO REGULATION BTR"), p("None were sent during the year."),
               p("CODE OF ETHICS"), p("We have adopted a code of ethics."),
               p("UNDERTAKING AND CONSENT TO SERVICE OF PROCESS"), p("A. Undertaking"),
               p("The registrant undertakes to make its staff available."), p("B. Consent to Service of Process"),
               p("The registrant has filed a Form F-X."), p("SIGNATURES"))
    fs = extract_sections(submission("40-F", "2022-03-04", ("40-F", doc)))
    assert fs["DCP"].text.endswith("Control is effective.")
    assert fs["UNDERTAKING"].text == "A. Undertaking\nThe registrant undertakes to make its staff available."
    assert fs["CONSENT"].text.startswith("B. Consent to Service of Process")


def test_forty_f_fees_with_pre_approval_and_governance_by_content():
    doc = html(p("CODE OF ETHICS"), p("We have adopted a code of ethics."),
               p("PRE-APPROVAL POLICIES AND PROCEDURES"), p("The audit committee pre-approves all services."),
               p("PRINCIPAL ACCOUNTANT FEES AND SERVICES"), p("Fees billed by our auditor are shown below."),
               p("CORPORATE GOVERNANCE"),
               p("Our practices differ from the NYSE listing standards in the ways described in this section of the report."),
               p("UNDERTAKING"), p("The registrant undertakes to make its staff available."), p("SIGNATURES"))
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert fs["FEES"].text.startswith("PRE-APPROVAL POLICIES AND PROCEDURES")
    assert fs["FEES"].text.endswith("Fees billed by our auditor are shown below.")
    assert fs["NYSE_GOV"].text.startswith("CORPORATE GOVERNANCE\nOur practices differ")


def test_forty_f_sections_are_found_by_title():
    doc = html(p("ANNUAL INFORMATION FORM"), p("The annual information form is filed as Exhibit 99.1."),
               p("DISCLOSURE CONTROLS AND PROCEDURES"), p("Our disclosure controls are effective."),
               p("AUDIT COMMITTEE FINANCIAL EXPERT"), p("Our board has determined that one member is an expert."),
               p("CODE OF ETHICS"), p("We have adopted a code of ethics."),
               p("PRINCIPAL ACCOUNTANT FEES AND SERVICES"), p("Fees are disclosed in the AIF."),
               p("UNDERTAKING AND CONSENT TO SERVICE OF PROCESS"), p("The registrant undertakes to make its staff available."),
               p("SIGNATURES"))
    fs = extract_sections(submission("40-F", "2022-03-04", ("40-F", doc)))
    assert {"AIF", "DCP", "ACFE", "CODE", "FEES", "UNDERTAKING", "CONSENT"} <= set(fs.items)
    assert fs["CODE"].text == "CODE OF ETHICS\nWe have adopted a code of ethics."
    assert fs["UNDERTAKING"].combined_with == ["CONSENT"]


def test_truncated_submission_is_refused():
    full = submission("10-K", "2024-02-01", ("10-K", TENK), ("EX-21", "Subsidiaries of the registrant."))
    with pytest.raises(ValueError, match="truncated"):
        extract_sections(full[: full.rindex("</DOCUMENT>")])


def test_header_supplies_form_and_date_and_offsets_index_the_text():
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", TENK)))
    assert (fs.form, fs.filing_date, fs.accession) == ("10-K", "2024-02-01", "0000000000-00-000001")
    for s in fs.items.values():
        assert fs.text[s.start:s.end].rstrip("\n") == s.text


@pytest.mark.network
def test_real_annual_report():
    from edgar import Filing, set_identity

    set_identity("LangAlpha EdgarEngine user@example.com")
    filing = Filing(cik=320193, company="", form="10-K", filing_date="2023-11-03", accession_no="0000320193-23-000106")
    from edgar_engine.sections import sections_for_filing

    fs = sections_for_filing(filing)
    for item in ("1", "1A", "7", "7A", "8", "9A", "15"):
        assert fs[item].status == "present"
    assert len(fs["7"].text) > 10000



def test_an_addressing_sentence_with_a_dotted_number_names_only_the_forms_items():
    body = html(p("PART I"), p("ITEM 1. BUSINESS"), p(BODY), p("PART III"),
                p("The information required by Items 10 and 11.01 is incorporated by reference to the proxy statement."),
                p("ITEM 12. SECURITY OWNERSHIP"), p(BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", body)))
    assert fs["10"].status == "stub" and "11.01" not in fs.items and "11" not in fs.items
