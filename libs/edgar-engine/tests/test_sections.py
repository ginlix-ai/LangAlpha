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


def test_a_pointer_to_pages_of_another_exhibit_does_not_resolve_to_this_documents_pages():
    main = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"),
                p("The information required by this item is included on pages 2 through 3 of Exhibit 99.1 hereto and is "
                  "incorporated herein by reference."), p("1"),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("2"), p("Balance sheet. " + BODY), p("3"),
                p("Notes. " + BODY), p("4"), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2004-03-30", ("10-K", main)))
    md = fs["7"]
    assert md.status == "stub" and md.target.kind == "exhibit" and not md.target.spans


def test_a_pointer_to_a_note_in_another_exhibit_stays_external():
    main = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"),
                p("See Note 7 in Exhibit 99.1 hereto, which is incorporated herein by reference."),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("Note 7. Commitments and Contingencies"),
                p("We lease our offices."), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2004-03-30", ("10-K", main)))
    md = fs["7"]
    assert md.status == "stub" and md.target.kind == "exhibit" and not md.target.spans


def test_an_item_numeral_outside_the_forms_numbering_is_not_a_heading():
    doc = html(p("PART I"), p("ITEM 1. BUSINESS"), p(BODY), p("ITEM XXI. OTHER MATTERS"), p(BODY),
               p("ITEM 2. PROPERTIES"), p(BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2004-03-30", ("10-K", doc)))
    assert "2" in fs.items and "21" not in fs.items


def test_a_pointer_to_a_note_keeps_its_numbered_subsections():
    doc = html(p("PART I. FINANCIAL INFORMATION"), p("Item 1. Financial Statements"), p(BODY),
               p("NOTES TO CONDENSED CONSOLIDATED FINANCIAL STATEMENTS"), p("Note 6. Debt"), p("We have a revolving credit facility."),
               p("Note 7. Commitments and Contingencies"), p("We are a defendant in a patent lawsuit filed in 2014."),
               p("1. Significant Estimates"), p("We accrue for losses we can estimate."),
               p("Note 8. Subsequent Events"), p("None."),
               p("Item 2. Management's Discussion and Analysis"), p(BODY), p("PART II. OTHER INFORMATION"),
               p("Item 1. Legal Proceedings"), p("See Note 7, Commitments and Contingencies, of the notes to the financial statements."),
               p("Item 6. Exhibits"), p("31.1 Certification"), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2015-05-01", ("10-Q", doc)))
    assert fs["II-1"].target.text.endswith("1. Significant Estimates\nWe accrue for losses we can estimate.")


def test_a_ten_q_item_pointer_without_a_part_stays_in_its_own_part():
    doc = html(p("PART I. FINANCIAL INFORMATION"), p("Item 1. Financial Statements"), p(BODY),
               p("Item 2. Management's Discussion and Analysis"), p(BODY), p("PART II. OTHER INFORMATION"),
               p("Item 1. Legal Proceedings"), p("We are a defendant in a patent lawsuit filed in 2014."),
               p("Item 1A. Risk Factors"), p("See Item 1 for the lawsuit that could affect our results."),
               p("Item 6. Exhibits"), p("31.1 Certification"), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2015-05-01", ("10-Q", doc)))
    assert fs["II-1A"].status == "stub" and "patent lawsuit" in fs["II-1A"].target.text


def test_a_ten_q_heading_keeps_its_inline_part():
    doc = html(p("FORM 10-Q/A"), p("EXPLANATORY NOTE"), p("This amendment restates the legal proceedings disclosure."),
               p("PART II, ITEM 1. LEGAL PROCEEDINGS"), p("We are a defendant in a patent lawsuit filed in 2014."), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q/A", "2015-06-01", ("10-Q/A", doc)))
    assert list(fs.items) == ["II-1"]


def test_a_pointer_to_pages_resolves_only_when_every_page_is_found():
    main = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"),
                p("The information required by this item appears on pages 2 and 4 of the Annual Report to Shareholders "
                  "and is incorporated herein by reference."),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("SIGNATURES"))
    ex13 = html(p("Letter to Shareholders. " + BODY), p("1"), p("Results of operations. " + BODY), p("2"),
                p("Report of Independent Auditors. " + BODY), p("3"))
    fs = extract_sections(submission("10-K", "1998-03-30", ("10-K", main), ("EX-13", ex13)))
    assert fs["7"].status == "stub" and not fs["7"].target.spans


def test_a_ten_q_item_pointer_honors_a_part_named_after_the_item():
    doc = html(p("PART I. FINANCIAL INFORMATION"), p("Item 1. Financial Statements"), p(BODY),
               p("Item 2. Management's Discussion and Analysis"), p("Liquidity improved this quarter. " + BODY),
               p("PART II. OTHER INFORMATION"), p("Item 1. Legal Proceedings"), p("None."),
               p("Item 1A. Risk Factors"), p("See Item 2 of Part I of this report for the risks that could affect our results."),
               p("Item 2. Unregistered Sales of Equity Securities"), p("None."),
               p("Item 6. Exhibits"), p("31.1 Certification"), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2015-05-01", ("10-Q", doc)))
    assert fs["II-1A"].status == "stub" and "Liquidity improved" in fs["II-1A"].target.text


def test_a_page_numbered_in_its_header_and_footer_ends_at_the_footer():
    main = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"),
                p("The information required by this item appears on page 2 of the Annual Report to Shareholders "
                  "and is incorporated herein by reference."),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("SIGNATURES"))
    ex13 = html(p("ACME 1"), p("Letter to Shareholders. " + BODY), p("1"),
                p("ACME 2"), p("Management's Discussion and Analysis. " + BODY), p("2"),
                p("ACME 3"), p("Report of Independent Auditors. " + BODY), p("3"))
    fs = extract_sections(submission("10-K", "1998-03-30", ("10-K", main), ("EX-13", ex13)))
    text = fs["7"].target.text
    assert "Management's Discussion and Analysis" in text and "Letter to Shareholders" not in text


def test_a_statements_pointer_naming_a_note_resolves_to_the_note():
    doc = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY),
               p("ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA"),
               p("Quarterly results are presented in Note 7 to the consolidated financial statements."),
               p("ITEM 9A. CONTROLS AND PROCEDURES"), p("Our disclosure controls are effective."), p("SIGNATURES"),
               p("INDEX TO CONSOLIDATED FINANCIAL STATEMENTS"), p("Consolidated Balance Sheets"), p(BODY),
               p("Note 6. Debt"), p("We have a revolving credit facility."),
               p("Note 7. Quarterly Results"), p("Fourth-quarter sales rose."), p("Note 8. Subsequent Events"), p("None."))
    fs = extract_sections(submission("10-K", "2015-03-01", ("10-K", doc)))
    assert fs["8"].status == "stub"
    assert fs["8"].target.text == "Note 7. Quarterly Results\nFourth-quarter sales rose."


def test_an_abbreviated_f_page_range_keeps_its_prefix():
    main = html(p("PART II"), p("ITEM 7A. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET RISK"),
                p("The information required by this item appears on pages F-1-3 of this report."), p("1"),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("2"), p("Selected data. " + BODY), p("3"),
                p("Interest rate risk. " + BODY), p("F-1"), p("Currency risk. " + BODY), p("F-2"),
                p("Commodity risk. " + BODY), p("F-3"), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2004-03-30", ("10-K", main)))
    assert fs["7A"].target.text.startswith("Interest rate risk") and "Commodity risk" in fs["7A"].target.text


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


FORTY_F_WRAPPER = [
    p("DISCLOSURE CONTROLS AND PROCEDURES"), p("Our disclosure controls are effective."),
    p("AUDIT COMMITTEE FINANCIAL EXPERT"), p("Our board has determined that one member is an expert."),
    p("CODE OF ETHICS"), p("We have adopted a code of ethics."),
    p("UNDERTAKING AND CONSENT TO SERVICE OF PROCESS"), p("The registrant undertakes to make its staff available."),
    p("SIGNATURES"), p("Pursuant to the requirements of the Exchange Act, the registrant has duly caused this report to be signed.")]


def test_forty_f_keeps_listed_documents_beside_a_printed_one():
    doc = html(p("PRINCIPAL DOCUMENTS"), p("The following documents are filed as part of this annual report:"),
               p("A. Annual Information Form for the year ended December 31, 2023"),
               p("B. Management's Discussion and Analysis for the year ended December 31, 2023"),
               p("C. Audited Annual Financial Statements for the year ended December 31, 2023"), *FORTY_F_WRAPPER,
               p("DOCUMENT 3"), p("AUDITED ANNUAL FINANCIAL STATEMENTS"), p("For the years ended December 31, 2023 and 2022"),
               p("Consolidated balance sheet. " + BODY))
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert fs["AFS"].text.startswith("AUDITED ANNUAL FINANCIAL STATEMENTS")
    assert fs["AIF"].text.startswith("A. Annual Information Form") and fs["MDA"].text.startswith("B. Management's")


def test_forty_f_lists_every_document_an_entry_names():
    doc = html(p("PRINCIPAL DOCUMENTS"), p("The following documents are filed as part of this annual report:"),
               p("A. Annual Information Form for the year ended December 31, 2023"),
               p("B. Audited Annual Financial Statements and Management's Discussion and Analysis for the year ended "
                 "December 31, 2023"), *FORTY_F_WRAPPER)
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert {"AIF", "AFS", "MDA"} <= set(fs.items)
    assert fs["MDA"].text == fs["AFS"].text and fs["AFS"].text.startswith("B. Audited")


def test_forty_f_ending_on_a_document_label_still_parses():
    doc = html(p("ANNUAL INFORMATION FORM"), p("The annual information form is filed as Exhibit 99.1."), *FORTY_F_WRAPPER, p("DOCUMENT 1"))
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert "CODE" in fs.items


def test_forty_f_finds_an_unlabeled_aif_before_a_labeled_document():
    doc = html(p("ANNUAL INFORMATION FORM"), p("For the year ended December 31, 2023"),
               *[p("Our business. " + BODY) for _ in range(40)], *FORTY_F_WRAPPER,
               p("DOCUMENT 2"), p("MANAGEMENT'S DISCUSSION AND ANALYSIS"), p("For the year ended December 31, 2023"), p("Results. " + BODY))
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert fs["AIF"].text.startswith("ANNUAL INFORMATION FORM") and fs["MDA"].text.startswith("MANAGEMENT'S DISCUSSION")


def test_a_letter_spaced_part_or_item_keyword_closes_up_before_its_numeral():
    from edgar_engine.sections.text import normalize
    assert [normalize(s) for s in ("P A R T I", "P A R T IV", "P A R T I I", "I T E M S 1 and 2", "I T E M I Z E D costs")] == [
        "PART I", "PART IV", "PART II", "ITEMS 1 and 2", "I T E M I Z E D costs"]


def test_truncated_submission_is_refused():
    full = submission("10-K", "2024-02-01", ("10-K", TENK), ("EX-21", "Subsidiaries of the registrant."))
    with pytest.raises(ValueError, match="truncated"):
        extract_sections(full[: full.rindex("</DOCUMENT>")])


def test_a_submission_cut_between_documents_or_holding_none_is_refused():
    full = "<SEC-DOCUMENT>0000000000-00-000001.txt\n" + submission(
        "10-K", "2024-02-01", ("10-K", TENK), ("EX-13", "Annual report.")) + "\n</SEC-DOCUMENT>\n"
    assert extract_sections(full)["1"].status == "present"
    with pytest.raises(ValueError, match="truncated"):
        extract_sections(full[: full.rindex("<DOCUMENT>")])
    with pytest.raises(ValueError, match="truncated"):
        extract_sections(b"")


def test_a_submission_naming_no_form_reads_its_first_text_document():
    fs = extract_sections(submission("10-K", "2004-03-01", ("GRAPHIC", "begin 644 logo.gif\nM1234\nend"), ("EX-99", TENK)))
    assert fs.items and not fs.text.startswith("begin 644")


def test_an_unsupported_form_family_is_refused():
    from edgar_engine.sections.vocab import form_family
    assert [form_family(f) for f in ("10-K405/A", "10-KT", "8-K12B", "10-Q/A")] == ["10-K", "10-K", "8-K", "10-Q"]
    with pytest.raises(ValueError):
        form_family("10-KSB")


def test_a_filing_too_deep_to_parse_reads_as_text_with_its_entities_and_paragraphs():
    from edgar_engine.sections.rendering import html_lines
    body = "<html><body>" + "<div>" * 300 + "<p>Item&nbsp;1.&nbsp;Business<p>We make widgets &amp; gadgets.</body></html>"
    lines = [ln.strip() for ln in html_lines(body)]
    assert "Item 1. Business" in lines and "We make widgets & gadgets." in lines


def test_an_amendment_reads_its_own_document_before_a_copy_typed_as_the_original():
    copy = html(p("PART I"), p("ITEM 1. BUSINESS"), p("The original text. " + BODY), p("SIGNATURES"))
    amended = html(p("PART III"), p("ITEM 10. DIRECTORS"), p("The amended text. " + BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-K/A", "2024-04-29", ("10-K", copy), ("10-K/A", amended)))
    assert list(fs.items) == ["10"] and "The amended text" in fs["10"].text


def test_paragraphs_that_never_close_their_font_tags_are_read_to_the_end():
    paras = "".join(f"<P><FONT SIZE=2>{BODY} {i}" for i in range(400))
    body = f"<html><body><P><FONT SIZE=2>ITEM 1. BUSINESS{paras}<P><FONT SIZE=2>ITEM 7. MANAGEMENT'S DISCUSSION{paras}</body></html>"
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", body)))
    assert fs["7"].status == "present" and fs["7"].text.endswith(f"{BODY} 399")


def test_a_compact_filing_date_reads_as_iso_and_a_malformed_one_is_refused():
    raw = submission("10-K", "2024-02-01", ("10-K", TENK))
    assert extract_sections(raw, filing_date="20240201").filing_date == "2024-02-01"
    with pytest.raises(ValueError, match="filing_date"):
        extract_sections(raw, filing_date="02/01/2024")


def test_the_main_document_stays_first_when_an_exhibit_has_a_lower_sequence():
    from edgar_engine.sections.documents import main_documents, split
    sub = submission("10-K", "2005-03-01", ("EX-13", html(p("Annual report."))), ("10-K", TENK))
    assert [d.type for d in main_documents(split(sub), "10-K")] == ["10-K", "EX-13"]


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


def test_a_note_pointer_resolves_to_the_note_heading_not_a_sentence_naming_the_note():
    body = html(p("PART II"), p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY),
                p("Note 7 to the consolidated financial statements provides more information about our borrowings."),
                p("ITEM 7A. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET RISK"),
                p("See Note 7, Debt, to the consolidated financial statements."),
                p("ITEM 8. FINANCIAL STATEMENTS"), p(BODY), p("NOTE 6 - LEASES"), p(BODY), p("NOTE 7 - DEBT"),
                p("We borrowed under a revolving credit facility."), p("NOTE 8 - TAXES"), p(BODY),
                p("ITEM 9. CHANGES IN AND DISAGREEMENTS"), p("None."), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2015-02-01", ("10-K", body)))
    assert fs["7A"].status == "stub" and fs["7A"].target.text.startswith("NOTE 7 - DEBT")


def test_pages_joined_by_and_are_those_pages_not_a_range():
    annual = html(*[p(b) + p(str(k)) for k, b in enumerate(
        ["Letter to shareholders. " + BODY, "Market data. " + BODY, "Selected data. " + BODY, "Dividends. " + BODY], 1)])
    body = html(p("PART II"), p("ITEM 5. MARKET FOR REGISTRANT'S COMMON EQUITY"),
                p("The information required by this item appears on pages 2 and 4 of the Annual Report to Shareholders "
                  "and is incorporated herein by reference."), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2005-03-01", ("10-K", body), ("EX-13", annual)))
    target = fs["5"].target.text
    assert "Market data" in target and "Dividends" in target and "Selected data" not in target


def test_a_quarterly_amendment_restating_only_item_2_has_no_item_1():
    body = html(p("FORM 10-Q/A"), p("For the quarterly period ended March 31, 2024"), p("EXPLANATORY NOTE"),
                p("This Amendment No. 1 amends Item 2 of our Quarterly Report on Form 10-Q originally filed on May 2, 2024."),
                p("PART I"), p("ITEM 2. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY), p("SIGNATURES"))
    assert set(extract_sections(submission("10-Q/A", "2024-06-01", ("10-Q/A", body))).items) == {"I-2"}
    original = html(p("PART I"), p("CONDENSED CONSOLIDATED BALANCE SHEETS"), p("Total assets 1,234"),
                    p("ITEM 2. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p(BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-Q", "2024-05-01", ("10-Q", original)))
    assert fs["I-1"].text.startswith("CONDENSED CONSOLIDATED BALANCE SHEETS")


def test_a_pointer_to_an_item_of_this_report_resolves_inside_it():
    body = html(p("PART I"), p("ITEM 1. BUSINESS"), p(BODY), p("ITEM 2. PROPERTIES"),
                p("See Item 7 of this Annual Report on Form 10-K."), p("PART II"),
                p("ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS"), p("We own three plants. " + BODY), p("SIGNATURES"))
    fs = extract_sections(submission("10-K", "2024-02-01", ("10-K", body)))
    assert fs["2"].status == "stub" and fs["2"].target.kind == "in_document"
    assert fs["2"].target.text.startswith("ITEM 7.")


def test_an_addressing_sentence_wrapped_over_two_lines_is_a_stub():
    body = ("PART I\n\nITEM 1. BUSINESS\n\n" + BODY + "\n\nPART III\n\n"
            "The information required by Items 10 through 13 will be contained in\n"
            "our definitive proxy statement for the 1999 annual meeting.\n\nPART IV\n\n"
            "ITEM 14. EXHIBITS, FINANCIAL STATEMENT SCHEDULES AND REPORTS ON FORM 8-K\n\n27 Financial Data Schedule\n\nSIGNATURES\n")
    fs = extract_sections(submission("10-K", "1999-03-01", ("10-K", body)))
    assert {fs[i].status for i in ("10", "11", "12", "13")} == {"stub"}
    assert fs["10"].target.kind == "proxy"


def test_stacked_eight_k_headings_share_the_body_that_follows_them_all():
    body = html(p("Item 2.02 Results of Operations and Financial Condition."), p("Item 7.01 Regulation FD Disclosure."),
                p("Item 9.01 Financial Statements and Exhibits."),
                p("On May 1, 2024 the company issued a press release announcing its first quarter results, attached as Exhibit 99.1."),
                p("SIGNATURES"))
    fs = extract_sections(submission("8-K", "2024-05-01", ("8-K", body)))
    assert fs["2.02"].text == fs["7.01"].text == fs["9.01"].text and "press release" in fs["2.02"].text
    assert set(fs["2.02"].combined_with) == {"7.01", "9.01"}


def test_forty_f_combined_document_title_opens_every_document_it_names():
    doc = html(p("ANNUAL INFORMATION FORM"), p("The annual information form is filed as Exhibit 99.1."),
               p("DISCLOSURE CONTROLS AND PROCEDURES"), p("Our disclosure controls are effective."),
               p("CODE OF ETHICS"), p("We have adopted a code of ethics."),
               p("UNDERTAKING AND CONSENT TO SERVICE OF PROCESS"), p("The registrant undertakes to make its staff available."),
               p("SIGNATURES"), p("Pursuant to the requirements of the Exchange Act, the registrant has duly caused this report to be signed."),
               p("DOCUMENT 2"), p("MANAGEMENT'S DISCUSSION AND ANALYSIS AND CONSOLIDATED FINANCIAL STATEMENTS"),
               p("For the year ended December 31, 2023"), p(BODY), p("Consolidated balance sheet. " + BODY))
    fs = extract_sections(submission("40-F", "2024-03-01", ("40-F", doc)))
    assert fs["MDA"].start == fs["AFS"].start


def test_forty_f_run_in_title_needs_the_line_before_to_end_a_sentence():
    from edgar_engine.sections.fortyf import match_title
    title = "Code of Ethics. The Company has adopted a code of ethics for its officers."
    assert match_title(title, "the board approved the plan that the company had previously described and adopted") is None
    assert match_title(title, "the board approved the plan the company described in its \u201creport\u201d") == (("CODE",), "runin")
