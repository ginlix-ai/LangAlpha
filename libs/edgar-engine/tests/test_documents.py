"""submission_documents on small synthetic submissions shaped like an 8-K with press-release exhibits."""
import os
import time
from types import SimpleNamespace

import pytest

from edgar_engine import SubmissionDocument, submission_documents
from edgar_engine.documents import attached_documents, exhibit_index, read_with_documents, readable
from edgar_engine.sections.documents import decode

BODY = "The company reported results for the quarter ended in the period."


def document(seq: int, typ: str, filename: str, description: str, body: str) -> str:
    return (f"<DOCUMENT>\n<TYPE>{typ}\n<SEQUENCE>{seq}\n<FILENAME>{filename}\n<DESCRIPTION>{description}\n"
            f"<TEXT>\n{body}\n</TEXT>\n</DOCUMENT>")


def submission(*docs: str) -> str:
    head = ("<SEC-HEADER>\nACCESSION NUMBER:\t\t0000000000-00-000001\nCONFORMED SUBMISSION TYPE:\t8-K\n"
            "FILED AS OF DATE:\t\t20240201\n</SEC-HEADER>\n")
    return "<SEC-DOCUMENT>0000000000-00-000001.txt\n" + head + "\n".join(docs) + "\n</SEC-DOCUMENT>\n"


FULL = submission(
    document(1, "8-K", "main8k.htm", "FORM 8-K",
             "<html><body><p>Item 2.02 Results of Operations</p><p>" + BODY + "</p></body></html>"),
    document(2, "EX-99.1", "pressrelease.htm", "Press release",
             "<html><body><p>Revenue was up.</p><table><tr><td>Revenue</td><td>$</td><td>1,234</td></tr></table></body></html>"),
    document(3, "EX-99.2", "commentary.htm", "CFO commentary", "<html><body><p>Outlook is stable.</p></body></html>"),
    document(4, "GRAPHIC", "logo.jpg", "Logo", "begin 644 logo.jpg\nM1234567890\nend"),
    document(5, "EX-101.SCH", "widget-20240131.xsd", "XBRL schema", "<xsd:schema>" + "x" * 50 + "</xsd:schema>"))


def test_every_text_document_is_returned_in_sequence_order_with_its_own_rendering():
    got = submission_documents(FULL, keep=None)
    assert [(d.seq, d.type, d.filename, d.description) for d in got] == [
        (1, "8-K", "main8k.htm", "FORM 8-K"), (2, "EX-99.1", "pressrelease.htm", "Press release"),
        (3, "EX-99.2", "commentary.htm", "CFO commentary"), (5, "EX-101.SCH", "widget-20240131.xsd", "XBRL schema")]
    assert got[0].text == "Item 2.02 Results of Operations\n" + BODY
    assert got[1].text == "Revenue was up.\nRevenue | $ 1,234"
    assert got[2].text == "Outlook is stable."
    assert isinstance(got[0], SubmissionDocument)


def test_keep_skips_documents_and_a_binary_one_is_never_returned():
    seen = []

    def keep(typ, filename):
        seen.append((typ, filename))
        return not typ.startswith("EX-101")

    assert [d.type for d in submission_documents(FULL, keep)] == ["8-K", "EX-99.1", "EX-99.2"]
    assert ("EX-101.SCH", "widget-20240131.xsd") in seen and ("GRAPHIC", "logo.jpg") not in seen
    assert "GRAPHIC" not in [d.type for d in submission_documents(FULL, lambda typ, filename: True)]


def test_documents_come_back_in_sequence_order_whatever_their_order_in_the_file():
    swapped = submission(document(2, "EX-99.1", "b.htm", "", "<p>Second.</p>"), document(1, "8-K", "a.htm", "", "<p>First.</p>"))
    assert [d.text for d in submission_documents(swapped)] == ["First.", "Second."]


def test_a_truncated_submission_or_one_with_no_document_is_refused():
    with pytest.raises(ValueError, match="truncated"):
        submission_documents(FULL[: FULL.rindex("</DOCUMENT>")])
    with pytest.raises(ValueError, match="truncated"):
        submission_documents(FULL[: FULL.rindex("<DOCUMENT>")])
    with pytest.raises(ValueError, match="truncated"):
        submission_documents("")

def test_by_default_xbrl_graphics_and_viewer_pages_are_left_out():
    assert [d.type for d in submission_documents(FULL)] == ["8-K", "EX-99.1", "EX-99.2"]
    assert not readable("HTML", "R2.htm") and readable("EX-99.1", "ex991.htm") and readable("HTML", "report.htm")
    assert not readable("EX-99.1", "ex991.pdf")


def test_attached_documents_leave_out_the_document_the_sections_are_read_from():
    assert [d.type for d in attached_documents(FULL, "8-K")] == ["EX-99.1", "EX-99.2"]


def test_read_with_documents_reads_one_download_and_then_its_caches(monkeypatch, tmp_path):
    from edgar_engine import filings as filings_mod
    from edgar_engine.cache import DocumentsCache, LocalDirStore, SectionsCache
    from edgar_engine.filings import FilingRef

    fetched = []
    filing = SimpleNamespace(accession_no="0001234567-24-000001", text_url="u",
                             full_text_submission=lambda: fetched.append(1) or FULL)
    monkeypatch.setattr(filings_mod, "get_filing", lambda ref: filing)
    ref = FilingRef(1234567, "0001234567-24-000001", "8-K", "2024-01-31")
    store = LocalDirStore(tmp_path)

    first = read_with_documents(ref, SectionsCache(store), DocumentsCache(store))
    again = read_with_documents(ref, SectionsCache(store), DocumentsCache(store))

    assert fetched == [1] and again == first
    sections, docs = first
    assert "2.02" in sections.items and sections["2.02"].source == ref.accession
    assert [d.type for d in docs] == ["EX-99.1", "EX-99.2"]


def test_the_exhibit_index_reads_the_item_that_lists_exhibits_whatever_its_heading():
    from edgar_engine import FilingSections, Section

    text = "Item 9.01\n| Exhibit No. | Description |\n| 99.1 | Press Release, dated May 1 |\n104 Cover Page |\n 99.2" + " " * 5000 + "x"
    fs = FilingSections(accession="a", form="8-K", filing_date="2024-05-01", text=text, engine_version="t",
                        items={"9.01": Section(item="9.01", status="present", text=text, start=0, end=len(text))})
    started = time.perf_counter()
    assert exhibit_index(fs) == {"99.1": "Press Release, dated May 1", "104": "Cover Page", "99.2": "x"}
    assert time.perf_counter() - started < 0.5



@pytest.mark.network
def test_live_earnings_release_exhibits_are_returned():
    from edgar import Filing, set_identity

    set_identity(os.environ["EDGAR_IDENTITY"])
    filing = Filing(cik=1045810, company="", form="8-K", filing_date="2026-11-17", accession_no="0001045810-26-000088")
    got = {d.type: d for d in submission_documents(decode(filing.full_text_submission()))}
    assert [got[t].filename for t in ("EX-99.1", "EX-99.2", "EX-99.3")] == [
        "q3fy27pr.htm", "q3fy27cfocommentary.htm", "supplementalfinancialinfor.htm"]
    assert all(got[t].text for t in ("EX-99.1", "EX-99.2", "EX-99.3"))
