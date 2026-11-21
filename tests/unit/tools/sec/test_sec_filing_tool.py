"""get_sec_filing on the EDGAR engine: what it writes to the workspace and what it shows the model."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from edgar_engine import FilingSections, SubmissionDocument
from edgar_engine.documents import exhibit_index
from edgar_engine.filings import FilingRef

from src.data_client.ginlix_data.sec_filings import FilingCalendar, OwedReport
from src.tools.sec import edgar, render, tool
from tests.unit.tools.sec.fakes import ACC, AMEND, FOLDER, INCOME, ORIGINAL, REF, Backend, filing, section


@pytest.fixture
def acme(monkeypatch):
    """One company with one 10-K and no earnings call, read without the network."""
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), [])
    state = SimpleNamespace(amendments=[], unread=[], call=None,
                            fin={ACC: edgar.Financials({"revenue": 2e9}, "| Revenue | 2 |", "USD", held=INCOME)})
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "fiscal_labels", AsyncMock(return_value={}))
    monkeypatch.setattr(edgar, "select", lambda *a: REF)
    monkeypatch.setattr(edgar, "sections", AsyncMock(side_effect=lambda co, ref: ORIGINAL))
    monkeypatch.setattr(edgar, "financials", AsyncMock(side_effect=lambda ref: state.fin.get(ref.accession, edgar.Financials())))
    monkeypatch.setattr(edgar, "amendments", AsyncMock(side_effect=lambda co, labels, ref: (state.amendments, state.unread)))
    monkeypatch.setattr(edgar, "periodic_reports", lambda co, labels: [REF])
    monkeypatch.setattr(edgar, "recent_8ks", lambda co, after=None: [])
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: pytest.fail("no 8-K read expected")))
    monkeypatch.setattr(tool, "fetch_matching_earnings_call", AsyncMock(side_effect=lambda *a: state.call))
    return state


@pytest.mark.asyncio
async def test_the_item_files_are_written_before_the_index_that_lists_them(acme):
    backend = Backend()
    content, artifact = await tool.get_sec_filing_async("ACME", "10-K", backend=backend)

    assert [sorted(b) for b in backend.batches] == [
        sorted(f"{FOLDER}/{f}" for f in ("item_1.md", "item_7.md", "item_11.md", "full.md", "statements.md")),
        [f"{FOLDER}/index.md"]]
    assert set(backend.files) == {f"{FOLDER}/{f}" for f in
                                  ("item_1.md", "item_7.md", "item_11.md", "full.md", "statements.md", "index.md")}
    assert backend.files[f"{FOLDER}/item_11.md"].startswith(
        "ACME 10-K FY2025, Item 11: Executive Compensation (stub, points to the proxy statement)\n"
        f"From accession {ACC}, filed 2026-02-01. Source: {edgar.index_url(1, ACC)}\n")
    assert f"Each item's full text is in {FOLDER}/." in content
    assert "| 7 | Management's Discussion and Analysis of Financial Condition and Results of Operations | present |" in content
    assert "Sales rose" not in content and "<!--" not in content
    assert "| Revenue | $2.00B |" in content
    assert artifact["folder"] == FOLDER and artifact["sections_extracted"] == 3 and artifact["accession"] == ACC


@pytest.mark.asyncio
async def test_a_folder_written_by_this_engine_version_keeps_its_item_files(acme):
    backend = Backend()
    await tool.get_sec_filing_async("ACME", "10-K", backend=backend)
    backend.files[f"{FOLDER}/item_7.md"] = "kept"

    await tool.get_sec_filing_async("ACME", "10-K", backend=backend)

    assert backend.files[f"{FOLDER}/item_7.md"] == "kept"


@pytest.mark.asyncio
async def test_a_file_that_failed_to_write_keeps_the_index_out_so_the_next_call_writes_it_again(acme):
    backend = Backend(fail={f"{FOLDER}/item_7.md"})
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=backend)
    assert f"{FOLDER}/index.md" not in backend.files and "call again to write the files" in content

    backend.fail = None
    await tool.get_sec_filing_async("ACME", "10-K", backend=backend)

    assert f"{FOLDER}/item_7.md" in backend.batches[-2] and f"{FOLDER}/index.md" in backend.files


@pytest.mark.asyncio
async def test_statements_read_after_a_failure_are_written_by_the_next_call(acme):
    acme.fin[ACC] = edgar.Financials(failed=True)
    backend = Backend()
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=backend)
    assert render.STATEMENTS_FAILED in content and f"{FOLDER}/statements.md" not in backend.files

    acme.fin[ACC] = edgar.Financials({"revenue": 2e9}, "| Revenue | 2 |", "USD", held=INCOME)
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=backend)

    assert backend.files[f"{FOLDER}/statements.md"] == "| Revenue | 2 |" and "statements.md (income" in content


@pytest.mark.parametrize(("currency", "cell"), [("USD", "$2.00B"), ("JPY", "JPY 2.00B"), (None, "2.00B")])
def test_key_metrics_carry_the_reporting_currency_and_never_guess_one(currency, cell):
    lines = render.metrics_section(edgar.Financials({"revenue": 2e9}, "s", currency), "2026-02-01")
    assert f"| Revenue | {cell} |" in lines


@pytest.mark.asyncio
async def test_a_failed_write_returns_the_index_without_file_paths(acme):
    content, artifact = await tool.get_sec_filing_async("ACME", "10-K", backend=Backend(fail="write_failed: gone"))

    assert "| Item | Title | Status | Size |\n" in content and "item_7.md" not in content
    assert render.STATUS_LEGEND_NO_FILES in content and "its file holds" not in content
    assert "Also in the folder" not in content
    assert content.rstrip().endswith(render.files_not_written(FOLDER, f"{FOLDER}/item_1.md: write_failed: gone"))
    assert "folder" not in artifact


@pytest.mark.asyncio
async def test_as_amended_writes_merged_items_that_name_the_filing_their_text_comes_from(acme):
    acme.amendments = [filing(section("11", "Item 11. Executive Compensation\nThe CEO earned $1."),
                              accession=AMEND, form="10-K/A", filed="2026-04-30", scope="partial")]
    backend = Backend()
    content, artifact = await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=backend)

    item = backend.files[f"{FOLDER}/as_amended/item_11.md"]
    assert item.startswith("ACME 10-K FY2025, Item 11: Executive Compensation (present)\n"
                           f"From accession {AMEND}, filed 2026-04-30. Source: {edgar.index_url(1, AMEND)}\n")
    assert "The CEO earned $1." in item
    assert f"As amended through {AMEND} (filed 2026-04-30)." in content
    assert "| as_amended/item_11.md |" in content and artifact["view"] == "as_amended"
    assert "restates items 11" in backend.files[f"{FOLDER}/index.md"]


@pytest.mark.asyncio
async def test_as_amended_figures_come_from_the_amendment_that_restates_the_statements(acme):
    acme.amendments = [filing(section("8", "Item 8. Financial Statements\nRestated."), accession=AMEND, form="10-K/A",
                              filed="2026-04-30", scope="partial")]
    acme.fin[AMEND] = edgar.Financials({"revenue": 1e9}, "| Revenue | 1 |", "USD", held=INCOME)
    backend = Backend()
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=backend)

    assert "| Revenue | $1.00B |" in content and "as_amended/statements.md (income" in content
    assert backend.files[f"{FOLDER}/as_amended/statements.md"] == "| Revenue | 1 |"
    assert "| Revenue | $2.00B |" in backend.files[f"{FOLDER}/index.md"]


@pytest.mark.asyncio
async def test_an_amendment_that_cannot_be_read_is_named_and_the_original_still_returned(acme):
    acme.unread = [FilingRef(1, AMEND, "10-K/A", "2026-04-30", amends=ACC)]
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=Backend())

    assert f"As filed. Amended by {AMEND} (filed 2026-04-30), not merged because it could not be read." in content
    assert "| 7 |" in content and 'view="as_amended"' not in content

    acme.amendments = [filing(section("11", "Item 11. Executive Compensation\nPay."), accession="0000000001-26-000003",
                              form="10-K/A", filed="2026-03-30", scope="partial")]
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=Backend())
    assert ("As amended through 0000000001-26-000003 (filed 2026-03-30). Each item file names the filing its text "
            f"comes from. Not merged, because it could not be read: {AMEND} (filed 2026-04-30).") in content


@pytest.mark.asyncio
async def test_a_later_amendment_rewrites_the_files_whose_text_it_changes(acme):
    acme.amendments = [filing(section("11", "Item 11. Executive Compensation\nFirst."), accession=AMEND,
                              form="10-K/A", filed="2026-04-30", scope="partial")]
    backend = Backend()
    await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=backend)
    acme.amendments.append(filing(section("11", "Item 11. Executive Compensation\nSecond."),
                                  accession="0000000001-26-000003", form="10-K/A", filed="2026-05-30", scope="partial"))

    await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=backend)

    assert "Second." in backend.files[f"{FOLDER}/as_amended/item_11.md"]


@pytest.mark.asyncio
async def test_the_insight_form_carries_the_mdna_and_then_the_call_cut_at_the_cap(acme, monkeypatch):
    from datetime import date

    from src.tools.sec.earnings_call import EarningsCall

    acme.call = EarningsCall("Operator: welcome. " * 50, 2025, 4, date(2026, 1, 28))
    whole, _ = await tool.get_sec_filing_async("ACME", "10-K")
    monkeypatch.setattr(tool, "INSIGHT_CAP_CHARS", whole.index("## Earnings call") + 300)

    content, artifact = await tool.get_sec_filing_async("ACME", "10-K")

    mdna = content.index("## Item 7: Management's Discussion")
    call = content.index("## Earnings call: Q4 FY2025, held 2026-01-28, reporting the period ended 2025-12-31")
    assert mdna < call and content.rstrip().endswith(render.CUT_CALL) and len(content) <= tool.INSIGHT_CAP_CHARS
    assert "earnings_call.md" not in content and "File |" not in content
    assert artifact["has_earnings_call"] is True and "folder" not in artifact


@pytest.mark.asyncio
@pytest.mark.parametrize(("args", "expected"), [
    ({"filing_type": "S-1"}, render.bad_form("S-1")),
    ({"view": "amended"}, render.bad_view("amended")),
    ({"filing_type": "10-K", "fiscal_period": "Q1"}, render.period_on_annual("10-K")),
    ({"filing_type": "10-Q", "fiscal_period": "Q4"}, render.bad_period("Q4")),
    ({"filing_type": "8-K", "fiscal_year": 2023}, render.eight_k_period()),
    ({"filing_type": "8-K", "fiscal_period": "Q1"}, render.eight_k_period()),
    ({"filing_type": "8-K", "view": "as_amended"}, render.eight_k_amended()),
])
async def test_arguments_the_tool_cannot_serve_are_answered_before_any_request(args, expected, monkeypatch):
    monkeypatch.setattr(edgar, "company", lambda s: pytest.fail("no request expected"))
    assert await tool.get_sec_filing_async("ACME", **args) == (expected, {})


def test_recent_reports_name_a_newer_report_and_add_an_older_returned_one_under_a_gap():
    def ref(acc, form, end, filed, fy, fp):
        return FilingRef(1, acc, form, filed, end, fy, fp)

    reports = [ref("a", "10-Q", "2026-09-30", "2026-11-01", 2026, "Q3"), ref("b", "10-Q", "2026-06-30", "2026-08-01", 2026, "Q2"),
               ref("c", "10-Q", "2026-03-31", "2026-05-01", 2026, "Q1"), ref("d", "10-K", "2025-12-31", "2026-02-01", 2025, "FY")]
    old = ref("z", "20-F", "2015-12-31", "2016-04-01", 2015, "FY")

    lines = render.recent_reports_section(reports, old, foreign=True, calendar=None)

    assert lines[1] == ("4 newer reports are on file; the latest is the 10-Q for Q3 of fiscal year 2026, period ended "
                        "2026-09-30, filed 2026-11-01. Fetch it for later figures.")
    assert lines[-4:-2] == ["| ... | | | | |", "| 20-F | FY2015 | 2015-12-31 | 2016-04-01 | returned |"]
    assert lines[-1] == "Interim results of a foreign issuer come in 6-K reports, which this tool does not fetch."
    assert render.recent_reports_section(reports, reports[0], foreign=False, calendar=None)[1] == "This is the latest periodic report on file."


def test_an_8k_document_is_labeled_by_its_description_else_its_exhibit_list_entry_else_its_first_line():
    def doc(type_, description, text="Press release text"):
        return SubmissionDocument(2, type_, "x.htm", description, text)

    exhibits = exhibit_index(filing(section("9.01", "Item 9.01 Financial Statements and Exhibits\n"
                                                         "| Exhibit No. | Description |\n"
                                                         "| 99.1 | Press Release, dated November 17, 2026 |"),
                                         form="8-K"))
    assert exhibits == {"99.1": "Press Release, dated November 17, 2026"}
    assert render.document_label(doc("EX-99.2", "CFO Commentary"), exhibits) == "CFO Commentary"
    assert render.document_label(doc("EX-99.1", "EX-99.1"), exhibits) == "Press Release, dated November 17, 2026"
    assert render.document_label(doc("EX-99.3", "", "\n  Supplemental data\nmore"), exhibits) == "Supplemental data"
    label = render.document_label(doc("EX-99.1", "EX-99.1"), exhibits)
    assert render.document_file_name(doc("EX-99.1", ""), label) == "ex-99.1_press-release-dated-november-17-2026.md"


def test_concurrent_reads_of_one_accession_compute_it_once_and_waiters_hold_no_pool_thread():
    release, calls = threading.Event(), []

    def slow(key):
        calls.append(key)
        assert release.wait(5), "a waiter held the pool thread the other read needed"
        return key

    def other(key):
        release.set()
        return key

    slow_read, other_read = edgar._shared(lambda key: key)(slow), edgar._shared(lambda key: key)(other)

    async def main():
        waiting = [asyncio.create_task(slow_read("sections:x")) for _ in range(edgar._pool._max_workers + 1)]
        await asyncio.sleep(0.05)
        assert await asyncio.wait_for(other_read("sections:y"), 5) == "sections:y"
        return await asyncio.gather(*waiting)

    assert asyncio.run(main()) == ["sections:x"] * (edgar._pool._max_workers + 1) and calls == ["sections:x"]
    assert not edgar._inflight


@pytest.mark.asyncio
async def test_8k_amendments_are_listed_and_the_inline_form_carries_each_filings_own_text(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), [
        {"form": "8-K/A", "accession_number": AMEND, "filing_date": "2099-01-02", "items": "5.02"},
        {"form": "8-K", "accession_number": ACC, "filing_date": "2099-01-01", "items": "5.07"},
        {"form": "S-8", "accession_number": "x", "filing_date": "2099-01-01", "items": ""}])
    texts = {AMEND: "Item 5.02 Departure of Directors\nThe CFO resigned.", ACC: "Item 5.07 Vote\nHolders voted."}
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (filing(section(k.items[0], texts[k.ref.accession]),
                                                            accession=k.ref.accession, form=k.ref.form), [])))

    content, artifact = await tool.get_sec_filing_async("ACME", "8-K")

    assert f"## 2099-01-02, accession {AMEND} (8-K/A)" in content and f"## 2099-01-01, accession {ACC}\n" in content
    assert "The CFO resigned." in content and "Holders voted." in content and "in its folder" not in content
    assert [f["form"] for f in artifact["filings"]] == ["8-K/A", "8-K"]


@pytest.mark.asyncio
async def test_an_8k_that_cannot_be_read_says_so(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()),
                       [{"form": "8-K", "accession_number": ACC, "filing_date": "2099-01-01", "items": "8.01"}])
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (_ for _ in ()).throw(ValueError("truncated"))))

    content, artifact = await tool.get_sec_filing_async("ACME", "8-K", backend=Backend())

    assert render.EIGHT_K_UNREADABLE in content and "folder" not in artifact["filings"][0]


@pytest.mark.asyncio
async def test_an_8k_with_no_item_found_keeps_its_whole_text(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()),
                       [{"form": "8-K/A", "accession_number": ACC, "filing_date": "2099-01-01", "items": ""}])
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (FilingSections(
        accession=ACC, form="8-K/A", filing_date="2099-01-01", items={}, text="Explanatory note.", engine_version="t"), [])))
    backend = Backend()

    content, artifact = await tool.get_sec_filing_async("ACME", "8-K", backend=backend)

    folder = f"data/sec/ACME/8-K_2099-01-01_{ACC}"
    assert backend.files[f"{folder}/full.md"] == "Explanatory note." and "- full.md (under 1k tokens in all)" in content
    assert artifact["filings"][0]["folder"] == folder


def test_only_an_unknown_company_reads_as_not_a_filer(monkeypatch):
    import edgar as edgartools
    from edgar.exceptions import CompanyNotFoundError

    def lookup(error):
        def company(symbol):
            raise error
        monkeypatch.setattr(edgartools, "Company", company)
        return lambda: asyncio.run(edgar.company("ACME"))

    with pytest.raises(edgar.NotAFiler):
        lookup(CompanyNotFoundError("ACME"))()
    with pytest.raises(TimeoutError):
        lookup(TimeoutError("slow"))()



def test_the_8k_reminder_keeps_a_same_day_8k_accepted_after_the_report():
    rows = [{"form": "8-K", "accession_number": "a", "filing_date": "2099-01-01", "acceptanceDateTime": "2099-01-01T16:30:00"},
            {"form": "8-K", "accession_number": "b", "filing_date": "2099-01-01", "acceptanceDateTime": "2099-01-01T08:00:00"}]
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), rows)
    report = FilingRef(1, ACC, "10-Q", "2099-01-01", accepted="2099-01-01T12:00:00")
    assert [k.ref.accession for k in edgar.recent_8ks(co, after=report)] == ["a"]
    assert [k.ref.accession for k in edgar.recent_8ks(co, after=replace(report, accepted=None))] == ["a", "b"]


@pytest.mark.asyncio
async def test_only_a_document_labeled_as_a_release_marks_a_press_release(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()),
                       [{"form": "8-K", "accession_number": ACC, "filing_date": "2099-01-01", "items": "7.01"}])
    deck = SubmissionDocument(2, "EX-99.1", "deck.htm", "Investor Presentation", "Slides")
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (filing(section("7.01", "Item 7.01 Regulation FD"), form="8-K"), [deck])))
    _, artifact = await tool.get_sec_filing_async("ACME", "8-K")
    assert artifact["filings"][0]["has_press_release"] is False

    deck = replace(deck, description="Press release dated January 1")
    _, artifact = await tool.get_sec_filing_async("ACME", "8-K")
    assert artifact["filings"][0]["has_press_release"] is True

@pytest.mark.asyncio
async def test_statements_that_failed_to_read_are_named_and_the_rest_still_written(acme):
    acme.fin[ACC] = edgar.Financials({"revenue": 0.0}, "| Revenue | 0 |", "USD", unread=("cash flow statement",),
                                     held=("income statement", "balance sheet"))
    backend = Backend()
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=backend)

    assert "| Revenue | $0.00B |" in content and render.statements_unread(["cash flow statement"]) in content
    assert "statements.md (income statement and balance sheet," in content
    assert backend.files[f"{FOLDER}/statements.md"] == "| Revenue | 0 |"


@pytest.mark.asyncio
async def test_the_amended_index_names_full_md_as_the_filing_as_first_filed(acme):
    acme.amendments = [filing(section("7", "Item 7. MD&A\nRestated.", source=AMEND), accession=AMEND, form="10-K/A",
                              filed="2026-04-30", scope="partial")]
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=Backend())
    assert "full.md (the whole filing as first filed," in content


@pytest.mark.asyncio
async def test_a_failed_write_still_names_the_matched_earnings_call(acme):
    acme.call = SimpleNamespace(quarter=4, fiscal_year=2025, call_date=__import__("datetime").date(2026, 1, 28),
                                transcript="We sold anvils.")
    content, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=Backend(fail="write_failed: gone"))
    assert "Earnings call matched: the Q4 FY2025 earnings call held 2026-01-28" in content


@pytest.mark.asyncio
async def test_an_upload_error_with_no_message_still_reads_as_a_failure():
    class Raising(Backend):
        async def aupload_files(self, files):
            raise TimeoutError()

    assert await tool._upload(Raising(), FOLDER, {"a.md": "x"}) == "TimeoutError"


@pytest.mark.asyncio
async def test_the_insight_8k_form_keeps_the_part_of_an_oversized_document_that_fits(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()),
                       [{"form": "8-K", "accession_number": ACC, "filing_date": "2099-01-01", "items": ""}])
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    big = "Item 2.02 Results\n" + "Revenue rose. " * 20_000
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (filing(section("2.02", big), accession=ACC, form="8-K"), [])))

    content, artifact = await tool.get_sec_filing_async("ACME", "8-K")

    assert "Revenue rose." in content and content.rstrip().endswith(render.CUT_DOCUMENTS)
    assert len(content) <= tool.INSIGHT_CAP_CHARS and "2.02 Results of Operations" in content
    assert artifact["filings"][0]["items"] == ["2.02"]


def test_the_8k_reminder_keeps_a_same_day_8k_with_no_acceptance_time():
    rows = [{"form": "8-K", "accession_number": "a", "filing_date": "2099-01-01"}]
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), rows)
    report = FilingRef(1, ACC, "10-Q", "2099-01-01", accepted="2099-01-01T12:00:00")
    assert [k.ref.accession for k in edgar.recent_8ks(co, after=report)] == ["a"]


@pytest.mark.parametrize(("on_recent_page", "full_history"), [(True, False), (False, True)])
def test_amendments_of_a_filing_older_than_the_recent_page_are_read_from_the_whole_history(
        monkeypatch, on_recent_page, full_history):
    rows = [{"form": "10-K", "accession_number": ACC if on_recent_page else "newer", "filing_date": "2026-02-01"}]
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), rows)
    asked = []
    monkeypatch.setattr(edgar, "filings_of", lambda co, form, labels, **kw: asked.append(kw["full_history"]) or [])
    assert asyncio.run(edgar.amendments(co, {}, REF)) == ([], []) and asked == [full_history]


def test_a_cache_that_cannot_be_read_reads_as_a_miss():
    class Down:
        def get(self, key):
            raise ConnectionError("storage down")

    assert edgar._MeteredStore(Down()).get("sections/x") is None


def test_the_8k_reminder_compares_acceptance_times_as_timestamps():
    from datetime import datetime

    rows = [{"form": "8-K", "accession_number": "a", "filing_date": "2099-01-01",
             "acceptanceDateTime": datetime(2099, 1, 1, 18, 0)}]
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()), rows)
    report = FilingRef(1, ACC, "10-Q", "2099-01-01", accepted="2099-01-01T17:00:00")
    assert [k.ref.accession for k in edgar.recent_8ks(co, after=report)] == ["a"]


def test_8ks_older_than_a_short_recent_page_are_read_from_the_whole_history():
    from datetime import date, timedelta

    today, old = (date.today() - timedelta(days=edgar.EIGHT_K_DAYS)).isoformat(), (date.today() - timedelta(days=60)).isoformat()
    full = [{"form": "8-K", "accession_number": "new", "filing_date": today},
            {"form": "8-K", "accession_number": "old", "filing_date": old}]
    asked = []
    obj = SimpleNamespace(get_filings=lambda **kw: asked.append(kw) or SimpleNamespace(
        data=SimpleNamespace(to_pylist=lambda: full)))
    page = [{"form": "424B2", "accession_number": f"p{i}", "filing_date": today} for i in range(edgar.RECENT_PAGE_MIN)]
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(obj), page)
    assert [k.ref.accession for k in edgar.recent_8ks(co)] == ["old", "new"]
    assert asked and asked[0]["trigger_full_load"] is True


@pytest.mark.asyncio
async def test_the_amended_index_points_citations_of_amended_content_at_their_own_source(acme):
    acme.amendments = [filing(section("7", "Item 7. MD&A\nRestated.", source=AMEND), accession=AMEND, form="10-K/A",
                              filed="2026-04-30", scope="partial")]
    amended, _ = await tool.get_sec_filing_async("ACME", "10-K", view="as_amended", backend=Backend())
    filed, _ = await tool.get_sec_filing_async("ACME", "10-K", backend=Backend())
    assert render.CITE_AMENDED in amended and render.CITE in filed and render.CITE_AMENDED not in filed


@pytest.mark.asyncio
async def test_the_8k_reminder_is_read_on_the_sec_pool_not_the_event_loop(acme, monkeypatch):
    threads = []
    monkeypatch.setattr(edgar, "recent_8ks", lambda co, after=None: threads.append(threading.current_thread().name) or [])
    await tool.get_sec_filing_async("ACME", "10-K", backend=Backend())
    await tool.get_sec_filing_async("ACME", "8-K")
    assert len(threads) == 2 and all(t.startswith("sec") for t in threads)


@pytest.mark.asyncio
async def test_a_reminder_that_cannot_be_read_still_returns_the_filing(acme, monkeypatch):
    def down(co, after=None):
        raise TimeoutError("slow")

    monkeypatch.setattr(edgar, "recent_8ks", down)
    content, artifact = await tool.get_sec_filing_async("ACME", "10-K", backend=Backend())
    assert artifact["folder"] == FOLDER and artifact["recent_8k_count"] == 0


@pytest.mark.asyncio
async def test_an_8k_amendment_is_kept_whole_so_its_explanatory_note_is_not_lost(monkeypatch):
    co = edgar.Company("ACME", "Acme Corp", 1, edgar.edgar_rows(object()),
                       [{"form": "8-K/A", "accession_number": ACC, "filing_date": "2099-01-01", "items": "9.01"}])
    note, item = "Explanatory Note: this amendment adds the audited statements.\n", "Item 9.01 Financial Statements\nSee 99.1."
    sections = replace(filing(section("9.01", item, start=len(note)), accession=ACC, form="8-K/A"), text=note + item)
    monkeypatch.setattr(edgar, "company", AsyncMock(return_value=co))
    monkeypatch.setattr(edgar, "eight_k", AsyncMock(side_effect=lambda k: (sections, [])))
    backend = Backend()

    content, _ = await tool.get_sec_filing_async("ACME", "8-K", backend=backend)
    inline, _ = await tool.get_sec_filing_async("ACME", "8-K")

    folder = f"data/sec/ACME/8-K_2099-01-01_{ACC}"
    assert backend.files[f"{folder}/full.md"] == note + item and f"{folder}/item_9.01.md" not in backend.files
    assert "- full.md (" in content and "Explanatory Note" in inline


FILED = {"n": ("NT 10-Q", "2026-05-12"), "t": ("15-12G", "2026-04-20")}


def _calendar(verdict, latest=ACC, termination=None, today="2026-04-01", **next_fields):
    owed = {"form": "10-Q", "fiscal_period": "Q1", "period_end": "2026-03-31", "window": ("2026-04-28", "2026-05-02"),
            "deadline": "2026-05-11", "latest_deadline": "2026-05-11", "notice": None, **next_fields}
    return edgar.Calendar(FilingCalendar(1, verdict, latest, termination, OwedReport(**owed)), FILED, today)


ANNUAL = FilingRef(1, ACC, "10-K", "2026-02-01", report_date="2025-12-31", fiscal_year=2025, fiscal_period="FY")


@pytest.mark.parametrize(("calendar", "line"), [
    (_calendar("current"), "This is the latest periodic report on file. The next is the 10-Q for Q1 of fiscal year 2026 "
     "(period end 2026-03-31), expected between 2026-04-28 and 2026-05-02 and due by 2026-05-11."),
    (_calendar("expected"), "This is the latest periodic report on file, and the next may be filed any day: the 10-Q for "
     "Q1 of fiscal year 2026 (period end 2026-03-31), expected between 2026-04-28 and 2026-05-02 and due by 2026-05-11."),
    (_calendar("extended", notice="n", latest_deadline="2026-05-18"), "This is the latest periodic report on file. "
     "The 10-Q for Q1 of fiscal year 2026 (period end 2026-03-31) was due by 2026-05-11; the company filed a notice "
     "of late filing on 2026-05-12, which extends the due date to 2026-05-18."),
    (_calendar("overdue", latest_deadline="2026-05-18"), "This is the latest periodic report on file. The 10-Q for Q1 "
     "of fiscal year 2026 (period end 2026-03-31) is late: it was due by 2026-05-11 and is still not on file after "
     "2026-05-18, the latest date it could have been on time."),
    (_calendar("deregistered", termination="t"), "This is the latest periodic report on file, and no newer "
     "one is expected: the company filed Form 15-12G on 2026-04-20 to stop reporting, and the 10-Q for Q1 of fiscal "
     "year 2026 (period end 2026-03-31) due by 2026-05-11 was not filed."),
    (_calendar("unknown"), "This is the latest periodic report on file."),
    (_calendar("current", latest="other"), "This is the latest periodic report on file."),
    (_calendar("extended", notice="missing"), "This is the latest periodic report on file."),
    (replace(_calendar("current"), served=replace(_calendar("current").served, next=None)),
     "This is the latest periodic report on file."),
    (None, "This is the latest periodic report on file."),
])
def test_the_calendar_says_what_the_latest_report_on_file_is_followed_by(calendar, line):
    assert render.recent_reports_section([ANNUAL], ANNUAL, foreign=False, calendar=calendar)[1] == line


@pytest.mark.parametrize(("day", "line"), [
    ("2026-05-11", "This is the latest periodic report on file, and the next may be filed any day: the 10-Q for Q1 of "
     "fiscal year 2026 (period end 2026-03-31), expected between 2026-04-28 and 2026-05-02 and due by 2026-05-11."),
    ("2026-05-12", "This is the latest periodic report on file. The next, the 10-Q for Q1 of fiscal year 2026 (period "
     "end 2026-03-31), was due by 2026-05-11 and is not on file yet; it can still be on time until 2026-05-15."),
    ("2026-05-16", "This is the latest periodic report on file. The 10-Q for Q1 of fiscal year 2026 (period end "
     "2026-03-31) is late: it was due by 2026-05-11 and is still not on file after 2026-05-15, the latest date it "
     "could have been on time."),
])
@pytest.mark.parametrize("served", ["expected", "current"])
def test_an_expected_report_is_worded_by_the_day_it_is_read_not_the_day_the_calendar_was(day, line, served):
    calendar = _calendar(served, latest_deadline="2026-05-15", today=day)
    assert render.recent_reports_section([ANNUAL], ANNUAL, foreign=False, calendar=calendar)[1] == line


@pytest.mark.parametrize(("window", "shown"), [
    (("2026-05-04", "2026-05-14"), "expected between 2026-05-04 and 2026-05-11 and due by 2026-05-11"),
    (("2026-05-12", "2026-05-14"), "expected between 2026-05-12 and 2026-05-14 and due by 2026-05-11"),
])
def test_a_window_that_runs_past_the_due_date_is_shown_ending_at_it(window, shown):
    calendar = _calendar("current", window=window)
    assert shown in render.recent_reports_section([ANNUAL], ANNUAL, foreign=False, calendar=calendar)[1]


def test_an_extension_that_has_run_out_by_the_day_it_is_read_is_worded_as_late():
    calendar = _calendar("extended", notice="n", latest_deadline="2026-05-18", today="2026-05-19")
    assert render.recent_reports_section([ANNUAL], ANNUAL, foreign=False, calendar=calendar)[1] == (
        "This is the latest periodic report on file. The 10-Q for Q1 of fiscal year 2026 (period end 2026-03-31) is "
        "late: it was due by 2026-05-11 and is still not on file after 2026-05-18, the latest date it could have been "
        "on time.")


def test_a_report_that_lapsed_by_the_day_it_is_read_after_a_form_15_is_worded_as_deregistered():
    calendar = _calendar("expected", termination="t", today="2026-05-12")
    assert render.recent_reports_section([ANNUAL], ANNUAL, foreign=False, calendar=calendar)[1] == (
        "This is the latest periodic report on file, and no newer one is expected: the company filed Form 15-12G on "
        "2026-04-20 to stop reporting, and the 10-Q for Q1 of fiscal year 2026 (period end 2026-03-31) due by "
        "2026-05-11 was not filed.")


def test_the_calendar_names_the_next_fiscal_year_after_an_annual_report_and_no_period_when_unlabeled():
    q3 = replace(ANNUAL, form="10-Q", fiscal_period="Q3")
    assert render._next_period(ANNUAL, "Q1") == " for Q1 of fiscal year 2026"
    assert render._next_period(q3, "FY") == " for fiscal year 2025"
    assert render._next_period(replace(ANNUAL, form="20-F"), "FY") == " for fiscal year 2026"
    assert render._next_period(replace(ANNUAL, fiscal_year=None), "Q1") == ""
