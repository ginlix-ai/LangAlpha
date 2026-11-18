import pandas as pd
import pytest

from edgar_engine.thirteenf import (check_units, consensus_prices, holding_rows, parse_13f, read_13f_dataset,
                                    resolve_amendments, ticker_for_cusip)
from edgar_engine.thirteenf.cusip import check_digit_ok, class_ticker

PRIMARY = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:com="http://www.sec.gov/edgar/common">
  <headerData><submissionType>{form}</submissionType>
    <filerInfo><filer><credentials><cik>0000444444</cik><ccc>XXXXXXXX</ccc></credentials></filer>
      <periodOfReport>06-30-2024</periodOfReport></filerInfo></headerData>
  <formData><coverPage><reportCalendarOrQuarter>06-30-2024</reportCalendarOrQuarter>{amend}
    <filingManager><name>Example Capital LLC</name></filingManager><reportType>13F HOLDINGS REPORT</reportType></coverPage>
    <summaryPage><tableEntryTotal>{entries}</tableEntryTotal><tableValueTotal>{total}</tableValueTotal></summaryPage>
  </formData>
  <schemaVersion>X0202</schemaVersion>
</edgarSubmission>"""

ROW = """<ns1:infoTable><ns1:nameOfIssuer>{name}</ns1:nameOfIssuer><ns1:titleOfClass>COM</ns1:titleOfClass>
  <ns1:cusip>{cusip}</ns1:cusip><ns1:value>{value}</ns1:value>
  <ns1:shrsOrPrnAmt><ns1:sshPrnamt>{shares}</ns1:sshPrnamt><ns1:sshPrnamtType>SH</ns1:sshPrnamtType></ns1:shrsOrPrnAmt>
  <ns1:investmentDiscretion>SOLE</ns1:investmentDiscretion>
  <ns1:votingAuthority><ns1:Sole>{shares}</ns1:Sole><ns1:Shared>0</ns1:Shared><ns1:None>0</ns1:None></ns1:votingAuthority>
</ns1:infoTable>"""

# The prefixed 13F namespace declared after xsi: a reader that takes the first declared namespace finds no rows.
TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<ns1:informationTable xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:ns1="http://www.sec.gov/edgar/document/thirteenf/informationtable"
  xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd">
{rows}
</ns1:informationTable>"""


def report(rows, *, form="13F-HR", entries=None, total=None, amend="", accession="0000444444-24-000001"):
    body = "".join(ROW.format(name=n, cusip=c, value=v, shares=s) for n, c, v, s in rows)
    entries = len(rows) if entries is None else entries
    total = sum(v for _, _, v, _ in rows) if total is None else total
    return f"""<SEC-DOCUMENT>
<SEC-HEADER>
ACCESSION NUMBER:\t\t{accession}
CONFORMED SUBMISSION TYPE:\t{form}
CONFORMED PERIOD OF REPORT:\t20240630
FILED AS OF DATE:\t\t20240814
</SEC-HEADER>
<DOCUMENT><TYPE>{form}<SEQUENCE>1<FILENAME>primary_doc.xml<TEXT><XML>
{PRIMARY.format(form=form, amend=amend, entries=entries, total=total)}
</XML></TEXT></DOCUMENT>
<DOCUMENT><TYPE>INFORMATION TABLE<SEQUENCE>2<FILENAME>table.xml<TEXT><XML>
{TABLE.format(rows=body)}
</XML></TEXT></DOCUMENT>
</SEC-DOCUMENT>"""


ROWS = [("APPLE INC", "037833100", 2_100_000, 10_000), ("Microsoft corp", "594918104", 4_500_000, 10_000)]


def test_parse_reads_a_table_whose_namespace_is_declared_after_xsi():
    f = parse_13f(report(ROWS))
    assert (f.form, f.filer_cik, f.filer_name, f.period, f.filed) == ("13F-HR", 444444, "Example Capital LLC", "2024-06-30", "2024-08-14")
    assert (f.schema_version, f.declared_entries, f.declared_value, f.report_type) == ("X0202", 2, 6_600_000, "13F HOLDINGS REPORT")
    assert len(f.holdings) == 2 and f.checks == () and not f.is_amendment
    h = f.holdings[1]
    assert (h.cusip, h.value, h.shares, h.sh_prn, h.discretion, h.vote_sole, h.put_call) == (
        "594918104", 4_500_000, 10_000, "SH", "SOLE", 10_000, None)
    assert f.value_unit.unit == "dollars" and holding_rows(f)[0]["value_usd"] == 2_100_000


def test_checks_flag_a_table_that_does_not_match_its_summary_page():
    f = parse_13f(report(ROWS, entries=9, total=579_182_557))
    assert set(f.checks) == {"declared_entries_mismatch", "declared_value_mismatch"}
    nil = parse_13f(report([("NONE", "000000000", 0, 0)], entries=0, total=0))
    assert "nil_table" in nil.checks


def test_amendment_fields():
    amend = "<isAmendment>true</isAmendment><amendmentNo>2</amendmentNo><amendmentInfo><amendmentType>RESTATEMENT</amendmentType></amendmentInfo>"
    f = parse_13f(report(ROWS, form="13F-HR/A", amend=amend))
    assert f.is_amendment and (f.amendment_no, f.amendment_type) == (2, "RESTATEMENT")


def test_no_primary_document_is_none():
    assert parse_13f("<SEC-DOCUMENT>nothing</SEC-DOCUMENT>") is None


def _filings(*rows):
    return pd.DataFrame(rows, columns=["accession", "filer_cik", "period", "form", "filed", "amendment_type"])


def test_resolve_amendments():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None),
                 ("a2", 1, p, "13F-HR/A", "2024-08-05", "NEW HOLDINGS"),
                 ("b1", 2, p, "13F-HR", "2024-08-01", None),
                 ("b2", 2, p, "13F-HR/A", "2024-08-02", "NEW HOLDINGS"),
                 ("b3", 2, p, "13F-HR/A", "2024-08-09", "RESTATEMENT"),
                 ("c0", 3, p, "13F-HR/A", "2024-08-03", None))
    assert resolve_amendments(f) == ["a1", "a2", "b3", "c0"]


def test_new_holdings_that_repeats_the_report_is_a_restatement():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None), ("a2", 1, p, "13F-HR/A", "2024-09-01", "NEW HOLDINGS"))
    base = [("a1", f"C{i}", 100.0 + i, 10.0) for i in range(20)]
    rows = pd.DataFrame(base + [("a2", c, s, v) for _, c, s, v in base] + [("a2", "NEW", 5.0, 1.0)],
                        columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, rows) == ["a2"]
    added = pd.DataFrame(base + [("a2", "NEW", 5.0, 1.0)], columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, added) == ["a1", "a2"]
    repriced = pd.DataFrame(base + [("a2", c, s, v * 1.1) for _, c, s, v in base[:12]]
                            + [("a2", f"N{i}", 1.0, 1.0) for i in range(8)], columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, repriced) == ["a2"]


def _holdings(filer, accession, scale=1.0):
    prices = {"C1": 100.0, "C2": 50.0, "C3": 20.0, "C4": 10.0}
    return [{"accession": accession, "filer_cik": filer, "cusip": c, "shares": 1000.0, "value": 1000 * px * scale,
             "sh_prn": "SH", "put_call": None} for c, px in prices.items()]


def test_check_units_flags_a_report_in_thousands():
    rows = pd.DataFrame([r for i in range(6) for r in _holdings(i, f"a{i}")] + _holdings(99, "thousands", 0.001))
    assert consensus_prices(rows)["C1"] == pytest.approx(100.0)
    v = check_units(rows).set_index("accession")
    assert v.loc["a0", "verdict"] == "ok" and v.loc["thousands", "verdict"] == "too_small"
    assert v.loc["thousands", "scale_guess"] == pytest.approx(1000)


def test_cusip_to_ticker():
    assert check_digit_ok("037833100") and not check_digit_ok("037833101") and not check_digit_ok("80851310R")
    assert ticker_for_cusip("037833100") == "AAPL"
    assert ticker_for_cusip(" 037833100 ".lower()) == "AAPL"
    assert ticker_for_cusip("037833101") is None and ticker_for_cusip(None) is None
    assert class_ticker("BRKB") == "BRK-B" and class_ticker("AAPL") == "AAPL"


def test_read_13f_dataset(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT\n"
        "A-1\t14-AUG-2024\t13F-HR\t444444\t30-JUN-2024\nA-0\t14-MAY-2024\t13F-HR\t444444\t31-MAR-2024\n"
        "N-1\t14-AUG-2024\t13F-NT\t555555\t30-JUN-2024\n")
    (tmp_path / "COVERPAGE.tsv").write_text(
        "ACCESSION_NUMBER\tISAMENDMENT\tAMENDMENTNO\tAMENDMENTTYPE\tFILINGMANAGER_NAME\tREPORTTYPE\n"
        "A-1\tN\t\t\tExample Capital\t13F HOLDINGS REPORT\nA-0\tN\t\t\tExample Capital\t13F HOLDINGS REPORT\n")
    (tmp_path / "SUMMARYPAGE.tsv").write_text(
        "ACCESSION_NUMBER\tTABLEENTRYTOTAL\tTABLEVALUETOTAL\nA-1\t1\t2100000\nA-0\t1\t1\n")
    (tmp_path / "INFOTABLE.tsv").write_text(
        "ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI\tVALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\tPUTCALL\t"
        "INVESTMENTDISCRETION\tOTHERMANAGER\tVOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE\n"
        "A-1\t7\tAPPLE INC\tCOM\t037833100\t\t2100000\t10000\tSH\t\tSOLE\t\t10000\t0\t0\n"
        "A-0\t3\tAPPLE INC\tCOM\t037833100\t\t1\t10\tSH\t\tSOLE\t\t10\t0\t0\n")
    filings, rows = read_13f_dataset(tmp_path, period="2024-06-30")
    assert list(filings.accession) == ["A-1"] and filings.iloc[0].filer_cik == 444444
    assert list(rows.accession) == ["A-1"] and rows.iloc[0].value == 2_100_000 and rows.iloc[0].seq == 0
    assert rows.iloc[0].value_usd == 2_100_000
    _, older = read_13f_dataset(tmp_path, period="2024-03-31")
    assert older.iloc[0].value == 1 and older.iloc[0].value_usd == 1000  # a sub-dollar implied price: thousands


def test_amendments_ignore_notices_and_refuse_other_forms():
    p = "2024-06-30"
    f = _filings(("h1", 1, p, "13F-HR", "2024-08-01", None), ("n1", 1, p, "13F-NT", "2024-08-02", None))
    assert resolve_amendments(f) == ["h1"]
    with pytest.raises(ValueError):
        resolve_amendments(_filings(("x", 1, p, "10-K", "2024-08-01", None)))


def test_a_report_never_votes_on_its_own_consensus():
    # Five filers in dollars and one in thousands: with its own vote the median would still read dollars, but
    # each dollar filer must be judged against the others only, and the outlier against the five.
    rows = pd.DataFrame([r for i in range(5) for r in _holdings(i, f"a{i}")] + _holdings(99, "thousands", 0.001))
    v = check_units(rows).set_index("accession")
    assert v.loc["thousands", "verdict"] == "too_small"
    assert (v.drop(index="thousands").verdict == "ok").all()


def test_a_truncated_information_table_is_no_table():
    text = report(ROWS)
    cut = text[:text.index("</ns1:informationTable>")]
    f = parse_13f(cut)
    assert f is not None and not f.has_information_table and f.holdings == ()


def test_a_short_new_holdings_amendment_never_replaces_the_report():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None), ("a2", 1, p, "13F-HR/A", "2024-09-01", "NEW HOLDINGS"))
    cols = ["accession", "cusip", "shares", "value", "sh_prn", "put_call"]
    base = [("a1", f"C{i}", 100.0 + i, 10.0, "SH", None) for i in range(200)]
    correction = pd.DataFrame(base + [("a2", "C0", 100.0, 12.0, "SH", None)], columns=cols)
    assert resolve_amendments(f, correction) == ["a1", "a2"]
    call = pd.DataFrame(base[:1] + [("a2", "C0", 100.0, 1.0, "SH", "Call")], columns=cols)
    assert resolve_amendments(f, call) == ["a1", "a2"]


def test_a_new_holdings_amendment_that_refiles_an_earlier_one_replaces_it():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None), ("a2", 1, p, "13F-HR/A", "2024-09-01", "NEW HOLDINGS"),
                 ("a3", 1, p, "13F-HR/A", "2024-09-05", "NEW HOLDINGS"))
    added = [(f"N{i}", 5.0 + i, 1.0) for i in range(5)]
    rows = pd.DataFrame([("a1", f"C{i}", 100.0 + i, 10.0) for i in range(20)] + [("a2", *r) for r in added]
                        + [("a3", *r) for r in added], columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, rows) == ["a1", "a3"]


def test_a_new_holdings_amendment_that_refiles_several_earlier_ones_replaces_them_all():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None),
                 *[(f"a{i}", 1, p, "13F-HR/A", f"2024-09-0{i}", "NEW HOLDINGS") for i in (2, 3, 4)])
    first, second = [(f"N{i}", 5.0 + i, 1.0) for i in range(10)], [(f"M{i}", 5.0 + i, 1.0) for i in range(10)]
    rows = pd.DataFrame([("a1", f"C{i}", 100.0 + i, 10.0) for i in range(100)] + [("a2", *r) for r in first]
                        + [("a3", *r) for r in second] + [("a4", *r) for r in first + second],
                        columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, rows) == ["a1", "a4"]


def test_a_new_holdings_amendment_that_refiles_only_an_addition_keeps_the_original():
    p = "2024-06-30"
    f = _filings(("a1", 1, p, "13F-HR", "2024-08-01", None),
                 *[(f"a{i}", 1, p, "13F-HR/A", f"2024-09-0{i}", "NEW HOLDINGS") for i in (2, 3)])
    added = [(f"N{i}", 5.0 + i, 1.0) for i in range(10)]
    rows = pd.DataFrame([("a1", f"C{i}", 100.0 + i, 10.0) for i in range(3)] + [("a2", *r) for r in added]
                        + [("a3", *r) for r in added], columns=["accession", "cusip", "shares", "value"])
    assert resolve_amendments(f, rows) == ["a1", "a3"]


def test_reports_filed_the_same_day_go_original_first():
    p = "2026-03-31"
    f = _filings(("0000924171-26-000004", 1, p, "13F-HR/A", "2026-05-12", "RESTATEMENT"),
                 ("0001081198-26-000008", 1, p, "13F-HR", "2026-05-12", None)).assign(amendment_no=[1, None])
    assert resolve_amendments(f) == ["0000924171-26-000004"]


def test_check_units_calls_a_split_report_mixed_and_judges_every_report():
    peers = [r for i in range(6) for r in _holdings(i, f"a{i}")]
    split = [r | {"value": r["value"] * 0.001} for r in _holdings(98, "split")[:3]] + _holdings(98, "split")[3:]
    option = [{"accession": "option", "filer_cik": 97, "cusip": "C1", "shares": 10.0, "value": 5.0, "sh_prn": "SH",
               "put_call": "Call"}]
    v = check_units(pd.DataFrame(peers + split + option)).set_index("accession")
    assert v.loc["split", "verdict"] == "mixed" and v.loc["split", "scale_guess"] == 1
    assert v.loc["option", "verdict"] == "undetermined" and v.loc["option", "n_cmp"] == 0


def test_a_report_with_exactly_a_fifth_of_its_rows_at_the_right_scale_is_mixed():
    c5 = [{"accession": f"a{i}", "filer_cik": i, "cusip": "C5", "shares": 1000.0, "value": 5000.0, "sh_prn": "SH",
           "put_call": None} for i in range(6)]
    peers = [r for i in range(6) for r in _holdings(i, f"a{i}")] + c5
    edge = _holdings(98, "edge", 0.001) + [c5[0] | {"accession": "edge", "filer_cik": 98}]
    v = check_units(pd.DataFrame(peers + edge)).set_index("accession")
    assert v.loc["edge", "frac_low"] == pytest.approx(0.8) and v.loc["edge", "verdict"] == "mixed"


def test_a_data_set_report_filed_under_the_dollar_schema_reads_dollars(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT\n"
        "L-1\t10-DEC-2025\t13F-HR/A\t444444\t30-SEP-2022\nO-1\t14-NOV-2022\t13F-HR\t444444\t30-SEP-2022\n")
    (tmp_path / "COVERPAGE.tsv").write_text(
        "ACCESSION_NUMBER\tISAMENDMENT\tAMENDMENTNO\tAMENDMENTTYPE\tFILINGMANAGER_NAME\tREPORTTYPE\n"
        "L-1\tY\t1\tNEW HOLDINGS\tExample Capital\t13F HOLDINGS REPORT\n"
        "O-1\tN\t\t\tExample Capital\t13F HOLDINGS REPORT\n")
    (tmp_path / "SUMMARYPAGE.tsv").write_text("ACCESSION_NUMBER\tTABLEENTRYTOTAL\tTABLEVALUETOTAL\nL-1\t1\t1\nO-1\t1\t1\n")
    (tmp_path / "INFOTABLE.tsv").write_text(
        "ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI\tVALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\tPUTCALL\t"
        "INVESTMENTDISCRETION\tOTHERMANAGER\tVOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE\n"
        "L-1\t1\tAPPLE INC\tCOM\t037833100\t\t2100000\t10000\tSH\t\tSOLE\t\t10000\t0\t0\n"
        "O-1\t1\tAPPLE INC\tCOM\t037833100\t\t2100\t10000\tSH\t\tSOLE\t\t10000\t0\t0\n")
    _, rows = read_13f_dataset(tmp_path, period="2022-09-30")
    usd = rows.set_index("accession").value_usd
    assert usd["L-1"] == 2_100_000 and usd["O-1"] == 2_100_000


def test_summary_numbers_past_int_range_read_as_none():
    assert parse_13f(report(ROWS, entries="1e999")).declared_entries is None
