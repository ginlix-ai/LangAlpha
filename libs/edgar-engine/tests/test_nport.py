import math

import pandas as pd
from edgar_engine import _datasets
from edgar_engine.nport import (
    EQUITY,
    filing_row,
    holding_rows,
    latest_positions,
    parse_nport,
    read_nport_dataset,
    security_cusips,
)

PRIMARY = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/nport" xmlns:com="http://www.sec.gov/edgar/common"
  xmlns:ncom="http://www.sec.gov/edgar/nportcommon" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <headerData><submissionType>{form}</submissionType>
    <filerInfo><filer><issuerCredentials><cik>0000777777</cik><ccc>XXXXXXXX</ccc></issuerCredentials></filer>
      <seriesClassInfo><seriesId>S000012345</seriesId><classId>C000000001</classId><classId>C000000002</classId>
      </seriesClassInfo></filerInfo></headerData>
  <formData>
    <genInfo><regName>Example  Funds Trust</regName><regFileNumber>811-00001</regFileNumber><regCik>0000777777</regCik>
      <regLei>LEI0000000000000REG1</regLei><seriesName>Example Index Fund</seriesName><seriesId>S000012345</seriesId>
      <seriesLei>LEI000000000000SER01</seriesLei><repPdEnd>2026-12-31</repPdEnd><repPdDate>2026-09-30</repPdDate>
      <isFinalFiling>N</isFinalFiling></genInfo>
    <fundInfo><totAssets>1000500.00</totAssets><totLiabs>500.00</totLiabs><netAssets>1000000.00</netAssets></fundInfo>
    <invstOrSecs>
      <invstOrSec><name>Example Corp</name><lei>LEI00000000000ISSUE1</lei><title>Example Corp Common</title>
        <cusip>123456789</cusip><identifiers><isin value="US1234567890"/><ticker value="EXMP"/></identifiers>
        <balance>1000.00000000</balance><units>NS</units><curCd>USD</curCd><valUSD>50000.00</valUSD>
        <pctVal>5.000000000000</pctVal><payoffProfile>Long</payoffProfile><assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat><invCountry>US</invCountry><isRestrictedSec>N</isRestrictedSec>
        <fairValLevel>1</fairValLevel></invstOrSec>
      <invstOrSec><name>Bund Issuer</name><lei>N/A</lei><title>Bund 2.5% 2030</title><cusip>N/A</cusip>
        <identifiers><isin value="DE0001102101"/><other otherDesc="Inhouse Asset ID" value="ABC123"/>
          <other otherDesc="SEDOL" value="B000001"/></identifiers>
        <balance>200000.00</balance><units>PA</units><currencyConditional curCd="EUR" exchangeRt="0.92"/>
        <valUSD>210000.00</valUSD><pctVal>21.0</pctVal><payoffProfile>Long</payoffProfile><assetCat>DBT</assetCat>
        <issuerCat>NUSS</issuerCat><invCountry>DE</invCountry><isRestrictedSec>N</isRestrictedSec>
        <fairValLevel>2</fairValLevel></invstOrSec>
      <invstOrSec><name>N/A</name><lei>N/A</lei><title>Index Future</title><cusip>N/A</cusip>
        <identifiers><ticker value="ESZ6"/></identifiers><balance>3</balance><units>OU</units>
        <descOthUnits>Contracts</descOthUnits><curCd>USD</curCd><valUSD>-1200.50</valUSD><pctVal>-0.12005</pctVal>
        <payoffProfile>N/A</payoffProfile><assetConditional assetCat="OTHER" desc="Futures"/>
        <issuerConditional desc="Exchange" issuerCat="OTHER"/><invCountry>US</invCountry>
        <isRestrictedSec>N</isRestrictedSec><fairValLevel>1</fairValLevel>
        <derivativeInfo><futrDeriv derivCat="FUT"><payOffProf>Long</payOffProf></futrDeriv></derivativeInfo>
      </invstOrSec>
      <invstOrSec><name>Pref Bank</name><lei>LEI00000000000ISSUE2</lei><title>Pref Bank Pfd Series A</title>
        <cusip>99999a105</cusip><balance>-500</balance><units>NS</units><curCd>USD</curCd><valUSD>-12500</valUSD>
        <pctVal>-1.25</pctVal><payoffProfile>Short</payoffProfile><assetCat>EP</assetCat><issuerCat>CORP</issuerCat>
        <invCountry>US</invCountry><isRestrictedSec>Y</isRestrictedSec><fairValLevel>2</fairValLevel></invstOrSec>
    </invstOrSecs>
  </formData>
</edgarSubmission>"""


def submission(form="NPORT-P", accession="0000777777-26-000009"):
    return f"""<SEC-DOCUMENT>
<SEC-HEADER>
ACCESSION NUMBER:\t\t{accession}
CONFORMED SUBMISSION TYPE:\t{form}
CONFORMED PERIOD OF REPORT:\t20260930
FILED AS OF DATE:\t\t20261125
</SEC-HEADER>
<DOCUMENT><TYPE>{form}<SEQUENCE>1<FILENAME>primary_doc.xml<TEXT><XML>
{PRIMARY.format(form=form)}
</XML></TEXT></DOCUMENT>
<DOCUMENT><TYPE>EX-99<SEQUENCE>2<FILENAME>schedule.htm<TEXT><html><body><table><tr><td>x</td></tr></table></body></html>
</TEXT></DOCUMENT>
</SEC-DOCUMENT>"""


def test_parse_reads_the_fund_and_every_investment_in_document_order():
    f = parse_nport(submission())
    assert filing_row(f) == {
        "accession": "0000777777-26-000009", "form": "NPORT-P", "filed": "2026-11-25", "cik": 777777,
        "registrant": "Example Funds Trust", "registrant_lei": "LEI0000000000000REG1", "file_number": "811-00001",
        "series_id": "S000012345", "series_name": "Example Index Fund", "series_lei": "LEI000000000000SER01",
        "class_ids": "C000000001;C000000002", "report_date": "2026-09-30", "fiscal_year_end": "2026-12-31",
        "total_assets": 1000500.0, "total_liabilities": 500.0, "net_assets": 1000000.0, "is_final": False}
    assert [h.seq for h in f.holdings] == [0, 1, 2, 3] and not f.is_amendment
    eq, bund, fut, pref = f.holdings
    assert (eq.cusip, eq.isin, eq.ticker, eq.balance, eq.units, eq.value_usd, eq.pct_val, eq.asset_cat,
            eq.issuer_cat, eq.payoff_profile, eq.restricted, eq.fair_value_level) == (
        "123456789", "US1234567890", "EXMP", 1000.0, "NS", 50000.0, 5.0, "EC", "CORP", "Long", False, "1")
    assert (bund.currency, bund.exchange_rate, bund.other_ids) == ("EUR", 0.92, "Inhouse Asset ID:ABC123;SEDOL:B000001")
    assert (fut.units, fut.other_units, fut.asset_cat, fut.other_asset, fut.issuer_cat, fut.other_issuer,
            fut.derivative_cat, fut.value_usd) == ("OU", "Contracts", "OTHER", "Futures", "OTHER", "Exchange", "FUT",
                                                   -1200.5)
    assert (pref.cusip, pref.payoff_profile, pref.restricted, pref.isin) == ("99999A105", "Short", True, None)


def test_holding_rows_keep_only_the_asset_categories_asked_for():
    f = parse_nport(submission())
    assert [r["cusip"] for r in holding_rows(f, EQUITY)] == ["123456789", "99999A105"]
    rows = holding_rows(f)
    assert len(rows) == 4 and rows[0]["series_id"] == "S000012345" and rows[0]["report_date"] == "2026-09-30"


def test_parse_refuses_what_is_not_a_complete_report():
    assert parse_nport("<SEC-DOCUMENT>no xml here</SEC-DOCUMENT>") is None
    cut = submission().split("</invstOrSecs>")[0]
    assert parse_nport(cut) is None
    assert parse_nport(submission(form="NPORT-P/A")).is_amendment
    assert parse_nport(submission(form="NPORT-NP")) is None


H_COLS = ["ACCESSION_NUMBER", "HOLDING_ID", "ISSUER_NAME", "ISSUER_LEI", "ISSUER_TITLE", "ISSUER_CUSIP", "BALANCE",
          "UNIT", "OTHER_UNIT_DESC", "CURRENCY_CODE", "CURRENCY_VALUE", "EXCHANGE_RATE", "PERCENTAGE",
          "PAYOFF_PROFILE", "ASSET_CAT", "OTHER_ASSET", "ISSUER_TYPE", "OTHER_ISSUER", "INVESTMENT_COUNTRY",
          "IS_RESTRICTED_SECURITY", "FAIR_VALUE_LEVEL", "DERIVATIVE_CAT"]
ACC = "0000777777-26-000009"


def _tsv(path, cols, rows):
    path.write_text("\t".join(cols) + "\n" + "".join("\t".join(r) + "\n" for r in rows))


def dataset(tmp_path):
    """The report above as SEC's data set holds it (holding keys out of document order on disk), plus a notice and a
    fund with no series."""
    _tsv(tmp_path / "SUBMISSION.tsv", ["ACCESSION_NUMBER", "FILING_DATE", "FILE_NUM", "SUB_TYPE",
                                       "REPORT_ENDING_PERIOD", "REPORT_DATE", "IS_LAST_FILING"],
         [[ACC, "25-NOV-2026", "", "NPORT-P", "31-DEC-2026", "30-SEP-2026", "N"],
          ["0000888888-26-000001", "20-NOV-2026", "", "NPORT-P", "31-MAR-2027", "31-AUG-2026", "Y"],
          ["0000999999-26-000001", "20-NOV-2026", "", "NT NPORT-P", "", "", ""]])
    _tsv(tmp_path / "REGISTRANT.tsv", ["ACCESSION_NUMBER", "CIK", "REGISTRANT_NAME", "FILE_NUM", "LEI"],
         [[ACC, "0000777777", "Example  Funds Trust", "811-00001", "LEI0000000000000REG1"],
          ["0000888888-26-000001", "0000888888", "Closed End Income Fund", "811-00002", ""]])
    _tsv(tmp_path / "FUND_REPORTED_INFO.tsv", ["ACCESSION_NUMBER", "SERIES_NAME", "SERIES_ID", "SERIES_LEI",
                                               "TOTAL_ASSETS", "TOTAL_LIABILITIES", "NET_ASSETS"],
         [[ACC, "Example Index Fund", "S000012345", "LEI000000000000SER01", "1000500", "500", "1000000"],
          ["0000888888-26-000001", "Closed End Income Fund", "", "", "10", "0", "10"]])
    _tsv(tmp_path / "FUND_REPORTED_HOLDING.tsv", H_COLS, [
        [ACC, "100", "Bund Issuer", "N/A", "Bund 2.5% 2030", "N/A", "200000", "PA", "", "EUR", "210000", "0.92", "21",
         "Long", "DBT", "", "NUSS", "", "DE", "N", "2", ""],
        [ACC, "99", "Example Corp", "LEI00000000000ISSUE1", "Example Corp Common", "123456789", "1000", "NS", "",
         "USD", "50000", "", "5", "Long", "EC", "", "CORP", "", "US", "N", "1", ""],
        [ACC, "102", "Pref Bank", "LEI00000000000ISSUE2", "Pref Bank Pfd Series A", "99999a105", "-500", "NS", "",
         "USD", "-12500", "", "-1.25", "Short", "EP", "", "CORP", "", "US", "Y", "2", ""],
        # The future's nested reference instrument, flattened into a row of its own: no investment of Part C.
        [ACC, "1011", "N/A", "", "FX Contract", "", "1", "OU", "Contracts", "USD", "0", "", "0", "", "DFE", "", "",
         "", "", "", "", "FWD"],
        [ACC, "101", "N/A", "N/A", "Index Future", "N/A", "3", "OU", "Contracts", "USD", "-1200.5", "", "-.12005",
         "N/A", "OTHER", "Futures", "OTHER", "Exchange", "US", "N", "1", "FUT"],
        ["0000888888-26-000001", "7", "Example Corp", "", "Common", "123456789", "1", "NS", "", "USD", "50", "", "5",
         "Long", "EC", "", "CORP", "", "US", "N", "1", ""]])
    _tsv(tmp_path / "IDENTIFIERS.tsv", ["HOLDING_ID", "IDENTIFIERS_ID", "IDENTIFIER_ISIN", "IDENTIFIER_TICKER",
                                        "OTHER_IDENTIFIER", "OTHER_IDENTIFIER_DESC"],
         [["99", "2", "", "EXMP", "", ""], ["99", "1", "US1234567890", "", "", ""],
          ["100", "3", "DE0001102101", "", "", ""], ["100", "4", "", "", "ABC123", "Inhouse Asset ID"],
          ["100", "5", "", "", "B000001", "SEDOL"], ["101", "6", "", "ESZ6", "", ""]])
    return tmp_path


def _plain(rows):
    return [{k: (None if v is None or v is pd.NA or (isinstance(v, float) and math.isnan(v)) else v) for k, v in r.items()}
            for r in rows]


def test_data_set_reads_into_the_parse_columns_and_values(tmp_path, monkeypatch):
    monkeypatch.setattr(_datasets, "CHUNK_BYTES", 400)  # several blocks, each cut before the next is read
    filings, holdings = read_nport_dataset(dataset(tmp_path))
    f = parse_nport(submission())
    want = filing_row(f) | {"class_ids": None}
    assert _plain(filings[filings.accession == ACC].to_dict("records")) == [want]
    mine = holdings[holdings.accession == ACC]
    assert _plain(mine.to_dict("records")) == holding_rows(f)


def test_data_set_keeps_reports_only_and_a_fund_with_no_series(tmp_path):
    filings, holdings = read_nport_dataset(dataset(tmp_path), EQUITY)
    assert sorted(filings.accession) == [ACC, "0000888888-26-000001"]
    solo = filings[filings.accession == "0000888888-26-000001"].iloc[0]
    assert pd.isna(solo.series_id) and bool(solo.is_final) and solo.cik == 888888
    assert list(holdings.cusip) == ["123456789", "99999A105", "123456789"]
    assert list(holdings.seq) == [0, 1, 0]


def test_data_set_reads_only_the_accessions_asked_for(tmp_path):
    filings, holdings = read_nport_dataset(dataset(tmp_path), EQUITY, accessions=[ACC])
    assert list(filings.accession) == [ACC] and set(holdings.accession) == {ACC}


def test_data_set_and_parse_agree_on_seq_and_identifiers_trimmed_at_their_ends(tmp_path):
    path = dataset(tmp_path)
    ids = path / "IDENTIFIERS.tsv"
    ids.write_text(ids.read_text().replace("\tEXMP\t", "\t EXMP \t").replace("B000001\tSEDOL", " B0 00001 \t SEDOL "))
    f = parse_nport(submission().replace('<ticker value="EXMP"/>', '<ticker value=" EXMP "/>').replace(
        'otherDesc="SEDOL" value="B000001"', 'otherDesc=" SEDOL " value=" B0 00001 "'))
    for cats in (EQUITY, None):
        _, holdings = read_nport_dataset(path, cats)
        assert _plain(holdings[holdings.accession == ACC].to_dict("records")) == holding_rows(f, cats)
    assert [(r["seq"], r["ticker"]) for r in holding_rows(f, EQUITY)] == [(0, "EXMP"), (1, None)]
    assert holding_rows(f)[1]["other_ids"] == "Inhouse Asset ID:ABC123;SEDOL:B0 00001"


def _f(acc, series, rd, filed, form="NPORT-P", cik=1, final=False):
    return {"accession": acc, "form": form, "filed": filed, "cik": cik, "registrant": "Trust", "series_id": series,
            "series_name": "Fund", "report_date": rd, "net_assets": 1e6, "is_final": final}


def _h(acc, cusip, balance, isin=None, units="NS", payoff="Long", cat="EC", value=None):
    return {"accession": acc, "cusip": cusip, "isin": isin, "balance": balance, "units": units, "payoff_profile": payoff,
            "asset_cat": cat, "value_usd": value if value is not None else balance * 10, "pct_val": 1.0,
            "name": "Example Corp", "title": "Common"}


AAPL, MSFT = "037833100", "594918104"


def test_latest_positions_take_each_funds_latest_standing_report_and_the_one_before():
    filings = pd.DataFrame([
        _f("a-1", "S1", "2026-03-31", "2026-05-20"),
        _f("a-2", "S1", "2026-06-30", "2026-08-20"),
        _f("a-3", "S1", "2026-06-30", "2026-09-02", form="NPORT-P/A"),   # restates a-2
        _f("a-4", "S1", "2026-03-31", "2026-09-03", form="NPORT-P/A"),   # restates an older report only
        _f("b-1", None, "2026-05-31", "2026-07-25", cik=42),             # no series: the registrant is the fund
        _f("c-1", "S3", "2026-06-30", "2026-08-25", final=True),         # its final report
    ])
    holdings = pd.DataFrame([
        _h("a-1", AAPL, 100), _h("a-4", AAPL, 90),
        _h("a-2", AAPL, 1), _h("a-3", AAPL, 120), _h("a-3", AAPL, 30), _h("a-3", MSFT, 5),
        _h("b-1", "N/A", 7, isin="US5949181045"), _h("b-1", "N/A", 8, isin="GB0000000000"),
        _h("b-1", AAPL, 3, payoff="Short"), _h("b-1", AAPL, 4, units="PA"), _h("b-1", AAPL, 6, cat="DBT"),
        _h("c-1", AAPL, 50),
    ])
    pos = latest_positions(filings, holdings).sort_values(["fund", "cusip"]).reset_index(drop=True)
    assert list(zip(pos.fund, pos.cusip, pos.shares, pos.accession)) == [
        ("CIK0000000042", MSFT, 7.0, "b-1"), ("S1", AAPL, 150.0, "a-3"), ("S1", MSFT, 5.0, "a-3")]
    assert pos.shares_prior[1] == 90.0 and pd.isna(pos.shares_prior[2]) and pd.isna(pos.shares_prior[0])
    assert pos.prior_report_date[1] == "2026-03-31" and pd.isna(pos.prior_report_date[0])
    kept = latest_positions(filings, holdings, final_drops=False)
    assert "S3" in set(kept.fund)


def test_a_position_whose_values_are_all_missing_keeps_them_missing():
    nan = float("nan")
    holdings = pd.DataFrame([_h("a-1", AAPL, 10, value=nan), _h("a-1", AAPL, 5, value=nan),
                             _h("a-1", MSFT, 5, value=nan), _h("a-1", MSFT, 5)]).assign(pct_val=[nan, nan, 1.0, nan])
    pos = latest_positions(pd.DataFrame([_f("a-1", "S1", "2026-06-30", "2026-08-20")]), holdings).set_index("cusip")
    assert pd.isna(pos.value_usd[AAPL]) and pd.isna(pos.pct_val[AAPL])
    assert (pos.value_usd[MSFT], pos.pct_val[MSFT]) == (50.0, 1.0)


def test_an_amendment_filed_the_day_of_its_original_stands_whatever_the_accession_order():
    filings = pd.DataFrame([
        _f("z-9", "S1", "2026-06-30", "2026-08-20"),
        _f("a-1", "S1", "2026-06-30", "2026-08-20", form="NPORT-P/A"),
    ])
    holdings = pd.DataFrame([_h("z-9", AAPL, 1), _h("a-1", AAPL, 120)])
    pos = latest_positions(filings, holdings)
    assert list(zip(pos.cusip, pos.shares, pos.accession)) == [(AAPL, 120.0, "a-1")]


def test_security_cusips_fall_back_to_the_cusip_inside_a_us_or_canadian_isin():
    h = pd.DataFrame({"cusip": [AAPL, "N/A", "000000000", "037833101", None],
                      "isin": [None, "US5949181045", "CA0000000000", "US0378331005", "GB0002634946"]})
    assert security_cusips(h).tolist() == [AAPL, MSFT, None, AAPL, None]
