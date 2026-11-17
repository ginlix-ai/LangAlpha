from dataclasses import replace

import pandas as pd
import pytest

from edgar_engine.ownership import (belongs_to_issuer, insider_statistics, parse_ownership, read_insider_dataset,
                                    transaction_rows)

LINE = """<nonDerivativeTransaction>
  <securityTitle><value>Common Stock</value></securityTitle>
  <transactionDate><value>{date}</value></transactionDate>
  <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>{code}</transactionCode>
    <equitySwapInvolved>0</equitySwapInvolved></transactionCoding>
  <transactionAmounts><transactionShares><value>{shares}</value></transactionShares>
    <transactionPricePerShare><value>{price}</value><footnoteId id="F1"/></transactionPricePerShare>
    <transactionAcquiredDisposedCode><value>{ad}</value></transactionAcquiredDisposedCode></transactionAmounts>
  <postTransactionAmounts><sharesOwnedFollowingTransaction><value>{after}</value></sharesOwnedFollowingTransaction>
  </postTransactionAmounts>
  <ownershipNature><directOrIndirectOwnership><value>{di}</value></directOrIndirectOwnership>{nature}</ownershipNature>
</nonDerivativeTransaction>"""


def line(date="2024-05-02", code="S", shares="100", price="10.5", ad="D", after="900", di="D", nature=""):
    n = f"<natureOfOwnership><value>{nature}</value></natureOfOwnership>" if nature else ""
    return LINE.format(date=date, code=code, shares=shares, price=price, ad=ad, after=after, di=di, nature=n)


def document(lines, *, form="4", issuer="0000111111", aff="1", derivative="", holdings=""):
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0508</schemaVersion><documentType>{form}</documentType><periodOfReport>2024-05-02</periodOfReport>
  <aff10b5One>{aff}</aff10b5One>
  <issuer><issuerCik>{issuer}</issuerCik><issuerName>Example Corp</issuerName><issuerTradingSymbol>EXC</issuerTradingSymbol></issuer>
  <reportingOwner><reportingOwnerId><rptOwnerCik>0000222222</rptOwnerCik><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>true</isOfficer><officerTitle>Chief Executive Officer</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>{''.join(lines)}{holdings}</nonDerivativeTable>
  <derivativeTable>{derivative}</derivativeTable>
  <footnotes><footnote id="F1">Weighted average price.</footnote><footnote id="F2">Vests in   three
  installments.</footnote></footnotes>
</ownershipDocument>"""


def submission(doc, accession="0000111111-24-000001", form="4", filed="20240506"):
    return f"""<SEC-DOCUMENT>{accession}.txt : {filed}
<SEC-HEADER>
ACCESSION NUMBER:\t\t{accession}
CONFORMED SUBMISSION TYPE:\t{form}
CONFORMED PERIOD OF REPORT:\t20240502
FILED AS OF DATE:\t\t{filed}
</SEC-HEADER>
<DOCUMENT>
<TYPE>{form}
<SEQUENCE>1
<FILENAME>form4.xml
<TEXT>
<XML>
{doc}
</XML>
</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>"""


DERIVATIVE = """<derivativeTransaction>
  <securityTitle><value>Stock Option (right to buy)</value></securityTitle>
  <conversionOrExercisePrice><value>12.25</value></conversionOrExercisePrice>
  <transactionDate><value>2024-05-01-05:00</value></transactionDate>
  <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>M</transactionCode></transactionCoding>
  <transactionAmounts><transactionShares><value>50</value></transactionShares>
    <transactionPricePerShare><value>0</value></transactionPricePerShare>
    <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode></transactionAmounts>
  <exerciseDate><footnoteId id="F2"/></exerciseDate>
  <expirationDate><value>2030-01-01</value></expirationDate>
  <underlyingSecurity><underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
    <underlyingSecurityShares><value>50</value></underlyingSecurityShares></underlyingSecurity>
  <postTransactionAmounts><sharesOwnedFollowingTransaction><value>150</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
  <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
</derivativeTransaction>"""

HOLDING = """<nonDerivativeHolding><securityTitle><value>Common Stock</value></securityTitle>
  <postTransactionAmounts><sharesOwnedFollowingTransaction><value>2,500</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
  <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
  <natureOfOwnership><value>By Trust</value></natureOfOwnership></ownershipNature></nonDerivativeHolding>"""


def test_parse_reads_every_line_as_filed():
    doc = document([line(), line(), line(code="P", ad="A", shares="10", di="I", nature="By Spouse")],
                   derivative=DERIVATIVE, holdings=HOLDING)
    f = parse_ownership(submission(doc))
    assert (f.accession, f.form, f.filed, f.period) == ("0000111111-24-000001", "4", "2024-05-06", "2024-05-02")
    assert (f.issuer_cik, f.issuer_ticker, f.aff10b5_one) == (111111, "EXC", True)
    assert f.owners[0].relationship == "director; officer: Chief Executive Officer"
    assert [t.table for t in f.transactions] == ["nonderivative"] * 3 + ["derivative"]
    assert [t.seq for t in f.transactions] == [0, 1, 2, 0]
    a, b, c, d = f.transactions
    assert replace(a, seq=1) == b and (a.code, a.shares, a.price, a.acquired_disposed, a.owned_after) == ("S", 100, 10.5, "D", 900)
    assert a.footnote_ids == {"price": ("F1",)}
    assert (c.direct_indirect, c.nature_of_ownership) == ("I", "By Spouse")
    assert (d.date, d.price, d.exercise_price, d.exercise_date, d.expiration_date) == ("2024-05-01", 0, 12.25, None, "2030-01-01")
    assert d.footnote_ids["exercise_date"] == ("F2",)
    assert f.footnotes["F2"] == "Vests in three installments."
    assert len(f.holdings) == 1 and (f.holdings[0].owned, f.holdings[0].nature_of_ownership) == (2500, "By Trust")


def test_parse_reads_bare_xml_and_none_without_document():
    f = parse_ownership(document([line()]).encode())
    assert f.filed is None and f.accession is None and len(f.transactions) == 1
    assert parse_ownership("<html>no ownership here</html>") is None


def test_unset_10b5_1_flag_is_none():
    f = parse_ownership(document([line()]).replace("<aff10b5One>1</aff10b5One>", ""))
    assert f.aff10b5_one is None


def test_issuer_filter_uses_the_parsed_issuer():
    f = parse_ownership(document([line()], issuer="0000333333"))
    assert belongs_to_issuer(f, 333333) and belongs_to_issuer(f, "0000333333")
    assert not belongs_to_issuer(f, 222222)


def test_transaction_rows_carry_filing_columns():
    rows = transaction_rows(parse_ownership(submission(document([line()]))))
    assert rows[0]["issuer_cik"] == 111111 and rows[0]["owner_cik"] == 222222 and rows[0]["aff10b5_one"] is True
    assert rows[0]["derivative"] is False and rows[0]["footnote_ids"] == {"price": ["F1"]}


def _rows(*docs):
    out = []
    for i, (form, lines) in enumerate(docs):
        out += transaction_rows(parse_ownership(submission(document(lines, form=form), accession=f"0000111111-24-00000{i}", form=form)))
    return pd.DataFrame(out)


def test_statistics_definition():
    rows = _rows(
        ("4", [line(), line(), line(code="P", ad="A", shares="40"), line(code="A", ad="A", shares="0")]),
        ("4/A", [line(shares="60")]),
        ("4", [line()]),                      # the same line in another filing counts again
        ("5", [line(shares="1000")]),         # Form 5 is not counted
        ("4", [line(date="2024-07-01", code="P", ad="A", shares="7")]),
    )
    s = insider_statistics(rows)
    assert list(zip(s.year, s.quarter)) == [(2024, 3), (2024, 2)]
    q2 = s.iloc[1]
    assert (q2.acquired_transactions, q2.disposed_transactions) == (2, 3)
    assert (q2.total_acquired, q2.total_disposed) == (40, 260)
    assert (q2.total_purchases, q2.total_sales) == (1, 3)
    assert q2.acquired_disposed_ratio == pytest.approx(0.6667)
    assert q2.average_acquired == 20 and q2.average_disposed == pytest.approx(260 / 3)
    q3 = s.iloc[0]
    assert (q3.disposed_transactions, q3.acquired_disposed_ratio, q3.average_disposed) == (0, 0.0, 0.0)


def test_statistics_of_nothing_is_empty():
    assert insider_statistics(pd.DataFrame(columns=["accession", "form", "issuer_cik"])).empty


def test_read_insider_dataset(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\tAFF10B5ONE\n"
        "0000111111-24-000001\t06-MAY-2024\t02-MAY-2024\t4\t111111\tExample Corp\tEXC\t1\n")
    (tmp_path / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\tRPTOWNER_TXT\n"
        "0000111111-24-000001\t222222\tDoe Jane\tDirector,Officer\tCEO\t\n")
    tcols = ("ACCESSION_NUMBER\t{sk}\tSECURITY_TITLE\tTRANS_DATE\tDEEMED_EXECUTION_DATE\tTRANS_FORM_TYPE\tTRANS_CODE\t"
             "EQUITY_SWAP_INVOLVED\tTRANS_SHARES\tTRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\tSHRS_OWND_FOLWNG_TRANS\t"
             "VALU_OWND_FOLWNG_TRANS\tDIRECT_INDIRECT_OWNERSHIP\tNATURE_OF_OWNERSHIP\n")
    (tmp_path / "NONDERIV_TRANS.tsv").write_text(
        tcols.format(sk="NONDERIV_TRANS_SK")
        + "0000111111-24-000001\t2\tCommon\t02-MAY-2024\t\t4\tS\t0\t100\t10.5\tD\t900\t\tD\t\n"
        + "0000111111-24-000001\t1\tCommon\t02-MAY-2024\t\t4\tP\t0\t40\t10\tA\t1000\t\tI\tBy Trust\n")
    (tmp_path / "DERIV_TRANS.tsv").write_text(tcols.format(sk="DERIV_TRANS_SK"))
    d = read_insider_dataset(tmp_path)
    assert list(d.code) == ["P", "S"] and list(d.seq) == [0, 1]
    r = d.iloc[0]
    assert (r.form, r.filed, r.issuer_cik, r.owner_cik, r.aff10b5_one) == ("4", "2024-05-06", 111111, 222222, True)
    assert r.owner_relationship == "director; officer: CEO" and r.nature_of_ownership == "By Trust"
    assert insider_statistics(d).iloc[0].total_purchases == 1


class _Response:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


_WHOLE, _CUT = b"<SEC-DOCUMENT>x</SEC-DOCUMENT>\n", b"<SEC-DOCUMENT>x</DOC"


@pytest.mark.parametrize("body,refetched,complete", [(_WHOLE, None, True), (_CUT, _CUT, False), (_CUT, _WHOLE, True)])
def test_fetch_never_caches_a_truncated_submission(monkeypatch, tmp_path, body, refetched, complete):
    import edgar.httprequests

    from edgar_engine import submission
    from edgar_engine.cache import LocalDirStore, SubmissionCache
    from edgar_engine.submission import TruncatedSubmission, fetch_submission

    monkeypatch.setattr(edgar.httprequests, "get_with_retry", lambda url: _Response(body))
    monkeypatch.setattr(submission, "_fetch_uncached", lambda url: refetched)
    cache = SubmissionCache(LocalDirStore(tmp_path))
    if complete:
        assert fetch_submission(1, "0000000001-24-000001", cache) == _WHOLE
    else:
        with pytest.raises(TruncatedSubmission):
            fetch_submission(1, "0000000001-24-000001", cache)
    assert (cache.get("0000000001-24-000001") is not None) == complete



def test_a_document_cut_short_or_not_well_formed_is_no_document():
    doc = document([line(), line(code="P", ad="A")])
    assert parse_ownership(submission(doc)[:-400]) is None
    assert parse_ownership(doc.replace("Example Corp", "AT&T INC")) is None


def test_holding_footnotes_use_the_holding_field_names():
    held = HOLDING.replace("<value>2,500</value>", '<value>2,500</value><footnoteId id="F1"/>')
    f = parse_ownership(document([line()], holdings=held))
    assert f.holdings[0].footnote_ids == {"owned": ("F1",)}


def test_statistics_count_option_series_that_differ_only_in_strike():
    other = DERIVATIVE.replace("12.25", "30.00").replace("2030-01-01", "2032-01-01")
    f = parse_ownership(submission(document([], derivative=DERIVATIVE + other)))
    assert insider_statistics(pd.DataFrame(transaction_rows(f))).iloc[0].disposed_transactions == 2


def test_statistics_need_an_issuer_and_tolerate_missing_forms():
    rows = pd.DataFrame(transaction_rows(parse_ownership(submission(document([line()])))))
    mixed = pd.concat([rows, rows.assign(form=float("nan"), accession="x")], ignore_index=True)
    assert insider_statistics(mixed).iloc[0].disposed_transactions == 1
    with pytest.raises(ValueError):
        insider_statistics(rows.assign(issuer_cik=None))
    with pytest.raises(ValueError, match="accession"):
        insider_statistics(rows.assign(accession=None))


def test_insider_data_set_footnotes_and_a_missing_10b5_1_column(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\n"
        "0000111111-22-000001\t06-MAY-2022\t02-MAY-2022\t4\t111111\tExample Corp\tEXC\n")
    (tmp_path / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\tRPTOWNER_TXT\n"
        "0000111111-22-000001\t222222\tDoe Jane\tDirector\t\t\n")
    tcols = ("ACCESSION_NUMBER\t{sk}\tSECURITY_TITLE\tTRANS_DATE\tDEEMED_EXECUTION_DATE\tTRANS_FORM_TYPE\tTRANS_CODE\t"
             "EQUITY_SWAP_INVOLVED\tTRANS_SHARES\tTRANS_SHARES_FN\tTRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\t"
             "SHRS_OWND_FOLWNG_TRANS\tVALU_OWND_FOLWNG_TRANS\tDIRECT_INDIRECT_OWNERSHIP\tNATURE_OF_OWNERSHIP\n")
    grant = "0000111111-22-000001\t{sk}\tUnits\t02-MAY-2022\t\t4\tA\t0\t17314\t{fn}\t0\tA\t17314\t\tD\t\n"
    (tmp_path / "NONDERIV_TRANS.tsv").write_text(
        tcols.format(sk="NONDERIV_TRANS_SK") + grant.format(sk=1, fn="F1") + grant.format(sk=2, fn="F2, F3"))
    (tmp_path / "DERIV_TRANS.tsv").write_text(tcols.format(sk="DERIV_TRANS_SK"))
    d = read_insider_dataset(tmp_path)
    assert list(d.footnote_ids) == [{"shares": ["F1"]}, {"shares": ["F2", "F3"]}]
    assert d.aff10b5_one.isna().all()
