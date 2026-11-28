from types import SimpleNamespace

import pytest

from edgar_engine import api
from edgar_engine.ownership import RELATIONSHIPS, normalize_relationship, notice_rows, parse_form144

SECURITY = """<securitiesInformation><securitiesClassTitle>{cls}</securitiesClassTitle>
  <brokerOrMarketmakerDetails><name>{broker}</name><address><com:street1>1 Main St</com:street1></address></brokerOrMarketmakerDetails>
  <noOfUnitsSold>{units}</noOfUnitsSold><aggregateMarketValue>{value}</aggregateMarketValue>
  <noOfUnitsOutstanding>1000000</noOfUnitsOutstanding><approxSaleDate>{sale}</approxSaleDate>
  <securitiesExchangeName>{exchange}</securitiesExchangeName></securitiesInformation>"""
ACQUIRED = """<securitiesToBeSold><securitiesClassTitle>{cls}</securitiesClassTitle><acquiredDate>01/15/2020</acquiredDate>
  <natureOfAcquisitionTransaction>{nature}</natureOfAcquisitionTransaction><nameOfPersonfromWhomAcquired>Issuer</nameOfPersonfromWhomAcquired>
  <isGiftTransaction>N</isGiftTransaction><amountOfSecuritiesAcquired>{shares}</amountOfSecuritiesAcquired>
  <paymentDate>01/15/2020</paymentDate><natureOfPayment>Cash</natureOfPayment></securitiesToBeSold>"""
SOLD = """<securitiesSoldInPast3Months><sellerDetails><name>Doe Jane</name></sellerDetails>
  <securitiesClassTitle>{cls}</securitiesClassTitle><saleDate>{date}</saleDate>
  <amountOfSecuritiesSold>{shares}</amountOfSecuritiesSold><grossProceeds>{proceeds}</grossProceeds></securitiesSoldInPast3Months>"""


def security(cls="Common", broker="Broker One", units="1000", value="25000.00", sale="05/02/2024", exchange="NASDAQ"):
    return SECURITY.format(cls=cls, broker=broker, units=units, value=value, sale=sale, exchange=exchange)


def acquired(cls="Common", nature="Restricted Stock Vesting", shares="1000"):
    return ACQUIRED.format(cls=cls, nature=nature, shares=shares)


def sold(cls="Common", date="03/01/2024", shares="500", proceeds="12000.50"):
    return SOLD.format(cls=cls, date=date, shares=shares, proceeds=proceeds)


def document(blocks="", *, form="144", filer="0000222222", issuer="0000111111", previous="", relationships=("Officer",),
             plans=("01/02/2024",), nothing="N"):
    prev = f"<previousAccessionNumber>{previous}</previousAccessionNumber>" if previous else ""
    rels = "".join(f"<relationshipToIssuer>{r}</relationshipToIssuer>" for r in relationships)
    plan = "".join(f"<planAdoptionDate>{p}</planAdoptionDate>" for p in plans)
    return f"""<?xml version="1.0" encoding="UTF-8"?><edgarSubmission xmlns="http://www.sec.gov/edgar/ownership" xmlns:com="http://www.sec.gov/edgar/common">
  <headerData><submissionType>{form}</submissionType>{prev}
    <filerInfo><filer><filerCredentials><cik>{filer}</cik><ccc>XXXXXXXX</ccc></filerCredentials></filer></filerInfo></headerData>
  <formData>
    <issuerInfo><issuerCik>{issuer}</issuerCik><issuerName>EXAMPLE CORP</issuerName>
      <nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>Doe Jane</nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>
      <relationshipsToIssuer>{rels}</relationshipsToIssuer></issuerInfo>
    {blocks}
    <nothingToReportFlagOnSecuritiesSoldInPast3Months>{nothing}</nothingToReportFlagOnSecuritiesSoldInPast3Months>
    <noticeSignature><noticeDate>05/02/2024</noticeDate><planAdoptionDates>{plan}</planAdoptionDates>
      <signature>/s/ Jane Doe</signature></noticeSignature>
  </formData>
</edgarSubmission>"""


def submission(doc, accession="0000222222-24-000001", form="144", filed="20240502"):
    return f"""<SEC-DOCUMENT>{accession}.txt : {filed}
<SEC-HEADER>
ACCESSION NUMBER:\t\t{accession}
CONFORMED SUBMISSION TYPE:\t{form}
FILED AS OF DATE:\t\t{filed}
</SEC-HEADER>
<DOCUMENT>
<TYPE>{form}
<SEQUENCE>1
<FILENAME>primary_doc.xml
<TEXT>
<XML>
{doc}
</XML>
</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>"""


def test_parse_reads_every_block_as_filed():
    doc = document(security() + acquired() + acquired(nature="Open Market Purchase", shares="250") + sold()
                   + sold(date="02/01/2024", shares="100", proceeds="2400"))
    f = parse_form144(submission(doc))
    assert (f.accession, f.form, f.filed, f.issuer_cik, f.filer_cik, f.seller) == (
        "0000222222-24-000001", "144", "2024-05-02", 111111, 222222, "Doe Jane")
    s, = f.securities
    assert (s.security_class, s.broker, s.units_to_sell, s.market_value, s.units_outstanding, s.approx_sale_date,
            s.exchange) == ("Common", "Broker One", 1000, 25000, 1_000_000, "2024-05-02", "NASDAQ")
    assert [(a.date, a.nature, a.shares, a.gift) for a in f.acquisitions] == [
        ("2020-01-15", "Restricted Stock Vesting", 1000, False), ("2020-01-15", "Open Market Purchase", 250, False)]
    assert [(p.date, p.shares, p.proceeds) for p in f.past_sales] == [("2024-03-01", 500, 12000.5),
                                                                    ("2024-02-01", 100, 2400)]
    assert (f.nothing_sold_past_3_months, f.notice_date, f.plan_adoption_dates) == (False, "2024-05-02", ("2024-01-02",))


def test_an_amendment_names_the_notice_it_amends():
    f = parse_form144(submission(document(security(), form="144/A", previous="0000222222-24-000001"),
                                 accession="0000222222-24-000002", form="144/A"))
    assert (f.form, f.previous_accession) == ("144/A", "0000222222-24-000001")


def test_bare_xml_parses_and_no_document_is_none():
    f = parse_form144(document(security()).encode())
    assert f.accession is None and f.filed is None and len(f.securities) == 1
    assert parse_form144("<html>no notice here</html>") is None


def test_a_cut_short_document_is_no_notice():
    doc = document(security())
    assert parse_form144(submission(doc[: len(doc) // 2])) is None


def test_another_form_on_the_same_root_is_no_notice():
    form_d = document(security()).replace("<submissionType>144</submissionType>", "<submissionType>D</submissionType>")
    assert parse_form144(form_d) is None


def test_a_date_off_the_calendar_is_none():
    f = parse_form144(document(security(sale="02/30/2024")))
    assert f.securities[0].approx_sale_date is None


def test_seller_cik_is_the_filer_unless_the_issuer_filed():
    assert parse_form144(document(security())).seller_cik == 222222
    assert parse_form144(document(security(), filer="0000111111")).seller_cik is None


@pytest.mark.parametrize("text, expected", [
    ("Officer", {"officer"}), ("Director", {"director"}), ("10% Stockholder", {"ten_percent_owner"}),
    ("PRESIDENT AND DIRECTOR", {"officer", "director"}), ("Director, 10% Stockholder", {"director", "ten_percent_owner"}),
    ("Chief Innovation Officer", {"officer"}), ("CEO", {"officer"}), ("Former Officer", {"other"}),
    ("Former Director", {"other"}), ("Affiliate", {"other"}),
    ("Member of immediate family of any of the foregoing", {"other"}), ("", set()),
    ("Board Member", {"director"}), ("Chairman of the Board", {"director"}), ("Affiliate of Director (1)", {"other"}),
    ("Rule 144(e) aggregation affiliate of Officer", {"other"}), ("Former Officer & Director", {"other"}),
    ("10% Owner", {"ten_percent_owner"}), ("Stockholder", {"other"}),
    ("NON EXECUTIVE DIRECTOR", {"director"}), ("Non-Executive Chairman", {"director"}),
    ("Executive Chairman", {"officer", "director"}), ("Not Affiliated", set()), ("No Relationship", set()),
    ("10% holder disclaim affiliate", {"ten_percent_owner"}), ("Non-affiliate shareholder", {"other"}),
    ("Director (Former Officer)", {"director", "other"}), ("Officer (retired CEO)", {"officer", "other"}),
    ("Former Officer (Director)", {"other"}), ("Affiliate (of Director)", {"other"}), ("Officer (CEO)", {"officer"}),
    ("Directors", {"director"}), ("Officers and Directors", {"officer", "director"}), ("Officer (Retired)", {"other"}),
    ("Managing Director", {"officer"}), ("Former Director and current Officer", {"other", "officer"}),
    ("Former Officer and Director, current 10% Owner", {"other", "ten_percent_owner"}),
])
def test_relationship_is_normalized(text, expected):
    assert normalize_relationship(text) == expected


def test_relationship_lists_the_vocabulary_in_order_once():
    f = parse_form144(document(security(), relationships=("Officer", "Director", "OFFICER", "Shareholder")))
    assert f.relationship == ("director", "officer", "other")
    assert set(f.relationship) <= set(RELATIONSHIPS)


def test_one_row_per_notice_and_class():
    doc = document(security(cls="Class A Common", units="100", value="1000")
                   + security(cls="Class A Common", broker="Broker Two", units="50", value="600", sale="05/01/2024")
                   + security(cls="Class B Common", units="10", value="90", exchange="NYSE")
                   + acquired(cls="Class B Common", shares="10") + acquired(cls="Class A Common", shares="150")
                   + sold(cls="Example Corp", shares="7"), plans=("01/02/2024", "03/04/2024"))
    a, b = notice_rows(parse_form144(submission(doc)))
    assert (a["class_seq"], a["security_class"], a["shares_to_sell"], a["market_value"], a["approx_sale_date"],
            a["broker"]) == (0, "Class A Common", 150, 1600, "2024-05-01", "Broker One; Broker Two")
    assert (b["class_seq"], b["security_class"], b["shares_to_sell"], b["exchange"]) == (1, "Class B Common", 10, "NYSE")
    assert a["acquired"] == [{"date": "2020-01-15", "nature": "Restricted Stock Vesting", "shares": 150}]
    assert [x["shares"] for x in b["acquired"]] == [10]
    assert a["sold_past_3_months"] == [{"date": "2024-03-01", "shares": 7, "proceeds": 12000.5}]
    assert b["sold_past_3_months"] == []
    assert a["plan_adoption_date"] == b["plan_adoption_date"] == "2024-03-04"
    assert a["relationship"] == ["officer"] and a["seller_cik"] == 222222


def test_a_single_class_takes_every_block_whatever_its_title():
    row, = notice_rows(parse_form144(document(security() + acquired(cls="Example Corp") + sold(cls="Shares"))))
    assert len(row["acquired"]) == 1 and len(row["sold_past_3_months"]) == 1


def test_a_notice_with_no_class_is_one_row():
    row, = notice_rows(parse_form144(document(acquired(), plans=())))
    assert row["security_class"] is None and row["shares_to_sell"] is None and len(row["acquired"]) == 1
    assert row["plan_adoption_date"] is None


def _listing(rows):
    return SimpleNamespace(data=SimpleNamespace(to_pylist=lambda: rows))


def test_notices_page_newest_first_and_fetch_only_the_page(monkeypatch):
    listed = [{"form": "144", "accession_number": "a1", "filing_date": "2024-05-01"},
              {"form": "144/A", "accession_number": "a3", "filing_date": "2024-05-02"},
              {"form": "144", "accession_number": "a2", "filing_date": "2024-05-02"},
              {"form": "4", "accession_number": "x9", "filing_date": "2024-05-03"},
              {"form": "144", "accession_number": "a0", "filing_date": "2024-04-30"}]
    fetched = []
    monkeypatch.setattr(api, "_company", lambda c: SimpleNamespace(cik=111111, get_filings=lambda form: _listing(listed)))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: fetched.append(accession) or accession)
    monkeypatch.setattr(api, "parse_form144", lambda body, accession: SimpleNamespace(accession=accession, issuer_cik=111111))
    page = api.get_form144_notices(111111, limit=2)
    assert [n.accession for n in page.notices] == ["a3", "a2"] and fetched == ["a3", "a2"]
    assert (page.total, page.offset, page.next_offset) == (4, 0, 2)
    fetched.clear()
    page = api.get_form144_notices(111111, limit=2, offset=2)
    assert [n.accession for n in page.notices] == ["a1", "a0"] and fetched == ["a1", "a0"]
    assert page.next_offset is None
    assert api.get_form144_notices(111111, limit=2, offset=9).notices == ()


def test_a_notice_for_another_issuer_is_left_off_with_a_warning(monkeypatch):
    listed = [{"form": "144", "accession_number": "a1", "filing_date": "2024-05-01"}]
    monkeypatch.setattr(api, "_company", lambda c: SimpleNamespace(cik=111111, get_filings=lambda form: _listing(listed)))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: b"")
    monkeypatch.setattr(api, "parse_form144", lambda body, accession: SimpleNamespace(accession=accession, issuer_cik=5))
    with pytest.warns(UserWarning, match="a1: a Form 144 for issuer CIK 5"):
        page = api.get_form144_notices(111111)
    assert page.notices == () and page.total == 1


def test_a_page_reads_past_notices_filed_as_seller_until_it_is_full(monkeypatch):
    listed = [{"form": "144", "accession_number": "s2", "filing_date": "2024-05-03"},
              {"form": "144", "accession_number": "s1", "filing_date": "2024-05-02"},
              {"form": "144", "accession_number": "i1", "filing_date": "2024-05-01"},
              {"form": "144", "accession_number": "i0", "filing_date": "2024-04-30"}]
    monkeypatch.setattr(api, "_company", lambda c: SimpleNamespace(cik=111111, get_filings=lambda form: _listing(listed)))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: accession)
    monkeypatch.setattr(api, "parse_form144", lambda body, accession: SimpleNamespace(
        accession=accession, issuer_cik=5 if accession.startswith("s") else 111111))
    with pytest.warns(UserWarning, match="a Form 144 for issuer CIK 5") as caught:
        page = api.get_form144_notices(111111, limit=2)
    assert [str(w.message)[:2] for w in caught] == ["s2", "s1"]
    assert [n.accession for n in page.notices] == ["i1", "i0"] and (page.total, page.next_offset) == (4, None)


def test_a_page_reads_at_most_twice_its_limit(monkeypatch):
    listed = [{"form": "144", "accession_number": f"s{i}", "filing_date": f"2024-05-{20 - i:02d}"} for i in range(5)]
    listed.append({"form": "144", "accession_number": "i0", "filing_date": "2024-04-01"})
    fetched = []
    monkeypatch.setattr(api, "_company", lambda c: SimpleNamespace(cik=111111, get_filings=lambda form: _listing(listed)))
    monkeypatch.setattr(api, "fetch_submission", lambda cik, accession, cache: fetched.append(accession) or accession)
    monkeypatch.setattr(api, "parse_form144", lambda body, accession: SimpleNamespace(
        accession=accession, issuer_cik=5 if accession.startswith("s") else 111111))
    with pytest.warns(UserWarning, match="a Form 144 for issuer CIK 5"):
        page = api.get_form144_notices(111111, limit=2)
    assert (page.notices, fetched, page.next_offset) == ((), ["s0", "s1", "s2", "s3"], 4)
    with pytest.warns(UserWarning, match="a Form 144 for issuer CIK 5"):
        page = api.get_form144_notices(111111, limit=2, offset=4)
    assert [n.accession for n in page.notices] == ["i0"] and page.next_offset is None
