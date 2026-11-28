import pandas as pd
import pytest

from edgar_engine.ownership import (HOLDING_COLUMNS, OWNER_COLUMNS, ROSTER_COLUMNS, holding_rows, insider_roster,
                                    insider_shares, is_common, owner_rows, parse_ownership, read_insider_holdings,
                                    read_insider_owners, roster_positions, security_class)

from test_ownership import HOLDING, document, line, submission

ISSUER = 111111


def txn(acc, owner, after, *, period="2024-05-02", filed="2024-05-06", seq=0, di="D", nature=None, code="S",
        date=None, title="Common Stock", derivative=False, owners=None, form="4"):
    return {"issuer_cik": ISSUER, "accession": acc, "form": form, "filed": filed, "period": period,
            "owner_ciks": owners or str(owner), "seq": seq, "security_title": title, "direct_indirect": di,
            "nature_of_ownership": nature, "owned_after": after, "derivative": derivative, "code": code,
            "date": date or period}


def held(acc, owner, owned, *, period="2024-05-02", filed="2024-05-06", seq=0, di="I", nature="By Trust",
         title="Common Stock", owners=None, form="4"):
    return {"issuer_cik": ISSUER, "accession": acc, "form": form, "filed": filed, "period": period,
            "owner_ciks": owners or str(owner), "seq": seq, "security_title": title, "direct_indirect": di,
            "nature_of_ownership": nature, "owned": owned, "derivative": False}


def owner(acc, cik, *, filed="2024-05-06", name="Doe Jane", director=False, officer=False, title=None, ten=False,
          other=False):
    return {"accession": acc, "form": "4", "filed": filed, "period": filed, "issuer_cik": ISSUER, "owner_cik": cik,
            "owner_name": name, "is_director": director, "is_officer": officer, "officer_title": title,
            "is_ten_percent_owner": ten, "is_other": other, "other_text": None}


def frames(txns=(), holds=(), owners=()):
    t = pd.DataFrame(list(txns), columns=list(txn("a", 1, 0)))
    h = pd.DataFrame(list(holds), columns=list(held("a", 1, 0)))
    return t, h, pd.DataFrame(list(owners), columns=list(OWNER_COLUMNS))


def test_parse_gives_holdings_and_every_owner_of_a_joint_filing():
    second = ("<reportingOwner><reportingOwnerId><rptOwnerCik>0000333333</rptOwnerCik><rptOwnerName>Doe Family Trust"
              "</rptOwnerName></reportingOwnerId><reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner>"
              "<isOther>1</isOther><otherText>Trust</otherText></reportingOwnerRelationship></reportingOwner>")
    doc = document([line()], holdings=HOLDING).replace("</reportingOwner>", "</reportingOwner>" + second, 1)
    f = parse_ownership(submission(doc))
    h = holding_rows(f)
    assert list(h[0]) == list(HOLDING_COLUMNS)
    assert (h[0]["owned"], h[0]["nature_of_ownership"], h[0]["owner_ciks"], h[0]["derivative"]) == \
        (2500, "By Trust", "222222;333333", False)
    o = owner_rows(f)
    assert [list(r) for r in o] == [list(OWNER_COLUMNS)] * 2
    assert [(r["owner_cik"], r["is_director"], r["is_officer"], r["officer_title"]) for r in o] == \
        [(222222, True, True, "Chief Executive Officer"), (333333, False, False, None)]
    assert (o[1]["is_ten_percent_owner"], o[1]["is_other"], o[1]["other_text"]) == (True, True, "Trust")


def test_read_insider_holdings_and_owners(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\n"
        "0000111111-24-000001\t06-MAY-2024\t02-MAY-2024\t3\t0000111111\tExample Corp\tEXC\n")
    (tmp_path / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\tRPTOWNER_TXT\n"
        "0000111111-24-000001\t0000222222\tDoe Jane\tDirector,Officer\tCEO\t\n"
        "0000111111-24-000001\t0000333333\tDoe Trust\tTenPercentOwner\t\t\n")
    (tmp_path / "NONDERIV_HOLDING.tsv").write_text(
        "ACCESSION_NUMBER\tNONDERIV_HOLDING_SK\tSECURITY_TITLE\tSHRS_OWND_FOLWNG_TRANS\tSHRS_OWND_FOLWNG_TRANS_FN\t"
        "VALU_OWND_FOLWNG_TRANS\tDIRECT_INDIRECT_OWNERSHIP\tNATURE_OF_OWNERSHIP\tNATURE_OF_OWNERSHIP_FN\n"
        "0000111111-24-000001\t9\tCommon Stock\t2500\t\t\tI\tBy Trust\tF1\n"
        "0000111111-24-000001\t8\tCommon Stock\t1000\tF2\t\tD\t\t\n")
    (tmp_path / "DERIV_HOLDING.tsv").write_text(
        "ACCESSION_NUMBER\tDERIV_HOLDING_SK\tSECURITY_TITLE\tCONV_EXERCISE_PRICE\tEXERCISE_DATE\tEXERCISE_DATE_FN\t"
        "EXPIRATION_DATE\tUNDLYNG_SEC_TITLE\tUNDLYNG_SEC_SHARES\tSHRS_OWND_FOLWNG_TRANS\tVALU_OWND_FOLWNG_TRANS\t"
        "DIRECT_INDIRECT_OWNERSHIP\tNATURE_OF_OWNERSHIP\n"
        "0000111111-24-000001\t1\tStock Option\t12.25\t01-JAN-2025\tF3\t01-JAN-2030\tCommon Stock\t50\t50\t\tD\t\n")
    h = read_insider_holdings(tmp_path)
    assert list(h.columns) == list(HOLDING_COLUMNS)
    assert list(zip(h.table, h.seq, h.owned)) == [("nonderivative", 0, 1000), ("nonderivative", 1, 2500),
                                                  ("derivative", 0, 50)]
    assert list(h.footnote_ids) == [{"owned": ["F2"]}, {"nature_of_ownership": ["F1"]}, {"exercise_date": ["F3"]}]
    d = h.iloc[2]
    assert (d.exercise_price, d.exercise_date, d.expiration_date, d.underlying_shares) == \
        (12.25, "2025-01-01", "2030-01-01", 50)
    assert (d.form, d.filed, d.issuer_cik, d.owner_cik, d.owner_ciks) == ("3", "2024-05-06", ISSUER, 222222,
                                                                          "222222;333333")
    o = read_insider_owners(tmp_path)
    assert list(o.columns) == list(OWNER_COLUMNS)
    assert list(zip(o.owner_cik, o.is_director, o.is_officer, o.is_ten_percent_owner)) == \
        [(222222, True, True, False), (333333, False, False, True)]
    assert o.officer_title[0] == "CEO" and pd.isna(o.officer_title[1])


def test_common_and_class():
    assert is_common("Class A Common Stock") and is_common("Common Units") and is_common("Ordinary Shares")
    assert not any(map(is_common, ["Series A Preferred Stock", "Restricted Stock Units", "RSUs", "Warrants",
                                   "8% Senior Notes", "Performance Share Units", None]))
    assert (security_class("Class B Common Stock"), security_class("Series A common"),
            security_class("Common Stock, $0.01 par value")) == ("B", "A", "")


def test_a_position_comes_from_the_latest_report_stating_it():
    t, h, _ = frames([
        txn("a1", 1, 1000, period="2024-01-10", filed="2024-01-12"),
        txn("a2", 1, 900, period="2024-03-01", filed="2024-03-03", seq=0),
        txn("a2", 1, 700, period="2024-03-01", filed="2024-03-03", seq=1),      # the filing's balance
        txn("a3", 1, 5000, period="2023-12-31", filed="2024-03-10"),             # a Form 5 for an earlier period
        txn("a4", 1, None, period="2024-04-01", filed="2024-04-02"),             # amount left to a footnote
        txn("a5", 1, 3000, period="2024-03-01", filed="2024-04-20", nature="401(k)"),
    ])
    pos = roster_positions(t, h)
    d = pos[pos.nature == ""].iloc[0]
    assert (d.shares, d.as_of, d.accession, d.lines) == (700, "2024-03-01", "a2", "a2/t1")
    assert pos[pos.nature == "401(k)"].shares.tolist() == [3000]


def test_an_amendment_replaces_its_original():
    t, h, _ = frames([txn("a1", 1, 1000, filed="2024-05-06"), txn("a2", 1, 1100, filed="2024-05-20", form="4/A")])
    assert roster_positions(t, h).shares.tolist() == [1100]
    # The amendment keeps its original's place, so a report of the same date filed between them stands.
    t, h, _ = frames([txn("a1", 1, 1000, filed="2024-05-06"), txn("a3", 1, 1050, filed="2024-05-10", form="5"),
                      txn("a2", 1, 1100, filed="2024-05-20", form="4/A")])
    assert roster_positions(t, h).shares.tolist() == [1050]


def test_a_class_keeps_its_full_designation():
    assert [security_class(x) for x in ("Class A-100 Common Stock", "Class A-200 Common Stock", "Class M-1",
                                        "Class B-Voting Common Stock")] == ["A-100", "A-200", "M-1", "B"]
    t, h, _ = frames([txn("a1", 1, 100, title="Class A-100 Common Stock"),
                      txn("a1", 1, 200, title="Class A-200 Common Stock", seq=1)])
    assert sorted(roster_positions(t, h).shares.tolist()) == [100, 200]


def test_holdings_lines_are_holdings_of_their_own():
    t, h, _ = frames(
        [txn("a1", 1, 400, di="I", nature="By Trust", seq=0), txn("a1", 1, 300, di="I", nature="By Trust", seq=1)],
        [held("a1", 1, 2000, seq=0), held("a1", 1, 50, seq=1, nature="by  trust."),
         held("a0", 1, 9999, period="2024-01-01", filed="2024-01-03")])
    pos = roster_positions(t, h)
    assert pos.shares.tolist() == [2350] and pos.lines.tolist() == ["a1/t1;a1/h0;a1/h1"]


def test_derivative_and_non_common_lines_are_not_shares():
    t, h, _ = frames([txn("a1", 1, 10, derivative=True), txn("a1", 1, 20, title="Series A Preferred Stock"),
                      txn("a1", 1, 30, title="Class B Common Stock")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("B", 30)]


def test_roster_rows_window_order_and_identity():
    t, h, o = frames(
        [txn("a1", 1, 100, period="2024-02-01", filed="2024-02-03", code="M", seq=0),
         txn("a1", 1, 50, period="2024-02-01", filed="2024-02-03", code="S", seq=1),
         txn("a1", 1, 10, period="2024-02-01", filed="2024-02-03", code="M", derivative=True, seq=5),
         txn("b1", 2, 500, period="2024-02-01", filed="2024-02-03", code="P"),
         txn("c1", 3, 80, period="2020-01-01", filed="2020-01-03")],
        [held("a1", 1, 75, period="2024-02-01", filed="2024-02-03")],
        [owner("a0", 1, filed="2023-01-01", name="Old Name", director=True),
         owner("a1", 1, filed="2024-02-03", name="Doe Jane", officer=True, title="CFO"),
         owner("b1", 2, filed="2024-02-03", name="Roe Rick", director=True),
         owner("c1", 3, filed="2020-01-03", name="Gone Gary", director=True),
         owner("d1", 4, filed="2024-02-05", name="Form Three Fay", other=True)])
    r = insider_roster(t, h, o, since="2022-02-04")
    assert list(r.columns) == list(ROSTER_COLUMNS)
    assert r.owner_cik.tolist() == [2, 1, 4]                         # 500 > 50 + 75; no position last; 3 left out
    jane = r.iloc[1]
    assert (jane.owner_name, jane.is_officer, jane.is_director, jane.officer_title) == ("Doe Jane", True, False, "CFO")
    assert (jane.shares_direct, jane.as_of_direct, jane.shares_indirect, jane.as_of_indirect) == \
        (50, "2024-02-01", 75, "2024-02-01")
    assert jane.indirect_detail == [{"nature": "By Trust", "shares": 75}]
    assert (jane.latest_transaction, jane.latest_transaction_date) == ("S", "2024-02-01")
    assert (jane.latest_filing_date, jane.latest_accession) == ("2024-02-03", "a1")
    fay = r.iloc[2]
    assert pd.isna(fay.shares_direct) and pd.isna(fay.shares_indirect) and fay.indirect_detail == []
    assert pd.isna(fay.latest_transaction) and fay.is_other
    assert insider_roster(t, h, o).owner_cik.tolist() == [2, 1, 3, 4]


def test_a_joint_filing_belongs_to_each_owner_and_counts_once():
    t, h, o = frames([txn("a1", 1, 1000, owners="1;2", di="I", nature="By Fund")],
                     owners=[owner("a1", 1, director=True), owner("a1", 2, ten=True)])
    r = insider_roster(t, h, o)
    assert r.shares_indirect.tolist() == [1000, 1000]
    pos = roster_positions(t, h)
    assert insider_shares(r, pos, "C").tolist() == [1000]


def _insider_case():
    t, h, o = frames(
        [txn("a1", 1, 100), txn("a1", 1, 40, di="I", nature="By Spouse", seq=1),
         txn("b1", 2, 7000, di="I", nature="By Fund"), txn("c1", 3, 7000, di="I", nature="By Fund"),
         txn("d1", 4, 9)],
        owners=[owner("a1", 1, officer=True), owner("b1", 2, ten=True), owner("c1", 3, ten=True),
                owner("d1", 4, other=True)])
    return insider_roster(t, h, o), roster_positions(t, h)


@pytest.mark.parametrize("definition,expected", [("A", 100), ("B", 140), ("C", 7140)])
def test_insider_definitions(definition, expected):
    r, pos = _insider_case()
    assert insider_shares(r, pos, definition).tolist() == [expected]


def test_insider_shares_keep_to_the_roster_and_known_definitions():
    r, pos = _insider_case()
    assert insider_shares(r[r.owner_cik != 1], pos, "B").empty
    with pytest.raises(ValueError, match="definition"):
        insider_shares(r, pos, "D")


def test_definition_c_counts_a_block_once_across_owners_and_each_of_one_owners_own_blocks():
    t, h, o = frames(
        [txn("a1", 1, 500, di="I", nature="By Fund"), txn("b1", 2, 500),
         txn("c1", 3, 800), txn("c1", 3, 800, di="I", nature="By Trust", seq=1), txn("d1", 4, 800)],
        owners=[owner("a1", 1, director=True), owner("b1", 2, ten=True), owner("c1", 3, ten=True),
                owner("d1", 4, ten=True)])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "B").tolist() == [500]
    assert insider_shares(r, pos, "C").tolist() == [500 + 800 + 800]


def test_definition_c_takes_a_joint_block_as_the_directors_before_matching_10_percent_owners():
    t, h, o = frames([txn("a1", 1, 500, owners="1;2"), txn("b1", 1, 500, di="I", nature="By Trust")],
                     owners=[owner("a1", 1, ten=True), owner("a1", 2, director=True), owner("b1", 1, ten=True)])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "B").tolist() == [500]
    assert insider_shares(r, pos, "C").tolist() == [500]


@pytest.mark.parametrize("single,double", [(1, 2), (2, 1)])
def test_definition_c_pairs_equal_blocks_one_for_one_whatever_the_owners_order(single, double):
    t, h, o = frames([txn("a1", single, 800), txn("b1", double, 800),
                      txn("b1", double, 800, di="I", nature="By Trust", seq=1)],
                     owners=[owner("a1", single, ten=True), owner("b1", double, ten=True)])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "C").tolist() == [1600]


def test_definition_c_takes_a_joint_block_and_its_owners_own_equal_block_as_one():
    t, h, o = frames([txn("a1", 1, 500, owners="1;2", di="I", nature="By Fund"), txn("b1", 2, 500),
                      txn("b1", 2, 700, di="I", nature="By Trust", seq=1)],
                     owners=[owner("a1", 1, ten=True), owner("a1", 2, ten=True), owner("b1", 2, ten=True)])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "C").tolist() == [1200]


def test_definition_c_keeps_a_joint_filing_whole_when_a_co_owner_is_not_counted():
    t, h, o = frames([txn("a1", 1, 500, owners="1;2", di="I", nature="By Fund"), txn("b1", 2, 500)],
                     owners=[owner("a1", 1, other=True), owner("a1", 2, ten=True), owner("b1", 2, ten=True)])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "C").tolist() == [500]
    # The co-owner's later report of its own leaves the joint line the fund's and the co-owner's together.
    t, h, o = frames([txn("a1", 1, 500, owners="1;2", di="I", nature="By Fund"), txn("b1", 2, 500),
                      txn("c1", 1, 600, filed="2024-06-03", period="2024-05-30", di="I", nature="By Fund")],
                     owners=[owner("a1", 1, other=True), owner("a1", 2, ten=True), owner("b1", 2, ten=True),
                             owner("c1", 1, other=True, filed="2024-06-03")])
    r, pos = insider_roster(t, h, o), roster_positions(t, h)
    assert insider_shares(r, pos, "C").tolist() == [500]


def test_a_position_from_a_report_with_no_filing_date_is_kept():
    t, h, _ = frames([txn("a1", 1, 100, filed=None)], [held("a1", 1, 40, filed=None)])
    assert sorted(roster_positions(t, h).shares.tolist()) == [40, 100]


def test_a_line_whose_filing_states_no_period_or_filing_date_is_dated_by_its_transaction():
    t, h, _ = frames([txn("a1", 1, 100, period=None, filed=None, date="2024-05-01")])
    assert roster_positions(t, h)[["shares", "as_of"]].values.tolist() == [[100, "2024-05-01"]]


def test_an_owner_is_placed_by_a_dated_filing_over_one_with_no_filing_date():
    t, h, o = frames([txn("a1", 1, 100, filed="2024-05-06"), txn("x1", 1, 100, filed=None)],
                     owners=[owner("a1", 1, director=True), owner("x1", 1, filed=None, director=True)])
    r = insider_roster(t, h, o, since="2024-01-01")
    assert r[["owner_cik", "latest_accession"]].values.tolist() == [[1, "a1"]]


def test_roster_of_nothing_is_empty():
    t, h, o = frames()
    assert insider_roster(t, h, o).empty and roster_positions(t, h).empty


def test_natures_the_data_set_packed_together_go_back_to_their_indirect_lines(tmp_path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\n"
        "0000111111-24-000001\t06-MAY-2024\t02-MAY-2024\t4\t111111\tExample Corp\tEXC\n"
        "0000111111-24-000002\t06-MAY-2024\t02-MAY-2024\t4\t111111\tExample Corp\tEXC\n")
    (tmp_path / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\tRPTOWNER_TXT\n"
        "0000111111-24-000001\t222222\tDoe Jane\tDirector\t\t\n0000111111-24-000002\t222222\tDoe Jane\tDirector\t\t\n")
    head = ("ACCESSION_NUMBER\tNONDERIV_HOLDING_SK\tSECURITY_TITLE\tSHRS_OWND_FOLWNG_TRANS\tVALU_OWND_FOLWNG_TRANS\t"
            "DIRECT_INDIRECT_OWNERSHIP\tNATURE_OF_OWNERSHIP\tNATURE_OF_OWNERSHIP_FN\n")
    packed = [("1", "1000", "D", "By 401(k)", ""), ("2", "9", "I", "By LLC", "F1"), ("3", "116", "I", "By Spouse", ""),
              ("4", "273", "I", "", "")]
    kept = [("5", "50", "D", "Held in plan", ""), ("6", "60", "I", "By Trust", "")]  # more natures than indirect lines
    rows = [("0000111111-24-000001", *r) for r in packed] + [("0000111111-24-000002", *r) for r in kept]
    (tmp_path / "NONDERIV_HOLDING.tsv").write_text(head + "".join(
        f"{a}\t{sk}\tCommon Stock\t{n}\t\t{di}\t{nat}\t{fn}\n" for a, sk, n, di, nat, fn in rows))
    (tmp_path / "DERIV_HOLDING.tsv").write_text(head.replace("NONDERIV", "DERIV"))
    h = read_insider_holdings(tmp_path)
    first = h[h.accession == "0000111111-24-000001"]
    assert [None if pd.isna(n) else n for n in first.nature_of_ownership] == [None, "By 401(k)", "By LLC", "By Spouse"]
    assert list(first.footnote_ids) == [{}, {}, {"nature_of_ownership": ["F1"]}, {}]
    second = h[h.accession == "0000111111-24-000002"]
    assert list(second.nature_of_ownership) == ["Held in plan", "By Trust"]



def test_a_report_is_dated_by_its_last_transaction_and_its_balance_by_the_latest_dated_line():
    t, h, _ = frames([
        txn("f5", 1, 953, period="2025-12-31", filed="2026-02-10"),                  # a Form 5
        txn("f4", 1, 5, period="2025-06-02", filed="2026-03-04", derivative=True),   # a late Table II grant
        txn("f4", 1, 1101, period="2025-06-02", filed="2026-03-04", date="2026-03-02", seq=0),
        txn("f4", 1, 1000, period="2025-06-02", filed="2026-03-04", date="2026-02-27", seq=1),  # earlier, listed last
    ])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.as_of, pos.accession)) == [(1101, "2026-03-02", "f4")]


def test_holdings_keyed_by_their_nature_however_it_is_written():
    t, h, _ = frames(
        [txn("a1", 1, 500, di="I", nature="Samsara BioCapital LP", period="2025-01-02", filed="2025-01-03")],
        [held("a2", 1, 600, nature="By Samsara BioCapital, L.P. (1)", period="2025-06-02", filed="2025-06-03"),
         held("a3", 1, 10, nature="See Footnote 4", period="2025-01-02", filed="2025-01-03"),
         held("a4", 1, 10, nature="See footnote 5", period="2025-06-02", filed="2025-06-03")])
    pos = roster_positions(t, h)
    assert sorted(zip(pos.nature, pos.shares)) == [("", 10), ("samsara biocapital lp", 600)]


def test_a_class_named_or_dropped_later_is_one_position():
    t, h, _ = frames([txn("a1", 1, 2370, title="Class A Common Stock", period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 2500, title="Common Stock", period="2025-06-02", filed="2025-06-03"),
                      txn("b1", 2, 7, title="Class A Common Stock"), txn("b1", 2, 9, title="Class B Common Stock")])
    pos = roster_positions(t, h)
    assert list(zip(pos.owner_cik, pos.security_class, pos.shares)) == [(1, "", 2500), (2, "A", 7), (2, "B", 9)]


def test_an_amendment_restating_an_early_line_still_replaces_its_original():
    t, h, _ = frames([txn("o", 1, 123584, period="2026-02-17", filed="2026-02-19", date="2026-02-17", seq=0),
                      txn("o", 1, 121000, period="2026-02-17", filed="2026-02-19", date="2026-02-18", seq=1),
                      txn("a", 1, 142061, period="2026-02-17", filed="2026-03-02", date="2026-02-17", form="4/A"),
                      txn("x", 2, 50, period="2026-02-17", filed="2026-02-21", date="2026-02-20"),
                      txn("y", 2, 40, period="2026-02-17", filed="2026-03-02", date="2026-02-17")])
    pos = roster_positions(t, h)
    assert list(zip(pos.owner_cik, pos.shares, pos.as_of, pos.accession)) == \
        [(1, 142061, "2026-02-18", "a"), (2, 50, "2026-02-20", "x")]


def test_preferred_depositary_shares_are_not_common():
    assert not is_common("Series C Depositary Shares") and not is_common("Depositary Shares")
    assert is_common("American Depositary Shares") and is_common("Depositary Shares representing Common Stock")



def test_two_classes_a_filing_names_together_stay_two_positions():
    t, h, _ = frames([txn("a1", 1, 121247, title="Common Stock", period="2026-01-05", filed="2026-01-07"),
                      txn("a1", 1, 697056, title="Class B Common Stock", period="2026-01-05", filed="2026-01-07", seq=1),
                      txn("a2", 1, 121300, title="Common Stock", period="2026-03-02", filed="2026-03-04")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("", 121300), ("B", 697056)]


def test_an_old_unqualified_title_gives_way_to_every_class_reported_after_it():
    t, h, _ = frames([txn("a1", 1, 300, title="Common Stock", period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 100, title="Class A Common Stock", period="2025-06-02", filed="2025-06-03"),
                      txn("a3", 1, 150, title="Class B Common Stock", period="2025-07-02", filed="2025-07-03")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("A", 100), ("B", 150)]


def test_a_holdings_line_repeating_the_balance_counts_once():
    t, h, _ = frames([txn("a1", 1, 1388185, di="I", nature="By Trust")],
                     [held("a1", 1, 1388185, nature="By Trust"), held("a1", 1, 1388185, seq=1, nature="By Trust"),
                      held("a1", 1, 5000, seq=2, nature="By Trust")])
    pos = roster_positions(t, h)
    assert (pos.shares.tolist(), pos.lines.tolist()) == ([2781370], ["a1/t0;a1/h1;a1/h2"])


def test_each_title_of_a_class_keeps_its_own_balance_in_a_report():
    t, h, _ = frames([txn("a1", 1, 6691, title="Restricted Common Stock", seq=0),
                      txn("a1", 1, 878, title="Common Stock", seq=1),
                      txn("a1", 1, 900, title="Common Stock, par value $0.01 per share", seq=2),
                      txn("b1", 2, 100, title="Voting Common Stock", seq=0),
                      txn("b1", 2, 500, title="Non-Voting Common Stock", seq=1)])
    pos = roster_positions(t, h)
    assert list(zip(pos.owner_cik, pos.security_class, pos.shares)) == [(1, "", 7591), (2, "", 100),
                                                                        (2, "NON-VOTING", 500)]


def test_classes_depositary_shares_and_abbreviations():
    assert [security_class(s) for s in ["CL B Common Stock", "Class M-1 Common Stock", "Class M Common Stock",
                                        "American Depositary Shares", "ADSs", "Common Stock (classic)",
                                        "Ordinary Shares (represented by American Depositary Shares)",
                                        "Non-Voting Common Stock", "Class B Non-voting Common Stock"]] == \
        ["B", "M-1", "M", "ADS", "ADS", "", "", "NON-VOTING", "B"]
    t, h, _ = frames([txn("a1", 1, 10, title="Ordinary Shares"), txn("a1", 1, 4, title="American Depositary Shares")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("", 10), ("ADS", 4)]


def test_units_awarded_in_table_i_are_not_shares():
    assert not any(map(is_common, ["Class C Google Stock Units", "Stock Units", "Dividend Equivalent Units"]))
    assert is_common("Common Units") and is_common("Class C Capital Stock")


def test_an_amendment_of_an_early_line_that_repeats_its_amount_leaves_the_later_balance():
    t, h, _ = frames([txn("o", 1, 900, period="2026-02-17", filed="2026-02-19", seq=0),
                      txn("o", 1, 800, period="2026-02-17", filed="2026-02-19", seq=1),
                      txn("a", 1, 900, period="2026-02-17", filed="2026-03-02", form="4/A"),
                      txn("p", 1, 700, period="2026-02-10", filed="2026-03-05", date="2026-02-10", form="4/A")])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.as_of, pos.lines)) == [(800, "2026-02-17", "o/t1")]


def test_an_amendment_that_corrects_a_title_restates_all_of_its_lines():
    t, h, _ = frames([txn("o", 1, 168530, period="2026-06-01", filed="2026-06-02"),
                      txn("o", 1, 50, period="2026-06-01", filed="2026-06-02", title="Restricted Stock", seq=1)],
                     [held("a", 1, 169733, period="2026-06-01", filed="2026-06-03", di="D", nature=None, form="4/A")])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.lines)) == [(169783, "o/t1;a/h0")]


def test_a_line_dated_after_its_filing_dates_neither_the_report_nor_the_latest_transaction():
    t, h, o = frames([txn("j", 1, 42786, period="2026-01-23", filed="2026-01-27", date="2027-01-25", code="M"),
                      txn("k", 1, 10093, period="2026-04-25", filed="2026-04-28", code="S")],
                     owners=[owner("j", 1, filed="2026-01-27", director=True),
                             owner("k", 1, filed="2026-04-28", director=True)])
    r = insider_roster(t, h, o)
    assert (r.shares_direct[0], r.as_of_direct[0]) == (10093, "2026-04-25")
    assert (r.latest_transaction[0], r.latest_transaction_date[0]) == ("S", "2026-04-25")


def test_a_serving_officer_whose_latest_filing_ticks_the_section_16_box_stays_listed():
    t, h, o = frames([txn("a1", 1, 100), txn("b1", 2, 3350000, filed="2026-01-15", period="2026-01-13")],
                     owners=[owner("a1", 1, director=True),
                             owner("b1", 2, filed="2026-01-15", officer=True, title="President and CEO")])
    o["not_subject_to_section16"] = [False, True]
    r = insider_roster(t, h, o)
    assert r.owner_cik.tolist() == [2, 1] and r.officer_title[0] == "President and CEO"
    assert insider_shares(r, roster_positions(t, h), "B").tolist() == [3350100]


def test_the_section_16_mark_is_read_from_the_filing_and_the_data_set(tmp_path):
    doc = document([line()]).replace("<aff10b5One>", "<notSubjectToSection16>1</notSubjectToSection16><aff10b5One>")
    assert [r["not_subject_to_section16"] for r in owner_rows(parse_ownership(submission(doc)))] == [True]
    assert owner_rows(parse_ownership(submission(document([line()]))))[0]["not_subject_to_section16"] in (None, False)
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tNOT_SUBJECT_SEC16\tDOCUMENT_TYPE\tISSUERCIK\n"
        "0000111111-24-000001\t06-MAY-2024\t02-MAY-2024\t1\t4\t0000111111\n"
        "0000111111-24-000002\t06-MAY-2024\t02-MAY-2024\t0\t4\t0000111111\n")
    (tmp_path / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\tRPTOWNER_TXT\n"
        "0000111111-24-000001\t0000222222\tDoe Jane\tDirector\t\t\n"
        "0000111111-24-000002\t0000333333\tRoe Rick\tDirector\t\t\n")
    o = read_insider_owners(tmp_path)
    assert list(o.columns) == list(OWNER_COLUMNS) and o.not_subject_to_section16.tolist() == [True, False]


def test_frames_with_no_columns_are_frames_of_nothing():
    t, _, o = frames([txn("a1", 1, 100)], owners=[owner("a1", 1, director=True)])
    assert roster_positions(t, pd.DataFrame([])).shares.tolist() == [100]
    assert insider_roster(t, pd.DataFrame([]), o).shares_direct.tolist() == [100]
    assert roster_positions(pd.DataFrame([]), pd.DataFrame([])).empty


def test_non_voting_shares_stay_a_position_when_a_later_report_names_only_the_voting_ones():
    t, h, _ = frames([txn("a1", 1, 2659837, di="I", nature="By LP", period="2026-07-09", filed="2026-07-13"),
                      txn("a1", 1, 40579081, di="I", nature="By LP", title="Non-Voting Common Stock", seq=1,
                          period="2026-07-09", filed="2026-07-13"),
                      txn("a2", 1, 8566379, di="I", nature="By LP", period="2026-07-15", filed="2026-07-17")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("", 8566379), ("NON-VOTING", 40579081)]


def test_an_amendment_holdings_line_stands_for_the_one_in_its_place():
    t, h, _ = frames([txn("o", 1, 900, di="I", nature="By Trust", period="2026-03-02", filed="2026-03-05"),
                      txn("o", 1, 800, di="I", nature="By Trust", period="2026-03-02", filed="2026-03-05",
                          date="2026-03-04", seq=1),
                      txn("a", 1, 900, di="I", nature="By Trust", period="2026-03-02", filed="2026-03-20", form="4/A")],
                     [held("o", 1, 500, period="2026-03-02", filed="2026-03-05"),
                      held("a", 1, 500, period="2026-03-02", filed="2026-03-20", form="4/A")])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.lines)) == [(1300, "o/t1;o/h0")]


def test_a_position_names_the_latest_filing_it_reads():
    t, h, _ = frames([txn("o", 1, 100, period="2026-03-02", filed="2026-03-04"),
                      txn("a", 1, 120, period="2026-03-02", filed="2026-03-20", form="4/A")],
                     [held("o", 1, 50, title="Restricted Stock", di="D", nature=None, period="2026-03-02",
                           filed="2026-03-04")])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.accession, pos.lines)) == [(170, "a", "a/t0;o/h0")]


def test_non_voting_shares_named_by_class_and_then_not_are_one_position():
    t, h, _ = frames([txn("a1", 1, 100, title="Class B Non-Voting Common Stock", period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 90, title="Non-Voting Common Stock", period="2025-06-02", filed="2025-06-03"),
                      txn("a3", 1, 40, title="Common Stock", period="2025-06-02", filed="2025-06-03", seq=1)])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("", 40), ("NON-VOTING", 90)]


def test_a_filing_naming_a_class_without_a_balance_still_names_both_titles():
    t, h, _ = frames([txn("a1", 1, 100, period="2025-01-02", filed="2025-01-03"),
                      txn("a1", 1, None, title="Class B Common Stock", period="2025-01-02", filed="2025-01-03", seq=1),
                      txn("b1", 1, 50, title="Class B Common Stock", period="2025-03-02", filed="2025-03-03")])
    assert sorted(roster_positions(t, h).shares.tolist()) == [50, 100]


def test_an_amendment_naming_a_class_renames_its_originals_plain_title():
    t, h, _ = frames([txn("a1", 1, 100, period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 100, title="Class A Common Stock", period="2025-01-02", filed="2025-01-10",
                          form="4/A")])
    assert roster_positions(t, h)[["security_class", "shares"]].values.tolist() == [["A", 100]]
    t, h, _ = frames([txn("a1", 1, 100, period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 100, title="Class A Common Stock", period="2025-01-02", filed=None, form="4/A")])
    assert roster_positions(t, h)[["security_class", "shares"]].values.tolist() == [["A", 100]]


def test_plain_and_non_voting_shares_are_never_one_renamed_position():
    t, h, _ = frames([txn("a1", 1, 40, title="Common Stock", period="2025-01-02", filed="2025-01-03"),
                      txn("a2", 1, 500, title="Non-Voting Common Stock", period="2025-06-02", filed="2025-06-03")])
    pos = roster_positions(t, h)
    assert list(zip(pos.security_class, pos.shares)) == [("", 40), ("NON-VOTING", 500)]


def test_accession_follows_filing_order_not_kind_or_number():
    t, h, _ = frames([txn("z", 1, 100, period="2026-03-02", filed="2026-03-04"),
                      txn("b", 1, 120, period="2026-03-02", filed="2026-03-20", form="4/A")],
                     [held("z", 1, 50, title="Restricted Stock", di="D", nature=None, period="2026-03-02",
                           filed="2026-03-04")])
    pos = roster_positions(t, h)
    assert list(zip(pos.shares, pos.accession)) == [(170, "b")]
