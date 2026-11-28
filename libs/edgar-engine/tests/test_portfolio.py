"""edgar_engine.thirteenf.portfolio: positions, the prior-period join, weights and order."""
from edgar_engine.thirteenf import portfolio, positions


def line(cusip, shares, value, *, sh_prn="SH", put_call=None, status="as_filed", accession="a1", issuer="X"):
    return {"cusip": cusip, "sh_prn": sh_prn, "put_call": put_call, "shares": shares, "value": value,
            "value_status": status, "accession": accession, "issuer": issuer, "title_class": "COM", "ticker": None}


def test_lines_of_one_position_add_up_and_join_accessions():
    got = positions([line("A", 10, 100.0, accession="a2"), line("A", 5, 50.0, accession="a1"),
                     line("A", 1, 10.0, put_call="PUT")])
    assert got[("A", "SH", None)]["shares"] == 15
    assert got[("A", "SH", None)]["value"] == 150.0
    assert got[("A", "SH", None)]["accession"] == "a1;a2"
    assert got[("A", "SH", "PUT")]["shares"] == 1


def test_an_unknown_line_value_leaves_the_position_unknown_and_excluded():
    got = positions([line("A", 10, 100.0), line("A", 5, None, status="excluded")])[("A", "SH", None)]
    assert got["value"] is None and got["value_status"] == "excluded"
    assert positions([line("A", 1, 1.0), line("A", 1, 2.0, status="rescaled")])[("A", "SH", None)][
        "value_status"] == "rescaled"


def test_an_unknown_value_or_share_count_stays_unknown_whether_none_or_nan():
    nan = float("nan")
    rows = portfolio([line("A", 10, 100.0), line("B", 5, nan, status="excluded"), line("C", nan, 50.0),
                      line("D", None, 25.0), line("E", 2, 25.0)],
                     [line("C", 4, 40.0), line("D", 3, 30.0), line("E", None, 20.0), line("F", nan, 9.0)])
    by = {r["cusip"]: r for r in rows}
    assert by["A"]["weight"] == 0.5 and by["B"]["value"] is None and by["B"]["weight"] is None
    assert [by[c]["change"] for c in "CDE"] == [None, None, None]
    assert (by["C"]["shares"], by["C"]["shares_prior"], by["E"]["shares_prior"]) == (None, 4, None)
    assert (by["A"]["change"], by["F"]["change"], by["F"]["shares_prior"]) == ("new", "sold_out", None)


def test_change_labels_against_the_period_before():
    rows = portfolio([line("NEW", 5, 50.0), line("UP", 20, 200.0), line("DOWN", 5, 60.0), line("SAME", 7, 70.0),
                      line("SAME", 3, 30.0)],
                     [line("UP", 10, 90.0), line("DOWN", 8, 80.0), line("SAME", 10, 95.0), line("GONE", 4, 40.0)])
    by = {r["cusip"]: r for r in rows}
    assert {c: r["change"] for c, r in by.items()} == {
        "NEW": "new", "UP": "increased", "DOWN": "decreased", "SAME": "unchanged", "GONE": "sold_out"}
    assert by["NEW"]["shares_prior"] == 0 and by["UP"]["shares_prior"] == 10 and by["SAME"]["shares_prior"] == 10
    assert by["GONE"]["shares"] == 0 and by["GONE"]["value"] == 0 and by["GONE"]["shares_prior"] == 4
    assert by["GONE"]["accession"] is None and by["GONE"]["weight"] == 0


def test_put_call_and_sh_prn_are_separate_positions_in_the_join():
    rows = portfolio([line("A", 10, 10.0, put_call="CALL")], [line("A", 10, 10.0)])
    assert [(r["put_call"], r["change"]) for r in rows] == [("CALL", "new"), (None, "sold_out")]


def test_no_report_before_makes_every_position_new_and_an_unreadable_period_leaves_change_unknown():
    assert [r["change"] for r in portfolio([line("A", 1, 1.0)], [])] == ["new"]
    row = portfolio([line("A", 1, 1.0)], None)[0]
    assert row["change"] is None and row["shares_prior"] is None


def test_a_missing_put_call_is_one_key_however_it_is_spelled():
    rows = portfolio([line("A", 10, 10.0, put_call=float("nan"))], [line("A", 10, 10.0, put_call=""), line("A", 5, 5.0)])
    assert [(r["put_call"], r["change"], r["shares_prior"]) for r in rows] == [(None, "decreased", 15)]


def test_a_report_omitting_confidential_holdings_leaves_change_unknown_and_sells_nothing_out():
    rows = portfolio([line("A", 1, 1.0)], [line("A", 2, 2.0), line("GONE", 4, 40.0)], omits_confidential=True)
    assert [(r["cusip"], r["change"], r["shares_prior"]) for r in rows] == [("A", None, None)]


def test_weight_is_a_fraction_of_the_known_value():
    rows = portfolio([line("A", 1, 300.0), line("B", 1, 100.0), line("C", 1, None, status="excluded")], [])
    assert {r["cusip"]: r["weight"] for r in rows} == {"A": 0.75, "B": 0.25, "C": None}


def test_order_is_value_then_unknown_then_sold_out_with_a_total_tiebreak():
    rows = portfolio([line("B", 1, 10.0), line("A", 1, 10.0), line("Z", 1, 99.0), line("N", 1, None),
                      line("M", 1, None), line("A", 1, 10.0, put_call="CALL")],
                     [line("AQ", 1, 1.0), line("AP", 1, 1.0)])
    assert [(r["cusip"], r["put_call"]) for r in rows] == [
        ("Z", None), ("A", None), ("A", "CALL"), ("B", None), ("M", None), ("N", None), ("AP", None), ("AQ", None)]
