"""The 13F data set reader and the batch unit checks, against what they must equal: the data set's text read cell by
cell, and the one-report resolver and the pandas peer check they replace."""
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from edgar_engine.thirteenf import check_units, read_13f_dataset, read_13f_dataset_table, resolve_value_unit
from edgar_engine.thirteenf.units import MIN_FILERS, MIN_ROWS, MIXED_SHARE, OFF, resolve_value_units

INFO_HEADER = ("ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI\tVALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\t"
               "PUTCALL\tINVESTMENTDISCRETION\tOTHERMANAGER\tVOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE\n")


def window(tmp_path, info_rows: list[str]):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT\n"
        "A-2\t14-AUG-2024\t13F-HR\t222222\t30-JUN-2024\nA-1\t15-AUG-2024\t13F-HR\t111111\t30-JUN-2024\n"
        "N-1\t14-AUG-2024\t13F-NT\t555555\t30-JUN-2024\nB-1\t01-AUG-2024\t13F-HR/A\t333333\t31-MAR-2024\n")
    (tmp_path / "COVERPAGE.tsv").write_text(
        "ACCESSION_NUMBER\tISAMENDMENT\tAMENDMENTNO\tAMENDMENTTYPE\tFILINGMANAGER_NAME\tREPORTTYPE\n"
        "A-2\tN\t\t\tSecond Capital\t13F HOLDINGS REPORT\nA-1\tN\t\t\tFirst Capital\t13F HOLDINGS REPORT\n"
        "B-1\tY\t1\tRESTATEMENT\tThird Capital\t13F HOLDINGS REPORT\n")
    (tmp_path / "SUMMARYPAGE.tsv").write_text("ACCESSION_NUMBER\tTABLEENTRYTOTAL\tTABLEVALUETOTAL\nA-2\t2\t1\nA-1\t2\t1\n")
    (tmp_path / "INFOTABLE.tsv").write_text(INFO_HEADER + "".join(r + "\n" for r in info_rows))
    return tmp_path


# Out of order, interleaved, a row of a filing the window does not list, untrimmed and lower-case codes, CSV quotes.
ROWS = [
    "A-2\t9\t  Apple Inc \t COM\t037833100 \t\t2100000\t10000\tSH\t\tSOLE\t\t10000\t0\t0",
    'A-1\t7\t"Acme ""Holdings"" Corp"\tCL A\tg1151c101\tBBG000B9XRY4\t5000\t100\t sh\t Put \tsole\t1,2\t100\t0\t0',
    "X-9\t1\tStray Corp\tCOM\t000000000\t\t1\t1\tSH\t\tSOLE\t\t1\t0\t0",
    'A-1\t3\t5" Notes due 2030\tNOTE\t123456AB1\t\t900\t1000\tPRN\t\tDFND\t\t0\t0\t0',
    "A-2\t4\tMicrosoft corp\tCOM\t594918104\t\t4500000\t10000\tSH\t\tSOLE\t3\t10000\t0\t0",
    "B-1\t1\tApple Inc\tCOM\t037833100\t\t21\t10000\tSH\t\tSOLE\t\t10000\t0\t0",
]


def test_the_reader_returns_each_line_as_the_data_set_states_it(tmp_path):
    filings, rows = read_13f_dataset(window(tmp_path, ROWS))
    assert list(filings.accession) == ["A-2", "A-1", "B-1"]
    s = lambda *v: pd.Series(v, dtype="str")  # noqa: E731
    expected = pd.DataFrame({
        "accession": s("A-1", "A-1", "A-2", "A-2", "B-1"),
        "filer_cik": pd.Series([111111, 111111, 222222, 222222, 333333], dtype="Int64"),
        "period": s("2024-06-30", "2024-06-30", "2024-06-30", "2024-06-30", "2024-03-31"),
        "seq": [0, 1, 0, 1, 0],
        "issuer": s('5" Notes due 2030', 'Acme "Holdings" Corp', "Microsoft corp", "Apple Inc", "Apple Inc"),
        "title_class": s("NOTE", "CL A", "COM", "COM", "COM"),
        "cusip": s("123456AB1", "G1151C101", "594918104", "037833100", "037833100"),
        "figi": s(None, "BBG000B9XRY4", None, None, None),
        "value": [900, 5000, 4500000, 2100000, 21], "shares": [1000, 100, 10000, 10000, 10000],
        "sh_prn": s("PRN", "SH", "SH", "SH", "SH"), "put_call": s(None, "Put", None, None, None),
        "discretion": s("DFND", "SOLE", "SOLE", "SOLE", "SOLE"), "other_manager": s(None, "1,2", "3", None, None),
        "vote_sole": [0, 100, 10000, 10000, 10000], "vote_shared": [0] * 5, "vote_none": [0] * 5,
        # B-1's one priceable line implies $0.0021 a share: thousands, by the resolver's sub-dollar rule.
        "value_usd": [900, 5000, 4500000, 2100000, 21000],
    })
    pd.testing.assert_frame_equal(rows, expected)


def test_the_table_holds_text_as_codes_and_numbers_as_integers(tmp_path):
    _, rows = read_13f_dataset_table(window(tmp_path, ROWS))
    for name in ("accession", "period", "issuer", "title_class", "cusip", "figi", "sh_prn", "put_call", "discretion",
                 "other_manager"):
        assert pa.types.is_dictionary(rows.schema.field(name).type), name
    for name in ("filer_cik", "seq", "value", "shares", "vote_sole", "vote_shared", "vote_none", "value_usd"):
        assert rows.schema.field(name).type == pa.int64(), name
    # Only the values the kept lines use: the stray filing's issuer is not among the codes.
    assert "Stray Corp" not in rows["issuer"].combine_chunks().dictionary.to_pylist()


def test_an_empty_number_reads_as_a_float_column_and_a_decimal_as_its_value(tmp_path):
    rows = [ROWS[0].replace("\t10000\t0\t0", "\t10000\t\t0"), ROWS[4].replace("4500000", "4500000.5")]
    _, got = read_13f_dataset(window(tmp_path, rows))
    assert got.vote_shared.dtype == np.float64 and got.vote_shared.isna().tolist() == [False, True]
    assert got.value.dtype == np.float64 and got.value.tolist() == [4500000.5, 2100000.0]
    assert got.vote_sole.dtype == np.int64
    # Read as integers (no decimal in the window), an empty cell still makes the column float.
    _, table = read_13f_dataset_table(window(tmp_path, rows[:1] + ROWS[4:5]))
    assert table.schema.field("vote_shared").type == pa.float64() and table["vote_shared"].null_count == 1
    assert table.schema.field("value").type == pa.int64()


def test_a_line_arrow_cannot_split_is_read_as_pandas_reads_it(tmp_path):
    # A line one cell short, and a line broken outside quotes: the missing cells are empty, as read_table leaves them.
    short = ROWS[4].rsplit("\t", 1)[0]
    broken = ROWS[0].replace("Apple Inc ", "Apple\nInc ")
    _, rows = read_13f_dataset(window(tmp_path, [short, broken]))
    assert rows.issuer.tolist() == ["Microsoft corp", "Apple"]
    assert rows.vote_none.isna().all() and rows.loc[1, ["value", "value_usd", "vote_sole"]].isna().all()
    assert rows.loc[0, ["value", "value_usd", "vote_sole"]].tolist() == [4500000, 4500000, 10000]


def test_a_line_break_in_a_quoted_cell_is_read_as_pandas_reads_it(tmp_path):
    quoted = ROWS[0].replace("  Apple Inc ", '"Apple\nInc"')
    _, rows = read_13f_dataset(window(tmp_path, [quoted, ROWS[4]]))
    assert rows.issuer.tolist() == ["Microsoft corp", "Apple\nInc"]


def test_a_quote_left_open_raises_instead_of_ending_the_table(tmp_path):
    # The open quote in the last cell would run on to the end of the file, taking the next line with it.
    with pytest.raises(pd.errors.ParserError):
        read_13f_dataset(window(tmp_path, [ROWS[0][:-1] + '"0', ROWS[4]]))


@pytest.mark.parametrize("other", [ROWS[0], ROWS[0].rsplit("\t", 1)[0]])
def test_a_kept_line_with_no_infotable_sk_raises(tmp_path, other):
    with pytest.raises(ValueError):
        read_13f_dataset(window(tmp_path, [other, ROWS[4].replace("\t4\t", "\t\t", 1)]))


def test_an_integer_past_2_53_beside_an_empty_cell_rounds_as_pandas_rounds_it(tmp_path):
    big = ROWS[4].replace("\t10000\t0\t0", "\t9007199254740993\t0\t0")
    _, rows = read_13f_dataset(window(tmp_path, [big, ROWS[0].replace("\t10000\t0\t0", "\t\t0\t0")]))
    assert rows.vote_sole.tolist()[0] == float(9007199254740993) and np.isnan(rows.vote_sole[1])


def test_codes_are_cleaned_as_the_pandas_reader_cleans_them(tmp_path):
    _, rows = read_13f_dataset(window(tmp_path, [ROWS[4].replace("\tSOLE\t3", "\tstraße\u2003\t3")]))
    assert rows.discretion.tolist() == ["STRAẞE"]


def _line(acc, value, shares, sh_prn="SH", put_call=None):
    return {"accession": acc, "value": value, "shares": shares, "sh_prn": sh_prn, "put_call": put_call}


def test_the_batch_resolver_gives_each_report_the_one_report_resolution():
    rng = np.random.default_rng(7)
    lines, reports = [], []
    # Sub-dollar shares at the resolver's boundaries (0.35, 0.5, 0.65), around its minimum row count, and random.
    plans = [(7, 20), (10, 20), (13, 20), (5, 9), (5, 10), (9, 9), (0, 4), (0, 0)] + [
        (int(k), int(n)) for n in rng.integers(1, 40, 30) for k in [rng.integers(0, n + 1)]]
    for i, (k, n) in enumerate(plans):
        acc = f"R-{i}"
        lines += [_line(acc, 1.0, 10.0) for _ in range(k)] + [_line(acc, 50.0, 10.0) for _ in range(n - k)]
        # Lines no resolver prices: principal, an option, nothing held, nothing typed, a blank put or call.
        lines += [_line(acc, 1.0, 10.0, "PRN"), _line(acc, 1.0, 10.0, put_call="Call"), _line(acc, 0.0, 5.0),
                  _line(acc, 5.0, np.nan), _line(acc, 5.0, 10.0, None), _line(acc, 0.5, 10.0, put_call="")]
        reports.append({"accession": acc, "schema_version": ["X0202", None, "X0201"][i % 3],
                        "period": ["2022-06-30", "2024-06-30", None][i % 3 if i % 3 else 0]})
    rows, reports = pd.DataFrame(lines), pd.DataFrame(reports)
    got = resolve_value_units(rows, reports)
    text = lambda v: v if isinstance(v, str) else None  # noqa: E731
    for r in reports.itertuples():
        own = rows[rows.accession == r.accession]
        assert got[r.accession] == resolve_value_unit(zip(own.value, own.shares, own.sh_prn, own.put_call),
                                                       text(r.schema_version), text(r.period)), r.accession


def _reference_check_units(rows):
    """check_units as it was written over pandas groupbys and a merge, before it ran over arrays."""
    def others(s):
        n, i = len(s), np.arange(len(s))
        if n < 2:
            return np.full(n, np.nan)
        w = lambda j: np.where(j < i, s[min(j, n - 1)], s[min(j + 1, n - 1)])  # noqa: E731
        m = n - 1
        return w(m // 2) if m % 2 else (w(m // 2 - 1) + w(m // 2)) / 2

    accessions = pd.Index(rows.accession.unique(), name="accession")
    out = pd.DataFrame({"n_cmp": 0, "median_lr": np.nan, "frac_low": np.nan, "frac_high": np.nan}, index=accessions)
    pc = rows.put_call.fillna("").astype(str).str.strip()
    x = rows[(rows.sh_prn == "SH") & (pc == "") & (rows.shares > 0) & (rows.value > 0)]
    pf = x.assign(px=x.value / x.shares).groupby(["cusip", "filer_cik"]).px.median().reset_index()
    pf = pf.sort_values(["cusip", "px"])
    pf["others"] = pf.groupby("cusip").px.transform(lambda s: others(s.to_numpy()))
    pf["n_others"] = pf.groupby("cusip").px.transform("size") - 1
    x = x.merge(pf[pf.n_others >= MIN_FILERS][["cusip", "filer_cik", "others"]], on=["cusip", "filer_cik"])
    if len(x):
        x = x.assign(lr=np.log10((x.value / x.shares) / x.others))
        g = x.assign(low=x.lr < -OFF, high=x.lr > OFF).groupby("accession")
        out.loc[g.size().index, ["n_cmp", "median_lr", "frac_low", "frac_high"]] = pd.DataFrame(
            {"n_cmp": g.size(), "median_lr": g.lr.median(), "frac_low": g.low.mean(), "frac_high": g.high.mean()})
    out["n_cmp"] = out.n_cmp.astype(int)
    out["verdict"] = "ok"
    out.loc[out.median_lr < -OFF, "verdict"] = "too_small"
    out.loc[out.median_lr > OFF, "verdict"] = "too_large"
    split = (((out.verdict == "too_small") & (out.frac_low <= 1 - MIXED_SHARE))
             | ((out.verdict == "too_large") & (out.frac_high <= 1 - MIXED_SHARE)))
    mixed = (out.verdict == "ok") & ((out.frac_low >= MIXED_SHARE) | (out.frac_high >= MIXED_SHARE))
    out.loc[split | mixed, "verdict"] = "mixed"
    out.loc[out.n_cmp < MIN_ROWS, "verdict"] = "undetermined"
    out["scale_guess"] = np.where(out.verdict.isin(["too_small", "too_large"]), 10 ** (-out.median_lr.round(0)), 1.0)
    return out.reset_index()[["accession", "n_cmp", "median_lr", "frac_low", "frac_high", "verdict", "scale_guess"]]


@pytest.mark.parametrize("seed", range(5))
def test_the_peer_check_equals_its_pandas_form(seed):
    rng = np.random.default_rng(seed)
    n = 4000
    filer = rng.integers(1, 60, n)
    price = rng.choice([10.0, 10.0, 25.0, 40.0, 99.5], n)  # repeated prices: ties within a CUSIP's votes
    scale = np.where(filer % 11 == 0, 0.001, np.where(filer % 13 == 0, 1000.0, 1.0))
    scale = np.where((filer % 7 == 0) & (rng.random(n) < 0.5), 0.001, scale)  # reports that mix scales
    shares = rng.integers(1, 5000, n).astype(float)
    rows = pd.DataFrame({
        "accession": [f"A-{f}-{f % 3}" for f in filer], "filer_cik": filer,
        "cusip": rng.choice([f"C{i:02d}" for i in range(25)] + [None], n),
        "value": shares * price * scale, "shares": shares,
        "sh_prn": rng.choice(["SH", "SH", "SH", "PRN"], n), "put_call": rng.choice([None, None, None, "Put", " "], n),
    })
    rows.loc[rng.random(n) < 0.02, "value"] = 0.0
    pd.testing.assert_frame_equal(check_units(rows), _reference_check_units(rows), check_exact=True)


def _peers(n_filers=8, price=10.0):
    """One CUSIP held by n_filers filers at one price, three lines each, each filer under its own accession."""
    return pd.DataFrame([{"accession": f"P-{f}", "filer_cik": f, "cusip": "C1", "value": price * 100, "shares": 100.0,
                          "sh_prn": "SH", "put_call": None} for f in range(n_filers) for _ in range(3)])


def test_a_line_with_no_accession_counts_toward_no_report():
    # The last report has no line to compare, so lines counted toward it would show.
    rows = pd.concat([_peers(), pd.DataFrame([{"accession": "Z-1", "filer_cik": 50, "cusip": "C1", "value": 1.0,
                                               "shares": 1.0, "sh_prn": "PRN", "put_call": None}])], ignore_index=True)
    stray = pd.DataFrame([{"accession": None, "filer_cik": 99, "cusip": "C1", "value": 10.0, "shares": 100.0,
                           "sh_prn": "SH", "put_call": None}] * 3)
    got = check_units(pd.concat([stray, rows], ignore_index=True)).set_index("accession")
    want = check_units(rows).set_index("accession")
    pd.testing.assert_frame_equal(got.loc[want.index], want)


def test_a_price_that_is_not_a_number_counts_toward_nothing():
    rows = _peers()
    rows.loc[0, ["value", "shares"]] = np.inf
    clean = _peers().drop(index=0)
    pd.testing.assert_frame_equal(check_units(rows), check_units(clean))


def test_a_price_of_zero_from_infinite_shares_counts_toward_nothing():
    rows = _peers()
    rows.loc[0, "shares"] = np.inf
    pd.testing.assert_frame_equal(check_units(rows), check_units(_peers().drop(index=0)))


def test_missing_cells_of_nullable_columns_are_not_priceable():
    rows = _peers()
    nullable = rows.astype({"sh_prn": "string", "put_call": "string", "value": "Float64", "shares": "Float64"})
    nullable.loc[0, "sh_prn"] = pd.NA
    nullable.loc[1, "value"] = pd.NA
    plain = rows.drop(index=[0, 1])
    pd.testing.assert_frame_equal(check_units(nullable).set_index("accession").loc[["P-1", "P-2"]],
                                  check_units(plain).set_index("accession").loc[["P-1", "P-2"]])
    reports = pd.DataFrame({"accession": ["P-0"], "schema_version": ["X0202"], "period": ["2024-06-30"]})
    assert resolve_value_units(nullable, reports)["P-0"].priceable_rows == 1
