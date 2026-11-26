"""Who holds a security, from N-PORT reports: each fund's position as of its latest public report.

A fund is its EDGAR series (a registrant with no series, such as a closed-end fund, is its own fund). Its reports
stand one per report date: a later filing for the same date (an NPORT-P/A, which restates the whole report, or a
re-filing) replaces an earlier one. A fund's latest report is the one with the latest report date; the one before it
gives the prior position. A holding counts as a share position when it is equity (EC, EP), counted in shares (NS)
and long, and its security is the CUSIP as filed, or the one a US or Canadian ISIN carries when the CUSIP is not a
valid one (many funds file N/A beside an ISIN). A fund's lines in one security add up to one position.
"""
from __future__ import annotations

import pandas as pd

from ..thirteenf.cusip import check_digit_ok
from ..thirteenf.parse import NIL_CUSIP
from .parse import EQUITY

POSITION_COLUMNS = ("fund", "series_id", "cik", "registrant", "series_name", "cusip", "issuer", "title", "shares",
                    "value_usd", "pct_val", "report_date", "filed", "accession", "net_assets", "shares_prior",
                    "prior_report_date")


def fund_keys(filings: pd.DataFrame) -> pd.Series:
    """Each filing's fund: its series id, or "CIK" and the registrant's ten-digit CIK when it names no series."""
    cik = filings.cik.map(lambda c: None if pd.isna(c) else f"CIK{int(c):010d}")
    return filings.series_id.astype(object).where(filings.series_id.notna(), cik)


def standing_reports(filings: pd.DataFrame) -> pd.DataFrame:
    """The report that stands for each fund and report date (the latest filed; on one day an amendment after an
    original, then the higher accession), with a fund column, newest report date first within each fund."""
    f = filings.assign(fund=fund_keys(filings), _amended=filings.form.astype(str).str.upper().str.endswith("/A"))
    f = f[f.fund.notna() & f.report_date.notna()]
    f = f.sort_values(["fund", "report_date", "filed", "_amended", "accession"])
    f = f.drop_duplicates(["fund", "report_date"], keep="last")
    return f.sort_values(["fund", "report_date"], ascending=[True, False]).drop(columns="_amended")


def security_cusips(holdings: pd.DataFrame) -> pd.Series:
    """The security of each holding: its CUSIP when the check digit holds (and it is not the nil 000000000), else the
    CUSIP inside a US or CA ISIN."""
    def valid(c: str) -> bool:
        return c != NIL_CUSIP and check_digit_ok(c)

    def one(cusip, isin) -> str | None:
        c = cusip.strip().upper() if isinstance(cusip, str) else ""
        if valid(c):
            return c
        i = isin.strip().upper() if isinstance(isin, str) else ""
        return i[2:11] if len(i) == 12 and i[:2] in ("US", "CA") and valid(i[2:11]) else None
    return pd.Series([one(c, i) for c, i in zip(holdings["cusip"], holdings["isin"])], index=holdings.index, dtype=object)


def _positions(holdings: pd.DataFrame) -> pd.DataFrame:
    h = holdings[holdings.asset_cat.isin(EQUITY) & (holdings.units == "NS") & (holdings.payoff_profile == "Long")
                 & (holdings.balance > 0)]
    h = h.assign(_cusip=security_cusips(h)).dropna(subset=["_cusip"])
    g = h.groupby(["accession", "_cusip"], sort=False)
    return (g[["balance", "value_usd", "pct_val"]].sum(min_count=1).join(g[["name", "title"]].first()).reset_index()
            .rename(columns={"_cusip": "cusip", "balance": "shares", "name": "issuer"}))


def latest_positions(filings: pd.DataFrame, holdings: pd.DataFrame, *, final_drops: bool = True) -> pd.DataFrame:
    """Every fund's share position in every security as of its latest report (POSITION_COLUMNS), with shares_prior
    from its report before (NaN when that report held none, or there is no earlier report). filings and holdings are
    FILING_COLUMNS and HOLDING_COLUMNS rows, every report of the funds that should count; a fund whose latest report
    is its final filing (it has stopped reporting) is left out unless final_drops is False."""
    r = standing_reports(filings)
    rank = r.groupby("fund").cumcount()
    latest, prior = r[rank == 0], r[rank == 1]
    if final_drops:
        latest = latest[latest.is_final.astype(object).ne(True)]
    pos = _positions(holdings[holdings.accession.isin(set(latest.accession))])
    meta = latest.set_index("accession")[["fund", "series_id", "cik", "registrant", "series_name", "report_date",
                                          "filed", "net_assets"]]
    pos = pos.join(meta, on="accession")
    before = _positions(holdings[holdings.accession.isin(set(prior.accession))])
    before = before.join(prior.set_index("accession")[["fund", "report_date"]], on="accession")
    before = before.rename(columns={"shares": "shares_prior", "report_date": "prior_report_date"})
    pos = pos.merge(before[["fund", "cusip", "shares_prior"]], on=["fund", "cusip"], how="left")
    pos = pos.join(prior.set_index("fund").report_date.rename("prior_report_date"), on="fund")
    return pos[list(POSITION_COLUMNS)].reset_index(drop=True)
