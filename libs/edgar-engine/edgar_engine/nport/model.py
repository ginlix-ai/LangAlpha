"""What a Form N-PORT report holds, as filed."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NportHolding:
    """One investment of Part C, in document order (seq). Codes are as filed: units NS (shares), PA (principal), NC
    (contracts) or OU (other, described in other_units); asset_cat EC (common equity), EP (preferred), DBT, STIV, ...;
    issuer_cat CORP, UST, RF (registered fund), ...; payoff_profile Long, Short or N/A. A category reported as
    "other" carries its description in other_asset or other_issuer. pct_val is a percent of net assets, as filed."""
    seq: int
    name: str | None
    lei: str | None
    title: str | None
    cusip: str | None
    isin: str | None
    ticker: str | None
    other_ids: str | None
    balance: float | None
    units: str | None
    other_units: str | None
    currency: str | None
    exchange_rate: float | None
    value_usd: float | None
    pct_val: float | None
    payoff_profile: str | None
    asset_cat: str | None
    other_asset: str | None
    issuer_cat: str | None
    other_issuer: str | None
    country: str | None
    restricted: bool | None
    fair_value_level: str | None
    derivative_cat: str | None


@dataclass(frozen=True)
class NportFiling:
    """An NPORT-P or NPORT-P/A: one fund series' portfolio as of report_date. class_ids lists the series' classes
    the filing names (";"-joined)."""
    accession: str | None
    form: str | None
    filed: str | None
    cik: int | None
    registrant: str | None
    registrant_lei: str | None
    file_number: str | None
    series_id: str | None
    series_name: str | None
    series_lei: str | None
    class_ids: str | None
    report_date: str | None
    fiscal_year_end: str | None
    total_assets: float | None
    total_liabilities: float | None
    net_assets: float | None
    is_final: bool | None
    holdings: tuple[NportHolding, ...]

    @property
    def is_amendment(self) -> bool:
        return (self.form or "").upper().endswith("/A")
