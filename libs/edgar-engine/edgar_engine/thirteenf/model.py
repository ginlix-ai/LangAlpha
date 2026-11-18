"""What a Form 13F holdings report holds, as filed."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UnitResolution:
    """The unit of a report's value column. source says what decided it: the rows' implied prices, the schema
    version, the report period, or a default; ambiguous is True when the rows could not settle it."""
    unit: str
    source: str
    ambiguous: bool
    priceable_rows: int = 0

    @property
    def multiplier(self) -> int:
        return 1000 if self.unit == "thousands" else 1


@dataclass(frozen=True)
class Holding13F:
    seq: int
    issuer: str | None
    title_class: str | None
    cusip: str | None
    figi: str | None
    value: float | None
    shares: float | None
    sh_prn: str | None
    put_call: str | None
    discretion: str | None
    other_manager: str | None
    vote_sole: float | None
    vote_shared: float | None
    vote_none: float | None


@dataclass(frozen=True)
class ThirteenFFiling:
    accession: str | None
    form: str | None
    filer_cik: int | None
    filer_name: str | None
    period: str | None
    filed: str | None
    report_type: str | None
    amendment_no: int | None
    amendment_type: str | None
    schema_version: str | None
    declared_entries: int | None
    declared_value: float | None
    holdings: tuple[Holding13F, ...]
    value_unit: UnitResolution
    checks: tuple[str, ...]
    has_information_table: bool = True

    @property
    def is_amendment(self) -> bool:
        return (self.form or "").upper().endswith("/A")
