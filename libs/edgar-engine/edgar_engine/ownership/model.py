"""What an ownership filing (Form 3, 4 or 5) holds, as filed."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping

Table = Literal["nonderivative", "derivative"]


@dataclass(frozen=True)
class ReportingOwner:
    cik: int | None
    name: str | None
    is_director: bool = False
    is_officer: bool = False
    officer_title: str | None = None
    is_ten_percent_owner: bool = False
    is_other: bool = False
    other_text: str | None = None

    @property
    def relationship(self) -> str:
        parts = []
        if self.is_director:
            parts.append("director")
        if self.is_officer:
            parts.append(f"officer: {self.officer_title}" if self.officer_title else "officer")
        if self.is_ten_percent_owner:
            parts.append("10% owner")
        if self.is_other:
            parts.append(f"other: {self.other_text}" if self.other_text else "other")
        return "; ".join(parts)


@dataclass(frozen=True)
class Transaction:
    """One line of Table I (nonderivative) or Table II (derivative). A value the filer left to a footnote is None,
    with the footnote ids under its field name in footnote_ids."""
    table: Table
    seq: int
    security_title: str | None
    date: str | None
    deemed_date: str | None
    form_type: str | None
    code: str | None
    equity_swap: bool | None
    shares: float | None
    price: float | None
    acquired_disposed: str | None
    owned_after: float | None
    value_owned_after: float | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    exercise_price: float | None = None
    exercise_date: str | None = None
    expiration_date: str | None = None
    underlying_title: str | None = None
    underlying_shares: float | None = None
    total_value: float | None = None
    footnote_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def derivative(self) -> bool:
        return self.table == "derivative"


@dataclass(frozen=True)
class OwnershipHolding:
    """A holdings-only line: what is owned, with no transaction reported."""
    table: Table
    seq: int
    security_title: str | None
    owned: float | None
    value_owned: float | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    exercise_price: float | None = None
    exercise_date: str | None = None
    expiration_date: str | None = None
    underlying_title: str | None = None
    underlying_shares: float | None = None
    footnote_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class OwnershipFiling:
    accession: str | None
    form: str | None
    period: str | None
    filed: str | None
    issuer_cik: int | None
    issuer_name: str | None
    issuer_ticker: str | None
    owners: tuple[ReportingOwner, ...]
    aff10b5_one: bool | None
    original_filed: str | None
    footnotes: Mapping[str, str]
    remarks: str | None
    transactions: tuple[Transaction, ...]
    holdings: tuple[OwnershipHolding, ...]
    schema_version: str | None = None
