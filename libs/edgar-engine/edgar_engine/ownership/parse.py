"""Read an ownership document (Forms 3, 4, 5 and their amendments) from a full-text submission or its XML."""
from __future__ import annotations

from dataclasses import fields

from .. import _xml
from ..submission import decode, header
from .model import OwnershipFiling, OwnershipHolding, ReportingOwner, Table, Transaction

# Element (relative to a table line) -> transaction field name, for the values and their footnotes.
_FIELDS = {
    "security_title": ("securityTitle",),
    "exercise_price": ("conversionOrExercisePrice",),
    "date": ("transactionDate",),
    "deemed_date": ("deemedExecutionDate",),
    "code": ("transactionCoding", "transactionCode"),
    "form_type": ("transactionCoding", "transactionFormType"),
    "equity_swap": ("transactionCoding", "equitySwapInvolved"),
    "shares": ("transactionAmounts", "transactionShares"),
    "total_value": ("transactionAmounts", "transactionTotalValue"),
    "price": ("transactionAmounts", "transactionPricePerShare"),
    "acquired_disposed": ("transactionAmounts", "transactionAcquiredDisposedCode"),
    "exercise_date": ("exerciseDate",),
    "expiration_date": ("expirationDate",),
    "underlying_title": ("underlyingSecurity", "underlyingSecurityTitle"),
    "underlying_shares": ("underlyingSecurity", "underlyingSecurityShares"),
    "owned_after": ("postTransactionAmounts", "sharesOwnedFollowingTransaction"),
    "value_owned_after": ("postTransactionAmounts", "valueOwnedFollowingTransaction"),
    "direct_indirect": ("ownershipNature", "directOrIndirectOwnership"),
    "nature_of_ownership": ("ownershipNature", "natureOfOwnership"),
}
# transactionCoding carries its own footnotes, which belong to the code.
_FOOTNOTE_PARENT = {"code": ("transactionCoding",)}
# A holding states what is owned, not what is owned after a transaction.
_HOLDING_NAMES = {"owned_after": "owned", "value_owned_after": "value_owned"}


def _read_line(el) -> tuple[dict, dict]:
    values = {name: _xml.text(el, *path) for name, path in _FIELDS.items()}
    notes = {}
    for name, path in _FIELDS.items():
        node = _xml.first(el, *_FOOTNOTE_PARENT.get(name, path))
        ids = () if node is None else tuple(f.get("id") for f in node.iter("footnoteId") if f.get("id"))
        if ids:
            notes[name] = ids
    return values, notes


def _upper(s: str | None) -> str | None:
    return s.upper() if s else None


def _common(v: dict) -> dict:
    return dict(security_title=v["security_title"], direct_indirect=_upper(v["direct_indirect"]),
                nature_of_ownership=v["nature_of_ownership"], exercise_price=_xml.number(v["exercise_price"]),
                exercise_date=_xml.iso_date(v["exercise_date"]), expiration_date=_xml.iso_date(v["expiration_date"]),
                underlying_title=v["underlying_title"], underlying_shares=_xml.number(v["underlying_shares"]))


def _transaction(el, table: Table, seq: int) -> Transaction:
    v, notes = _read_line(el)
    return Transaction(
        table=table, seq=seq, date=_xml.iso_date(v["date"]), deemed_date=_xml.iso_date(v["deemed_date"]),
        form_type=v["form_type"], code=_upper(v["code"]), equity_swap=_xml.flag(v["equity_swap"]),
        shares=_xml.number(v["shares"]), price=_xml.number(v["price"]), acquired_disposed=_upper(v["acquired_disposed"]),
        owned_after=_xml.number(v["owned_after"]), value_owned_after=_xml.number(v["value_owned_after"]),
        total_value=_xml.number(v["total_value"]), footnote_ids=notes, **_common(v))


def _holding(el, table: Table, seq: int) -> OwnershipHolding:
    v, notes = _read_line(el)
    return OwnershipHolding(
        table=table, seq=seq, owned=_xml.number(v["owned_after"]), value_owned=_xml.number(v["value_owned_after"]),
        footnote_ids={_HOLDING_NAMES.get(k, k): ids for k, ids in notes.items()}, **_common(v))


def _cik(s: str | None) -> int | None:
    s = s.strip() if s else s
    return int(s) if s and s.isascii() and s.isdigit() else None


def _owner(el) -> ReportingOwner:
    rel = _xml.first(el, "reportingOwnerRelationship")
    return ReportingOwner(
        cik=_cik(_xml.text(el, "reportingOwnerId", "rptOwnerCik")),
        name=_xml.text(el, "reportingOwnerId", "rptOwnerName"),
        is_director=bool(_xml.flag(_xml.text(rel, "isDirector"))),
        is_officer=bool(_xml.flag(_xml.text(rel, "isOfficer"))),
        officer_title=_xml.text(rel, "officerTitle"),
        is_ten_percent_owner=bool(_xml.flag(_xml.text(rel, "isTenPercentOwner"))),
        is_other=bool(_xml.flag(_xml.text(rel, "isOther"))),
        other_text=_xml.text(rel, "otherText"))


def _lines(root, table_tag: str, line_tag: str) -> list:
    table = root.find(table_tag)
    return [] if table is None else table.findall(line_tag)


def parse_ownership(submission: bytes | str, *, accession: str | None = None) -> OwnershipFiling | None:
    """The ownership document of a full-text submission (the accession's .txt) or a bare ownership XML.

    Rows are every line as filed, in document order: identical lines stay separate, a holdings-only line is a
    holding and never a transaction, and the price is the filed transactionPricePerShare (a derivative line's
    exercise price is its own field). None when the text holds no complete, well-formed ownershipDocument.
    """
    text = decode(submission)
    root = _xml.element(submission, "ownershipDocument")
    if root is None:
        return None
    head = header(text)
    footnotes = {f.get("id"): " ".join("".join(f.itertext()).split()) for f in root.iter("footnote") if f.get("id")}
    transactions = [_transaction(el, "nonderivative", i)
                    for i, el in enumerate(_lines(root, "nonDerivativeTable", "nonDerivativeTransaction"))]
    transactions += [_transaction(el, "derivative", i)
                     for i, el in enumerate(_lines(root, "derivativeTable", "derivativeTransaction"))]
    holdings = [_holding(el, "nonderivative", i)
                for i, el in enumerate(_lines(root, "nonDerivativeTable", "nonDerivativeHolding"))]
    holdings += [_holding(el, "derivative", i)
                 for i, el in enumerate(_lines(root, "derivativeTable", "derivativeHolding"))]
    return OwnershipFiling(
        accession=accession or head.get("accession"),
        form=_xml.text(root, "documentType") or head.get("form"),
        period=_xml.iso_date(_xml.text(root, "periodOfReport")) or head.get("period"),
        filed=head.get("filing_date"),
        issuer_cik=_cik(_xml.text(root, "issuer", "issuerCik")),
        issuer_name=_xml.text(root, "issuer", "issuerName"),
        issuer_ticker=_xml.text(root, "issuer", "issuerTradingSymbol"),
        owners=tuple(_owner(el) for el in root.findall("reportingOwner")),
        aff10b5_one=_xml.flag(_xml.text(root, "aff10b5One")),
        original_filed=_xml.iso_date(_xml.text(root, "dateOfOriginalSubmission")),
        footnotes=footnotes,
        remarks=_xml.text(root, "remarks"),
        transactions=tuple(transactions),
        holdings=tuple(holdings),
        schema_version=_xml.text(root, "schemaVersion"))


def belongs_to_issuer(filing: OwnershipFiling, cik: int | str) -> bool:
    """True when cik is the filing's issuer. EDGAR lists an ownership filing under the issuer and under every
    reporting owner, so a company's own list holds filings where it is only the owner."""
    return filing.issuer_cik is not None and filing.issuer_cik == int(cik)


_FILING_COLUMNS = ("accession", "form", "filed", "period", "issuer_cik", "issuer_name", "issuer_ticker", "owner_cik",
                   "owner_ciks", "owner_name", "owner_relationship", "aff10b5_one")
_LINE_FIELDS = tuple(f.name for f in fields(Transaction))
TRANSACTION_COLUMNS = _FILING_COLUMNS + ("derivative",) + _LINE_FIELDS


def transaction_rows(filing: OwnershipFiling) -> list[dict]:
    """One flat row per transaction line (TRANSACTION_COLUMNS), with the filing's issuer, first owner and 10b5-1
    flag on each."""
    first = filing.owners[0] if filing.owners else None
    common = {
        "accession": filing.accession, "form": filing.form, "filed": filing.filed, "period": filing.period,
        "issuer_cik": filing.issuer_cik, "issuer_name": filing.issuer_name, "issuer_ticker": filing.issuer_ticker,
        "owner_cik": first.cik if first else None,
        "owner_ciks": ";".join(str(o.cik) for o in filing.owners if o.cik is not None),
        "owner_name": first.name if first else None,
        "owner_relationship": first.relationship if first else None,
        "aff10b5_one": filing.aff10b5_one,
    }
    rows = []
    for t in filing.transactions:
        line = {name: getattr(t, name) for name in _LINE_FIELDS}
        line["footnote_ids"] = {k: list(v) for k, v in t.footnote_ids.items()}
        rows.append({**common, "derivative": t.derivative, **line})
    return rows
