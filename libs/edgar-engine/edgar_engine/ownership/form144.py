"""Form 144 (notice of proposed sale of securities under Rule 144) and Form 144/A, read from the filing's XML.

An affiliate files a Form 144 when placing an order to sell restricted or control securities; EDGAR has required the
XML form since 2023-04-13. A notice names the issuer, the person for whose account the securities are to be sold, that
person's relationship to the issuer, and per class of security the broker, the units to be sold, their market value
and the approximate date of sale, then how the securities were acquired and what the person sold in the past three
months. A 144/A is a whole notice again, naming the notice it amends (previous_accession).

The reading is as strict as for Forms 3, 4 and 5: a document that is cut short or not well formed is no notice.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from .. import _xml
from ..submission import decode, header

__all__ = ["FORM144_FORMS", "NOTICE_COLUMNS", "RELATIONSHIPS", "Acquisition", "Form144Filing", "PastSale",
           "SecurityToSell", "normalize_relationship", "notice_rows", "parse_form144"]

FORM144_FORMS = ("144", "144/A")
#: The relationship vocabulary, in the order a row lists it.
RELATIONSHIPS = ("director", "officer", "ten_percent_owner", "other")


@dataclass(frozen=True)
class SecurityToSell:
    """One securitiesInformation block: a class of security to be sold and the broker selling it."""
    seq: int
    security_class: str | None
    broker: str | None
    units_to_sell: float | None
    market_value: float | None
    units_outstanding: float | None
    approx_sale_date: str | None
    exchange: str | None


@dataclass(frozen=True)
class Acquisition:
    """One securitiesToBeSold block: how and when securities to be sold were acquired."""
    seq: int
    security_class: str | None
    date: str | None
    nature: str | None
    acquired_from: str | None
    gift: bool | None
    donor_acquired_date: str | None
    shares: float | None
    payment_date: str | None
    payment_nature: str | None


@dataclass(frozen=True)
class PastSale:
    """One securitiesSoldInPast3Months block."""
    seq: int
    seller: str | None
    security_class: str | None
    date: str | None
    shares: float | None
    proceeds: float | None


@dataclass(frozen=True)
class Form144Filing:
    accession: str | None
    form: str | None
    filed: str | None
    previous_accession: str | None
    filer_cik: int | None
    issuer_cik: int | None
    issuer_name: str | None
    seller: str | None
    relationships_as_filed: tuple[str, ...]
    securities: tuple[SecurityToSell, ...]
    acquisitions: tuple[Acquisition, ...]
    nothing_sold_past_3_months: bool | None
    past_sales: tuple[PastSale, ...]
    notice_date: str | None
    plan_adoption_dates: tuple[str, ...]
    remarks: str | None

    @property
    def relationship(self) -> tuple[str, ...]:
        """The relationships as filed, normalized to RELATIONSHIPS (see normalize_relationship)."""
        found = set().union(*(normalize_relationship(r) for r in self.relationships_as_filed)) \
            if self.relationships_as_filed else set()
        return tuple(r for r in RELATIONSHIPS if r in found)

    @property
    def seller_cik(self) -> int | None:
        """The CIK the notice was filed under, which is the seller's: None when it is the issuer's own, since an
        issuer filing for its insider names no CIK of the insider's."""
        return None if self.filer_cik is None or self.filer_cik == self.issuer_cik else self.filer_cik


_CLAUSE = re.compile(r"\s*(?:[,;/&+]|\band\b)\s*", re.I)
_DIRECTOR = re.compile(r"\b((?<!managing\s)directors?|chairman|chairwoman|chair|board)\b", re.I)
_OFFICER = re.compile(r"\b(officers?|managing\s+directors?|ceo|cfo|coo|cto|cao|president|secretary|treasurer|controller|general counsel|chief|"
                      r"(?<!non[- ])executive|(?<!non[- ])exec\.?|vp|evp|svp)\b", re.I)
_TEN_PERCENT = re.compile(r"\b10\s*%|\bten\s+percent\b|\b10\s+percent\b", re.I)
_FORMER = re.compile(r"\b(former|formerly|retired|resigned)\b", re.I)
_CURRENT = re.compile(r"\b(current|currently)\b", re.I)
_FORMER_PAREN = re.compile(r"\(([^()]*\b(?:former|formerly|retired|resigned)\b[^()]*)\)", re.I)
# A clause naming someone else's relationship: an affiliate or relative of an officer is neither.
_RELATED = re.compile(r"\b(affiliates?|affiliated|family|spouse|wife|husband|son|daughter|child|children|relative|"
                      r"aggregation|trust)\b", re.I)
# A clause denying a relationship ("No Relationship", "Not Affiliated", "10% holder disclaims affiliate status") names none.
_DENIAL = re.compile(r"\b(not|non|no|disclaims?|disclaimed|disclaiming)\b[\s-]*(an?\s+)?"
                     r"(affiliates?|affiliated|affiliation|relationship)\b(\s+status)?", re.I)


def normalize_relationship(text: str | None) -> set[str]:
    """The RELATIONSHIPS one relationshipToIssuer entry names. The entry is free text ("Officer", "10% Stockholder",
    "PRESIDENT AND DIRECTOR", "Former Officer"), read clause by clause: a director or chair is a director, an officer
    or an officer's title (CEO, President, Secretary, Chief, Managing Director ...) an officer, a 10% holder a
    ten_percent_owner, and a former one ("Former" leading an entry governs all of it unless a later clause says
    "current"), an affiliate or relative of one ("Affiliate of Director"), or anything else (a shareholder) other. A
    parenthesis naming a former relationship, as in "Director (Former Officer)", is a clause of its own, and a bare
    one, as in "Officer (Retired)", makes its clause former. A non-executive director is no officer, and a denial
    ("No Relationship", "Not Affiliated") names nothing. Empty for an empty entry."""
    out: set[str] = set()
    former = bool(text) and _FORMER.match(text.strip()) is not None
    text = _FORMER_PAREN.sub(lambda m: m[0] if _FORMER.fullmatch(m[1].strip()) else f", {m[1]},", text or "")
    for clause in (_DENIAL.sub(" ", c) for c in _CLAUSE.split(text) if c and c.strip()):
        if not clause.strip():
            continue
        former = former and not _CURRENT.search(clause)
        if former or _FORMER.search(clause) or _RELATED.search(clause):
            out.add("other")
            continue
        hit = {name for name, rx in (("director", _DIRECTOR), ("officer", _OFFICER),
                                     ("ten_percent_owner", _TEN_PERCENT)) if rx.search(clause)}
        out |= hit or {"other"}
    return out


_US_SLASH = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _date(s: str | None) -> str | None:
    """YYYY-MM-DD from the form's MM/DD/YYYY (or an ISO date); None for anything that is not a calendar date."""
    if s and (m := _US_SLASH.match(s.strip())):
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
        except ValueError:
            return None
    return _xml.iso_date(s)


def _cik(s: str | None) -> int | None:
    s = s.strip() if s else s
    return int(s) if s and s.isascii() and s.isdigit() else None


def _security(el, seq: int) -> SecurityToSell:
    return SecurityToSell(
        seq=seq, security_class=_xml.text(el, "securitiesClassTitle"),
        broker=_xml.text(el, "brokerOrMarketmakerDetails", "name"),
        units_to_sell=_xml.number(_xml.text(el, "noOfUnitsSold")),
        market_value=_xml.number(_xml.text(el, "aggregateMarketValue")),
        units_outstanding=_xml.number(_xml.text(el, "noOfUnitsOutstanding")),
        approx_sale_date=_date(_xml.text(el, "approxSaleDate")),
        exchange=_xml.text(el, "securitiesExchangeName"))


def _acquisition(el, seq: int) -> Acquisition:
    return Acquisition(
        seq=seq, security_class=_xml.text(el, "securitiesClassTitle"), date=_date(_xml.text(el, "acquiredDate")),
        nature=_xml.text(el, "natureOfAcquisitionTransaction"), acquired_from=_xml.text(el, "nameOfPersonfromWhomAcquired"),
        gift=_xml.flag(_xml.text(el, "isGiftTransaction")), donor_acquired_date=_date(_xml.text(el, "donarAcquiredDate")),
        shares=_xml.number(_xml.text(el, "amountOfSecuritiesAcquired")), payment_date=_date(_xml.text(el, "paymentDate")),
        payment_nature=_xml.text(el, "natureOfPayment"))


def _past_sale(el, seq: int) -> PastSale:
    return PastSale(
        seq=seq, seller=_xml.text(el, "sellerDetails", "name"), security_class=_xml.text(el, "securitiesClassTitle"),
        date=_date(_xml.text(el, "saleDate")), shares=_xml.number(_xml.text(el, "amountOfSecuritiesSold")),
        proceeds=_xml.number(_xml.text(el, "grossProceeds")))


def parse_form144(submission: bytes | str, *, accession: str | None = None) -> Form144Filing | None:
    """The Form 144 or 144/A of a full-text submission (the accession's .txt) or a bare edgarSubmission XML.

    Blocks are kept as filed, in document order. Dates are YYYY-MM-DD; a value the filer left out is None. None when
    the text holds no complete, well-formed edgarSubmission, or one that is not a Form 144 (Form D and other forms
    share the root element)."""
    root = _xml.element(submission, "edgarSubmission")
    if root is None:
        return None
    head = header(decode(submission))
    form = _xml.text(root, "headerData", "submissionType") or head.get("form")
    data = root.find("formData")
    if form not in FORM144_FORMS or data is None or data.find("issuerInfo") is None:
        return None
    issuer = data.find("issuerInfo")
    signature = data.find("noticeSignature")
    return Form144Filing(
        accession=accession or head.get("accession"),
        form=form,
        filed=head.get("filing_date"),
        previous_accession=_xml.text(root, "headerData", "previousAccessionNumber"),
        filer_cik=_cik(_xml.text(root, "headerData", "filerInfo", "filer", "filerCredentials", "cik")),
        issuer_cik=_cik(_xml.text(issuer, "issuerCik")),
        issuer_name=_xml.text(issuer, "issuerName"),
        seller=_xml.text(issuer, "nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold"),
        relationships_as_filed=tuple(t for el in issuer.iter("relationshipToIssuer")
                                     if (t := " ".join((el.text or "").split()))),
        securities=tuple(_security(el, i) for i, el in enumerate(data.findall("securitiesInformation"))),
        acquisitions=tuple(_acquisition(el, i) for i, el in enumerate(data.findall("securitiesToBeSold"))),
        nothing_sold_past_3_months=_xml.flag(_xml.text(data, "nothingToReportFlagOnSecuritiesSoldInPast3Months")),
        past_sales=tuple(_past_sale(el, i) for i, el in enumerate(data.findall("securitiesSoldInPast3Months"))),
        notice_date=_date(_xml.text(signature, "noticeDate")),
        plan_adoption_dates=tuple(d for el in (signature.iter("planAdoptionDate") if signature is not None else ())
                                  if (d := _date(el.text))),
        remarks=_xml.text(data, "remarks"))


NOTICE_COLUMNS = ("accession", "form", "filed", "previous_accession", "issuer_cik", "issuer_name", "seller",
                  "seller_cik", "relationship", "class_seq", "security_class", "shares_to_sell", "market_value",
                  "units_outstanding", "approx_sale_date", "exchange", "broker", "plan_adoption_date", "notice_date",
                  "acquired", "sold_past_3_months")


def _class_key(title: str | None) -> str:
    return " ".join((title or "").lower().split())


def _sum(values) -> float | None:
    got = [v for v in values if v is not None]
    return sum(got) if got else None


def _joined(values) -> str | None:
    return "; ".join(dict.fromkeys(v for v in values if v)) or None


def notice_rows(filing: Form144Filing) -> list[dict]:
    """One flat row per security class of the notice (NOTICE_COLUMNS), in the order the classes are first filed.

    Blocks naming the same class (one per broker) make one row: units and market value summed, the earliest
    approximate sale date, brokers and exchanges joined with "; ". acquired ({date, nature, shares}) and
    sold_past_3_months ({date, shares, proceeds}) list the blocks of the row's class; with a single class, or for a
    block whose class title names none of the notice's classes (filers often title these by the issuer's name), they
    go to the first row, so no block is listed twice. plan_adoption_date is the latest 10b5-1 plan adoption date the
    notice gives, relationship the normalized list. A notice with no class to sell is one row with those fields None.
    """
    groups: dict[str, list[SecurityToSell]] = {}
    for s in filing.securities:
        groups.setdefault(_class_key(s.security_class), []).append(s)
    keys = list(groups) or [""]

    def home(title: str | None) -> str:
        key = _class_key(title)
        return key if key in groups and len(keys) > 1 else keys[0]

    acquired = {k: [] for k in keys}
    for a in filing.acquisitions:
        acquired[home(a.security_class)].append({"date": a.date, "nature": a.nature, "shares": a.shares})
    sold = {k: [] for k in keys}
    for p in filing.past_sales:
        sold[home(p.security_class)].append({"date": p.date, "shares": p.shares, "proceeds": p.proceeds})
    common = {
        "accession": filing.accession, "form": filing.form, "filed": filing.filed,
        "previous_accession": filing.previous_accession, "issuer_cik": filing.issuer_cik,
        "issuer_name": filing.issuer_name, "seller": filing.seller, "seller_cik": filing.seller_cik,
        "relationship": list(filing.relationship),
        "plan_adoption_date": max(filing.plan_adoption_dates, default=None), "notice_date": filing.notice_date,
    }
    rows = []
    for seq, key in enumerate(keys):
        blocks = groups.get(key, [])
        sale_dates = [b.approx_sale_date for b in blocks if b.approx_sale_date]
        rows.append({
            **common, "class_seq": seq,
            "security_class": blocks[0].security_class if blocks else None,
            "shares_to_sell": _sum(b.units_to_sell for b in blocks),
            "market_value": _sum(b.market_value for b in blocks),
            "units_outstanding": next((b.units_outstanding for b in blocks if b.units_outstanding is not None), None),
            "approx_sale_date": min(sale_dates, default=None),
            "exchange": _joined(b.exchange for b in blocks), "broker": _joined(b.broker for b in blocks),
            "acquired": acquired[key], "sold_past_3_months": sold[key],
        })
    return rows
