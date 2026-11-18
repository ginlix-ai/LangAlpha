"""Read a 13F holdings report from its full-text submission: cover and summary page from the primary document,
holdings from the XML information table (2013 onward; the older text tables are not read)."""
from __future__ import annotations

from dataclasses import fields

from .. import _xml
from ..submission import decode, header
from .model import Holding13F, ThirteenFFiling, UnitResolution
from .units import resolve_value_unit

# Summary-page total against the row sum: past this the attached table is not the one the cover describes
# (another manager's table, a truncated or transposed table).
VALUE_TOLERANCE = 0.005
NIL_CUSIP = "000000000"


def _int(s: str | None) -> int | None:
    n = _xml.number(s)
    try:
        return int(n) if n is not None else None
    except (OverflowError, ValueError):
        return None


def _upper(s: str | None) -> str | None:
    return s.upper() if s else None


def _holding(el, seq: int) -> Holding13F:
    t = lambda *p: _xml.text(el, *p)  # noqa: E731
    return Holding13F(
        seq=seq, issuer=t("nameOfIssuer"), title_class=t("titleOfClass"), cusip=_upper(t("cusip")), figi=t("figi"),
        value=_xml.number(t("value")), shares=_xml.number(t("shrsOrPrnAmt", "sshPrnamt")),
        sh_prn=_upper(t("shrsOrPrnAmt", "sshPrnamtType")), put_call=t("putCall"),
        discretion=_upper(t("investmentDiscretion")), other_manager=t("otherManager"),
        vote_sole=_xml.number(t("votingAuthority", "Sole")), vote_shared=_xml.number(t("votingAuthority", "Shared")),
        vote_none=_xml.number(t("votingAuthority", "None")))


def _checks(holdings: list[Holding13F], declared_entries: int | None, declared_value: float | None) -> tuple[str, ...]:
    out = []
    if holdings and all((h.cusip or "") == NIL_CUSIP or not (h.value or h.shares) for h in holdings):
        out.append("nil_table")
    if declared_entries is not None and declared_entries != len(holdings):
        out.append("declared_entries_mismatch")
    total = sum(h.value or 0 for h in holdings)
    if declared_value is not None and abs(total - declared_value) > VALUE_TOLERANCE * max(abs(declared_value), 1.0):
        out.append("declared_value_mismatch")
    return tuple(out)


def parse_13f(submission: bytes | str, *, accession: str | None = None) -> ThirteenFFiling | None:
    """The report in a 13F-HR or 13F-HR/A submission, None when it has no complete 13F primary document.

    Holdings are every information-table row as filed, in document order; has_information_table is False when
    the submission carries no complete, well-formed table. value is as filed; value_unit says whether that is
    dollars or thousands (edgartools' resolver over the rows, schema and period), and checks lists generic sanity
    flags: nil_table, declared_entries_mismatch, declared_value_mismatch.
    """
    text = decode(submission)
    head = header(text)
    primary = _xml.element(submission, "edgarSubmission")
    if primary is None:
        return None
    cover = _xml.first(primary, "formData", "coverPage")
    summary = _xml.first(primary, "formData", "summaryPage")
    info = _xml.element(submission, "informationTable")
    holdings = [] if info is None else [_holding(el, i) for i, el in enumerate(info.iter("infoTable"))]
    declared_entries = _int(_xml.text(summary, "tableEntryTotal"))
    declared_value = _xml.number(_xml.text(summary, "tableValueTotal"))
    sv = next(primary.iter("schemaVersion"), None)
    schema = " ".join((sv.text or "").split()) or None if sv is not None else None
    period = (_xml.iso_date(_xml.text(primary, "headerData", "filerInfo", "periodOfReport")) or head.get("period")
              or _xml.iso_date(_xml.text(cover, "reportCalendarOrQuarter")))
    unit = resolve_value_unit([(h.value, h.shares, h.sh_prn, h.put_call) for h in holdings], schema, period)
    cik = _xml.text(primary, "headerData", "filerInfo", "filer", "credentials", "cik")
    return ThirteenFFiling(
        accession=accession or head.get("accession"),
        form=_xml.text(primary, "headerData", "submissionType") or head.get("form"),
        filer_cik=int(cik) if cik and cik.isascii() and cik.isdigit() else None,
        filer_name=_xml.text(cover, "filingManager", "name"),
        period=period, filed=head.get("filing_date"),
        report_type=_upper(_xml.text(cover, "reportType")),
        amendment_no=_int(_xml.text(cover, "amendmentNo")),
        amendment_type=_upper(_xml.text(cover, "amendmentInfo", "amendmentType")),
        schema_version=schema,
        declared_entries=declared_entries, declared_value=declared_value,
        holdings=tuple(holdings), value_unit=unit,
        checks=_checks(holdings, declared_entries, declared_value),
        has_information_table=info is not None)


_LINE_FIELDS = tuple(f.name for f in fields(Holding13F))
HOLDING_COLUMNS = ("accession", "filer_cik", "period") + _LINE_FIELDS + ("value_usd",)


def holding_rows(filing: ThirteenFFiling, unit: UnitResolution | None = None) -> list[dict]:
    """One flat row per holding (HOLDING_COLUMNS); value_usd is value times the multiplier of unit (default: the
    filing's own)."""
    mult = (unit or filing.value_unit).multiplier
    common = {"accession": filing.accession, "filer_cik": filing.filer_cik, "period": filing.period}
    return [{**common, **{n: getattr(h, n) for n in _LINE_FIELDS},
             "value_usd": h.value * mult if h.value is not None else None} for h in filing.holdings]
