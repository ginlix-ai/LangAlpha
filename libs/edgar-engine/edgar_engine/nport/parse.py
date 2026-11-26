"""Read an N-PORT report (NPORT-P, NPORT-P/A) from its full-text submission: the fund from the header and Parts A
and B, every investment of Part C."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import fields

from .. import _xml
from ..submission import decode, header
from .model import NportFiling, NportHolding

# Asset categories of equity: common (EC) and preferred (EP). Most of a quarter's holdings are debt.
EQUITY = ("EC", "EP")
FORMS = ("NPORT-P", "NPORT-P/A")


def _upper(s: str | None) -> str | None:
    return s.upper() if s else None


def _attr(el, name: str) -> str | None:
    if el is None:
        return None
    s = " ".join((el.get(name) or "").split())
    return s or None


def _coded(el, tag: str, conditional: str, attr: str) -> tuple[str | None, str | None]:
    """A category filed as <tag>CODE</tag>, or as <conditional attr="OTHER" desc="..."/> with its description."""
    code = _xml.text(el, tag)
    if code is not None:
        return code, None
    cond = el.find(conditional)
    return _attr(cond, attr), _attr(cond, "desc")


def _identifiers(el) -> tuple[str | None, str | None, str | None]:
    """ISIN, ticker and the other identifiers ("desc:value" joined by ";"), each as filed but for its ends: spaces
    inside an identifier (an option symbol's padding) are part of it."""
    ids = el.find("identifiers")
    if ids is None:
        return None, None, None
    def value(e, name="value"):
        return (e.get(name) or "").strip() or None

    isin = next((v for i in ids.iter("isin") if (v := value(i))), None)
    ticker = next((v for t in ids.iter("ticker") if (v := value(t))), None)
    other = [f"{value(o, 'otherDesc') or ''}:{value(o) or ''}" for o in ids.iter("other")]
    return isin, ticker, ";".join(other) or None


def _derivative_cat(el) -> str | None:
    info = el.find("derivativeInfo")
    if info is None:
        return None
    return next((v for child in info if isinstance(child.tag, str) and (v := _attr(child, "derivCat"))), None)


def _holding(el, seq: int) -> NportHolding:
    def t(*p):
        return _xml.text(el, *p)

    isin, ticker, other_ids = _identifiers(el)
    asset_cat, other_asset = _coded(el, "assetCat", "assetConditional", "assetCat")
    issuer_cat, other_issuer = _coded(el, "issuerCat", "issuerConditional", "issuerCat")
    cur = el.find("currencyConditional")
    return NportHolding(
        seq=seq, name=t("name"), lei=t("lei"), title=t("title"), cusip=_upper(t("cusip")), isin=isin, ticker=ticker,
        other_ids=other_ids, balance=_xml.number(t("balance")), units=t("units"), other_units=t("descOthUnits"),
        currency=t("curCd") or _attr(cur, "curCd"), exchange_rate=_xml.number(_attr(cur, "exchangeRt")),
        value_usd=_xml.number(t("valUSD")), pct_val=_xml.number(t("pctVal")), payoff_profile=t("payoffProfile"),
        asset_cat=asset_cat, other_asset=other_asset, issuer_cat=issuer_cat, other_issuer=other_issuer,
        country=t("invCountry"), restricted=_xml.flag(t("isRestrictedSec")), fair_value_level=t("fairValLevel"),
        derivative_cat=_derivative_cat(el))


def parse_nport(submission: bytes | str, *, accession: str | None = None) -> NportFiling | None:
    """The report in an NPORT-P or NPORT-P/A submission, None for any other form or when it has no complete N-PORT
    primary document.

    holdings are every investment of Part C as filed, in document order; categories and units keep their codes
    (see NportHolding). report_date is the date the portfolio is reported as of (Item A.3.b), fiscal_year_end the
    fund's fiscal year end (A.3.a).
    """
    primary = _xml.element(submission, "edgarSubmission")
    if primary is None:
        return None
    form_data = primary.find("formData")
    gen = _xml.first(form_data, "genInfo")
    if gen is None:
        return None
    head = header(decode(submission))
    form = _xml.text(primary, "headerData", "submissionType") or head.get("form")
    if form not in FORMS:
        return None
    filer = _xml.first(primary, "headerData", "filerInfo")
    classes = [c for info in (filer.iter("seriesClassInfo") if filer is not None else ())
               for c in (_xml.text(x) for x in info.iter("classId")) if c]
    fund = _xml.first(form_data, "fundInfo")
    invs = _xml.first(form_data, "invstOrSecs")
    cik = _xml.text(gen, "regCik") or _xml.text(filer, "filer", "issuerCredentials", "cik")
    return NportFiling(
        accession=accession or head.get("accession"),
        form=form,
        filed=head.get("filing_date"),
        cik=int(cik) if cik and cik.isascii() and cik.isdigit() else None,
        registrant=_xml.text(gen, "regName"), registrant_lei=_xml.text(gen, "regLei"),
        file_number=_xml.text(gen, "regFileNumber"),
        series_id=_upper(_xml.text(gen, "seriesId") or _xml.text(filer, "seriesClassInfo", "seriesId")),
        series_name=_xml.text(gen, "seriesName"), series_lei=_xml.text(gen, "seriesLei"),
        class_ids=";".join(classes) or None,
        report_date=_xml.iso_date(_xml.text(gen, "repPdDate")), fiscal_year_end=_xml.iso_date(_xml.text(gen, "repPdEnd")),
        total_assets=_xml.number(_xml.text(fund, "totAssets")),
        total_liabilities=_xml.number(_xml.text(fund, "totLiabs")),
        net_assets=_xml.number(_xml.text(fund, "netAssets")),
        is_final=_xml.flag(_xml.text(gen, "isFinalFiling")),
        holdings=tuple(_holding(el, i) for i, el in enumerate(invs.iter("invstOrSec"))) if invs is not None else ())


_FUND_FIELDS = tuple(f.name for f in fields(NportFiling) if f.name != "holdings")
_LINE_FIELDS = tuple(f.name for f in fields(NportHolding))
FILING_COLUMNS = _FUND_FIELDS
HOLDING_COLUMNS = ("accession", "series_id", "report_date") + _LINE_FIELDS


def filing_row(filing: NportFiling) -> dict:
    """The filing's fund fields as one flat row (FILING_COLUMNS)."""
    return {n: getattr(filing, n) for n in _FUND_FIELDS}


def holding_rows(filing: NportFiling, asset_categories: Iterable[str] | None = None) -> list[dict]:
    """One flat row per holding (HOLDING_COLUMNS), only those of asset_categories (e.g. EQUITY) when given. seq
    numbers the rows returned, in document order, as read_nport_dataset numbers its own."""
    keep = None if asset_categories is None else frozenset(asset_categories)
    common = {"accession": filing.accession, "series_id": filing.series_id, "report_date": filing.report_date}
    kept = [h for h in filing.holdings if keep is None or h.asset_cat in keep]
    return [{**common, **{n: getattr(h, n) for n in _LINE_FIELDS}, "seq": i} for i, h in enumerate(kept)]
