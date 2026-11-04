"""Earnings reports, each joined to the fiscal quarter it announced.

An earnings row carries an announcement date and its EPS; the income
statements carry the quarter and its diluted EPS. ginlix-data's CN rows name
the quarter (``fiscalDateEnding``) and carry the statement's own basic EPS;
FMP's stable rows name no quarter and carry the consensus-basis EPS analysts
estimate, so those are placed by date. The join happens once, here, and the
overview's text and artifact read the joined record.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .currency import CurrencyArg, fmt_money, fmt_price
from .fiscal_periods import infer_fiscal_period, period_name, report_period_ends
from .utils import format_percentage

_DAYS_PER_QUARTER = 91
# ginlix-data's CN rows: a periodic report or its preliminary (快报), whose EPS
# is the statement's basic EPS rather than a consensus figure.
_STATEMENT_KINDS = ("report", "express")


def diluted_eps(stmt: Dict[str, Any]) -> Optional[float]:
    # FMP's stable statements spell it ``epsDiluted``, as CN rows do; yfinance
    # rows (shaped by the provider's own mapper, not FMP's renames) still
    # keep the v3 ``epsdiluted``.
    value = stmt.get("epsDiluted")
    return value if value is not None else stmt.get("epsdiluted")


@dataclass(frozen=True)
class EarningsReport:
    date: str
    eps_actual: Optional[float]
    eps_estimate: Optional[float]
    revenue_actual: Optional[float]
    revenue_estimate: Optional[float]
    # "Q4 FY2026"; for a report still to come, the quarter after the last one out.
    period: Optional[str]
    # The income statement's, when it is out and per the same share as the
    # row's EPS (TSM's statement is TWD per share, its EPS USD per ADS).
    diluted_eps: Optional[float]
    # True for FMP's consensus-basis EPS, False for a CN statement's basic EPS.
    consensus_basis: bool = True

    @property
    def reported(self) -> bool:
        return self.eps_actual is not None


def join_earnings(
    rows: List[Dict[str, Any]],
    income: List[Dict[str, Any]],
    lookup: Dict[str, str],
    *,
    eps_currency: Optional[str],
) -> List[EarningsReport]:
    """The provider's reported and upcoming earnings rows as reports, newest first.

    A row that names its period end keeps it. One that does not is aligned by
    date with the statements no row named; two such rows reported on one day
    (an A-share's annual and Q1 results, on FMP) cannot be told apart, so
    neither is placed. A report still without a period end (one still to come,
    or one older than the statements fetched) is placed by its distance from
    the nearest one with an end, one period per report at the statements' own
    cadence (half a year for a half-yearly reporter).

    A statement's diluted EPS joins its report only when the statement names
    ``eps_currency``, the row's own (None for no statement EPS at all), so an
    issuer that changed reporting currency or a row that names none puts no
    figure on the wrong basis.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    kept: Dict[Tuple[str, str], Dict[str, Any]] = {}
    ambiguous = set()
    for row in rows:
        day = str(row.get("date") or "")[:10]
        key = (day, str(row.get("fiscalDateEnding") or "")[:10])
        reported = row.get("epsActual") is not None
        # A past row never filled in is no report; a same-key duplicate keeps the reported one.
        if not day or not (reported or day >= today):
            continue
        if key in kept and reported and kept[key].get("epsActual") is not None and not key[1]:
            ambiguous.add(key)
        if key not in kept or reported:
            kept[key] = row
    keys = sorted(k for k in kept if k not in ambiguous)
    named = {k[1] for k in keys if k[1]}
    aligned = report_period_ends(
        [k[0] for k in keys if not k[1] and kept[k].get("epsActual") is not None],
        [s.get("date") for s in income if str(s.get("date") or "")[:10] not in named],
        annual_ends=[end for end, name in lookup.items() if name.startswith("Q4")],
    )
    ends = [k[1] or aligned.get(k[0]) for k in keys]
    anchors = [i for i, end in enumerate(ends) if end]
    step = _cadence_days(sorted({str(s.get("date"))[:10] for s in income if s.get("date")} | named))
    statements = {str(s.get("date") or "")[:10]: s for s in income}
    reports = []
    for i, (key, end) in enumerate(zip(keys, ends)):
        row = kept[key]
        if end:
            period = period_name(end, lookup)
        elif anchors:
            anchor = min(anchors, key=lambda j: abs(j - i))
            guess = datetime.strptime(ends[anchor], "%Y-%m-%d") + timedelta(days=step * (i - anchor))
            period = infer_fiscal_period(guess.strftime("%Y-%m-%d"), lookup)
        else:
            period = None
        stmt = statements.get(end) if end else None
        reports.append(EarningsReport(
            date=key[0],
            eps_actual=row.get("epsActual"),
            eps_estimate=row.get("epsEstimated"),
            revenue_actual=row.get("revenueActual"),
            revenue_estimate=row.get("revenueEstimated"),
            period=period,
            diluted_eps=(
                diluted_eps(stmt)
                if stmt is not None and eps_currency and stmt.get("reportedCurrency") == eps_currency
                else None
            ),
            consensus_basis=row.get("kind") not in _STATEMENT_KINDS,
        ))
    return reports[::-1]


def _cadence_days(ends: List[str]) -> int:
    """The median gap between period ends; a quarter without two."""
    dates = [datetime.strptime(e, "%Y-%m-%d") for e in ends]
    gaps = sorted((b - a).days for a, b in zip(dates, dates[1:]))
    return gaps[len(gaps) // 2] if gaps else _DAYS_PER_QUARTER


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> List[str]:
    return [
        f"| {' | '.join(headers)} |",
        f"|{'|'.join('-' * (len(h) + 2) for h in headers)}|",
        *(f"| {' | '.join(row)} |" for row in rows),
    ]


def _surprise(actual: Optional[float], estimate: Optional[float]) -> Optional[str]:
    if actual is None or not estimate:
        return None
    return format_percentage((actual - estimate) / abs(estimate) * 100)


def next_earnings_lines(reports: List[EarningsReport], cur: CurrencyArg) -> List[str]:
    # A missing actual on a past row means "never populated", not "upcoming".
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    upcoming = [r for r in reports if not r.reported and r.date >= today]
    if not upcoming:
        return []
    nxt = upcoming[-1]  # newest first, so the soonest is last
    out = [
        "### Next Earnings Report",
        "",
        f"**Report Date:** {nxt.date}",
        f"**Fiscal Period:** {nxt.period or 'N/A'}",
    ]
    if nxt.eps_estimate is not None:
        out.append(f"**EPS Estimate:** {fmt_price(nxt.eps_estimate, cur)}")
    if nxt.revenue_estimate is not None:
        out.append(f"**Revenue Estimate:** {fmt_money(nxt.revenue_estimate, cur)}")
    return out + [""]


def earnings_lines(reports: List[EarningsReport], cur: CurrencyArg) -> List[str]:
    """Reported earnings against estimates.

    FMP's actual EPS is on the basis its estimates use, which often excludes
    one-time items (Apple's FY26 Q4 tariff refund, a drugmaker's acquisition
    charges), so the income statement's diluted EPS stands beside it and
    neither is passed off as the other. A CN row's EPS is the statement's basic
    EPS and is named so. The diluted EPS is only ever in the row's currency.
    """
    reported = [r for r in reports if r.reported]
    if not reported:
        return []
    latest = reported[0]
    eps_label = "EPS (consensus basis)" if latest.consensus_basis else "EPS (basic, reported)"
    heading = f"{latest.period}, reported {latest.date}" if latest.period else latest.date
    out = ["### Earnings Performance", "", f"**Latest Quarter ({heading}):**", ""]

    eps = fmt_price(latest.eps_actual, cur)
    eps_surprise = _surprise(latest.eps_actual, latest.eps_estimate)
    out.append(
        f"- **{eps_label}:** {eps} actual vs {fmt_price(latest.eps_estimate, cur)} "
        f"estimate ({eps_surprise} surprise)"
        if eps_surprise
        else f"- **{eps_label}:** {eps} (no estimate available)"
    )
    if latest.diluted_eps is not None:
        out.append(f"- **EPS (diluted, income statement):** {fmt_price(latest.diluted_eps, cur)}")
    if latest.revenue_actual is not None:
        revenue = fmt_money(latest.revenue_actual, cur)
        rev_surprise = _surprise(latest.revenue_actual, latest.revenue_estimate)
        out.append(
            f"- **Revenue:** {revenue} actual vs {fmt_money(latest.revenue_estimate, cur)} "
            f"estimate ({rev_surprise} surprise)"
            if rev_surprise
            else f"- **Revenue:** {revenue} (no estimate available)"
        )

    trend = reported[:4]
    with_diluted = any(r.diluted_eps is not None for r in trend)
    if len(trend) > 1:
        headers = ["Date", "Fiscal Period", eps_label]
        headers += ["EPS (diluted, income statement)"] if with_diluted else []
        rows = [
            [
                r.date,
                r.period or "N/A",
                fmt_price(r.eps_actual, cur),
                *([fmt_price(r.diluted_eps, cur) if r.diluted_eps is not None else "N/A"]
                  if with_diluted else []),
                fmt_money(r.revenue_actual, cur),
            ]
            for r in trend
        ]
        out += ["", "**Recent Earnings Trend:**", "", *_table([*headers, "Revenue"], rows)]
    if with_diluted and latest.consensus_basis:
        out += [
            "",
            "*Consensus-basis EPS is the figure analysts estimate, which often excludes one-time "
            "items; diluted EPS from the income statement includes them. Compare an estimate only "
            "with the consensus-basis actual.*",
        ]
    return out + [""]


def earnings_surprises(reports: List[EarningsReport]) -> List[Dict[str, Any]]:
    """The artifact's series, oldest first, for the surprise chart."""
    return [
        {
            "period": r.period or r.date,
            "date": r.date,
            "epsActual": r.eps_actual,
            "epsEstimate": r.eps_estimate,
            "revenueActual": r.revenue_actual,
            "revenueEstimate": r.revenue_estimate,
        }
        for r in reversed(reports)
        if r.reported
    ]
