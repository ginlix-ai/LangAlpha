"""Revenue by product and geography for the company overview."""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ._shared import _safe_result
from .currency import CurrencyArg, fmt_money


@dataclass(frozen=True)
class Segment:
    """The newest reported period's revenue per segment."""

    date: str
    revenues: Dict[str, float]


@dataclass(frozen=True)
class SegmentBreakdown:
    product: Optional[Segment]
    geo: Optional[Segment]
    # "quarter", or "annual" for an issuer that files segments only yearly.
    scope: str = "quarter"


def _latest(rows: List[Any]) -> Optional[Segment]:
    """The newest ``{date: {segment: value}}`` row, when it carries any figure."""
    record = rows[0] if rows else None
    if not record or not isinstance(record, dict):
        return None
    date = list(record.keys())[0]
    revenues = record[date]
    if revenues and isinstance(revenues, dict):
        return Segment(date, revenues)
    return None


async def fetch_segments(financial: Any, symbol: str) -> SegmentBreakdown:
    """Product and geography segments, plus the period they cover.

    Quarterly first; a name that files segments only annually (every CN issuer,
    many small caps) falls back to its fiscal-year rows rather than showing none.
    """
    async def pair(period: str) -> tuple[list, list]:
        product, geo = await asyncio.gather(
            financial.get_revenue_by_segment(
                symbol, segment_type="product", period=period, structure="flat"
            ),
            financial.get_revenue_by_segment(
                symbol, segment_type="geography", period=period, structure="flat"
            ),
            return_exceptions=True,
        )
        return _safe_result(product, []), _safe_result(geo, [])

    scope = "quarter"
    product, geo = await pair(scope)
    if not (product or geo):
        scope = "annual"
        product, geo = await pair(scope)
    return SegmentBreakdown(_latest(product), _latest(geo), scope)


def _period_label(date_str: str, lookup: Dict[str, str], scope: str) -> str:
    label = lookup.get(date_str)
    if scope == "annual":
        # The quarterly lookup names a fiscal year-end "Q4 FY2025".
        return label[3:] if label and label.startswith("Q4 ") else f"Fiscal year ended {date_str}"
    return label or f"Period ending {date_str}"


def segment_lines(
    breakdown: SegmentBreakdown, lookup: Dict[str, str], currency: CurrencyArg
) -> List[str]:
    """The overview's revenue breakdown section; empty without a segment.

    *currency* is the statement currency: segments are reported figures.
    """
    if breakdown.product is None and breakdown.geo is None:
        return []
    scope_label = "Latest Fiscal Year" if breakdown.scope == "annual" else "Latest Quarter"
    out = [f"### Revenue Breakdown ({scope_label})", ""]
    # segment, column header, its rule, how many rows print (None: all)
    tables = (
        (breakdown.product, "Product", "|---------|---------|------------|", 5),
        (breakdown.geo, "Region", "|--------|---------|------------|", None),
    )
    for segment, column, rule, top in tables:
        if segment is None:
            continue
        period = _period_label(segment.date, lookup, breakdown.scope)
        out += [
            f"**By {column} ({period}):**",
            f"*Report Date: {segment.date}*",
            "",
            f"| {column} | Revenue | Percentage |",
            rule,
        ]
        total = sum(segment.revenues.values())
        ranked = sorted(segment.revenues.items(), key=lambda x: x[1], reverse=True)
        for name, revenue in ranked[:top]:
            percentage = (revenue / total * 100) if total > 0 else 0
            out.append(f"| {name} | {fmt_money(revenue, currency)} | {percentage:.1f}% |")
        out.append("")
    return out
