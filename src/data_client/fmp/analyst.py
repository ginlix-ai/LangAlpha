"""FMP's analyst rows in the shape the fundamentals tools publish.

FMP marks a metric nobody estimates with zeros and names no period on an
earnings report; both are settled here, once, for every caller.
"""

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..normalize import scale_price
from ..report_periods import report_period_ends

Row = Dict[str, Any]

# Each estimated metric, with the count of analysts behind it where FMP gives one.
_ESTIMATE_METRICS = {
    "revenue": "numAnalystsRevenue",
    "ebitda": None,
    "ebit": None,
    "netIncome": None,
    "sgaExpense": None,
    "eps": "numAnalystsEps",
}
_TARGET_CONSENSUS = {"high": "targetHigh", "low": "targetLow", "median": "targetMedian", "mean": "targetConsensus"}
_RATING_COUNTS = {
    "strongBuy": "analystRatingsStrongBuy",
    "buy": "analystRatingsBuy",
    "hold": "analystRatingsHold",
    "sell": "analystRatingsSell",
    "strongSell": "analystRatingsStrongSell",
}
_RATING_ACTION = ("date", "gradingCompany", "previousGrade", "newGrade", "action")
_SURPRISE = ("epsActual", "epsEstimated", "revenueActual", "revenueEstimated")


def target_consensus(rows: List[Row], places: int) -> List[Row]:
    return [{name: scale_price(row.get(key), places) for name, key in _TARGET_CONSENSUS.items()} for row in rows]


def rating_counts(rows: List[Row]) -> List[Row]:
    return [{"date": row.get("date"), **{name: row.get(key) for name, key in _RATING_COUNTS.items()}} for row in rows]


def rating_actions(rows: List[Row]) -> List[Row]:
    return [{key: row.get(key) for key in _RATING_ACTION} for row in rows]


def _local_day(stamp: Any, tz: Optional[str]) -> Optional[str]:
    """*stamp*'s calendar day where the listing trades; FMP stamps targets in UTC."""
    text = str(stamp or "")
    try:
        when = datetime.fromisoformat(text)
    except ValueError:
        return text[:10] or None
    if when.tzinfo is None or tz is None:
        return text[:10]
    return when.astimezone(ZoneInfo(tz)).date().isoformat()


def price_targets(rows: List[Row], places: int, tz: Optional[str]) -> List[Row]:
    """Published targets; FMP split-adjusts both the target and the price it was set against."""
    return [
        {
            "date": _local_day(row.get("publishedDate"), tz),
            "analystCompany": row.get("analystCompany"),
            "priceTarget": scale_price(row.get("adjPriceTarget"), places),
            "priceWhenPosted": scale_price(row.get("priceWhenPosted"), places),
            "newsTitle": row.get("newsTitle"),
            "newsURL": row.get("newsURL"),
        }
        for row in rows
    ]


def estimate_rows(rows: List[Row]) -> List[Row]:
    """Estimate rows with each metric nobody estimates as null: no analysts where
    FMP counts them (a zero consensus they agree on stays), else all zeros."""
    out = []
    for row in rows:
        shaped = {key: value for key, value in row.items() if key != "symbol"}
        for metric, analysts in _ESTIMATE_METRICS.items():
            keys = [f"{metric}{bound}" for bound in ("Avg", "Low", "High")]
            count = shaped.get(analysts) if analysts else None
            if count == 0 if count is not None else all(shaped.get(key) == 0 for key in keys):
                shaped.update(dict.fromkeys(keys))
        out.append(shaped)
    return out


def surprise_rows(reports: List[Row], statements: List[Row]) -> List[Row]:
    """Earnings reports, each with the period end it reported (``fiscalDateEnding``).

    Reported rows are aligned with the quarterly statements' period ends. Two
    reports on one day cannot be told apart, so neither is placed. A past row
    FMP never filled in is no report, so it is dropped, as the calendar does.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = [(str(report.get("date") or "")[:10], report) for report in reports]
    rows = [(day, report) for day, report in rows if day and (_reported(report) or day >= today)]
    reported = Counter(day for day, report in rows if _reported(report))
    ends = report_period_ends(
        [day for day, seen in reported.items() if seen == 1],
        [row.get("date") for row in statements],
        annual_ends=[row.get("date") for row in statements if row.get("period") == "Q4"],
    )
    return [
        {
            "date": day,
            "fiscalDateEnding": ends.get(day) if _reported(report) else None,
            **{key: report.get(key) for key in _SURPRISE},
        }
        for day, report in rows
    ]


def _reported(report: Row) -> bool:
    return report.get("epsActual") is not None or report.get("revenueActual") is not None
