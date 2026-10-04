# pyright: ignore
"""Company overview: profile, financials, analyst views and segments.

One gather (:func:`_gather_company`) fetches what an overview is drawn from and
one builder (:func:`_company_artifact`) shapes it, so the agent tool and the
REST endpoint cannot drift apart on what a field means. The agent additionally
reads :func:`_company_text`, from the same inputs.
"""

from dataclasses import dataclass, field
from typing import Any, Awaitable, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import logging
import asyncio

from langchain_core.runnables import RunnableConfig

from .currency import DisplaySpec, fmt_count, fmt_money, fmt_price
from .fiscal_periods import (
    fiscal_period_lookup,
    infer_fiscal_period,
    match_filing_to_fiscal_period,
)
from .display import (
    _is_us_clock,
    _market_status_line,
    _symbol_currency,
    resolve_listing,
)
from .index_overview import fetch_index_overview, index_overview_text
from .quote_format import (
    FRESHNESS_WORDS,
    build_live_stamp,
    current_price,
    stamp_provider_quote,
    stamp_quote,
)
from .segments import SegmentBreakdown, fetch_segments, segment_lines
from .utils import finite_or_none, format_percentage, get_market_session
from src.data_client import get_financial_data_provider, get_market_data_provider
from src.data_client.ginlix_data.directory import display_names
from market_protocol import AssetClass, InstrumentRef

from ._shared import _fmp_request, _get_user_id, _safe_result

logger = logging.getLogger(__name__)


# Provider key -> row label, in table order; the artifact keeps the keys.
_PERIODS = (
    ("1D", "1 Day"), ("5D", "5 Days"), ("1M", "1 Month"),
    ("3M", "3 Months"), ("6M", "6 Months"), ("ytd", "YTD"),
    ("1Y", "1 Year"), ("3Y", "3 Years"), ("5Y", "5 Years"),
)
_RATINGS = (
    ("strongBuy", "Strong Buy"), ("buy", "Buy"), ("hold", "Hold"),
    ("sell", "Sell"), ("strongSell", "Strong Sell"),
)
# A snapshot's market_status -> the "Market Status:" label.
_STATUS_LABELS = {
    "early_trading": "Pre-Market",
    "open": "Regular Hours",
    "late_trading": "After-Hours",
    "closed": "Market Closed",
}


def _reported_currency(rows: List[Dict], fallback: Optional[str]) -> Optional[str]:
    """ISO code the statement rows are reported in, else *fallback*.

    A statement is reported in the issuer's own currency, which need not be the
    listing currency, so a revenue or cash-flow figure cannot borrow the quote's
    symbol.
    """
    for row in rows or []:
        code = row.get("reportedCurrency")
        if code:
            return str(code)
    return fallback


def _margin(stmt: Dict, ratio_key: str, numerator_key: str) -> Optional[float]:
    """Margin fraction for an income-statement row.

    Prefers the provider's ratio field when present; FMP's stable API dropped
    the v3-era ``*Ratio`` fields, so otherwise it is derived from the raw
    dollar fields still in the payload (``numerator / revenue``).
    """
    ratio = stmt.get(ratio_key)
    if ratio is not None:
        return ratio
    revenue = stmt.get("revenue")
    numerator = stmt.get(numerator_key)
    if revenue and numerator is not None:
        return numerator / revenue
    return None


def _first(rows: Any) -> Optional[Dict[str, Any]]:
    return rows[0] if rows else None


def _plus(value: float) -> str:
    return "+" if value >= 0 else ""


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _kv_table(rows: List[Tuple[str, str]]) -> List[str]:
    return ["| Metric | Value |", "|--------|-------|", *(f"| {m} | {v} |" for m, v in rows)]


@dataclass
class CompanyInputs:
    """Everything one overview is drawn from, fetched once.

    A list endpoint that serves one record (performance, ratios, the provider
    quote) is held as that record, ``None`` when the list was empty. The
    ``extended`` fetches (filings, grades, the snapshot, float and short data)
    stay empty for a caller that does not ask for them.
    """

    symbol: str
    ref: Optional[InstrumentRef]
    cur: DisplaySpec
    profile: Dict[str, Any]
    name: str
    name_en: Optional[str]
    income: List[Dict[str, Any]]
    earnings: List[Dict[str, Any]]
    cash_flow: List[Dict[str, Any]]
    performance: Optional[Dict[str, Any]]
    metrics: Optional[Dict[str, Any]]
    ratios: Optional[Dict[str, Any]]
    price_target: Optional[Dict[str, Any]]
    ratings: Optional[Dict[str, Any]]
    quote: Optional[Dict[str, Any]]
    segments: SegmentBreakdown
    # Statement figures are in the issuer's reporting currency, which need
    # not be the listing one (CNY for 0700.HK).
    stmt_cur: Optional[str]
    cash_flow_cur: Optional[str]
    fiscal: Dict[str, str]
    snapshot: Optional[Dict[str, Any]] = None
    filings_10q: List[Dict[str, Any]] = field(default_factory=list)
    filings_10k: List[Dict[str, Any]] = field(default_factory=list)
    recent_grades: List[Dict[str, Any]] = field(default_factory=list)
    pt_summary: List[Dict[str, Any]] = field(default_factory=list)
    share_float: Optional[Dict[str, Any]] = None
    short_interest: Optional[Dict[str, Any]] = None
    short_volume: Optional[Dict[str, Any]] = None

    @property
    def has_snapshot(self) -> bool:
        return self.snapshot is not None and self.snapshot.get("price") is not None

    @property
    def price(self) -> Optional[float]:
        # A profile's price is the last PUBLISHED close, which for a venue whose
        # daily figures land after the bell is yesterday's for the whole session.
        # The snapshot is the only row that prints during one, so it owns the
        # headline price; the profile is the fallback when there is no snapshot.
        return current_price(self.snapshot or {}) or self.profile.get("price")


async def _first_row(rows_aw: Any) -> Optional[Dict[str, Any]]:
    return _first(await rows_aw)


async def _snapshot(symbol: str, user_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """The snapshot row, for its extended-hours split; None on any failure."""
    try:
        mdp = await get_market_data_provider()
        snaps = await mdp.get_snapshots([symbol], asset_type="stocks", user_id=user_id)
        return snaps[0] if snaps else None
    except Exception:
        return None


def _record_with(value: Any, key: str) -> Optional[Dict[str, Any]]:
    """*value* when it is a record carrying *key*, else None."""
    return value if isinstance(value, dict) and value.get(key) is not None else None


async def _gather_named(calls: Dict[str, Awaitable[Any]]) -> Dict[str, Any]:
    """Await *calls* in parallel, each result (or exception) under its own name."""
    results = await asyncio.gather(*calls.values(), return_exceptions=True)
    return dict(zip(calls, results))


async def _gather_company(
    provider: Any,
    symbol: str,
    ref: Optional[InstrumentRef],
    *,
    user_id: Optional[str] = None,
    extended: bool = False,
) -> Optional[CompanyInputs]:
    """Fetch an overview's inputs in parallel; None when there is no profile.

    ``extended`` adds SEC filings, grade history, the snapshot that splits out
    extended hours, and float and short data. The agent asks for them; the REST
    view does not, since its own client already holds a live snapshot. Both
    shape the artifact from whatever was fetched, so the agent's carries the
    snapshot quote and share structure the REST one leaves out.
    """
    financial, intel = provider.financial, provider.intel
    profile_rows = await financial.get_company_profile(symbol)
    if not profile_rows:
        return None
    profile = profile_rows[0]
    # CN/HK listings render local-name-first with the English name alongside.
    local_name, directory_en = await display_names(symbol)

    calls: Dict[str, Awaitable[Any]] = {
        "income": financial.get_income_statements(symbol, period="quarter", limit=8),
        "earnings": financial.get_earnings_history(symbol, limit=10),
        "performance": financial.get_price_performance(symbol),
        "metrics": financial.get_key_metrics(symbol),
        "ratios": financial.get_financial_ratios(symbol),
        "price_target": financial.get_analyst_price_targets(symbol),
        "ratings": financial.get_analyst_ratings(symbol),
        "segments": fetch_segments(financial, symbol),
        "quote": financial.get_realtime_quote(symbol),
        "cash_flow": financial.get_cash_flows(symbol, period="quarter", limit=8),
    }
    if extended:
        calls.update({
            "filings_10q": _fmp_request("get_sec_filings", symbol, filing_type="10-Q", limit=3),
            "filings_10k": _fmp_request("get_sec_filings", symbol, filing_type="10-K", limit=2),
            "grades": _fmp_request("get_stock_grades", symbol, limit=10),
            "pt_summary": _fmp_request("get_price_target_summary", symbol),
            "snapshot": _snapshot(symbol, user_id),
        })
        if intel is not None:
            calls.update({
                "share_float": intel.get_float_shares(symbol, user_id=user_id),
                "short_interest": _first_row(intel.get_short_interest(
                    symbol, limit=1, sort="settlement_date.desc", user_id=user_id)),
                "short_volume": _first_row(intel.get_short_volume(
                    symbol, limit=1, sort="date.desc", user_id=user_id)),
            })
    got = await _gather_named(calls)

    def listed(name: str) -> Any:
        return _safe_result(got.get(name), [])

    income, cash_flow = listed("income"), listed("cash_flow")
    cur = _symbol_currency(ref)
    stmt_cur = _reported_currency(income, _reported_currency(cash_flow, cur.currency))
    inputs = CompanyInputs(
        symbol=symbol,
        ref=ref,
        cur=cur,
        profile=profile,
        name=profile.get("nameLocal") or local_name or profile.get("companyName", symbol),
        name_en=profile.get("companyName") or directory_en,
        income=income,
        earnings=listed("earnings"),
        cash_flow=cash_flow,
        performance=_first(listed("performance")),
        metrics=_first(listed("metrics")),
        ratios=_first(listed("ratios")),
        price_target=_first(listed("price_target")),
        ratings=_first(listed("ratings")),
        quote=_first(listed("quote")),
        segments=listed("segments") or SegmentBreakdown(None, None),
        stmt_cur=stmt_cur,
        cash_flow_cur=_reported_currency(cash_flow, stmt_cur),
        fiscal=fiscal_period_lookup(income),
    )
    if extended:
        inputs.filings_10q = listed("filings_10q")
        inputs.filings_10k = listed("filings_10k")
        inputs.recent_grades = listed("grades")
        inputs.pt_summary = listed("pt_summary")
        inputs.snapshot = _safe_result(got.get("snapshot"), None)
        inputs.share_float = _record_with(_safe_result(got.get("share_float")), "free_float")
        inputs.short_interest = _record_with(_safe_result(got.get("short_interest")), "short_interest")
        inputs.short_volume = _record_with(_safe_result(got.get("short_volume")), "short_volume_ratio")
    return inputs


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------


def _artifact_quote(inp: CompanyInputs) -> Optional[Dict[str, Any]]:
    """The quote block: the snapshot's session split when one priced, else the
    provider quote. Both carry the provider quote's year range and valuation."""
    fmp = inp.quote or {}
    ratios = inp.ratios or {}
    # A CN quote (rt_k) carries no P/E; the TTM ratios do.
    valuation = {"pe": fmp.get("pe") or ratios.get("priceToEarningsRatioTTM"), "eps": fmp.get("eps")}
    if inp.has_snapshot:
        snap = inp.snapshot
        return {
            "regularClose": snap.get("price"),
            "lastTradePrice": snap.get("last_trade_price"),
            "marketStatus": snap.get("market_status"),
            "change": snap.get("change"),
            "changePct": snap.get("change_percent"),
            "regularChange": snap.get("regular_trading_change"),
            "regularChangePct": snap.get("regular_trading_change_percent"),
            "earlyTradingChangePct": snap.get("early_trading_change_percent"),
            "lateTradingChangePct": snap.get("late_trading_change_percent"),
            "dayHigh": snap.get("high"),
            "dayLow": snap.get("low"),
            "yearHigh": fmp.get("yearHigh"),
            "yearLow": fmp.get("yearLow"),
            "open": snap.get("open"),
            "previousClose": snap.get("previous_close"),
            "volume": snap.get("volume"),
            "avgVolume": fmp.get("avgVolume"),
            "marketCap": fmp.get("marketCap"),
            **valuation,
            **stamp_quote(snap, ref=inp.ref),
        }
    if inp.quote is None:
        return None
    return {
        "price": fmp.get("price"),
        "change": fmp.get("change"),
        "changePct": fmp.get("changePercentage"),
        "dayHigh": fmp.get("dayHigh"),
        "dayLow": fmp.get("dayLow"),
        "yearHigh": fmp.get("yearHigh"),
        "yearLow": fmp.get("yearLow"),
        "open": fmp.get("open"),
        "previousClose": fmp.get("previousClose"),
        "volume": fmp.get("volume"),
        "avgVolume": fmp.get("avgVolume"),
        "marketCap": fmp.get("marketCap"),
        **valuation,
        **stamp_provider_quote(fmp, inp.symbol, inp.ref),
    }


def _company_artifact(inp: CompanyInputs) -> Dict[str, Any]:
    """The ``company_overview`` artifact: series oldest-first, for charting."""
    lookup = inp.fiscal
    artifact: Dict[str, Any] = {
        "type": "company_overview",
        "symbol": inp.symbol,
        "name": inp.name,
        "nameEn": inp.name_en if inp.name_en and inp.name_en != inp.name else None,
        "currency": inp.cur.currency,
        "reportedCurrency": inp.stmt_cur,
    }
    quote = _artifact_quote(inp)
    if quote is not None:
        artifact["quote"] = quote
    # Single latest records, not full histories.
    for key, record in (
        ("float", inp.share_float),
        ("shortInterest", inp.short_interest),
        ("shortVolume", inp.short_volume),
    ):
        if record is not None:
            artifact[key] = record
    if inp.performance is not None:
        changes = inp.performance
        artifact["performance"] = {
            k: changes.get(k) for k, _ in _PERIODS if changes.get(k) is not None
        }
    if inp.ratings is not None:
        artifact["analystRatings"] = {
            **{k: inp.ratings.get(k, 0) for k, _ in _RATINGS},
            "consensus": inp.ratings.get("consensus", "N/A"),
        }
    if inp.segments.product is not None:
        artifact["revenueByProduct"] = inp.segments.product.revenues
    if inp.segments.geo is not None:
        artifact["revenueByGeo"] = inp.segments.geo.revenues
    if inp.income:
        artifact["quarterlyFundamentals"] = [
            {
                "period": lookup.get(stmt.get("date"), stmt.get("date", "")),
                "date": stmt.get("date"),
                "revenue": stmt.get("revenue"),
                "netIncome": stmt.get("netIncome"),
                "grossProfit": stmt.get("grossProfit"),
                "operatingIncome": stmt.get("operatingIncome"),
                "ebitda": stmt.get("ebitda"),
                "epsDiluted": stmt.get("epsdiluted"),
                "grossMargin": _margin(stmt, "grossProfitRatio", "grossProfit"),
                "operatingMargin": _margin(stmt, "operatingIncomeRatio", "operatingIncome"),
                "netMargin": _margin(stmt, "netIncomeRatio", "netIncome"),
            }
            for stmt in reversed(inp.income)
        ]
    reported = [e for e in inp.earnings if e.get("epsActual") is not None]
    if reported:
        artifact["earningsSurprises"] = [
            {
                "period": lookup.get(e.get("fiscalDateEnding"), e.get("date", "")),
                "date": e.get("date"),
                "epsActual": e.get("epsActual"),
                "epsEstimate": e.get("epsEstimated"),
                "revenueActual": e.get("revenueActual"),
                "revenueEstimate": e.get("revenueEstimated"),
            }
            for e in reversed(reported)
        ]
    if inp.cash_flow:
        artifact["cashFlow"] = [
            {
                "period": lookup.get(cf.get("date"), cf.get("date", "")),
                "date": cf.get("date"),
                "operatingCashFlow": cf.get("operatingCashFlow"),
                "capitalExpenditure": cf.get("capitalExpenditure"),
                "freeCashFlow": cf.get("freeCashFlow"),
            }
            for cf in reversed(inp.cash_flow)
        ]
    return artifact


# ---------------------------------------------------------------------------
# Agent text
# ---------------------------------------------------------------------------


def _quote_detail_rows(
    cur: DisplaySpec, open_: Any, prev_close: Any, low: Any, high: Any,
    year_low: Any, year_high: Any, volume: Any, avg_volume: Any,
) -> List[Tuple[str, str]]:
    rows = []
    if open_:
        rows.append(("Open", fmt_price(open_, cur)))
    if prev_close:
        rows.append(("Previous Close", fmt_price(prev_close, cur)))
    if low and high:
        rows.append(("Day Range", f"{fmt_price(low, cur)} - {fmt_price(high, cur)}"))
    if year_low and year_high:
        rows.append(("52-Week Range", f"{fmt_price(year_low, cur)} - {fmt_price(year_high, cur)}"))
    if volume:
        vol_str = fmt_count(volume)
        rows.append(("Volume", f"{vol_str} (Avg: {fmt_count(avg_volume)})" if avg_volume else vol_str))
    return rows


def _snapshot_price_lines(snap: Dict[str, Any], status: str, cur: DisplaySpec) -> List[str]:
    """Regular close, the extended-hours print off it, and the day's move."""
    out = []
    reg_close = snap.get("price")  # session.close = regular session close
    last_price = snap.get("last_trade_price")  # actual current price
    reg_change = snap.get("regular_trading_change")
    reg_change_pct = snap.get("regular_trading_change_percent")
    if reg_close is not None:
        if reg_change is not None and reg_change_pct is not None:
            sign = _plus(reg_change)
            out.append(
                f"**Regular Close:** {fmt_price(reg_close, cur)} ({sign}{reg_change:.2f} / {sign}{reg_change_pct:.3f}%)"
            )
        else:
            out.append(f"**Regular Close:** {fmt_price(reg_close, cur)}")

    is_extended = status in ("early_trading", "late_trading")
    if is_extended and last_price is not None and reg_close is not None and last_price != reg_close:
        ext_label = "Pre-Market" if status == "early_trading" else "After-Hours"
        ext_change = snap.get(f"{status}_change")
        ext_change_pct = snap.get(f"{status}_change_percent")
        if ext_change is not None and ext_change_pct is not None:
            sign = _plus(ext_change)
            out.append(
                f"**{ext_label} Price:** {fmt_price(last_price, cur)} ({sign}{ext_change:.2f} / {sign}{ext_change_pct:.3f}% from close)"
            )
        else:
            diff = last_price - reg_close
            diff_pct = (diff / reg_close * 100) if reg_close else 0
            sign = _plus(diff)
            out.append(
                f"**{ext_label} Price:** {fmt_price(last_price, cur)} ({sign}{diff:.2f} / {sign}{diff_pct:.2f}% from close)"
            )

    total_change = snap.get("change")
    total_change_pct = snap.get("change_percent")
    if total_change is not None and total_change_pct is not None:
        sign = _plus(total_change)
        out.append(
            f"**Day Change (from prev close):** {sign}{total_change:.2f} / {sign}{total_change_pct:.3f}%"
        )
    return out + [""]


def _quote_heading(freshness: Dict[str, Any]) -> str:
    """The quote section's title, naming any freshness short of current.

    Only a row measured or declared current is called real-time. A delayed one
    says so, and a provider quote that carries neither a tier nor a print time
    reads as unknown rather than borrowing the title.
    """
    words = FRESHNESS_WORDS.get(freshness.get("label"))
    return f"### Quote ({words})" if words else "### Real-Time Quote"


def _provider_price_line(fmp: Dict[str, Any], cur: DisplaySpec) -> Optional[str]:
    """The provider quote's price and move; a null field drops out, never 0."""
    price = finite_or_none(fmp.get("price"))
    if price is None:
        return None
    change = finite_or_none(fmp.get("change"))
    change_pct = finite_or_none(fmp.get("changePercentage"))
    move = [
        *([f"{_plus(change)}{change:.2f}"] if change is not None else []),
        *([f"{_plus(change_pct)}{change_pct:.2f}%"] if change_pct is not None else []),
    ]
    line = f"**Price:** {fmt_price(price, cur)}"
    return f"{line} ({' / '.join(move)})" if move else line


def _quote_lines(inp: CompanyInputs) -> List[str]:
    """The quote: the snapshot's extended-hours breakdown when it priced, else
    the provider quote, titled by how current the shown row is."""
    snap = inp.snapshot if inp.has_snapshot else None
    fmp = inp.quote
    if snap is None and fmp is None:
        return []
    cur = inp.cur
    session_name, current_time_et = get_market_session()
    status = snap.get("market_status", "") if snap is not None else ""
    label = _STATUS_LABELS.get(status, session_name.replace("_", " ").title())
    stamp = (
        stamp_quote(snap, ref=inp.ref) if snap is not None
        else stamp_provider_quote(fmp, inp.symbol, inp.ref)
    )
    out = [_quote_heading(stamp["freshness"])]
    # US: snapshot/session label + ET clock. Non-US: phase from the exchange
    # calendar + exchange-local clock (the US-Eastern phase is meaningless for a
    # foreign listing; snapshot.market_status is US-centric).
    status_line = _market_status_line(inp.ref, _is_us_clock(inp.ref), label, current_time_et)
    if status_line:
        out.append(status_line)
    out.append("")

    if snap is not None:
        out += _snapshot_price_lines(snap, status, cur)
        # The snapshot has no 52-week range or average volume; the provider quote does.
        fmp = fmp or {}
        rows = _quote_detail_rows(
            cur, snap.get("open"), snap.get("previous_close"), snap.get("low"), snap.get("high"),
            fmp.get("yearLow"), fmp.get("yearHigh"), snap.get("volume"), fmp.get("avgVolume"),
        )
    else:
        price_line = _provider_price_line(fmp, cur)
        if price_line:
            out += [price_line, ""]
        rows = _quote_detail_rows(
            cur, fmp.get("open"), fmp.get("previousClose"), fmp.get("dayLow"), fmp.get("dayHigh"),
            fmp.get("yearLow"), fmp.get("yearHigh"), fmp.get("volume"), fmp.get("avgVolume"),
        )
    if rows:
        out += _kv_table(rows) + [""]
    return out


def _share_structure_lines(inp: CompanyInputs) -> List[str]:
    share_float, si, sv = inp.share_float, inp.short_interest, inp.short_volume
    if share_float is None and si is None and sv is None:
        return []
    rows = []
    free_float = (share_float or {}).get("free_float")
    if share_float is not None:
        if free_float:
            rows.append(("Float", fmt_count(free_float)))
        ff_pct = share_float.get("free_float_percent")
        if ff_pct is not None:
            rows.append(("Float %", f"{ff_pct:.1f}%"))
    if si is not None:
        si_val = si["short_interest"]
        si_date = si.get("settlement_date", "")
        si_str = f"{si_val:,.0f}"  # a share count, whatever numeric type it arrives as
        rows.append(("Short Interest", f"{si_str} (as of {si_date})" if si_date else si_str))
        if free_float:
            rows.append(("Short % of Float", f"{si_val / free_float * 100:.2f}%"))
        dtc = si.get("days_to_cover")
        if dtc:
            rows.append(("Days to Cover", f"{dtc:.2f}"))
    if sv is not None:
        sv_ratio = sv["short_volume_ratio"]
        sv_date = sv.get("date", "")
        rows.append(("Short Volume Ratio", f"{sv_ratio:.1f}% (as of {sv_date})" if sv_date else f"{sv_ratio:.1f}%"))
    out = ["### Share Structure", ""]
    if rows:
        out += _kv_table(rows) + [""]
    return out


def _performance_lines(inp: CompanyInputs) -> List[str]:
    changes = inp.performance
    if changes is None:
        return []
    rows = [
        (label, format_percentage(changes.get(k)))
        for k, label in _PERIODS
        if changes.get(k) is not None
    ]
    out = ["### Stock Price Performance", ""]
    if rows:
        out += ["| Period | Performance |", "|--------|-------------|"]
        out += [f"| {period} | {perf} |" for period, perf in rows] + [""]
    return out


def _metrics_lines(inp: CompanyInputs) -> List[str]:
    # Most rows come from ratios, so either endpoint is enough to render the
    # table: one failing among the parallel fetches drops only its own rows.
    if inp.metrics is None and inp.ratios is None:
        return []
    metrics, ratios = inp.metrics or {}, inp.ratios or {}
    rows: List[Tuple[str, str]] = []

    def row(label: str, value: Any, fmt: str, *, zero_is_real: bool = False, pct: bool = False) -> None:
        # A ratio over a zero denominator can arrive as Infinity, a float or a
        # string the yfinance fallback passes through untouched. One prints
        # "infx", the other raises and replaces the whole overview with an error.
        value = finite_or_none(value)
        if value is None or (not value and not zero_is_real):
            return
        rows.append((label, fmt.format(value * 100 if pct else value)))

    # Field names are FMP stable's (key-metrics-ttm, ratios-ttm), which the
    # yfinance fallback emits too. Stable moved the valuation multiples to
    # ratios and ROE/ROA to key metrics.
    # FMP writes 0 when a ratio's denominator is zero: Apple reports no
    # interest expense, so its interest coverage comes back 0. A price or EV
    # multiple and a coverage ratio are never really 0, so those rows drop a 0
    # like a missing value.
    row("P/E Ratio", ratios.get("priceToEarningsRatioTTM"), "{:.2f}x")
    row("P/B Ratio", ratios.get("priceToBookRatioTTM"), "{:.2f}x")
    row("PEG Ratio", ratios.get("priceToEarningsGrowthRatioTTM"), "{:.2f}")
    row("EV/OCF", metrics.get("evToOperatingCashFlowTTM"), "{:.2f}x")
    # A reported 0 is real for returns, margins and the balance-sheet ratios
    # (a break-even margin, a debt-free company). Always fractions: a ROE of
    # 1.55 is 155%.
    row("ROE (Return on Equity)", metrics.get("returnOnEquityTTM"), "{:.2f}%", zero_is_real=True, pct=True)
    row("ROA (Return on Assets)", metrics.get("returnOnAssetsTTM"), "{:.2f}%", zero_is_real=True, pct=True)
    row("Net Profit Margin", ratios.get("netProfitMarginTTM"), "{:.2f}%", zero_is_real=True, pct=True)
    row("Operating Margin", ratios.get("operatingProfitMarginTTM"), "{:.2f}%", zero_is_real=True, pct=True)
    row("Debt/Equity Ratio", ratios.get("debtToEquityRatioTTM"), "{:.2f}", zero_is_real=True)
    row("Current Ratio", ratios.get("currentRatioTTM"), "{:.2f}", zero_is_real=True)
    row("Quick Ratio", ratios.get("quickRatioTTM"), "{:.2f}", zero_is_real=True)
    row("Interest Coverage", ratios.get("interestCoverageRatioTTM"), "{:.2f}x")

    out = ["### Key Financial Metrics (TTM)", "*Data based on Trailing Twelve Months*", ""]
    out += _kv_table(rows) if rows else ["*No financial metrics available*"]
    return out + [""]


def _filing_day(filing: Dict[str, Any]) -> str:
    day = filing.get("filingDate", "N/A")
    return day.split(" ")[0] if day and " " in day else day


def _filing_lines(inp: CompanyInputs) -> List[str]:
    if not (inp.filings_10q or inp.filings_10k):
        return []
    lookup = inp.fiscal
    out = [
        "### SEC Filing Dates",
        "",
        "| Filing Type | Filing Date | Fiscal Period |",
        "|-------------|-------------|---------------|",
    ]
    for filing in inp.filings_10k[:1]:  # Just the latest
        # A 10-K includes Q4, so it is labeled with the latest fiscal Q4.
        fiscal_period = next(
            (f"{name} (Annual)" for _, name in sorted(lookup.items(), reverse=True) if name.startswith("Q4")),
            "Annual",
        )
        out.append(f"| **10-K** | {_filing_day(filing)} | {fiscal_period} |")
    for filing in inp.filings_10q[:3]:  # Last 3 quarterly reports
        filing_date = _filing_day(filing)
        fiscal_period = match_filing_to_fiscal_period(filing_date, inp.earnings, lookup)
        out.append(f"| **10-Q** (Quarterly) | {filing_date} | {fiscal_period} |")
    out.append("")
    # US stocks carry no exchange suffix (.SH, .SZ, .HK, etc.).
    if "." not in inp.symbol or inp.symbol.endswith(".US"):
        out += [
            "*Tip: Use `get_sec_filing` tool to fetch complete earnings call transcripts and SEC filings.*",
            "",
        ]
    return out


def _next_earnings_lines(inp: CompanyInputs) -> List[str]:
    # Upcoming = not yet reported AND not in the past. A missing epsActual on a
    # past row means "never populated", not "upcoming".
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    upcoming = [
        cal
        for cal in inp.earnings
        if cal.get("epsActual") is None and cal.get("date") and str(cal.get("date")) >= today_iso
    ]
    if not upcoming:
        return []
    upcoming.sort(key=lambda x: x.get("date", "9999-99-99"))
    nxt = upcoming[0]
    lookup, earn_cur = inp.fiscal, _earnings_currency(inp)
    fiscal_ending = nxt.get("fiscalDateEnding", "N/A")
    eps_estimate = nxt.get("epsEstimated")
    rev_estimate = nxt.get("revenueEstimated")
    period_name = lookup.get(fiscal_ending)
    if not period_name and fiscal_ending != "N/A":
        period_name = infer_fiscal_period(fiscal_ending, lookup)
    time_desc = {
        "amc": " (After Market Close)",
        "bmo": " (Before Market Open)",
    }.get(nxt.get("time", ""), "")
    out = [
        "### Next Earnings Report",
        "",
        f"**Report Date:** {nxt.get('date', 'N/A')}{time_desc}",
        f"**Fiscal Period:** {period_name or 'N/A'}",
        f"**Fiscal Period End:** {fiscal_ending}",
    ]
    if eps_estimate is not None:
        out.append(f"**EPS Estimate:** {fmt_price(eps_estimate, earn_cur)}")
    if rev_estimate is not None:
        out.append(f"**Revenue Estimate:** {fmt_money(rev_estimate, earn_cur)}")
    return out + [""]


def _earnings_currency(inp: CompanyInputs) -> Optional[str]:
    """Earnings rows are consensus figures in the listing's trading currency,
    not the statements': TSM's EPS is USD per ADS while it reports in TWD."""
    return inp.cur.currency


def _earnings_lines(inp: CompanyInputs) -> List[str]:
    reported = [e for e in inp.earnings if e.get("epsActual") is not None]
    if not reported:
        return []
    lookup, earn_cur = inp.fiscal, _earnings_currency(inp)
    latest = reported[0]
    announce_date = latest.get("date", "N/A")
    fiscal_ending = latest.get("fiscalDateEnding")
    eps_actual, eps_estimate = latest.get("epsActual"), latest.get("epsEstimated")
    revenue_actual, revenue_estimate = latest.get("revenueActual"), latest.get("revenueEstimated")
    fiscal_label = lookup.get(fiscal_ending, "") if fiscal_ending else ""
    latest_label = f"{announce_date} ({fiscal_label})" if fiscal_label else announce_date
    out = ["### Earnings Performance", "", f"**Latest Quarter ({latest_label}):**", ""]

    if eps_estimate and eps_estimate != 0:
        eps_surprise = ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100
        out.append(
            f"- **EPS:** {fmt_price(eps_actual, earn_cur)} actual vs {fmt_price(eps_estimate, earn_cur)} estimate ({format_percentage(eps_surprise)} surprise)"
        )
    else:
        out.append(f"- **EPS:** {fmt_price(eps_actual, earn_cur)} (no estimate available)")
    if revenue_actual is not None:
        if revenue_estimate and revenue_estimate != 0:
            rev_surprise = ((revenue_actual - revenue_estimate) / abs(revenue_estimate)) * 100
            out.append(
                f"- **Revenue:** {fmt_money(revenue_actual, earn_cur)} actual vs "
                f"{fmt_money(revenue_estimate, earn_cur)} estimate "
                f"({format_percentage(rev_surprise)} surprise)"
            )
        else:
            out.append(f"- **Revenue:** {fmt_money(revenue_actual, earn_cur)} (no estimate available)")

    if len(reported) > 1:
        out += [
            "",
            "**Recent Earnings Trend:**",
            "",
            "| Date | Fiscal Period | EPS | Revenue |",
            "|------|---------------|-----|---------|",
        ]
        for quarter in reported[:4]:
            q_fiscal_ending = quarter.get("fiscalDateEnding")
            q_label = lookup.get(q_fiscal_ending, "N/A") if q_fiscal_ending else "N/A"
            out.append(
                f"| {quarter.get('date', 'N/A')} | {q_label} | {fmt_price(quarter.get('epsActual'), earn_cur)}"
                f" | {fmt_money(quarter.get('revenueActual'), earn_cur)} |"
            )
    return out + [""]


def _cash_flow_lines(inp: CompanyInputs) -> List[str]:
    if not inp.cash_flow:
        return []
    cur = inp.cash_flow_cur
    out = [
        "### Cash Flow (Quarterly)",
        "",
        "| Period | Operating CF | CapEx | Free CF |",
        "|--------|-------------|-------|---------|",
    ]
    for cf in inp.cash_flow[:8]:
        cf_date = cf.get("date", "N/A")
        out.append(
            f"| {inp.fiscal.get(cf_date, cf_date)} | {fmt_money(cf.get('operatingCashFlow'), cur)}"
            f" | {fmt_money(cf.get('capitalExpenditure'), cur)} | {fmt_money(cf.get('freeCashFlow'), cur)} |"
        )
    return out + [""]


def _analyst_lines(inp: CompanyInputs) -> List[str]:
    cur, price = inp.cur, inp.price
    out = ["### Analyst Consensus & Ratings", ""]

    pt = inp.price_target
    if pt is not None:
        median, low, high = pt.get("targetMedian"), pt.get("targetLow"), pt.get("targetHigh")
        consensus = pt.get("targetConsensus")
        out += ["**Price Targets:**", ""]
        pt_rows = []
        if median and price:
            upside = (median - price) / price * 100
            pt_rows.append(
                ("Consensus Target", f"{fmt_price(median, cur)} ({_plus(upside)}{upside:.1f}% from current)")
            )
        if low and high:
            pt_rows.append(("Target Range", f"{fmt_price(low, cur)} - {fmt_price(high, cur)}"))
        if consensus:
            # Numeric on every current provider, where it is a price and has to
            # carry the listing currency; a provider that words it instead
            # still prints as given.
            numeric = finite_or_none(consensus)
            pt_rows.append((
                "Analyst Consensus",
                fmt_price(numeric, cur) if numeric is not None else str(consensus),
            ))
        if pt_rows:
            out += [f"- **{label}:** {value}" for label, value in pt_rows] + [""]

    gs = inp.ratings
    if gs is not None:
        counts = [(label, gs.get(key, 0)) for key, label in _RATINGS]
        total = sum(n for _, n in counts)
        if total > 0:
            out += [
                "**Rating Distribution:**",
                "",
                "| Rating | Count | Percentage |",
                "|--------|-------|------------|",
            ]
            out += [f"| {label} | {n} | {n / total * 100:.1f}% |" for label, n in counts if n > 0]
            out += ["", f"**Overall Consensus:** {gs.get('consensus', 'N/A').upper()}", ""]

    if inp.recent_grades:
        out += ["**Recent Analyst Actions:**", "", "| Date | Firm | Action |", "|------|------|--------|"]
        for grade in inp.recent_grades[:5]:
            new_grade = grade.get("newGrade", "N/A")
            previous_grade = grade.get("previousGrade", "")
            action = grade.get("action", "N/A")
            if previous_grade and previous_grade != new_grade:
                action_str = f"{action} to {new_grade} (from {previous_grade})"
            else:
                action_str = f"{action} {new_grade}"
            out.append(f"| {grade.get('date', 'N/A')} | {grade.get('gradingCompany', 'N/A')} | {action_str} |")
        out.append("")

    if inp.pt_summary:
        out += ["**Top Analyst Firms:**", "", "| Firm | Analyst | Price Target |", "|------|---------|--------------|"]
        for firm in inp.pt_summary[:5]:
            target = firm.get("adjPriceTarget")
            target_str = fmt_price(target, cur) if target else "N/A"
            out.append(f"| {firm.get('analystCompany', 'N/A')} | {firm.get('analystName', '-')} | {target_str} |")
        out.append("")
    return out


def _company_text(inp: CompanyInputs) -> str:
    """The agent's markdown overview, led by a live stamp while the venue is open."""
    cur, price = inp.cur, inp.price
    profile = inp.profile
    market_cap = fmt_money(profile.get("marketCap"), cur)
    out = [
        f"## Company Overview: {inp.symbol}",
        f"**Company:** {inp.name}",
        f"**Retrieved:** {_utc_stamp()}",
        f"**Market:** {profile.get('exchangeShortName') or profile.get('exchange') or 'N/A'}",
        "",
        f"Company: {inp.name} ({inp.symbol})",
        f"Sector: {profile.get('sector') or 'N/A'} | Industry: {profile.get('industry') or 'N/A'}",
        f"Market Cap: {market_cap} | Current Price: {fmt_price(price, cur)}"
        if price
        else f"Market Cap: {market_cap}",
        "",
        *_quote_lines(inp),
        *_share_structure_lines(inp),
        *_performance_lines(inp),
        *_metrics_lines(inp),
        *_filing_lines(inp),
        *_next_earnings_lines(inp),
        *_earnings_lines(inp),
        *_cash_flow_lines(inp),
        *_analyst_lines(inp),
        *segment_lines(inp.segments, inp.fiscal, inp.stmt_cur),
    ]
    # Best-effort: a malformed snapshot must never blow away the assembled overview.
    stamp = None
    if inp.snapshot:
        try:
            stamp = build_live_stamp([inp.snapshot], asset_class=AssetClass.EQUITY)
        except Exception:
            stamp = None
    if stamp:
        out.insert(0, stamp + "\n")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


async def fetch_company_overview_data(symbol: str) -> Dict[str, Any]:
    """The overview artifact for the REST endpoint, the agent tool's shape.

    Raises on a provider failure, which the endpoint turns into its error
    response; a symbol with no profile answers a bare artifact.
    """
    # Equity-hinted like the REST boundary: a ticker that collides with an index
    # alias (COMP) is the company; ^GSPC and 000300.SH still resolve as indexes.
    ref, symbol = resolve_listing(symbol, AssetClass.EQUITY)
    if ref is not None and ref.asset_class is AssetClass.INDEX:
        return await fetch_index_overview(symbol, ref)
    provider = await get_financial_data_provider()
    if provider.financial is None:
        return {"type": "company_overview", "symbol": symbol}
    inputs = await _gather_company(provider, symbol, ref)
    if inputs is None:
        return {"type": "company_overview", "symbol": symbol}
    return _company_artifact(inputs)


def _error_text(symbol: str, message: str) -> str:
    return f"## Company Overview: {symbol}\n**Retrieved:** {_utc_stamp()}\n**Status:** Error\n\n{message}"


async def fetch_company_overview(
    symbol: str,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch comprehensive investment analysis overview for a company.

    Retrieves and formats investment-relevant data including financial health ratings,
    analyst consensus, earnings performance, and revenue segmentation.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SH", "0700.HK")
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    try:
        provider = await get_financial_data_provider()
        # Equity-hinted like the REST boundary (see fetch_company_overview_data).
        ref, symbol = resolve_listing(symbol, AssetClass.EQUITY)
        if ref is not None and ref.asset_class is AssetClass.INDEX:
            artifact = await fetch_index_overview(symbol, ref, user_id=_get_user_id(config))
            return index_overview_text(artifact, _symbol_currency(ref)), artifact
        if provider.financial is None:
            content = _error_text(symbol, "No financial data source configured")
            return content, {"type": "company_overview", "symbol": symbol}
        inputs = await _gather_company(
            provider, symbol, ref, user_id=_get_user_id(config), extended=True
        )
        if inputs is None:
            content = _error_text(symbol, f"No data found for symbol {symbol}")
            return content, {"type": "company_overview", "symbol": symbol}
        result = _company_text(inputs)
        logger.debug(f"Retrieved comprehensive investment overview for {symbol}")
        return result, _company_artifact(inputs)

    except Exception as e:
        logger.error(f"Error retrieving company overview for {symbol}: {e}")
        content = _error_text(symbol, f"Error retrieving company overview: {str(e)}")
        return content, {"type": "company_overview", "symbol": symbol, "error": str(e)}
