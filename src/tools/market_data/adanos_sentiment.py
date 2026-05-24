"""Adanos Market Sentiment API helpers for native finance tools."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

import httpx

ADANOS_API_BASE_URL = "https://api.adanos.org"
ADANOS_SOURCES = ("reddit", "x", "news", "polymarket")


def _parse_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()


def _normalize_sources(sources: list[str] | str | None) -> list[str]:
    if sources is None:
        requested = list(ADANOS_SOURCES)
    elif isinstance(sources, str):
        requested = [source.strip().lower() for source in sources.split(",") if source.strip()]
    else:
        requested = [str(source).strip().lower() for source in sources if str(source).strip()]

    invalid = [source for source in requested if source not in ADANOS_SOURCES]
    if invalid:
        raise ValueError(f"Unsupported Adanos source(s): {', '.join(invalid)}")
    return requested


def _sentiment_value(payload: dict[str, Any]) -> Any:
    for key in ("sentiment_score", "sentiment", "overall_sentiment", "average_sentiment"):
        if key in payload:
            return payload[key]
    return None


def _mentions_value(payload: dict[str, Any]) -> Any:
    for key in ("mentions", "mention_count", "total_mentions", "count"):
        if key in payload:
            return payload[key]
    return None


async def _fetch_adanos_json(
    *,
    path: str,
    params: dict[str, Any],
    api_key: str,
    base_url: str,
    timeout: float,
) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(
            f"{base_url.rstrip('/')}{path}",
            headers={"X-API-Key": api_key},
            params={key: value for key, value in params.items() if value not in (None, "")},
        )
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {"data": payload}


def _format_adanos_sentiment_report(
    *,
    symbol: str | None,
    mode: str,
    days: int,
    end_date: str | None,
    results: dict[str, dict[str, Any]],
    errors: dict[str, str],
) -> str:
    title = f"Adanos Market Sentiment: {symbol}" if symbol else "Adanos Market Sentiment: Market"
    lines = [f"## {title}", f"**Window:** {days} days"]
    if end_date:
        lines.append(f"**End date:** {end_date}")
    lines.append("")

    if results:
        lines.extend(["| Source | Sentiment | Mentions / Count | Status |", "|--------|-----------|------------------|--------|"])
        for source, payload in results.items():
            sentiment = _sentiment_value(payload)
            mentions = _mentions_value(payload)
            lines.append(
                f"| {source} | {sentiment if sentiment is not None else 'N/A'} | "
                f"{mentions if mentions is not None else 'N/A'} | ok |"
            )

    if errors:
        if results:
            lines.append("")
        lines.extend(["| Source | Error |", "|--------|-------|"])
        for source, message in errors.items():
            lines.append(f"| {source} | {message} |")

    if not results and not errors:
        lines.append("No Adanos sentiment data returned.")

    if mode == "market":
        lines.append("\nUse this as broad market mood context, not as a standalone trading signal.")
    else:
        lines.append("\nUse this as one external sentiment input alongside price, fundamentals, and news.")
    return "\n".join(lines)


async def fetch_adanos_market_sentiment(
    symbol: str | None = None,
    *,
    sources: list[str] | str | None = None,
    days: int = 7,
    end_date: str | None = None,
    mode: str = "stock",
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 20.0,
) -> tuple[str, dict[str, Any]]:
    """Fetch optional Adanos sentiment for a US equity or broad market."""
    resolved_api_key = (api_key or os.getenv("ADANOS_API_KEY") or "").strip()
    if not resolved_api_key:
        raise ValueError("ADANOS_API_KEY is required to fetch Adanos market sentiment data.")

    mode_text = str(mode or "stock").strip().lower()
    if mode_text not in {"stock", "market"}:
        raise ValueError("mode must be 'stock' or 'market'.")

    ticker = str(symbol or "").strip().upper()
    if mode_text == "stock" and not ticker:
        raise ValueError("symbol is required when mode='stock'.")

    day_count = max(int(days), 1)
    end_text = _parse_date(end_date) or datetime.now(timezone.utc).date().isoformat()
    requested_sources = _normalize_sources(sources)
    resolved_base_url = base_url or os.getenv("ADANOS_API_BASE_URL") or ADANOS_API_BASE_URL

    results: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    for source in requested_sources:
        path = (
            f"/{source}/stocks/v1/market-sentiment"
            if mode_text == "market"
            else f"/{source}/stocks/v1/stock/{ticker}"
        )
        try:
            results[source] = await _fetch_adanos_json(
                path=path,
                params={"days": day_count, "to": end_text},
                api_key=resolved_api_key,
                base_url=resolved_base_url,
                timeout=timeout,
            )
        except Exception as exc:
            errors[source] = str(exc)

    content = _format_adanos_sentiment_report(
        symbol=ticker if mode_text == "stock" else None,
        mode=mode_text,
        days=day_count,
        end_date=end_text,
        results=results,
        errors=errors,
    )
    artifact = {
        "type": "adanos_market_sentiment",
        "mode": mode_text,
        "symbol": ticker if mode_text == "stock" else None,
        "sources": requested_sources,
        "days": day_count,
        "end_date": end_text,
        "results": results,
        "errors": errors,
    }
    return content, artifact
