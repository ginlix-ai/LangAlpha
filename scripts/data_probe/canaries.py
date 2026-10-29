"""Canary baskets: the symbols each cell is probed with.

Small on purpose — a basket is a coverage question ("does this token serve
Beijing-listed equities at all?"), not a sample, and every extra symbol costs
a call against the tightest quota in the chain.
"""

from __future__ import annotations

# market -> asset_class -> canaries
CANARIES: dict[str, dict[str, list[str]]] = {
    "us": {
        "equity": ["AAPL", "MSFT"],
        "index": ["^GSPC"],
        # No fund basket: the protocol reads a US ETF as an equity, so SPY
        # routes through the equity cell and a fund cell would go unread.
    },
    "cn": {
        # One per listing venue: SSE, SZSE, BSE. BSE (.BJ) is the entitlement
        # edge — most tokens carry SSE/SZSE and stop there.
        "equity": ["600519.SS", "000858.SZ", "920300.BJ"],
        "index": ["000001.SS"],
        "fund": ["510300.SS"],
    },
    "hk": {
        "equity": ["0700.HK", "9988.HK"],
        "index": ["^HSI"],
    },
    "jp": {
        "equity": ["7203.T"],
        "index": ["^N225"],
    },
    "uk": {
        "equity": ["VOD.L"],
        "index": ["^FTSE"],
    },
    "eu": {
        "equity": ["SAP.DE"],
        "index": ["^GDAXI"],
    },
}

MARKETS: tuple[str, ...] = tuple(CANARIES)

ASSET_CLASS_ORDER: tuple[str, ...] = ("equity", "index", "fund")


def asset_classes_for(market: str) -> list[str]:
    basket = CANARIES.get(market, {})
    return [c for c in ASSET_CLASS_ORDER if basket.get(c)]


def canaries_for(market: str, asset_class: str) -> list[str]:
    return list(CANARIES.get(market, {}).get(asset_class, []))


def representative(market: str) -> str | None:
    """One symbol that stands for the market's clock (used by ``plan``)."""
    for cls in asset_classes_for(market):
        return CANARIES[market][cls][0]
    return None
