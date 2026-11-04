"""How ginlix-data spells a symbol on its v1 routes and its realtime feed."""

from __future__ import annotations

from market_protocol import us_class_share


def stock_ticker(symbol: str) -> str:
    """ginlix-data's spelling of a US stock, which is Massive's: a class share
    takes a dot (BRK-B is BRK.B there, and BRK-B finds nothing)."""
    share = us_class_share(symbol.upper())
    return ".".join(share) if share else symbol
