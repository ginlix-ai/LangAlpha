"""Yahoo's side of the symbol boundary for this package.

Yahoo resolves Shanghai only as ``.SS``, where everything outside this package
writes ``.SH``. Every symbol handed to yfinance goes through one of these
helpers, so a new call site cannot forget the respell; a row Yahoo returns
goes back through ``display_spelling`` before it leaves the package.
"""

from __future__ import annotations

import yfinance as yf
from market_protocol import vendor_spelling


class _CookieCacheWithA3:
    """yfinance's cookie cache, less an entry without the ``A3`` cookie.

    yfinance reads the cached cookie's ``A3`` without checking it is there, so
    one saved without it fails every request in every later process with
    ``KeyError: 'A3'``. Such an entry reads as a miss and is deleted. The cache
    stays: yfinance refetches the cookie on every request without it. The
    sandbox servers carry the same guard in ``mcp_servers/_yf_common.py``,
    since this package is mirrored into sandboxes where that one may not import.
    """

    checks_a3 = True

    def __init__(self, inner):
        self._inner = inner

    def lookup(self, strategy):
        entry = self._inner.lookup(strategy)
        try:
            next(iter(entry["cookie"].values()))["/"]["A3"]
        except (TypeError, KeyError, StopIteration, AttributeError):
            if entry is not None:
                self._inner.store(strategy, None)  # a None store deletes the row
            return None
        return entry

    def store(self, strategy, cookie):
        self._inner.store(strategy, cookie)


def _guard_cookie_cache() -> None:
    try:
        from yfinance import cache
        current = cache.get_cookie_cache()
        if not getattr(current, "checks_a3", False):
            cache._CookieCacheManager._Cookie_cache = _CookieCacheWithA3(current)
    except (ImportError, AttributeError):
        pass  # a release that moved the cache; the load it guards moved with it


_guard_cookie_cache()


def yahoo_symbol(symbol: str, *, is_index: bool = False) -> str:
    """Yahoo's spelling of *symbol*; a bare index gets its caret (``^GSPC``).

    A venue-suffixed index (``000300.SS``) is already a Yahoo symbol and never
    takes a caret.
    """
    s = vendor_spelling(symbol)
    if not is_index or s.startswith("^") or "." in s:
        return s
    return f"^{s}"


def yahoo_ticker(symbol: str) -> yf.Ticker:
    return yf.Ticker(yahoo_symbol(symbol))


def yahoo_symbol_search(symbol: str, **kwargs) -> yf.Search:
    """A search keyed on a ticker; a free-text name query calls ``yf.Search`` as typed."""
    return yf.Search(yahoo_symbol(symbol), **kwargs)
