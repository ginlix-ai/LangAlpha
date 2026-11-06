"""Currency-aware display formatting for market-data tool output.

Field names and numeric values stay currency-neutral; only human-readable
display strings gain a currency prefix. USD keeps the ``$`` it always printed,
but a negative value now leads with its sign (``-$1.23``, where the old
formatting wrote ``$-1.23``) and one that rounds to zero drops it.
"""

from typing import NamedTuple, Optional, Union

# ISO 4217 code -> display prefix. Unknown codes fall back to "<ISO> " so a
# value renders as e.g. "CHF 12.34".
_SYMBOLS = {
    "USD": "$",
    "GBP": "£",
    "HKD": "HK$",
    "EUR": "€",
    "JPY": "¥",
    "CNY": "CN¥",
}


class DisplaySpec(NamedTuple):
    """Currency code + display precision for one instrument.

    ``decimals`` is resolved from the protocol (``display_decimals_for``) so the
    formatters carry no currency-specific precision table of their own.
    """

    currency: Optional[str]
    decimals: int
    # No currency prefix: an index level is points, and a listing on a venue
    # the protocol does not know has no known currency.
    bare: bool = False
    # Zeros past the second decimal are dropped: a coin carries eight places
    # for SHIB's $0.00000464, which would pad BTC to $76,810.32810000.
    trim: bool = False


# What the formatters accept for their second argument: a resolved spec, a bare
# ISO 4217 code, or ``None`` (both bare forms default to 2 decimals).
CurrencyArg = Union[DisplaySpec, str, None]


def quote_spec(spec: DisplaySpec, price: Optional[float]) -> DisplaySpec:
    """*spec* for quoting *price*: under one unit it keeps four places, since a US
    stock under a dollar quotes in hundredths of a cent and a Hong Kong penny
    stock in tenths of a cent. An A-share under a yuan still ticks in fen, so a
    yuan price keeps its own places."""
    if price is None or abs(price) >= 1 or spec.currency == "CNY":
        return spec
    return spec._replace(decimals=max(spec.decimals, 4))


def _spec(currency: CurrencyArg) -> DisplaySpec:
    """Coerce a bare code / ``None`` to a 2-decimal spec; pass specs through."""
    if isinstance(currency, DisplaySpec):
        return currency
    return DisplaySpec(currency, 2)


def _sign(value: float, decimals: int) -> tuple[str, float]:
    """Split the sign off so it leads the symbol ("-CN¥1.23", never "CN¥-1.23").

    A value that rounds to zero at the shown precision drops its sign.
    """
    if value < 0 and round(value, decimals) != 0:
        return "-", -value
    return "", abs(value)


def currency_symbol(code: Optional[str]) -> str:
    """Display prefix for an ISO 4217 code; ``None`` -> USD "$".

    Unknown codes return "<ISO> " (trailing space) so concatenation yields
    "CHF 12.34".
    """
    if not code:
        return "$"
    code = code.upper()
    return _SYMBOLS.get(code, f"{code} ")


def fmt_price(
    value: Optional[float],
    currency: CurrencyArg = None,
    decimals: Optional[int] = None,
    group: bool = False,
) -> str:
    """Format a price with its currency prefix, e.g. "£0.99", "HK$318.20".

    ``currency`` is a :class:`DisplaySpec` (currency + protocol decimals) or a
    bare ISO 4217 code (2 decimals). ``decimals`` overrides the spec when given.
    ``group=True`` adds thousands separators ("$6,120.50"). ``None`` value -> "N/A".
    """
    if value is None:
        return "N/A"
    spec = _spec(currency)
    dec = spec.decimals if decimals is None else decimals
    grouping = "," if group else ""
    sign, value = _sign(value, dec)
    prefix = "" if spec.bare else currency_symbol(spec.currency)
    text = f"{value:{grouping}.{dec}f}"
    if spec.trim and dec > 2:
        whole, _, places = text.partition(".")
        text = f"{whole}.{places.rstrip('0').ljust(2, '0')}"
    return f"{sign}{prefix}{text}"


def fmt_quote(value: Optional[float], currency: CurrencyArg = None, group: bool = False) -> str:
    """:func:`fmt_price` for a traded price, at the precision it quotes in
    (:func:`quote_spec`): each figure is judged by its own size, so a 52-week
    high above a dollar keeps two places beside a sub-dollar last trade."""
    if value is None:
        return "N/A"
    return fmt_price(value, quote_spec(_spec(currency), value), group=group)


def _scaled(value: float, prefix: str, places: int = 2) -> str:
    """The one magnitude ladder (T/B/M, grouped below a million), sign first.

    Money and counts differ only in ``prefix`` and in the ``places`` shown below
    a million, so they cannot drift apart on where a suffix starts or which
    side of the symbol a minus sits.
    """
    sign, value = _sign(value, places)
    for scale, unit in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if value >= scale:
            return f"{sign}{prefix}{value / scale:.2f}{unit}"
    return f"{sign}{prefix}{value:,.{places}f}"


def fmt_money(value: Optional[float], currency: CurrencyArg = None) -> str:
    """Large money figure with a currency prefix, e.g. "$3.68T", "HK$2.50B".

    A statement figure (revenue, cash flow) is reported in the issuer's own
    currency, which need not be the listing one, so pass its
    ``reportedCurrency`` rather than the instrument's. ``None`` -> "N/A".
    """
    if value is None:
        return "N/A"
    spec = _spec(currency)
    return _scaled(value, "" if spec.bare else currency_symbol(spec.currency))


def fmt_count(value: Optional[float], places: int = 2) -> str:
    """Share counts and volumes: the money ladder without a currency prefix.

    ``places=0`` prints a share count whole below a million; the default keeps
    two, since a crypto bar's volume can be fractional.
    """
    if value is None:
        return "N/A"
    return _scaled(value, "", places)
