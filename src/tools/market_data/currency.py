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
    # An index level is points, not money: it prints with no currency prefix.
    bare: bool = False


# What the formatters accept for their second argument: a resolved spec, a bare
# ISO 4217 code, or ``None`` (both bare forms default to 2 decimals).
CurrencyArg = Union[DisplaySpec, str, None]


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
    return f"{sign}{prefix}{value:{grouping}.{dec}f}"


def _scaled(value: float, prefix: str) -> str:
    """The one magnitude ladder (T/B/M, grouped below a million), sign first.

    Money and counts differ only in ``prefix``, so they cannot drift apart on
    where a suffix starts or which side of the symbol a minus sits.
    """
    sign, value = _sign(value, 2)
    for scale, unit in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if value >= scale:
            return f"{sign}{prefix}{value / scale:.2f}{unit}"
    return f"{sign}{prefix}{value:,.2f}"


def fmt_money(value: Optional[float], currency: CurrencyArg = None) -> str:
    """Large money figure with a currency prefix, e.g. "$3.68T", "HK$2.50B".

    A statement figure (revenue, cash flow) is reported in the issuer's own
    currency, which need not be the listing one, so pass its
    ``reportedCurrency`` rather than the instrument's. ``None`` -> "N/A".
    """
    if value is None:
        return "N/A"
    return _scaled(value, currency_symbol(_spec(currency).currency))


def fmt_count(value: Optional[float]) -> str:
    """Share counts and volumes: the money ladder without a currency prefix."""
    if value is None:
        return "N/A"
    return _scaled(value, "")
