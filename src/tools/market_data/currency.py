"""Currency-aware display formatting for market-data tool output.

Field names and numeric values stay currency-neutral; only human-readable
display strings gain a currency prefix. USD output is byte-identical to the
legacy hardcoded ``$`` formatting, so US symbols are unaffected.
"""

from typing import NamedTuple, Optional, Union

from .utils import format_number

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


# What the formatters accept for their second argument: a resolved spec, a bare
# ISO 4217 code, or ``None`` (both bare forms default to 2 decimals).
CurrencyArg = Union[DisplaySpec, str, None]


def _spec(currency: CurrencyArg) -> DisplaySpec:
    """Coerce a bare code / ``None`` to a 2-decimal spec; pass specs through."""
    if isinstance(currency, DisplaySpec):
        return currency
    return DisplaySpec(currency, 2)


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
    value: Optional[float], currency: CurrencyArg = None, decimals: Optional[int] = None
) -> str:
    """Format a price with its currency prefix, e.g. "£0.99", "HK$318.20".

    ``currency`` is a :class:`DisplaySpec` (currency + protocol decimals) or a
    bare ISO 4217 code (2 decimals). ``decimals`` overrides the spec when given.
    ``None`` value -> "N/A".
    """
    if value is None:
        return "N/A"
    spec = _spec(currency)
    dec = spec.decimals if decimals is None else decimals
    return f"{currency_symbol(spec.currency)}{value:.{dec}f}"


def fmt_money(
    value: Optional[float], currency: CurrencyArg = None, suffix: bool = True
) -> str:
    """Currency-aware large-number formatting (mirrors utils.format_number).

    Applies B/M/T suffixes for magnitudes and a currency prefix. For USD the
    output is byte-identical to ``format_number``. ``None`` value -> "N/A".
    Use only for values priced in the instrument's listing currency (market
    cap, price targets); statement figures (revenue, cash flow) may be
    reported in a different currency and keep plain ``format_number`` until
    reportedCurrency is plumbed through.
    """
    if value is None:
        return "N/A"
    sym = currency_symbol(_spec(currency).currency)
    if suffix and abs(value) >= 1e12:
        return f"{sym}{value / 1e12:.2f}T"
    if suffix and abs(value) >= 1e9:
        return f"{sym}{value / 1e9:.2f}B"
    if suffix and abs(value) >= 1e6:
        return f"{sym}{value / 1e6:.2f}M"
    if suffix:
        return f"{sym}{value:,.2f}"
    return f"{value:,.2f}"


def fmt_count(value: Optional[float]) -> str:
    """Currency-neutral large-number formatting for share counts / volumes.

    Byte-identical to ``format_number(value)`` with the currency prefix stripped
    (delegates to it, so B/M/T suffixes, ``None`` -> "N/A", and negatives match
    exactly). Use for quantities that carry no currency, never for prices.
    """
    return format_number(value).replace("$", "")
