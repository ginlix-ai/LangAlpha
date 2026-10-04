"""Canonical instrument identity: to_canonical / to_legacy_api / to_display.

Instrument keys are ``symbol.MIC`` (ISO 10383): ``AAPL.XNAS``, ``0700.XHKG``;
synthetic segments ``SPX.INDEX``, ``BTC-USD.CRYPTO``, ``EUR-USD.FX``. The MIC
is *listing* identity; feeds stay consolidated tape (``FeedScope.COMPOSITE``).

Suffix→MIC is a documented heuristic: a Yahoo/FMP suffix cannot always name
the venue (``.DE`` can't tell XETR from XFRA) and a bare US ticker cannot name
its listing exchange. Defaults favor a *correct calendar* over a precise MIC
(XNYS and XNAS share sessions); the YAML seed registry
(``instruments.yaml``) overrides per instrument.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from types import MappingProxyType

import yaml

from .enums import AssetClass
from .models import InstrumentRef

# ---------------------------------------------------------------------------
# MIC metadata
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _MicInfo:
    calendar_id: str
    tz: str
    currency: str
    # Routing token (``market_of``). Vendors are entitled per country, not per
    # venue, so several MICs share one.
    market: str
    suffix: str | None = None  # venue suffix ("" ⇒ US-style bare)
    # Yahoo's suffix, where FMP and yfinance resolve a different one (``vendor_symbol``).
    vendor_suffix: str | None = None
    display_unit: str | None = None  # e.g. quotes arrive in GBX on XLON
    exchange_code: str | None = None  # the exchange's own short name (SSE, HKEX)


# Heuristic default for bare US tickers: listing venue is unknowable from the
# symbol alone; XNYS and XNAS share the same calendar so freshness/session
# logic is unaffected. The seed registry pins well-known listings precisely.
US_DEFAULT_MIC = "XNYS"

# ISO 10383 "no market" placeholder for unrecognized suffixes: calendar and tz
# fall back to US, the routing token is ``other``.
UNKNOWN_MIC = "XXXX"

_MICS: dict[str, _MicInfo] = {
    "XNYS": _MicInfo("XNYS", "America/New_York", "USD", "us", suffix=""),
    "XNAS": _MicInfo("XNYS", "America/New_York", "USD", "us", suffix=""),
    "XHKG": _MicInfo("XHKG", "Asia/Hong_Kong", "HKD", "hk", suffix="HK", exchange_code="HKEX"),
    "XSHG": _MicInfo(
        "XSHG", "Asia/Shanghai", "CNY", "cn", suffix="SH", vendor_suffix="SS", exchange_code="SSE",
    ),
    "XSHE": _MicInfo("XSHG", "Asia/Shanghai", "CNY", "cn", suffix="SZ", exchange_code="SZSE"),
    # Beijing Stock Exchange (ISO 10383 BJSE; XBSE is Bucharest) trades the
    # Shanghai calendar and sessions.
    "BJSE": _MicInfo("XSHG", "Asia/Shanghai", "CNY", "cn", suffix="BJ", exchange_code="BSE"),
    "XLON": _MicInfo("XLON", "Europe/London", "GBP", "uk", suffix="L", display_unit="GBX"),
    "XTKS": _MicInfo("XTKS", "Asia/Tokyo", "JPY", "jp", suffix="T"),
    "XTSE": _MicInfo("XTSE", "America/Toronto", "CAD", "ca", suffix="TO"),
    "XASX": _MicInfo("XASX", "Australia/Sydney", "AUD", "au", suffix="AX"),
    "XPAR": _MicInfo("XPAR", "Europe/Paris", "EUR", "eu", suffix="PA"),
    "XETR": _MicInfo("XETR", "Europe/Berlin", "EUR", "eu", suffix="DE"),
    "XAMS": _MicInfo("XAMS", "Europe/Amsterdam", "EUR", "eu", suffix="AS"),
    "XMIL": _MicInfo("XMIL", "Europe/Rome", "EUR", "eu", suffix="MI"),
    "XMAD": _MicInfo("XMAD", "Europe/Madrid", "EUR", "eu", suffix="MC"),
    "XSWX": _MicInfo("XSWX", "Europe/Zurich", "CHF", "eu", suffix="SW"),
    "XKRX": _MicInfo("XKRX", "Asia/Seoul", "KRW", "kr", suffix="KS"),
    "XKOS": _MicInfo("XKRX", "Asia/Seoul", "KRW", "kr", suffix="KQ"),
    "XTAI": _MicInfo("XTAI", "Asia/Taipei", "TWD", "tw", suffix="TW"),
    "XSES": _MicInfo("XSES", "Asia/Singapore", "SGD", "sg", suffix="SI"),
    "XBOM": _MicInfo("XBOM", "Asia/Kolkata", "INR", "in", suffix="BO"),
    "XNSE": _MicInfo("XBOM", "Asia/Kolkata", "INR", "in", suffix="NS"),
    UNKNOWN_MIC: _MicInfo("XNYS", "America/New_York", "USD", "other", suffix=None),
}

_SUFFIX_TO_MIC: dict[str, str] = {
    info.suffix: mic for mic, info in _MICS.items() if info.suffix
}
# Shanghai has two spellings, both accepted forever. ``.SH`` is how the
# exchange, Tushare, Chinese brokers and users write it, so it is the one we
# store, compare and show; Yahoo's ``.SS`` is what FMP and yfinance resolve,
# so only their adapters write it (``vendor_symbol``, ``vendor_spelling``).
_SUFFIX_TO_MIC["SS"] = "XSHG"

# CN code ranges are partitioned by instrument type per exchange, so the code
# alone names the class: SSE 000xxx, SZSE 399xxx and BSE 899xxx are indices
# only, SSE 5xxxxx and SZSE 15/16/18xxxx are listed funds only.
_CN_INDEX_PREFIXES: dict[str, tuple[str, ...]] = {
    "XSHG": ("000",), "XSHE": ("399",), "BJSE": ("899",),
}
_CN_FUND_PREFIXES: dict[str, tuple[str, ...]] = {"XSHG": ("5",), "XSHE": ("15", "16", "18")}


def _is_code(symbol: str) -> bool:
    # ASCII only: str.isdigit() also accepts digits int() cannot parse.
    return symbol.isascii() and symbol.isdigit()


def _venue_asset_class(symbol: str, mic: str) -> AssetClass:
    if _is_code(symbol):
        if symbol.startswith(_CN_INDEX_PREFIXES.get(mic, ())):
            return AssetClass.INDEX
        if symbol.startswith(_CN_FUND_PREFIXES.get(mic, ())):
            return AssetClass.FUND
    return AssetClass.EQUITY


def _venue_code(symbol: str, mic: str) -> str:
    """HKEX codes are written with and without leading zeros (700, 0700,
    00700); all name one listing, so they collapse to the four-digit form
    Yahoo and FMP resolve."""
    if mic == "XHKG" and _is_code(symbol):
        return (symbol.lstrip("0") or "0").zfill(4)
    return symbol

# Synthetic (non-ISO) key segments for instruments without a listing venue.
_SYNTHETIC_SEGMENTS = frozenset({"INDEX", "CRYPTO", "FX"})

ALWAYS_24_7 = "ALWAYS_24_7"
WEEKDAYS_24_5 = "WEEKDAYS_24_5"

# ---------------------------------------------------------------------------
# Index families
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _IndexFamily:
    family: str    # canonical + display spelling (SPX)
    legacy: str    # the legacy API's bare spelling (GSPC)
    # Home venue: its calendar, tz, currency and market describe the index's
    # session and quote unit. Synthetic ``INDEX`` remains the key segment.
    home_mic: str = US_DEFAULT_MIC


_INDEX_FAMILIES: dict[str, _IndexFamily] = {
    f.family: f
    for f in (
        _IndexFamily("SPX", "GSPC"),
        _IndexFamily("DJI", "DJI"),
        _IndexFamily("COMP", "IXIC"),
        _IndexFamily("NDX", "NDX"),
        _IndexFamily("RUT", "RUT"),
        _IndexFamily("VIX", "VIX"),
        _IndexFamily("HSI", "HSI", home_mic="XHKG"),
        _IndexFamily("HSCE", "HSCE", home_mic="XHKG"),
        _IndexFamily("N225", "N225", home_mic="XTKS"),
        _IndexFamily("FTSE", "FTSE", home_mic="XLON"),
        _IndexFamily("GDAXI", "GDAXI", home_mic="XETR"),
        _IndexFamily("FCHI", "FCHI", home_mic="XPAR"),
        _IndexFamily("STOXX50E", "STOXX50E", home_mic="XETR"),
    )
}

# Every bare spelling of an index → its family. Prefixes (``^``, ``I:``) are
# stripped before the lookup.
_INDEX_ALIASES: dict[str, str] = {}
for _f in _INDEX_FAMILIES.values():
    for _alias in (_f.family, _f.legacy):
        _INDEX_ALIASES[_alias.upper()] = _f.family

# Market of each caret-prefixed foreign index, for callers that group index
# spellings by region.
CARET_INDEX_REGIONS: dict[str, str] = {
    f"^{f.legacy}": _MICS[f.home_mic].market
    for f in _INDEX_FAMILIES.values()
    if _MICS[f.home_mic].market != "us"
}


def market_of(ref: InstrumentRef) -> str:
    """Routing token for *ref*: ``us`` / ``cn`` / ``hk`` / ... / ``crypto`` / ``fx`` / ``other``.

    A family index (``SPX.INDEX``) routes by its home calendar, which is its
    home venue's MIC.
    """
    if ref.asset_class is AssetClass.CRYPTO:
        return "crypto"
    if ref.asset_class is AssetClass.FX:
        return "fx"
    info = _MICS.get(ref.calendar_id if ref.mic == "INDEX" else ref.mic)
    return info.market if info is not None else "other"


def market_home(market: str) -> tuple[str, str] | None:
    """``(calendar_id, tz)`` every venue in *market* trades on, or None.

    None when the market's venues keep different calendars (``eu``) or it
    names no venue at all: no single clock answers for it then.
    """
    homes = {
        (info.calendar_id, info.tz)
        for mic, info in _MICS.items()
        if info.market == market and mic != UNKNOWN_MIC
    }
    return homes.pop() if len(homes) == 1 else None


def venue_suffixes(*markets: str) -> frozenset[str]:
    """Every suffix spelling (``SH``, ``SS``, ``SZ`` …) that names a venue in *markets*."""
    return frozenset(s for s, mic in _SUFFIX_TO_MIC.items() if _MICS[mic].market in markets)


def exchange_code(mic: str | None) -> str | None:
    """The exchange's own short name for *mic* (``SSE`` for XSHG), where one is declared."""
    info = _MICS.get(mic or "")
    return info.exchange_code if info else None


# ---------------------------------------------------------------------------
# Currency display defaults (ISO 4217 minor-unit digits)
# ---------------------------------------------------------------------------

_MINOR_UNIT_DIGITS: dict[str, int] = {"JPY": 0, "KRW": 0}


def display_decimals_for(currency: str, asset_class: AssetClass | str) -> int:
    """Default display decimals: ISO 4217 minor units; crypto 8, index levels 2, FX to the pip.

    ``asset_class`` is the enum or its value; any other string raises ValueError.
    """
    # Every branch below compares by identity, which a plain string fails.
    asset_class = AssetClass(asset_class)
    if asset_class is AssetClass.CRYPTO:
        return 8
    # An index level is points, not money: N225 and KOSPI print 2 places like any index.
    if asset_class is AssetClass.INDEX:
        return 2
    # SSE/SZSE funds (ETF/LOF) tick in 0.001 CNY, a tenth of the minor unit.
    if asset_class is AssetClass.FUND and currency.upper() == "CNY":
        return 3
    minor = _MINOR_UNIT_DIGITS.get(currency.upper(), 2)
    # A rate is quoted to the pip, two places past the quote currency's minor
    # unit: EUR-USD 1.1690, USD-JPY 147.42.
    if asset_class is AssetClass.FX:
        return minor + 2
    return minor


# ---------------------------------------------------------------------------
# Seed registry (YAML)
# ---------------------------------------------------------------------------

_SEED_FIELDS = frozenset(
    {"mic", "name", "currency", "price_currency", "display_unit", "calendar_id"}
)

# Minor quote units, by the decimal places to their major unit. The series
# builder divides by these, so any other display_unit would be served
# unscaled, and a seed may name only these.
MINOR_UNIT_PLACES: Mapping[str, int] = MappingProxyType({"GBX": 2})

_CALENDAR_IDS = frozenset(
    {info.calendar_id for info in _MICS.values()} | {ALWAYS_24_7, WEEKDAYS_24_5}
)


def _seed_row(key: str, row: dict) -> dict:
    """*row* with its values normalized, or ValueError naming *key*.

    A typo would otherwise load and misbehave later: ``display_unit: gbx``
    silently drops the pence scaling, and an unknown ``calendar_id`` fails on
    the first session lookup.
    """
    out = dict(row)
    if "mic" in row:
        out["mic"] = str(row["mic"]).upper()
        if out["mic"] not in _MICS:
            raise ValueError(f"instruments.yaml: {key}: unknown mic {row['mic']!r}")
    if "name" in row and not isinstance(row["name"], str | None):
        raise ValueError(f"instruments.yaml: {key}: name {row['name']!r} is not a string; quote it")
    for field in ("currency", "price_currency"):
        if field in row:
            value = row[field]
            code = value.strip().upper() if isinstance(value, str) else ""
            if not (len(code) == 3 and code.isascii() and code.isalpha()):
                raise ValueError(
                    f"instruments.yaml: {key}: {field} {value!r} is not a three-letter code"
                )
            out[field] = code
    if "display_unit" in row:
        unit = row["display_unit"]
        # An explicit null stays: it clears the venue's unit (a USD line on XLON).
        if unit is not None:
            unit = unit.strip().upper() if isinstance(unit, str) else ""
            if unit not in MINOR_UNIT_PLACES:
                raise ValueError(
                    f"instruments.yaml: {key}: unknown display_unit {row['display_unit']!r}"
                )
        out["display_unit"] = unit
    if "calendar_id" in row and (
        not isinstance(row["calendar_id"], str) or row["calendar_id"] not in _CALENDAR_IDS
    ):
        raise ValueError(f"instruments.yaml: {key}: unknown calendar_id {row['calendar_id']!r}")
    return out


def _parse_seed(text: str) -> dict[str, dict]:
    """The seed rows of an ``instruments.yaml`` text, keyed by display spelling.

    Lookups use our spelling (``600519.SH``, ``0700.HK``), so a key written
    another way is read in that form, and two spellings of one listing are
    duplicates. Raises ValueError naming the offending key. The file is read
    as YAML 1.1, where an unquoted ``ON`` or ``NO`` loads as a boolean and
    ``700`` as an int; such a key would otherwise fail far from here, on the
    first lookup that needs a string.
    """
    raw = yaml.safe_load(text) or {}
    if not isinstance(raw, dict):
        raise ValueError("instruments.yaml: top level must be a mapping")
    rows = raw.get("instruments") or {}
    if not isinstance(rows, dict):
        raise ValueError("instruments.yaml: 'instruments' must be a mapping")
    seeds: dict[str, dict] = {}
    written: dict[str, str] = {}
    for key, row in rows.items():
        if not isinstance(key, str):
            raise ValueError(f"instruments.yaml: key {key!r} is not a string; quote it")
        if not isinstance(row, dict):
            raise ValueError(f"instruments.yaml: {key}: expected a mapping of overrides")
        if unknown := set(row) - _SEED_FIELDS:
            raise ValueError(f"instruments.yaml: {key}: unknown fields {sorted(map(str, unknown))}")
        spelling = display_spelling(key)
        if spelling in seeds:
            raise ValueError(f"instruments.yaml: {key}: duplicate of {written[spelling]}")
        seeds[spelling] = _seed_row(key, row)
        written[spelling] = key
    return seeds


@lru_cache(maxsize=1)
def _seed_registry() -> dict[str, dict]:
    # The file ships inside the package, so a missing one is a broken install:
    # it raises rather than resolving every US listing to the default MIC.
    resource = files("market_protocol") / "instruments.yaml"
    return _parse_seed(resource.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_instrument_key(key: str) -> tuple[str, str]:
    """Split ``symbol.MIC`` into ``(symbol, mic_segment)``. Raises ValueError."""
    if "." not in key:
        raise ValueError(f"Not an instrument key (no MIC segment): {key!r}")
    symbol, segment = key.rsplit(".", 1)
    if not symbol or not segment:
        raise ValueError(f"Malformed instrument key: {key!r}")
    return symbol, segment


def _clean(symbol: str) -> str:
    # A Chinese IME types full-width forms (600519．ＳＨ) that NFKC folds, and
    # the ideographic full stop (600519。SH), which NFKC leaves alone.
    return unicodedata.normalize("NFKC", symbol).replace("。", ".").strip().upper()


# Longer than any real spelling (an OCC option symbol runs to 21), short enough
# that a pasted paragraph never reaches a cache key or a vendor URL.
_MAX_SYMBOL_LEN = 64

# A consumer interpolates the symbol into a URL path, where each of these
# would end the segment, start a query or fragment, or begin an escape.
_URL_METACHARS = frozenset("/\\?#%")


def _checked(symbol: object) -> str:
    """*symbol* cleaned, or ValueError when no instrument could be spelled that way.

    Checked after the fold, which turns a full-width ``．．／`` into ``../``.
    A denylist rather than an allowlist: real spellings carry ``^ . - = & :``
    and the set is open. Surrounding whitespace is still trimmed, since a
    pasted symbol often brings a newline along.
    """
    if not isinstance(symbol, str):
        raise ValueError(f"Symbol must be a string, not {type(symbol).__name__}")
    s = _clean(symbol)
    if not s:
        raise ValueError("Empty symbol")
    if len(s) > _MAX_SYMBOL_LEN:
        raise ValueError(f"Symbol longer than {_MAX_SYMBOL_LEN} characters")
    for ch in s:
        # Categories C* and Z*: controls, format characters such as a
        # zero-width space, and every separator.
        if ch in _URL_METACHARS or ch.isspace() or unicodedata.category(ch)[0] in "CZ":
            raise ValueError(f"Symbol {s!r} contains {ch!r}")
    return s


def _require_stem(symbol: str) -> None:
    """Refuse a symbol that names nothing: empty, punctuation alone, or an empty dot segment.

    ``.HK`` and ``AAPL.`` are typos, and ``..`` as a URL path segment climbs a level.
    """
    if not any(ch.isalnum() for ch in symbol) or "" in symbol.split("."):
        raise ValueError(f"Empty symbol: {symbol!r}")


def _spell(symbol: str, mic: str, *, vendor: bool = False) -> str:
    """*symbol* with *mic*'s venue suffix; bare where the venue has none (US)."""
    info = _MICS.get(mic)
    if info is None or not info.suffix:
        return symbol
    return f"{symbol}.{(vendor and info.vendor_suffix) or info.suffix}"


def _key(symbol: str, segment: str) -> str:
    """``symbol.SEGMENT``, refused when it would be too long to parse back."""
    key = f"{symbol}.{segment}"
    if len(key) > _MAX_SYMBOL_LEN:
        raise ValueError(f"Symbol {symbol!r} is too long for a key on {segment}")
    return key


def _is_canonical_key(s: str) -> bool:
    if "." not in s:
        return False
    segment = s.rsplit(".", 1)[1].upper()
    return segment in _SYNTHETIC_SEGMENTS or segment in _MICS


def _index_ref(family_key: str) -> InstrumentRef:
    _require_stem(family_key)
    fam = _INDEX_FAMILIES.get(family_key)
    if fam is None:
        fam = _IndexFamily(family_key, family_key)  # unknown: its own family
    home = _MICS[fam.home_mic]
    return InstrumentRef(
        instrument_key=_key(fam.family, "INDEX"),
        symbol=fam.family,
        mic="INDEX",
        asset_class=AssetClass.INDEX,
        currency=home.currency,
        price_currency=home.currency,
        calendar_id=home.calendar_id,
        tz=home.tz,
        index_family=fam.family,
    )


_PAIR_CLASSES = (AssetClass.CRYPTO, AssetClass.FX)


def _refuse_pair_hint(listing: str, asset_class: AssetClass | None) -> None:
    if asset_class in _PAIR_CLASSES:
        raise ValueError(
            f"{listing!r} is a venue listing, not a pair: the {asset_class} hint does not apply"
        )


def _venue_ref(
    symbol: str,
    mic: str,
    *,
    asset_class: AssetClass | None = None,
) -> InstrumentRef:
    """Ref for a venue-listed instrument (equity, listed fund, or an exchange
    index such as 000001.XSHG, whose key stays ``code.MIC`` because the venue's
    code space already separates it from stocks)."""
    _require_stem(symbol)
    symbol = _venue_code(symbol, mic)
    # Seeds are keyed by our spelling on the parsed venue, so AMD.DE (or its
    # key AMD.XETR) never picks up the seed that pins US AMD to XNAS.
    seed = _seed_registry().get(_spell(symbol, mic)) or {}
    mic = seed.get("mic", mic)
    info = _MICS.get(mic, _MICS[UNKNOWN_MIC])
    currency = seed.get("currency", info.currency)
    derived = _venue_asset_class(symbol, mic)
    # The key carries no asset class, so a hint that contradicts the listing
    # would file one class's data under another's key. A pair hint is refused
    # outright, and a CN code's class is the code range's alone (600519.SH on
    # the indexes endpoint). Elsewhere an EQUITY hint only suppresses the
    # bare-ticker index autodetect (COMP the stock vs COMP the index); it
    # cannot turn an index code into a stock.
    _refuse_pair_hint(f"{symbol}.{mic}", asset_class)
    code_decides = info.market == "cn" and _is_code(symbol)
    if asset_class is AssetClass.INDEX and derived is AssetClass.EQUITY and not code_decides:
        # Only a code space that separates indexes from stocks can key an index
        # on its venue. FTSE.XLON would reparse as a stock quoted in pence, so
        # the index takes its family key, the one ^FTSE reaches.
        return _index_ref(_INDEX_ALIASES.get(symbol, symbol))
    if asset_class is None or derived is not AssetClass.EQUITY or code_decides:
        asset_class = derived
    return InstrumentRef(
        instrument_key=_key(symbol, mic),
        symbol=symbol,
        mic=mic,
        asset_class=asset_class,
        name=seed.get("name"),
        currency=currency,
        price_currency=seed.get("price_currency", currency),
        display_unit=seed.get("display_unit", info.display_unit),
        calendar_id=seed.get("calendar_id", info.calendar_id),
        tz=info.tz,
    )


def _venue_suffix_split(bare: str) -> tuple[str, str] | None:
    """``(stem, mic)`` when *bare* ends in a known venue suffix, else None."""
    if "." not in bare:
        return None
    stem, suffix = bare.rsplit(".", 1)
    mic = _SUFFIX_TO_MIC.get(suffix)
    return (stem, mic) if mic and stem else None


def _index(spelling: str) -> InstrumentRef:
    """Ref for an index spelling, with or without its ``^`` / ``I:`` prefix.

    An index on a venue whose code range names it (000001.SH, ^000001.SS) stays
    a venue-listed ref; every other index gets its family's INDEX key, so
    ^FTSE.L is FTSE.INDEX.
    """
    # Both prefixes, in any order and count (^I:SPX), so the stem keyed here is
    # the one a reparse of the minted key sees.
    bare = spelling
    while bare.startswith(("^", "I:")):
        bare = bare.removeprefix("I:").lstrip("^")
    if (venue := _venue_suffix_split(bare)) is not None:
        stem, mic = venue
        return _venue_ref(stem, mic, asset_class=AssetClass.INDEX)
    return _index_ref(_INDEX_ALIASES.get(bare, bare))


def _pair_ref(symbol: str, segment: str) -> InstrumentRef:
    """Crypto/FX pair: ``BTC-USD.CRYPTO`` / ``EUR-USD.FX``.

    A six-letter run splits here, so the key ``USDJPY.FX`` names the pair the
    spelling ``USDJPY`` does, quoted in yen, not a second one quoted in dollars.
    """
    if "-" not in symbol and len(symbol) == 6:
        symbol = f"{symbol[:3]}-{symbol[3:]}"  # EURUSD → EUR-USD
    for leg in symbol.split("-"):
        _require_stem(leg)
    asset_class = AssetClass.CRYPTO if segment == "CRYPTO" else AssetClass.FX
    quote = symbol.rsplit("-", 1)[1] if "-" in symbol else "USD"
    return InstrumentRef(
        instrument_key=_key(symbol, segment),
        symbol=symbol,
        mic=segment,
        asset_class=asset_class,
        currency=quote,
        price_currency=quote,
        calendar_id=ALWAYS_24_7 if asset_class is AssetClass.CRYPTO else WEEKDAYS_24_5,
        tz="UTC",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def to_canonical(
    symbol: str,
    *,
    asset_class: AssetClass | str | None = None,
) -> InstrumentRef:
    """Resolve any legacy or canonical spelling to one InstrumentRef.

    ``asset_class`` is the router's hint (stocks vs indexes endpoints), as the
    enum or its value; when absent, ``^``/``I:`` prefixes and the known index
    families auto-detect as indexes. A CN exchange code's own range outranks
    the hint.

    Raises ValueError for anything that cannot spell an instrument, a non-string,
    an unknown hint or a crypto or FX hint on a venue listing included, so one
    ``except ValueError`` covers every bad input.
    """
    s = _checked(symbol)
    if asset_class is not None:
        # Every branch below compares by identity, which a plain string fails.
        asset_class = AssetClass(asset_class)

    if _is_canonical_key(s):
        sym, segment = parse_instrument_key(s)
        if segment == "INDEX":
            return _index(sym)
        if segment in _SYNTHETIC_SEGMENTS:
            return _pair_ref(sym, segment)
        return _venue_ref(sym, segment, asset_class=asset_class)

    if asset_class in _PAIR_CLASSES or s.endswith("=X"):
        # An =X suffix is an FX spelling whatever the hint says, so VOD.L=X is
        # refused like VOD.L with an FX hint rather than minted as VOD.L.FX,
        # and EURUSD=X stays FX under a crypto hint.
        pair_class = AssetClass.FX if s.endswith("=X") else asset_class
        sym = s.removesuffix("=X")
        # .US is the US venue, as the venue-listed path below reads it.
        if sym.endswith(".US") or _venue_suffix_split(sym) is not None:
            _refuse_pair_hint(s, pair_class)
        if "-" not in sym and len(sym) == 3 and s.endswith("=X"):
            # Yahoo names a USD-based rate by its quote alone: JPY=X is USD-JPY.
            sym = f"USD-{sym}"
        return _pair_ref(sym, "CRYPTO" if pair_class is AssetClass.CRYPTO else "FX")

    is_index = (
        asset_class is AssetClass.INDEX
        or s.startswith(("^", "I:"))
        or (asset_class is None and s in _INDEX_ALIASES)
    )
    if is_index:
        return _index(s)

    # Venue-listed path
    bare = s.removesuffix(".US")
    if (venue := _venue_suffix_split(bare)) is not None:
        stem, mic = venue
        return _venue_ref(stem, mic, asset_class=asset_class)
    if "." in bare:
        # Unknown suffix (e.g. share classes): whole string is the symbol.
        return _venue_ref(bare, UNKNOWN_MIC, asset_class=asset_class)
    return _venue_ref(bare, US_DEFAULT_MIC, asset_class=asset_class)


def from_instrument_key(key: str) -> InstrumentRef:
    """Ref for a key received from another service. Raises ValueError.

    ``to_canonical`` reads an unknown last segment as part of the symbol (a
    share class), so a key minted on a venue a newer pin added would resolve
    here to a different key, on the US calendar and in dollars. A key from the
    wire is refused instead when its segment is not one this pin knows.
    """
    s = _checked(key)
    if not _is_canonical_key(s):
        raise ValueError(f"Not an instrument key on a known venue: {s!r}")
    return to_canonical(s)


def is_family_index(ref: InstrumentRef) -> bool:
    """Venue-less index (``SPX.INDEX``) as opposed to an exchange index that
    lives in a venue's code space (``000001.XSHG``) and spells like a stock.

    Vendors spell the two differently (``^GSPC`` vs ``000001.SH``).
    """
    return ref.asset_class is AssetClass.INDEX and ref.mic == "INDEX"


def _legacy(ref: InstrumentRef, *, vendor: bool) -> str:
    if is_family_index(ref):
        return _INDEX_FAMILIES[ref.index_family].legacy if ref.index_family in _INDEX_FAMILIES else ref.symbol
    if ref.asset_class in (AssetClass.EQUITY, AssetClass.INDEX, AssetClass.FUND):
        return _spell(ref.symbol, ref.mic, vendor=vendor)
    return ref.symbol


def to_legacy_api(ref: InstrumentRef) -> str:
    """The REST-layer spelling: bare US ticker, ``0700.HK``, ``600519.SH``, ``GSPC``."""
    return _legacy(ref, vendor=False)


def to_display(ref: InstrumentRef) -> str:
    """Human-facing spelling: ``AAPL``, ``0700.HK``, ``600519.SH``, ``SPX``."""
    if is_family_index(ref):
        return ref.index_family or ref.symbol
    return to_legacy_api(ref)


def vendor_symbol(ref: InstrumentRef) -> str:
    """The legacy spelling with the venue suffix FMP and yfinance resolve (``600519.SS``).

    Only an adapter for one of those vendors calls this; everything else holds
    :func:`to_legacy_api`. Only the suffix differs: a family index, FX or crypto
    pair comes back as the legacy spelling (``GSPC``, ``EUR-USD``), which each
    vendor spells its own way, so the adapter maps those itself.
    """
    return _legacy(ref, vendor=True)


def _respell_suffix(symbol: str, *, vendor: bool) -> str:
    s = _clean(symbol)
    venue = _venue_suffix_split(s)
    if venue is None:
        return s
    stem, mic = venue
    return _spell(_venue_code(stem, mic), mic, vendor=vendor)


def display_spelling(symbol: str) -> str:
    """A raw symbol string in our spelling: trimmed, uppercased, venue suffix respelled.

    For strings that never went through :func:`to_canonical`: what a vendor row
    or a user typed, on its way into storage or a comparison (``600519.ss``
    becomes ``600519.SH``, ``700.HK`` becomes ``0700.HK``). Only a venue-suffixed
    symbol is respelled, so a caret, a family index or an unknown suffix
    survives. It never raises, and so it validates nothing: what goes into a
    request or a URL comes from :func:`vendor_spelling` or :func:`to_canonical`.
    """
    return _respell_suffix(symbol, vendor=False)


def vendor_spelling(symbol: str) -> str:
    """A raw symbol string with its venue suffix in the vendors' form.

    The inverse of :func:`display_spelling`, for an FMP or yfinance adapter that
    receives a symbol string rather than a ref: ``600519.SH`` becomes
    ``600519.SS``, which is what those vendors resolve. The result goes into a
    vendor request, so a string :func:`to_canonical` would refuse raises
    ValueError here too.
    """
    s = _checked(symbol)
    _require_stem(s)
    return _respell_suffix(s, vendor=True)


def index_families() -> tuple[InstrumentRef, ...]:
    """A ref for every known index family, for adapters that key a vendor spelling on it."""
    return tuple(_index_ref(family) for family in _INDEX_FAMILIES)
