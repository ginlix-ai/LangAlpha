"""Canonical instrument identity: to_canonical / to_provider / to_display.

One entry point replaces the scattered normalizers (`_INDEX_SYMBOL_MAP`,
`_DISPLAY_ALIASES`, `_US_INDEX_SYMBOLS`, ad-hoc `^`/`I:` stripping, …).

Instrument keys are ``symbol.MIC`` (ISO 10383): ``AAPL.XNAS``, ``0700.XHKG``;
synthetic segments ``SPX.INDEX``, ``BTC-USD.CRYPTO``, ``EUR-USD.FX``. The MIC
is *listing* identity — feeds stay consolidated tape (``FeedScope.COMPOSITE``).

Suffix→MIC is a documented heuristic: a Yahoo/FMP suffix cannot always name
the venue (``.DE`` can't tell XETR from XFRA) and a bare US ticker cannot name
its listing exchange. Defaults favor a *correct calendar* over a precise MIC
(XNYS and XNAS share sessions); the YAML seed registry
(``instruments.yaml``) overrides per instrument.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

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
    suffix: str | None = None  # legacy Yahoo/FMP suffix ("" ⇒ US-style bare)
    display_unit: str | None = None  # e.g. quotes arrive in GBX on XLON


# Heuristic default for bare US tickers: listing venue is unknowable from the
# symbol alone; XNYS and XNAS share the same calendar so freshness/session
# logic is unaffected. The seed registry pins well-known listings precisely.
US_DEFAULT_MIC = "XNYS"

# ISO 10383 "no market" placeholder for unrecognized suffixes; calendar/tz
# fall back to US (matching today's `symbol_market() == "other"` behavior).
UNKNOWN_MIC = "XXXX"

_MICS: dict[str, _MicInfo] = {
    "XNYS": _MicInfo("XNYS", "America/New_York", "USD", suffix=""),
    "XNAS": _MicInfo("XNYS", "America/New_York", "USD", suffix=""),
    "XHKG": _MicInfo("XHKG", "Asia/Hong_Kong", "HKD", suffix="HK"),
    "XSHG": _MicInfo("XSHG", "Asia/Shanghai", "CNY", suffix="SS"),
    "XSHE": _MicInfo("XSHG", "Asia/Shanghai", "CNY", suffix="SZ"),
    "XLON": _MicInfo("XLON", "Europe/London", "GBP", suffix="L", display_unit="GBX"),
    "XTKS": _MicInfo("XTKS", "Asia/Tokyo", "JPY", suffix="T"),
    "XTSE": _MicInfo("XTSE", "America/Toronto", "CAD", suffix="TO"),
    "XASX": _MicInfo("XASX", "Australia/Sydney", "AUD", suffix="AX"),
    "XPAR": _MicInfo("XPAR", "Europe/Paris", "EUR", suffix="PA"),
    "XETR": _MicInfo("XETR", "Europe/Berlin", "EUR", suffix="DE"),
    "XAMS": _MicInfo("XAMS", "Europe/Amsterdam", "EUR", suffix="AS"),
    "XMIL": _MicInfo("XMIL", "Europe/Rome", "EUR", suffix="MI"),
    "XMAD": _MicInfo("XMAD", "Europe/Madrid", "EUR", suffix="MC"),
    "XSWX": _MicInfo("XSWX", "Europe/Zurich", "CHF", suffix="SW"),
    "XKRX": _MicInfo("XKRX", "Asia/Seoul", "KRW", suffix="KS"),
    "XKOS": _MicInfo("XKRX", "Asia/Seoul", "KRW", suffix="KQ"),
    "XTAI": _MicInfo("XTAI", "Asia/Taipei", "TWD", suffix="TW"),
    "XSES": _MicInfo("XSES", "Asia/Singapore", "SGD", suffix="SI"),
    "XBOM": _MicInfo("XBOM", "Asia/Kolkata", "INR", suffix="BO"),
    "XNSE": _MicInfo("XBOM", "Asia/Kolkata", "INR", suffix="NS"),
    UNKNOWN_MIC: _MicInfo("XNYS", "America/New_York", "USD", suffix=None),
}

_SUFFIX_TO_MIC: dict[str, str] = {
    info.suffix: mic for mic, info in _MICS.items() if info.suffix
}

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
    legacy: str    # today's REST-layer bare spelling (GSPC)
    polygon: str   # ginlix-data / Polygon spelling (I:SPX)
    calendar_id: str = "XNYS"


_INDEX_FAMILIES: dict[str, _IndexFamily] = {
    f.family: f
    for f in (
        _IndexFamily("SPX", "GSPC", "I:SPX"),
        _IndexFamily("DJI", "DJI", "I:DJI"),
        _IndexFamily("COMP", "IXIC", "I:COMP"),
        _IndexFamily("NDX", "NDX", "I:NDX"),
        _IndexFamily("RUT", "RUT", "I:RUT"),
        _IndexFamily("VIX", "VIX", "I:VIX"),
    )
}

# Every known spelling of an index → its family.
_INDEX_ALIASES: dict[str, str] = {}
for _f in _INDEX_FAMILIES.values():
    for _alias in (_f.family, _f.legacy, _f.polygon, f"^{_f.family}", f"^{_f.legacy}"):
        _INDEX_ALIASES[_alias.upper()] = _f.family

# Region of each caret-prefixed foreign index (no dot suffix to parse). The
# single source for provider-chain routing and the tool-level region baskets;
# folds into _INDEX_FAMILIES seeding when Phase-1 adds these venues' calendars.
CARET_INDEX_REGIONS: dict[str, str] = {
    "^HSI": "hk", "^HSCE": "hk",
    "^N225": "jp",
    "^FTSE": "uk",
    "^GDAXI": "eu", "^FCHI": "eu", "^STOXX50E": "eu",
}

# ---------------------------------------------------------------------------
# Currency display defaults (ISO 4217 minor-unit digits)
# ---------------------------------------------------------------------------

_MINOR_UNIT_DIGITS: dict[str, int] = {"JPY": 0, "KRW": 0}


def display_decimals_for(currency: str, asset_class: AssetClass) -> int:
    """Default display decimals: ISO 4217 minor units, crypto overridden to 8."""
    if asset_class is AssetClass.CRYPTO:
        return 8
    return _MINOR_UNIT_DIGITS.get(currency.upper(), 2)


# ---------------------------------------------------------------------------
# Seed registry (YAML)
# ---------------------------------------------------------------------------

_SEED_PATH = Path(__file__).parent / "instruments.yaml"


@lru_cache(maxsize=1)
def _seed_registry() -> dict[str, dict]:
    if not _SEED_PATH.exists():
        return {}
    raw = yaml.safe_load(_SEED_PATH.read_text()) or {}
    return {k.upper(): v for k, v in (raw.get("instruments") or {}).items()}


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


def _is_canonical_key(s: str) -> bool:
    if "." not in s:
        return False
    segment = s.rsplit(".", 1)[1].upper()
    return segment in _SYNTHETIC_SEGMENTS or segment in _MICS


def _index_ref(family_key: str) -> InstrumentRef:
    fam = _INDEX_FAMILIES.get(family_key)
    if fam is None:
        # Unknown index: keep the bare spelling as its own family; Polygon
        # spelling mirrors GinlixDataSource._index_symbol's I:{bare} fallback.
        fam = _IndexFamily(family_key, family_key, f"I:{family_key}")
    return InstrumentRef(
        instrument_key=f"{fam.family}.INDEX",
        symbol=fam.family,
        mic="INDEX",
        asset_class=AssetClass.INDEX,
        currency="USD",
        price_currency="USD",
        calendar_id=fam.calendar_id,
        tz=_MICS[US_DEFAULT_MIC].tz,
        index_family=fam.family,
    )


def _equity_ref(symbol: str, mic: str, seed: dict | None = None) -> InstrumentRef:
    seed = seed or {}
    mic = str(seed.get("mic", mic)).upper()
    info = _MICS.get(mic, _MICS[UNKNOWN_MIC])
    currency = str(seed.get("currency", info.currency)).upper()
    return InstrumentRef(
        instrument_key=f"{symbol}.{mic}",
        symbol=symbol,
        mic=mic,
        asset_class=AssetClass.EQUITY,
        name=seed.get("name"),
        currency=currency,
        price_currency=str(seed.get("price_currency", currency)).upper(),
        display_unit=seed.get("display_unit", info.display_unit),
        calendar_id=str(seed.get("calendar_id", info.calendar_id)),
        tz=info.tz,
    )


def _pair_ref(symbol: str, segment: str) -> InstrumentRef:
    """Crypto/FX pair: ``BTC-USD.CRYPTO`` / ``EUR-USD.FX``."""
    asset_class = AssetClass.CRYPTO if segment == "CRYPTO" else AssetClass.FX
    quote = symbol.rsplit("-", 1)[1] if "-" in symbol else "USD"
    return InstrumentRef(
        instrument_key=f"{symbol}.{segment}",
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
    asset_class: AssetClass | None = None,
) -> InstrumentRef:
    """Resolve any legacy or canonical spelling to one InstrumentRef.

    ``asset_class`` is the router's hint (stocks vs indexes endpoints); when
    absent, ``^``/``I:`` prefixes and the six known index families
    auto-detect as indexes.
    """
    s = symbol.strip().upper()
    if not s:
        raise ValueError("Empty symbol")

    if _is_canonical_key(s):
        sym, segment = parse_instrument_key(s)
        if segment == "INDEX":
            return _index_ref(_INDEX_ALIASES.get(sym, sym))
        if segment in _SYNTHETIC_SEGMENTS:
            return _pair_ref(sym, segment)
        return _equity_ref(sym, segment, _seed_registry().get(sym))

    if asset_class is AssetClass.CRYPTO or asset_class is AssetClass.FX:
        sym = s.removesuffix("=X")
        if "-" not in sym and len(sym) == 6:
            sym = f"{sym[:3]}-{sym[3:]}"  # EURUSD → EUR-USD
        return _pair_ref(sym, "CRYPTO" if asset_class is AssetClass.CRYPTO else "FX")
    if s.endswith("=X"):
        sym = s.removesuffix("=X")
        if len(sym) == 6:
            sym = f"{sym[:3]}-{sym[3:]}"
        return _pair_ref(sym, "FX")

    is_index = (
        asset_class is AssetClass.INDEX
        or s.startswith(("^", "I:"))
        or (asset_class is None and s in _INDEX_ALIASES)
    )
    if is_index:
        bare = s.removeprefix("I:").lstrip("^")
        return _index_ref(_INDEX_ALIASES.get(bare, _INDEX_ALIASES.get(s, bare)))

    # Equity path
    bare = s.removesuffix(".US")
    seed = _seed_registry().get(bare)
    if "." in bare:
        stem, suffix = bare.rsplit(".", 1)
        mic = _SUFFIX_TO_MIC.get(suffix)
        if mic:
            return _equity_ref(stem, mic, seed)
        # Unknown suffix (e.g. share classes): whole string is the symbol.
        return _equity_ref(bare, UNKNOWN_MIC, seed)
    return _equity_ref(bare, US_DEFAULT_MIC, seed)


def to_legacy_api(ref: InstrumentRef) -> str:
    """Today's REST-layer spelling: bare US ticker, ``0700.HK``, ``GSPC``."""
    if ref.asset_class is AssetClass.INDEX:
        return _INDEX_FAMILIES[ref.index_family].legacy if ref.index_family in _INDEX_FAMILIES else ref.symbol
    if ref.asset_class is AssetClass.EQUITY:
        info = _MICS.get(ref.mic)
        if info and info.suffix:
            return f"{ref.symbol}.{info.suffix}"
        return ref.symbol
    return ref.symbol


def to_provider(ref: InstrumentRef, provider: str) -> str:
    """Provider-native spelling for upstream requests."""
    if provider in ("fmp", "yfinance"):
        if ref.asset_class is AssetClass.INDEX:
            return f"^{to_legacy_api(ref)}"
        if ref.asset_class is AssetClass.CRYPTO:
            return ref.symbol if provider == "yfinance" else ref.symbol.replace("-", "")
        if ref.asset_class is AssetClass.FX:
            compact = ref.symbol.replace("-", "")
            return f"{compact}=X" if provider == "yfinance" else compact
        return to_legacy_api(ref)
    if provider == "ginlix-data":
        if ref.asset_class is AssetClass.INDEX:
            fam = _INDEX_FAMILIES.get(ref.index_family or "")
            return fam.polygon if fam else f"I:{ref.symbol}"
        return to_legacy_api(ref)
    raise ValueError(f"Unknown provider: {provider!r}")


def to_display(ref: InstrumentRef) -> str:
    """Human-facing spelling: ``AAPL``, ``0700.HK``, ``SPX``."""
    if ref.asset_class is AssetClass.INDEX:
        return ref.index_family or ref.symbol
    if ref.asset_class is AssetClass.EQUITY:
        return to_legacy_api(ref)
    return ref.symbol


# ---------------------------------------------------------------------------
# Legacy index spelling map
# ---------------------------------------------------------------------------


def index_legacy_to_polygon() -> dict[str, str]:
    """Legacy bare index symbol → Polygon wire spelling (``GSPC`` → ``I:SPX``)."""
    return {f.legacy: f.polygon for f in _INDEX_FAMILIES.values()}
