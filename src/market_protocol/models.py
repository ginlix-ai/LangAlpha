"""Protocol record and container models (Pydantic v2).

Wire rules: prices are float64 in major currency units; timestamps are
``ts_event``/``ts_recv``/``asof``/``fetched_at`` Unix milliseconds UTC;
bar anchor is the OPEN of the aggregate window. Evolution is additive-only
(``schema_version``); deprecated fields live ≥2 minors before removal.

Serialize with ``Series.to_wire()`` (or ``model_dump(by_alias=True)``) so the
``schema`` header field keeps its wire name.
"""

from __future__ import annotations

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field

from .enums import AssetClass, FeedScope, PriceTreatment, Tier

SCHEMA_VERSION = 1


class Gap(BaseModel):
    """A missing sub-range inside a series' returned coverage (ms, [start, end))."""

    start: int
    end: int


class Coverage(BaseModel):
    """What was asked for vs what came back.

    ``gaps`` is required bookkeeping before any chunk may be considered
    immutable — a range fetched through a provider outage must not freeze
    incomplete.
    """

    requested_start: int | None = None
    requested_end: int | None = None
    returned_start: int | None = None
    returned_end: int | None = None
    truncated: bool = False
    is_complete: bool = False
    gaps: list[Gap] = Field(default_factory=list)


class OhlcvBar(BaseModel):
    """One aggregate window. ``ts_event`` = window OPEN, Unix ms UTC.

    ``volume`` is required-but-nullable: null means "not applicable"
    (index bars), never "unknown". The ``time`` computed field is the
    transitional legacy alias — removed from protocol endpoints after
    Phase 4; legacy wrapper responses keep it indefinitely.
    """

    model_config = ConfigDict(populate_by_name=True)

    ts_event: int = Field(validation_alias=AliasChoices("ts_event", "time"))
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    vwap: float | None = None
    trades: int | None = None
    is_final: bool = False

    @computed_field  # type: ignore[prop-decorator]
    @property
    def time(self) -> int:
        return self.ts_event


class SeriesHeader(BaseModel):
    """Declared lineage and semantics of a record series — never inferred."""

    model_config = ConfigDict(populate_by_name=True)

    instrument_key: str
    schema_id: str = Field(
        validation_alias=AliasChoices("schema", "schema_id"),
        serialization_alias="schema",
    )
    price_treatment: PriceTreatment
    # publisher/asof/fetched_at are null only for empty/legacy envelopes that
    # carry no lineage; a filled series always declares all three.
    publisher: str | None = None
    tier: Tier
    feed_scope: FeedScope = FeedScope.COMPOSITE
    price_currency: str
    display_decimals: int
    display_unit: str | None = None
    ts_unit: str = "ms"
    latest_trading_date: str | None = None
    revision: int = 0
    asof: int | None = None
    coverage: Coverage = Field(default_factory=Coverage)
    fetched_at: int | None = None
    watermark: int | None = None
    schema_version: int = SCHEMA_VERSION


class Series(BaseModel):
    """The one container for record streams — becomes cache envelope v4."""

    header: SeriesHeader
    records: list[OhlcvBar]

    def to_wire(self) -> dict:
        return self.model_dump(by_alias=True, mode="json")


class InstrumentRef(BaseModel):
    """Registry identity for one instrument (YAML-seeded, heuristic-backed).

    ``mic`` is primary/listing identity (synthetic ``INDEX``/``CRYPTO``/``FX``
    segments for non-listed instruments); ``display_unit`` is a hint (e.g.
    GBX quotes on XLON) — unit conversion itself is per-provider-normalizer,
    never derived from a global flag.
    """

    instrument_key: str
    symbol: str
    mic: str
    asset_class: AssetClass
    name: str | None = None
    currency: str
    price_currency: str
    display_unit: str | None = None
    calendar_id: str
    tz: str
    index_family: str | None = None
