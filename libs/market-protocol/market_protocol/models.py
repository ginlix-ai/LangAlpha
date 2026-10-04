"""Protocol record and container models (Pydantic v2).

Wire rules: prices are finite float64 in major currency units; ``ts_event``,
``Quote.as_of`` and the header's ``asof``/``fetched_at``/``watermark`` are Unix
milliseconds UTC; a bar is anchored at the OPEN of its aggregate window, so a
daily bar is stamped 00:00 of its trade date in ``InstrumentRef.tz`` (UTC for
crypto and FX), not at the session's open. Evolution is additive (``schema_version``): a reader ignores a field it does
not know, but enums are closed, so a reader's pin must move before a producer
emits a new enum value. The wire form is ``Series.to_wire()``, or
``model_dump(by_alias=True, mode="json")`` for a model without one; a bare
``model_dump_json()`` writes the header's ``schema`` as ``schema_id``.
"""

from __future__ import annotations

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field, field_validator

from .enums import AssetClass, FeedScope, PriceTreatment, Tier
from .intervals import to_schema

SCHEMA_VERSION = 1


class Gap(BaseModel):
    """A missing sub-range inside a series' returned coverage (ms, [start, end))."""

    start: int
    end: int


class Coverage(BaseModel):
    """What was asked for vs what came back.

    ``gaps`` names the holes inside the returned range, so a consumer that
    stores a range as final can tell one fetched through a provider outage
    from a complete one.
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
    (index bars), never "unknown". The ``time`` computed field mirrors
    ``ts_event`` for readers of the legacy bar shape and is always emitted.
    """

    # A field set after construction meets the same finite rule, so a NaN
    # cannot reach the wire through an assignment.
    model_config = ConfigDict(
        populate_by_name=True, allow_inf_nan=False, validate_assignment=True
    )

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
    """Declared lineage and semantics of a record series, never inferred."""

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

    @field_validator("schema_id", mode="before")
    @classmethod
    def _canonical_schema(cls, value: object) -> object:
        # Here rather than in a builder, so a header from any producer or off
        # the wire carries the canonical id (1min is ohlcv-1m) or fails.
        return to_schema(value) if isinstance(value, str) else value


class Series(BaseModel):
    """The one container for record streams."""

    header: SeriesHeader
    records: list[OhlcvBar]

    def to_wire(self) -> dict:
        """The wire form, keeping the header's ``schema`` alias that ``model_dump_json()`` drops."""
        return self.model_dump(by_alias=True, mode="json")


class InstrumentRef(BaseModel):
    """Registry identity for one instrument (YAML-seeded, heuristic-backed).

    ``mic`` is primary/listing identity (synthetic ``INDEX``/``CRYPTO``/``FX``
    segments for non-listed instruments). ``currency`` is the listing's and
    ``price_currency`` the one its prices are in, which differ for a USD-quoted
    fund on a GBP venue. ``display_unit`` is the unit the venue quotes in where
    that is not ``price_currency``'s major unit (GBX on XLON): rows reach
    :func:`~market_protocol.series.build_series` in it, and the builder scales
    them by it.
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


class Quote(BaseModel):
    """A point-in-time price for one instrument, with its provenance.

    ``as_of`` is when the price is known to hold, in ms UTC: the print time
    when the feed publishes one, else when the row was received. ``tier`` is
    the feed's freshness, declared by the publisher, never inferred from the
    transport. ``name_local`` is the listing-market name (e.g. 贵州茅台) where
    it differs from ``name``.
    """

    model_config = ConfigDict(allow_inf_nan=False, validate_assignment=True)

    instrument_key: str
    symbol: str
    name: str | None = None
    name_local: str | None = None
    price: float
    change: float | None = None
    change_percent: float | None = None
    previous_close: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    amount: float | None = None
    currency: str
    tier: Tier
    publisher: str
    as_of: int | None = None
