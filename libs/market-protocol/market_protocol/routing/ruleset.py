"""Ruleset schema, ordering policy and loader.

A **cell** is one routing decision: ``(market, asset_class, surface, interval)``.
Each cell lists every probed provider with what the probe measured, ordered by
:func:`order_providers`. The policy is code (reviewable, deterministic); the
measurements are data (regenerated when entitlements change). Nothing in a
ruleset file identifies a token beyond a short fingerprint.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ..enums import AssetClass, PriceTreatment, Tier
from ..freshness import MIN_CREDIBLE_LAG_S, classify_lag
from ..intervals import schema_seconds, to_schema

logger = logging.getLogger(__name__)

# 2: intraday cells key on the schema id (``ohlcv-1m``). Version 1 stored the
# bare width (``1m``); ``CellKey`` reads either, so a v1 file loads and is
# migrated in memory. A new field bumps the version too: a reader ignores
# fields it does not know, so an older writer would drop it on a rewrite
# unless the version stops that writer first.
RULESET_VERSION = 2
_MIGRATABLE_VERSIONS = frozenset({1})

# A provider whose per-minute quota is below this cannot back a chart load
# for a handful of symbols; it is kept in the cell (so the file shows why)
# but excluded from routing.
MIN_CALLS_PER_MINUTE_FOR_BARS = 5


class Surface(StrEnum):
    INTRADAY = "intraday"
    DAILY = "daily"
    SNAPSHOT = "snapshot"
    # Not probed: ordered by a deployment's configured defaults, never a cell.
    FUNDAMENTALS = "fundamentals"
    DIRECTORY = "directory"
    STATUS = "status"
    NEWS = "news"


# The surfaces an entitlement probe measures and a ruleset carries cells for.
PROBED_SURFACES: tuple[Surface, ...] = (Surface.INTRADAY, Surface.DAILY, Surface.SNAPSHOT)


class CellKey(BaseModel, frozen=True):
    # Routing token (``market_of``): us / cn / hk / jp / kr / tw / sg / in / au /
    # ca / uk / eu / crypto / fx / other.
    market: str
    asset_class: AssetClass
    surface: Surface
    interval: str | None = None       # intraday: schema id (``ohlcv-1m``); None otherwise

    @field_validator("interval", mode="before")
    @classmethod
    def _schema_id(cls, value: object) -> object:
        return to_schema(value) if isinstance(value, str) else value

    def as_str(self) -> str:
        tail = f"/{self.interval}" if self.interval else ""
        return f"{self.market}/{self.asset_class}/{self.surface.value}{tail}"


class ProbedProvider(BaseModel):
    """What the probe measured for one provider in one cell."""

    # The probe fills a record field by field; validating each assignment keeps
    # a tier or treatment typed however the value arrived.
    model_config = ConfigDict(validate_assignment=True)

    name: str
    entitled: bool = True                     # False on a permission denial
    coverage_hit: int = 0                     # canaries that returned usable rows
    coverage_total: int = 0
    session_complete: bool | None = None      # bars: last bar reaches the venue's final bar
    anchors_ok: bool | None = None            # bars: timestamps land on venue anchors, no fabricated bars
    lag_s: int | None = None                  # measured expected-minus-actual, open session only
    tier: Tier | None = None                  # measured class when lag_s known, else declared
    calls_per_minute: int | None = None       # parsed from the provider's own rate-limit message
    cost_calls: int | None = None             # calls this provider needed per canary request
    price_treatment: PriceTreatment | None = None  # bars: what the series declares
    excluded_reason: str | None = None        # set by the policy; None means routable
    notes: list[str] = Field(default_factory=list)

    @property
    def covers(self) -> bool:
        return self.coverage_total > 0 and self.coverage_hit > 0

    @property
    def routable(self) -> bool:
        return self.excluded_reason is None


class Cell(BaseModel):
    key: CellKey
    probed_at: datetime
    # Legacy envelope phase (pre / open / post / closed), not MarketPhase:
    # generated files already carry this vocabulary.
    phase_at_probe: str = "closed"
    canaries: list[str] = Field(default_factory=list)
    providers: list[ProbedProvider] = Field(default_factory=list)

    def routable_names(self) -> list[str]:
        return [p.name for p in self.providers if p.routable]


class ProviderInfo(BaseModel):
    configured: bool = True
    fingerprint: str | None = None            # sha256 prefix of the credential, never the credential


class Ruleset(BaseModel):
    version: int = RULESET_VERSION
    generated_at: datetime
    probe_version: int = 1
    providers: dict[str, ProviderInfo] = Field(default_factory=dict)
    cells: list[Cell] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

_TIER_RANK = {Tier.REALTIME: 0, Tier.DELAYED_15M: 1, Tier.EOD: 2}
# Neither measured nor declared: no claim to any tier.
_UNKNOWN_TIER_RANK = len(_TIER_RANK)
# Within a tier, any measured lag ranks ahead of an unmeasured one.
_UNMEASURED_LAG_S = 10**9
# A provider that never stated a quota has not been seen to run out of one.
_UNSTATED_CALLS_PER_MINUTE = 10**6
# Unknown treatment ranks with raw: a provider that does not declare
# adjustment cannot be assumed to have applied it.
_ADJUSTMENT_RANK = {PriceTreatment.DIVIDEND_ADJUSTED: 0, PriceTreatment.SPLIT_ADJUSTED: 0}


def order_providers(surface: Surface | str, providers: Iterable[ProbedProvider]) -> list[ProbedProvider]:
    """Apply the routing policy: hard filters first, then a fixed ranking.

    Filters set ``excluded_reason`` and keep the provider in the list so the
    file explains itself. Ranking, first difference wins: routable before
    excluded; adjusted before raw for daily bars; freshness tier; measured lag,
    unmeasured last; more calls per minute; fewer calls per request. The tier is
    the one the measured lag earns when there is one (``classify_lag``, so past
    the delayed ceiling a feed ranks as end-of-day), because a measurement
    outranks a claim; else the declared tier; else below every tier. A lag
    further ahead of the clock than ``MIN_CREDIBLE_LAG_S`` measures a clock
    fault, not freshness, so it ranks as no lag and no tier. Stable, so equal
    providers keep probe order.
    """
    # Every check below compares by identity, which a plain string fails.
    surface = Surface(surface)
    out: list[ProbedProvider] = []
    for p in providers:
        p = p.model_copy(deep=True)
        p.excluded_reason = None
        quota_below_floor = (
            surface is not Surface.SNAPSHOT
            and p.calls_per_minute is not None
            and p.calls_per_minute < MIN_CALLS_PER_MINUTE_FOR_BARS
        )
        if not p.entitled:
            p.excluded_reason = "not_entitled"
        elif quota_below_floor:
            # Checked before coverage: a throttled provider returns no rows,
            # and "no coverage" would hide the real reason.
            p.excluded_reason = "quota_below_floor"
        elif not p.covers:
            p.excluded_reason = "no_coverage"
        elif surface is not Surface.SNAPSHOT and p.session_complete is False:
            p.excluded_reason = "incomplete_session"
        elif surface is not Surface.SNAPSHOT and p.anchors_ok is False:
            p.excluded_reason = "bad_anchors"
        out.append(p)

    def rank(p: ProbedProvider) -> tuple:
        excluded = 0 if p.routable else 1
        # Daily bars: an adjusted series ranks above a raw one before any
        # freshness, cost or quota consideration; a chart of history is wrong without it.
        adjustment = _ADJUSTMENT_RANK.get(p.price_treatment, 1) if surface is Surface.DAILY else 0
        if p.lag_s is None:
            tier, lag = p.tier, _UNMEASURED_LAG_S
        elif p.lag_s < MIN_CREDIBLE_LAG_S:
            # The recorded tier is this lag's class, so it is discarded with it.
            tier, lag = None, _UNMEASURED_LAG_S
        else:
            tier, lag = classify_lag(p.lag_s), p.lag_s
        tier_rank = _TIER_RANK.get(tier, _UNKNOWN_TIER_RANK)
        quota = -(p.calls_per_minute if p.calls_per_minute is not None else _UNSTATED_CALLS_PER_MINUTE)
        cost = p.cost_calls if p.cost_calls is not None else 1
        return (excluded, adjustment, tier_rank, lag, quota, cost)

    out.sort(key=rank)
    return out


# ---------------------------------------------------------------------------
# Runtime view
# ---------------------------------------------------------------------------


class RoutingTable:
    """Read-only lookup the provider chain consults per request.

    Returns the ordered routable provider names for a cell, or ``None`` when
    the ruleset has no cell for it, in which case the chain falls back to the
    caller's configured provider order. Interval lookups fall back to the
    interval-less cell of the same surface, then to the probed interval nearest
    the one asked for (the widest probed bar that is no wider than it, else
    the narrowest), so a probe that measured ``1m`` and ``5m`` still governs
    ``4h``: entitlement, coverage and session anchors are properties of the
    provider's feed for that venue, not of one bar width.
    """

    def __init__(self, ruleset: Ruleset) -> None:
        self._ruleset = ruleset
        self._index: dict[CellKey, list[str]] = {
            c.key: c.routable_names() for c in ruleset.cells
        }

    @property
    def ruleset(self) -> Ruleset:
        return self._ruleset

    def order_for(
        self,
        surface: Surface | str,
        market: str,
        asset_class: AssetClass | str,
        interval: str | None = None,
    ) -> list[str] | None:
        """Routable providers for a cell; *interval* is any spelling ``to_schema`` reads."""
        surface = Surface(surface)
        asset_class = AssetClass(asset_class)
        schema: str | None = None
        if interval is not None:
            try:
                schema = to_schema(interval)
            except ValueError:
                schema = None  # unreadable width: only the interval-less cell can answer
            else:
                key = CellKey(market=market, asset_class=asset_class, surface=surface, interval=schema)
                if key in self._index:
                    return list(self._index[key])
        key = CellKey(market=market, asset_class=asset_class, surface=surface)
        if key in self._index:
            return list(self._index[key])
        if schema is not None:
            nearest = self._nearest_interval(surface, market, asset_class, schema)
            if nearest is not None:
                return list(self._index[nearest])
        return None

    def _nearest_interval(
        self, surface: Surface, market: str, asset_class: AssetClass, schema: str
    ) -> CellKey | None:
        want = schema_seconds(schema)
        candidates = [
            (schema_seconds(key.interval), key)
            for key in self._index
            if key.interval
            and key.surface is surface
            and key.market == market
            and key.asset_class is asset_class
        ]
        if not candidates:
            return None
        below = [c for c in candidates if c[0] <= want]
        pick = max(below) if below else min(candidates)
        return pick[1]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


class _RulesetLoader(yaml.SafeLoader):
    """``SafeLoader`` that refuses anchors and aliases.

    A generated ruleset never uses them, and a handful of nested aliases is
    enough to expand into millions of nodes once validation walks the tree.
    """

    def compose_node(self, parent, index):
        event = self.peek_event()
        if isinstance(event, yaml.AliasEvent) or event.anchor is not None:
            raise yaml.composer.ComposerError(
                None, None, "anchors and aliases are not allowed in a ruleset", event.start_mark
            )
        return super().compose_node(parent, index)


class _UnknownVersion(ValueError):
    def __init__(self, version: int) -> None:
        super().__init__(f"unknown ruleset version {version}")
        self.version = version


def load_ruleset(path: str | os.PathLike[str], *, strict: bool = False) -> Ruleset | None:
    """Parse the ruleset at *path*; ``None`` when it is absent.

    Where the file lives is the deployment's decision, so the caller names it.
    A reader also gets ``None`` for a file it cannot use (logged), and a
    malformed cell is logged and skipped rather than costing the whole file:
    the other cells still route, and that one falls back to the configured
    order. A writer passes *strict*, which raises ValueError for anything but
    an absent file, so a rewrite never starts over a file it could not read
    or drops a cell or a version it does not know.
    """
    path = Path(path)
    try:
        if not path.exists():
            return None
        version, rs = _parse(path, strict=strict)
    except Exception as exc:  # a broken file must never take the chain down
        if strict:
            raise ValueError(f"{path}: {exc}") from exc
        if isinstance(exc, _UnknownVersion):
            logger.warning(
                "data_routing.ruleset.version_mismatch | path=%s file=%s expected=%s",
                path, exc.version, RULESET_VERSION,
            )
        else:
            logger.warning("data_routing.ruleset.unreadable | path=%s error=%s", path, exc)
        return None
    if version in _MIGRATABLE_VERSIONS:
        logger.info(
            "data_routing.ruleset.migrated | path=%s file=%s to=%s", path, version, RULESET_VERSION
        )
    return rs


def _parse(path: Path, *, strict: bool) -> tuple[int, Ruleset]:
    raw = yaml.load(path.read_bytes(), Loader=_RulesetLoader)
    if not isinstance(raw, dict):
        raise ValueError("the top level is not a mapping")
    # Read before anything is validated: the version decides how the rest
    # reads, and a file that states none makes no promise about the schema.
    version = raw.get("version")
    if type(version) is not int:  # bool is an int to Python, never a version
        raise ValueError(f"version is {version!r}, not an integer")
    if version != RULESET_VERSION and version not in _MIGRATABLE_VERSIONS:
        raise _UnknownVersion(version)
    cells = raw.get("cells", [])
    if not isinstance(cells, list):
        raise ValueError("cells is not a list")
    rs = Ruleset.model_validate(
        {**raw, "version": RULESET_VERSION, "cells": _valid_cells(path, cells, strict=strict)}
    )
    return version, rs


def _valid_cells(path: Path, items: list[object], *, strict: bool) -> list[Cell]:
    cells: list[Cell] = []
    for index, item in enumerate(items):
        try:
            cells.append(Cell.model_validate(item))
        except ValidationError as exc:
            if strict:
                raise ValueError(f"cell {index} ({_key_label(item)}): {exc}") from exc
            logger.warning(
                "data_routing.ruleset.cell_skipped | path=%s index=%s key=%s error=%s",
                path, index, _key_label(item), exc,
            )
    return cells


def _key_label(item: object) -> str:
    """The cell's key as ``as_str`` spells it, else as the file wrote it."""
    key = item.get("key") if isinstance(item, dict) else None
    try:
        return CellKey.model_validate(key).as_str()
    except ValidationError:
        return repr(key)
