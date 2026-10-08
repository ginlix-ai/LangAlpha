"""User-data IO layer: fetch + serialize + diff + apply for portfolio, watchlist, preferences, user.

Called by ``profile_files`` to serve the four virtual files at
``.agents/user/profile/{portfolio,watchlist,preference,user}.json`` and validate agent writes.

Decimal precision: stdlib ``json`` cannot emit ``Decimal`` as a JSON number, so
quantity / cost fields are serialized as JSON strings (e.g. ``"quantity": "100.50"``).
On parse, strings are converted back to ``Decimal`` so round-trips are exact
across ``DECIMAL(18,8)`` / ``DECIMAL(18,4)`` columns.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import available_timezones

from psycopg.types.json import Json

from ptc_agent.agent.backends.db_json_route import UnreadableJsonError, UserDataValidationError, load_json
from src.server.database import portfolio as portfolio_db
from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.database.pool import get_db_connection
from src.server.models.user import normalize_instrument_type, normalize_symbol
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)

# Sentinel hash returned for empty/cold-user payloads. Stable across replicas
# so two readers of an empty profile agree on the version without hitting DB
# timestamps that don't exist yet.
EMPTY_VERSION = "sha256:0"


# =============================================================================
# JSON encoding / decoding helpers
# =============================================================================


def _json_default(obj: Any) -> Any:
    """JSON serialization hook for Decimal / datetime / UUID."""
    if isinstance(obj, Decimal):
        # Emit as string — stdlib json has no native Decimal-as-number support.
        return format(obj.normalize(), "f") if obj == obj.normalize() else str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Not JSON serializable: {type(obj).__name__}")


def serialize_json(payload: dict[str, Any]) -> str:
    """Render the read payload as pretty-printed JSON the agent can edit."""
    return json.dumps(payload, default=_json_default, indent=2, ensure_ascii=False)


def parse_json(content: str, file: str) -> dict[str, Any]:
    """Parse agent-written JSON. Raises UserDataValidationError on parse failure."""
    try:
        return load_json(content)
    except json.JSONDecodeError as e:
        raise UserDataValidationError(
            error_type="parse_error",
            file=file,
            field_path=f"line {e.lineno} col {e.colno}",
            hint=f"invalid JSON: {e.msg}. Re-read the file and write a syntactically valid JSON object.",
        ) from None
    except UnreadableJsonError as e:
        raise UserDataValidationError(error_type="parse_error", file=file, field_path="", hint=f"invalid JSON: {e}") from None


def _coerce_decimal(value: Any, file: str, field_path: str) -> Decimal:
    """Convert a JSON value to Decimal. Accepts strings, ints, floats."""
    if value is None:
        raise UserDataValidationError(
            error_type="schema_error",
            file=file,
            field_path=field_path,
            hint="required numeric field is missing or null",
        )
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, str)):
            return Decimal(value)
        if isinstance(value, float):
            # Round-trip through str to avoid float repr noise (e.g. 0.1 → "0.1").
            return Decimal(str(value))
    except (InvalidOperation, ValueError):
        pass
    raise UserDataValidationError(
        error_type="schema_error",
        file=file,
        field_path=field_path,
        hint=f"expected a numeric value (string or number), got {value!r}",
    )


def _coerce_str(value: Any, file: str, field_path: str, *, required: bool = True) -> str | None:
    """Convert to str. Returns None for null when not required."""
    if value is None:
        if required:
            raise UserDataValidationError(
                error_type="schema_error",
                file=file,
                field_path=field_path,
                hint="required string field is missing or null",
            )
        return None
    if not isinstance(value, str):
        raise UserDataValidationError(
            error_type="schema_error",
            file=file,
            field_path=field_path,
            hint=f"expected string, got {type(value).__name__}",
        )
    return value


# ---------------------------------------------------------------------------
# Strict-validation helpers
#
# The agent writes JSON; the DB enforces uniqueness and length constraints.
# Catching typos, duplicates, and overlong values at parse time gives the
# agent a clear hint ("unknown field 'symbo' — did you mean 'symbol'?")
# instead of an opaque ``constraint_error`` after a rollback.
# ---------------------------------------------------------------------------

# Allowed top-level keys per object. `id` is tolerated on input (silently
# dropped) — see test_agent_supplied_id_is_silently_ignored.
_PORTFOLIO_ROW_KEYS: frozenset[str] = frozenset({
    "symbol", "instrument_type", "exchange", "name",
    "quantity", "average_cost", "currency", "account_name",
    "notes", "first_purchased_at",
})

_WATCHLIST_ROW_KEYS: frozenset[str] = frozenset({
    "name", "description", "is_default", "items",
})

_WATCHLIST_ITEM_KEYS: frozenset[str] = frozenset({
    "symbol", "instrument_type", "exchange", "name",
    "notes", "alert_settings",
})

# Column length limits — kept in sync with migrations/versions/001_initial_schema.py.
_PORTFOLIO_MAX_LEN: dict[str, int] = {
    "symbol": 50, "instrument_type": 30, "exchange": 50,
    "name": 255, "currency": 10, "account_name": 100,
}
_WATCHLIST_MAX_LEN: dict[str, int] = {"name": 100}
_WATCHLIST_ITEM_MAX_LEN: dict[str, int] = {
    "symbol": 50, "instrument_type": 30, "exchange": 50, "name": 255,
}


def _reject_unknown_keys(
    row: dict[str, Any],
    allowed: frozenset[str],
    *,
    file: str,
    path: str,
    tolerate: frozenset[str] = frozenset({"id"}),
) -> None:
    """Reject unknown fields with a helpful suggestion. Tolerated keys are silently ignored."""
    unknown = set(row) - allowed - tolerate
    if not unknown:
        return
    sample = next(iter(unknown))
    suggestion = _suggest_field(sample, allowed)
    suggest_part = f" — did you mean {suggestion!r}?" if suggestion else ""
    raise UserDataValidationError(
        "schema_error", file, path,
        f"unknown field(s) {sorted(unknown)!r}. allowed: {sorted(allowed)!r}.{suggest_part}",
    )


def _suggest_field(unknown: str, allowed: frozenset[str]) -> str | None:
    """Cheap typo suggestion: substring containment or shared 3-char prefix."""
    lower = unknown.lower()
    for name in allowed:
        nl = name.lower()
        if lower == nl or lower in nl or nl in lower:
            return name
        if len(lower) >= 3 and lower[:3] == nl[:3]:
            return name
    return None


def _check_max_len(
    value: str | None, max_len: int, *, file: str, path: str,
) -> None:
    if value is not None and len(value) > max_len:
        raise UserDataValidationError(
            "schema_error", file, path,
            f"value too long ({len(value)} chars); max is {max_len}.",
        )


def _require_nonempty_str(value: Any, *, file: str, path: str) -> str:
    """Required string that must be non-empty after stripping whitespace."""
    coerced = _coerce_str(value, file, path, required=True)
    stripped = coerced.strip() if coerced is not None else ""
    if not stripped:
        raise UserDataValidationError(
            "schema_error", file, path,
            "value is empty or whitespace-only.",
        )
    return stripped


def _reject_negative(
    value: Decimal | None, *, file: str, path: str,
) -> None:
    if value is not None and value < 0:
        raise UserDataValidationError(
            "schema_error", file, path,
            f"must be >= 0, got {value}.",
        )


def _content_hash(payload: dict[str, Any]) -> str:
    """Hash of the agent-visible content for use as ``__version__``.

    Excludes ``__version__`` itself so the hash is stable across the serialize →
    parse round-trip. Hashing content (not ``updated_at``) means an unrelated
    writer that bumps timestamps without changing agent-visible state leaves
    the version unchanged — no spurious version_conflict on the next write.

    Canonicalization recursively sorts list elements (by JSON string form) so
    the hash is invariant to row-order differences between the pre-check fetch
    and the in-transaction recheck. Without this, an ``ORDER BY`` drift between
    fetch helpers would surface as a phantom version_conflict.
    """
    canonical = _canonicalize({k: v for k, v in payload.items() if k != "__version__"})
    encoded = json.dumps(canonical, default=_json_default, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _canonicalize(obj: Any) -> Any:
    """Return a representation of ``obj`` with all nested lists in stable order."""
    if isinstance(obj, dict):
        return {k: _canonicalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return sorted(
            (_canonicalize(item) for item in obj),
            key=lambda x: json.dumps(x, default=_json_default, sort_keys=True, ensure_ascii=False),
        )
    return obj


def _stamp_version(payload: dict[str, Any]) -> dict[str, Any]:
    """Set ``__version__`` to the content hash of ``payload`` in place."""
    payload["__version__"] = _content_hash(payload)
    return payload


# =============================================================================
# Portfolio
# =============================================================================


async def fetch_portfolio_for_user(user_id: str) -> list[dict[str, Any]]:
    """All `user_portfolios` rows for the user, sorted newest-first (matches existing API)."""
    return await portfolio_db.get_user_portfolio(user_id)


async def count_portfolio_for_user(user_id: str) -> int:
    """Lightweight count for the awareness block."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM user_portfolios WHERE user_id = %s",
                (user_id,),
            )
            (count,) = await cur.fetchone()
            return int(count)


def serialize_portfolio(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """DB rows → JSON-ready dict (numbers as strings to preserve Decimal precision)."""
    holdings: list[dict[str, Any]] = []
    for row in rows:
        first_purchased = row.get("first_purchased_at")
        # DB column is TIMESTAMPTZ; emit YYYY-MM-DD so the agent-visible form
        # matches the README and round-trips cleanly without phantom diffs from
        # Postgres canonicalizing a bare date back to a midnight datetime.
        if isinstance(first_purchased, datetime):
            first_purchased_iso = first_purchased.date().isoformat()
        elif isinstance(first_purchased, date):
            first_purchased_iso = first_purchased.isoformat()
        else:
            first_purchased_iso = None
        holdings.append(
            {
                "symbol": row["symbol"],
                "instrument_type": row["instrument_type"],
                "exchange": row.get("exchange"),
                "name": row.get("name"),
                "quantity": format(row["quantity"], "f") if row.get("quantity") is not None else None,
                "average_cost": format(row["average_cost"], "f") if row.get("average_cost") is not None else None,
                "currency": row.get("currency"),
                "account_name": row.get("account_name"),
                "notes": row.get("notes"),
                "first_purchased_at": first_purchased_iso,
            }
        )
    return _stamp_version({
        "__version__": EMPTY_VERSION,
        "holdings": holdings,
    })


@dataclass
class PortfolioDiff:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)  # user_portfolio_ids

    def __bool__(self) -> bool:
        return bool(self.inserts or self.updates or self.deletes)


def parse_portfolio(content: str) -> list[dict[str, Any]]:
    """The holdings ``content`` holds, validated and normalized. Nothing here
    depends on the rows, so a save refuses bad content before reading any."""
    file = "portfolio.json"
    data = parse_json(content, file)

    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    holdings = data.get("holdings")
    if not isinstance(holdings, list):
        raise UserDataValidationError("schema_error", file, "holdings", "must be an array")

    parsed: list[dict[str, Any]] = []
    seen_payload_keys: set[tuple[str, str, str | None]] = set()

    for idx, item in enumerate(holdings):
        if not isinstance(item, dict):
            raise UserDataValidationError(
                "schema_error", file, f"holdings[{idx}]", "must be a JSON object",
            )

        _reject_unknown_keys(
            item, _PORTFOLIO_ROW_KEYS, file=file, path=f"holdings[{idx}]",
        )

        # Same normalization the dashboard's Pydantic models apply so the
        # agent and the UI agree on row identity (uppercase ticker, lowercase
        # instrument type). Without this, agent writes like "aapl"/"Stock"
        # bypass dedup against existing "AAPL"/"stock" rows.
        symbol = normalize_symbol(
            _require_nonempty_str(item.get("symbol"), file=file, path=f"holdings[{idx}].symbol")
        )
        instrument_type = normalize_instrument_type(
            _require_nonempty_str(
                item.get("instrument_type"), file=file, path=f"holdings[{idx}].instrument_type",
            )
        )
        quantity = _coerce_decimal(item.get("quantity"), file, f"holdings[{idx}].quantity")
        _reject_negative(quantity, file=file, path=f"holdings[{idx}].quantity")
        account_name = _coerce_str(item.get("account_name"), file, f"holdings[{idx}].account_name", required=False)
        avg_cost_raw = item.get("average_cost")
        average_cost = _coerce_decimal(avg_cost_raw, file, f"holdings[{idx}].average_cost") if avg_cost_raw is not None else None
        _reject_negative(average_cost, file=file, path=f"holdings[{idx}].average_cost")

        # Reject duplicates within the payload — DB has UNIQUE (user, symbol,
        # instrument_type, account_name) and would error out asymmetrically
        # depending on insert order.
        dup_key = (symbol, instrument_type, account_name)
        if dup_key in seen_payload_keys:
            raise UserDataValidationError(
                "schema_error", file, f"holdings[{idx}]",
                f"duplicate holding {dup_key!r} — same (symbol, instrument_type, account_name) "
                "already appears earlier in the array. Each holding row must be unique.",
            )
        seen_payload_keys.add(dup_key)

        normalized = {
            "symbol": symbol,
            "instrument_type": instrument_type,
            "exchange": _coerce_str(item.get("exchange"), file, f"holdings[{idx}].exchange", required=False),
            "name": _coerce_str(item.get("name"), file, f"holdings[{idx}].name", required=False),
            "quantity": quantity,
            "average_cost": average_cost,
            "currency": _coerce_str(item.get("currency"), file, f"holdings[{idx}].currency", required=False) or "USD",
            "account_name": account_name,
            "notes": _coerce_str(item.get("notes"), file, f"holdings[{idx}].notes", required=False),
            "first_purchased_at": _coerce_str(item.get("first_purchased_at"), file, f"holdings[{idx}].first_purchased_at", required=False),
        }

        for field_name, max_len in _PORTFOLIO_MAX_LEN.items():
            _check_max_len(
                normalized.get(field_name), max_len,
                file=file, path=f"holdings[{idx}].{field_name}",
            )
        parsed.append(normalized)
    return parsed


def diff_portfolio(
    holdings: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
) -> PortfolioDiff:
    """What writing ``parse_portfolio``'s holdings over ``current_rows`` changes."""
    # Identity is the natural unique key; the agent never sees DB UUIDs.
    by_unique_key: dict[tuple[str, str, str | None], dict[str, Any]] = {
        (r["symbol"], r["instrument_type"], r.get("account_name")): r for r in current_rows
    }

    diff = PortfolioDiff()
    seen_ids: set[str] = set()
    for normalized in holdings:
        existing = by_unique_key.get(
            (normalized["symbol"], normalized["instrument_type"], normalized["account_name"])
        )

        if existing is None:
            diff.inserts.append(normalized)
            continue

        seen_ids.add(str(existing["user_portfolio_id"]))
        # Only emit update if any field actually changed.
        if _portfolio_row_differs(existing, normalized):
            diff.updates.append({**normalized, "id": str(existing["user_portfolio_id"])})

    diff.deletes = [
        str(r["user_portfolio_id"]) for r in current_rows
        if str(r["user_portfolio_id"]) not in seen_ids
    ]
    return diff


def _portfolio_row_differs(existing: dict[str, Any], proposed: dict[str, Any]) -> bool:
    """Return True if any column the agent can change differs from the DB row."""
    for field_name in ("symbol", "instrument_type", "exchange", "name", "currency", "account_name", "notes"):
        if (existing.get(field_name) or None) != (proposed.get(field_name) or None):
            return True
    if (existing.get("quantity") or Decimal(0)) != (proposed.get("quantity") or Decimal(0)):
        return True
    if (existing.get("average_cost")) != (proposed.get("average_cost")):
        return True
    # Compare on the date component only — serialize_portfolio emits YYYY-MM-DD,
    # so a TIMESTAMPTZ existing value at any wall-clock time on the same day is
    # not a diff from the agent's perspective.
    proposed_date = proposed.get("first_purchased_at")
    existing_date = existing.get("first_purchased_at")
    if isinstance(existing_date, datetime):
        existing_iso = existing_date.date().isoformat()
    elif isinstance(existing_date, date):
        existing_iso = existing_date.isoformat()
    else:
        existing_iso = existing_date
    if (existing_iso or None) != (proposed_date or None):
        return True
    return False


async def write_portfolio_diff(cur: Any, diff: PortfolioDiff, user_id: str) -> None:
    """Issue the diff's statements on ``cur``, whose transaction holds the
    profile lock and has checked the version, so the statements need no
    per-row CAS. Each kind of change goes as one batch, sent in one round
    trip."""
    if not diff:
        return

    if diff.inserts:
        await cur.executemany(
            """
            INSERT INTO user_portfolios (
                user_portfolio_id, user_id, symbol, instrument_type, exchange,
                name, quantity, average_cost, currency, account_name,
                notes, metadata, first_purchased_at, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            [
                (
                    row.get("id") or str(uuid4()),
                    user_id, row["symbol"], row["instrument_type"],
                    row.get("exchange"), row.get("name"), row["quantity"],
                    row.get("average_cost"), row.get("currency") or "USD",
                    row.get("account_name"), row.get("notes"),
                    Json({}), row.get("first_purchased_at"),
                )
                for row in diff.inserts
            ],
        )

    if diff.updates:
        await cur.executemany(
            """
            UPDATE user_portfolios SET
                symbol = %s, instrument_type = %s, exchange = %s, name = %s,
                quantity = %s, average_cost = %s, currency = %s,
                account_name = %s, notes = %s, first_purchased_at = %s,
                updated_at = NOW()
            WHERE user_portfolio_id = %s AND user_id = %s
            """,
            [
                (
                    row["symbol"], row["instrument_type"], row.get("exchange"),
                    row.get("name"), row["quantity"], row.get("average_cost"),
                    row.get("currency") or "USD", row.get("account_name"),
                    row.get("notes"), row.get("first_purchased_at"),
                    row["id"], user_id,
                )
                for row in diff.updates
            ],
        )

    if diff.deletes:
        await cur.execute(
            "DELETE FROM user_portfolios WHERE user_id = %s "
            "AND user_portfolio_id = ANY(%s)",
            (user_id, diff.deletes),
        )

    logger.info(
        "[user_data_io] applied portfolio diff user_id=%s inserts=%d updates=%d deletes=%d",
        user_id, len(diff.inserts), len(diff.updates), len(diff.deletes),
    )


# =============================================================================
# Watchlist
# =============================================================================


async def fetch_watchlist_for_user(user_id: str) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """(watchlists, items_grouped_by_watchlist_id). Two parallel queries."""
    watchlists, items = await asyncio.gather(
        watchlist_db.get_user_watchlists(user_id),
        watchlist_db.get_all_user_watchlist_items(user_id),
    )
    return watchlists, items


async def list_watchlist_symbols_for_user(user_id: str) -> list[str]:
    """Distinct watchlist symbols in first-added order, for the market vote."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT wi.symbol, MIN(wi.created_at) AS first_added
                FROM watchlist_items wi
                INNER JOIN watchlists w ON wi.watchlist_id = w.watchlist_id
                WHERE w.user_id = %s AND wi.symbol IS NOT NULL
                GROUP BY wi.symbol
                ORDER BY first_added, wi.symbol
                """,
                (user_id,),
            )
            return [str(symbol) for (symbol, _first) in await cur.fetchall()]


async def count_watchlist_for_user(user_id: str) -> tuple[int, int]:
    """(num_watchlists, total_items) for the awareness block."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM watchlists WHERE user_id = %s", (user_id,))
            (wl_count,) = await cur.fetchone()
            await cur.execute(
                """
                SELECT COUNT(*) FROM watchlist_items wi
                INNER JOIN watchlists w ON wi.watchlist_id = w.watchlist_id
                WHERE w.user_id = %s
                """,
                (user_id,),
            )
            (item_count,) = await cur.fetchone()
            return int(wl_count), int(item_count)


def serialize_watchlist(
    watchlists: list[dict[str, Any]],
    items_by_wl: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    out_watchlists: list[dict[str, Any]] = []
    for wl in watchlists:
        wl_id = str(wl["watchlist_id"])
        items = []
        for item in items_by_wl.get(wl_id, []):
            items.append(
                {
                    "symbol": item["symbol"],
                    "instrument_type": item["instrument_type"],
                    "exchange": item.get("exchange"),
                    "name": item.get("name"),
                    "notes": item.get("notes"),
                    "alert_settings": item.get("alert_settings") or {},
                }
            )
        out_watchlists.append(
            {
                "name": wl["name"],
                "description": wl.get("description"),
                "is_default": bool(wl.get("is_default")),
                "items": items,
            }
        )

    return _stamp_version({
        "__version__": EMPTY_VERSION,
        "watchlists": out_watchlists,
    })


@dataclass
class WatchlistDiff:
    wl_inserts: list[dict[str, Any]] = field(default_factory=list)
    wl_updates: list[dict[str, Any]] = field(default_factory=list)
    wl_deletes: list[str] = field(default_factory=list)
    item_inserts: list[dict[str, Any]] = field(default_factory=list)  # carries watchlist_id
    item_updates: list[dict[str, Any]] = field(default_factory=list)
    item_deletes: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return any([
            self.wl_inserts, self.wl_updates, self.wl_deletes,
            self.item_inserts, self.item_updates, self.item_deletes,
        ])


def parse_watchlist(content: str) -> list[dict[str, Any]]:
    """The watchlists ``content`` holds, each with its ``items``, validated
    and normalized. Nothing here depends on the rows, so a save refuses bad
    content before reading any."""
    file = "watchlist.json"
    data = parse_json(content, file)
    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    watchlists_payload = data.get("watchlists")
    if not isinstance(watchlists_payload, list):
        raise UserDataValidationError("schema_error", file, "watchlists", "must be an array")

    parsed: list[dict[str, Any]] = []
    seen_wl_names: set[str] = set()
    default_seen = False

    for wl_idx, wl in enumerate(watchlists_payload):
        if not isinstance(wl, dict):
            raise UserDataValidationError("schema_error", file, f"watchlists[{wl_idx}]", "must be an object")

        _reject_unknown_keys(
            wl, _WATCHLIST_ROW_KEYS, file=file, path=f"watchlists[{wl_idx}]",
        )

        name = _require_nonempty_str(wl.get("name"), file=file, path=f"watchlists[{wl_idx}].name")
        _check_max_len(
            name, _WATCHLIST_MAX_LEN["name"],
            file=file, path=f"watchlists[{wl_idx}].name",
        )
        if name in seen_wl_names:
            raise UserDataValidationError(
                "schema_error", file, f"watchlists[{wl_idx}].name",
                f"duplicate watchlist name {name!r} — names must be unique across the array.",
            )
        seen_wl_names.add(name)

        description = _coerce_str(wl.get("description"), file, f"watchlists[{wl_idx}].description", required=False)
        is_default = bool(wl.get("is_default", False))
        if is_default:
            if default_seen:
                raise UserDataValidationError(
                    "schema_error", file, f"watchlists[{wl_idx}].is_default",
                    "only one watchlist may be marked is_default=true",
                )
            default_seen = True

        # Items
        items_payload = wl.get("items", [])
        if not isinstance(items_payload, list):
            raise UserDataValidationError("schema_error", file, f"watchlists[{wl_idx}].items", "must be an array")

        items: list[dict[str, Any]] = []
        seen_item_keys: set[tuple[str, str]] = set()
        for it_idx, item in enumerate(items_payload):
            if not isinstance(item, dict):
                raise UserDataValidationError(
                    "schema_error", file, f"watchlists[{wl_idx}].items[{it_idx}]", "must be an object",
                )
            _reject_unknown_keys(
                item, _WATCHLIST_ITEM_KEYS,
                file=file, path=f"watchlists[{wl_idx}].items[{it_idx}]",
            )

            symbol = normalize_symbol(
                _require_nonempty_str(
                    item.get("symbol"), file=file,
                    path=f"watchlists[{wl_idx}].items[{it_idx}].symbol",
                )
            )
            instrument_type = normalize_instrument_type(
                _require_nonempty_str(
                    item.get("instrument_type"), file=file,
                    path=f"watchlists[{wl_idx}].items[{it_idx}].instrument_type",
                )
            )

            dup_key = (symbol, instrument_type)
            if dup_key in seen_item_keys:
                raise UserDataValidationError(
                    "schema_error", file, f"watchlists[{wl_idx}].items[{it_idx}]",
                    f"duplicate item {dup_key!r} — same (symbol, instrument_type) "
                    f"already appears in watchlist {name!r}. Each item must be unique within a watchlist.",
                )
            seen_item_keys.add(dup_key)

            normalized = {
                "symbol": symbol,
                "instrument_type": instrument_type,
                "exchange": _coerce_str(item.get("exchange"), file, f"watchlists[{wl_idx}].items[{it_idx}].exchange", required=False),
                "name": _coerce_str(item.get("name"), file, f"watchlists[{wl_idx}].items[{it_idx}].name", required=False),
                "notes": _coerce_str(item.get("notes"), file, f"watchlists[{wl_idx}].items[{it_idx}].notes", required=False),
                "alert_settings": item.get("alert_settings") or {},
            }

            for field_name, max_len in _WATCHLIST_ITEM_MAX_LEN.items():
                _check_max_len(
                    normalized.get(field_name), max_len,
                    file=file, path=f"watchlists[{wl_idx}].items[{it_idx}].{field_name}",
                )
            items.append(normalized)

        parsed.append({"name": name, "description": description, "is_default": is_default, "items": items})
    return parsed


def diff_watchlist(
    watchlists: list[dict[str, Any]],
    current_watchlists: list[dict[str, Any]],
    current_items_by_wl: dict[str, list[dict[str, Any]]],
) -> WatchlistDiff:
    """What writing ``parse_watchlist``'s watchlists over the current DB
    state changes."""
    # Identity is the natural unique key; the agent never sees DB UUIDs.
    # Watchlist rename = recreate (delete old, insert new with fresh items),
    # since `name` is the only stable identity the agent has.
    wl_by_name: dict[str, dict[str, Any]] = {w["name"]: w for w in current_watchlists}

    diff = WatchlistDiff()
    seen_wl_ids: set[str] = set()

    for wl in watchlists:
        name, description, is_default = wl["name"], wl["description"], wl["is_default"]
        existing_wl = wl_by_name.get(name)
        current_items: list[dict[str, Any]] = (
            current_items_by_wl.get(str(existing_wl["watchlist_id"]), [])
            if existing_wl is not None
            else []
        )

        if existing_wl is None:
            new_id = str(uuid4())
            diff.wl_inserts.append(
                {"id": new_id, "name": name, "description": description, "is_default": is_default}
            )
            target_wl_id = new_id
            existing_items_by_key: dict[Any, dict[str, Any]] = {}
        else:
            target_wl_id = str(existing_wl["watchlist_id"])
            seen_wl_ids.add(target_wl_id)
            if (
                (existing_wl.get("description") or None) != (description or None)
                or bool(existing_wl.get("is_default")) != is_default
            ):
                diff.wl_updates.append(
                    {"id": target_wl_id, "name": name, "description": description, "is_default": is_default}
                )
            existing_items_by_key = {
                (it["symbol"], it["instrument_type"]): it for it in current_items
            }

        seen_item_ids: set[str] = set()
        for item in wl["items"]:
            normalized = {"watchlist_id": target_wl_id, **item}
            existing_item = existing_items_by_key.get((item["symbol"], item["instrument_type"]))
            if existing_item is None:
                diff.item_inserts.append(normalized)
            else:
                seen_item_ids.add(str(existing_item["watchlist_item_id"]))
                if _watchlist_item_differs(existing_item, normalized):
                    normalized["id"] = str(existing_item["watchlist_item_id"])
                    diff.item_updates.append(normalized)

        if existing_wl is not None:
            diff.item_deletes.extend(
                str(it["watchlist_item_id"]) for it in current_items
                if str(it["watchlist_item_id"]) not in seen_item_ids
            )

    diff.wl_deletes = [
        str(w["watchlist_id"]) for w in current_watchlists
        if str(w["watchlist_id"]) not in seen_wl_ids
    ]
    return diff


def _watchlist_item_differs(existing: dict[str, Any], proposed: dict[str, Any]) -> bool:
    for field_name in ("symbol", "instrument_type", "exchange", "name", "notes"):
        if (existing.get(field_name) or None) != (proposed.get(field_name) or None):
            return True
    if (existing.get("alert_settings") or {}) != (proposed.get("alert_settings") or {}):
        return True
    return False


async def write_watchlist_diff(cur: Any, diff: WatchlistDiff, user_id: str) -> None:
    """Issue the diff's statements on ``cur``, under the same lock and
    version check as ``write_portfolio_diff``, one batch per kind of change.

    The parse lets at most one watchlist be the default, so clearing the
    others once, before the batch that sets it, is what clearing them before
    each such row did.
    """
    if not diff:
        return

    # Item deletes first: explicit order even though watchlist deletes would
    # cascade through item rows.
    if diff.item_deletes:
        await cur.execute(
            "DELETE FROM watchlist_items WHERE user_id = %s "
            "AND watchlist_item_id = ANY(%s)",
            (user_id, diff.item_deletes),
        )

    if diff.wl_inserts:
        if any(wl.get("is_default") for wl in diff.wl_inserts):
            await cur.execute(
                "UPDATE watchlists SET is_default = FALSE, updated_at = NOW() "
                "WHERE user_id = %s AND is_default = TRUE",
                (user_id,),
            )
        await cur.executemany(
            """
            INSERT INTO watchlists (
                watchlist_id, user_id, name, description, is_default,
                display_order, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, 0, NOW(), NOW())
            """,
            [
                (wl["id"], user_id, wl["name"], wl.get("description"), bool(wl.get("is_default")))
                for wl in diff.wl_inserts
            ],
        )

    if diff.wl_updates:
        default = next((wl for wl in diff.wl_updates if wl.get("is_default")), None)
        if default is not None:
            await cur.execute(
                "UPDATE watchlists SET is_default = FALSE, updated_at = NOW() "
                "WHERE user_id = %s AND is_default = TRUE AND watchlist_id != %s",
                (user_id, default["id"]),
            )
        await cur.executemany(
            """
            UPDATE watchlists
            SET name = %s, description = %s, is_default = %s, updated_at = NOW()
            WHERE watchlist_id = %s AND user_id = %s
            """,
            [
                (
                    wl["name"], wl.get("description"), bool(wl.get("is_default")),
                    wl["id"], user_id,
                )
                for wl in diff.wl_updates
            ],
        )

    # Item inserts (honor agent-provided id if any, else generate)
    if diff.item_inserts:
        await cur.executemany(
            """
            INSERT INTO watchlist_items (
                watchlist_item_id, watchlist_id, user_id, symbol, instrument_type,
                exchange, name, notes, alert_settings, metadata,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            [
                (
                    item.get("id") or str(uuid4()),
                    item["watchlist_id"], user_id, item["symbol"], item["instrument_type"],
                    item.get("exchange"), item.get("name"), item.get("notes"),
                    Json(item.get("alert_settings") or {}),
                    Json({}),
                )
                for item in diff.item_inserts
            ],
        )

    if diff.item_updates:
        await cur.executemany(
            """
            UPDATE watchlist_items SET
                symbol = %s, instrument_type = %s, exchange = %s, name = %s,
                notes = %s, alert_settings = %s, updated_at = NOW()
            WHERE watchlist_item_id = %s AND user_id = %s
            """,
            [
                (
                    item["symbol"], item["instrument_type"], item.get("exchange"),
                    item.get("name"), item.get("notes"),
                    Json(item.get("alert_settings") or {}),
                    item["id"], user_id,
                )
                for item in diff.item_updates
            ],
        )

    # Watchlist deletes last (cascade removes any remaining items)
    if diff.wl_deletes:
        await cur.execute(
            "DELETE FROM watchlists WHERE user_id = %s "
            "AND watchlist_id = ANY(%s)",
            (user_id, diff.wl_deletes),
        )

    logger.info(
        "[user_data_io] applied watchlist diff user_id=%s wl_ins=%d wl_upd=%d wl_del=%d it_ins=%d it_upd=%d it_del=%d",
        user_id,
        len(diff.wl_inserts), len(diff.wl_updates), len(diff.wl_deletes),
        len(diff.item_inserts), len(diff.item_updates), len(diff.item_deletes),
    )


# =============================================================================
# Preferences
# =============================================================================


_PREFERENCE_KEYS = ("risk_preference", "investment_preference", "agent_preference", "other_preference")
# Keys exposed to the agent in preference.json. `other_preference` and
# `model_preference` are server-managed JSONB columns (onboarding state,
# platform flags, model routing) — keeping them out of the agent's view means
# the agent can't accidentally clobber them via replace-mode writes.
_AGENT_PREFERENCE_KEYS = ("risk_preference", "investment_preference", "agent_preference")
# Both server-managed keys are tolerated on input (silently dropped — see
# test_parse_silently_drops_other_preference) but treated as "known".
_PREFERENCE_ROOT_KEYS: frozenset[str] = frozenset(
    ("__version__", "other_preference", "model_preference", *_AGENT_PREFERENCE_KEYS)
)


async def fetch_preferences_for_user(user_id: str) -> dict[str, Any] | None:
    """Single row from `user_preferences`. None if the user has never set any."""
    return await user_db.get_user_preferences(user_id)


async def exists_preferences_for_user(user_id: str) -> bool:
    """Lightweight EXISTS for the awareness block."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT EXISTS(SELECT 1 FROM user_preferences WHERE user_id = %s)",
                (user_id,),
            )
            (exists,) = await cur.fetchone()
            return bool(exists)


def serialize_preferences(prefs: dict[str, Any] | None) -> dict[str, Any]:
    """Serialize agent-visible preference columns. Excludes `other_preference`."""
    payload: dict[str, Any] = {"__version__": EMPTY_VERSION}
    src = prefs or {}
    for key in _AGENT_PREFERENCE_KEYS:
        payload[key] = src.get(key) or {}
    return _stamp_version(payload)


def parse_preferences(content: str) -> dict[str, Any]:
    """Parse + validate. Returns the preference values dict."""
    file = "preference.json"
    data = parse_json(content, file)
    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    _reject_unknown_keys(
        data, _PREFERENCE_ROOT_KEYS, file=file, path="", tolerate=frozenset(),
    )

    result: dict[str, Any] = {}
    # Only parse agent-visible keys; ignore any `other_preference` the agent
    # might have copied through — that column is server-managed and the
    # applier preserves whatever's already in the DB.
    for key in _AGENT_PREFERENCE_KEYS:
        value = data.get(key)
        if value is None:
            result[key] = {}
            continue
        if not isinstance(value, dict):
            raise UserDataValidationError(
                "schema_error", file, key,
                f"expected an object, got {type(value).__name__}",
            )
        result[key] = value
    return result


def preferences_equal(current: dict[str, Any] | None, values: dict[str, Any]) -> bool:
    """True when the parsed payload is byte-equal to the current DB row.

    Callers use this to skip a no-op write transaction on identical edits.
    Treats missing/None values and empty dicts as equivalent (the schema
    normalizes both to ``{}``).
    """
    if current is None:
        return all(not (values.get(k) or {}) for k in _AGENT_PREFERENCE_KEYS)
    return all(
        (current.get(k) or {}) == (values.get(k) or {})
        for k in _AGENT_PREFERENCE_KEYS
    )


async def write_preferences(
    cur: Any, values: dict[str, Any], current: dict[str, Any] | None, user_id: str,
) -> None:
    """Replace-mode upsert of the agent-visible preference columns, over
    ``current`` as the version check read it.

    Writes only ``risk_preference``, ``investment_preference``, ``agent_preference``.
    ``other_preference`` and ``model_preference`` are left untouched: both are
    server-managed (onboarding state, platform flags, model routing) and not
    exposed to the agent. New users get empty objects on first insert, from
    the explicit value and the column default respectively; existing rows
    keep theirs. The caller drops the preferences cache once it commits.
    """
    if current is not None:
        await cur.execute(
            """
            UPDATE user_preferences SET
                risk_preference = %s::jsonb,
                investment_preference = %s::jsonb,
                agent_preference = %s::jsonb,
                updated_at = NOW()
            WHERE user_id = %s
            """,
            (
                Json(values.get("risk_preference") or {}),
                Json(values.get("investment_preference") or {}),
                Json(values.get("agent_preference") or {}),
                user_id,
            ),
        )
    else:
        await cur.execute(
            """
            INSERT INTO user_preferences (
                user_preference_id, user_id,
                risk_preference, investment_preference,
                agent_preference, other_preference,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            (
                str(uuid4()), user_id,
                Json(values.get("risk_preference") or {}),
                Json(values.get("investment_preference") or {}),
                Json(values.get("agent_preference") or {}),
                Json({}),
            ),
        )
    logger.info("[user_data_io] applied preferences user_id=%s", user_id)


# =============================================================================
# User (the account row)
# =============================================================================


_USER_KEYS: frozenset[str] = frozenset({"name", "timezone", "locale", "onboarding_completed"})
# Column lengths from migrations/versions/001_initial_schema.py.
_USER_MAX_LEN: dict[str, int] = {"name": 255, "timezone": 100, "locale": 20}
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")

# A language, then an optional script and region: `en`, `en-US`, `zh-Hans-CN`,
# `es-419`. At most 12 characters, inside the column. Variants and extensions
# are left out, since no reader of the column (the app's language switch, the
# prompt) has a use for them.
_LOCALE_TAG = re.compile(r"([A-Za-z]{2,3})(?:-([A-Za-z]{4}))?(?:-([A-Za-z]{2}|[0-9]{3}))?")


@functools.cache
def _iana_zones() -> dict[str, str]:
    """Every IANA zone name, keyed by its lowercase spelling. Not ``ZoneInfo``
    alone, which also opens files under the zone directory that name no zone
    a browser formats in (``posix/...``, ``right/...``). ``Factory`` and
    ``localtime`` are left out for the same reason: browsers refuse both."""
    return {
        zone.lower(): zone
        for zone in available_timezones()
        if zone not in ("Factory", "localtime")
    }


async def fetch_user_for_user(user_id: str) -> dict[str, Any] | None:
    """The user's row, None when there is none."""
    return await user_db.get_user(user_id)


def serialize_user(row: dict[str, Any] | None) -> dict[str, Any]:
    """The columns the agent may set, from the user's row."""
    src = row or {}
    return _stamp_version({
        "__version__": EMPTY_VERSION,
        "name": src.get("name"),
        "timezone": src.get("timezone"),
        "locale": src.get("locale"),
        "onboarding_completed": bool(src.get("onboarding_completed")),
    })


def parse_user(content: str) -> dict[str, Any]:
    """The columns ``content`` sets. A key left out keeps its value, so only
    the keys written are returned; a null clears a string column. Zone and
    locale are checked only by ``diff_user``, against the row: a value saved
    before these checks existed must survive a save that leaves it as it is."""
    file = "user.json"
    data = parse_json(content, file)
    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    _reject_unknown_keys(data, _USER_KEYS, file=file, path="", tolerate=frozenset())

    values: dict[str, Any] = {}
    for key, max_len in _USER_MAX_LEN.items():
        if key in data:
            value = _coerce_str(data[key], file, key, required=False)
            # Blank says no more than null does.
            value = (value or "").strip() or None
            _check_max_len(value, max_len, file=file, path=key)
            # Each value is printed as one line of the prompt's profile block.
            if value is not None and _CONTROL_CHARS.search(value):
                raise UserDataValidationError(
                    "schema_error", file, key, "must be one line, without control characters",
                )
            values[key] = value
    if "onboarding_completed" in data:
        flag = data["onboarding_completed"]
        if not isinstance(flag, bool):
            raise UserDataValidationError(
                "schema_error", file, "onboarding_completed",
                f"expected true or false, got {type(flag).__name__}",
            )
        values["onboarding_completed"] = flag
    return values


# POSIX signs: Etc/GMT+8 is eight hours behind UTC, the reverse of what a user
# means by "GMT+8", so taking one for the other runs every turn and new
# automation sixteen hours off.
_ETC_OFFSET = re.compile(r"etc/gmt[+-][1-9][0-9]*")


def _zone(value: str) -> str:
    if _ETC_OFFSET.fullmatch(value.lower()):
        raise UserDataValidationError(
            "schema_error", "user.json", "timezone",
            f"{value!r} counts its offset backwards ('Etc/GMT+8' is UTC-8). Use the zone of the "
            "user's city, like 'Asia/Shanghai'.",
        )
    zone = _iana_zones().get(value.lower())
    if zone is None:
        raise UserDataValidationError(
            "schema_error", "user.json", "timezone",
            f"{value!r} is not an IANA time zone. Use a name like 'America/New_York' or 'Asia/Shanghai', "
            "or null to clear it.",
        )
    return zone


def _locale(value: str) -> str:
    match = _LOCALE_TAG.fullmatch(value)
    if match is None:
        raise UserDataValidationError(
            "schema_error", "user.json", "locale",
            f"{value!r} is not a language tag. Use a language and region joined by a hyphen, "
            "like 'en-US' or 'zh-CN', or null to clear it.",
        )
    # Tags compare without regard to case; the app matches the usual
    # spelling, so `zh-cn` is stored as `zh-CN`.
    language, script, region = match.groups()
    return "-".join(
        part for part in (language.lower(), script and script.title(), region and region.upper()) if part
    )


def diff_user(values: dict[str, Any], row: dict[str, Any] | None) -> dict[str, Any]:
    """The columns writing ``parse_user``'s values over ``row`` changes, by
    column, each value checked and in its stored spelling."""
    current = row or {}
    changes: dict[str, Any] = {}
    for key, value in values.items():
        stored = current.get(key)
        # parse_user strips what it reads, so compare a stored value the same
        # way, or one saved with stray spaces is checked as if it were new.
        if isinstance(stored, str):
            stored = stored.strip()
        if value is not None and value != stored:
            if key == "timezone":
                value = _zone(value)
            elif key == "locale":
                value = _locale(value)
        if value != current.get(key):
            changes[key] = value
    return changes


async def write_user(cur: Any, changes: dict[str, Any], user_id: str) -> None:
    """One UPDATE of the columns ``changes`` names, as the account's other
    writers make it. Raises when the user has no row, since the save would
    otherwise report a change it never made."""
    builder = UpdateQueryBuilder()
    for column, value in changes.items():
        builder.add_field(column, value, nullable=True)
    query, params = builder.build(table="users", where_clause="user_id = %s", where_params=[user_id])
    await cur.execute(query, params)
    if cur.rowcount == 0:
        raise LookupError(f"no users row for user_id={user_id}")
    logger.info("[user_data_io] applied user changes user_id=%s columns=%s", user_id, sorted(changes))
