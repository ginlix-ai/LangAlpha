"""User-data IO layer: fetch + serialize + diff + apply for portfolio, watchlist, preferences.

Called by ``UserDataBackend`` to serve the three virtual files at
``.agents/user/profile/{portfolio,watchlist,preference}.json`` and validate agent writes.

Decimal precision: stdlib ``json`` cannot emit ``Decimal`` as a JSON number, so
quantity / cost fields are serialized as JSON strings (e.g. ``"quantity": "100.50"``).
On parse, strings are converted back to ``Decimal`` so round-trips are exact
across ``DECIMAL(18,8)`` / ``DECIMAL(18,4)`` columns.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database import portfolio as portfolio_db
from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.database.conversation import get_db_connection
from src.server.models.user import normalize_instrument_type, normalize_symbol

logger = logging.getLogger(__name__)

# Sentinel hash returned for empty/cold-user payloads. Stable across replicas
# so two readers of an empty profile agree on the version without hitting DB
# timestamps that don't exist yet.
EMPTY_VERSION = "sha256:0"


# =============================================================================
# Errors
# =============================================================================


@dataclass
class UserDataValidationError(Exception):
    """Raised when a write payload fails parse / schema / version / constraint checks.

    The backend's `awrite_text` surfaces the `message` verbatim to the agent's
    Write/Edit tool, so it must be self-explanatory and tell the agent how to recover.
    """

    error_type: str  # "parse_error" | "schema_error" | "version_conflict" | "constraint_error"
    file: str  # e.g. "portfolio.json"
    field_path: str  # e.g. "holdings[2].quantity"
    hint: str

    @property
    def message(self) -> str:
        base = f"{self.error_type}:{self.file}:{self.field_path}: {self.hint}"
        # Parse + schema failures usually mean the agent guessed at the shape.
        # Point it at the static schema doc so the next retry is informed.
        # Skip when the error itself concerns README.md (it's the wrong pointer
        # for someone trying to edit README).
        if self.error_type in {"parse_error", "schema_error"} and self.file != "README.md":
            base += " See .agents/user/profile/README.md for the full schema and examples."
        return base

    def __str__(self) -> str:
        return self.message


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
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise UserDataValidationError(
            error_type="parse_error",
            file=file,
            field_path=f"line {e.lineno} col {e.colno}",
            hint=f"invalid JSON: {e.msg}. Re-read the file and write a syntactically valid JSON object.",
        ) from None


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


# Stable salt so the advisory lock key namespace can't collide with any other
# pg_advisory_xact_lock callers that hash arbitrary user_ids.
_PROFILE_LOCK_KEY_PREFIX = "userdata:profile:"


async def _acquire_profile_lock(cur: Any, user_id: str) -> None:
    """Acquire a Postgres advisory tx-lock to serialize cross-process writes.

    Auto-released on COMMIT/ROLLBACK. Same key for all three files (portfolio,
    watchlist, preference) so an in-flight write of any one file blocks the
    others — appropriate because the agent's payload_version is per-file and
    interleaved cross-file writes can still produce inconsistent reads.
    """
    await cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (_PROFILE_LOCK_KEY_PREFIX + user_id,),
    )


@asynccontextmanager
async def _locked_version_check(
    user_id: str,
    *,
    file: str,
    payload_version: str,
    fetch_and_version: Callable[[Any, str], Awaitable[tuple[Any, str]]],
    conflict_message: str,
):
    """Open a transaction, take the advisory lock, recheck content hash.

    Centralises the CAS pattern shared by ``apply_portfolio_diff``,
    ``apply_watchlist_diff``, and ``apply_preferences``. The caller passes
    ``fetch_and_version(cur, user_id)`` which must return
    ``(current_data, current_version)`` using the same cursor so the read is
    transactionally consistent with the subsequent writes. Raises
    ``version_conflict`` on hash mismatch before yielding.

    Yields ``(cur, current_data)`` — the writer issues its INSERT/UPDATE/
    DELETE statements on ``cur`` and current_data is available for any
    post-lock business logic (e.g. reading existing ``other_preference``).
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await _acquire_profile_lock(cur, user_id)
                current_data, current_version = await fetch_and_version(cur, user_id)
                if payload_version != current_version:
                    raise UserDataValidationError(
                        "version_conflict", file, "__version__", conflict_message,
                    )
                yield cur, current_data


async def _fetch_portfolio_rows(cur: Any, user_id: str) -> list[dict[str, Any]]:
    """Inline portfolio fetch on the caller's cursor (transactional-consistent)."""
    async with cur.connection.cursor(row_factory=dict_row) as inner:
        await inner.execute(
            """
            SELECT
                user_portfolio_id, user_id, symbol, instrument_type, exchange,
                name, quantity, average_cost, currency, account_name,
                notes, metadata, first_purchased_at, created_at, updated_at
            FROM user_portfolios
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        return [dict(row) for row in await inner.fetchall()]


async def _fetch_watchlists_rows(
    cur: Any, user_id: str,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Inline watchlist + items fetch on the caller's connection.

    ORDER BY clauses match the existing module-level helpers
    (``watchlist_db.get_user_watchlists`` / ``get_all_user_watchlist_items``)
    so the pre-check hash and the in-transaction recheck hash agree on row order.
    """
    async with cur.connection.cursor(row_factory=dict_row) as inner:
        await inner.execute(
            """
            SELECT watchlist_id, user_id, name, description, is_default,
                   display_order, created_at, updated_at
            FROM watchlists
            WHERE user_id = %s
            ORDER BY is_default DESC, display_order ASC, created_at ASC
            """,
            (user_id,),
        )
        watchlists = [dict(row) for row in await inner.fetchall()]

        await inner.execute(
            """
            SELECT wi.watchlist_item_id, wi.watchlist_id, wi.user_id, wi.symbol,
                   wi.instrument_type, wi.exchange, wi.name, wi.notes,
                   wi.alert_settings, wi.metadata, wi.created_at, wi.updated_at
            FROM watchlist_items wi
            INNER JOIN watchlists w ON wi.watchlist_id = w.watchlist_id
            WHERE w.user_id = %s
            ORDER BY wi.created_at DESC
            """,
            (user_id,),
        )
        items_by_wl: dict[str, list[dict[str, Any]]] = {}
        for row in await inner.fetchall():
            items_by_wl.setdefault(str(row["watchlist_id"]), []).append(dict(row))
    return watchlists, items_by_wl


async def _fetch_preferences_row(cur: Any, user_id: str) -> dict[str, Any] | None:
    """Inline preferences fetch on the caller's connection."""
    async with cur.connection.cursor(row_factory=dict_row) as inner:
        await inner.execute(
            """
            SELECT user_preference_id, user_id, risk_preference, investment_preference,
                   agent_preference, other_preference, created_at, updated_at
            FROM user_preferences
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = await inner.fetchone()
        return dict(row) if row else None


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

    def is_empty(self) -> bool:
        return not self.inserts and not self.updates and not self.deletes


def parse_and_diff_portfolio(
    content: str,
    current_rows: list[dict[str, Any]],
) -> PortfolioDiff:
    """Parse agent JSON, validate, diff against ``current_rows``."""
    file = "portfolio.json"
    data = parse_json(content, file)

    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    holdings = data.get("holdings")
    if not isinstance(holdings, list):
        raise UserDataValidationError("schema_error", file, "holdings", "must be an array")

    # Identity is the natural unique key — the agent never sees DB UUIDs.
    by_unique_key: dict[tuple[str, str, str | None], dict[str, Any]] = {
        (r["symbol"], r["instrument_type"], r.get("account_name")): r for r in current_rows
    }

    diff = PortfolioDiff()
    seen_ids: set[str] = set()
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

        existing = by_unique_key.get((symbol, instrument_type, account_name))

        if existing is None:
            diff.inserts.append(normalized)
            continue

        seen_ids.add(str(existing["user_portfolio_id"]))
        # Only emit update if any field actually changed.
        if _portfolio_row_differs(existing, normalized):
            normalized["id"] = str(existing["user_portfolio_id"])
            diff.updates.append(normalized)

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


async def _portfolio_state(cur: Any, user_id: str) -> tuple[list[dict[str, Any]], str]:
    rows = await _fetch_portfolio_rows(cur, user_id)
    return rows, serialize_portfolio(rows)["__version__"]


async def apply_portfolio_diff(
    diff: PortfolioDiff,
    user_id: str,
    *,
    payload_version: str,
) -> None:
    """Apply inserts/updates/deletes in a single transaction.

    Race protection delegated to ``_locked_version_check``: advisory tx-lock
    keyed by ``(user_id, profile)`` plus a content-hash recheck. With the
    lock held, individual statements no longer need per-row CAS.
    """
    if diff.is_empty():
        return

    async with _locked_version_check(
        user_id,
        file="portfolio.json",
        payload_version=payload_version,
        fetch_and_version=_portfolio_state,
        conflict_message=(
            "portfolio changed between read and write. "
            "Re-read .agents/user/profile/portfolio.json and reapply your edit."
        ),
    ) as (cur, _current_rows):
        for row in diff.inserts:
            await cur.execute(
                """
                INSERT INTO user_portfolios (
                    user_portfolio_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    row.get("id") or str(uuid4()),
                    user_id, row["symbol"], row["instrument_type"],
                    row.get("exchange"), row.get("name"), row["quantity"],
                    row.get("average_cost"), row.get("currency") or "USD",
                    row.get("account_name"), row.get("notes"),
                    Json({}), row.get("first_purchased_at"),
                ),
            )

        for row in diff.updates:
            await cur.execute(
                """
                UPDATE user_portfolios SET
                    symbol = %s, instrument_type = %s, exchange = %s, name = %s,
                    quantity = %s, average_cost = %s, currency = %s,
                    account_name = %s, notes = %s, first_purchased_at = %s,
                    updated_at = NOW()
                WHERE user_portfolio_id = %s AND user_id = %s
                """,
                (
                    row["symbol"], row["instrument_type"], row.get("exchange"),
                    row.get("name"), row["quantity"], row.get("average_cost"),
                    row.get("currency") or "USD", row.get("account_name"),
                    row.get("notes"), row.get("first_purchased_at"),
                    row["id"], user_id,
                ),
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

    def is_empty(self) -> bool:
        return not any([
            self.wl_inserts, self.wl_updates, self.wl_deletes,
            self.item_inserts, self.item_updates, self.item_deletes,
        ])


def parse_and_diff_watchlist(
    content: str,
    current_watchlists: list[dict[str, Any]],
    current_items_by_wl: dict[str, list[dict[str, Any]]],
) -> WatchlistDiff:
    """Parse + diff watchlists and items against the current DB state."""
    file = "watchlist.json"
    data = parse_json(content, file)
    if not isinstance(data, dict):
        raise UserDataValidationError("schema_error", file, "", "root must be a JSON object")

    watchlists_payload = data.get("watchlists")
    if not isinstance(watchlists_payload, list):
        raise UserDataValidationError("schema_error", file, "watchlists", "must be an array")

    # Identity is the natural unique key — the agent never sees DB UUIDs.
    # Watchlist rename = recreate (delete old, insert new with fresh items),
    # since `name` is the only stable identity the agent has.
    wl_by_name: dict[str, dict[str, Any]] = {w["name"]: w for w in current_watchlists}

    diff = WatchlistDiff()
    seen_wl_ids: set[str] = set()
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

        # Items
        items_payload = wl.get("items", [])
        if not isinstance(items_payload, list):
            raise UserDataValidationError("schema_error", file, f"watchlists[{wl_idx}].items", "must be an array")

        seen_item_ids: set[str] = set()
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
                "watchlist_id": target_wl_id,
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

            existing_item = existing_items_by_key.get((symbol, instrument_type))
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


async def _watchlist_state(
    cur: Any, user_id: str,
) -> tuple[tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]], str]:
    wl, items = await _fetch_watchlists_rows(cur, user_id)
    return (wl, items), serialize_watchlist(wl, items)["__version__"]


async def apply_watchlist_diff(
    diff: WatchlistDiff,
    user_id: str,
    *,
    payload_version: str,
) -> None:
    """Apply watchlist + item diff in a single transaction.

    Race protection delegated to ``_locked_version_check`` (same pattern as
    ``apply_portfolio_diff``).
    """
    if diff.is_empty():
        return

    async with _locked_version_check(
        user_id,
        file="watchlist.json",
        payload_version=payload_version,
        fetch_and_version=_watchlist_state,
        conflict_message=(
            "watchlists changed between read and write. "
            "Re-read .agents/user/profile/watchlist.json and reapply your edit."
        ),
    ) as (cur, _current):
        # Item deletes first — explicit order even though watchlist
        # deletes would cascade through item rows.
        if diff.item_deletes:
            await cur.execute(
                "DELETE FROM watchlist_items WHERE user_id = %s "
                "AND watchlist_item_id = ANY(%s)",
                (user_id, diff.item_deletes),
            )

        # Watchlist inserts
        for wl in diff.wl_inserts:
            if wl.get("is_default"):
                await cur.execute(
                    "UPDATE watchlists SET is_default = FALSE, updated_at = NOW() "
                    "WHERE user_id = %s AND is_default = TRUE",
                    (user_id,),
                )
            await cur.execute(
                """
                INSERT INTO watchlists (
                    watchlist_id, user_id, name, description, is_default,
                    display_order, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 0, NOW(), NOW())
                """,
                (wl["id"], user_id, wl["name"], wl.get("description"), bool(wl.get("is_default"))),
            )

        # Watchlist updates
        for wl in diff.wl_updates:
            if wl.get("is_default"):
                await cur.execute(
                    "UPDATE watchlists SET is_default = FALSE, updated_at = NOW() "
                    "WHERE user_id = %s AND is_default = TRUE AND watchlist_id != %s",
                    (user_id, wl["id"]),
                )
            await cur.execute(
                """
                UPDATE watchlists
                SET name = %s, description = %s, is_default = %s, updated_at = NOW()
                WHERE watchlist_id = %s AND user_id = %s
                """,
                (
                    wl["name"], wl.get("description"), bool(wl.get("is_default")),
                    wl["id"], user_id,
                ),
            )

        # Item inserts (honor agent-provided id if any, else generate)
        for item in diff.item_inserts:
            await cur.execute(
                """
                INSERT INTO watchlist_items (
                    watchlist_item_id, watchlist_id, user_id, symbol, instrument_type,
                    exchange, name, notes, alert_settings, metadata,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    item.get("id") or str(uuid4()),
                    item["watchlist_id"], user_id, item["symbol"], item["instrument_type"],
                    item.get("exchange"), item.get("name"), item.get("notes"),
                    Json(item.get("alert_settings") or {}),
                    Json({}),
                ),
            )

        # Item updates
        for item in diff.item_updates:
            await cur.execute(
                """
                UPDATE watchlist_items SET
                    symbol = %s, instrument_type = %s, exchange = %s, name = %s,
                    notes = %s, alert_settings = %s, updated_at = NOW()
                WHERE watchlist_item_id = %s AND user_id = %s
                """,
                (
                    item["symbol"], item["instrument_type"], item.get("exchange"),
                    item.get("name"), item.get("notes"),
                    Json(item.get("alert_settings") or {}),
                    item["id"], user_id,
                ),
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
# Keys exposed to the agent in preference.json. `other_preference` is a
# server-managed JSONB column (onboarding state, internal flags) — keeping it
# out of the agent's view means the agent can't accidentally clobber it via
# replace-mode writes.
_AGENT_PREFERENCE_KEYS = ("risk_preference", "investment_preference", "agent_preference")
# `other_preference` is tolerated on input (silently dropped — see
# test_parse_silently_drops_other_preference) but treated as "known".
_PREFERENCE_ROOT_KEYS: frozenset[str] = frozenset(
    ("__version__", "other_preference", *_AGENT_PREFERENCE_KEYS)
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


async def _preferences_state(cur: Any, user_id: str) -> tuple[dict[str, Any] | None, str]:
    row = await _fetch_preferences_row(cur, user_id)
    return row, serialize_preferences(row)["__version__"]


async def apply_preferences(
    values: dict[str, Any], user_id: str, *, payload_version: str,
) -> None:
    """Replace-mode upsert of the agent-visible preference columns.

    Writes only ``risk_preference``, ``investment_preference``, ``agent_preference``.
    ``other_preference`` is intentionally left untouched — it's server-managed
    (onboarding state, internal flags) and not exposed to the agent. New users
    get ``other_preference = '{}'`` on first insert; existing rows keep theirs.
    """
    async with _locked_version_check(
        user_id,
        file="preference.json",
        payload_version=payload_version,
        fetch_and_version=_preferences_state,
        conflict_message=(
            "preferences changed between read and write. "
            "Re-read .agents/user/profile/preference.json and reapply your edit."
        ),
    ) as (cur, current):
        if current is not None:
            # Existing row: replace agent-visible cols, keep other_preference.
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
            # First insert — other_preference defaults to empty object.
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
    await user_db.invalidate_user_prefs_cache(user_id)
    logger.info("[user_data_io] applied preferences user_id=%s", user_id)
