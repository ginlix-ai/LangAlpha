"""Postgres store for chart layers.

A chart instance is ``(workspace_id, chart_id)`` where
``chart_id = "{SYMBOL}:{timeframe}"`` — same symbol+timeframe edits the same
chart; a different ticker or timeframe is a new chart. One row per layer
(``src/tools/chart_annotation/layers.py``): ``annotation_id`` is the layer's
own id, so republishing an id replaces it, and ``payload`` is the layer body,
which can run to megabytes. The table and its id column keep their first
names, from the per-drawing annotations it held before layers (migration 117
turned those into layers).

Durable (no TTL) and workspace-scoped: layers cascade away only when the
workspace is deleted. Reached from the agent's tools via the shared app pool.
"""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.models.chart_layer import LayerRef
from src.server.models.user import normalize_symbol
from src.server.utils.pg_sanitize import encoded_jsonb, strip_pg_nul_str

logger = logging.getLogger(__name__)

# Safety valves for the read paths, far above what one chart or symbol
# legitimately holds: a row cap, and a cap on the bytes of the layers read,
# since one layer may run to 1.5 MB. A read past either keeps the newest
# layers, the ones a user has just been shown appearing, and logs the rest.
_MAX_LAYER_ROWS = 2000
_MAX_LAYER_BYTES = 16 * 1024 * 1024

_UPSERT_SQL = """
    INSERT INTO chart_annotations
        (workspace_id, chart_id, symbol, timeframe, annotation_id, payload)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (workspace_id, chart_id, annotation_id)
    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
"""

# Only layer rows. A legacy annotation (``ann_<hex>``, a flat drawing; a
# layer's id has no ``_``) can still be written by a worker of the previous
# build during a rolling deploy, after migration 117 converted the rest: no
# read, delete or count may meet one, and a later run of 117's conversion
# turns it into a layer.
_LAYERS_ONLY = "NOT starts_with(annotation_id, 'ann_')"


def _stored(row: Mapping[str, Any]) -> dict[str, Any]:
    """A row as the REST list and the tool read it: its address, from the
    columns, and the layer."""
    ref = LayerRef(
        str(row["workspace_id"]), row["chart_id"], row["symbol"], row["timeframe"], row["annotation_id"]
    )
    return ref.stored(row["payload"])


def _capped_select(where: str, order: str) -> str:
    """The most recently published layer rows matching ``where`` (a
    republished layer counts by its latest publish) that fit both caps, in
    ``order``, each with ``matched``: how many the caps let in before the
    byte cap (the row cap's cut shows as ``matched`` reaching it).

    ``where`` is built from literal clause strings only; values are bound as
    the leading ``%s`` placeholders, then the row and byte caps."""
    return f"""
        SELECT workspace_id, chart_id, symbol, timeframe, annotation_id, payload, matched
        FROM (
            SELECT *, count(*) OVER () AS matched,
                   sum(octet_length(payload::text)) OVER (ORDER BY updated_at DESC, annotation_id) AS reach
            FROM (
                SELECT workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at, updated_at
                FROM chart_annotations WHERE {where}
                ORDER BY updated_at DESC, annotation_id LIMIT %s
            ) newest
        ) sized
        WHERE reach <= %s
        ORDER BY {order}
    """


def _warn_if_capped(rows: Sequence[Mapping[str, Any]], workspace_id: str, scope: str) -> None:
    """Log (never silently) when a read left layers out."""
    matched = rows[0]["matched"] if rows else 0
    if matched >= _MAX_LAYER_ROWS or len(rows) < matched:
        logger.warning(
            "[chart_layers] read capped (workspace=%s %s): %d layer(s) returned, the newest within "
            "%d rows and %d bytes; older ones were not returned",
            workspace_id,
            scope,
            len(rows),
            _MAX_LAYER_ROWS,
            _MAX_LAYER_BYTES,
        )


async def add_layers(layers: Sequence[tuple[LayerRef, Mapping[str, Any]]]) -> None:
    """Upsert layer rows, each ``(ref, layer body)``, in one transaction:
    all are stored or none is. Raises on any DB failure so the caller can
    stay fail-closed."""
    if not layers:
        return
    rows = await encode_layers(layers)
    async with get_db_connection() as conn:
        async with conn.transaction():
            await insert_layers(conn, rows)


async def encode_layers(layers: Sequence[tuple[LayerRef, Mapping[str, Any]]]) -> list[tuple[Any, ...]]:
    """The rows ``insert_layers`` writes, encoded in a worker thread: a run's
    layers can come to megabytes of JSON, which would stall the event loop."""
    return await asyncio.to_thread(lambda: [_row_params(ref, body) for ref, body in layers])


def _row_params(ref: LayerRef, body: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        strip_pg_nul_str(ref.workspace_id),
        strip_pg_nul_str(ref.chart_id),
        strip_pg_nul_str(ref.symbol),
        strip_pg_nul_str(ref.timeframe),
        strip_pg_nul_str(ref.layer_id),
        encoded_jsonb(body),
    )


async def insert_layers(conn, rows: Sequence[tuple[Any, ...]]) -> None:
    """Upsert rows ``encode_layers`` made on the caller's connection and
    transaction, for a write that must commit with another (a command's
    finalize)."""
    if not rows:
        return
    async with conn.cursor() as cur:
        await cur.executemany(_UPSERT_SQL, rows)


async def list_layers(workspace_id: str, chart_id: str) -> list[dict[str, Any]]:
    """Every layer on one chart instance, oldest first (``LayerRef.stored``)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _capped_select(f"workspace_id = %s AND chart_id = %s AND {_LAYERS_ONLY}", "created_at"),
                (workspace_id, chart_id, _MAX_LAYER_ROWS, _MAX_LAYER_BYTES),
            )
            rows = await cur.fetchall()
    _warn_if_capped(rows, workspace_id, f"chart={chart_id}")
    return [_stored(row) for row in rows]


async def list_charts(
    workspace_id: str,
    symbol: str | None = None,
    timeframe: str | None = None,
) -> list[dict[str, Any]]:
    """Return chart instances for a workspace, optionally filtered.

    Each instance is ``{chart_id, symbol, timeframe, layers: [...]}``, grouped
    by chart_id, its layers (``LayerRef.stored``) oldest first.
    """
    clauses = ["workspace_id = %s", _LAYERS_ONLY]
    params: list[Any] = [workspace_id]
    if symbol:
        clauses.append("symbol = %s")
        params.append(normalize_symbol(symbol))
    if timeframe:
        clauses.append("timeframe = %s")
        params.append(timeframe)
    where = " AND ".join(clauses)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # ``where`` is assembled only from the literal clause strings above;
            # every user-supplied value is a %s placeholder bound via ``params``,
            # so this f-string carries no SQL-injection surface.
            await cur.execute(
                _capped_select(where, "chart_id, created_at"),
                [*params, _MAX_LAYER_ROWS, _MAX_LAYER_BYTES],
            )
            rows = await cur.fetchall()
    _warn_if_capped(rows, workspace_id, f"symbol={symbol}")

    charts: dict[str, dict[str, Any]] = {}
    for row in rows:
        chart = charts.setdefault(
            row["chart_id"],
            {"chart_id": row["chart_id"], "symbol": row["symbol"], "timeframe": row["timeframe"], "layers": []},
        )
        chart["layers"].append(_stored(row))
    return list(charts.values())


async def remove_layers(workspace_id: str, chart_id: str, ids: Sequence[str]) -> list[str]:
    """Delete layers by id from a chart instance. Returns the ids removed, so
    the caller can name the ones that matched nothing."""
    if not ids:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                DELETE FROM chart_annotations
                WHERE workspace_id = %s AND chart_id = %s AND annotation_id = ANY(%s)
                  AND {_LAYERS_ONLY}
                RETURNING annotation_id
                """,
                (workspace_id, chart_id, list(ids)),
            )
            return [row[0] for row in await cur.fetchall()]


async def clear_chart(workspace_id: str, chart_id: str) -> int:
    """Delete every layer in a chart instance. Returns count cleared."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                DELETE FROM chart_annotations
                WHERE workspace_id = %s AND chart_id = %s AND {_LAYERS_ONLY}
                """,
                (workspace_id, chart_id),
            )
            return cur.rowcount or 0
