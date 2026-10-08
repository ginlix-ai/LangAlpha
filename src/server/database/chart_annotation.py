"""Postgres store for agent-drawn chart annotations.

A chart instance is ``(workspace_id, chart_id)`` where
``chart_id = "{SYMBOL}:{timeframe}"`` — same symbol+timeframe edits the same
chart; a different ticker or timeframe is a new chart. One row per annotation;
``payload`` holds the full renderable annotation dict.

Durable (no TTL) and workspace-scoped: annotations cascade away only when the
workspace is deleted. Reached from the agent tool via the shared app pool.
"""

import logging
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.models.user import normalize_symbol
from src.server.utils.pg_sanitize import SafeJson, strip_pg_nul_str

logger = logging.getLogger(__name__)

# Safety valve for the read paths — far above any legitimate annotation count
# for one chart / one symbol. Caps an unbounded result set (and the resulting
# response size) if a runaway agent ever drew an absurd number; truncation is
# logged so it is never silent.
_MAX_ANNOTATION_ROWS = 2000

# Shared SQL so the single-write, single-read, and combined write+read paths
# never drift. ``add_and_list_annotations`` runs both on ONE pooled connection:
# autocommit makes the read see the just-upserted row, so a draw costs one
# checkout instead of two.
_UPSERT_SQL = """
    INSERT INTO chart_annotations
        (workspace_id, chart_id, symbol, timeframe, annotation_id, payload)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (workspace_id, chart_id, annotation_id)
    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
"""

# ``read_at_us`` dates the read in the same statement, so it orders snapshots:
# parallel draws commit in any order, and a set's size or members can't say
# which read came last once something else removed rows in between.
_CHART_SELECT_SQL = """
    SELECT payload,
           (extract(epoch FROM statement_timestamp()) * 1000000)::bigint AS read_at_us
    FROM chart_annotations
    WHERE workspace_id = %s AND chart_id = %s
    ORDER BY created_at
    LIMIT %s
"""


def _stored(symbol: str, annotation: dict[str, Any]) -> dict[str, Any]:
    """The payload as stored: it repeats the symbol, so in the column's spelling."""
    if "symbol" not in annotation:
        return annotation
    return {**annotation, "symbol": normalize_symbol(symbol)}


def _upsert_params(
    workspace_id: str,
    chart_id: str,
    symbol: str,
    timeframe: str,
    annotation: dict[str, Any],
) -> tuple[Any, ...]:
    """Bind tuple for ``_UPSERT_SQL`` — same NUL-stripping for every write path."""
    return (
        strip_pg_nul_str(workspace_id),
        strip_pg_nul_str(chart_id),
        strip_pg_nul_str(normalize_symbol(symbol)),
        strip_pg_nul_str(timeframe),
        strip_pg_nul_str(annotation["annotation_id"]),
        SafeJson(_stored(symbol, annotation)),
    )


def _warn_if_capped(row_count: int, workspace_id: str, chart_id: str) -> None:
    """Log (never silently) when a chart-scoped read hit the row cap."""
    if row_count >= _MAX_ANNOTATION_ROWS:
        logger.warning(
            "[chart_annotation] chart read hit the %d-row cap "
            "(workspace=%s chart=%s); some annotations were not returned",
            _MAX_ANNOTATION_ROWS,
            workspace_id,
            chart_id,
        )


def make_chart_id(symbol: str, timeframe: str) -> str:
    """Disclosed instance key: ``{SYMBOL}:{timeframe}``, the ticker in the one
    spelling rows store, so ``600519.SS`` and ``600519.SH`` are one chart."""
    return f"{normalize_symbol(symbol)}:{timeframe.strip()}"


async def add_annotation(
    workspace_id: str,
    chart_id: str,
    symbol: str,
    timeframe: str,
    annotation: dict[str, Any],
) -> None:
    """Upsert one annotation into a chart instance (idempotent on annotation_id).

    Raises on any DB failure so the caller can stay fail-closed.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _UPSERT_SQL,
                _upsert_params(workspace_id, chart_id, symbol, timeframe, annotation),
            )


async def list_annotations(workspace_id: str, chart_id: str) -> list[dict[str, Any]]:
    """Return every annotation for one chart instance, oldest first."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _CHART_SELECT_SQL,
                (workspace_id, chart_id, _MAX_ANNOTATION_ROWS),
            )
            rows = await cur.fetchall()
    _warn_if_capped(len(rows), workspace_id, chart_id)
    return [row["payload"] for row in rows]


async def add_and_list_annotations(
    workspace_id: str,
    chart_id: str,
    symbol: str,
    timeframe: str,
    annotation: dict[str, Any],
) -> tuple[list[dict[str, Any]], int | None]:
    """Upsert one annotation; return the instance's full set, oldest first,
    and when that set was read (epoch microseconds, None if not read back).

    Write and read share one pooled connection (autocommit, so the read sees the
    just-written row) — one checkout per draw instead of two. Raises on the write
    so the caller stays fail-closed; the read-back is best-effort and falls back
    to the just-written annotation so a transient read error can't mask a
    successful write.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _UPSERT_SQL,
                _upsert_params(workspace_id, chart_id, symbol, timeframe, annotation),
            )
        try:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    _CHART_SELECT_SQL,
                    (workspace_id, chart_id, _MAX_ANNOTATION_ROWS),
                )
                rows = await cur.fetchall()
        except Exception:
            logger.exception("[chart_annotation] read-back after upsert failed")
            return [_stored(symbol, annotation)], None
    _warn_if_capped(len(rows), workspace_id, chart_id)
    read_at_us = rows[0]["read_at_us"] if rows else None
    return [row["payload"] for row in rows], read_at_us


async def list_charts(
    workspace_id: str,
    symbol: str | None = None,
    timeframe: str | None = None,
) -> list[dict[str, Any]]:
    """Return chart instances for a workspace, optionally filtered.

    Each instance is ``{chart_id, symbol, timeframe, annotations: [...]}``,
    grouped by chart_id with annotations ordered oldest first.
    """
    clauses = ["workspace_id = %s"]
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
                f"""
                SELECT chart_id, symbol, timeframe, payload
                FROM chart_annotations
                WHERE {where}
                ORDER BY chart_id, created_at
                LIMIT %s
                """,
                [*params, _MAX_ANNOTATION_ROWS],
            )
            rows = await cur.fetchall()

    if len(rows) >= _MAX_ANNOTATION_ROWS:
        logger.warning(
            "[chart_annotation] list_charts hit the %d-row cap "
            "(workspace=%s symbol=%s); some annotations were not returned",
            _MAX_ANNOTATION_ROWS,
            workspace_id,
            symbol,
        )

    charts: dict[str, dict[str, Any]] = {}
    for row in rows:
        cid = row["chart_id"]
        chart = charts.get(cid)
        if chart is None:
            chart = {
                "chart_id": cid,
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "annotations": [],
            }
            charts[cid] = chart
        chart["annotations"].append(row["payload"])
    return list(charts.values())


async def remove_annotations(
    workspace_id: str,
    chart_id: str,
    ids: list[str],
) -> int:
    """Delete specific annotation ids from a chart instance. Returns count removed."""
    if not ids:
        return 0
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM chart_annotations
                WHERE workspace_id = %s AND chart_id = %s AND annotation_id = ANY(%s)
                """,
                (workspace_id, chart_id, list(ids)),
            )
            return cur.rowcount or 0


async def clear_chart(workspace_id: str, chart_id: str) -> int:
    """Delete every annotation in a chart instance. Returns count cleared."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM chart_annotations
                WHERE workspace_id = %s AND chart_id = %s
                """,
                (workspace_id, chart_id),
            )
            return cur.rowcount or 0
