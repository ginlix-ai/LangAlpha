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
from psycopg.types.json import Json

from src.server.database.conversation import get_db_connection

logger = logging.getLogger(__name__)


def make_chart_id(symbol: str, timeframe: str) -> str:
    """Disclosed instance key: ``{SYMBOL}:{timeframe}`` (uppercased ticker)."""
    return f"{symbol.strip().upper()}:{timeframe.strip()}"


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
    annotation_id = annotation["annotation_id"]
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO chart_annotations
                    (workspace_id, chart_id, symbol, timeframe, annotation_id, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (workspace_id, chart_id, annotation_id)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                """,
                (
                    workspace_id,
                    chart_id,
                    symbol.upper(),
                    timeframe,
                    annotation_id,
                    Json(annotation),
                ),
            )


async def list_annotations(workspace_id: str, chart_id: str) -> list[dict[str, Any]]:
    """Return every annotation for one chart instance, oldest first."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT payload
                FROM chart_annotations
                WHERE workspace_id = %s AND chart_id = %s
                ORDER BY created_at
                """,
                (workspace_id, chart_id),
            )
            rows = await cur.fetchall()
    return [row["payload"] for row in rows]


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
        params.append(symbol.upper())
    if timeframe:
        clauses.append("timeframe = %s")
        params.append(timeframe)
    where = " AND ".join(clauses)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT chart_id, symbol, timeframe, payload
                FROM chart_annotations
                WHERE {where}
                ORDER BY chart_id, created_at
                """,
                params,
            )
            rows = await cur.fetchall()

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
