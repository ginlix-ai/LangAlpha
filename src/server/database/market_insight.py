"""Database operations for market insights."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

CARD_COLUMNS = (
    "market_insight_id::text, type, headline, summary, topics, model, created_at, completed_at"
)
ALL_COLUMNS = (
    "market_insight_id::text, user_id, type, status, headline, summary, content, "
    "topics, sources, model, error_message, generation_time_ms, metadata, "
    "created_at, completed_at"
)


@dataclass(frozen=True)
class SystemInsightClaim:
    """A claim's outcome: the generating row this worker now owns, or the id
    of the row that holds the type and whether it is still being generated."""

    row: Optional[dict] = None
    holder_id: Optional[str] = None
    holder_in_flight: bool = False


async def claim_system_insight(
    type: str,
    *,
    fresh_within: timedelta,
    in_flight_within: timedelta,
    model: str,
    metadata: Optional[dict] = None,
) -> SystemInsightClaim:
    """Insert a generating system insight of ``type``, unless one completed
    within ``fresh_within`` or one is still being generated.

    Every backend worker runs the schedule and wakes on the same instant, so
    the check and the insert share one transaction under a per-type advisory
    lock: the first claims the slot, and the rest are told who holds it. A
    generating row holds the type only for ``in_flight_within``, so a worker
    that died mid-generation does not block the next claim for long; a
    failed row never holds it.
    """
    insight_id = str(uuid4())
    now = datetime.now(timezone.utc)
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"market_insight:{type}",),
                )
                await cur.execute(
                    """
                    SELECT market_insight_id::text, status FROM market_insights
                    WHERE user_id IS NULL AND type = %s
                      AND ((status = 'completed' AND completed_at > %s)
                           OR (status = 'generating' AND created_at > %s))
                    ORDER BY status = 'completed' DESC
                    LIMIT 1
                    """,
                    (type, now - fresh_within, now - in_flight_within),
                )
                holder = await cur.fetchone()
                if holder:
                    return SystemInsightClaim(
                        holder_id=holder["market_insight_id"],
                        holder_in_flight=holder["status"] == "generating",
                    )
                await cur.execute(
                    """
                    INSERT INTO market_insights
                        (market_insight_id, user_id, type, status, model, metadata, created_at)
                    VALUES (%s, NULL, %s, 'generating', %s, %s, %s)
                    RETURNING market_insight_id::text, user_id, type, status, model, metadata, created_at
                    """,
                    (insight_id, type, model, Json(metadata), now),
                )
                return SystemInsightClaim(row=dict(await cur.fetchone()))


async def create_market_insight_if_not_generating(
    model: str,
    type: str = "daily_brief",
    user_id: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> Optional[dict]:
    """Atomically insert a generating insight, respecting the partial unique index.

    Returns the new row, or None if a generating row already exists for this user
    (ON CONFLICT from idx_market_insights_user_generating).
    """
    insight_id = str(uuid4())
    now = datetime.now(timezone.utc)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO market_insights
                    (market_insight_id, user_id, type, status, model, metadata, created_at)
                VALUES (%s, %s, %s, 'generating', %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING market_insight_id::text, user_id, type, status, model, metadata, created_at
                """,
                (insight_id, user_id, type, model, Json(metadata), now),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def complete_market_insight(
    market_insight_id: str,
    headline: str,
    summary: str,
    content: list,
    topics: list,
    sources: list,
    generation_time_ms: int,
) -> bool:
    """Mark an insight as completed with generated content.

    Only transitions from 'generating' → 'completed' (atomic).
    Returns True if the row was updated, False if it was already completed/failed.
    """
    now = datetime.now(timezone.utc)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE market_insights
                SET status = 'completed',
                    headline = %s,
                    summary = %s,
                    content = %s,
                    topics = %s,
                    sources = %s,
                    generation_time_ms = %s,
                    completed_at = %s
                WHERE market_insight_id = %s
                  AND status = 'generating'
                """,
                (
                    headline,
                    summary,
                    Json(content),
                    Json(topics),
                    Json(sources),
                    generation_time_ms,
                    now,
                    market_insight_id,
                ),
            )
            return cur.rowcount > 0


async def fail_market_insight(market_insight_id: str, error_message: str) -> None:
    """Mark an insight as failed. Only transitions from 'generating'."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE market_insights
                SET status = 'failed', error_message = %s
                WHERE market_insight_id = %s
                  AND status = 'generating'
                """,
                (error_message, market_insight_id),
            )


async def get_market_insight(market_insight_id: str) -> Optional[dict]:
    """Get a single insight by ID (all columns)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ALL_COLUMNS}
                FROM market_insights
                WHERE market_insight_id = %s
                """,
                (market_insight_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_todays_market_insights(
    user_id: Optional[str] = None,
) -> list[dict]:
    """Get all completed insights for today (America/New_York).

    Returns card columns only, ordered newest first.
    When user_id is provided, returns UNION ALL of system insights (user_id IS NULL)
    and user's personal insights, each hitting its own partial index.
    System insights fall back to yesterday if none today; user insights do not.
    """
    et = ZoneInfo("America/New_York")
    today = datetime.now(et).date()
    day_start = datetime.combine(today, datetime.min.time(), tzinfo=et).astimezone(
        timezone.utc
    )
    day_end = datetime.combine(
        today + timedelta(days=1), datetime.min.time(), tzinfo=et
    ).astimezone(timezone.utc)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Query 1: system insights (always)
            await cur.execute(
                f"""
                SELECT {CARD_COLUMNS}
                FROM market_insights
                WHERE status = 'completed'
                  AND created_at >= %s AND created_at < %s
                  AND user_id IS NULL
                ORDER BY created_at DESC
                """,
                (day_start, day_end),
            )
            system_rows = [dict(r) for r in await cur.fetchall()]

            # System fallback: yesterday's most recent if none today
            if not system_rows:
                yesterday_start = datetime.combine(
                    today - timedelta(days=1), datetime.min.time(), tzinfo=et
                ).astimezone(timezone.utc)
                await cur.execute(
                    f"""
                    SELECT {CARD_COLUMNS}
                    FROM market_insights
                    WHERE status = 'completed'
                      AND created_at >= %s AND created_at < %s
                      AND user_id IS NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (yesterday_start, day_start),
                )
                fallback = await cur.fetchone()
                if fallback:
                    system_rows = [dict(fallback)]

            # Query 2: user insights (only if user_id provided)
            user_rows: list[dict] = []
            if user_id is not None:
                await cur.execute(
                    f"""
                    SELECT {CARD_COLUMNS}
                    FROM market_insights
                    WHERE status = 'completed'
                      AND created_at >= %s AND created_at < %s
                      AND user_id = %s
                    ORDER BY created_at DESC
                    """,
                    (day_start, day_end, user_id),
                )
                user_rows = [dict(r) for r in await cur.fetchall()]

            # Merge and sort by created_at DESC
            merged = system_rows + user_rows
            merged.sort(key=lambda r: r["created_at"], reverse=True)
            return merged


async def get_user_generating_insight(user_id: str) -> Optional[dict]:
    """Get an in-progress insight for the user (idempotency check)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ALL_COLUMNS}
                FROM market_insights
                WHERE user_id = %s AND status = 'generating'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_user_recent_completed_insight(
    user_id: str, within_minutes: int = 5
) -> Optional[dict]:
    """Get a recently completed personalized insight for dedup."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=within_minutes)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ALL_COLUMNS}
                FROM market_insights
                WHERE user_id = %s
                  AND status = 'completed'
                  AND type = 'personalized'
                  AND completed_at >= %s
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (user_id, cutoff),
            )
            row = await cur.fetchone()
            return dict(row) if row else None
