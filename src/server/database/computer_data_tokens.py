"""What every worker knows of the market-data token each computer's sandbox
holds: when it runs out and which sandbox took it, never the token itself;
and of a revocation of its tokens the platform has yet to confirm."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_BINDABLE


@dataclass(frozen=True)
class DataTokenRow:
    #: When the last token saved runs out; None with none, or after a stop.
    expires_at: datetime | None = None
    #: The sandbox that took it: None until a write lands, since one can fail
    #: after its mint, and again once a revocation may have ended it.
    held_by: str | None = None
    #: What a save names to replace this row; 0 with no row.
    generation: int = 0


async def load(computer_id: str) -> DataTokenRow:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT expires_at, held_by, generation FROM computer_data_tokens"
                " WHERE computer_id = %s",
                (computer_id,),
            )
            row = await cur.fetchone()
    if row is None:
        return DataTokenRow()
    return DataTokenRow(row["expires_at"], row["held_by"], int(row["generation"]))


@dataclass(frozen=True)
class Saved:
    #: False when the computer is not its user's or is leaving service.
    serving: bool
    #: The row's new generation; None when it moved since it was read.
    generation: int | None


async def save(
    computer_id: str,
    user_id: str,
    sandbox_id: str,
    expires_at: datetime,
    generation: int,
) -> Saved:
    """Record a token expiring at ``expires_at``, held by no sandbox yet, if
    the row is still at ``generation`` and the computer still runs on
    ``sandbox_id``.

    The share lock on the computer orders the save against a stop, as the file
    mount's token does: one saved first is revoked after it, and one waiting
    on it finds the computer stopping. The generation fences out a save that
    read the row before a stop and lands after the computer started again, and
    the sandbox one that read it before a rebuild or a tier move: its token
    would go to a sandbox the computer has left and stay good there until it
    runs out.

    The row also keeps when the last token saved since the previous stop runs
    out, what the next stop owes a revocation until: a shorter configured
    lifetime puts the newest token's expiry before an older one's.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH serving AS (
                    SELECT computer_id, user_id FROM computers
                    WHERE computer_id = %(computer_id)s AND user_id = %(user_id)s
                      AND provider_ref = %(sandbox_id)s
                      AND {FENCE_BINDABLE}
                    FOR SHARE
                ),
                saved AS (
                    INSERT INTO computer_data_tokens AS t
                        (computer_id, user_id, expires_at, tokens_until, held_by,
                         generation, updated_at)
                    SELECT computer_id, user_id, %(expires_at)s, %(expires_at)s,
                           NULL, %(generation)s + 1, NOW()
                    FROM serving
                    ON CONFLICT (computer_id) DO UPDATE
                        SET expires_at = EXCLUDED.expires_at, held_by = NULL,
                            tokens_until = GREATEST(
                                t.tokens_until, t.expires_at, EXCLUDED.expires_at
                            ),
                            user_id = EXCLUDED.user_id,
                            generation = t.generation + 1, updated_at = NOW()
                        WHERE t.generation = %(generation)s
                    RETURNING generation
                )
                SELECT EXISTS (SELECT 1 FROM serving) AS serving,
                       (SELECT generation FROM saved) AS generation
                """,
                {
                    "computer_id": computer_id,
                    "user_id": user_id,
                    "sandbox_id": sandbox_id,
                    "expires_at": expires_at,
                    "generation": generation,
                },
            )
            row = await cur.fetchone()
    return Saved(bool(row["serving"]), row["generation"])


async def mark_held(computer_id: str, generation: int, sandbox_id: str | None) -> bool:
    """Record that ``sandbox_id`` took the token saved at ``generation``, and
    only that one."""
    async with get_db_connection() as conn:
        cur = await conn.execute(
            "UPDATE computer_data_tokens SET held_by = %s, updated_at = NOW()"
            " WHERE computer_id = %s AND generation = %s",
            (sandbox_id, computer_id, generation),
        )
        return cur.rowcount > 0


@dataclass(frozen=True)
class Revocation:
    """A revocation to send to the platform, and the row it settles."""

    computer_id: str
    #: Whose tokens to revoke, as recorded with what is owed: the platform keys
    #: a revocation by user and computer, and the computer may have changed
    #: hands since a token was minted.
    users: tuple[str, ...]
    #: The row's generation when it was read, before the request is sent:
    #: every tombstone up to it is covered by the platform's answer.
    generation: int


def _users(*user_ids: Any) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(u) for u in user_ids if u))


async def revoke(computer_id: str) -> Revocation | None:
    """Leave the row with no token and a new generation, inserting it if the
    computer has none, record the revocation as owed until the last token it
    covers runs out, and answer what to send the platform.

    Kept rather than deleted, so a mint in flight across the revoke finds the
    generation it read gone; nothing remains to revoke when the computer row
    itself is gone, since the cascade took this one with it. The owed one runs
    until the last token saved since the previous stop runs out, a bound it
    clears for the next stop, so a stop recorded while an earlier one is still
    owed keeps the later of the two, and the users of both.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO computer_data_tokens AS t
                    (computer_id, user_id, expires_at, held_by, generation,
                     revoke_generation, revoke_users, revoke_attempted_at,
                     updated_at)
                SELECT computer_id, user_id, NULL, NULL, 1, 1,
                       ARRAY[user_id]::text[], NOW(), NOW()
                FROM computers WHERE computer_id = %(computer_id)s
                ON CONFLICT (computer_id) DO UPDATE
                    SET expires_at = NULL, held_by = NULL, tokens_until = NULL,
                        generation = t.generation + 1,
                        revoke_until = GREATEST(
                            t.revoke_until, t.tokens_until, t.expires_at
                        ),
                        revoke_generation = t.generation + 1,
                        -- The row's user and the computer's, which differ
                        -- until a save after the computer changes hands.
                        revoke_users = ARRAY(
                            SELECT DISTINCT u FROM unnest(
                                CASE WHEN t.revoke_until > NOW()
                                     THEN t.revoke_users END
                                || ARRAY[t.user_id, EXCLUDED.user_id]::text[]
                            ) AS u
                        ),
                        -- This revoke sends it next, so the sweep leaves
                        -- it a retry interval before sending it again.
                        revoke_attempted_at = NOW(),
                        updated_at = NOW()
                RETURNING generation, revoke_users
                """,
                {"computer_id": computer_id},
            )
            row = await cur.fetchone()
    if row is None:
        return None
    return Revocation(
        computer_id, _users(*row["revoke_users"]), int(row["generation"])
    )


async def revoked(revocation: Revocation, users: tuple[str, ...]) -> bool:
    """Record that the platform confirmed ``revocation`` for ``users``, and
    answer whether the computer is in service, so its sandbox may have lost a
    live token.

    Only those users leave what is owed, which ends once none is left; what a
    stop recorded while the request was in flight owes stays whole. The
    platform refuses every token of theirs issued before it handled the
    request, which can include the one the sandbox holds, minted after the
    tombstone by a start that read it. So whatever the row records stops
    counting as held, and the generation moves: a push that read the row
    before this point minted a token that may be refused already, and its save
    or hold now misses. A push that reads the row after this point mints after
    the revocation, which spares its token. The expiries stay, the bound on
    what a later stop owes. What the sandbox is owed comes first in the
    sweep's next pass.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH owed AS (
                    SELECT computer_id,
                           CASE WHEN COALESCE(revoke_generation, 0) <= %(generation)s
                                THEN ARRAY(SELECT u FROM unnest(revoke_users) AS u
                                           WHERE u <> ALL(%(users)s::text[]))
                                ELSE revoke_users END AS users
                    FROM computer_data_tokens
                    WHERE computer_id = %(computer_id)s
                    FOR UPDATE
                ),
                confirmed AS (
                    UPDATE computer_data_tokens AS t
                    SET revoke_users = NULLIF(owed.users, '{}'),
                        revoke_until = CASE WHEN cardinality(owed.users) > 0
                                            THEN t.revoke_until END,
                        held_by = NULL, attempted_at = NULL,
                        generation = t.generation + 1, updated_at = NOW()
                    FROM owed
                    WHERE t.computer_id = owed.computer_id
                    RETURNING t.computer_id
                )
                SELECT EXISTS (
                    SELECT 1 FROM confirmed JOIN computers c USING (computer_id)
                    WHERE c.status IN ('starting', 'running')
                      AND c.provider_ref IS NOT NULL
                ) AS in_service
                """,
                {
                    "computer_id": revocation.computer_id,
                    "generation": revocation.generation,
                    "users": list(users),
                },
            )
            row = await cur.fetchone()
    return bool(row and row["in_service"])


async def claim_revocations(*, retry_after: timedelta, limit: int) -> list[Revocation]:
    """Take up to ``limit`` revocations still owed, the longest unsent first,
    marking each sent so that no worker takes it again for ``retry_after``.

    One whose last token has run out is no longer owed: the platform refuses
    every token it covers on expiry alone.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT computer_id FROM computer_data_tokens
                    WHERE revoke_until > NOW()
                      AND (revoke_attempted_at IS NULL
                           OR revoke_attempted_at < NOW() - %(retry_after)s)
                    ORDER BY revoke_attempted_at ASC NULLS FIRST, computer_id
                    LIMIT %(limit)s
                )
                UPDATE computer_data_tokens AS t
                SET revoke_attempted_at = NOW()
                FROM due
                WHERE t.computer_id = due.computer_id
                  -- Again on the locked row: a sibling's claim since the
                  -- listing has already taken it.
                  AND t.revoke_until > NOW()
                  AND (t.revoke_attempted_at IS NULL
                       OR t.revoke_attempted_at < NOW() - %(retry_after)s)
                RETURNING t.computer_id, t.generation, t.revoke_users
                """,
                {"retry_after": retry_after, "limit": limit},
            )
            rows = await cur.fetchall()
    return [
        Revocation(
            str(row["computer_id"]),
            _users(*row["revoke_users"]),
            int(row["generation"]),
        )
        for row in rows
    ]


async def load_in_service(computer_id: str) -> dict[str, Any] | None:
    """The computer as ``claim_due`` lists it, with its status, while it is
    starting or running on a sandbox; None otherwise."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT computer_id, user_id, provider_ref, kind, provider_config,
                       root_dir, is_always_on, status,
                       COALESCE(last_activity_at, updated_at) AS last_activity
                FROM computers
                WHERE computer_id = %s AND status IN ('starting', 'running')
                  AND provider_ref IS NOT NULL
                """,
                (computer_id,),
            )
            row = await cur.fetchone()
    return dict(row) if row else None


async def claim_due(
    *,
    refresh_below: timedelta,
    retry_after: timedelta,
    limit: int,
) -> list[dict[str, Any]]:
    """Take up to ``limit`` running computers whose sandbox holds no token with
    more than ``refresh_below`` left, marking each attempted so that no worker
    takes it again for ``retry_after``.

    Never-tried computers come first, then the longest untried. A computer
    whose sandbox keeps failing (not running, unreachable, or the mint refused)
    is marked like the rest, so it waits behind the others rather than taking
    the front of every pass, and is tried about once per ``retry_after``
    however many workers sweep. Each row carries what the idle reaper judges
    the computer by, always-on and when it was last used, since its idle limit
    is its provider's own.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT c.computer_id, c.user_id
                    FROM computers c
                    LEFT JOIN computer_data_tokens t
                        ON t.computer_id = c.computer_id
                    WHERE c.status = 'running' AND c.provider_ref IS NOT NULL
                      AND (t.expires_at IS NULL
                           OR t.expires_at < NOW() + %(refresh_below)s
                           OR t.held_by IS DISTINCT FROM c.provider_ref)
                      AND (t.attempted_at IS NULL
                           OR t.attempted_at < NOW() - %(retry_after)s)
                    ORDER BY t.attempted_at ASC NULLS FIRST,
                             t.expires_at ASC NULLS FIRST, c.computer_id
                    LIMIT %(limit)s
                ),
                claimed AS (
                    -- A computer with no row yet gets one at generation 0,
                    -- which is what a save that read no row names.
                    INSERT INTO computer_data_tokens AS t
                        (computer_id, user_id, generation, attempted_at)
                    SELECT computer_id, user_id, 0, NOW() FROM due
                    ON CONFLICT (computer_id) DO UPDATE SET attempted_at = NOW()
                        WHERE t.attempted_at IS NULL
                           OR t.attempted_at < NOW() - %(retry_after)s
                    RETURNING t.computer_id
                )
                SELECT c.computer_id, c.user_id, c.provider_ref, c.kind,
                       c.provider_config, c.root_dir, c.is_always_on,
                       COALESCE(c.last_activity_at, c.updated_at) AS last_activity
                FROM claimed JOIN computers c USING (computer_id)
                """,
                {
                    "refresh_below": refresh_below,
                    "retry_after": retry_after,
                    "limit": limit,
                },
            )
            return [dict(row) for row in await cur.fetchall()]
