"""Database access for SSH tunnels and the gateway's keys (migration 084).

The gateway is the one writer of a tunnel's liveness; the server's workers
read it, and create and delete the rows. A tunnel counts as live while its
latest connect is newer than its latest disconnect and the gateway has seen
it within ``LIVE_WITHIN``: a gateway that died without writing the disconnect
stops refreshing ``seen_at``, so its tunnels lapse on their own.

A tunnel that is not live is only judged down while the gateway itself is
settled (``ssh_gateway_state``, migration 129): it beat within
``LIVE_WITHIN`` and started at least that long ago. A gateway that died, or
just restarted and is still taking its machines back, is the service's
outage, never every user's machine gone at once.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from psycopg.rows import dict_row

from src.server.database.encryption import get_encryption_key
from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_NOT_DELETED, advisory_key

# How often the gateway refreshes ``seen_at``; three missed refreshes and a
# tunnel no longer counts as connected.
HEARTBEAT = timedelta(minutes=1)
LIVE_WITHIN = 3 * HEARTBEAT

# Held by the running gateway for its whole life: it alone writes liveness,
# and a second one would mark the first one's tunnels down at its start.
_GATEWAY_LOCK = advisory_key("ssh_gateway", "singleton")

# How long after stamping ``swept_at`` a take-off may still be running the
# machine's uninstall: twice the longest it runs. No pairing renews the tunnel
# meanwhile. A claim stamps it too, so each claims only a tunnel it takes off,
# or forgets, at once.
TAKE_OFF_WINDOW = timedelta(seconds=30)

# Far more machines than anyone connects out, so only a pairing token used to
# mint tunnels in a loop reaches it.
MAX_TUNNELS_PER_USER = 50

# Literals rather than parameters, so every statement that judges a tunnel
# can include them as they are.
_WITHIN_LIVE = f"make_interval(secs => {LIVE_WITHIN.total_seconds()})"

# How long a machine may go unseen before work waiting on it gives up: a
# notification parked for it, and a command left running there.
AWAY_GIVE_UP_DAYS = 7

_LIVE = (
    "(connected_at IS NOT NULL "
    " AND (disconnected_at IS NULL OR connected_at > disconnected_at) "
    f" AND seen_at > NOW() - {_WITHIN_LIVE})"
)
# Down since a newer pairing of its machine superseded it (migration 124). A
# connect after that stamp, as from a folder restored with the old keys, is
# the machine back on this tunnel, so its next drop reads as any other.
_PAIRED_ELSEWHERE = (
    "(superseded_at IS NOT NULL"
    " AND (connected_at IS NULL OR superseded_at > connected_at)"
    f" AND NOT {_LIVE})"
)
# The gateway has beaten within ``LIVE_WITHIN`` and has run that long, the
# time its machines' keepalives take to give up on a connection the previous
# gateway held and connect again.
_GATEWAY_SETTLED = (
    "EXISTS (SELECT 1 FROM ssh_gateway_state g"
    f" WHERE g.seen_at > NOW() - {_WITHIN_LIVE}"
    f" AND g.started_at <= NOW() - {_WITHIN_LIVE})"
)
# A gateway beating but not settled: it has just started, and its machines
# are still connecting back to it.
_GATEWAY_STARTING = (
    "EXISTS (SELECT 1 FROM ssh_gateway_state g"
    f" WHERE g.seen_at > NOW() - {_WITHIN_LIVE}"
    f" AND g.started_at > NOW() - {_WITHIN_LIVE})"
)
# A row's ``state``: superseded, then not judged while the gateway is not
# settled (a row a previous gateway saw live is not live on this one yet, so a
# dial to it fails as this gateway's start), then live, else down. ``live``
# stays a column of its own: whether the machine was last seen connected,
# settled or not.
_COLUMNS = (
    "tunnel_id::text AS tunnel_id, user_id, public_key, fingerprint, hostname, "
    "username, host_key, port, created_at, paired_at, "
    f"connected_at, disconnected_at, seen_at, {_LIVE} AS live, "
    f"CASE WHEN {_PAIRED_ELSEWHERE} THEN 'paired_elsewhere' "
    f"WHEN NOT {_GATEWAY_SETTLED} THEN 'unsettled' "
    f"WHEN {_LIVE} THEN 'live' ELSE 'down' END AS state"
)

# No computer that is not deleted names the tunnel ``t``, whoever owns it: a
# computer moved to another user still rides the tunnel it was made on.
_UNNAMED = (
    "NOT EXISTS (SELECT 1 FROM computers c"
    " WHERE c.provider_config->>'tunnel' = t.tunnel_id::text"
    f" AND c.{FENCE_NOT_DELETED})"
)
# No take-off of the tunnel ``ssh_tunnels`` may be running its uninstall.
_NOT_TAKING_OFF = (
    "(ssh_tunnels.swept_at IS NULL"
    " OR ssh_tunnels.swept_at < NOW() - %(take_off_window)s)"
)


def _tunneled_sql(computer_id: str, tunnel_rows: str = "TRUE") -> str:
    """SQL true while the computer ``computer_id`` (an SQL text expression)
    is not deleted and connects out through a tunnel, and ``tunnel_rows``
    (SQL over its ``computers c`` row) holds. A computer reached directly or
    deleted never matches."""
    return f"""EXISTS (
        SELECT 1 FROM computers c
        WHERE c.computer_id = ({computer_id})::uuid
          AND c.kind = 'ssh' AND c.{FENCE_NOT_DELETED}
          AND COALESCE(c.provider_config->>'tunnel', '') <> ''
          AND {tunnel_rows})"""


def _no_tunnel_row_sql(tunnel_ok: str) -> str:
    """SQL over a ``computers c`` row: no row of its tunnel that ``tunnel_ok``
    holds for."""
    return f"""NOT EXISTS (
              SELECT 1 FROM ssh_tunnels
              WHERE ssh_tunnels.tunnel_id::text = c.provider_config->>'tunnel'
                AND {tunnel_ok})"""


def _tunnel_judged_sql(computer_id: str, tunnel_ok: str) -> str:
    """SQL true while the gateway is settled and the computer ``computer_id``
    (an SQL text expression) connects out through a tunnel with no row that
    ``tunnel_ok`` holds for. None matches while the gateway is not settled
    (``_GATEWAY_SETTLED``): its dials then fail as the gateway's outage."""
    return f"({_GATEWAY_SETTLED} AND {_tunneled_sql(computer_id, _no_tunnel_row_sql(tunnel_ok))})"


# Not down: live, or superseded by a newer pairing of its machine, which is
# not a machine that will answer later but one its owner must reconnect, so
# a dial says that rather than work waiting for it.
_NOT_DOWN = f"({_LIVE} OR {_PAIRED_ELSEWHERE})"


def computer_tunnel_down_sql(computer_id: str) -> str:
    """SQL true while the computer ``computer_id`` (an SQL text expression)
    connects out through a tunnel that is not live, for a statement every
    worker may run, and the one judgment of it (:func:`computer_tunnel_down`
    evaluates it alone). A tunnel its machine was paired again past is not
    down (``_NOT_DOWN``)."""
    return _tunnel_judged_sql(computer_id, _NOT_DOWN)


async def gateway_settled(*, conn=None) -> bool:
    """Whether the gateway has run long enough to judge its tunnels
    (``_GATEWAY_SETTLED``), on ``conn`` where a caller passes one."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"SELECT {_GATEWAY_SETTLED} AS settled")
            row = await cur.fetchone()
            return bool(row and row["settled"])


async def computer_tunnel_down(computer_id: str, *, conn=None) -> bool:
    """Whether the computer connects out through a tunnel that is not live
    (:func:`computer_tunnel_down_sql`), on ``conn`` where a caller passes one."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {computer_tunnel_down_sql('%(computer_id)s')} AS down",
                {"computer_id": str(computer_id)},
            )
            row = await cur.fetchone()
            return bool(row and row["down"])


def computer_tunnel_lost_sql(computer_id: str) -> str:
    """SQL true while the computer ``computer_id`` (an SQL text expression)
    connects out through a tunnel the gateway has not seen for
    ``AWAY_GIVE_UP_DAYS``, or that is gone: a machine that has not come back
    in that long, which work waiting on it gives up on."""
    return _tunnel_judged_sql(
        computer_id,
        "COALESCE(ssh_tunnels.seen_at, ssh_tunnels.created_at)"
        f" > NOW() - make_interval(days => {AWAY_GIVE_UP_DAYS})",
    )


def awaited_computer_back_sql(awaits: str, workspace_id: str) -> str:
    """SQL true once work waiting for the computer ``awaits`` (an SQL text
    expression) may look again: it no longer connects through a tunnel, its
    tunnel is not down, or the workspace ``workspace_id`` (an SQL
    expression) its turn runs in is on another computer now (moved, or its
    computer removed). Its start then looks at the computer it is on now,
    and waits again naming that one if it is away too. Not while a gateway
    that just started settles: its machines are still connecting back, and
    work let go would dial one that may be asleep and be skipped for it. A
    gateway that stopped beating judges no tunnel down, so work is let go
    and fails as the service's outage."""
    return f"""(NOT {_tunneled_sql(awaits)}
        OR (NOT {_GATEWAY_STARTING} AND NOT {computer_tunnel_down_sql(awaits)})
        OR EXISTS (
        SELECT 1 FROM workspaces ws
        WHERE ws.workspace_id = ({workspace_id})
          AND ws.computer_id IS NOT NULL
          AND ws.computer_id::text IS DISTINCT FROM ({awaits})))"""


class TunnelKeyTaken(Exception):
    """Another user's tunnel has this key."""


class TooManyTunnels(Exception):
    """The user holds ``MAX_TUNNELS_PER_USER`` tunnels, none with this key."""


async def upsert_tunnel(
    user_id: str, *, public_key: str, fingerprint: str, hostname: str
) -> Optional[Dict[str, Any]]:
    """The user's tunnel for this key, made on first ask; None while it is
    being taken off its machine (:func:`renew_tunnel`).

    Pairing a machine again sends the key its tunnel already has, which finds
    the same row, so the computers already on it keep working.

    Raises:
        TunnelKeyTaken: One machine login paired from two accounts keeps one
            key, and a second tunnel for it would take the machine's service
            from the first account's computers.
        TooManyTunnels: The user holds ``MAX_TUNNELS_PER_USER`` already.
    """
    params = dict(
        user_id=user_id,
        public_key=public_key,
        fingerprint=fingerprint,
        hostname=hostname,
        take_off_window=TAKE_OFF_WINDOW,
    )
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            # The count holds against the user's other pairings at once.
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("ssh_tunnels", user_id),),
            )
            await cur.execute(
                """
                SELECT bool_or(user_id <> %(user_id)s) AS theirs,
                       bool_or(user_id = %(user_id)s) AS ours,
                       (SELECT count(*) FROM ssh_tunnels
                        WHERE user_id = %(user_id)s) AS held
                FROM ssh_tunnels WHERE fingerprint = %(fingerprint)s
                """,
                params,
            )
            found = await cur.fetchone()
            assert found is not None
            if found["theirs"]:
                raise TunnelKeyTaken(fingerprint)
            if not found["ours"] and found["held"] >= MAX_TUNNELS_PER_USER:
                raise TooManyTunnels(user_id)
            await cur.execute(
                f"""
                INSERT INTO ssh_tunnels (user_id, public_key, fingerprint, hostname)
                VALUES (%(user_id)s, %(public_key)s, %(fingerprint)s, %(hostname)s)
                ON CONFLICT (fingerprint)
                DO UPDATE SET hostname = EXCLUDED.hostname, paired_at = NOW()
                WHERE ssh_tunnels.user_id = EXCLUDED.user_id AND {_NOT_TAKING_OFF}
                RETURNING {_COLUMNS}
                """,
                params,
            )
            row = await cur.fetchone()
            if row is None:
                # Another account's insert can land after the read above.
                await cur.execute(
                    "SELECT 1 FROM ssh_tunnels WHERE fingerprint = %(fingerprint)s"
                    " AND user_id <> %(user_id)s",
                    params,
                )
                if await cur.fetchone() is not None:
                    raise TunnelKeyTaken(fingerprint)
            return row


async def renew_tunnel(user_id: str, tunnel_id: str) -> Optional[bool]:
    """Mark the user's tunnel paired again, for a pairing or a create using it.

    A take-off stamps the row before it runs the machine's uninstall, only if
    the row was not paired again since its claim (:func:`begin_take_off`).
    Either this lands first and the take-off leaves the machine alone, or the
    stamp does and this refuses while the uninstall may run. True when
    renewed, False while a take-off is running, None when the user has no
    such tunnel.
    """
    params = {
        "tunnel_id": tunnel_id,
        "user_id": user_id,
        "take_off_window": TAKE_OFF_WINDOW,
    }
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE ssh_tunnels SET paired_at = NOW()
                WHERE tunnel_id = %(tunnel_id)s::uuid AND user_id = %(user_id)s
                  AND {_NOT_TAKING_OFF}
                RETURNING 1
                """,
                params,
            )
            if await cur.fetchone() is not None:
                return True
            await cur.execute(
                "SELECT 1 FROM ssh_tunnels "
                "WHERE tunnel_id = %(tunnel_id)s::uuid AND user_id = %(user_id)s",
                params,
            )
            return False if await cur.fetchone() is not None else None


async def get_tunnel(tunnel_id: str) -> Optional[Dict[str, Any]]:
    """Any user's tunnel by id, for the gateway's login check."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM ssh_tunnels "
                "WHERE tunnel_id = %(tunnel_id)s::uuid",
                dict(tunnel_id=tunnel_id),
            )
            return await cur.fetchone()


async def get_user_tunnel(user_id: str, tunnel_id: str) -> Optional[Dict[str, Any]]:
    """The tunnel if it is this user's; None for anyone else's or none at all."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM ssh_tunnels "
                "WHERE tunnel_id = %(tunnel_id)s::uuid AND user_id = %(user_id)s",
                dict(tunnel_id=tunnel_id, user_id=user_id),
            )
            return await cur.fetchone()


async def get_tunnels(tunnel_ids: Iterable[str], *, conn=None) -> Dict[str, Dict[str, Any]]:
    """The tunnels a listing's computers name, by id, whoever owns each, on
    ``conn`` where a caller holding locks on it passes one."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM ssh_tunnels "
                "WHERE tunnel_id = ANY(%(tunnel_ids)s::uuid[])",
                dict(tunnel_ids=list(tunnel_ids)),
            )
            return {row["tunnel_id"]: row for row in await cur.fetchall()}


async def set_tunnel_login(
    tunnel_id: str, *, username: str, host_key: str, port: int
) -> None:
    """Keep the login a pairing reached the machine with, for its cleanup."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE ssh_tunnels SET username = %s, host_key = %s, port = %s "
                "WHERE tunnel_id = %s::uuid",
                (username, host_key, port, tunnel_id),
            )


async def claim_unused_tunnels(
    *,
    paired_before: timedelta,
    retry_after: timedelta,
    give_up_after: timedelta,
    limit: int,
) -> List[Dict[str, Any]]:
    """Take up to ``limit`` tunnels no computer names, paired ``paired_before`` ago.

    Only those the sweep acts on: with no login to reach the machine, live, or
    away for ``give_up_after``. One away for less is left unmarked, since the
    mark refuses a pairing while it is fresh. Each taken is marked tried, so
    that no worker takes it again for ``retry_after``: a machine that does not
    answer is tried about once per ``retry_after`` however many workers sweep.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH unused AS (
                    SELECT t.tunnel_id AS id FROM ssh_tunnels t
                    WHERE t.paired_at < NOW() - %(paired_before)s
                      AND (t.swept_at IS NULL
                           OR t.swept_at < NOW() - %(retry_after)s)
                      AND {_UNNAMED}
                      AND (t.username = '' OR t.host_key = ''
                           OR {_LIVE}
                           OR COALESCE(t.seen_at, t.paired_at)
                              < NOW() - %(give_up_after)s)
                    ORDER BY t.swept_at ASC NULLS FIRST, t.paired_at
                    LIMIT %(limit)s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ssh_tunnels SET swept_at = NOW()
                FROM unused WHERE tunnel_id = unused.id
                RETURNING {_COLUMNS}
                """,
                dict(
                    paired_before=paired_before,
                    retry_after=retry_after,
                    give_up_after=give_up_after,
                    limit=limit,
                ),
            )
            return list(await cur.fetchall())


async def claim_released_tunnel(
    tunnel_id: str,
    *,
    retry_after: timedelta,
    made_at: Optional[datetime],
    paired_within: timedelta,
) -> Optional[Dict[str, Any]]:
    """Take the tunnel for the release of a computer made at ``made_at``.

    Marked tried as the sweep marks it, so of two computers removed together,
    or a removal and a sweep, one runs the machine's uninstall. None when a
    computer names it, it was tried within ``retry_after``, or it was paired
    again since ``made_at`` and within ``paired_within``: that pairing is for
    a computer about to be made on the same machine, which needs the service,
    and the sweep takes the tunnel if none is.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE ssh_tunnels t SET swept_at = NOW()
                WHERE t.tunnel_id = %(tunnel_id)s::uuid
                  AND (t.swept_at IS NULL OR t.swept_at < NOW() - %(retry_after)s)
                  AND {_UNNAMED}
                  AND (%(made_at)s::timestamptz IS NULL
                       OR NOT (t.paired_at > %(made_at)s::timestamptz
                               AND t.paired_at > NOW() - %(paired_within)s))
                RETURNING {_COLUMNS}
                """,
                dict(
                    tunnel_id=tunnel_id,
                    retry_after=retry_after,
                    made_at=made_at,
                    paired_within=paired_within,
                ),
            )
            return await cur.fetchone()


async def taken_off_elsewhere(tunnel_id: str, *, within: timedelta) -> bool:
    """Whether a release or the sweep claimed the unused tunnel within ``within``.

    That claimant may still log in to the machine anew, with the key line a
    removal would otherwise take off, and a sweep that finds the login refused
    forgets the tunnel while its service stays on the machine.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT 1 FROM ssh_tunnels t
                WHERE t.tunnel_id = %(tunnel_id)s::uuid
                  AND t.swept_at >= NOW() - %(within)s
                  AND {_UNNAMED}
                """,
                {"tunnel_id": tunnel_id, "within": within},
            )
            return await cur.fetchone() is not None


async def begin_take_off(tunnel_id: str, *, paired_at: datetime) -> bool:
    """Stamp the tunnel as being taken off its machine, if still as claimed.

    ``paired_at`` is the caller's claim. False when the tunnel was paired
    again, or given a computer, since then: its machine's service is wanted.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE ssh_tunnels t SET swept_at = NOW()
                WHERE t.tunnel_id = %(tunnel_id)s::uuid
                  AND t.paired_at = %(paired_at)s
                  AND {_UNNAMED}
                """,
                {"tunnel_id": tunnel_id, "paired_at": paired_at},
            )
            return cur.rowcount > 0


async def delete_unused_tunnel(tunnel_id: str, *, paired_at: datetime) -> bool:
    """Forget the tunnel unless a computer names it.

    ``paired_at`` is the caller's claim: a tunnel paired again since then is
    being set up, and is left for a computer to be made on it.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                DELETE FROM ssh_tunnels t
                WHERE t.tunnel_id = %(tunnel_id)s::uuid
                  AND t.paired_at = %(paired_at)s
                  AND {_UNNAMED}
                """,
                {"tunnel_id": tunnel_id, "paired_at": paired_at},
            )
            return cur.rowcount > 0


# -- Liveness, written by the gateway alone ------------------------------------


async def try_gateway_lock(conn: Any) -> bool:
    """Take the gateway's lock on *conn*'s session; False when another has it.

    A session lock, so it lasts until *conn* closes, however the process ends.
    """
    async with conn.cursor() as cur:
        await cur.execute("SELECT pg_try_advisory_lock(%s)", (_GATEWAY_LOCK,))
        row = await cur.fetchone()
    return bool(row and row[0])


async def check_gateway_lock(conn: Any) -> None:
    """Raise when *conn*'s session has ended, and the gateway's lock with it.

    Nothing else releases a session lock, so the session answering means the
    lock is still held.
    """
    async with conn.cursor() as cur:
        await cur.execute("SELECT 1")


async def mark_connected(tunnel_id: str) -> bool:
    """The machine's forward is up. False when the row is gone."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE ssh_tunnels SET connected_at = NOW(), seen_at = NOW() "
                "WHERE tunnel_id = %s::uuid",
                (tunnel_id,),
            )
            return cur.rowcount > 0


async def mark_disconnected(tunnel_id: str) -> None:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE ssh_tunnels SET disconnected_at = NOW() "
                "WHERE tunnel_id = %s::uuid",
                (tunnel_id,),
            )


async def mark_superseded(tunnel_id: str) -> None:
    """A newer pairing of the tunnel's machine superseded it, as its machine
    said, logged in with the tunnel's key."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE ssh_tunnels SET superseded_at = NOW() "
                "WHERE tunnel_id = %s::uuid",
                (tunnel_id,),
            )


async def gateway_beat(tunnel_ids: Iterable[str]) -> Set[str]:
    """The gateway's heartbeat: it is up, and the tunnels it holds still
    connected; answers those that still exist.

    One transaction, so a tunnel never outlives the gateway's own beat: when
    the beats stop, the tunnels lapse no earlier than the gateway stops
    counting as settled, and none reads as down for it.
    """
    ids = list(tunnel_ids)
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("UPDATE ssh_gateway_state SET seen_at = NOW()")
            if not ids:
                return set()
            await cur.execute(
                "UPDATE ssh_tunnels SET seen_at = NOW() "
                "WHERE tunnel_id = ANY(%s::uuid[]) "
                "RETURNING tunnel_id::text AS tunnel_id",
                (ids,),
            )
            return {row["tunnel_id"] for row in await cur.fetchall()}


_ALL_DOWN = (
    "UPDATE ssh_tunnels SET disconnected_at = NOW() "
    "WHERE connected_at IS NOT NULL "
    "AND (disconnected_at IS NULL OR connected_at > disconnected_at)"
)


async def gateway_started() -> int:
    """At the gateway's start: none of the connections it held survived it,
    and it is not settled until its machines have had time to connect again.

    One transaction, so no worker sees the tunnels down under the previous
    gateway's beat, which may still be fresh after a crash.
    """
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO ssh_gateway_state (singleton, started_at, seen_at) "
                "VALUES (TRUE, NOW(), NOW()) ON CONFLICT (singleton) "
                "DO UPDATE SET started_at = NOW(), seen_at = NOW()"
            )
            await cur.execute(_ALL_DOWN)
            return cur.rowcount


async def gateway_stopped() -> int:
    """At the gateway's stop: its connections end with it, and no tunnel
    reads as down until a gateway is back and settled."""
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute("UPDATE ssh_gateway_state SET seen_at = NULL")
            await cur.execute(_ALL_DOWN)
            return cur.rowcount


# -- The gateway's keys -----------------------------------------------------------


async def ensure_gateway_key(
    name: str, generate: Callable[[], Tuple[str, str]]
) -> Tuple[str, str]:
    """The gateway key called *name*, as (public line, private text).

    ``generate`` makes a new pair when there is none yet. The gateway and the
    server's workers may ask at once; the first insert wins and every caller
    answers the stored pair.
    """
    enc_key = get_encryption_key()
    select = (
        "SELECT public_key, pgp_sym_decrypt(private_key, %s) AS private_key "
        "FROM ssh_gateway_keys WHERE name = %s"
    )
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(select, (enc_key, name))
            row = await cur.fetchone()
            if row is None:
                public_key, private_key = generate()
                await cur.execute(
                    """
                    INSERT INTO ssh_gateway_keys (name, public_key, private_key)
                    VALUES (%s, %s, pgp_sym_encrypt(%s, %s))
                    ON CONFLICT (name) DO NOTHING
                    """,
                    (name, public_key, private_key, enc_key),
                )
                await cur.execute(select, (enc_key, name))
                row = await cur.fetchone()
            assert row is not None
            return row["public_key"], row["private_key"]
