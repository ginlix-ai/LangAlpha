"""Consolidate migration 046 computers into per-user machines with layout v4 folders.

Run presync before 046, once hours ahead and again before downtime: HTTP backup
keeps acquisition with the app to avoid identity races and works across schemas,
so a window that lands 046 and then aborts before the fold can presync again
against the migrated schema before the next attempt; folders later restore lazily
from the mirror using per-folder sync markers.
Run consolidate after migration, with the app already stopped, under EGU(user),
C(primary) and C(folding): the full-window --apply refuses to start (exit 2)
while /health still answers at --base-url, and --i-stopped-the-app overrides
that probe for an app that is down somewhere this script cannot dial, while
--user-id rehearses one user against a live app. Under --apply each retired
machine's sandbox is stopped too, about a second apiece, so a window folding
hundreds of machines spends minutes there. Run reap-orphans after the default
seven-day soak so unsynced bytes remain recoverable.
Keep presync imports compatible with the old container and defer provider imports
to reap; importing src.config, migrations, or server startup risks load_dotenv
silently retargeting writes, which is why the shared fences, column list and key
derivation come from src.server.database.sql_fences, a leaf that imports nothing,
while the rest of the mirrored SQL is checked by
tests/unit/scripts/ops/test_consolidate_computers.py.
Both modes write NDJSON, a plan under dry run and an audit for manual reversal
under --apply, and a record lands only once its user's transaction committed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

# Script execution puts scripts/ops on sys.path, not the root containing src.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ops._db import build_db_uri  # noqa: E402
from src.server.database.sql_fences import (  # noqa: E402
    COMPUTER_COLS,
    FENCE_LIVE_WORKSPACE,
    FENCE_NOT_DELETED,
    advisory_key,
    workspace_dir_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consolidate_computers")


# Mirror sandbox/migration.py:CURRENT_LAYOUT_VERSION. 046's backfill sets no
# layout_version, so a backfilled row reads 0, which means never stamped, not v0.
CURRENT_LAYOUT_VERSION = 4

# Unknown tiers rank below known tiers so they cannot win a fold.
# Limit logged refs; retired rows retain the full list.
_REFS_SHOWN = 12

TIER_RANK = {"standard": 0, "performance": 1, "max": 2}


def computer_advisory_key(computer_id: str) -> int:
    """Match database/computer.py's C(computer) lock domain."""
    return advisory_key("C", computer_id)


def user_egress_lock_key(user_id: str) -> int:
    """Match lock_user_egress_state: sync_egress_grants takes EGU(user) and C(computer)."""
    return advisory_key("EGU", user_id)


def abbreviated(values: Sequence[str]) -> str:
    """Keep one log line readable; the audit record carries the full list."""
    shown = ", ".join(values[:_REFS_SHOWN])
    rest = len(values) - _REFS_SHOWN
    return f"{shown}, and {rest} more" if rest > 0 else shown


def computer_status_channel(computer_id: str) -> str:
    """Mirror workspace_status_pubsub.status_channel."""
    return f"computer:status:{computer_id}"


def workspace_status_channel(workspace_id: str) -> str:
    """Mirror workspace_status_pubsub.workspace_status_channel."""
    return f"ws:status:{workspace_id}"


def redis_url() -> str:
    """Mirror RedisCacheClient's precedence: config.yaml's redis.url, then REDIS_URL."""
    import yaml

    path = Path(__file__).resolve().parents[2] / "config.yaml"
    try:
        section = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("redis")
    except OSError:
        section = None
    configured = (section or {}).get("url")
    return configured or os.getenv("REDIS_URL", "redis://localhost:6379/0")


def sandbox_config_hash(computer: dict[str, Any]) -> str:
    """Match ComputerManager._compute_sandbox_config_hash without loading deployment config.

    The computers row has NOT NULL kind and root_dir, so its deployment fallbacks
    are unreachable.
    """
    data: dict[str, Any] = {
        "provider": computer["kind"],
        "working_dir": computer["root_dir"],
    }
    if computer.get("provider_config"):
        data["provider_config"] = computer["provider_config"]
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:8]


def sandbox_config_stamp(computer: dict[str, Any]) -> dict[str, Any]:
    """Re-stamp using ComputerManager._sandbox_config_stamp to prevent destructive migration.

    A stale hash would recreate the shared sandbox on next start, disrupting siblings.
    """
    return {
        "sandbox_config_hash": sandbox_config_hash(computer),
        "sandbox_provider": computer["kind"],
        "sandbox_working_dir": computer["root_dir"],
    }


# The three run totals that no single row stands behind.
_FAILURE_KEYS = ("presync_failed", "users_failed", "reap_failed")


class _Sink:
    """Everything the run records: the NDJSON file, the tally, and the fleet wakeups.

    A user's fold runs in one transaction that a single failure rolls back, so a
    queued record and its tally land only once the caller has committed; a
    provider call, which no rollback undoes, lands immediately under a
    ``provider.`` key so the report never reads a stopped sandbox as a committed row.
    """

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        self.refs: list[str] = []
        self._fh = path.open("a", encoding="utf-8") if path else None
        self.tally: dict[str, int] = {}
        self._records: list[dict[str, Any]] = []
        self._pending_tally: list[str] = []
        self._pending_refs: list[str] = []
        self._publishes: list[tuple[str, dict[str, Any]]] = []
        self._providers: Any = None
        self._redis: Any = None

    def queue(self, action: str, *, also: Sequence[str] = (), **fields: Any) -> None:
        """One row changed: hold the record and its count until the caller commits."""
        self._records.append(self._shape(action, fields))
        self._pending_tally.append(action)
        self._pending_tally.extend(also)

    def note(
        self, key: str, n: int = 1, *, record: Optional[str] = None, **fields: Any
    ) -> None:
        """Count, and optionally record, what a rollback does not take back.

        A run total, an observation behind a warning, and a provider call are all
        equally true whether or not the surrounding transaction commits.
        """
        self.tally[key] = self.tally.get(key, 0) + n
        if record is not None:
            self._emit(self._shape(record, fields))

    def hold_ref(self, ref: str) -> None:
        """A retired machine's sandbox, for the list the reap pass works from."""
        self._pending_refs.append(ref)

    def queue_status(
        self, computer_id: str, status: str, *, workspace_ids: Sequence[str] = ()
    ) -> None:
        """Mirror _publish_shadowed: the machine's channel, plus each shadowed workspace."""
        self._publishes.append(
            (
                computer_status_channel(computer_id),
                {"computer_id": computer_id, "status": status},
            )
        )
        for workspace_id in workspace_ids:
            self._publishes.append(
                (
                    workspace_status_channel(workspace_id),
                    {"workspace_id": workspace_id, "status": status},
                )
            )

    def drop_pending(self) -> None:
        self._records.clear()
        self._pending_tally.clear()
        self._pending_refs.clear()
        self._publishes.clear()

    async def flush(self) -> None:
        for key in self._pending_tally:
            self.tally[key] = self.tally.get(key, 0) + 1
        self._pending_tally.clear()
        self.refs.extend(self._pending_refs)
        self._pending_refs.clear()
        for record in self._records:
            self._emit(record)
        self._records.clear()
        await self._publish()

    def failed(self) -> bool:
        """A sandbox that would not stop is the reap pass's job, not a failed run."""
        return any(self.tally.get(key) for key in _FAILURE_KEYS)

    def line(self) -> str:
        parts = [f"{k}={v}" for k, v in sorted(self.tally.items()) if v]
        return " ".join(parts) or "nothing to do"

    async def stop_sandbox(self, row: dict[str, Any]) -> str:
        """Stop the sandbox: 'stopped', 'gone' or 'failed'.

        A rolled-back user leaves the sandbox stopped, which its next start undoes.
        """
        providers = self._provider_cache()
        try:
            provider = await providers.get(row)
            runtime = await provider.get(row["provider_ref"])
            # The folding machines are usually already stopped (046 backfills
            # a stopped shadow per workspace), and Daytona rejects a stop on
            # a stopped sandbox as "not in a stoppable state". That is the
            # outcome we want, not a failure to log and count.
            from ptc_agent.core.sandbox.runtime import RuntimeState

            if await runtime.get_state() in (
                RuntimeState.STOPPED,
                RuntimeState.ARCHIVED,
            ):
                return "stopped"
            await runtime.stop()
            return "stopped"
        except Exception as e:
            if providers.is_gone(row, e):
                return "gone"
            logger.error(
                "      sandbox %s of computer %s did not stop, and the reap pass "
                "deletes it after the soak: %s",
                row["provider_ref"],
                row["computer_id"],
                e,
            )
            return "failed"

    def _provider_cache(self) -> Any:
        if self._providers is None:
            self._providers = _ProviderCache()
        return self._providers

    async def close(self) -> None:
        if self._providers is not None:
            await self._providers.close()
            self._providers = None
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception as e:
                logger.warning("Redis close failed: %s", e)
            self._redis = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    @staticmethod
    def _shape(action: str, fields: dict[str, Any]) -> dict[str, Any]:
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action,
            **fields,
        }

    def _emit(self, record: dict[str, Any]) -> None:
        if self._fh is None:
            return
        self._fh.write(json.dumps(record, default=str) + "\n")
        self._fh.flush()

    async def _publish(self) -> None:
        """Wake the workers that cached a machine this fold moved or retired.

        Best-effort: a missed wake leaves the runtime on its polling fallback.
        """
        pending, self._publishes = self._publishes, []
        if not pending:
            return
        try:
            if self._redis is None:
                import redis.asyncio as aioredis

                self._redis = aioredis.Redis.from_url(redis_url())
            for channel, payload in pending:
                await self._redis.publish(channel, json.dumps(payload))
        except Exception as e:
            logger.warning(
                "%d status publish(es) failed, so a worker holding one of these "
                "machines drops its handle on its next poll instead: %s",
                len(pending),
                e,
            )


async def _fetch(
    conn: psycopg.AsyncConnection, sql: str, params: Any = None
) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]


async def _has_computers(conn: psycopg.AsyncConnection) -> bool:
    """Presync must work before migration 046; consolidate and reap require it."""
    rows = await _fetch(conn, "SELECT to_regclass('public.computers') AS t")
    return rows[0]["t"] is not None


async def _presync_targets(
    conn: psycopg.AsyncConnection, user_id: Optional[str], *, has_computers: bool
) -> list[dict]:
    """Before 046, sandbox_id and workspace status identify machines; afterward computers do."""
    scope = "AND w.user_id = %(user_id)s" if user_id else ""
    params: dict[str, Any] = {"user_id": user_id} if user_id else {}
    if has_computers:
        sql = f"""
            WITH multi AS (
                SELECT w.user_id
                FROM workspaces w
                JOIN computers c ON c.computer_id = w.computer_id
                WHERE w.{FENCE_LIVE_WORKSPACE} AND c.{FENCE_NOT_DELETED} {scope}
                GROUP BY w.user_id
                HAVING count(DISTINCT w.computer_id) > 1
            )
            SELECT w.workspace_id, w.user_id, w.name, w.dir_name, w.last_activity_at
            FROM workspaces w
            JOIN computers c ON c.computer_id = w.computer_id
            JOIN multi m ON m.user_id = w.user_id
            WHERE w.{FENCE_LIVE_WORKSPACE} AND c.status = 'running'
            ORDER BY w.user_id, w.workspace_id
        """
    else:
        sql = f"""
            WITH multi AS (
                SELECT w.user_id
                FROM workspaces w
                WHERE w.{FENCE_LIVE_WORKSPACE} AND w.sandbox_id IS NOT NULL {scope}
                GROUP BY w.user_id
                HAVING count(DISTINCT w.sandbox_id) > 1
            )
            SELECT w.workspace_id, w.user_id, w.name,
                   NULL::text AS dir_name, w.last_activity_at
            FROM workspaces w
            JOIN multi m ON m.user_id = w.user_id
            WHERE w.status = 'running'
            ORDER BY w.user_id, w.workspace_id
        """
    return await _fetch(conn, sql, params or None)


async def _merge_user_ids(
    conn: psycopg.AsyncConnection, user_id: Optional[str]
) -> list[str]:
    """A lone computer without a primary needs repair or the next workspace creates another."""
    ws_scope = "AND w.user_id = %(user_id)s" if user_id else ""
    c_scope = "AND c.user_id = %(user_id)s" if user_id else ""
    rows = await _fetch(
        conn,
        f"""
        SELECT user_id FROM (
            SELECT w.user_id
            FROM workspaces w
            JOIN computers c ON c.computer_id = w.computer_id
            WHERE w.computer_id IS NOT NULL
              AND w.{FENCE_LIVE_WORKSPACE}
              AND c.{FENCE_NOT_DELETED}
              {ws_scope}
            GROUP BY w.user_id
            HAVING count(DISTINCT w.computer_id) > 1
                OR count(*) FILTER (WHERE c.is_primary) = 0
                OR count(*) FILTER (
                       WHERE w.sandbox_id IS NULL
                         AND c.status = 'running'
                         AND c.provider_ref IS NOT NULL) > 0
            UNION
            -- A machine stuck in 'creating' is unreachable and may hold no
            -- workspace at all, so it cannot be found through the join above.
            SELECT c.user_id
            FROM computers c
            WHERE c.status = 'creating' {c_scope}
        ) u
        ORDER BY user_id
        """,
        {"user_id": user_id} if user_id else None,
    )
    return [r["user_id"] for r in rows]


async def _user_computers(conn: psycopg.AsyncConnection, user_id: str) -> list[dict]:
    """Match 046's activity ranking and computer_id DESC tie-break for repeatable elections."""
    return await _fetch(
        conn,
        f"""
        SELECT {COMPUTER_COLS},
               act.activity_at,
               COALESCE(act.live_workspaces, 0) AS live_workspaces
        FROM computers c
        LEFT JOIN LATERAL (
            SELECT max(COALESCE(w.last_activity_at, w.updated_at, w.created_at))
                       AS activity_at,
                   count(*) AS live_workspaces
            FROM workspaces w
            WHERE w.computer_id = c.computer_id AND w.{FENCE_LIVE_WORKSPACE}
        ) act ON TRUE
        WHERE c.user_id = %(user_id)s AND c.{FENCE_NOT_DELETED}
        ORDER BY COALESCE(act.activity_at, c.last_activity_at, c.updated_at,
                          c.created_at) DESC NULLS LAST,
                 c.computer_id DESC
        """,
        {"user_id": user_id},
    )


async def _live_workspaces(
    conn: psycopg.AsyncConnection, computer_id: str
) -> list[dict]:
    return await _fetch(
        conn,
        f"""
        SELECT workspace_id, user_id, name, dir_name, status, computer_id,
               last_activity_at, config
        FROM workspaces
        WHERE computer_id = %(computer_id)s AND {FENCE_LIVE_WORKSPACE}
        ORDER BY created_at, workspace_id
        """,
        {"computer_id": computer_id},
    )


async def _presync(
    conn: psycopg.AsyncConnection,
    *,
    apply: bool,
    user_id: Optional[str],
    base_url: str,
    timeout: float,
    sink: _Sink,
) -> None:
    """Downtime stops the app, not sandboxes; reap would destroy changes absent from the mirror."""
    import httpx

    has_computers = await _has_computers(conn)
    logger.info(
        "presync: reading the %s schema",
        "post-migration" if has_computers else "pre-migration",
    )
    rows = await _presync_targets(conn, user_id, has_computers=has_computers)
    users_seen = len({r["user_id"] for r in rows})
    sink.note("users_seen", users_seen)
    logger.info(
        "presync: %d running workspace(s) across %d user(s) with a merge ahead",
        len(rows),
        users_seen,
    )
    if not rows:
        logger.info(
            "A stopped machine needs no presync: its mirror is current from the "
            "sync that runs at stop."
        )
        return

    if not apply:
        for row in rows:
            logger.info(
                "  would sync workspace %s (%s) of user %s",
                row["workspace_id"],
                row["dir_name"] or row["name"],
                row["user_id"],
            )
        return

    token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
    if not token:
        logger.error(
            "INTERNAL_SERVICE_TOKEN is not set, so the backup route cannot be "
            "called as a service. Refusing to run."
        )
        sink.note("presync_failed", len(rows))
        return

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        for row in rows:
            wid = str(row["workspace_id"])
            started = time.monotonic()
            try:
                resp = await client.post(
                    f"/api/v1/workspaces/{wid}/files/backup",
                    headers={
                        "X-Service-Token": token,
                        "X-User-Id": row["user_id"],
                    },
                )
            except Exception as e:
                logger.error("  workspace %s sync failed: %s", wid, e)
                sink.note(
                    "presync_failed",
                    record="presync_failed",
                    workspace_id=wid,
                    error=str(e),
                )
                continue
            elapsed = time.monotonic() - started
            if resp.status_code != 200:
                logger.error(
                    "  workspace %s sync returned %s: %s",
                    wid,
                    resp.status_code,
                    resp.text[:300],
                )
                sink.note(
                    "presync_failed",
                    record="presync_failed",
                    workspace_id=wid,
                    status_code=resp.status_code,
                )
                continue
            body = resp.json()
            sink.note("files_synced", int(body.get("synced") or 0))
            errors = int(body.get("errors") or 0)
            sink.note("sync_errors", errors)
            level = logger.warning if errors else logger.info
            level(
                "  workspace %s (%s) synced=%s skipped=%s deleted=%s errors=%s "
                "oversized=%s bytes=%s in %.1fs",
                wid,
                row["dir_name"],
                body.get("synced"),
                body.get("skipped"),
                body.get("deleted"),
                errors,
                body.get("oversized"),
                body.get("total_size"),
                elapsed,
            )
            # --presync-audit carries this watermark to detect activity after backup.
            sink.queue(
                "presync",
                workspace_id=wid,
                user_id=row["user_id"],
                dir_name=row["dir_name"],
                last_activity_at=row["last_activity_at"],
                result={
                    k: body.get(k)
                    for k in (
                        "synced",
                        "skipped",
                        "deleted",
                        "errors",
                        "oversized",
                        "total_size",
                    )
                },
            )
            # No transaction wraps an HTTP backup, so each call is its own commit.
            await sink.flush()

    if sink.tally.get("sync_errors") or sink.tally.get("presync_failed"):
        logger.error(
            "presync finished with %d in-sync error(s) and %d failed call(s). "
            "Fix these by hand before the window: a workspace whose mirror is "
            "incomplete loses the unmirrored files when its sandbox is reaped.",
            sink.tally.get("sync_errors", 0),
            sink.tally.get("presync_failed", 0),
        )


def _presync_watermarks(path: Optional[Path]) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    marks: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("action") == "presync" and record.get("workspace_id"):
            marks[str(record["workspace_id"])] = record.get("last_activity_at") or ""
    return marks


def elect_primary(computers: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Preserve the primary the app already uses; only its absence permits activity election."""
    for computer in computers:
        if computer["is_primary"]:
            return computer
    return computers[0] if computers else None


def highest_tier(computers: Sequence[dict[str, Any]]) -> Optional[str]:
    """Folding must preserve the largest tier the user paid for; recreate cost is tier-independent."""
    if not computers:
        return None
    best = max(computers, key=lambda c: TIER_RANK.get(c["resource_tier"], -1))
    return best["resource_tier"]


def pick_grant_survivor(
    group: Sequence[dict[str, Any]], primary_id: str
) -> dict[str, Any]:
    """Keep the primary's incumbent policy to avoid silently changing relay access.

    Without an incumbent, the caller's ordering selects the freshest folding row.
    """
    for grant in group:
        if str(grant["computer_id"]) == primary_id:
            return grant
    return group[0]


def _free_dir_name(workspace: dict[str, Any], taken: set[str]) -> tuple[str, bool]:
    """Widen the suffix as create_workspace_on_computer does for WorkspaceDirNameTaken.

    The name belongs to the user and may legitimately match another project, so a
    collision costs the folder name, not the fold; the flag says one was given up.
    """
    current = (workspace.get("dir_name") or "").strip()
    if current and current not in taken:
        return current, False
    for attempt in range(4):
        candidate = workspace_dir_name(
            workspace.get("name"),
            str(workspace["workspace_id"]),
            hex_chars=4 + 4 * attempt,
        )
        if candidate not in taken:
            return candidate, bool(current)
    raise RuntimeError(f"No free folder name for workspace {workspace['workspace_id']}")


async def _adopt_grants(
    conn: psycopg.AsyncConnection,
    *,
    primary_id: str,
    folding_ids: Sequence[str],
    workspace_ids: Sequence[str],
    apply: bool,
    sink: _Sink,
) -> None:
    """Keep revoked losers for relay JWTs that still name their grant_id.

    Migration 047's unique (computer_id, kind, connection_id, server_name) index has
    no status predicate, so losers must retain their original computer labels, and
    the adoption carries _adopt_grants_onto_computer's NOT EXISTS guard: a revoked
    row the primary already holds for the subject blocks the move, and the runtime's
    answer is that the blocking row survives and the mover is revoked.
    Include workspace_ids to catch 046's NULL-computer grants before their next sync
    collides with an adopted grant.
    """
    rows = await _fetch(
        conn,
        """
        SELECT grant_id, user_id, workspace_id, computer_id, kind, connection_id,
               server_name, status, tool_allowlist, tool_denylist,
               policy_required, destination_url, updated_at
        FROM sandbox_egress_grants
        WHERE status = 'active'
          AND (computer_id = ANY(%(scope)s::uuid[])
               OR workspace_id = ANY(%(workspaces)s::uuid[]))
        ORDER BY kind, connection_id, server_name, updated_at DESC, grant_id
        """,
        {
            "scope": [primary_id, *folding_ids],
            "workspaces": list(workspace_ids),
        },
    )
    if not rows:
        return

    async def revoke(grant_id: str, survivor_id: Optional[str]) -> None:
        # The loser's claimants still need the subject; they move to the
        # survivor so its next sweep does not read them as having left.
        if survivor_id is not None:
            await conn.execute(
                """
                INSERT INTO sandbox_egress_grant_claims (grant_id, workspace_id)
                SELECT %(survivor)s::uuid, workspace_id
                FROM sandbox_egress_grant_claims
                WHERE grant_id = %(grant_id)s
                ON CONFLICT DO NOTHING
                """,
                {"survivor": survivor_id, "grant_id": grant_id},
            )
        await conn.execute(
            """
            UPDATE sandbox_egress_grants
            SET status = 'revoked', updated_at = NOW()
            WHERE grant_id = %(grant_id)s AND status = 'active'
            """,
            {"grant_id": grant_id},
        )

    async def primary_holder(row: dict) -> Optional[str]:
        """The primary's own row for this subject, when the guard blocked a move."""
        holder = await _fetch(
            conn,
            """
            SELECT grant_id FROM sandbox_egress_grants
            WHERE computer_id = %(primary)s AND kind = %(kind)s
              AND connection_id IS NOT DISTINCT FROM %(connection_id)s::uuid
              AND server_name IS NOT DISTINCT FROM %(server_name)s
            LIMIT 1
            """,
            {
                "primary": primary_id,
                "kind": row["kind"],
                "connection_id": row["connection_id"],
                "server_name": row.get("server_name"),
            },
        )
        return str(holder[0]["grant_id"]) if holder else None

    # Match the index's NULL equality and both oauth_mcp/header_mcp subject columns.
    by_key: dict[tuple[str, Optional[str], Optional[str]], list[dict]] = {}
    for row in rows:
        key = (
            row["kind"],
            str(row["connection_id"]) if row["connection_id"] else None,
            row.get("server_name") or None,
        )
        by_key.setdefault(key, []).append(row)

    for (kind, connection_id, server_name), group in by_key.items():
        subject = connection_id or server_name
        survivor = pick_grant_survivor(group, primary_id)
        for row in group:
            gid = str(row["grant_id"])
            if row is survivor:
                if str(row["computer_id"] or "") == primary_id:
                    continue
                # A dry run cannot know whether the guard blocks, so it reports
                # the move it would attempt.
                moved = True
                if apply:
                    cur = await conn.execute(
                        """
                        UPDATE sandbox_egress_grants g
                        SET computer_id = %(primary)s, updated_at = NOW()
                        WHERE g.grant_id = %(grant_id)s AND g.status = 'active'
                          AND NOT EXISTS (
                              SELECT 1 FROM sandbox_egress_grants o
                              WHERE o.computer_id = %(primary)s
                                AND o.kind = g.kind
                                AND o.connection_id IS NOT DISTINCT FROM g.connection_id
                                AND o.server_name IS NOT DISTINCT FROM g.server_name
                          )
                        """,
                        {"primary": primary_id, "grant_id": gid},
                    )
                    moved = bool(cur.rowcount)
                if moved:
                    logger.info(
                        "    grant %s (%s/%s) adopted onto primary",
                        gid,
                        kind,
                        subject,
                    )
                    sink.queue(
                        "grant_adopted",
                        grant_id=gid,
                        kind=kind,
                        connection_id=connection_id,
                        server_name=server_name,
                        before={"computer_id": row["computer_id"]},
                        after={"computer_id": primary_id},
                    )
                    continue
                logger.warning(
                    "    grant %s (%s/%s) cannot move onto the primary, which "
                    "already holds a row for that subject; revoking it instead, "
                    "so the primary's row is the survivor",
                    gid,
                    kind,
                    subject,
                )
                await revoke(gid, await primary_holder(row))
                sink.queue(
                    "grant_revoked",
                    grant_id=gid,
                    kind=kind,
                    connection_id=connection_id,
                    server_name=server_name,
                    reason="primary_holds_subject",
                    before={"status": "active", "computer_id": row["computer_id"]},
                    after={"status": "revoked"},
                )
                continue

            differs = (
                row["tool_allowlist"] != survivor["tool_allowlist"]
                or row["tool_denylist"] != survivor["tool_denylist"]
                or bool(row["policy_required"]) != bool(survivor["policy_required"])
            )
            if differs:
                logger.warning(
                    "    grant %s (%s/%s) revoked in favour of %s, and their tool "
                    "policies differ: loser allowlist=%s denylist=%s "
                    "policy_required=%s; survivor allowlist=%s denylist=%s "
                    "policy_required=%s",
                    gid,
                    kind,
                    subject,
                    survivor["grant_id"],
                    row["tool_allowlist"],
                    row["tool_denylist"],
                    row["policy_required"],
                    survivor["tool_allowlist"],
                    survivor["tool_denylist"],
                    survivor["policy_required"],
                )
            else:
                logger.info(
                    "    grant %s (%s/%s) revoked in favour of %s (same policy)",
                    gid,
                    kind,
                    subject,
                    survivor["grant_id"],
                )
            if apply:
                await revoke(gid, str(survivor["grant_id"]))
            sink.queue(
                "grant_revoked",
                also=("grant_policy_differs",) if differs else (),
                grant_id=gid,
                kind=kind,
                connection_id=connection_id,
                server_name=server_name,
                survivor_grant_id=str(survivor["grant_id"]),
                policy_differs=differs,
                before={
                    "status": "active",
                    "computer_id": row["computer_id"],
                    "tool_allowlist": row["tool_allowlist"],
                    "tool_denylist": row["tool_denylist"],
                    "policy_required": row["policy_required"],
                },
                after={"status": "revoked"},
            )


async def _consolidate_user(
    conn: psycopg.AsyncConnection,
    user_id: str,
    *,
    apply: bool,
    sink: _Sink,
    watermarks: dict[str, str],
) -> None:
    # The locks ride --apply: a fleet-wide dry run would otherwise block every
    # live grant sync for the length of the read.
    if apply:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)", (user_egress_lock_key(user_id),)
        )

    computers = await _user_computers(conn, user_id)
    if not computers:
        return
    primary = elect_primary(computers)
    assert primary is not None
    primary_id = str(primary["computer_id"])

    # Re-read under C(computer): yield if another writer elected a different primary.
    if apply:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)", (computer_advisory_key(primary_id),)
        )
    computers = await _user_computers(conn, user_id)
    settled = elect_primary(computers)
    if settled is None or str(settled["computer_id"]) != primary_id:
        logger.warning(
            "  user %s: the primary moved to %s while taking the lock; skipping",
            user_id,
            settled and settled["computer_id"],
        )
        return
    primary = settled

    folding = [c for c in computers if str(c["computer_id"]) != primary_id]
    # C(folding) too, so a live worker's machine-decision lock cannot tear a
    # folding machine down mid-fold. Sorted by id: two runs take them in one order.
    if apply:
        for folding_id in sorted(str(c["computer_id"]) for c in folding):
            await conn.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (computer_advisory_key(folding_id),),
            )
    logger.info(
        "  user %s: primary %s (%s, tier=%s, layout_version=%s, %s live "
        "workspace(s)), %d computer(s) folding",
        user_id,
        primary_id,
        primary["status"],
        primary["resource_tier"],
        primary["layout_version"],
        primary["live_workspaces"],
        len(folding),
    )

    if _layout_blocks_the_fold(primary, folding, user_id=user_id, sink=sink):
        return

    stamp = sandbox_config_stamp(primary)
    creating = await _settle_creating(conn, computers, apply=apply, sink=sink)

    # Before the rebind loop: its shadow copy reads the tier this raises.
    changes = _fold_primary_columns(primary, folding, user_id=user_id, sink=sink)
    if changes and apply:
        await _write_primary_columns(conn, primary_id, changes)

    folded = await _rebind_folding_workspaces(
        conn,
        primary=primary,
        folding=folding,
        user_id=user_id,
        stamp=stamp,
        watermarks=watermarks,
        apply=apply,
        sink=sink,
    )
    adopted = await _adopt_machine_sandbox(conn, primary_id, apply=apply, sink=sink)

    primary_workspaces = await _live_workspaces(conn, primary_id)
    _warn_stale_hashes(primary_workspaces, folded, stamp, sink=sink)
    await _adopt_grants(
        conn,
        primary_id=primary_id,
        folding_ids=[str(c["computer_id"]) for c in folding],
        workspace_ids=sorted(
            {*folded, *(str(w["workspace_id"]) for w in primary_workspaces)}
        ),
        apply=apply,
        sink=sink,
    )
    retired = await _retire_losers(
        conn, folding, user_id=user_id, apply=apply, sink=sink
    )

    # An apply already has the rebinds in this transaction's own snapshot; a dry
    # run has to add what it only planned.
    live_on_primary = len(primary_workspaces) + (0 if apply else len(folded))
    zeroed = await _zero_mcp_config_version(
        conn, primary, live_on_primary, apply=apply, sink=sink
    )

    if any((creating, changes, folded, adopted, retired, zeroed)):
        sink.note("users_changed")


def _layout_blocks_the_fold(
    primary: dict[str, Any],
    folding: Sequence[dict[str, Any]],
    *,
    user_id: str,
    sink: _Sink,
) -> bool:
    """A fold is safe when the primary's root is already v4, or when 046 recorded the workspace it was built from.

    The v3-to-v4 sweep then moves that folder's files rather than the triggering
    workspace's. With neither, the layout is unknown: layout_version 0 is 046
    never stamping it, not v0.
    """
    primary_id = str(primary["computer_id"])
    primary_layout = int(primary["layout_version"] or 0)
    if not (
        any(c["live_workspaces"] for c in folding)
        and primary_layout < CURRENT_LAYOUT_VERSION
        and not primary["origin_workspace_id"]
    ):
        return False
    logger.warning(
        "  user %s: the primary %s reads layout_version %s against v%d and "
        "names no origin_workspace_id, so nothing says which folder its "
        "root sweep would fill, and %s still hold live workspaces. Skipping "
        "the user: the sweep would pull work/, data/ and agent.md into "
        "whichever folder its next start finds, which the next backup reads "
        "as deletions.",
        user_id,
        primary_id,
        primary_layout,
        CURRENT_LAYOUT_VERSION,
        abbreviated([f"{c['computer_id']}=v{c['layout_version']}" for c in folding]),
    )
    sink.queue(
        "user_skipped",
        user_id=user_id,
        reason="layout_unknown",
        computer_id=primary_id,
        primary_layout_version=primary_layout,
        folding_layout_versions={
            str(c["computer_id"]): c["layout_version"] for c in folding
        },
    )
    return True


def _fold_primary_columns(
    primary: dict[str, Any],
    folding: Sequence[dict[str, Any]],
    *,
    user_id: str,
    sink: _Sink,
) -> dict[str, Any]:
    """The primary's whole column fold, decided from the rows already read.

    is_primary, resource_tier and is_always_on are each a pure function of the
    user's computers, so one dict carries all three into one statement against
    that row rather than three.
    """
    primary_id = str(primary["computer_id"])
    changes: dict[str, Any] = {}

    if not primary["is_primary"]:
        changes["is_primary"] = True
        logger.info("    electing %s as the primary (no primary existed)", primary_id)
        sink.queue(
            "primary_elected",
            computer_id=primary_id,
            user_id=user_id,
            before={"is_primary": False},
            after={"is_primary": True},
        )

    best_tier = highest_tier([primary, *folding])
    if TIER_RANK.get(best_tier, -1) > TIER_RANK.get(primary["resource_tier"], -1):
        changes["resource_tier"] = best_tier
        donors = [
            str(c["computer_id"]) for c in folding if c["resource_tier"] == best_tier
        ]
        logger.info(
            "    raising primary tier %s to %s (from computer %s)",
            primary["resource_tier"],
            best_tier,
            ", ".join(donors),
        )
        sink.queue(
            "tier_raised",
            computer_id=primary_id,
            donors=donors,
            before={"resource_tier": primary["resource_tier"]},
            after={"resource_tier": best_tier},
        )

    if any(c["is_always_on"] for c in folding) and not primary["is_always_on"]:
        changes["is_always_on"] = True
        logger.info("    folding always-on onto the primary")
        sink.queue(
            "always_on_folded",
            computer_id=primary_id,
            before={"is_always_on": False},
            after={"is_always_on": True},
        )

    return changes


async def _rebind_folding_workspaces(
    conn: psycopg.AsyncConnection,
    *,
    primary: dict[str, Any],
    folding: Sequence[dict[str, Any]],
    user_id: str,
    stamp: dict[str, Any],
    watermarks: dict[str, str],
    apply: bool,
    sink: _Sink,
) -> list[str]:
    """Move every folding machine's live projects onto the primary, each under a free folder."""
    primary_id = str(primary["computer_id"])
    # No status predicate: idx_workspaces_computer_dir has none either and a
    # delete is a tombstone, so a deleted project still owns its folder name.
    taken = {
        str(r["dir_name"])
        for r in await _fetch(
            conn,
            """
            SELECT dir_name FROM workspaces
            WHERE computer_id = %(computer_id)s
              AND dir_name IS NOT NULL AND dir_name <> ''
            """,
            {"computer_id": primary_id},
        )
    }
    folded: list[str] = []

    for old in folding:
        old_id = str(old["computer_id"])
        if old["layout_version"] > primary["layout_version"]:
            logger.warning(
                "    computer %s is at layout_version %s and the primary at %s; "
                "the folded folders are created under the primary's layout",
                old_id,
                old["layout_version"],
                primary["layout_version"],
            )
        for workspace in await _live_workspaces(conn, old_id):
            wid = str(workspace["workspace_id"])
            mark = watermarks.get(wid)
            if mark and str(workspace["last_activity_at"] or "") > mark:
                sink.note("moved_since_presync")
                logger.warning(
                    "    workspace %s was active after presync recorded it (%s > "
                    "%s); its mirror may be behind its sandbox",
                    wid,
                    workspace["last_activity_at"],
                    mark,
                )
            dir_name, reslugged = _free_dir_name(workspace, taken)
            taken.add(dir_name)
            folded.append(wid)
            logger.info(
                "    rebinding workspace %s %r -> %s as %s",
                wid,
                workspace["dir_name"],
                primary_id,
                dir_name,
            )
            if apply:
                rebound = await _rebind(
                    conn,
                    workspace_id=wid,
                    computer_id=primary_id,
                    expected_computer_id=old_id,
                    dir_name=dir_name,
                    stamp=stamp,
                )
                if rebound is None:
                    raise RuntimeError(
                        f"Workspace {wid} was not rebindable to {primary_id} "
                        f"(expected {old_id}); aborting user {user_id}"
                    )
            sink.queue(
                "workspace_rebound",
                also=("dir_name_reslugged",) if reslugged else (),
                workspace_id=wid,
                user_id=user_id,
                before={"computer_id": old_id, "dir_name": workspace["dir_name"]},
                after={"computer_id": primary_id, "dir_name": dir_name},
                stamp=stamp,
            )
    return folded


def _warn_stale_hashes(
    primary_workspaces: Sequence[dict[str, Any]],
    folded: Sequence[str],
    stamp: dict[str, Any],
    *,
    sink: _Sink,
) -> None:
    """Report a primary project's stale hash, never rewrite it.

    Overwriting would hide a pending migration that recreates the shared machine
    and restores every sibling from the mirror.
    """
    for existing in primary_workspaces:
        if str(existing["workspace_id"]) in folded:
            continue
        stored = (existing.get("config") or {}).get("sandbox_config_hash")
        if stored and stored != stamp["sandbox_config_hash"]:
            sink.note("stale_hash_found")
            logger.warning(
                "    workspace %s on the primary is stamped %s, not %s: its next "
                "start recreates the machine and every folded folder restores "
                "from the mirror",
                existing["workspace_id"],
                stored,
                stamp["sandbox_config_hash"],
            )


async def _retire_losers(
    conn: psycopg.AsyncConnection,
    folding: Sequence[dict[str, Any]],
    *,
    user_id: str,
    apply: bool,
    sink: _Sink,
) -> int:
    """Tombstone every folding machine, keeping provider_ref for the reap pass."""
    for old in folding:
        old_id = str(old["computer_id"])
        if old["provider_ref"]:
            sink.hold_ref(f"{old['kind']}:{old['provider_ref']}")
        logger.info(
            "    retiring computer %s (status %s, provider_ref kept for the reap pass)",
            old_id,
            old["status"],
        )
        stopped = False
        if apply:
            retired = await _retire_computer(conn, old_id, expected=old["status"])
            if retired is None:
                raise RuntimeError(
                    f"Computer {old_id} was not retirable from status "
                    f"{old['status']}; aborting user {user_id}"
                )
            stranded = retired.get("shadowed_workspace_ids") or []
            if stranded:
                # A nonempty shadow list means retirement deleted a missed workspace; roll back.
                raise RuntimeError(
                    f"Retiring computer {old_id} would have tombstoned live "
                    f"workspaces {stranded}; aborting user {user_id}"
                )
            sink.queue_status(old_id, "deleted")
            if old["provider_ref"]:
                stopped = await _stop_retired_sandbox(conn, old, sink=sink)
        sink.queue(
            "computer_retired",
            computer_id=old_id,
            user_id=user_id,
            provider_ref=old["provider_ref"],
            kind=old["kind"],
            stopped=stopped,
            before={
                "status": old["status"],
                "is_primary": old["is_primary"],
                "is_always_on": old["is_always_on"],
            },
            after={"status": "deleted", "is_primary": False, "is_always_on": False},
        )
    return len(folding)


async def _stop_retired_sandbox(
    conn: psycopg.AsyncConnection, old: dict[str, Any], *, sink: _Sink
) -> bool:
    """Stop the loser's sandbox, recorded at once because no rollback restarts it."""
    cid = str(old["computer_id"])
    ref = old["provider_ref"]
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT 1 FROM computers
            WHERE kind = %s AND provider_ref = %s
              AND computer_id <> %s::uuid AND status <> 'deleted'
            LIMIT 1
            """,
            (old["kind"], ref, cid),
        )
        if await cur.fetchone() is not None:
            # A live machine still runs on this sandbox; stopping it would stop them.
            logger.info(
                "      sandbox %s is shared with a live machine; left running", ref
            )
            sink.note("provider.sandbox_shared")
            return False
    outcome = await sink.stop_sandbox(old)
    if outcome == "stopped":
        logger.info("      stopped sandbox %s", ref)
        sink.note(
            "provider.sandbox_stopped",
            record="sandbox_stopped",
            computer_id=cid,
            provider_ref=ref,
        )
        return True
    if outcome == "gone":
        logger.info("      sandbox %s is already gone", ref)
        sink.note("provider.sandbox_gone")
        return False
    sink.note(
        "provider.sandbox_stop_failed",
        record="sandbox_stop_failed",
        computer_id=cid,
        provider_ref=ref,
    )
    return False


async def _zero_mcp_config_version(
    conn: psycopg.AsyncConnection,
    primary: dict[str, Any],
    live_on_primary: int,
    *,
    apply: bool,
    sink: _Sink,
) -> bool:
    """A folded machine's union no longer matches the number one workspace lent it.

    Per-workspace writers reach no shared generation, so 0 says "unobserved"
    until the resolve becomes machine-scoped.
    """
    if live_on_primary <= 1 or primary["mcp_config_version"] == 0:
        return False
    primary_id = str(primary["computer_id"])
    logger.info(
        "    zeroing mcp_config_version on %s (%d workspaces on the machine)",
        primary_id,
        live_on_primary,
    )
    if apply:
        await conn.execute(
            f"""
            UPDATE computers SET mcp_config_version = 0, updated_at = NOW()
            WHERE computer_id = %(computer_id)s AND {FENCE_NOT_DELETED}
            """,
            {"computer_id": primary_id},
        )
    sink.queue(
        "mcp_config_version_zeroed",
        computer_id=primary_id,
        before={"mcp_config_version": primary["mcp_config_version"]},
        after={"mcp_config_version": 0},
    )
    return True


async def _settle_creating(
    conn: psycopg.AsyncConnection,
    computers: Sequence[dict],
    *,
    apply: bool,
    sink: _Sink,
) -> int:
    """Old creating rows cannot be acquired; new computers start stopped and provision on start."""
    settled = 0
    for computer in computers:
        if computer["status"] != "creating":
            continue
        cid = str(computer["computer_id"])
        settled += 1
        logger.info("    settling computer %s from creating to stopped", cid)
        if apply:
            cur = await conn.execute(
                f"""
                WITH comp AS (
                    UPDATE computers c
                    SET status = 'stopped', stopped_at = NOW(), updated_at = NOW()
                    WHERE c.computer_id = %(computer_id)s
                      AND c.status = 'creating' AND c.{FENCE_NOT_DELETED}
                    RETURNING computer_id
                )
                UPDATE workspaces w
                SET status = 'stopped', stopped_at = NOW(), updated_at = NOW()
                FROM comp
                WHERE w.computer_id = comp.computer_id
                  AND w.{FENCE_LIVE_WORKSPACE}
                RETURNING w.workspace_id
                """,
                {"computer_id": cid},
            )
            sink.queue_status(
                cid,
                "stopped",
                workspace_ids=[str(r[0]) for r in await cur.fetchall()],
            )
        sink.queue(
            "computer_settled",
            computer_id=cid,
            before={"status": "creating"},
            after={"status": "stopped"},
        )
        computer["status"] = "stopped"
    return settled


async def _adopt_machine_sandbox(
    conn: psycopg.AsyncConnection,
    computer_id: str,
    *,
    apply: bool,
    sink: _Sink,
) -> int:
    """Match adopt_computer_sandbox_into_workspaces, repairing only sandbox_id IS NULL.

    A different sandbox_id is a split binding; overwriting it would bypass the bind fence.
    """
    split = await _fetch(
        conn,
        f"""
        SELECT w.workspace_id, w.sandbox_id, c.provider_ref
        FROM workspaces w
        JOIN computers c ON c.computer_id = w.computer_id
        WHERE w.computer_id = %(computer_id)s
          AND w.{FENCE_LIVE_WORKSPACE}
          AND w.sandbox_id IS NOT NULL
          AND c.provider_ref IS NOT NULL
          AND w.sandbox_id <> c.provider_ref
        """,
        {"computer_id": computer_id},
    )
    for row in split:
        sink.note("split_binding_found")
        logger.warning(
            "    workspace %s names sandbox %s but its machine names %s: a split "
            "binding, left alone for a human",
            row["workspace_id"],
            row["sandbox_id"],
            row["provider_ref"],
        )

    unbound = await _fetch(
        conn,
        f"""
        SELECT w.workspace_id
        FROM workspaces w
        JOIN computers c ON c.computer_id = w.computer_id
        WHERE w.computer_id = %(computer_id)s
          AND w.{FENCE_LIVE_WORKSPACE}
          AND w.sandbox_id IS NULL
          AND c.provider_ref IS NOT NULL
          AND c.status = 'running'
        """,
        {"computer_id": computer_id},
    )
    if not unbound:
        return 0
    logger.info(
        "    adopting the machine's sandbox onto %d workspace(s) that name none",
        len(unbound),
    )
    if apply:
        await conn.execute(
            f"""
            WITH comp AS (
                SELECT computer_id, provider_ref, status, platform_secret_version
                FROM computers
                WHERE computer_id = %(computer_id)s
                  AND provider_ref IS NOT NULL
                  AND status = 'running'
                FOR SHARE
            )
            UPDATE workspaces w
            SET sandbox_id = comp.provider_ref,
                status = comp.status,
                platform_secret_version = comp.platform_secret_version,
                updated_at = NOW()
            FROM comp
            WHERE w.computer_id = comp.computer_id
              AND w.sandbox_id IS NULL
              AND w.{FENCE_LIVE_WORKSPACE}
            """,
            {"computer_id": computer_id},
        )
    for row in unbound:
        sink.queue(
            "sandbox_shadow_adopted",
            workspace_id=str(row["workspace_id"]),
            computer_id=computer_id,
            before={"sandbox_id": None},
        )
    return len(unbound)


async def _rebind(
    conn: psycopg.AsyncConnection,
    *,
    workspace_id: str,
    computer_id: str,
    expected_computer_id: Optional[str],
    dir_name: str,
    stamp: dict[str, Any],
) -> Optional[dict]:
    """Match rebind_workspace_to_computer's CAS and shadow copy, plus the config stamp.

    One statement because the runtime uses one: a separate shadow write can miss,
    and mcp_config_version stays out of it because that is the column every config
    write advances and the grant CAS gates on.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            WITH comp AS (
                SELECT computer_id, status, resource_tier, is_always_on,
                       provider_ref, platform_secret_version
                FROM computers
                WHERE computer_id = %(computer_id)s AND {FENCE_NOT_DELETED}
                FOR SHARE
            )
            UPDATE workspaces w
            SET computer_id = comp.computer_id,
                dir_name = COALESCE(%(dir_name)s, w.dir_name),
                status = comp.status,
                sandbox_id = comp.provider_ref,
                resource_tier = comp.resource_tier,
                is_always_on = comp.is_always_on,
                platform_secret_version = comp.platform_secret_version,
                config = COALESCE(w.config, '{{}}'::jsonb) || %(stamp)s::jsonb,
                updated_at = NOW()
            FROM comp
            WHERE w.workspace_id = %(workspace_id)s
              AND w.computer_id IS NOT DISTINCT FROM %(expected)s
              AND w.{FENCE_LIVE_WORKSPACE}
            RETURNING w.workspace_id, w.computer_id, w.dir_name
            """,
            {
                "computer_id": computer_id,
                "dir_name": dir_name,
                "workspace_id": workspace_id,
                "expected": expected_computer_id,
                "stamp": Json(stamp),
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


# The two columns a computers row also keeps on its workspace shadows, which the
# platform's capacity counts still read.
_SHADOWED_COLUMNS = ("resource_tier", "is_always_on")


async def _write_primary_columns(
    conn: psycopg.AsyncConnection, computer_id: str, changes: dict[str, Any]
) -> None:
    """The whole primary fold in one statement, shaped like _set_computer_scalar's pair.

    The IS DISTINCT FROM fence keeps a re-run a no-op even though the decisions
    were taken from a read this statement can no longer see.
    """
    assignments = ", ".join(f"{col} = %({col})s" for col in changes)
    fence = " OR ".join(f"c.{col} IS DISTINCT FROM %({col})s" for col in changes)
    computers_update = f"""
        UPDATE computers c
        SET {assignments}, updated_at = NOW()
        WHERE c.computer_id = %(computer_id)s AND c.{FENCE_NOT_DELETED}
          AND ({fence})
    """
    params: dict[str, Any] = {"computer_id": computer_id, **changes}
    shadowed = [col for col in changes if col in _SHADOWED_COLUMNS]
    if not shadowed:
        await conn.execute(computers_update, params)
        return
    shadow_assignments = ", ".join(f"{col} = %({col})s" for col in shadowed)
    await conn.execute(
        f"""
        WITH comp AS ({computers_update}
            RETURNING computer_id
        )
        UPDATE workspaces w
        SET {shadow_assignments}, updated_at = NOW()
        FROM comp
        WHERE w.computer_id = comp.computer_id AND w.{FENCE_LIVE_WORKSPACE}
        """,
        params,
    )


async def _retire_computer(
    conn: psycopg.AsyncConnection, computer_id: str, *, expected: str
) -> Optional[dict]:
    """Retain provider_ref so the delayed reap can find the sandbox holding the bytes.

    Clear is_always_on and stamp stopped_at: an always-on machine runs with
    auto_stop_minutes 0 and the idle reaper only scans running machines, so a loser
    left always-on bills until the reap pass a week later. Keep the shadow UPDATE to
    detect live workspaces that should have been rebound; a nonempty returned id
    list must abort retirement.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            WITH comp AS (
                UPDATE computers c
                SET status = 'deleted', is_primary = FALSE, is_always_on = FALSE,
                    stopped_at = NOW(), updated_at = NOW()
                WHERE c.computer_id = %(computer_id)s
                  AND c.{FENCE_NOT_DELETED}
                  AND c.status = %(expected)s
                RETURNING {COMPUTER_COLS}
            ),
            shadow AS (
                UPDATE workspaces w
                SET status = 'deleted', updated_at = NOW()
                FROM comp
                WHERE w.computer_id = comp.computer_id
                  AND w.{FENCE_LIVE_WORKSPACE}
                RETURNING w.workspace_id
            )
            SELECT {COMPUTER_COLS},
                   COALESCE((SELECT array_agg(workspace_id) FROM shadow),
                            ARRAY[]::uuid[]) AS shadowed_workspace_ids
            FROM comp
            """,
            {"computer_id": computer_id, "expected": expected},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def _workspace_identity(
    conn: psycopg.AsyncConnection, user_ids: Sequence[str]
) -> dict[str, Any]:
    """Workspace ids and rows must survive, so references without a foreign key stay valid.

    Scoped to the users being folded: a fleet-wide digest reads an unrelated
    user's concurrent project as this job having re-keyed a workspace.
    """
    rows = await _fetch(
        conn,
        """
        SELECT count(*) AS n,
               count(*) FILTER (WHERE status = 'deleted') AS deleted,
               md5(COALESCE(string_agg(workspace_id::text, ',' ORDER BY workspace_id), ''))
                   AS digest
        FROM workspaces
        WHERE user_id = ANY(%(users)s::text[])
        """,
        {"users": list(user_ids)},
    )
    return rows[0]


async def _consolidate(
    conn: psycopg.AsyncConnection,
    *,
    apply: bool,
    user_id: Optional[str],
    sink: _Sink,
    presync_audit: Optional[Path],
) -> None:
    watermarks = _presync_watermarks(presync_audit)
    if watermarks:
        logger.info("Read %d presync watermark(s).", len(watermarks))

    users = await _merge_user_ids(conn, user_id)
    sink.note("users_seen", len(users))
    if not users:
        logger.info(
            "Nothing to consolidate: every user's live workspaces already sit on "
            "one computer, that computer is their primary, no machine is stuck "
            "in 'creating' and no workspace is missing its machine's sandbox."
        )
        return
    logger.info("consolidate: %d user(s) to fold", len(users))

    # Only an apply can move a row, so only an apply has this invariant to check,
    # and only across the users it folds: a digest over the whole table reads an
    # unrelated user's concurrent project as a workspace this job re-keyed.
    identity_before = await _workspace_identity(conn, users) if apply else None

    started = time.monotonic()
    for uid in users:
        # Roll back only the failing user so one bad row does not consume the
        # window. The exit code still reports failure.
        try:
            async with conn.transaction():
                await _consolidate_user(
                    conn,
                    uid,
                    apply=apply,
                    sink=sink,
                    watermarks=watermarks,
                )
        except Exception:
            sink.note("users_failed")
            logger.error(
                "  user %s aborted, its transaction rolled back", uid, exc_info=True
            )
            sink.drop_pending()
        else:
            await sink.flush()
    elapsed = time.monotonic() - started
    per_user = elapsed / max(1, len(users))
    logger.info(
        "consolidate: %d user(s) in %.2fs (%.1f ms/user)",
        len(users),
        elapsed,
        per_user * 1000,
    )

    if identity_before is None:
        return
    identity_after = await _workspace_identity(conn, users)
    if identity_after != identity_before:
        raise RuntimeError(
            f"The set of workspace ids changed during the run: "
            f"{identity_before} -> {identity_after}. Nothing in this job may "
            f"create, delete or re-key a workspace."
        )
    logger.info(
        "Workspace ids unchanged across the %d folded user(s): %s row(s), %s "
        "tombstoned, digest %s. Every row elsewhere that names a workspace, "
        "including the ones that hold the id as a plain string with no foreign "
        "key, still resolves.",
        len(users),
        identity_before["n"],
        identity_before["deleted"],
        identity_before["digest"][:12],
    )


async def _reap(
    conn: psycopg.AsyncConnection,
    *,
    apply: bool,
    user_id: Optional[str],
    soak_days: int,
    sink: _Sink,
) -> None:
    """Delay deletion through the soak period so unsynced work remains recoverable."""
    scope = "AND c.user_id = %(user_id)s" if user_id else ""
    rows = await _fetch(
        conn,
        f"""
        SELECT c.computer_id, c.user_id, c.kind, c.provider_ref, c.root_dir,
               c.provider_config, c.updated_at
        FROM computers c
        WHERE c.status = 'deleted'
          AND c.provider_ref IS NOT NULL
          AND c.updated_at < NOW() - make_interval(days => %(soak)s)
          AND NOT EXISTS (
              SELECT 1 FROM workspaces w
              WHERE w.computer_id = c.computer_id AND w.{FENCE_LIVE_WORKSPACE}
          )
          -- A legacy row that never resolved through the app still names the
          -- sandbox by id; its files are only in that sandbox until it does.
          AND NOT EXISTS (
              SELECT 1 FROM workspaces w
              WHERE w.sandbox_id = c.provider_ref AND w.{FENCE_LIVE_WORKSPACE}
          )
          {scope}
        ORDER BY c.updated_at
        """,
        {"soak": soak_days, "user_id": user_id} if user_id else {"soak": soak_days},
    )
    sink.note("users_seen", len({r["user_id"] for r in rows}))
    logger.info(
        "reap-orphans: %d tombstoned computer(s) past a %d-day soak still name a "
        "sandbox",
        len(rows),
        soak_days,
    )
    for row in rows:
        logger.info(
            "  computer %s (%s) sandbox %s, tombstoned %s",
            row["computer_id"],
            row["kind"],
            row["provider_ref"],
            row["updated_at"],
        )
    if not rows or not apply:
        return

    providers = _ProviderCache()
    try:
        for row in rows:
            cid = str(row["computer_id"])
            ref = row["provider_ref"]
            try:
                provider = await providers.get(row)
                runtime = await provider.get(ref)
                await runtime.delete()
                logger.info("  deleted sandbox %s of computer %s", ref, cid)
                sink.note(
                    "provider.sandbox_deleted",
                    record="sandbox_deleted",
                    computer_id=cid,
                    provider_ref=ref,
                )
            except Exception as e:
                if providers.is_gone(row, e):
                    sink.note("provider.sandbox_gone")
                    logger.info("  sandbox %s of computer %s is already gone", ref, cid)
                else:
                    logger.error("  sandbox %s of computer %s: %s", ref, cid, e)
                    sink.note(
                        "reap_failed",
                        record="reap_failed",
                        computer_id=cid,
                        provider_ref=ref,
                        error=str(e),
                    )
                    continue
            await conn.execute(
                """
                UPDATE computers SET provider_ref = NULL, updated_at = NOW()
                WHERE computer_id = %(computer_id)s AND status = 'deleted'
                  AND provider_ref = %(provider_ref)s
                """,
                {"computer_id": cid, "provider_ref": ref},
            )
            sink.queue(
                "sandbox_reaped",
                computer_id=cid,
                kind=row["kind"],
                before={"provider_ref": ref},
                after={"provider_ref": None},
            )
            # Autocommit, so each cleared ref is its own commit.
            await sink.flush()
    finally:
        await providers.close()


class _ProviderCache:
    """Validate settings per provider to avoid shared config-reference aliasing.

    Match _provider_for via build_provider(kind, settings, working_dir).
    """

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str], Any] = {}
        self._sandbox_config = None

    def _settings(self, kind: str, overrides: Optional[dict]):
        if self._sandbox_config is None:
            import yaml

            from ptc_agent.config.utils import create_sandbox_config

            # Avoid load_core_from_files: load_dotenv could retarget this operator script.
            path = Path(__file__).resolve().parents[2] / "agent_config.yaml"
            self._sandbox_config = create_sandbox_config(
                yaml.safe_load(path.read_text(encoding="utf-8"))
            )
        base = getattr(self._sandbox_config, kind, None)
        if base is None:
            raise ValueError(f"Unknown sandbox provider: {kind!r}")
        if not overrides:
            return base
        unknown = sorted(set(overrides) - set(type(base).model_fields))
        if unknown:
            raise ValueError(
                f"provider_config for kind {kind!r} names unknown settings: "
                f"{', '.join(unknown)}"
            )
        return type(base).model_validate({**base.model_dump(), **overrides})

    async def get(self, row: dict[str, Any]):
        from ptc_agent.core.sandbox.providers import build_provider

        key = (
            row["kind"],
            json.dumps(row.get("provider_config") or {}, sort_keys=True),
        )
        client = self._clients.get(key)
        if client is None:
            client = build_provider(
                row["kind"],
                self._settings(row["kind"], row.get("provider_config")),
                working_dir=row["root_dir"],
            )
            self._clients[key] = client
        return client

    def is_gone(self, row: dict[str, Any], exc: Exception) -> bool:
        from ptc_agent.core.sandbox.runtime import SandboxFailureKind

        key = (
            row["kind"],
            json.dumps(row.get("provider_config") or {}, sort_keys=True),
        )
        client = self._clients.get(key)
        if client is None:
            return False
        return client.classify_error(exc) is SandboxFailureKind.SANDBOX_GONE

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.close()
            except Exception as e:
                logger.warning("Provider close failed: %s", e)
        self._clients.clear()


async def _window_is_open(args: argparse.Namespace) -> bool:
    """A full consolidate --apply rebinds every user, so no worker may be serving.

    The app is the authority on whether it is running, so probe its health route and
    refuse while anything answers. A --user-id rehearsal is exempt: it is one user,
    under the same locks a live worker takes.
    """
    if args.i_stopped_the_app:
        logger.warning(
            "--i-stopped-the-app given, so %s is not probed. A worker still "
            "serving turns keeps sessions on the machines this run retires.",
            args.base_url,
        )
        return True

    import httpx

    url = args.base_url.rstrip("/") + "/health"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
    except Exception as e:
        logger.info("Nothing answered %s (%s), so the app is stopped.", url, e)
        return True
    logger.error(
        "The app answered %s with HTTP %s. Stop the backend first (the window "
        "flips the edge to maintenance before this pass), rehearse one user "
        "with --user-id, or pass --i-stopped-the-app when the app is down "
        "somewhere this script cannot dial. Refusing to run.",
        url,
        resp.status_code,
    )
    return False


async def _run(args: argparse.Namespace) -> int:
    if not args.apply:
        logger.info("DRY RUN, nothing will be written. Re-run with --apply.")
    if args.pass_name == "consolidate" and args.apply and not args.user_id:
        if not await _window_is_open(args):
            return 2
    # The dry run's plan is what an operator diffs against the apply run's audit,
    # so both passes write one and --apply only changes the default name.
    stem = "consolidate_audit" if args.apply else "consolidate_plan"
    path = Path(
        args.audit or f"./{stem}.{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.ndjson"
    )
    logger.info("%s: %s", "Audit log" if args.apply else "Plan file", path)
    sink = _Sink(path)

    try:
        async with await psycopg.AsyncConnection.connect(
            build_db_uri(), autocommit=True
        ) as conn:
            if args.pass_name == "presync":
                await _presync(
                    conn,
                    apply=args.apply,
                    user_id=args.user_id,
                    base_url=args.base_url,
                    timeout=args.timeout,
                    sink=sink,
                )
            elif args.pass_name == "consolidate":
                await _consolidate(
                    conn,
                    apply=args.apply,
                    user_id=args.user_id,
                    sink=sink,
                    presync_audit=Path(args.presync_audit)
                    if args.presync_audit
                    else None,
                )
            else:
                await _reap(
                    conn,
                    apply=args.apply,
                    user_id=args.user_id,
                    soak_days=args.soak_days,
                    sink=sink,
                )
    finally:
        # In the finally so an unexpected raise still leaves the operator the
        # totals and the ref list they need to continue.
        logger.info("TOTALS (%s): %s", args.pass_name, sink.line())
        if sink.refs:
            refs = sorted(sink.refs)
            logger.info(
                "Sandboxes held for the reap pass: %d (%s). The full list is the "
                "retired rows themselves: computers with status 'deleted' and a "
                "provider_ref still set.",
                len(refs),
                abbreviated(refs),
            )
        await sink.close()
    return 1 if sink.failed() else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pass",
        dest="pass_name",
        required=True,
        choices=("presync", "consolidate", "reap-orphans"),
        help="Which pass to run. See the module docstring for the order.",
    )
    parser.add_argument(
        "--apply", action="store_true", help="Mutate rows (default: dry-run)."
    )
    parser.add_argument(
        "--user-id",
        metavar="USER",
        help="Restrict to one user, for the live rehearsal before the window.",
    )
    parser.add_argument(
        "--audit",
        metavar="PATH",
        help="NDJSON log, one record per row changed (default: "
        "./consolidate_audit.<ts>.ndjson under --apply, "
        "./consolidate_plan.<ts>.ndjson for a dry run).",
    )
    parser.add_argument(
        "--presync-audit",
        metavar="PATH",
        help="A presync run's audit log. The consolidate pass warns on any "
        "workspace that was active after presync recorded it.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("CONSOLIDATE_BASE_URL", "http://localhost:8000"),
        help="Where the app answers: the presync pass's backup calls, and the "
        "/health probe that guards a full consolidate --apply.",
    )
    parser.add_argument(
        "--i-stopped-the-app",
        action="store_true",
        help="Skip the /health probe that guards a full consolidate --apply, for "
        "an app that is stopped somewhere this script cannot dial.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=900.0,
        help="Per-workspace backup timeout in seconds (default: 900). A 2 GB "
        "workspace packs for minutes in a 1-CPU cgroup.",
    )
    parser.add_argument(
        "--soak-days",
        type=int,
        default=7,
        help="How long a computer must have been tombstoned before reap-orphans "
        "deletes its sandbox (default: 7).",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
