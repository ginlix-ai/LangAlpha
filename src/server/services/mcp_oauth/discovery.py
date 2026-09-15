"""Host-side tool discovery for the user catalog.

Every remote (``http``/``sse``) catalog row is discovered from this process
now, not only the OAuth ones: an OAuth row sends the bearer from the token
store, any other row sends its own headers with vault refs resolved, and an
open server sends nothing. The cache row lives in ``user_mcp_tool_schemas``
either way; a schema-digest change fans out a version bump so sessions
re-resolve, while an unchanged re-discovery stays silent. A stdio row has no
host-side path (its command is the user's code) and is left to the sandbox.

Discovery runs on create, on an edit that moves the fingerprint, on enable
and on import, so a row's tool count and its auth verdict are known before
the user's next turn rather than after it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_oauth import SERVABLE, ConnectionStatus, get_connection
from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    claim_probe_kick,
    get_catalog_server,
)
from src.server.database.mcp_tool_schemas import (
    SchemaWrite,
    upsert_user_tool_schemas,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_vault_secrets import get_user_secrets_decrypted
from src.server.models.mcp_server import ProbeResult
from src.server.services.mcp_identity import bounded_identity
from src.server.services.mcp_oauth.lifecycle import (
    TokenUnavailable,
    ensure_fresh_access_token,
)
from src.server.services.mcp_probe import (
    DISCOVERY_TIMEOUT_S,
    INVALID_HEADER_VALUE,
    ProbeOutcome,
    bounded_probe,
    missing_secrets_result,
    probe_result,
    rejected_header_result,
)

logger = logging.getLogger(__name__)

REMOTE_TRANSPORTS = ("http", "sse")


def _schema_digest(tools: list[dict]) -> str:
    canonical = json.dumps(tools, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def vault_ref_names(headers: dict[str, str] | None) -> list[str]:
    """Every ``${vault:NAME}`` a header map refers to, first occurrence first."""
    names: list[str] = []
    for value in (headers or {}).values():
        names.extend(n for n in VAULT_REF_RE.findall(value or "") if n not in names)
    return names


class RejectedHeaderValue(ValueError):
    """A resolved header value this process will not put on the wire.

    Raised rather than dropped, for the reason a missing ref is: a configured
    header that silently goes missing comes back as a 401 and reads as the
    wrong key. Each caller turns this into the refusal its own surface renders.
    """

    def __init__(self, header: str) -> None:
        super().__init__(INVALID_HEADER_VALUE)
        self.header = header


def resolve_header_refs(
    headers: dict[str, str] | None, secrets: dict[str, str]
) -> tuple[dict[str, str], list[str]]:
    """Expand ``${vault:NAME}`` in header values; also the names that had no value.

    A header with a missing ref is dropped rather than sent with the literal
    ref string: a server told ``Bearer ${vault:X}`` answers 401 and the row
    would then read as "rejected the credentials" when the truth is that no
    credential was configured yet.
    """
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key, value in (headers or {}).items():
        names = VAULT_REF_RE.findall(value or "")
        unknown = [n for n in names if n not in secrets]
        if unknown:
            missing.extend(n for n in unknown if n not in missing)
            continue
        expanded = VAULT_REF_RE.sub(lambda m: secrets[m.group(1)], value or "")
        # This sink's own control-character rule, which storage no longer
        # applies for it: a trailing newline came with the paste rather than
        # with the key, while a break inside the value is a second header, and
        # there is no reading of that worth guessing at.
        expanded = expanded.rstrip()
        if "\r" in expanded or "\n" in expanded:
            raise RejectedHeaderValue(key)
        resolved[key] = expanded
    return resolved, missing


def _discarded(server_name: str, write: SchemaWrite) -> TokenUnavailable:
    """The connection left the servable set while we were on the network.

    Answered exactly as the entry gate would have: the status IS the reason, so
    a disconnect that lands mid-discovery reads the same to the caller (409)
    whether it beat the gate or the write.
    """
    reason = write.connection_status or "unknown_connection"
    logger.info(
        "[mcp_discovery] dropped a discovery write for %s: connection is %s",
        server_name, reason,
    )
    return TokenUnavailable(reason)


def _superseded(server_name: str) -> TokenUnavailable:
    """The catalog config moved while we were on the network.

    The edit that moved it schedules its own rediscovery, which owns
    convergence — landing this result would key the snapshot to a dead
    fingerprint and delete the current one.
    """
    logger.info(
        "[mcp_discovery] dropped a stale discovery write for %s: config changed",
        server_name,
    )
    return TokenUnavailable("superseded", "server config changed during discovery")


@dataclass(frozen=True)
class _SnapshotWriter:
    """The fenced write every host-side discovery lands through.

    One transaction covers the fingerprint re-check and the write: a
    concurrent edit either commits first (this result is discarded as stale)
    or blocks on the FOR SHARE until this snapshot lands and the edit's own
    rediscovery supersedes it. Without the fence, a slow discovery finishing
    after a newer one deletes the current config's snapshot and resurrects a
    dead fingerprint's.
    """

    user_id: str
    server_name: str
    fingerprint: str
    connection_id: str | None

    async def write(self, *, probe: ProbeResult, **upsert_kwargs) -> SchemaWrite:
        from src.server.services.mcp_config import user_row_to_server_config
        from src.server.services.mcp_discovery import mcp_discovery_fingerprint

        async with get_db_connection() as conn:
            async with conn.transaction():
                current = await get_catalog_server(
                    self.user_id, self.server_name, conn=conn, for_share=True
                )
                if current is None or mcp_discovery_fingerprint(
                    user_row_to_server_config(current)
                ) != self.fingerprint:
                    raise _superseded(self.server_name)
                write = await upsert_user_tool_schemas(
                    self.user_id, self.server_name, self.fingerprint,
                    connection_id=self.connection_id, conn=conn,
                    last_probe=probe.model_dump(mode="json"),
                    **upsert_kwargs,
                )
        if write.row is None:
            # Refused under the write's own lock: the disconnect already
            # purged what we would be re-adding.
            raise _discarded(self.server_name, write)
        return write

    async def fail(self, probe: ProbeResult) -> dict:
        write = await self.write(
            probe=probe, status="error", error=probe.error,
            observed_meta={"probe": "host"},
        )
        return write.row

    async def settle(self, outcome: ProbeOutcome) -> dict:
        """Land a probe's verdict: an error row, or the tools plus a bump when
        the surface moved."""
        probe = probe_result(outcome)
        if not outcome.ok:
            logger.warning(
                "[mcp_discovery] discovery failed for %s: %s",
                self.server_name, outcome.error,
            )
            return await self.fail(probe)

        for name, reason in outcome.skipped:
            logger.info(
                "[mcp_discovery] skipped tool %r on %s: %s",
                name, self.server_name, reason,
            )
        digest = _schema_digest(outcome.tools)
        write = await self.write(
            probe=probe,
            tools=outcome.tools,
            status="ok",
            schema_digest=digest,
            # What the CACHED tools came with, which is why the server's own
            # mark is here as well as on the verdict: this copy survives a
            # later probe that fails, so the row keeps its icon.
            observed_meta={
                "probe": "host",
                "skipped": [list(s) for s in outcome.skipped],
                "server_info": bounded_identity(outcome.identity),
            },
        )
        if write.replaced_digest != digest:
            # Tool surface changed → sessions must regenerate wrappers. The
            # comparison is against what this write actually replaced, not a
            # snapshot read before it: two workers probing the same row both
            # read the pre-write digest, and the one that lands second would
            # see its own value there and decline the bump its row needs.
            await bump_user_workspaces_mcp_version(self.user_id)
        logger.info(
            "[mcp_discovery] discovered %d tools on %s (digest %s)",
            len(outcome.tools), self.server_name, digest[:12],
        )
        return write.row


async def refresh_user_tool_schemas(user_id: str, server_name: str) -> dict:
    """Discover an OAuth server's tools host-side and cache the snapshot.

    Returns the cache row. Never raises for discovery failures — they land as
    an ``error`` row (the no-downgrade upsert keeps the last good snapshot).
    Raises :class:`TokenUnavailable` when the connection is unusable, including
    when it stops being usable while this discovery is in flight.
    """
    from src.server.services.mcp_config import (
        same_consented_url,
        user_row_to_server_config,
    )
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    row = await get_catalog_server(user_id, server_name)
    if row is None:
        raise TokenUnavailable("unknown_server")
    connection = await get_connection(user_id, server_name)
    if connection is None:
        raise TokenUnavailable("unknown_connection")
    if connection.status not in SERVABLE:
        # Raised rather than recorded: a connection the user has to repair is
        # the caller's answer (409), not a discovery error row that would then
        # read as "this server's tools are broken".
        raise TokenUnavailable(str(connection.status))
    # This is where the token meets the URL: an edit may have moved the catalog
    # row off the endpoint the user consented to (and the revoke that follows
    # such an edit is not atomic with it), so re-bind here rather than trust
    # that every write path got there first.
    if connection.server_url and not same_consented_url(
        connection.server_url, row.get("url")
    ):
        raise TokenUnavailable("needs_reauth", "server URL changed since consent")

    writer = _SnapshotWriter(
        user_id, server_name,
        mcp_discovery_fingerprint(user_row_to_server_config(row)),
        connection.connection_id,
    )
    try:
        token = await ensure_fresh_access_token(connection.connection_id)
    except TokenUnavailable as e:
        # Recorded as an OAuth verdict rather than a bare failure: this row
        # holds a connection whose token cannot be minted, and reconnecting is
        # the whole repair.
        return await writer.fail(
            probe_result(
                ProbeOutcome(
                    ok=False, auth="oauth", error=f"token unavailable: {e.reason}"
                )
            )
        )

    outcome = await bounded_probe(
        row["url"],
        {"Authorization": token.header()},
        timeout_s=DISCOVERY_TIMEOUT_S,
        background=True,
    )
    return await writer.settle(outcome)


async def discover_catalog_server(user_id: str, server_name: str) -> dict | None:
    """Discover one catalog row from this process, whatever it authenticates with.

    Returns the cache row, or None for a row that has no host-side path (a
    stdio command, a legacy ``sse`` row, or a name that is not in the catalog).
    A row any connection short of revoked claims takes the OAuth refresh, and
    its :class:`TokenUnavailable` propagates; only a row nothing claims sends
    its own headers, vault refs resolved from the user's vault, and lands
    whatever the server said as the row.
    """
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    row = await get_catalog_server(user_id, server_name)
    # ``http`` only: the probe dials streamable HTTP, so an ``sse`` row has no
    # path from here and keeps the sandbox's own discovery.
    if row is None or row.get("transport") != "http" or not row.get("url"):
        return None
    connection = await get_connection(user_id, server_name)
    # Read the way the relay and the write-time warning read it: a connection
    # that has not been revoked claims the row whatever its state. The user was
    # told these headers are not sent while the server is OAuth-connected, so a
    # token that needs repair answers as the OAuth path does -- reconnect --
    # rather than quietly falling back to the headers.
    if connection is not None and connection.status is not ConnectionStatus.REVOKED:
        return await refresh_user_tool_schemas(user_id, server_name)

    writer = _SnapshotWriter(
        user_id, server_name,
        mcp_discovery_fingerprint(user_row_to_server_config(row)),
        None,
    )
    try:
        headers, missing = resolve_header_refs(
            row.get("headers"),
            await get_user_secrets_decrypted(
                user_id, vault_ref_names(row.get("headers"))
            ),
        )
    except RejectedHeaderValue:
        # Landed as the row's verdict rather than logged: the user changed a
        # secret, and the page is where they will look for what it broke.
        return await writer.fail(rejected_header_result())
    if missing:
        return await writer.fail(missing_secrets_result(missing))
    outcome = await bounded_probe(
        row["url"], headers, timeout_s=DISCOVERY_TIMEOUT_S, background=True
    )
    return await writer.settle(outcome)


# Strong refs: asyncio holds only weak references to tasks, so a bare
# fire-and-forget handle can be garbage-collected mid-flight.
_discovery_tasks: set[asyncio.Task] = set()
# Execution context for THIS worker, never truth about a row: which probe this
# process is running, and which of them a newer kick has queued behind it. A
# sibling worker's probe is invisible here, which is why the rate limit every
# worker has to agree on is the row's own ``probe_kicked_at`` stamp and not a
# dict.
_in_flight: dict[tuple[str, str], asyncio.Task] = {}
_rerun: set[tuple[str, str]] = set()
SELF_HEAL_INTERVAL_S = 120.0


async def _discovery_pass(user_id: str, name: str, reason: str) -> None:
    """One discovery, plus the live-sandbox resync a changed surface owes.

    Never raises: this runs detached, so a vendor failure has to land as a row
    and a log line rather than as a task nobody awaits.
    """
    from src.server.services.mcp_oauth.connect import _resync_live_sandboxes

    try:
        row = await discover_catalog_server(user_id, name)
    except TokenUnavailable as e:
        # A connection the user has to repair: nothing to write, the
        # connection status already says what to do.
        logger.info(
            "[mcp_discovery] %s discovery for %s skipped: %s", reason, name, e.reason
        )
        return
    except Exception:
        logger.warning(
            "[mcp_discovery] %s discovery failed for %s", reason, name, exc_info=True
        )
        return
    if row is not None and row.get("status") == "ok":
        try:
            await _resync_live_sandboxes(user_id)
        except Exception:
            logger.warning(
                "[mcp_discovery] resync after %s discovery of %s failed",
                reason, name, exc_info=True,
            )


def schedule_catalog_discovery(
    user_id: str, name: str, *, reason: str, throttle: bool = False
) -> None:
    """Discover a catalog row in the background, then resync live sandboxes.

    Fire-and-forget: discovery is a vendor network round-trip and must not
    hold a write's response. ``throttle`` is for callers that merely noticed a
    row without a snapshot (the list route) rather than changed one: those
    kicks are rate-limited per row, while a write always gets its probe.

    A write that lands while a probe is on the wire queues one more pass
    instead of returning. The running probe is asking about the configuration
    that write just replaced, so letting its completion stand for the new one
    left the edit unprobed until a self-heal noticed, two minutes later.
    """
    key = (user_id, name)
    if (running := _in_flight.get(key)) is not None and not running.done():
        if not throttle:
            _rerun.add(key)
        return

    async def _run() -> None:
        try:
            if not await claim_probe_kick(
                user_id, name,
                throttle_s=SELF_HEAL_INTERVAL_S if throttle else None,
            ):
                return
            while True:
                # Cleared before the pass, checked after it: a kick that
                # arrives while this one is on the wire is newer than what it
                # will land, and the loop owes it another round.
                _rerun.discard(key)
                await _discovery_pass(user_id, name, reason)
                if key not in _rerun:
                    return
        finally:
            _rerun.discard(key)

    task = asyncio.create_task(_run(), name=f"mcp-discover-{name}")
    _in_flight[key] = task
    _discovery_tasks.add(task)
    task.add_done_callback(_discovery_tasks.discard)
    task.add_done_callback(lambda t: _in_flight.pop(key, None) if _in_flight.get(key) is t else None)


def schedule_post_edit_rediscovery(
    user_id: str, name: str, *, prior: dict | None, updated: dict
) -> None:
    """Re-run host-side discovery after an edit moves a row's discovery
    fingerprint.

    The user-tier snapshot serves only under the CURRENT fingerprint, and no
    other path re-discovers a remote row host-side. Without this, an edit (a
    header, a trailing slash, ``discovery_uses_secrets``) empties the row's
    tools in every workspace until something else asks. A create (no prior)
    is a fingerprint move from nothing.
    """
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    if prior is not None and mcp_discovery_fingerprint(
        user_row_to_server_config(prior)
    ) == mcp_discovery_fingerprint(user_row_to_server_config(updated)):
        return
    if updated.get("transport") != "http":
        return
    schedule_catalog_discovery(user_id, name, reason="post-edit")
