"""Host-side tool discovery for the user catalog.

Every remote (``http``/``sse``) catalog row is discovered from this process
now, not only the OAuth ones: an OAuth row sends the bearer from the token
store, any other row sends its own headers with vault refs resolved, and an
open server sends nothing. The cache row lives in ``user_mcp_tool_schemas``
either way; a schema-digest change fans out a version bump so sessions
re-resolve, while an unchanged re-discovery stays silent. A stdio row has no
host-side path (its command is the user's code) and is left to the sandbox.

Discovery runs on the writes that move a row already in service, on the
switch that puts one in service, and on the self-heal a listing kicks, so a
row's tool count and its auth verdict are known before the user's next turn
rather than after it. A row that is switched off is never dialled from here:
create, import and promote all land inert templates, and what their headers
carry reaches the address they name when the user switches the row on.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_oauth import SERVABLE, ConnectionStatus, get_connection
from src.server.database.mcp_servers import (
    bump_user_versions,
    claim_probe_kick,
    get_catalog_server,
)
from src.server.database.mcp_tool_schemas import (
    SchemaWrite,
    upsert_user_tool_schemas,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_vault_secrets import get_user_secrets_decrypted
from src.server.models.mcp_server import OK_VERDICTS, ProbeResult
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


def _claimed(server_name: str, write: SchemaWrite) -> TokenUnavailable:
    """A connection took the row over while our header probe was on the network.

    The headers are no longer what this server is dialled with, so landing
    their snapshot would serve one account's tools to a session sending the
    other account's token. Raised as the same refusal a disconnect earns: the
    connection's status is the answer either way.
    """
    reason = write.connection_status or "unknown_connection"
    logger.info(
        "[mcp_discovery] dropped a header discovery write for %s: a connection "
        "now claims the row (%s)",
        server_name, reason,
    )
    return TokenUnavailable(reason)


def _crossed_servable(write: SchemaWrite, *, now_ok: bool) -> bool:
    """Whether this write moved the row in or out of the set a grant is issued for.

    Worth a version bump on its own, because the digest cannot see it: the
    no-downgrade upsert keeps the tools, the status and the digest of a row
    that answered once and now 401s, so only ``last_probe`` moved. A warm PTC
    session re-resolves its grants only when ``mcp_config_version`` changes, so
    without the bump the direct tool and its ``header_mcp`` grant outlive the
    verdict that retired them, and a vendor-side recovery never restores them.
    """
    return (write.replaced_verdict in OK_VERDICTS) is not now_ok


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


def _reprobed(server_name: str) -> TokenUnavailable:
    """A probe claimed after ours took this row over while we were on the network.

    The fingerprint cannot see this one: it hashes ``${vault:NAME}`` refs and
    never their values, so a rotated secret leaves it identical and the probe
    that dialled with the old key would otherwise overwrite the verdict the new
    one earned. The kick that rotation scheduled owns convergence.
    """
    logger.info(
        "[mcp_discovery] dropped a stale discovery write for %s: reprobed since",
        server_name,
    )
    return TokenUnavailable("superseded", "server was reprobed during discovery")


@dataclass(frozen=True)
class _SnapshotWriter:
    """The fenced write every host-side discovery lands through.

    One transaction covers the fingerprint re-check and the write, under the
    catalog row's FOR UPDATE: a concurrent edit either commits first (this
    result is discarded as stale) or blocks until this snapshot lands and the
    edit's own rediscovery supersedes it. Without the fence, a slow discovery
    finishing after a newer one deletes the current config's snapshot and
    resurrects a dead fingerprint's. The lock is exclusive rather than shared so
    that it fences writers against each other too: the verdict this write
    replaced is what decides the fan-out, and that reading is sound only while
    no sibling probe can land between our read and our write. The snapshot row's
    own lock cannot do it, because on the first probe of a config there is no
    row there yet to lock. The row's kick clock is the second fence, for the
    move the fingerprint cannot see: a rotated vault value. The version bump
    rides in that transaction too: committed apart, a failure between the two
    lands a verdict no warm session is ever told about, and the next probe
    repeats that verdict without crossing anything, so the bump is never made
    up.
    """

    user_id: str
    server_name: str
    fingerprint: str
    connection_id: str | None
    # The ``probe_kicked_at`` this discovery claimed, for callers that claimed
    # one. None keeps the fingerprint as the only fence, which is what the
    # paths with no kick behind them (a direct refresh, the add form) get.
    claimed_at: datetime | None = None

    def _reprobed_since(self, current: dict) -> bool:
        kicked = current.get("probe_kicked_at")
        if self.claimed_at is None or kicked is None:
            return False
        return datetime.fromisoformat(kicked) > self.claimed_at

    async def write(
        self, *, probe: ProbeResult, now_ok: bool, **upsert_kwargs
    ) -> SchemaWrite:
        from src.server.services.mcp_config import user_row_to_server_config
        from src.server.services.mcp_discovery import mcp_discovery_fingerprint

        async with get_db_connection() as conn:
            async with conn.transaction():
                current = await get_catalog_server(
                    self.user_id, self.server_name, conn=conn, for_update=True
                )
                if current is None or mcp_discovery_fingerprint(
                    user_row_to_server_config(current)
                ) != self.fingerprint:
                    raise _superseded(self.server_name)
                if self._reprobed_since(current):
                    raise _reprobed(self.server_name)
                write = await upsert_user_tool_schemas(
                    self.user_id, self.server_name, self.fingerprint,
                    connection_id=self.connection_id, conn=conn,
                    last_probe=probe.model_dump(mode="json"),
                    **upsert_kwargs,
                )
                # A moved tool surface means sessions must regenerate wrappers.
                # The digest arm compares against what this write actually
                # replaced, not a snapshot read before it: two workers probing
                # the same row both read the pre-write digest, and the one that
                # lands second would see its own value there and decline the
                # bump its row needs. The crossing arm also carries the
                # vendor-side recovery, which comes back with the same tools and
                # so the same digest.
                digest = upsert_kwargs.get("schema_digest")
                if write.row is not None and (
                    _crossed_servable(write, now_ok=now_ok)
                    or (now_ok and write.replaced_digest != digest)
                ):
                    async with conn.cursor() as cur:
                        await bump_user_versions(cur, self.user_id)
        if write.row is None:
            # Refused under the write's own lock, whichever way the credential
            # moved: a disconnect already purged what we would be re-adding, or
            # a connect means these headers no longer address this server.
            raise (
                _discarded(self.server_name, write)
                if self.connection_id is not None
                else _claimed(self.server_name, write)
            )
        return write

    async def fail(self, probe: ProbeResult) -> dict:
        write = await self.write(
            probe=probe, now_ok=False, status="error", error=probe.error,
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
            now_ok=True,
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
        logger.info(
            "[mcp_discovery] discovered %d tools on %s (digest %s)",
            len(outcome.tools), self.server_name, digest[:12],
        )
        return write.row


async def refresh_user_tool_schemas(
    user_id: str, server_name: str, *, claimed_at: datetime | None = None
) -> dict:
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
        claimed_at,
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


async def discover_catalog_server(
    user_id: str, server_name: str, *, claimed_at: datetime | None = None
) -> dict | None:
    """Discover one catalog row from this process, whatever it authenticates with.

    Returns the cache row, or None for a row that has no host-side path (a
    stdio command, a legacy ``sse`` row, or a name that is not in the catalog)
    and for one the user has not switched on.
    A row any connection short of revoked claims takes the OAuth refresh, and
    its :class:`TokenUnavailable` propagates; only a row nothing claims sends
    its own headers, vault refs resolved from the user's vault, and lands
    whatever the server said as the row. A connection that appears while those
    headers are on the wire claims the row too: that write is refused under the
    lock and raises the same exception.
    """
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    row = await get_catalog_server(user_id, server_name)
    # ``http`` only: the probe dials streamable HTTP, so an ``sse`` row has no
    # path from here and keeps the sandbox's own discovery.
    if row is None or row.get("transport") != "http" or not row.get("url"):
        return None
    if not row.get("enabled") or row.get("plugin_enabled") is False:
        # An inert template is not dialled, whichever kick asked. Create,
        # import and promote all land rows switched off, and an imported row's
        # headers carry the credential the same import just wrote into the
        # vault; sending it to the address a pasted file named is the user's
        # decision, and the switch is where they make it. A disabled plugin is
        # the same switch one level up: its rows keep their own flag but are
        # withheld from every runtime, so nothing they would be probed for can
        # run, and their credential stays off the wire with them. The add
        # form's own ``POST /servers/probe`` is a different door and still
        # dials what the user typed, in front of them.
        return None
    connection = await get_connection(user_id, server_name)
    # Read the way the relay and the write-time warning read it: a connection
    # that has not been revoked claims the row whatever its state. The user was
    # told these headers are not sent while the server is OAuth-connected, so a
    # token that needs repair answers as the OAuth path does -- reconnect --
    # rather than quietly falling back to the headers.
    if connection is not None and connection.status is not ConnectionStatus.REVOKED:
        return await refresh_user_tool_schemas(
            user_id, server_name, claimed_at=claimed_at
        )

    writer = _SnapshotWriter(
        user_id, server_name,
        mcp_discovery_fingerprint(user_row_to_server_config(row)),
        None,
        claimed_at,
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
# A write's stamp, still landing, for the rerun it queued to wait on: a stamp
# that commits after the rerun's claim leaves the row's epoch newer than the
# rerun's, and fences out the very write it was sent for. Each stamp waits on
# the one it replaced, so the newest stands for every write before it.
_pending_stamp: dict[tuple[str, str], asyncio.Task] = {}
SELF_HEAL_INTERVAL_S = 120.0


async def _discovery_pass(
    user_id: str, name: str, reason: str, *, claimed_at: datetime | None = None
) -> None:
    """One discovery, plus the live-sandbox resync a changed surface owes.

    Never raises: this runs detached, so a vendor failure has to land as a row
    and a log line rather than as a task nobody awaits.
    """
    from src.server.services.mcp_oauth.connect import _resync_live_sandboxes

    try:
        row = await discover_catalog_server(user_id, name, claimed_at=claimed_at)
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


def _detach(coro, name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    _discovery_tasks.add(task)
    task.add_done_callback(_discovery_tasks.discard)
    return task


async def _stamp_probe_kick(
    user_id: str, name: str, *, after: asyncio.Task | None = None
) -> None:
    if after is not None:
        # Waited on rather than awaited: a cancelled predecessor must not take
        # this stamp down with it.
        await asyncio.wait({after})
    try:
        await claim_probe_kick(user_id, name, throttle_s=None)
    except Exception:
        logger.warning(
            "[mcp_discovery] probe kick stamp failed for %s", name, exc_info=True
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
            # The stamp lands here rather than on the rerun: the probe on the
            # wire dialled with the configuration this write just replaced, and
            # a newer epoch is what fences its verdict out before it can write.
            # A throttled kick stamps nothing, because the probe already
            # running is the answer it asked for. The rerun still claims after
            # this stamp lands, which ``_pending_stamp`` orders.
            stamp = _detach(
                _stamp_probe_kick(user_id, name, after=_pending_stamp.get(key)),
                f"mcp-kick-{name}",
            )
            _pending_stamp[key] = stamp
            stamp.add_done_callback(
                lambda t: _pending_stamp.pop(key, None)
                if _pending_stamp.get(key) is t
                else None
            )
        return

    async def _run() -> None:
        # Only the first pass carries the caller's throttle: a rerun is queued
        # by a write alone, and a write is never rate-limited.
        throttle_s = SELF_HEAL_INTERVAL_S if throttle else None
        try:
            while True:
                # Cleared before the claim, checked after the pass: a kick that
                # arrives while this one is on the wire is newer than what it
                # will land, and the loop owes it another round.
                _rerun.discard(key)
                # The write's stamp runs detached: claiming ahead of it takes
                # an epoch the stamp then overtakes, and this pass would write
                # under a fence its own kick had already moved past.
                if (stamp := _pending_stamp.get(key)) is not None:
                    await asyncio.wait({stamp})
                claimed_at = await claim_probe_kick(
                    user_id, name, throttle_s=throttle_s
                )
                # Another worker stamped a newer probe inside the window, so
                # this pass would only dial the same server again.
                if not claimed_at:
                    return
                await _discovery_pass(user_id, name, reason, claimed_at=claimed_at)
                if key not in _rerun:
                    return
                throttle_s = None
        finally:
            _rerun.discard(key)

    task = _detach(_run(), f"mcp-discover-{name}")
    _in_flight[key] = task
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
