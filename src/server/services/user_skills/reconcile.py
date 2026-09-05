"""Two-way reconciler between a workspace sandbox and its ``user_skills`` rows.

Git model: the sandbox skill dir is the working copy, the workspace-tier DB row
is the remote, and the ledger entry's ``sync`` block is the tracking ref. Each
side is compared only against its own recorded base (``syncedTreeHash`` for the
tree, ``syncedDbHash`` for the row), so drift is detected without any
cross-environment hash. One pass runs under the workspace's SKILL_SYNC advisory
lock: a single sandbox report, per-name decisions (pull-up, push-down,
import, arbitrate, delete), then one batched apply exec. Every crash window
converges on the next pass — the apply script writes the ledger once at the
end, so a mid-apply death leaves either the old ledger (state re-detected) or
the full new one.

Invariants: content beats deletion (a dirty tree survives its row's deletion
by re-import; a dirty row survives its dir's deletion by re-push); conflicts
resolve sandbox-wins, and where object storage backs the archive the displaced
row content is retained under its own content-addressed key (a deployment
storing archives inline has nowhere to keep it, so the losing side is gone and
the conflict log says so); equal content on both sides heals a stale tracking
ref without data movement.
Deterministic failures (validation, reserved names, transfer bounds) are
recorded as ``lastFailedSync`` keyed on the exact (treeHash, dbHash) state so
they retry only when either side actually changes. A failure whose cause lies
outside that pair -- the per-user caps, a sibling's alias, whether object
storage is configured -- is never suppressed: the fingerprint could not see
the remedy, so suppressing it would wedge the skill permanently.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.middleware.skills.lock import MANAGED_SOURCE_TYPE
from ptc_agent.core.sandbox import skill_sync
from ptc_agent.core.sandbox.skill_sync import SkillSyncError

from src.server.database.user_skills import (
    SkillSyncLockBusy,
    create_user_skill,
    delete_user_skill_cas,
    get_user_skill_by_id,
    list_user_skills,
    update_user_skill_content_cas,
    workspace_skill_sync_lock,
)
from src.server.services import skill_archive_storage
from src.server.services.features import get_skill_command_overrides
from src.server.services.user_skills.commands import free_seed
from src.server.services.user_skills.limits import (
    MAX_SKILL_FILES,
    MAX_SKILL_INLINE_BLOB_BYTES,
    MAX_SKILL_SINGLE_FILE_BYTES,
    MAX_SKILL_UNCOMPRESSED_BYTES,
)
from src.server.services.user_skills.materialize import (
    drop_archive_if_unused,
    fetch_skill_archive,
)
from src.server.services.user_skills.validate import (
    SkillValidationError,
    ValidatedSkill,
    archive_file_pairs,
    reserved_skill_names,
    validate_skill_archive,
)

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = logging.getLogger(__name__)

# download_tree failure codes that depend only on the tree's content, so a
# retry without a tree change can never succeed and suppression is safe.
# Stamped by the sandbox script's fail(); carried on SkillSyncError.code.
_DETERMINISTIC_DOWNLOAD = frozenset(
    {"too_many_files", "tree_too_large", "file_too_large", "unsyncable"}
)

# A pass is bounded by the caps it enforces, so overrunning this means the
# sandbox stopped answering rather than that the tree got big. Generous enough
# for a full-cap tree over a slow link; the point is that the advisory lock is
# released instead of pinning the workspace behind a dead sandbox.
RECONCILE_TIMEOUT_SECONDS = 120


@dataclass
class ReconcileStats:
    """One pass's outcome counts, for the summary log line and tests."""

    pulled: int = 0
    pushed: int = 0
    imported: int = 0
    adopted: int = 0
    healed: int = 0
    conflicts: int = 0
    row_deletes: int = 0
    dir_deletes: int = 0
    skipped: int = 0
    failures: int = 0
    drifts: int = 0

    @property
    def changed(self) -> bool:
        return bool(
            self.pulled
            or self.pushed
            or self.imported
            or self.adopted
            or self.healed
            or self.conflicts
            or self.row_deletes
            or self.dir_deletes
        )


class _SyncFailure(Exception):
    """A per-skill step failed; ``suppress`` marks deterministic failures that
    should not retry until either side's content changes."""

    def __init__(self, kind: str, reason: str, *, suppress: bool):
        super().__init__(reason)
        self.kind = kind
        self.reason = reason
        self.suppress = suppress


@dataclass
class _Pass:
    sandbox: "PTCSandbox"
    user_id: str
    workspace_id: str
    report: dict[str, Any]
    ws_rows: dict[str, dict[str, Any]]
    user_rows: dict[str, dict[str, Any]]
    stats: ReconcileStats = field(default_factory=ReconcileStats)
    actions: list[dict[str, Any]] = field(default_factory=list)


async def reconcile_workspace_skills(
    sandbox: "PTCSandbox | None",
    *,
    user_id: str,
    workspace_id: str,
    source: str = "",
) -> ReconcileStats | None:
    """Run one reconcile pass; never raises — callers are turn/bringup paths
    that must not fail on sync trouble. Returns None when the sandbox isn't
    usable or the pass aborted before producing a report."""
    if sandbox is None or getattr(sandbox, "runtime", None) is None:
        return None
    try:
        async with workspace_skill_sync_lock(workspace_id):
            # Inside the lock on purpose. wait_for cancels only the inner
            # coroutine, so the lock's finally still runs its unlock on a live,
            # uncancelled task; a timeout wrapped around the whole block would
            # deliver the cancellation into that unlock and leave the advisory
            # lock held until the pooled connection dies, which is how one hung
            # sandbox makes a workspace permanently un-reconcilable.
            return await asyncio.wait_for(
                _run_pass(sandbox, user_id, workspace_id, source),
                timeout=RECONCILE_TIMEOUT_SECONDS,
            )
    except TimeoutError:
        logger.warning(
            "[skill_sync] pass timed out after %ss (ws=%s source=%s)",
            RECONCILE_TIMEOUT_SECONDS,
            workspace_id,
            source,
        )
        return None
    except SkillSyncLockBusy:
        # Another worker is mid-pass on this workspace; it sees the same state.
        logger.debug(
            "[skill_sync] pass skipped, lock held elsewhere (ws=%s source=%s)",
            workspace_id,
            source,
        )
        return None
    except Exception:
        logger.exception(
            "[skill_sync] reconcile pass failed (ws=%s source=%s)",
            workspace_id,
            source,
        )
        return None


async def _run_pass(
    sandbox: "PTCSandbox", user_id: str, workspace_id: str, source: str
) -> ReconcileStats:
    report = await skill_sync.report(sandbox)
    ws_rows = {
        r["name"]: r
        for r in await list_user_skills(user_id, workspace_id=workspace_id)
    }
    user_rows = {r["name"]: r for r in await list_user_skills(user_id)}
    ctx = _Pass(
        sandbox=sandbox,
        user_id=user_id,
        workspace_id=workspace_id,
        report=report,
        ws_rows=ws_rows,
        user_rows=user_rows,
    )

    for name in sorted(set(report) | set(ws_rows)):
        try:
            await _decide(ctx, name)
        except _SyncFailure as f:
            ctx.stats.failures += 1
            logger.warning(
                "[skill_sync] %s failed for %r (ws=%s): %s",
                f.kind,
                name,
                workspace_id,
                f.reason,
            )
            if f.suppress:
                _record_failure(ctx, name, f)
        except Exception:
            ctx.stats.failures += 1
            logger.exception(
                "[skill_sync] reconcile failed for %r (ws=%s)", name, workspace_id
            )

    if ctx.actions:
        results = await skill_sync.apply_actions(sandbox, ctx.actions)
        for res in results:
            if res.get("ok"):
                continue
            if res.get("drift"):
                # The agent wrote between report and apply; the guarded op
                # aborted and the next pass re-decides from fresh state.
                ctx.stats.drifts += 1
                logger.info(
                    "[skill_sync] drift aborted %s on %r (ws=%s)",
                    res.get("op"),
                    res.get("name"),
                    workspace_id,
                )
            else:
                ctx.stats.failures += 1
                logger.warning(
                    "[skill_sync] apply op %s failed on %r (ws=%s): %s",
                    res.get("op"),
                    res.get("name"),
                    workspace_id,
                    res.get("error"),
                )

    stats = ctx.stats
    if stats.changed or stats.failures or stats.drifts:
        logger.info(
            "[skill_sync] reconcile ws=%s source=%s pulled=%d pushed=%d "
            "imported=%d adopted=%d healed=%d conflicts=%d row_deletes=%d "
            "dir_deletes=%d skipped=%d failures=%d drifts=%d",
            workspace_id,
            source,
            stats.pulled,
            stats.pushed,
            stats.imported,
            stats.adopted,
            stats.healed,
            stats.conflicts,
            stats.row_deletes,
            stats.dir_deletes,
            stats.skipped,
            stats.failures,
            stats.drifts,
        )
    return stats


# --- Decision matrix ---


async def _decide(ctx: _Pass, name: str) -> None:
    rep = ctx.report.get(name) or {"present": False, "entry": None}
    entry = rep.get("entry")
    row = ctx.ws_rows.get(name)

    if entry and entry.get("owner") == "platform":
        return  # platform skills belong to the generic delivery path

    sync = (entry or {}).get("sync") or {}
    if row is not None and not row["enabled"]:
        await _withdraw(ctx, name, rep, entry, sync, row)
        return

    if sync.get("linkedSkillId"):
        await _decide_linked(ctx, name, rep, sync, row)
    elif entry and entry.get("sourceType") == MANAGED_SOURCE_TYPE:
        await _decide_managed(ctx, name, rep, row)
    elif rep.get("present"):
        await _decide_unlinked_dir(ctx, name, rep, row)
    elif entry is not None:
        # Agent-installed entry whose dir is gone — same cleanup the old
        # lock sync performed.
        ctx.actions.append({"op": "remove_entry", "name": name})
    elif row is not None:
        # Row with no sandbox trace at all: never delivered here. A deletion
        # would have left a linked entry behind, so absence of one is the
        # difference between "push it" and "propagate a deletion".
        await _push_down(ctx, name, rep, row)
        ctx.stats.pushed += 1


async def _decide_linked(
    ctx: _Pass,
    name: str,
    rep: dict[str, Any],
    sync: dict[str, Any],
    row: dict[str, Any] | None,
) -> None:
    skill_id = sync["linkedSkillId"]
    present = bool(rep.get("present"))

    if row is not None and row["user_skill_id"] != skill_id:
        # The name maps to a different row now (deleted and re-created via
        # the API while this sandbox slept). Re-link through the arbiter.
        if present:
            if not rep.get("syncable"):
                ctx.stats.skipped += 1
                return
            if _suppressed(rep, row):
                ctx.stats.skipped += 1
                return
            await _pull_up(ctx, name, row, conflict=True)
        else:
            await _push_down(ctx, name, rep, row)
            ctx.stats.pushed += 1
        return

    if present and row is not None:
        if not rep.get("syncable"):
            logger.info(
                "[skill_sync] %r unsyncable, skipped (ws=%s): %s",
                name,
                ctx.workspace_id,
                rep.get("reason"),
            )
            ctx.stats.skipped += 1
            return
        tree_dirty = rep.get("treeHash") != sync.get("syncedTreeHash")
        db_dirty = row["content_hash"] != sync.get("syncedDbHash")
        if not tree_dirty and not db_dirty:
            return
        if _suppressed(rep, row):
            ctx.stats.skipped += 1
            return
        if tree_dirty and not db_dirty:
            await _pull_up(ctx, name, row, conflict=False)
        elif db_dirty and not tree_dirty:
            await _push_down(ctx, name, rep, row)
            ctx.stats.pushed += 1
        else:
            # Both moved: the arbiter compares actual content — equal hashes
            # heal the ref; a true conflict resolves sandbox-wins.
            await _pull_up(ctx, name, row, conflict=True)
        return

    if present:
        # Dir exists, row-by-name gone. The row UUID tells a move from a
        # deletion.
        moved = await get_user_skill_by_id(ctx.user_id, skill_id)
        if (
            moved is not None
            and moved["name"] == name
            and moved["workspace_id"] is None
        ):
            # Promoted to the user tier: the generic managed path owns the
            # bytes from here on. Keep the files, drop the link.
            if rep.get("treeHash") != sync.get("syncedTreeHash"):
                logger.warning(
                    "[skill_sync] unpulled sandbox edits on %r superseded by "
                    "its move to the user tier (ws=%s)",
                    name,
                    ctx.workspace_id,
                )
            ctx.actions.append({"op": "update_sync", "name": name, "sync": None})
            ctx.stats.healed += 1
            return
        # Moved to another workspace, renamed, or deleted: this copy is
        # orphaned here.
        if not rep.get("syncable"):
            # Can't round-trip special files; keep the content as an
            # agent-installed entry so nothing ever prunes it.
            ctx.actions.append(
                {"op": "set_entry", "name": name, "entry": _local_entry(name, rep)}
            )
            return
        if rep.get("treeHash") != sync.get("syncedTreeHash"):
            # Content beats deletion: the edited tree survives as a new row.
            if _suppressed(rep, None):
                ctx.stats.skipped += 1
                return
            await _import_new(ctx, name)
        else:
            ctx.actions.append(
                {
                    "op": "delete_dir",
                    "name": name,
                    "expectTreeHash": sync.get("syncedTreeHash"),
                }
            )
            ctx.stats.dir_deletes += 1
        return

    # Dir gone, entry remains.
    if row is not None:
        if row["content_hash"] != sync.get("syncedDbHash"):
            # The row moved since last sync: dirty survivor wins, re-deliver.
            await _push_down(ctx, name, rep, row)
            ctx.stats.pushed += 1
        else:
            deleted = await delete_user_skill_cas(
                ctx.user_id, skill_id, sync.get("syncedDbHash") or ""
            )
            if deleted is not None:
                ctx.stats.row_deletes += 1
                await drop_archive_if_unused(ctx.user_id, deleted.get("archive_key"))
            # CAS-lost means the row changed concurrently; removing the entry
            # is still right — next pass sees row-without-trace and pushes.
            ctx.actions.append({"op": "remove_entry", "name": name})
    else:
        ctx.actions.append({"op": "remove_entry", "name": name})


async def _withdraw(
    ctx: _Pass,
    name: str,
    rep: dict[str, Any],
    entry: dict[str, Any] | None,
    sync: dict[str, Any],
    row: dict[str, Any],
) -> None:
    """A disabled row keeps its content but must not stay on disk.

    Only the sandbox copy goes; the row survives and re-enabling re-pushes it.
    Unpulled edits are pulled up first so disabling never destroys work — the
    dir is removed on the pass after, once it is clean.
    """
    if not rep.get("present"):
        if entry is not None:
            ctx.actions.append({"op": "remove_entry", "name": name})
        return
    if not rep.get("syncable"):
        ctx.stats.skipped += 1
        return
    synced = sync.get("syncedTreeHash")
    if rep.get("treeHash") != synced:
        if _suppressed(rep, row):
            ctx.stats.skipped += 1
            return
        # No tracking ref means the dir was never this row's copy, so the
        # displaced row content is retained the way any conflict is.
        await _pull_up(ctx, name, row, conflict=synced is None)
        return
    ctx.actions.append({"op": "delete_dir", "name": name, "expectTreeHash": synced})
    ctx.stats.dir_deletes += 1


async def _decide_managed(
    ctx: _Pass, name: str, rep: dict[str, Any], row: dict[str, Any] | None
) -> None:
    if row is None:
        # User-tier delivery or an orphan — the generic managed path owns
        # both (delivery for the former, prune for the latter).
        return
    # A pre-two-way-sync managed delivery of a workspace row: adopt it by
    # re-pushing the DB bytes with a link. Managed bytes are server-owned, so
    # any difference is delivery lag, not an agent edit — DB wins, and one
    # redundant push per adoption buys never having to diff here.
    await _push_down(ctx, name, rep, row)
    ctx.stats.adopted += 1


async def _decide_unlinked_dir(
    ctx: _Pass, name: str, rep: dict[str, Any], row: dict[str, Any] | None
) -> None:
    entry = rep.get("entry")
    if not rep.get("syncable") or not rep.get("wellFormed"):
        # Not importable — keep it visible in the ledger exactly like the old
        # lock sync did, and leave the files alone.
        if entry is None:
            ctx.actions.append(
                {"op": "set_entry", "name": name, "entry": _local_entry(name, rep)}
            )
        ctx.stats.skipped += 1
        return
    if _suppressed(rep, row):
        ctx.stats.skipped += 1
        return
    if row is not None:
        # Same name already lives as a workspace row: equal content adopts the
        # link, differing content is a conflict (sandbox wins).
        await _pull_up(ctx, name, row, conflict=True)
        return
    user_row = ctx.user_rows.get(name)
    if user_row is not None:
        await _absorb_user_shadow(ctx, name, user_row)
        return
    await _import_new(ctx, name)


# --- Operations ---


async def _pull_up(
    ctx: _Pass, name: str, row: dict[str, Any], *, conflict: bool
) -> None:
    """Sandbox → DB: validate the tree and CAS it over the observed row
    content. ``conflict`` marks the arbiter path, where the displaced row
    content is deliberately retained in object storage."""
    validated, tree_hash = await _download_validated(ctx, name)
    link_id = row["user_skill_id"]
    stamp = {
        "op": "update_sync",
        "name": name,
        "expectTreeHash": tree_hash,
        "sync": {
            "linkedSkillId": link_id,
            "syncedTreeHash": tree_hash,
            "syncedDbHash": validated.content_hash,
        },
    }
    if validated.content_hash == row["content_hash"]:
        # Both sides already agree; only the tracking ref was stale.
        ctx.stats.healed += 1
        ctx.actions.append(stamp)
        return

    key, blob = await _store(ctx, validated)
    try:
        updated, superseded = await update_user_skill_content_cas(
            ctx.user_id,
            link_id,
            row["content_hash"],
            description=validated.description,
            license=validated.license,
            frontmatter=validated.frontmatter,
            allowed_tools=validated.allowed_tools,
            content_hash=validated.content_hash,
            archive_key=key,
            archive_blob=blob,
            archive_bytes=len(validated.canonical_zip),
            file_count=validated.file_count,
        )
    except ValueError as e:
        # Not suppressible: every ValueError the writers raise reads other
        # rows (the per-user caps, a sibling's alias), so the fingerprint --
        # this tree's hash and this row's hash -- cannot see the remedy. Freeing
        # quota would otherwise leave the skill wedged for the life of the
        # sandbox, silently, with the cap message telling the user to do the
        # one thing that does not help.
        await drop_archive_if_unused(ctx.user_id, key)
        raise _SyncFailure("caps", str(e), suppress=False) from e
    except BaseException:
        await drop_archive_if_unused(ctx.user_id, key)
        raise
    if updated is None:
        # CAS lost — the row changed under us; next pass re-decides.
        await drop_archive_if_unused(ctx.user_id, key)
        return
    if conflict:
        ctx.stats.conflicts += 1
        if superseded:
            logger.warning(
                "[skill_sync] conflict on %r (ws=%s): sandbox content kept, "
                "displaced row content retained at %s",
                name,
                ctx.workspace_id,
                superseded,
            )
        else:
            # No object storage: the row carried its bytes inline and the CAS
            # overwrote them in place, so there is nothing left to point at.
            logger.warning(
                "[skill_sync] conflict on %r (ws=%s): sandbox content kept, "
                "displaced row content was inline and is gone",
                name,
                ctx.workspace_id,
            )
    else:
        ctx.stats.pulled += 1
        if superseded:
            await drop_archive_if_unused(ctx.user_id, superseded)
    ctx.actions.append(stamp)


async def _push_down(
    ctx: _Pass, name: str, rep: dict[str, Any], row: dict[str, Any]
) -> None:
    """DB → sandbox: stage the row's archive beside the live dir and swap it
    in atomically. Callers count the stat (pushed/adopted).

    The swap carries the state ``rep`` observed, so an agent that creates or
    edits the dir between report and apply aborts the swap instead of losing
    the write.
    """
    try:
        raw = await fetch_skill_archive(ctx.user_id, row)
    except Exception as e:
        raise _SyncFailure("fetch", str(e), suppress=False) from e
    try:
        pairs = await asyncio.to_thread(archive_file_pairs, raw)
    except SkillValidationError as e:
        raise _SyncFailure("unpack", str(e), suppress=True) from e
    staged = await skill_sync.stage_skill_files(ctx.sandbox, pairs)
    entry = _entry_from_row(row, pairs)
    entry["sync"] = {
        "linkedSkillId": row["user_skill_id"],
        "syncedDbHash": row["content_hash"],
        # syncedTreeHash + statCache are stamped by the swap op itself, from
        # the exact tree it just renamed into place.
    }
    action = {"op": "swap_staged", "name": name, "staged": staged, "entry": entry}
    if not rep.get("present"):
        action["expectAbsent"] = True
    elif rep.get("treeHash"):
        action["expectTreeHash"] = rep["treeHash"]
    # A present-but-unsyncable dir gets no guard: the only caller that reaches
    # it is the managed adoption below, where the bytes are server-owned and
    # DB-wins is the rule.
    ctx.actions.append(action)


async def _import_new(ctx: _Pass, name: str) -> None:
    """Sandbox-only tree → new workspace row (auto-import)."""
    if name in reserved_skill_names():
        raise _SyncFailure(
            "reserved", "name is reserved by a platform skill", suppress=True
        )
    validated, tree_hash = await _download_validated(ctx, name)
    await _create_linked_row(ctx, name, validated, tree_hash)


async def _absorb_user_shadow(
    ctx: _Pass, name: str, user_row: dict[str, Any]
) -> None:
    """An unlinked dir whose name matches a user-tier row: equal content means
    it's simply that row's delivered copy (relink managed); different content
    becomes a workspace shadow row so the sandbox's version wins here."""
    validated, tree_hash = await _download_validated(ctx, name)
    if validated.content_hash == user_row["content_hash"]:
        ctx.actions.append(
            {
                "op": "set_entry",
                "name": name,
                "entry": _entry_from_validated(name, validated),
            }
        )
        ctx.stats.healed += 1
        return
    await _create_linked_row(ctx, name, validated, tree_hash)


async def _create_linked_row(
    ctx: _Pass, name: str, validated: ValidatedSkill, tree_hash: str
) -> None:
    command = None
    if validated.command:
        # Frontmatter seed on sandbox import: same free_seed policy as upload.
        command = free_seed(
            validated,
            [*ctx.ws_rows.values(), *ctx.user_rows.values()],
            await get_skill_command_overrides(ctx.user_id),
        )
    key, blob = await _store(ctx, validated)
    try:
        row = await create_user_skill(
            ctx.user_id,
            name,
            workspace_id=ctx.workspace_id,
            command=command,
            description=validated.description,
            license=validated.license,
            frontmatter=validated.frontmatter,
            allowed_tools=validated.allowed_tools,
            confirmed=True,
            content_hash=validated.content_hash,
            archive_key=key,
            archive_blob=blob,
            archive_bytes=len(validated.canonical_zip),
            file_count=validated.file_count,
        )
    except ValueError as e:
        # Caps, or a name that collides with a sibling row's slash alias --
        # both about other rows, so not suppressible for the same reason as
        # the pull-up path above.
        await drop_archive_if_unused(ctx.user_id, key)
        raise _SyncFailure("insert", str(e), suppress=False) from e
    except BaseException:
        await drop_archive_if_unused(ctx.user_id, key)
        raise
    if row is None:
        # Name got taken between our list and the insert; next pass sees the
        # new row and arbitrates.
        await drop_archive_if_unused(ctx.user_id, key)
        return
    # Later names in this same pass seed their trigger against ctx.ws_rows.
    # Without this the snapshot is stale, so two sandbox skills declaring one
    # `command:` both seed it and the second insert dies on the command index
    # instead of installing name-triggered the way free_seed promises.
    ctx.ws_rows[name] = row
    ctx.stats.imported += 1
    entry = _entry_from_validated(name, validated)
    entry["sync"] = {
        "linkedSkillId": row["user_skill_id"],
        "syncedTreeHash": tree_hash,
        "syncedDbHash": validated.content_hash,
    }
    ctx.actions.append({"op": "set_entry", "name": name, "entry": entry})


# --- Helpers ---


async def _download_validated(
    ctx: _Pass, name: str
) -> tuple[ValidatedSkill, str]:
    try:
        raw, tree_hash = await skill_sync.download_tree(
            ctx.sandbox,
            name,
            max_files=MAX_SKILL_FILES,
            max_file_bytes=MAX_SKILL_SINGLE_FILE_BYTES,
            max_total_bytes=MAX_SKILL_UNCOMPRESSED_BYTES,
        )
    except SkillSyncError as e:
        deterministic = e.code in _DETERMINISTIC_DOWNLOAD
        raise _SyncFailure("download", str(e), suppress=deterministic) from e
    try:
        validated = await asyncio.to_thread(validate_skill_archive, raw)
    except SkillValidationError as e:
        raise _SyncFailure("validate", str(e), suppress=True) from e
    return validated, tree_hash


async def _store(
    ctx: _Pass, validated: ValidatedSkill
) -> tuple[str | None, bytes | None]:
    """Same storage posture as the upload endpoint: object storage when
    configured, bounded inline blob otherwise."""
    key = None
    if skill_archive_storage.is_configured():
        try:
            key = await skill_archive_storage.store_archive(
                user_id=ctx.user_id,
                content=validated.canonical_zip,
                content_hash=validated.content_hash,
            )
        except skill_archive_storage.SkillArchiveStorageError as e:
            raise _SyncFailure("store", str(e), suppress=False) from e
    if key is not None:
        return key, None
    if len(validated.canonical_zip) > MAX_SKILL_INLINE_BLOB_BYTES:
        # Also not suppressible: the blocker is whether object storage is
        # configured, which no fingerprint over the tree and the row can see.
        raise _SyncFailure(
            "store",
            "archive exceeds the inline fallback limit and object storage "
            "is unavailable",
            suppress=False,
        )
    return None, validated.canonical_zip


def _suppressed(rep: dict[str, Any], row: dict[str, Any] | None) -> bool:
    lf = ((rep.get("entry") or {}).get("sync") or {}).get("lastFailedSync")
    if not lf:
        return False
    return lf.get("treeHash") == rep.get("treeHash") and lf.get("dbHash") == (
        (row or {}).get("content_hash")
    )


def _record_failure(ctx: _Pass, name: str, f: _SyncFailure) -> None:
    rep = ctx.report.get(name) or {}
    row = ctx.ws_rows.get(name)
    lf = {
        "treeHash": rep.get("treeHash"),
        "dbHash": (row or {}).get("content_hash"),
        "kind": f.kind,
        "reason": f.reason[:200],
    }
    if rep.get("entry") is not None:
        new_sync = dict(rep["entry"].get("sync") or {})
        new_sync["lastFailedSync"] = lf
        ctx.actions.append({"op": "update_sync", "name": name, "sync": new_sync})
    elif rep.get("present"):
        entry = _local_entry(name, rep)
        entry["sync"] = {"lastFailedSync": lf}
        ctx.actions.append({"op": "set_entry", "name": name, "entry": entry})


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entry_base(
    name: str,
    *,
    description: str,
    license: str | None,
    frontmatter: dict[str, Any],
    allowed_tools: list[str],
    confirmed: bool,
    skill_md: bytes,
) -> dict[str, Any]:
    now = _now()
    return {
        "name": name,
        "description": description,
        "owner": "user",
        "source": f"user:{name}",
        "sourceType": MANAGED_SOURCE_TYPE,
        "computedHash": "sha256:" + sha256(skill_md).hexdigest(),
        "confirmed": confirmed,
        "license": license,
        "metadata": dict(frontmatter.get("metadata") or {}),
        "allowed_tools": list(allowed_tools),
        "installedAt": now,
        "updatedAt": now,
    }


def _entry_from_validated(name: str, validated: ValidatedSkill) -> dict[str, Any]:
    return _entry_base(
        name,
        description=validated.description,
        license=validated.license,
        frontmatter=validated.frontmatter,
        allowed_tools=validated.allowed_tools,
        confirmed=True,
        skill_md=validated.skill_md.encode(),
    )


def _entry_from_row(
    row: dict[str, Any], pairs: list[tuple[str, bytes]]
) -> dict[str, Any]:
    skill_md = next((c for rel, c in pairs if rel == "SKILL.md"), b"")
    return _entry_base(
        row["name"],
        description=row.get("description") or "",
        license=row.get("license"),
        frontmatter=row.get("frontmatter") or {},
        allowed_tools=row.get("allowed_tools") or [],
        confirmed=bool(row.get("confirmed")),
        skill_md=skill_md,
    )


def _local_entry(name: str, rep: dict[str, Any]) -> dict[str, Any]:
    """The old lock-sync shape for a sandbox-only skill the reconciler can't
    or won't import: agent-installed, so prune never touches it."""
    fm = rep.get("frontmatter") or {}
    description = fm.get("description") or ""
    now = _now()
    return {
        "name": name,
        "description": description,
        "owner": "user",
        "source": "local",
        "sourceType": "local",
        "computedHash": "",
        "confirmed": bool(description),
        "license": None,
        "metadata": {},
        "allowed_tools": [],
        "installedAt": now,
        "updatedAt": now,
    }
