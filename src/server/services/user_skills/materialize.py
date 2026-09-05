"""Host-side materialization of user skills + the per-turn bundle.

``resolve_user_skill_dir`` maintains a content-addressed cache view on the
host filesystem (``<root>/<user-hash>/<scope>/<view-hash>/<name>/SKILL.md``)
so the common case — nothing changed — is a single ``stat``. The view hash
covers every effective row's ``content_hash``, so any upload/delete/toggle
produces a new view dir and the stale one is GC'd. Views are namespaced per
scope (``user`` or a workspace hash) because GC removes siblings: without the
namespace, turns alternating between two workspaces would tear down each
other's views every sync. Concurrent workers racing to build the same view
converge via ``os.replace`` (the pattern assets.py/ptc_sandbox.py already
use).

``load_user_skill_bundle`` is the single entry point every caller uses: one
indexed query + one (Redis-cached) prefs read + the fast-path stat, plus two
concurrent scope reads when a ``workspace_id`` is given. With a
``workspace_id`` it resolves the workspace-effective view: workspace rows
shadow same-named user rows by name whatever their enabled state, and the
workspace's disables drop inherited skills. There is deliberately no extra
caching layer — a per-process TTL cache would be module-level state consulted
by a request path, which AGENTS.md forbids.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.server.database.account_disables import list_account_disables
from src.server.database.user_skills import (
    archive_key_unused_guard,
    get_user_skill_archive_blob,
    list_enabled_user_skills,
    list_user_skills,
    list_workspace_skill_disables,
)
from src.server.services import skill_archive_storage
from src.server.services.features import (
    get_disabled_builtin_skills,
    get_skill_command_overrides,
)
from src.server.services.user_skills.commands import effective_trigger
from src.server.services.user_skills.validate import safe_extract_archive

logger = logging.getLogger(__name__)

# Extraction scratch dirs are renamed into place on success; anything older
# than this was orphaned by a crash between mkdir and rename.
_TMP_TTL_SECONDS = 3600

# A scope dir (the user tier, or one workspace) survives its workspace, so it
# ages out instead. Long enough that an occasionally-used workspace keeps its
# view; short enough that a deleted one doesn't sit in the cache forever.
_SCOPE_TTL_SECONDS = 7 * 24 * 3600


@dataclass(frozen=True)
class UserSkillSpec:
    """What the agent build needs to know about one enabled user skill."""

    name: str
    description: str
    command: str
    # Tier marker: workspace-tier rows sort after user-tier ones in the slash
    # fold, so a workspace row wins a same-trigger collision in its workspace.
    workspace_scoped: bool = False


@dataclass(frozen=True)
class UserSkillBundle:
    """Everything the per-turn agent build consumes from the user skill tier.

    ``command_overrides`` is read-only by contract (the bundle is shared);
    consumers copy before mutating.
    """

    dir: str | None
    skills: tuple[UserSkillSpec, ...]
    disabled_builtins: frozenset[str]
    command_overrides: Mapping[str, str] = field(default_factory=dict)
    # Workspace-tier bodies live in their own view: the host still needs them
    # to answer slash commands and Flash inline delivery, but they must never
    # enter ``dir``, which is what the sandbox delivery path uploads.
    workspace_dir: str | None = None


EMPTY_USER_SKILL_BUNDLE = UserSkillBundle(
    dir=None, skills=(), disabled_builtins=frozenset()
)


def _cache_root() -> Path:
    configured = os.environ.get("USER_SKILLS_CACHE_DIR")
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "langalpha-user-skills"


def _user_dir(user_id: str) -> Path:
    # Never raw user ids on disk.
    return _cache_root() / sha256(user_id.encode()).hexdigest()[:16]


def _scope_key(workspace_id: str | None) -> str:
    if workspace_id is None:
        return "user"
    return "ws-" + sha256(workspace_id.encode()).hexdigest()[:16]


def _own_scope_key(workspace_id: str | None) -> str:
    """Scope for the workspace's OWN rows, kept apart from the delivery view
    so the two GC each other's siblings never."""
    return _scope_key(workspace_id) + "-own"


def _view_hash(rows: list[dict[str, Any]]) -> str:
    key = "\n".join(
        f"{r['name']}:{r['content_hash']}"
        for r in sorted(rows, key=lambda r: r["name"])
    )
    return sha256(key.encode()).hexdigest()[:32]


async def fetch_skill_archive(user_id: str, row: dict[str, Any]) -> bytes:
    """The canonical archive bytes for a row, from object storage or inline."""
    if row.get("archive_key"):
        return await skill_archive_storage.fetch_archive(row["archive_key"])
    blob = await get_user_skill_archive_blob(user_id, row["user_skill_id"])
    if blob is None:
        raise skill_archive_storage.SkillArchiveFetchError(
            f"skill {row['name']!r} has neither a storage key nor an inline blob"
        )
    return blob


async def drop_archive_if_unused(user_id: str, key: str | None) -> None:
    """Delete an archive object only once no row references it.

    Keys are content-addressed, so re-uploading content the user already has
    dedups onto the existing object. An unconditional delete on a failed or
    superseded write would take the other row's bytes with it.
    """
    if not key:
        return
    try:
        async with archive_key_unused_guard(key, user_id) as unused:
            if unused:
                await skill_archive_storage.delete_archive(key)
    except Exception:
        logger.exception("[user_skills] archive cleanup failed for %s", key)


def _extract_all(archives: list[tuple[str, bytes]], tmp: Path) -> set[str]:
    """Extract each archive under ``tmp``; return the names that would not.

    Per-archive, matching the fetch path: this runs inside the request path, so
    one archive the filesystem refuses would otherwise fail every turn the user
    takes for as long as the row exists. Stored members are all prefixed with
    the skill name, so a failure is contained to that one subtree.
    """
    failed: set[str] = set()
    for name, data in archives:
        try:
            safe_extract_archive(data, tmp)
            if not (tmp / name / "SKILL.md").is_file():
                raise ValueError(
                    f"archive for {name!r} did not produce {name}/SKILL.md"
                )
        except Exception:
            logger.error(
                "[user_skills] archive did not extract; skill dropped this turn "
                "(name=%s)",
                name,
                exc_info=True,
            )
            shutil.rmtree(tmp / name, ignore_errors=True)
            failed.add(name)
    return failed


def _gc_views(user_dir: Path, keep: str) -> None:
    """Drop superseded views, dead scopes, and crashed workers' staging dirs.

    A concurrent turn still reading an old view loses it mid-read only when
    the user mutated skills mid-turn — accepted; that turn's next load simply
    re-resolves. Scope dirs outlive their workspace (nothing tells this layer
    a workspace was deleted), so they age out instead: every resolve touches
    its scope, and a scope untouched for ``_SCOPE_TTL_SECONDS`` is dropped.
    """
    try:
        for sibling in user_dir.iterdir():
            if sibling.name != keep and sibling.is_dir():
                shutil.rmtree(sibling, ignore_errors=True)
    except OSError:
        pass
    _sweep(_cache_root() / ".tmp", _TMP_TTL_SECONDS)
    _sweep(user_dir.parent, _SCOPE_TTL_SECONDS, keep=user_dir.name)


def _sweep(root: Path, ttl: float, *, keep: str | None = None) -> None:
    cutoff = time.time() - ttl
    try:
        for entry in root.iterdir():
            if entry.name == keep or not entry.is_dir():
                continue
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
    except OSError:
        pass


async def resolve_user_skill_dir(
    user_id: str, rows: list[dict[str, Any]], *, scope: str = "user"
) -> tuple[str | None, list[dict[str, Any]]]:
    """Materialize the cache view for ``rows``; return ``(dir, rows_in_view)``.

    ``scope`` namespaces the view (and its sibling GC) so different scopes'
    views coexist. Returns ``(None, [])`` for an empty set so users with no
    skills cause zero manifest churn. A row whose archive can't be fetched is
    dropped with a warning rather than failing the turn; the next call
    retries it.
    """
    if not rows:
        return None, []

    user_dir = _user_dir(user_id) / scope
    view = user_dir / _view_hash(rows)
    if view.is_dir():
        # Mark the scope live: the fast path is the only signal a still-used
        # scope gives, and _gc_views ages scopes out by this timestamp.
        try:
            os.utime(user_dir)
        except OSError:
            pass
        return str(view), rows

    fetched = await asyncio.gather(
        *(fetch_skill_archive(user_id, row) for row in rows),
        return_exceptions=True,
    )
    archives: list[tuple[str, bytes]] = []
    ok_rows: list[dict[str, Any]] = []
    for row, result in zip(rows, fetched, strict=True):
        if isinstance(result, BaseException):
            if not isinstance(result, Exception):
                raise result  # cancellation is not a fetch failure
            logger.error(
                "[user_skills] archive fetch failed; skill dropped this turn "
                "(user=%s name=%s)",
                user_id,
                row["name"],
                exc_info=result,
            )
            continue
        archives.append((row["name"], result))
        ok_rows.append(row)
    if not ok_rows:
        return None, []
    if len(ok_rows) != len(rows):
        view = user_dir / _view_hash(ok_rows)
        if view.is_dir():
            return str(view), ok_rows

    tmp = _cache_root() / ".tmp" / uuid.uuid4().hex
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        failed = await asyncio.to_thread(_extract_all, archives, tmp)
        if failed:
            ok_rows = [r for r in ok_rows if r["name"] not in failed]
            if not ok_rows:
                shutil.rmtree(tmp, ignore_errors=True)
                return None, []
            # The view is content-addressed by its rows, so dropping one
            # re-addresses it.
            view = user_dir / _view_hash(ok_rows)
        user_dir.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, view)
    except OSError:
        # ENOTEMPTY/EEXIST — another worker won the race; its view is complete.
        shutil.rmtree(tmp, ignore_errors=True)
        if not view.is_dir():
            raise
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    await asyncio.to_thread(_gc_views, user_dir, view.name)
    return str(view), ok_rows


async def load_user_skill_bundle(
    user_id: str, workspace_id: str | None = None
) -> UserSkillBundle:
    """The single entry point: effective rows + disabled names + cache view.

    With a workspace, the effective set is the two-tier union with workspace
    rows shadowing same-named user rows, minus the workspace's disables of
    inherited skills. Those disables, and the skills owned by a bundle the
    user switched off, extend ``disabled_builtins``, so a
    platform skill they name drops from the registry and the sandbox upload;
    a user-tier name in the set is a no-op there (platform-only consumers)
    and takes effect through the row filter here.

    Two views, because one directory can't serve both jobs. The delivery view
    (``dir``) carries the USER tier only: workspace-tier rows are two-way
    synced by the reconciler, which is the sole writer of their sandbox dirs,
    so uploading them through the generic managed path would fight it.
    ``workspace_dir`` carries the workspace tier for host-side reads (slash
    commands, Flash inline delivery), which need every effective skill's body
    regardless of who delivers it. ``skills`` spans the full effective union.
    """
    rows = await list_enabled_user_skills(user_id, workspace_id=workspace_id)
    disabled = await get_disabled_builtin_skills(user_id)
    command_overrides = await get_skill_command_overrides(user_id)

    # A switched-off bundle subtracts the skills it ships, by name, into the
    # same set a per-skill disable writes to. Nothing downstream learns about
    # bundles: the registry, the sandbox upload and the delivery signature all
    # already key on this set, so the toggle re-syncs a warm sandbox for free.
    if bundle_names := (await list_account_disables(user_id)).bundles:
        # Local: services.plugins imports this module for the export path.
        # Enforcement, not a listing: this subtracts skills, so it has to read
        # the map taken when the running set was composed. A live re-read lets
        # a bundle renamed after boot answer for nothing, which hands the user
        # back skills they switched off while the registry still carries them.
        from src.server.services.plugins.bundled import enforcement_owners

        _, owned = enforcement_owners().owned_by(bundle_names)
        disabled = disabled | owned

    scope = "user"
    own: list[dict[str, Any]] = []
    if workspace_id is not None:
        # Shadowing is by NAME, not by enabled state. A disabled workspace row
        # still owns its name here: otherwise disabling it promotes the
        # inherited user-tier row into the delivery view, the asset sync writes
        # those bytes over the reconciler-owned dir (breaking the invariant
        # merge_lock_files documents), and the next pass CASes them back over
        # the workspace row's stored content. Disabling a workspace skill turns
        # that name off in the workspace, which is what the management list
        # already shows. Reading the unfiltered scope is a second query, so it
        # rides alongside the disables read rather than after it.
        ws_disabled, all_ws_rows = await asyncio.gather(
            list_workspace_skill_disables(workspace_id),
            list_user_skills(user_id, workspace_id=workspace_id),
        )
        if ws_disabled:
            disabled = disabled | ws_disabled
        ws_names = {r["name"] for r in all_ws_rows}
        effective = [
            r
            for r in rows
            if r["workspace_id"]
            or (r["name"] not in ws_names and r["name"] not in ws_disabled)
        ]
        physical = [r for r in effective if not r["workspace_id"]]
        own = [r for r in effective if r["workspace_id"]]
        # Reuse the plain user view (and its GC namespace) when the physical
        # view is identical to it — workspace rows never enter the physical
        # view, so only shadowing or a disable of a user-tier name forks it.
        if len(physical) != sum(1 for r in rows if not r["workspace_id"]):
            scope = _scope_key(workspace_id)
    else:
        effective = rows
        physical = rows

    (skill_dir, ok_rows), (ws_dir, ok_own) = await asyncio.gather(
        resolve_user_skill_dir(user_id, physical, scope=scope),
        resolve_user_skill_dir(user_id, own, scope=_own_scope_key(workspace_id)),
    )
    # A row whose archive can't be fetched has no body to read, so it drops
    # from the manifest and the slash menu too rather than advertising a
    # trigger that would resolve to nothing.
    ok_names = {r["name"] for r in (*ok_rows, *ok_own)}
    spec_rows = [r for r in effective if r["name"] in ok_names]
    return UserSkillBundle(
        dir=skill_dir,
        workspace_dir=ws_dir,
        skills=tuple(
            UserSkillSpec(
                name=r["name"],
                description=r["description"],
                command=effective_trigger(r),
                workspace_scoped=bool(r["workspace_id"]),
            )
            for r in spec_rows
        ),
        disabled_builtins=disabled,
        command_overrides=command_overrides,
    )


def skills_delivery_signature(
    user_skill_dir: str | None, disabled_skills: frozenset[str] | set[str]
) -> str:
    """One value that moves iff the sandbox's skill delivery inputs move.

    ``user_skill_dir`` is already content-addressed (the view hash is in the
    path), so the pair (dir, disabled names) covers everything the asset sync
    consumes for skills. The chat handlers compute this from the turn's
    already-loaded bundle and pass it into the session acquire, which compares
    it against ``Session.skills_signature`` and re-syncs on mismatch — the
    warm-path convergence trigger, costing zero extra reads.
    """
    payload = f"{user_skill_dir or ''}\n{','.join(sorted(disabled_skills))}"
    return sha256(payload.encode()).hexdigest()


async def sandbox_skill_sync_params(
    user_id: str | None,
    sandbox_skills_base: str,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Per-user kwargs for ``sync_sandbox_assets`` (``user_skill_dir`` +
    ``disabled_skills``), empty for anonymous callers so sites can splat it
    unconditionally."""
    if not user_id:
        return {}
    bundle = await load_user_skill_bundle(user_id, workspace_id)
    params: dict[str, Any] = {}
    if bundle.disabled_builtins:
        params["disabled_skills"] = bundle.disabled_builtins
    if bundle.dir:
        params["user_skill_dir"] = (bundle.dir, sandbox_skills_base)
    return params
