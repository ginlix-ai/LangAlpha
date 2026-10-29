"""Versioned sandbox layout migrations.

Each migration is a coroutine that transforms the sandbox filesystem from
version N to N+1.  Migrations run sequentially, in-place, before module sync.

Zero cost when current: one integer comparison against the already-downloaded
unified manifest.  No extra API calls.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable, Coroutine
from typing import Any

import structlog

from ptc_agent.core.paths import (
    COMPUTER_ROOT_ENTRIES,
    LEGACY_ROOT_CODE_DIR,
    LEGACY_ROOT_TOOLS_DIR,
    SandboxLayout,
    WorkspaceLayout,
)

logger = structlog.get_logger(__name__)

CURRENT_LAYOUT_VERSION = 4  # bump for each new layout change


class LayoutMigrationError(RuntimeError):
    """The project cannot be served until its file move finishes."""


#: The root is also the sandbox user's home; keep its runtime and shell state.
_COMPUTER_RESERVED: tuple[str, ...] = tuple(sorted(COMPUTER_ROOT_ENTRIES | {
    ".bashrc", ".bash_logout", ".bash_profile", ".bash_history", ".profile",
    ".zshrc", ".zprofile", ".zsh_history", ".ssh", ".cache", ".config",
    ".local", ".npm", ".ipython", ".python_history",
}))

#: Where the workspace memory tier used to be mirrored, at the computer root.
#: The spelling lives with the step that erases it rather than on the layout,
#: which now has exactly one name for that tier. The tree is a mirror of the
#: LangGraph store, so dropping it loses nothing the store does not hold.
_LEGACY_WORKSPACE_MEMORY_DIR = ".agents/workspace"


async def migrate_layout_v1_to_v2(
    runtime: Any, work_dir: str, *, dir_name: str | None = None,
    workspace_dir_names: tuple[str, ...] = ()
) -> None:
    """Consolidate .agent/ + skills/ → .agents/, code/ → .system/code/.

    Moves:
        .agent/threads/            → .agents/threads/
        .agent/user/               → .agents/user/
        .agent/large_tool_results/ → .agents/large_tool_results/
        code/                      → .system/code/
        skills/                    → removed (platform skills re-uploaded to .agents/skills/)

    IDEMPOTENT: each move checks source exists before moving, skips if already
    done.  Safe to re-run after partial failure.
    """
    moves = [
        (".agent/threads", ".agents/threads"),
        (".agent/user", ".agents/user"),
        (".agent/large_tool_results", ".agents/large_tool_results"),
        (LEGACY_ROOT_CODE_DIR, ".system/code"),
    ]

    # Ensure target directories exist
    await runtime.exec(
        f"mkdir -p {shlex.quote(f'{work_dir}/.agents/skills')} "
        f"{shlex.quote(f'{work_dir}/.system/code')}"
    )

    for src_rel, dst_rel in moves:
        src = f"{work_dir}/{src_rel}"
        dst = f"{work_dir}/{dst_rel}"
        cmd = (
            f"if [ -d {shlex.quote(src)} ]; then "
            f"mkdir -p {shlex.quote(dst)} && "
            f"cp -a {shlex.quote(src)}/. {shlex.quote(dst)}/ && "
            f"rm -rf {shlex.quote(src)}; "
            f"fi"
        )
        await runtime.exec(cmd)

    # Clean up old .agent/ directory if empty
    await runtime.exec(
        f"rmdir {shlex.quote(f'{work_dir}/.agent')} 2>/dev/null || true"
    )

    # Remove old top-level skills/ directory (platform skills will be
    # re-uploaded to .agents/skills/ by the sync pipeline)
    await runtime.exec(
        f"rm -rf {shlex.quote(f'{work_dir}/skills')}"
    )

    logger.info("Layout migration v1→v2 complete", work_dir=work_dir)


async def migrate_layout_v2_to_v3(
    runtime: Any, work_dir: str, *, dir_name: str | None = None,
    workspace_dir_names: tuple[str, ...] = ()
) -> None:
    """Remove legacy user-data markdown files now superseded by ``UserDataBackend``.

    Targets (idempotent, ``rm -f``):
        .agents/user/portfolio.md
        .agents/user/watchlist.md
        .agents/user/preference.md

    The agent reads ``.agents/user/profile/*.json`` through the composite
    backend (DB-backed); leaving these stale markdown files would let a Glob
    from ``.agents/user/`` surface mismatched content.
    """
    targets = [
        f"{work_dir}/.agents/user/portfolio.md",
        f"{work_dir}/.agents/user/watchlist.md",
        f"{work_dir}/.agents/user/preference.md",
    ]
    quoted = " ".join(shlex.quote(p) for p in targets)
    await runtime.exec(f"rm -f {quoted}")
    logger.info("Layout migration v2→v3 complete", work_dir=work_dir)


def build_v3_to_v4_script(
    work_dir: str, dir_name: str | None, *, workspace_dir_names: tuple[str, ...] = ()
) -> str:
    """The shell the v3 to v4 move runs, as one string.

    Split out from the step so it can be read as text: every clause is guarded
    by ``test -e``, so a re-run after a partial failure resumes rather than
    nesting a folder inside itself.
    """
    computer = SandboxLayout(work_dir)
    workspace = WorkspaceLayout(work_dir, dir_name or "")
    root_q = shlex.quote(computer.root)
    ws_q = shlex.quote(workspace.workspace)
    legacy_tools = computer.join(LEGACY_ROOT_TOOLS_DIR)
    legacy_tools_q = shlex.quote(legacy_tools)
    legacy_docs_q = shlex.quote(f"{legacy_tools}/docs")
    tools_q = shlex.quote(computer.tools)
    docs_q = shlex.quote(computer.tools_docs)
    reserved = "|".join(shlex.quote(name) for name in (*_COMPUTER_RESERVED, *workspace_dir_names))

    legacy_memory_q = shlex.quote(computer.join(_LEGACY_WORKSPACE_MEMORY_DIR))

    lines = [
        # Every clause below either moves data or deletes what it just copied,
        # so the first failure has to stop the script rather than let the next
        # clause read a tree that is not there.
        "set -e",
        # ``cp -a src && rm -rf src`` reports success when the cp fails: set -e
        # is ignored for a failing AND-OR list, so the script would carry on
        # and the step would be recorded as done. A function body is a plain
        # command, so a failure inside it does abort.
        "merge() { mkdir -p \"$2\"; cp -a \"$1/.\" \"$2/\"; rm -rf \"$1\"; }",
        f"root={root_q}",
        f"ws={ws_q}",
        'mkdir -p "$ws"',
        f"mkdir -p {tools_q} {docs_q}",
        # Docs before wrappers: the two halves of the legacy directory now have
        # different destinations, and moving the parent first would carry the
        # docs into _internal where neither the file panel nor a restore reaches.
        f"if [ -d {legacy_docs_q} ]; then merge {legacy_docs_q} {docs_q}; fi",
        # The wrappers are the one remaining root entry with a destination of
        # its own; leaving them would send them into the workspace folder with
        # everything else.
        f"if [ -d {legacy_tools_q} ]; then merge {legacy_tools_q} {tools_q}; fi",
        # The old root-level mirror of the workspace memory tier. The store is
        # the truth, so this is dropped rather than moved -- and leaving it
        # would give the tier a second spelling for a Glob to surface.
        f"rm -rf {legacy_memory_q}",
    ]

    if workspace.workspace != computer.root:
        # The restore marker is a claim about the files beside it. Left at the
        # root while they move, the folder reads as never restored and the
        # mirror is written over live edits.
        lines.append(
            'if [ -e "$root/.file_sync_marker" ] && [ ! -e "$ws/.file_sync_marker" ]; '
            'then mv "$root/.file_sync_marker" "$ws/.file_sync_marker"; fi'
        )
        lines.append(
            # Include project dotfiles without matching . or ..; preserve
            # dangling symlinks too, because they are workspace entries.
            'for src in "$root"/* "$root"/.[!.]* "$root"/..?*; do '
            'if [ ! -e "$src" ] && [ ! -L "$src" ]; then continue; fi; '
            'name=${src##*/}; '
            f'case "$name" in {reserved}) continue ;; esac; '
            'if [ "$src" = "$ws" ]; then continue; fi; '
            # Only persisted project folder names identify siblings. Ordinary
            # repositories can also contain .agents directories.
            'dst="$ws/$name"; '
            'if [ ! -e "$dst" ] && [ ! -L "$dst" ]; then mv "$src" "$dst"; '
            'elif [ -d "$src" ] && [ -d "$dst" ]; then '
            'echo "layout-v4: merged $name into the workspace folder" >&2; '
            'merge "$src" "$dst"; '
            "else "
            # The remaining shape is a root file whose name the folder already
            # holds. The folder's copy wins, and the root one is left where it
            # is rather than silently overwriting or vanishing -- but it is
            # said out loud, because the previous version of this branch did
            # neither and the leftover was invisible.
            'echo "layout-v4: kept $dst, left $src at the root" >&2; '
            "fi; "
            "done"
        )

        # Thread scratch and spilled tool results belong to the project that
        # produced them. A v3 machine holds exactly one workspace, so the root
        # copies are that workspace's and follow it into the folder, where a
        # later delete of the project takes them along.
        for relative in (
            WorkspaceLayout.THREADS_DIR,
            WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
        ):
            src = shlex.quote(computer.join(relative))
            dst = shlex.quote(workspace.join(relative))
            lines.append(f"if [ -d {src} ]; then merge {src} {dst}; fi")

    lines.append(
        "mkdir -p "
        f"{shlex.quote(workspace.skills)} "
        f"{shlex.quote(workspace.memory)} "
        f"{shlex.quote(workspace.tools)} "
        f"{shlex.quote(workspace.threads)}"
    )
    return "\n".join(lines)


async def migrate_layout_v3_to_v4(
    runtime: Any, work_dir: str, *, dir_name: str | None = None,
    workspace_dir_names: tuple[str, ...] = ()
) -> None:
    """Give the computer's own runtime a root and the workspace a folder.

    ``work/``, ``results/``, ``data/``, ``agent.md`` and anything else the
    agent left at the root move into ``<root>/<dir_name>``; the generated
    wrappers move under ``_internal`` so one copy serves every workspace. A
    computer whose workspace has no folder keeps its files where they are and
    only gains the new runtime location.
    """
    result = await runtime.exec(build_v3_to_v4_script(
        work_dir, dir_name, workspace_dir_names=workspace_dir_names
    ))
    exit_code = getattr(result, "exit_code", 0)
    stderr = (getattr(result, "stderr", "") or "").strip()
    if exit_code:
        # A half-moved layout must not be recorded as migrated: the next sync
        # is the only thing that will finish it, and it only re-runs while the
        # stamped version is still 3.
        msg = f"layout migration v3 to v4 failed (exit {exit_code}): {stderr[:500]}"
        raise RuntimeError(msg)
    logger.info(
        "Layout migration v3→v4 complete",
        work_dir=work_dir,
        dir_name=dir_name or None,
        notes=stderr[:2000] or None,
    )


# Registry: source_version → migration coroutine. Every step takes the same
# keywords so the registry can call them uniformly; only v3 to v4 reads
# ``dir_name``, because the earlier steps predate workspace folders.
LAYOUT_MIGRATIONS: dict[int, Callable[..., Coroutine]] = {
    1: migrate_layout_v1_to_v2,
    2: migrate_layout_v2_to_v3,
    3: migrate_layout_v3_to_v4,
}


async def run_layout_migrations(
    runtime: Any,
    work_dir: str,
    current_version: int,
    *,
    dir_name: str | None = None,
    workspace_dir_names: tuple[str, ...] = (),
) -> int:
    """Run pending migrations in order; returns the version actually reached.

    The last version that completed, never the target: the caller stamps this
    into the manifest, and a step that failed has to leave the stamp behind it
    so the next sync runs it again. A failed step blocks acquisition: serving
    the new project paths before the move completes hides the remaining files.
    """
    if current_version >= CURRENT_LAYOUT_VERSION:
        return current_version  # Zero cost — already current

    reached = current_version
    for v in range(current_version, CURRENT_LAYOUT_VERSION):
        migrator = LAYOUT_MIGRATIONS.get(v)
        if migrator is None:
            reached = v + 1
            continue
        logger.info(
            "Running layout migration",
            from_version=v,
            to_version=v + 1,
            work_dir=work_dir,
        )
        try:
            await migrator(runtime, work_dir, dir_name=dir_name, workspace_dir_names=workspace_dir_names)
        except Exception as exc:
            logger.error(
                "Layout migration failed; leaving the version behind it",
                from_version=v,
                to_version=v + 1,
                work_dir=work_dir,
                error=str(exc),
            )
            raise LayoutMigrationError(str(exc)) from exc
        reached = v + 1

    return reached
