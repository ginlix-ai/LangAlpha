"""The per-thread dirs a workspace keeps on the sandbox, and the prune of a
deleted thread's (and an archived thread's scratchpad) that the bring-up runs."""

import asyncio
import logging
import shlex
import time
from typing import TYPE_CHECKING, Any

from ptc_agent.core.paths import THREAD_DIR_NAME, THREAD_DIRS_SET_ASIDE, WorkspaceLayout

if TYPE_CHECKING:
    from src.server.database.conversation import ThreadPrefixes

logger = logging.getLogger(__name__)

# What the prune judges: workspace-relative dirs holding one per thread prefix.
_BASES: tuple[str, ...] = (
    WorkspaceLayout.THREADS_DIR,
    WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
    WorkspaceLayout.SCRATCHPAD_DIR,
)

#: The dir names found under each of the bases, by base.
ThreadDirListing = dict[str, set[str]]


def listing_script(layout: WorkspaceLayout) -> str:
    """The shell that lists what ``prune_dead_thread_dirs`` judges, for a
    caller to run inside a script of its own and hand back parsed.

    It first deletes whatever an earlier prune set aside and could not
    delete: no listing shows it, so no later judgment would come back to it.
    One it still cannot delete fails the listing, so no bring-up stamps the
    workspace pruned while its bytes stay on disk.
    """
    sweep = f"{_remove_set_aside(layout)} || exit 1\n"
    # Each line leads with its base, which has no space, then the dir's name.
    loops = "".join(
        f"for d in {shlex.quote(layout.join(base))}/*/; do\n"
        f'  [ -d "$d" ] && echo "{base} $(basename "$d")"\n'
        "done\n"
        for base in _BASES
    )
    return f"{sweep}{loops}true\n"


def parse_listing(stdout: str) -> ThreadDirListing:
    found: ThreadDirListing = {base: set() for base in _BASES}
    for line in stdout.splitlines():
        parts = line.split(" ")
        if len(parts) == 2 and parts[0] in found:
            found[parts[0]].add(parts[1])
    return found


async def prune_dead_thread_dirs(
    runtime: Any,
    layout: WorkspaceLayout,
    workspace_id: str,
    *,
    listing: ThreadDirListing | None = None,
) -> tuple["ThreadPrefixes", bool]:
    """Remove the dirs of deleted threads and the scratchpads of archived
    ones; return the prefixes that keep theirs and whether every dead dir is
    gone, which a bring-up stamp may claim.

    A delete or archive while the machine was down leaves its dirs behind. The
    listing runs before the thread read, so a thread created meanwhile is live
    by the time its dir could be judged; a caller passing ``listing`` ran
    ``listing_script`` before this call. A dead dir is judged again inside the
    read's fence and only moved aside there, so no run is admitted between the
    judgment and the dir leaving its path, and admission waits on renames
    rather than on a delete of any size. The caller holds the folder
    throughout.
    """
    from src.server.database.conversation import (
        fenced_workspace_thread_prefixes,
        get_workspace_thread_prefixes,
    )

    if listing is None:
        listed = await runtime.exec(listing_script(layout))
        # A failed exec or sweep: the output, empty or cut short, would read
        # as no dirs at all.
        if listed.exit_code == 0:
            listing = parse_listing(listed.stdout or "")
        else:
            logger.warning(
                f"Workspace {workspace_id}: could not list its thread dirs "
                f"(exit {listed.exit_code}): {(listed.stderr or listed.stdout or '')[:200]}"
            )
    kept = await get_workspace_thread_prefixes(workspace_id)
    if listing is None:
        return kept, False
    if not _dead(listing, kept, layout):
        return kept, True
    async with fenced_workspace_thread_prefixes(workspace_id) as kept:
        dead = _dead(listing, kept, layout)
        if not dead:
            return kept, True
        moved = await _set_aside_within_fence(runtime, layout, dead)
    # The folder this pass set aside, by the prefix they all share.
    removed = await runtime.exec(_remove_set_aside(layout))
    if moved.exit_code != 0 or removed.exit_code != 0:
        failed = moved if moved.exit_code != 0 else removed
        logger.warning(
            f"Workspace {workspace_id}: could not remove {len(dead)} dead thread "
            f"dirs (exit {failed.exit_code}): {(failed.stdout or failed.stderr or '')[:200]}"
        )
        return kept, False
    logger.info(f"Workspace {workspace_id}: removed {len(dead)} dead thread dirs")
    return kept, True


#: Renames take moments; admission to the workspace waits on them, so a
#: sandbox that hangs fails the prune rather than holding every turn.
_SET_ASIDE_TIMEOUT_S = 15
#: A timed-out exec is not always killed, and one still running after the
#: fence let go could move a dir a turn admitted since is writing in, so the
#: script makes no move past this, however late it started or stalled. The
#: gap to the timeout leaves room for the sandbox's clock to run behind the
#: server's.
_SET_ASIDE_MOVE_S = 10


#: What the set-aside prints as it exits, however it exits.
_SET_ASIDE_END = "set-aside-ended"


async def _set_aside_within_fence(runtime: Any, layout: WorkspaceLayout, dead: list[str]) -> Any:
    """Run the set-aside, and return only once it can move nothing more.

    The caller's fence has to outlast the script. An exec that raises, is
    cancelled or times out may leave it running in the sandbox, so unless
    its output shows the script ended, this waits out the timeout, by which
    the script's own deadline has passed on a sandbox clock up to the
    timeout's margin behind.
    """
    sent, moved = time.monotonic(), None
    try:
        moved = await runtime.exec(
            _set_aside_script(layout, dead, move_by=time.time() + _SET_ASIDE_MOVE_S),
            timeout=_SET_ASIDE_TIMEOUT_S,
        )
        return moved
    finally:
        if moved is None or _SET_ASIDE_END not in (moved.stdout or ""):
            await asyncio.sleep(max(0.0, sent + _SET_ASIDE_TIMEOUT_S - time.monotonic()))


def _dead(listing: ThreadDirListing, kept: "ThreadPrefixes", layout: WorkspaceLayout) -> list[str]:
    return [
        f"{layout.join(base)}/{name}"
        for base, names in listing.items()
        for name in names
        if THREAD_DIR_NAME.match(name) and name not in kept.keeps(base)
    ]


def _set_aside_script(layout: WorkspaceLayout, dead: list[str], *, move_by: float) -> str:
    """Rename every dead dir into a fresh folder beside them, one name each:
    the same thread's dirs under different bases share a name. A dir that
    fails to move fails the script and stays for the next prune, as does
    every dir the script reaches past ``move_by`` (epoch seconds)."""
    folder = f"{_set_aside(layout)}XXXXXX"
    moves = "".join(
        f'move {shlex.quote(path)} "$t/{i}" || rc=1\n' for i, path in enumerate(dead)
    )
    return (
        f"trap 'echo {_SET_ASIDE_END}' EXIT\n"
        f'move() {{ [ "$(date +%s)" -lt {int(move_by)} ] && mv -- "$1" "$2"; }}\n'
        f'[ "$(date +%s)" -lt {int(move_by)} ] || exit 1\n'
        f"t=$(mktemp -d {folder}) || exit 1\nrc=0\n{moves}exit $rc\n"
    )


def _remove_set_aside(layout: WorkspaceLayout) -> str:
    """Delete every folder a prune set aside. A dir the agent made read-only
    refuses ``rm`` to anyone but root, so a refusal opens the folders to
    their owner and tries once more."""
    folders = f"{_set_aside(layout)}*"
    return (
        f"rm -rf -- {folders} 2>/dev/null || "
        f"{{ chmod -R u+rwX -- {folders} 2>/dev/null; rm -rf -- {folders}; }}"
    )


def _set_aside(layout: WorkspaceLayout) -> str:
    """The quoted prefix of the folders a prune moves dead dirs into."""
    return shlex.quote(layout.join(THREAD_DIRS_SET_ASIDE))
