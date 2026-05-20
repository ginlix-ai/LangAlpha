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

logger = structlog.get_logger(__name__)

CURRENT_LAYOUT_VERSION = 3  # bump for each new layout change


async def migrate_layout_v1_to_v2(runtime: Any, work_dir: str) -> None:
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
        ("code", ".system/code"),
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


async def migrate_layout_v2_to_v3(runtime: Any, work_dir: str) -> None:
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


# Registry: source_version → migration coroutine
LAYOUT_MIGRATIONS: dict[int, Callable[..., Coroutine]] = {
    1: migrate_layout_v1_to_v2,
    2: migrate_layout_v2_to_v3,
}


async def run_layout_migrations(
    runtime: Any, work_dir: str, current_version: int
) -> int:
    """Run all pending migrations sequentially.  Returns new layout version."""
    if current_version >= CURRENT_LAYOUT_VERSION:
        return current_version  # Zero cost — already current

    for v in range(current_version, CURRENT_LAYOUT_VERSION):
        migrator = LAYOUT_MIGRATIONS.get(v)
        if migrator:
            logger.info(
                "Running layout migration",
                from_version=v,
                to_version=v + 1,
                work_dir=work_dir,
            )
            await migrator(runtime, work_dir)

    return CURRENT_LAYOUT_VERSION
