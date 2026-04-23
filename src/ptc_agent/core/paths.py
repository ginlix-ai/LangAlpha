"""Canonical workspace path constants shared across server, CLI, and sandbox.

All Python consumers (server, CLI, sandbox) import from here and derive their
own format (trailing ``/``, bare names, etc.).  The frontend (JS) keeps its own
copy in ``FilePanel.jsx`` with a comment pointing back to this file.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Agent system directories (toggleable in file listings, hidden in completions)
# ---------------------------------------------------------------------------
# These are agent-infrastructure dirs at the sandbox root (/home/workspace/).
# Update this set when adding new agent infrastructure directories.
AGENT_SYSTEM_DIRS: frozenset[str] = frozenset({
    ".system",
    "tools",
    "mcp_servers",
    ".agents",
    ".self-improve",
})

# ---------------------------------------------------------------------------
# Backup exclusion — dirs NOT persisted to DB during file sync.
# .agents is intentionally EXCLUDED here so .agents/skills/ gets backed up.
# ---------------------------------------------------------------------------
BACKUP_EXCLUDE_DIRS: frozenset[str] = frozenset({
    ".system",
    "tools",
    "mcp_servers",
    ".self-improve",
})

# Subdirs of .agents/ excluded from backup (ephemeral agent data).
BACKUP_EXCLUDE_AGENT_SUBDIRS: tuple[str, ...] = (
    ".agents/threads",
    ".agents/user",
    ".agents/large_tool_results",
)

# ---------------------------------------------------------------------------
# Long-term memory paths (store-backed, NOT on the sandbox filesystem).
# Agent tools route reads/writes under these prefixes to a LangGraph
# ``BaseStore`` instead of the sandbox, via ``CompositeFilesystemBackend``.
# Listed here so they appear as first-class workspace paths for tooling and
# documentation, even though the sandbox has no files at these locations.
# ---------------------------------------------------------------------------
MEMORY_USER_DIR: str = ".agents/user/memory"
MEMORY_WORKSPACE_DIR: str = ".agents/workspace/memory"
MEMORY_INDEX_FILENAME: str = "memory.md"

# ---------------------------------------------------------------------------
# Hidden path filters (always hidden from listings and completions)
# ---------------------------------------------------------------------------
HIDDEN_DIR_NAMES: frozenset[str] = frozenset({"_internal"})

# Directories always hidden from listings and excluded from file sync.
# Matched as path segments (at any depth) — e.g. "node_modules" matches
# both root-level and nested occurrences like "foo/node_modules/".
ALWAYS_HIDDEN_DIR_NAMES: frozenset[str] = frozenset({
    # Package managers / dependencies
    "node_modules",
    ".venv",
    "venv",
    "vendor",
    # Build artifacts
    ".next",
    ".nuxt",
    # Caches
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    # VCS
    ".git",
    # Environment / tool dirs
    ".npm",
    ".local",
    ".config",
    ".ipython",
})

ALWAYS_HIDDEN_PATH_SEGMENTS: tuple[str, ...] = ("/__pycache__/",)
ALWAYS_HIDDEN_BASENAMES: tuple[str, ...] = (
    "__init__.py",
    ".bash_logout",
    ".bashrc",
    ".profile",
)
ALWAYS_HIDDEN_SUFFIXES: tuple[str, ...] = (".pyc",)
