"""Shared dataclasses, constants, and module helpers for the sandbox package."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog




logger = structlog.get_logger(__name__)

# Lock entry fields excluded from skills manifest hash — these timestamps
# change on every computation and would force needless re-uploads.
_LOCK_VOLATILE_KEYS: frozenset[str] = frozenset({"installedAt", "updatedAt"})

# Shared runtime modules the builtin MCP server files import as siblings
# (``import _bootstrap`` / ``from mcp_servers._envelope import ...``). They are
# not server entry points, so the per-server upload loop never sees them —
# ship and hash them explicitly whenever any builtin server file ships, or the
# servers crash on import in synced sandboxes (and prune would delete them).
_MCP_SHARED_RUNTIME_FILES: tuple[str, ...] = (
    "_bootstrap.py",
    "_envelope.py",
    "_yf_common.py",
)

# Internal ``src.<pkg>`` packages mirrored into the sandbox's ``_internal/src/``
# so sandbox code and the builtin MCP servers can ``import src.<pkg>`` without
# the full repo. Shipped and hashed as ONE manifest module (``internal_packages``)
# because the upload is all-or-nothing; every regular file ships so data seeds
# (e.g. ``market_protocol/instruments.yaml``) can never silently drop.
_SANDBOX_INTERNAL_PACKAGES: tuple[str, ...] = ("data_client", "market_protocol")


@dataclass
class ChartData:
    """Captured chart from matplotlib execution."""

    type: str
    title: str
    png_base64: str | None = None
    elements: list[Any] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of code execution in sandbox."""

    success: bool
    stdout: str
    stderr: str
    duration: float
    files_created: list[str]
    files_modified: list[str]
    execution_id: str
    code_hash: str
    charts: list[ChartData] = field(default_factory=list)
    mcp_trace: list[dict] = field(default_factory=list)


@dataclass
class SyncResult:
    """Result of a unified sandbox asset sync operation."""

    refreshed_modules: list[str]
    forced: bool


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_dict(d: dict[str, str]) -> str:
    """Deterministic SHA-256 hash of a string→string dict."""
    payload = "\n".join(f"{k}:{v}" for k, v in sorted(d.items()))
    return hashlib.sha256(payload.encode()).hexdigest()


def _internal_package_files(src_dir: Path) -> list[tuple[Path, Path]]:
    """``(local_path, path relative to src/)`` for every internal-package file.

    Walks ``_SANDBOX_INTERNAL_PACKAGES`` with ``rglob("*")`` — every regular
    file except ``__pycache__`` ships, so data seeds can never silently drop
    out of the sync. Empty when ``src/__init__.py`` is missing.
    """
    src_init = src_dir / "__init__.py"
    if not src_init.exists():
        return []
    files: list[tuple[Path, Path]] = [(src_init, Path("__init__.py"))]
    for pkg in _SANDBOX_INTERNAL_PACKAGES:
        pkg_dir = (src_dir / pkg).resolve()
        if not pkg_dir.exists():
            logger.warning(
                "Internal package not found - skipping",
                package=pkg,
                package_dir=str(pkg_dir),
            )
            continue
        for file_path in sorted(pkg_dir.rglob("*")):
            if not file_path.is_file() or "__pycache__" in file_path.parts:
                continue
            files.append((file_path, file_path.relative_to(src_dir)))
    return files


def _resolve_local_path(local_path: str, config_dir: Path | None) -> str | None:
    """Resolve a relative file path, trying *config_dir* first, then CWD."""
    p = Path(local_path)
    if not p.is_absolute() and config_dir:
        candidate = (config_dir / local_path).resolve()
        if candidate.exists():
            return str(candidate)
    if p.exists():
        return str(p)
    return None


def _entry_name(entry) -> str:
    """Extract name from a file entry — handles both dict and object forms."""
    if isinstance(entry, dict):
        return str(entry.get("name", entry))
    return str(getattr(entry, "name", entry))


def _entry_is_dir(entry) -> bool:
    """Extract is_dir from a file entry — handles both dict and object forms."""
    if isinstance(entry, dict):
        return bool(entry.get("is_dir", False))
    return bool(getattr(entry, "is_dir", False))


def _get_sandbox_eligible_skills() -> tuple[set[str], set[str]]:
    """Return (sandbox_skill_names, all_registry_names) for flash-only filtering.

    Skills present in SKILL_REGISTRY but not in sandbox_skill_names are
    flash-only and should be skipped during sandbox operations.
    """
    from ptc_agent.agent.middleware.skills import SKILL_REGISTRY, get_sandbox_skill_names

    return get_sandbox_skill_names(), set(SKILL_REGISTRY.keys())

