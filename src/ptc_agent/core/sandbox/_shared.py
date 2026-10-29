"""Shared dataclasses, constants, and module helpers for the sandbox package."""

import hashlib
import importlib.util
import json
import secrets
import shlex
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import structlog

from .livefs_runtime import protocol as livefs_protocol

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
    "_browser.py",
    "_dates.py",
    "_envelope.py",
    "_extract.py",
    "_schemas.py",
    "_yf_common.py",
)

# Packages mirrored into the sandbox's ``_internal/src/`` so sandbox code and
# the builtin MCP servers can import them without the full repo. Each is keyed
# by its sandbox name and resolved from the module the backend itself imports,
# so the sandbox runs the same code as the server. Shipped and hashed as ONE
# manifest module (``internal_packages``) because the upload is all-or-nothing;
# every regular file ships so data seeds (e.g.
# ``market_protocol/instruments.yaml``) can never silently drop.
_SANDBOX_INTERNAL_PACKAGES: dict[str, str] = {
    "data_client": "src.data_client",
    "market_protocol": "market_protocol",
}


def _internal_package_dir(module: str) -> Path | None:
    spec = importlib.util.find_spec(module)
    if spec is None or spec.origin is None:
        return None
    return Path(spec.origin).resolve().parent


#: Backend-owned script that walks, hashes and moves workspace files inside the
#: sandbox. Lives beside this module on the host; ships to ``_internal/src/``.
TRANSFER_RUNTIME_SANDBOX_NAME = "wsfiles_transfer.py"
_TRANSFER_RUNTIME_SOURCE = Path(__file__).with_name("wsfiles_transfer_runtime.py")

#: The MCP supervisor, shipped as an importable package so the sandbox can run
#: it as ``python3 -m supervisor``. Rides ``internal_packages`` like the
#: transfer runtime, so a source edit is hashed and re-shipped rather than
#: silently staying at whatever version the sandbox booted with.
SUPERVISOR_SANDBOX_PACKAGE = "supervisor"
_SUPERVISOR_SOURCE_DIR = Path(__file__).with_name("supervisor_runtime")

#: The file mount daemon, shipped the same way and for the same reason: its
#: hash rides ``internal_packages``, and the daemon restarts itself when the
#: shipped code no longer matches what it runs.
LIVEFS_SANDBOX_PACKAGE = livefs_protocol.PACKAGE_NAME
_LIVEFS_SOURCE_DIR = Path(__file__).with_name("livefs_runtime")

#: ``site`` imports this by name at interpreter startup, which is how a turn's
#: Python lands in its own workspace folder without a prelude prepended to the
#: submitted source.
TURN_CWD_SANDBOX_NAME = "sitecustomize.py"
_TURN_CWD_SOURCE = Path(__file__).with_name("turn_cwd_sitecustomize.py")
#: Names the directory that ``sitecustomize.py`` chdirs into.
TURN_CWD_ENV = "PTC_TURN_CWD"
#: Names the file ``sitecustomize.py`` writes a code run's own unhandled
#: exception to, the only text a missing import is read from.
EXCEPTION_FILE_ENV = "PTC_EXCEPTION_FILE"


# The ``failure`` of a code or bash call that hit its time limit, which a tool
# counts apart from a sandbox that raised.
SANDBOX_TIMEOUT: Final = "timeout"


@dataclass
class SyncResult:
    """Result of a unified sandbox asset sync operation.

    ``layout_version`` is what the sandbox's filesystem is at once the sync
    finished, which is how the host stamps its own record of it without
    booting the machine to ask.
    """

    refreshed_modules: list[str]
    forced: bool
    layout_version: int | None = None


# Same-directory rename(2): atomic, and it fails rather than copying across a
# filesystem boundary the way ``mv`` would.
REPLACE_SCRIPT = "import os,sys; os.replace(sys.argv[1], sys.argv[2])"

# The token file is replaced only by a token minted no earlier: one written by
# a push that stalled across a newer one must not put the older, possibly
# revoked, token back. Ordered by the mint's ``iat_ms``, which the platform
# dates after any revocation standing at mint time, not by expiry, which a
# shorter configured lifetime would put out of order. A file without one (or
# none at all) loses. A discard exits nonzero, so the caller does not record
# as held a token the sandbox dropped.
REPLACE_UNLESS_NEWER_SCRIPT = (
    "import json,os,sys\n"
    "src, dst, issued = sys.argv[1], sys.argv[2], int(sys.argv[3])\n"
    "try:\n"
    "    held = int(json.load(open(dst)).get('iat_ms') or 0)\n"
    "except Exception:\n"
    "    held = 0\n"
    "if held > issued:\n"
    "    os.remove(src)\n"
    "    sys.exit('kept the newer token already held')\n"
    "os.replace(src, dst)\n"
)


def _direct_call(func: Callable[..., Awaitable[Any]], *args: Any) -> Awaitable[Any]:
    return func(*args)


async def publish_private_file(
    runtime: Any,
    path: str,
    payload: dict[str, Any],
    *,
    what: str,
    script: str = REPLACE_SCRIPT,
    args: tuple[str, ...] = (),
    call: Callable[..., Awaitable[Any]] = _direct_call,
) -> bool:
    """Write a credential file the sandbox reads on every call, atomically
    and owner-only.

    The content goes to a per-write temp path, gets its 0600 mode, then
    replaces the target, so a reader sees the whole old file or the whole new
    one, and two workers writing at once each land whole. True only on a
    confirmed write: ``exec`` reports a failed replace as a nonzero exit, not
    an exception, so the exit code is checked, and a caller must not record a
    write it did not make.
    """
    # 128-bit unique temp, so concurrent writers never share a staging file.
    tmp_path = f"{path}.{secrets.token_hex(16)}.tmp"
    published = False
    try:
        await call(runtime.upload_file, json.dumps(payload).encode("utf-8"), tmp_path)
        # chmod before the rename, so the file is 0600 the instant it appears
        # at the real path (uploads land 0644); argv keeps the paths out of
        # the shell.
        command = " ".join(
            [
                f"chmod 600 {shlex.quote(tmp_path)} && python3 -I -c",
                shlex.quote(script),
                shlex.quote(tmp_path),
                shlex.quote(path),
                *(shlex.quote(arg) for arg in args),
            ]
        )
        result = await call(runtime.exec, command)
        if getattr(result, "exit_code", -1) != 0:
            # stdout carries both streams; stderr is set only when the
            # command could not run at all.
            output = getattr(result, "stderr", "") or getattr(result, "stdout", "")
            raise RuntimeError(
                f"publish exec exit={getattr(result, 'exit_code', '?')} "
                f"output={output[:200]}"
            )
        logger.debug(f"Uploaded {what}", path=path)
        published = True
        return True
    except Exception as e:
        logger.warning(f"Failed to upload {what}", error=str(e))
        return False
    finally:
        # A failed or cancelled write would otherwise orphan the staging
        # file, a world-readable copy of the credential until something
        # replaced it; a warm sandbox rewriting every few minutes must not
        # accumulate them either.
        if not published:
            try:
                await call(runtime.exec, f"rm -f {shlex.quote(tmp_path)}")
            except Exception:
                pass


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_dict(d: dict[str, str]) -> str:
    """Deterministic SHA-256 hash of a string→string dict."""
    payload = "\n".join(f"{k}:{v}" for k, v in sorted(d.items()))
    return hashlib.sha256(payload.encode()).hexdigest()


def _internal_package_files(
    src_dir: Path, *, livefs: bool = True
) -> list[tuple[Path, Path]]:
    """``(local_path, path relative to src/)`` for every internal-package file.

    Walks ``_SANDBOX_INTERNAL_PACKAGES`` with ``rglob("*")`` — every regular
    file except ``__pycache__`` ships, so data seeds can never silently drop
    out of the sync. Empty when ``src/__init__.py`` is missing. ``livefs``
    False leaves out the file mount's daemon, for a computer that never mounts.
    """
    src_init = src_dir / "__init__.py"
    if not src_init.exists():
        return []
    files: list[tuple[Path, Path]] = [(src_init, Path("__init__.py"))]
    # The file-transfer runtime rides the same all-or-nothing module so it is
    # hashed into the manifest and re-shipped whenever it changes.
    files.append((_TRANSFER_RUNTIME_SOURCE, Path(TRANSFER_RUNTIME_SANDBOX_NAME)))
    files.append((_TURN_CWD_SOURCE, Path(TURN_CWD_SANDBOX_NAME)))
    for supervisor_file in sorted(_SUPERVISOR_SOURCE_DIR.glob("*.py")):
        files.append((
            supervisor_file,
            Path(SUPERVISOR_SANDBOX_PACKAGE) / supervisor_file.name,
        ))
    for livefs_file in sorted(_LIVEFS_SOURCE_DIR.glob("*.py")) if livefs else ():
        files.append((livefs_file, Path(LIVEFS_SANDBOX_PACKAGE) / livefs_file.name))
    for pkg, module in _SANDBOX_INTERNAL_PACKAGES.items():
        pkg_dir = _internal_package_dir(module)
        if pkg_dir is None or not pkg_dir.exists():
            logger.warning(
                "Internal package not found - skipping",
                package=pkg,
                package_dir=str(pkg_dir),
            )
            continue
        for file_path in sorted(pkg_dir.rglob("*")):
            if not file_path.is_file() or "__pycache__" in file_path.parts:
                continue
            files.append((file_path, Path(pkg) / file_path.relative_to(pkg_dir)))
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


def _resolve_disabled_skills(disabled: frozenset[str]) -> frozenset[str]:
    """Disables as the sync must read them: a skill another enabled skill
    declares in ``requires`` keeps its files in the sandbox."""
    from ptc_agent.agent.middleware.skills import resolve_disabled_skills

    return resolve_disabled_skills(disabled)

