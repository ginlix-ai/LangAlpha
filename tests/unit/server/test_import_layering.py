"""Import-layering guard: the tier contract is app → handlers → services → database.

``services/**`` importing ``handlers/**`` inverts the tier direction. The
existing violations are allowlisted below with exact counts and asserted by
equality in BOTH directions — a new violation fails immediately, and repairing
one also fails until its entry is deleted, so the allowlist can only burn down.
The one sanctioned residual (automation_executor) keeps its entry permanently
(a ``TODO(layering)`` marks the site).
"""

import ast
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"
SERVICES = SRC / "server" / "services"
HANDLERS_PREFIX = "src.server.handlers"

# (file relative to repo root, imported handlers module) -> occurrence count.
# Each repair phase deletes its rows; automation_executor is the sanctioned
# residual (# TODO(layering) at the site).
ALLOWED_VIOLATIONS: dict[tuple[str, str], int] = {
    ("src/server/services/automation_executor.py", "src.server.handlers.chat"): 1,
}

# Transitional legacy aliases (old import path kept as a shim after a move).
# alias module -> removal milestone. Empty until a phase actually ships an
# alias; the guard below forbids NEW imports of any listed path.
LEGACY_ALIASED_PATHS: dict[str, str] = {}

# The Postgres checkpoint saver may only be touched by these modules: the
# app-level pooled factory, the read-side helpers, and the two fenced writers
# (WriterGuard's per-run saver, the thread-mutation fence). A new importer is
# how a run path bypasses the guard session — extend this ONLY with a stated
# cross-worker story (src/server/AGENTS.md § Multi-worker review checklist).
POSTGRES_SAVER_MODULE = "langgraph.checkpoint.postgres"
ALLOWED_SAVER_IMPORTERS = {
    "src/server/services/thread_mutation.py",
    "src/server/services/writer_guard.py",
    "src/server/utils/checkpoint_helpers.py",
    "src/server/utils/checkpointer.py",
}


def _module_prefix_parts(py_file: Path) -> list[str]:
    """Dotted package parts of the file's containing package (repo-root based)."""
    rel = py_file.relative_to(REPO_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return parts[:-1] if parts else []


def _resolved_imports(py_file: Path) -> list[str]:
    tree = ast.parse(py_file.read_text(), filename=str(py_file))
    pkg = _module_prefix_parts(py_file)
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                out.append(node.module or "")
            else:
                base = pkg[: len(pkg) - (node.level - 1)]
                out.append(".".join(base + ([node.module] if node.module else [])))
    return out


def test_services_do_not_import_handlers():
    found: Counter[tuple[str, str]] = Counter()
    for py_file in sorted(SERVICES.rglob("*.py")):
        rel = str(py_file.relative_to(REPO_ROOT))
        for mod in _resolved_imports(py_file):
            if mod == HANDLERS_PREFIX or mod.startswith(HANDLERS_PREFIX + "."):
                found[(rel, mod)] += 1

    allowed = Counter()
    for key, count in ALLOWED_VIOLATIONS.items():
        allowed[key] = count

    new = found - allowed
    stale = allowed - found
    msg = []
    if new:
        msg.append(
            "NEW services→handlers imports (move the logic down a tier or, for a "
            "genuinely deferred repair, extend ALLOWED_VIOLATIONS with a spec "
            f"disposition): {dict(new)}"
        )
    if stale:
        msg.append(
            "Stale allowlist entries — the violation was repaired, delete its "
            f"row(s) from ALLOWED_VIOLATIONS: {dict(stale)}"
        )
    assert not msg, "\n".join(msg)


def test_no_new_imports_of_legacy_aliased_paths():
    if not LEGACY_ALIASED_PATHS:
        return
    offenders: list[tuple[str, str]] = []
    for py_file in sorted(SRC.rglob("*.py")):
        rel = str(py_file.relative_to(REPO_ROOT))
        for mod in _resolved_imports(py_file):
            for alias in LEGACY_ALIASED_PATHS:
                if mod == alias or mod.startswith(alias + "."):
                    offenders.append((rel, mod))
    assert not offenders, (
        "Imports of legacy aliased paths (import the new location instead; "
        f"aliases are transitional and carry a removal milestone): {offenders}"
    )


def test_postgres_saver_imports_are_confined():
    found: set[str] = set()
    for py_file in sorted((SRC / "server").rglob("*.py")):
        for mod in _resolved_imports(py_file):
            if mod == POSTGRES_SAVER_MODULE or mod.startswith(
                POSTGRES_SAVER_MODULE + "."
            ):
                found.add(str(py_file.relative_to(REPO_ROOT)))
    new = found - ALLOWED_SAVER_IMPORTERS
    stale = ALLOWED_SAVER_IMPORTERS - found
    msg = []
    if new:
        msg.append(
            "NEW importers of the Postgres checkpoint saver — checkpoint "
            "writes must ride a fenced session (WriterGuard / thread-mutation "
            "fence) or the app factory; see src/server/AGENTS.md § "
            f"Multi-worker: {sorted(new)}"
        )
    if stale:
        msg.append(
            "Stale ALLOWED_SAVER_IMPORTERS entries — the module no longer "
            f"imports the saver, delete its row(s): {sorted(stale)}"
        )
    assert not msg, "\n".join(msg)
