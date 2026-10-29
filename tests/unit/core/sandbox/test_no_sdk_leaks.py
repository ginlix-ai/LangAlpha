"""Gate test: verify Daytona SDK imports are confined to the provider module.

Uses AST analysis to scan all .py files under src/ and ensure that only the
allowed file (providers/daytona.py) imports from daytona. This prevents
SDK coupling from leaking back into the rest of the codebase.
"""

from __future__ import annotations

import ast
from pathlib import Path

# The only file allowed to import the Daytona SDK
ALLOWED_DAYTONA_IMPORTS = {
    "src/ptc_agent/core/sandbox/providers/daytona.py",
    "src/ptc_agent/core/sandbox/providers/daytona_runtime.py",
    "src/ptc_agent/core/sandbox/providers/daytona_secrets.py",
}

SRC_ROOT = Path(__file__).resolve().parents[4] / "src"


def _collect_python_files(root: Path) -> list[Path]:
    """Return all .py files under *root*, excluding __pycache__."""
    return [
        p
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts
    ]


def _file_imports_daytona(filepath: Path) -> bool:
    """Return True if *filepath* contains any import of daytona (AST-based)."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "daytona" or alias.name.startswith("daytona."):
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and (
                node.module == "daytona"
                or node.module.startswith("daytona.")
            ):
                return True
    return False


def test_no_daytona_import_leaks() -> None:
    """All Daytona imports must be inside the allowed provider module."""
    assert SRC_ROOT.is_dir(), f"src root not found: {SRC_ROOT}"

    violations: list[str] = []
    for py_file in _collect_python_files(SRC_ROOT):
        rel = py_file.relative_to(SRC_ROOT.parent)
        if str(rel) in ALLOWED_DAYTONA_IMPORTS:
            continue
        if _file_imports_daytona(py_file):
            violations.append(str(rel))

    assert violations == [], (
        "daytona imported outside allowed modules:\n"
        + "\n".join(f"  - {v}" for v in sorted(violations))
    )


def test_allowed_file_exists() -> None:
    """Sanity check: the allowed provider file actually exists."""
    for allowed in ALLOWED_DAYTONA_IMPORTS:
        path = SRC_ROOT.parent / allowed
        assert path.is_file(), f"Allowed file not found: {path}"


def test_allowed_file_does_import_daytona() -> None:
    """Sanity check: the provider file does actually import daytona."""
    for allowed in ALLOWED_DAYTONA_IMPORTS:
        path = SRC_ROOT.parent / allowed
        assert _file_imports_daytona(path), (
            f"Expected {allowed} to import daytona but it does not"
        )


# The only files allowed to import the E2B SDK
ALLOWED_E2B_IMPORTS = {
    "src/ptc_agent/core/sandbox/providers/e2b.py",
    "src/ptc_agent/core/sandbox/providers/e2b_runtime.py",
    "src/ptc_agent/core/sandbox/providers/e2b_secrets.py",
}


def _file_imports_e2b(filepath: Path) -> bool:
    """Return True if *filepath* contains any import of e2b (AST-based)."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "e2b" or alias.name.startswith("e2b."):
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and (
                node.module == "e2b" or node.module.startswith("e2b.")
            ):
                return True
    return False


def test_no_e2b_import_leaks() -> None:
    """All E2B imports must be inside the allowed provider modules."""
    assert SRC_ROOT.is_dir(), f"src root not found: {SRC_ROOT}"

    violations: list[str] = []
    for py_file in _collect_python_files(SRC_ROOT):
        rel = py_file.relative_to(SRC_ROOT.parent)
        if str(rel) in ALLOWED_E2B_IMPORTS:
            continue
        if _file_imports_e2b(py_file):
            violations.append(str(rel))

    assert violations == [], (
        "e2b imported outside allowed modules:\n"
        + "\n".join(f"  - {v}" for v in sorted(violations))
    )


# The only files allowed to import the SSH client library
ALLOWED_ASYNCSSH_IMPORTS = {
    "src/ptc_agent/core/sandbox/providers/ssh.py",
    "src/ptc_agent/core/sandbox/providers/ssh_runtime.py",
    "src/ptc_agent/core/sandbox/providers/_ssh_transport.py",
    "src/ptc_agent/core/sandbox/providers/_ssh_connect.py",
    # The tunnel gateway is an SSH server of its own, run beside the backend.
    "src/server/ssh_gateway/server.py",
    "src/server/ssh_gateway/admission.py",
}


def _file_imports_asyncssh(filepath: Path) -> bool:
    """Return True if *filepath* contains any import of asyncssh (AST-based)."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "asyncssh" or alias.name.startswith("asyncssh."):
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and (
                node.module == "asyncssh" or node.module.startswith("asyncssh.")
            ):
                return True
    return False


def test_no_asyncssh_import_leaks() -> None:
    """All asyncssh imports must be inside the SSH provider modules."""
    assert SRC_ROOT.is_dir(), f"src root not found: {SRC_ROOT}"

    violations: list[str] = []
    for py_file in _collect_python_files(SRC_ROOT):
        rel = py_file.relative_to(SRC_ROOT.parent)
        if str(rel) in ALLOWED_ASYNCSSH_IMPORTS:
            continue
        if _file_imports_asyncssh(py_file):
            violations.append(str(rel))

    assert violations == [], (
        "asyncssh imported outside allowed modules:\n"
        + "\n".join(f"  - {v}" for v in sorted(violations))
    )


def test_asyncssh_allowed_files_import_it() -> None:
    """Sanity check: each allowed SSH module exists and imports asyncssh."""
    for allowed in ALLOWED_ASYNCSSH_IMPORTS:
        path = SRC_ROOT.parent / allowed
        assert path.is_file(), f"Allowed file not found: {path}"
        assert _file_imports_asyncssh(path), (
            f"Expected {allowed} to import asyncssh but it does not"
        )
