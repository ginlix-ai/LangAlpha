"""The code the sandbox runs itself imports only the standard library.

The file mount daemon and the glob run under the sandbox's ``python3 -I``:
Python 3.12, no site-packages, no workspace on the path. Tests import them
from the server's newer venv, where a third-party import or newer syntax
passes and breaks only in the sandbox.
"""

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SANDBOX = REPO_ROOT / "src" / "ptc_agent" / "core" / "sandbox"
PACKAGE = SANDBOX / "livefs_runtime"
SANDBOX_PYTHON = (3, 12)

# Sent as the text of ``python3 -I -c``, so there is no package for a
# relative import to resolve in.
SCRIPTS = {PACKAGE / "boot.py", SANDBOX / "glob_runtime.py"}

# Imports a file tries expecting to miss, each under an ImportError handler
# that falls back to the standard library. Asserted both ways, so an entry
# whose import is gone fails as a new one does.
EXPECTED_MISSES: dict[tuple[str, str], str] = {
    ("livefs_runtime/mfusepy.py", "_winreg"): "Python 2's name, tried before winreg on Windows only",
}


def _runtime_files() -> list[Path]:
    return sorted([*PACKAGE.glob("*.py"), SANDBOX / "glob_runtime.py"])


def _rel(path: Path) -> str:
    return str(path.relative_to(SANDBOX))


def _imports(path: Path) -> list[tuple[int, str]]:
    """(level, module) for each import, parsed as the sandbox's Python would."""
    tree = ast.parse(path.read_text(), filename=str(path), feature_version=SANDBOX_PYTHON)
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.extend((0, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                out.append((node.level, node.module))
            else:
                # ``from . import mod`` names modules of the package.
                out.extend((node.level, alias.name) for alias in node.names)
    return out


def test_every_runtime_file_is_checked():
    files = set(_runtime_files())
    assert SCRIPTS <= files
    assert PACKAGE / "daemon.py" in files


def test_runtime_parses_as_the_sandbox_python():
    failures = []
    for path in _runtime_files():
        try:
            ast.parse(path.read_text(), filename=str(path), feature_version=SANDBOX_PYTHON)
        except SyntaxError as exc:
            failures.append(f"{_rel(path)}:{exc.lineno}: {exc.msg}")
    assert not failures, f"syntax newer than Python {SANDBOX_PYTHON}: {failures}"


def test_runtime_imports_only_the_standard_library_or_its_own_package():
    offenders: list[tuple[str, str]] = []
    misses: set[tuple[str, str]] = set()
    for path in _runtime_files():
        rel = _rel(path)
        for level, module in _imports(path):
            if level == 0:
                top = module.split(".")[0]
                if top in sys.stdlib_module_names:
                    continue
                if (rel, top) in EXPECTED_MISSES:
                    misses.add((rel, top))
                    continue
                offenders.append((rel, module))
            elif path in SCRIPTS or level != 1:
                offenders.append((rel, "." * level + module))
            elif not (PACKAGE / f"{module.split('.')[0]}.py").is_file():
                offenders.append((rel, f".{module} (no such module in the package)"))
    assert not offenders, (
        "sandbox runtime imports outside the standard library and its own "
        f"package: {offenders}"
    )
    assert misses == set(EXPECTED_MISSES), (
        "EXPECTED_MISSES out of date: "
        f"listed but not imported {set(EXPECTED_MISSES) - misses}"
    )
