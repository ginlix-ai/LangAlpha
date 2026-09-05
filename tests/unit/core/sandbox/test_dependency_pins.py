"""Guards the hand-mirrored sandbox dependency list.

``DEFAULT_DEPENDENCIES`` drives the Daytona snapshot and the no-snapshot
install; ``Dockerfile.sandbox`` re-spells the same list because a Docker build
cannot import Python. Nothing enforced the mirror, so the two images could
drift into different toolchains for the same workspace — the failure surfaces
as a mysterious ``ModuleNotFoundError`` in one provider only.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES

_ROOT = Path(__file__).resolve().parents[4]
_DOCKERFILE = _ROOT / "Dockerfile.sandbox"
_MCP_SETUP = _ROOT / "src" / "ptc_agent" / "core" / "sandbox" / "mcp_setup.py"
_DAYTONA = _ROOT / "src" / "ptc_agent" / "core" / "sandbox" / "providers" / "daytona.py"

# `echo '<spec>' > /tmp/…overrides.txt` — the uv --override file, written
# inline by every install path.
_OVERRIDE_RE = re.compile(r"echo '([^']+)' > /tmp/[\w.]*overrides\.txt")


def _logical_lines(text: str) -> list[str]:
    """Shell lines with backslash continuations folded into one string."""
    lines: list[str] = []
    buffer = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.endswith("\\"):
            buffer += line[:-1] + " "
            continue
        lines.append(buffer + line)
        buffer = ""
    if buffer:
        lines.append(buffer)
    return lines


def _dockerfile_install_packages() -> list[str]:
    """Package arguments of ``Dockerfile.sandbox``'s ``uv pip install``."""
    command = next(
        line for line in _logical_lines(_DOCKERFILE.read_text()) if "uv pip install" in line
    )
    tokens = shlex.split(command)
    start = tokens.index("install") + 1
    end = tokens.index("&&", start)
    packages: list[str] = []
    skip_value = False
    for token in tokens[start:end]:
        if skip_value:
            skip_value = False
        elif token.startswith("-"):
            skip_value = token == "--override"
        else:
            packages.append(token)
    return packages


def test_dockerfile_mirrors_default_dependencies():
    packages = _dockerfile_install_packages()
    assert set(packages) == set(DEFAULT_DEPENDENCIES)
    assert len(packages) == len(set(packages)), "duplicate package in Dockerfile.sandbox"


@pytest.mark.parametrize("path", [_DOCKERFILE, _MCP_SETUP, _DAYTONA])
def test_curl_cffi_override_is_spelled_identically(path: Path):
    """yfinance caps curl_cffi below what scrapling[all] needs; every install
    path resolves it with the same override, or one image ships a resolver
    conflict the others don't."""
    specs = _OVERRIDE_RE.findall(path.read_text())
    assert specs, f"no uv --override spec found in {path.name}"
    assert set(specs) == {"curl_cffi>=0.14"}


def test_no_range_specifiers_in_default_dependencies():
    """The list is joined into a shell command, where ``<`` becomes a redirect."""
    offenders = [d for d in DEFAULT_DEPENDENCIES if "<" in d or ">" in d]
    assert offenders == []
