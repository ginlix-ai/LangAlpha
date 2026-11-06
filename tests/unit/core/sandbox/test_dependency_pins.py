"""Guards the hand-mirrored sandbox dependency list.

``DEFAULT_DEPENDENCIES`` drives the Daytona snapshot, the E2B template and the no-snapshot
install; ``Dockerfile.sandbox`` re-spells the same list because a Docker build
cannot import Python. Nothing enforced the mirror, so the two images could
drift into different toolchains for the same workspace — the failure surfaces
as a mysterious ``ModuleNotFoundError`` in one provider only.
"""

from __future__ import annotations

import re
import shlex
import tomllib
from pathlib import Path

import pytest

from ptc_agent.core.sandbox._defaults import (
    DEFAULT_DEPENDENCIES,
    SANDBOX_PLAYWRIGHT_VERSION,
)

_ROOT = Path(__file__).resolve().parents[4]
_DOCKERFILE = _ROOT / "Dockerfile.sandbox"
_UV_LOCK = _ROOT / "uv.lock"
_MCP_SETUP = _ROOT / "src" / "ptc_agent" / "core" / "sandbox" / "mcp_setup.py"
_IMAGE_RECIPE = (
    _ROOT / "src" / "ptc_agent" / "core" / "sandbox" / "providers" / "_image_recipe.py"
)
_SSH_BOOTSTRAP = (
    _ROOT / "src" / "ptc_agent" / "core" / "sandbox" / "providers" / "_ssh_bootstrap.py"
)

_ARG_RE = re.compile(r"^ARG\s+(\w+)=(\S+)", re.MULTILINE)
_SUBST_RE = re.compile(r"\$\{(\w+)\}")


def _dockerfile_args() -> dict[str, str]:
    """``ARG NAME=default`` declarations, which the install lines interpolate."""
    return dict(_ARG_RE.findall(_DOCKERFILE.read_text()))


def _expand(text: str, args: dict[str, str]) -> str:
    return _SUBST_RE.sub(lambda m: args.get(m.group(1), m.group(0)), text)


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
    tokens = shlex.split(_expand(command, _dockerfile_args()))
    start = tokens.index("install") + 1
    end = tokens.index("&&", start)
    return [token for token in tokens[start:end] if not token.startswith("-")]


def test_dockerfile_mirrors_default_dependencies():
    packages = _dockerfile_install_packages()
    assert set(packages) == set(DEFAULT_DEPENDENCIES)
    assert len(packages) == len(set(packages)), "duplicate package in Dockerfile.sandbox"


def test_both_playwright_ports_ride_one_version():
    """Each language port resolves its browser revision from its own version
    number, so a drift between the npm and Python pins bakes a second Chromium
    (plus a second headless shell) into the image instead of sharing one."""
    args = _dockerfile_args()
    assert args["PLAYWRIGHT_VERSION"] == SANDBOX_PLAYWRIGHT_VERSION
    assert f"playwright=={SANDBOX_PLAYWRIGHT_VERSION}" in DEFAULT_DEPENDENCIES

    npm_line = next(
        line
        for line in _logical_lines(_DOCKERFILE.read_text())
        if "npm install -g" in line
    )
    assert f"playwright@{SANDBOX_PLAYWRIGHT_VERSION}" in _expand(npm_line, args)


def test_sandbox_yfinance_is_the_version_the_server_locks():
    """A rebuilt image must not move the sandbox's Yahoo client ahead of (or
    behind) the one uv.lock gives the server."""
    locked = next(
        package["version"]
        for package in tomllib.loads(_UV_LOCK.read_text())["package"]
        if package["name"] == "yfinance"
    )
    assert f"yfinance=={locked}" in DEFAULT_DEPENDENCIES


@pytest.mark.parametrize("path", [_DOCKERFILE, _MCP_SETUP, _IMAGE_RECIPE, _SSH_BOOTSTRAP])
def test_no_install_path_overrides_curl_cffi(path: Path):
    """yfinance requires curl_cffi>=0.17 and scrapling needs less, so nothing
    conflicts. An --override would replace yfinance's own floor and let an
    older curl_cffi through."""
    assert "--override" not in path.read_text()


def test_no_range_specifiers_in_default_dependencies():
    """The list is joined into a shell command, where ``<`` becomes a redirect."""
    offenders = [d for d in DEFAULT_DEPENDENCIES if "<" in d or ">" in d]
    assert offenders == []
