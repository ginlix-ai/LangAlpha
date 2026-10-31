"""What the deployed prompt was made from.

A pinned thread keeps sending the static prompt its thinking was produced
under, and stores a digest of these sources beside it, so a later build tells
a deploy (the sources changed) from a per-turn input that changed only what
they rendered: a user toggle, a direct MCP server failing discovery, the
memory gate. Read once per process from the files the code shipped with, so
every worker on one deploy computes the same value and no state is shared.
"""

from __future__ import annotations

import functools
import hashlib
from pathlib import Path
from typing import Any

import yaml

from ptc_agent.config.plugins import BUNDLES_DIR
from src.config.features import FEATURES, system_flag

_PROMPTS = Path(__file__).parent
_AGENT = _PROMPTS.parent


def _sources() -> list[Path]:
    """The files the static prompt is made from: the templates and
    prompts.yaml, and the Python that writes text into it with no user's
    input: the variables ``PTCAgent._build_system_prompt`` passes, the
    formatters of the subagent roster (with the built-ins' descriptions) and
    of the direct tools, and the built-in servers whose tools those lines
    describe."""
    return [
        *sorted((_PROMPTS / "templates").rglob("*.j2")),
        _PROMPTS / "config" / "prompts.yaml",
        *sorted(_PROMPTS.glob("*.py")),
        _AGENT / "agent.py",
        _AGENT / "subagents" / "builtins.py",
        _AGENT / "middleware" / "direct_mcp.py",
        *sorted(BUNDLES_DIR.rglob("*.py")),
    ]


@functools.cache
def source_digest() -> str:
    """sha256 over ``_sources`` and the feature catalog's defaults, which
    decide the static prompt for a user with no overrides.

    Broader than what the system template reaches (envelope rows, subagent
    prompts, every flag, whole modules), so most deploys count as a change of
    sources. That costs little: a pin whose content the build still composes
    takes the new digest on its next call, so only a thread already due can
    read it as a deploy, and a narrower hash would have to follow every
    include and every call that writes into the prompt."""
    digest = hashlib.sha256()
    for path in _sources():
        digest.update(path.relative_to(_AGENT.parents[2]).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    for key in sorted(FEATURES):
        digest.update(repr((key, *system_flag(key))).encode())
    return digest.hexdigest()


@functools.cache
def _config() -> dict[str, Any]:
    return yaml.safe_load((_PROMPTS / "config" / "prompts.yaml").read_text()) or {}


def prompt_version() -> int:
    """The hand-set version of this build's prompt, which a pin records."""
    return int(_config().get("prompt_version") or 0)


def rebuild_before() -> int:
    """The least ``prompt_version`` a thread may keep: one pinned below it
    rebuilds at its next turn start, however small or warm. Never above this
    build's own version, which a rebuilt pin records: past it, a rebuilt
    thread would stay below it and be forced again on every due."""
    return min(int(_config().get("rebuild_before") or 0), prompt_version())
