"""The server names the sandbox refuses, spelled once per place that decides.

A server name becomes the Python module its tool wrappers are generated into,
beside the runtime's own ``mcp_client``, so a hard keyword or that module's
name cannot hold it. The API model, the web form and migration 055's rename
plan each write the list out rather than read ``keyword``, so one can drift
from the others silently: the form accepts a name the API then refuses, or the
upgrade leaves a name in place that no longer imports.
"""

from __future__ import annotations

import importlib.util
import keyword
import re
from pathlib import Path

from src.server.models import mcp_server

REPO_ROOT = Path(__file__).resolve().parents[4]

_FORM = REPO_ROOT / "web/src/pages/ChatAgent/components/mcp/mcpSchemas.ts"
_MIGRATION = REPO_ROOT / "migrations/versions/055_user_level_mcp_and_vault.py"


def _read(path: Path) -> str:
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    return path.read_text()


def _form_names() -> set[str]:
    source = _read(_FORM)
    keywords = re.search(
        r"^const PYTHON_KEYWORDS: ReadonlySet<string> = new Set\(\[\n(.*?)^\]\);",
        source,
        re.MULTILINE | re.DOTALL,
    )
    runtime = re.search(r"^const RUNTIME_MODULE = '([^']+)';", source, re.MULTILINE)
    assert keywords and runtime, (
        f"the reserved names are no longer plain literals in {_FORM.name}"
    )
    return {runtime.group(1), *re.findall(r"'([^']+)'", keywords.group(1))}


def _migration_names() -> frozenset[str]:
    _read(_MIGRATION)
    spec = importlib.util.spec_from_file_location("migration_055", _MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._MODULE_NAMES


def test_every_copy_reserves_the_hard_keywords_and_the_runtime_module():
    """Hard keywords only: a soft keyword such as ``match`` is a legal module,
    so ``keyword.softkwlist`` stays out of every copy."""
    assert mcp_server._PY_KEYWORDS == set(keyword.kwlist)
    reserved = {mcp_server._RUNTIME_MODULE, *mcp_server._PY_KEYWORDS}
    assert _form_names() == reserved
    assert _migration_names() == reserved
