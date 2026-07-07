"""Enforce the agent-facing docstring contract (mcp_servers/AGENT_CONTRACT.md).

Every market-data MCP tool docstring is parsed into agent prompts and generated
sandbox wrappers, so this suite pins the contract: a Returns: section that the
codegen extractor turns into a real type hint, no rot-prone Example/Note
sections, and a moderate length budget. x_mcp_server is exempt per the contract.

It also content-pins the hand-tuned docstrings themselves (MCP servers plus the
direct market tools) against ``agent_docstring_lock.json`` — accidental
"improvements" fail loudly. This module never writes the lock; regeneration
lives in ``scripts/utils/update_agent_docstring_lock.py``.
"""

import asyncio
import difflib
import importlib
import json
import re
import textwrap
from pathlib import Path

import pytest

from src.ptc_agent.core.tool_generator import ToolFunctionGenerator

# Server key → module under mcp_servers/. Imported lazily in _all_tools() so a
# broken server surfaces as a contained test failure, not a module-wide
# collection error that also takes down the pin gate.
_SERVERS = {
    "price_data": "price_data_mcp_server",
    "options": "options_mcp_server",
    "fundamentals": "fundamentals_mcp_server",
    "macro": "macro_mcp_server",
    "yf_price": "yf_price_mcp_server",
    "yf_market": "yf_market_mcp_server",
    "yf_analysis": "yf_analysis_mcp_server",
    "yf_fundamentals": "yf_fundamentals_mcp_server",
}

# Target is ~800 chars (AGENT_CONTRACT.md); the hard cap leaves slight headroom.
_MAX_DOCSTRING_CHARS = 900

# Section headers that terminate the codegen Returns: capture and tend to rot.
_FORBIDDEN_SECTIONS = re.compile(r"^\s*(Examples?:|Note:)", re.MULTILINE)


def _all_tools() -> tuple[list[tuple[str, str, str, dict]], dict[str, str]]:
    """(server, tool, description, input_schema) entries + per-server errors."""
    entries: list[tuple[str, str, str, dict]] = []
    errors: dict[str, str] = {}
    for server_name, module_name in _SERVERS.items():
        try:
            module = importlib.import_module(f"mcp_servers.{module_name}")
            tools = asyncio.run(module.mcp.list_tools())
        except Exception as e:  # noqa: BLE001
            errors[server_name] = repr(e)
            continue
        for tool in tools:
            entries.append(
                (server_name, tool.name, tool.description or "", tool.inputSchema)
            )
    return entries, errors


_TOOLS, _COLLECTION_ERRORS = _all_tools()
_IDS = [f"{server}:{name}" for server, name, _, _ in _TOOLS]


def test_tool_collection_succeeded():
    assert not _COLLECTION_ERRORS, (
        f"tool collection failed for: {_COLLECTION_ERRORS}"
    )


def test_every_market_data_server_exposes_tools():
    assert len(_TOOLS) >= 50  # 8 servers; guards against silent registration loss


@pytest.mark.parametrize(("server", "name", "description", "schema"), _TOOLS, ids=_IDS)
def test_docstring_contract(server, name, description, schema):
    extractor = ToolFunctionGenerator()

    # A Returns: label must exist — prose-only contracts are lost by codegen.
    assert re.search(r"Returns?:", description), f"{server}:{name} has no Returns: section"

    # The extractor must recover a real type hint and description, not defaults.
    return_type, return_desc = extractor._extract_return_info(description)
    assert return_type != "Any", (
        f"{server}:{name} Returns: text does not yield a type hint — "
        "start the first line with 'dict:' or 'list[dict]:'"
    )
    assert return_desc != "Tool execution result"

    # Tools with parameters must document them.
    if schema.get("properties"):
        assert re.search(r"Args?:", description), f"{server}:{name} has params but no Args: section"

    # No Example/Note sections — they truncate the Returns capture and rot.
    assert not _FORBIDDEN_SECTIONS.search(description), (
        f"{server}:{name} contains an Example/Note section"
    )

    # Moderate length: docstrings ship in prompts across ~60 tools.
    assert len(description) <= _MAX_DOCSTRING_CHARS, (
        f"{server}:{name} docstring is {len(description)} chars (cap {_MAX_DOCSTRING_CHARS})"
    )


# ── Content pin: the tuned wording itself ────────────────────────────────────
#
# The structural checks above can't stop a well-formed rewording. The lock file
# snapshots every agent-facing docstring + signature so any drift — human or
# agent — fails the default unit run until consciously regenerated. This test
# is READ-ONLY by design: the only writer is the standalone script, so a leaked
# env var can never make the gate silently self-heal.

LOCK_PATH = Path(__file__).parent / "agent_docstring_lock.json"
UPDATE_CMD = "uv run python scripts/utils/update_agent_docstring_lock.py"

_DRIFT_PREAMBLE = f"""\
Agent-facing docstring/signature drift detected.

These docstrings are hand-tuned prompt surface: they are parsed into agent
prompts and generated sandbox wrappers, and their wording was deliberately
chosen (mcp_servers/AGENT_CONTRACT.md). Do NOT rephrase, expand, or "improve"
them as a side effect of other work.

If this drift is accidental: restore the locked wording (diffs below).
If it is an intentional, reviewed contract change: regenerate the lock with
  {UPDATE_CMD}

Drifted entries:
"""


def _direct_tools() -> list[tuple[str, str, dict]]:
    """(name, description, json_schema) for @tool functions DEFINED in the
    direct market tool module — a BaseTool merely imported there is excluded
    (the wrapped function's ``__module__`` is the definition-site check)."""
    from langchain_core.tools import BaseTool

    module = importlib.import_module("src.tools.market_data.tool")
    entries = []
    for obj in vars(module).values():
        if not isinstance(obj, BaseTool):
            continue
        fn = getattr(obj, "coroutine", None) or getattr(obj, "func", None)
        if fn is None or fn.__module__ != module.__name__:
            continue
        assert obj.args_schema is not None, f"{obj.name}: no args_schema to pin"
        entries.append(
            (obj.name, obj.description or "", obj.args_schema.model_json_schema())
        )
    return entries


def _pin_entry(description: str, schema: dict) -> dict:
    return {
        "doc_lines": description.splitlines(),
        "params": sorted(schema.get("properties", {})),
        "required": sorted(schema.get("required", [])),
    }


def current_pins() -> dict[str, dict]:
    """Snapshot of the full agent-facing pin surface.

    Shared with scripts/utils/update_agent_docstring_lock.py — the lock's only
    writer. ``doc_lines`` (not one escaped string) keeps committed diffs readable.
    """
    pins = {}
    for server, name, description, schema in _TOOLS:
        pins[f"mcp:{server}:{name}"] = _pin_entry(description, schema)
    for name, description, schema in _direct_tools():
        pins[f"direct:{name}"] = _pin_entry(description, schema)
    return pins


def test_direct_market_tools_are_collected():
    assert len(_direct_tools()) >= 7  # guards the pin against silent collection loss


def test_docstrings_match_tuned_lock():
    """Every agent-facing docstring/signature matches agent_docstring_lock.json."""
    if not LOCK_PATH.exists():
        pytest.fail(
            f"{LOCK_PATH.name} is missing — generate it with:\n  {UPDATE_CMD}",
            pytrace=False,
        )
    current = current_pins()
    locked = json.loads(LOCK_PATH.read_text())

    problems = []
    for key in sorted(set(locked) - set(current)):
        problems.append(f"- {key}: pinned but no longer exposed (stale lock entry)")
    for key in sorted(set(current) - set(locked)):
        problems.append(f"- {key}: new tool with no pin (regenerate the lock)")
    for key in sorted(set(current) & set(locked)):
        cur, old = current[key], locked[key]
        if cur["doc_lines"] != old["doc_lines"]:
            diff = "\n".join(
                difflib.unified_diff(
                    old["doc_lines"], cur["doc_lines"], "locked", "current", lineterm=""
                )
            )
            problems.append(f"- {key}: docstring drifted\n{textwrap.indent(diff, '    ')}")
        if cur["params"] != old["params"] or cur["required"] != old["required"]:
            problems.append(
                f"- {key}: signature drifted "
                f"(params {old['params']} -> {cur['params']}, "
                f"required {old['required']} -> {cur['required']})"
            )

    if problems:
        pytest.fail(_DRIFT_PREAMBLE + "\n".join(problems), pytrace=False)
