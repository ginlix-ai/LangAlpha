"""Keep the exported market-data tool list in step with the servers.

A deployment may serve the four market-data servers over HTTP from another
implementation. ``mcp_servers/contract/market_data.tools.json`` is the surface
such a server publishes as its ``tools/list``, so the bundled skills and the
generated sandbox wrappers read the same names, arguments, docstrings and
output schemas on either path. This module never writes the file; regeneration
lives in ``scripts/utils/export_market_data_tools.py``.
"""

import asyncio
import importlib
import json
from pathlib import Path

EXPORT_PATH = (
    Path(__file__).resolve().parents[3]
    / "mcp_servers"
    / "contract"
    / "market_data.tools.json"
)

_SERVERS = {
    "fundamentals": "plugins.langalpha_market_data.fundamentals_mcp_server",
    "macro": "plugins.langalpha_market_data.macro_mcp_server",
    "options": "plugins.langalpha_market_data.options_mcp_server",
    "price_data": "plugins.langalpha_market_data.price_data_mcp_server",
}


def current_export() -> dict:
    """Each server's name and its tools exactly as ``tools/list`` sends them."""
    export = {}
    for key, module_name in _SERVERS.items():
        server = importlib.import_module(module_name).mcp
        tools = asyncio.run(server.list_tools())
        export[key] = {
            "name": server.name,
            "tools": [
                tool.model_dump(mode="json", by_alias=True, exclude_none=True)
                for tool in tools
            ],
        }
    return export


def render(export: dict) -> str:
    # No sort_keys: the order of an input schema's properties is the order of
    # the generated wrapper's parameters.
    return json.dumps(export, indent=2, ensure_ascii=False) + "\n"


def test_export_matches_servers():
    expected = render(current_export())
    actual = EXPORT_PATH.read_text(encoding="utf-8") if EXPORT_PATH.exists() else ""
    assert actual == expected, (
        f"{EXPORT_PATH.name} is stale. Regenerate it with "
        "`uv run python scripts/utils/export_market_data_tools.py` and commit it "
        "with the tool change."
    )
