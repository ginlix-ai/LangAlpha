"""Vendored Agent Plugins canonical schemas — selected by ``$id``, never fetched.

The two JSON files beside this module are verbatim byte copies of the
canonical schemas published at agent-plugins.org (an opt-in integration test
diffs them against the live documents). Validators built over them must be
constructed with an empty ``referencing.Registry()`` so no ``$ref`` can ever
leave the document — the same no-remote-refs discipline as workflow response
schemas.
"""

import json
import re
from pathlib import Path
from typing import Any

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0.0"})

# The canonical $schema URL shape; group 1 = spec version, group 2 = document
# kind. Used to give "unsupported version" a real message instead of a bare
# const-mismatch error.
SCHEMA_URL_RE = re.compile(
    r"^https://agent-plugins\.org/schemas/(\d+\.\d+\.\d+)/(plugin|mcp)\.schema\.json$"
)

_DIR = Path(__file__).parent


def _load(filename: str) -> dict[str, Any]:
    return json.loads((_DIR / filename).read_text(encoding="utf-8"))


PLUGIN_SCHEMA = _load("plugin.schema.1.0.0.json")
MCP_SCHEMA = _load("mcp.schema.1.0.0.json")

SCHEMAS_BY_ID: dict[str, dict[str, Any]] = {
    PLUGIN_SCHEMA["$id"]: PLUGIN_SCHEMA,
    MCP_SCHEMA["$id"]: MCP_SCHEMA,
}
