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

from jsonschema.exceptions import ValidationError, best_match

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

# The key the mcp schema's server union is tagged by.
_TAG = "type"


def describe_schema_error(error: ValidationError) -> str:
    """Say which key is at fault, rather than reprinting the value that holds it.

    jsonschema reports a failed ``oneOf`` by printing the whole instance and
    saying it matched nothing, which for a tagged union is the least useful
    sentence available: an entry that omitted ``type`` — the omission almost
    every real-world mcp.json makes, since no vendor writes it for stdio — was
    told its own dict was "not valid under any of the given schemas" and never
    told which key to add. A union tagged by a const is discriminated, so the
    branch whose tag the instance matched is the only one whose complaint is
    about this instance; the others are describing a shape it never claimed.
    """
    branches: dict[int, list[ValidationError]] = {}
    for sub in error.context or ():
        head = next(iter(sub.schema_path), None)
        if not isinstance(head, int):
            return error.message
        branches.setdefault(head, []).append(sub)
    if not branches:
        return error.message

    tags: list[str] = []
    matched: list[ValidationError] = []
    for subs in branches.values():
        wrong_tag = next(
            (
                s
                for s in subs
                if s.validator == "const"
                and next(reversed(s.absolute_path), None) == _TAG
            ),
            None,
        )
        if wrong_tag is None:
            matched.extend(subs)
        else:
            tags.append(str(wrong_tag.validator_value))
    if matched:
        return best_match(matched).message
    written = (
        error.instance.get(_TAG) if isinstance(error.instance, dict) else None
    )
    return (
        f"{_TAG!r} is {written!r}, which is not one of the transports this "
        f"schema defines ({', '.join(tags)})"
    )
