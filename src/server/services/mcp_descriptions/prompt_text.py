"""Where a description meets the agent: the server's row in the tool manifest.

Read when a workspace's tool view is built, never copied onto a row, so a
better description reaches every connected row on its own and no migration
has to rewrite one that went stale.

Precedence: what the user wrote on the row, then the generated description,
then the shipped brokerage text. A brokerage row holds the shipped text from
the day it was connected (a refresh migration rewrites it when the shipped
text changes), so that text counts as nobody's edit.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from src.config.settings import get_mcp_descriptions_config
from src.server.database import mcp_descriptions as db
from src.server.services.brokerages import BROKERAGES, brokerage_for_url
from src.server.services.mcp_descriptions.keys import Scope, dial_identity
from src.server.services.mcp_descriptions.schema import PROFILE_PROMPT_VERSION

logger = logging.getLogger(__name__)

_SHIPPED = frozenset(b.description for b in BROKERAGES)


def users_own_words(row: Mapping[str, Any]) -> str:
    """What the user wrote on the row, if anything. Shipped brokerage text is
    nobody's edit, including on a row the user has since pointed elsewhere."""
    description = (row.get("description") or "").strip()
    return "" if description in _SHIPPED else description


def effective_description(
    row: Mapping[str, Any], profile: Mapping[str, Any] | None
) -> str:
    """The description the agent reads for one catalog row."""
    brokerage = brokerage_for_url(row.get("url"))
    return (
        users_own_words(row)
        or ((profile or {}).get("description") or "").strip()
        or (brokerage.description if brokerage else "")
    )


async def profiles_by_server(
    user_id: str,
    rows: Iterable[Mapping[str, Any]],
    connections: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Catalog row name -> its scope's current description, for a brokerage the
    one written for the grant its connection holds. With the service switched
    off, none: every row keeps its own text."""
    config = get_mcp_descriptions_config()
    if not config.enabled:
        return {}
    scopes = {}
    for row in rows:
        try:
            identity = dial_identity(row["name"], row)
        except Exception:
            # A row that does not parse is skipped by the resolver too, which
            # logs it; it must not take the rows that do parse down with it.
            continue
        scope = Scope.of(
            user_id, row["name"], row.get("url"), connections.get(row["name"]), identity
        )
        if scope.brokerage is not None or config.user_servers:
            scopes[row["name"]] = scope.key
    # A failed read fails the resolve, which is retried: a fallback stamped at
    # this config version would stand until something bumps it again.
    found = await db.current_descriptions(
        sorted(set(scopes.values())), prompt_version=PROFILE_PROMPT_VERSION
    )
    return {name: {"description": found[key]} for name, key in scopes.items() if key in found}
