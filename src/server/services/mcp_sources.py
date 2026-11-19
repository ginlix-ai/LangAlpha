"""Where the MCP servers a turn may use come from, each source gated once.

Resolution gathers the user's own tier from here and nowhere else (pinned
by ``tests/unit/server/test_mcp_sources_pin.py``), so a reader that forgot
the policy cannot put a server the policy withholds back in a turn.
"""

from __future__ import annotations

from typing import Any

from src.server.database import mcp_servers as mcp_db
from src.server.services.policy import (
    USER_CONNECTORS,
    Policy,
    governed_by_user_connectors,
)


async def user_tier_rows(
    user_id: str, *, policy: Policy | None = None
) -> list[dict[str, Any]] | None:
    """The user's own enabled servers the policy lets a turn use, or None
    when the policy is unknown.

    Under ``user_connectors: false`` only brokerages stay; the rows are not
    touched, so lifting the rule brings every one back unchanged. The rows are
    read only once the policy allows something to come of them.
    """
    if policy is None:
        from src.server.services.account import policy_for

        policy = await policy_for(user_id)
        if policy is None:
            return None
    rows = await mcp_db.list_enabled_user_servers(user_id)
    if policy.allows(USER_CONNECTORS):
        return rows
    return [row for row in rows if not governed_by_user_connectors(row.get("url"))]
