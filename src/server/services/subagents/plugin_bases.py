"""The subagents a user's plugins ship, as bases for the merge.

The bundles' ``agents/`` files, which the image fixes and each process reads
once, and installed plugins' rows (``plugin_subagents``), read per call as the
rest of a plugin's components are, so installing, updating, switching or
removing a plugin leaves nothing cached to go stale.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from ptc_agent.agent.subagents.merge import BaseSubagent, plugin_definition


def _base(plugin: str, row: Mapping[str, Any]) -> BaseSubagent:
    from src.server.database.plugin_subagents import plugin_key

    return BaseSubagent(plugin_key(plugin, row["name"]), plugin_definition(row))


async def plugin_bases(user_id: str, *, conn=None) -> list[tuple[BaseSubagent, bool]]:
    """Each plugin subagent with whether its plugin is on: bundles first, in
    bundle order, then installed plugins in install order."""
    from src.server.database.plugin_subagents import list_user_plugin_subagents
    from src.server.services.plugins.bundled import bundled_agent_plans

    bundled = bundled_agent_plans()
    off: Collection[str] = ()
    if bundled:
        from src.server.database.account_disables import list_account_disables

        # On the caller's connection: a caller passing one is holding it, and a
        # second checkout beside it can wait out the pool timeout.
        off = (await list_account_disables(user_id, conn=conn)).bundles
    rows = await list_user_plugin_subagents(user_id, conn=conn)
    return [
        *((_base(bundle, plan.row), bundle not in off) for bundle, plan in bundled),
        *((_base(row["plugin_name"], row), bool(row["plugin_enabled"])) for row in rows),
    ]
