"""Fan a package's ``agents/`` files into plugin_subagents rows, at install
and at update.

The skills' collision policy, per subagent: a name a built-in or a bundle's
subagent holds is skipped, one of the user's own is left untouched
(``exists``), and a name another plugin owns is refused for the whole install
before anything is written (``lifecycle._refuse_owned_names``).
"""

from __future__ import annotations

import logging
from typing import Any

from src.server.database.plugin_subagents import (
    MAX_PLUGIN_SUBAGENTS_PER_USER,
    clear_plugin_overrides,
    count_plugin_subagents,
    delete_plugin_subagent,
    insert_plugin_subagent,
    list_plugin_subagents,
    overrides_locked,
    replace_plugin_subagent,
)
from src.server.database.plugins import list_plugin_server_names
from src.server.database.user_subagents import list_user_subagents
from src.server.models.plugin import ComponentResult, Diagnostic, InstallReport
from src.server.services.plugins.agents import AgentPlan, link_connectors
from src.server.services.plugins.reconcile import ReconcileArms, reconcile

logger = logging.getLogger(__name__)

SUBAGENT_EXISTS_REASON = "a subagent with this name already exists; left untouched"
_RESERVED_REASON = "a built-in subagent has this name"
_CAP_REASON = (
    f"your plugins already add {MAX_PLUGIN_SUBAGENTS_PER_USER} subagents, the "
    "per-account limit; uninstall a plugin and update this one to add the rest"
)


def reserved_subagent_names() -> frozenset[str]:
    """Names a plugin's subagent can't take: the deployment's and the bundles'."""
    from ptc_agent.agent.subagents.merge import base_subagents
    from src.server.app import setup
    from src.server.services.plugins.bundled import bundled_agent_plans

    defined = (
        set(base_subagents(setup.agent_config.subagents))
        if setup.agent_config is not None
        else set()
    )
    return frozenset(defined | {plan.name for _, plan in bundled_agent_plans()})


async def _linked(
    user_id: str, plugin_id: str, plugin_name: str, plans: list[AgentPlan], *, conn
) -> list[AgentPlan]:
    """The plans linked to the servers the plugin holds, read after the
    server fan-out.

    Not to the package's entries: one left ``exists`` shares its name with a
    server of the user's, which the package never described.
    """
    if not any("preferred_connectors" in p.fields for p in plans):
        return plans
    rows = await list_plugin_server_names(user_id, plugin_id, conn=conn)
    return link_connectors(
        plans, {r["plugin_server_key"] or r["name"]: r["name"] for r in rows}, plugin_name
    )


async def relink_subagents(user_id: str, plugin_id: str, plugin_name: str) -> None:
    """Link the plugin's stored subagents to servers that landed after them.

    A held-back sse entry lands only when the user consents, so a subagent
    that prefers it still names the plugin's server until then; an update
    would link it, and this does it without one.
    """
    async with overrides_locked(user_id) as conn:
        rows = await list_plugin_subagents(user_id, plugin_id, conn=conn)
        plans = [
            AgentPlan(
                name=r["name"], text=r["file_text"], fields=r["fields"],
                instructions=r["instructions"], roles=r["roles"],
            )
            for r in rows
        ]
        for row, plan in zip(rows, await _linked(user_id, plugin_id, plugin_name, plans, conn=conn)):
            if plan.fields != row["fields"]:
                await replace_plugin_subagent(user_id, plugin_id, plan.row, plan.text, conn=conn)


async def fan_out_subagents(
    user_id: str,
    plugin_id: str,
    plans: list[AgentPlan],
    report: InstallReport,
    *,
    conn,
) -> None:
    """Create a row per plan that reads, reporting every plan.

    ``conn`` is an ``overrides_locked`` transaction, so a custom subagent the
    pane creates meanwhile can't take a name between the read and the insert.
    """
    if not plans:
        return
    reserved = reserved_subagent_names()
    own = {
        r["name"]
        for r in await list_user_subagents(user_id, conn=conn)
        if r.get("kind") == "custom"
    }
    held = await count_plugin_subagents(user_id, conn=conn)

    for plan in plans:
        report.diagnostics.extend(plan.diagnostics)
        if plan.skip_code is not None:
            report.components.append(
                ComponentResult.of(plan, "skipped", reason=plan.skip_reason or "")
            )
            continue
        if plan.name in reserved:
            report.components.append(
                ComponentResult.of(plan, "skipped", reason=_RESERVED_REASON)
            )
            report.diagnostics.append(Diagnostic(
                scope="subagent", target=plan.name, code="reserved_name",
                message=_RESERVED_REASON,
            ))
            continue
        if plan.name in own:
            report.components.append(
                ComponentResult.of(plan, "exists", reason=SUBAGENT_EXISTS_REASON)
            )
            continue
        if held >= MAX_PLUGIN_SUBAGENTS_PER_USER:
            report.components.append(ComponentResult.of(plan, "error", reason=_CAP_REASON))
            report.diagnostics.append(Diagnostic(
                level="error", scope="subagent", target=plan.name,
                code="cap_reached", message=_CAP_REASON,
            ))
            continue
        row = await insert_plugin_subagent(user_id, plugin_id, plan.row, plan.text, conn=conn)
        if row is None:
            # Taken between the refusal and this write.
            report.components.append(
                ComponentResult.of(plan, "exists", reason=SUBAGENT_EXISTS_REASON)
            )
            continue
        held += 1
        report.subagents_created += 1
        report.components.append(ComponentResult.of(plan, "created"))


async def install_subagents(
    user_id: str,
    plugin_id: str,
    plugin_name: str,
    plans: list[AgentPlan],
    report: InstallReport,
) -> None:
    """A fresh install's subagents, in one transaction under the override lock."""
    async with overrides_locked(user_id) as conn:
        # The plugin row is new, so overrides under its name are from an
        # earlier install whose uninstall left them (an older build's
        # dropped only the rows); kept, they would apply to this package.
        # None is the user's since: no base existed to override, pending
        # sign-in included, until this fan-out commits.
        cleared = await clear_plugin_overrides(user_id, plugin_name, conn=conn)
        if cleared:
            logger.info(
                f"[plugin_subagents] cleared stale overrides user_id={user_id} "
                f"plugin={plugin_name} overrides={cleared}"
            )
        plans = await _linked(user_id, plugin_id, plugin_name, plans, conn=conn)
        await fan_out_subagents(user_id, plugin_id, plans, report, conn=conn)


async def reconcile_subagents(
    user_id: str,
    plugin_id: str,
    plugin_name: str,
    plans: list[AgentPlan],
    report: InstallReport,
) -> None:
    """Reconcile the plugin's subagents by file name.

    A user's change to one is an override row, never an edit of the plugin's,
    so nothing detaches: an update replaces the base under the override, and
    the version the pane holds moves with it. One the package dropped goes
    with the user's changes to it, as at uninstall. All of it runs in one
    transaction under the override lock, so a pane save lands wholly before
    it or diffs against the bases it leaves.
    """
    async with overrides_locked(user_id) as conn:
        incoming = {
            p.name: p
            for p in await _linked(user_id, plugin_id, plugin_name, plans, conn=conn)
        }
        owned = {
            r["name"]: r
            for r in await list_plugin_subagents(user_id, plugin_id, conn=conn)
        }

        async def delete(name: str, _row: dict[str, Any]) -> None:
            if await delete_plugin_subagent(user_id, plugin_id, plugin_name, name, conn=conn):
                report.components.append(
                    ComponentResult(kind="subagent", key=name, name=name, status="deleted")
                )

        async def update(name: str, row: dict[str, Any], plan: AgentPlan) -> None:
            report.diagnostics.extend(plan.diagnostics)
            if plan.skip_code is not None:
                report.components.append(ComponentResult.of(
                    plan, "skipped", reason=(plan.skip_reason or "") + "; existing one kept",
                ))
                return
            stored = (row["file_text"], row["fields"], row["instructions"], row["roles"])
            if stored == (plan.text, plan.fields, plan.instructions, plan.roles):
                report.components.append(ComponentResult.of(plan, "unchanged"))
                return
            # Under the fan-out lock, which uninstall holds too, so the row is there.
            await replace_plugin_subagent(user_id, plugin_id, plan.row, plan.text, conn=conn)
            report.components.append(ComponentResult.of(plan, "updated"))

        async def detached(_name: str, _plan: AgentPlan) -> bool:
            return False

        async def create(fresh: list[AgentPlan]) -> None:
            await fan_out_subagents(user_id, plugin_id, fresh, report, conn=conn)

        await reconcile(
            owned,
            incoming,
            arms=ReconcileArms(
                delete=delete, update=update, create=create, detached=detached
            ),
        )
