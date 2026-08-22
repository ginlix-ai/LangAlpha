"""Fan a package's mcp.json entries into user_mcp_servers rows.

Entries land through the shared import loop, so a plugin install and a
hand-pasted ``mcpServers`` blob get the identical per-entry gauntlet
(reserved names, cap, embedded-credential vaulting, model validation) and
the same per-entry isolation: one bad entry never aborts the others. Legacy
sse entries are held back rather than installed — they are probed and
reported ``upgradable`` for the wizard's consent step.
"""

import logging

from pydantic import ValidationError

from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_catalog_server,
    list_catalog_servers,
)
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.models.mcp_server import (
    McpServerInput,
    ParsedMcpServer,
    isolation_warnings,
)
from src.server.models.plugin import ComponentResult, InstallReport
from src.server.services.mcp_config import builtin_names
from src.server.services.mcp_import import ImportScope, run_mcp_import
from src.server.services.plugins.mcp import McpEntryPlan

logger = logging.getLogger(__name__)


def _entry_warnings(plan: McpEntryPlan) -> list[str]:
    """Isolation nudges for an installed entry; policy-only, never blocking."""
    try:
        return isolation_warnings(McpServerInput(**plan.config))
    except ValidationError:
        # The import loop validated the vault-extracted variant; the raw
        # config can legally fail here. The warning is a nicety — drop it.
        return []


async def fan_out_servers(
    user_id: str,
    plugin_id: str,
    plans: list[McpEntryPlan],
    report: InstallReport,
) -> None:
    """Create a catalog row per installable plan, reporting every plan."""
    installable = [p for p in plans if p.installable]
    sse_plans = [
        p for p in plans if p.skip_code is None and p.transport == "sse"
    ]
    probed = {}
    if sse_plans:
        from src.server.services.plugins.probe import probe_all

        probed = {
            r.key: r
            for r in await probe_all(
                [(p.key, p.config["url"]) for p in sse_plans]
            )
        }
    for plan in plans:
        report.diagnostics.extend(plan.diagnostics)
        if plan.skip_code is not None:
            report.components.append(
                ComponentResult.of(
                    plan, "skipped", reason=plan.skip_reason or ""
                )
            )
        elif plan.transport == "sse":
            result = probed.get(plan.key)
            if result is not None and result.ok:
                reason = (
                    "legacy sse transport, but the endpoint answers "
                    "streamable HTTP — consent to install the upgrade"
                )
                if result.detail:
                    reason += f" ({result.detail})"
                status = "upgradable"
            else:
                status = "skipped"
                reason = (
                    "legacy sse transport; the endpoint did not answer a "
                    "streamable HTTP probe"
                )
                if result is not None and result.detail:
                    reason += f" ({result.detail})"
            report.components.append(
                ComponentResult.of(plan, status, reason=reason)
            )
    if not installable:
        return

    existing_rows = await list_catalog_servers(user_id)
    existing_names = {r["name"] for r in existing_rows}
    # First key wins, not last: two package keys can normalize to one MCP name
    # (`foo-bar` and `foo.bar`), and the import loop creates the first and
    # skips the rest as duplicates. Keeping the last key would stamp the
    # created row with the skipped entry's provenance, so a later update would
    # reconcile it against the wrong entry.
    key_by_name: dict[str, str] = {}
    for plan in installable:
        key_by_name.setdefault(plan.name, plan.key)

    async def create_secret(conn, secret) -> None:
        await create_user_secret(
            user_id, secret.name, secret.value, secret.description, conn=conn
        )

    async def persist(conn, server: McpServerInput) -> bool:
        await create_catalog_server(
            user_id,
            server.name,
            conn=conn,
            enabled=True,
            plugin_id=plugin_id,
            plugin_server_key=key_by_name[server.name],
            **server.to_catalog_fields(),
        )
        return True

    parsed = [
        ParsedMcpServer(
            original_name=p.key, name=p.name, renamed=p.renamed, config=p.config
        )
        for p in installable
    ]
    mcp_report = await run_mcp_import(
        parsed,
        scope=ImportScope(
            reserved_names=builtin_names(),
            existing_names=existing_names,
            current_count=len(existing_names),
            cap=MAX_CATALOG_SERVERS_PER_USER,
            cap_message=(
                f"Plugins server cap ({MAX_CATALOG_SERVERS_PER_USER}) reached"
            ),
            exists_message=(
                "a server with this name already exists; left untouched"
            ),
            existing_secret_names=set(await get_user_secret_names(user_id)),
            create_secret=create_secret,
            persist=persist,
        ),
    )

    plans_by_key = {p.key: p for p in installable}
    for result in mcp_report.results:
        plan = plans_by_key[result["original_name"]]
        status = result["status"]
        report.components.append(
            ComponentResult.of(
                plan,
                status,
                name=result["name"] or "",
                reason=result.get("reason") or result.get("error") or "",
                warnings=_entry_warnings(plan) if status == "created" else [],
            )
        )
    report.secrets_created.extend(mcp_report.secrets_created)
    report.servers_created += mcp_report.created
