"""The wizard's two post-install steps, replayed from the stored manifests.

Both write components or credentials after the plugin row already exists, so
both owe exactly what install owes: the fan-out, and the vault invalidation
that follows anything a ``${vault:NAME}`` reference resolves through. They
live here rather than in the router because that invalidation is a step a
second write path can silently omit, and one already had.
"""

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from src.server.database.mcp_servers import bump_user_workspaces_mcp_version
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
    update_user_secret,
)
from src.server.models.plugin import ComponentResult, InstallReport
from src.server.services.plugins.errors import PluginRejected
from src.server.services.plugins.extension import (
    NAMESPACE,
    LangalphaExtension,
    materialize_binds,
    parse_extension,
)
from src.server.services.plugins.grants import resolve_bind_grants
from src.server.services.plugins.manifest import manifest_extension
from src.server.services.plugins.mcp import McpEntryPlan, validate_mcp_document
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.vault_invalidation import (
    USER_TIER,
    after_secrets_changed,
)

logger = logging.getLogger(__name__)

MAX_SECRET_VALUE_CHARS = 4096


def stored_extension(plugin: Mapping[str, Any]) -> LangalphaExtension:
    """Parse the plugin's ``ai.langalpha`` namespace. Raises PluginFatal."""
    return parse_extension(
        manifest_extension(plugin.get("manifest") or {}, NAMESPACE)
    )


async def _stored_entry_plans(
    user_id: str, plugin: Mapping[str, Any]
) -> list[McpEntryPlan]:
    """Re-derive the entry plans, binds included, from what was installed."""
    _doc, plans, _diags = validate_mcp_document(
        json.dumps(plugin.get("mcp_document")).encode(),
        plugin_schema=(plugin.get("manifest") or {}).get("$schema"),
    )
    extension = stored_extension(plugin)
    grants = await resolve_bind_grants(
        user_id, extension, plugin_id=plugin["user_plugin_id"]
    )
    materialize_binds(extension, plans, grants.granted)
    return plans


async def apply_sse_upgrades(
    user_id: str, plugin: Mapping[str, Any], keys: Sequence[str]
) -> InstallReport:
    """Install consented held-back sse entries as streamable HTTP.

    The plans are re-derived from the stored manifests, so an upgrade months
    after install still lands the declared configuration.
    """
    sse_by_key = {
        p.key: p
        for p in await _stored_entry_plans(user_id, plugin)
        if p.skip_code is None and p.transport == "sse"
    }
    report = InstallReport()
    consented = []
    for key in keys:
        plan = sse_by_key.get(key)
        if plan is None:
            report.components.append(
                ComponentResult(
                    kind="mcp", key=key, status="error",
                    reason="not a held-back sse entry of this plugin",
                )
            )
            continue
        # Consent recorded: install as the modern transport it probed for.
        plan.transport = "http"
        consented.append(plan)
    if not consented:
        return report

    await fan_out_servers(
        user_id, plugin["user_plugin_id"], consented, report
    )
    await after_secrets_changed(
        USER_TIER, user_id, report.secrets_created, user_id=user_id
    )
    if report.servers_created:
        await bump_user_workspaces_mcp_version(user_id)
    logger.info(
        f"[plugins] sse upgrade user_id={user_id} name={plugin['name']} "
        f"servers={report.servers_created}"
    )
    return report


async def apply_bindings(
    user_id: str, plugin: Mapping[str, Any], secrets: Mapping[str, str]
) -> list[str]:
    """Fill declared plugin secrets into the user vault (create or update).

    Only names the plugin's ``ai.langalpha`` extension declares are accepted:
    this is the wizard's bindings step, not a general vault write. Raises
    PluginRejected for an undeclared name or an unusable value, and ValueError
    when the vault itself refuses the write.
    """
    extension = stored_extension(plugin)
    declared = {s.name: s for s in extension.secrets}
    unknown = sorted(set(secrets) - set(declared))
    if unknown:
        raise PluginRejected(
            f"not declared by this plugin: {', '.join(unknown)}"
        )
    for value in secrets.values():
        if not value or len(value) > MAX_SECRET_VALUE_CHARS:
            raise PluginRejected(
                f"secret values must be 1-{MAX_SECRET_VALUE_CHARS} characters"
            )

    # Declaring a name is not owning it. The same grant test the binds use:
    # without it this step overwrites whatever the user already had under that
    # name, and the wizard never even shows the field, because it lists only
    # names the vault is missing.
    grants = await resolve_bind_grants(
        user_id, extension, plugin_id=plugin["user_plugin_id"]
    )
    ungranted = sorted(set(secrets) - grants.granted)
    if ungranted:
        raise PluginRejected(
            f"your vault already holds {', '.join(ungranted)} for something "
            "else; edit it under Secrets if you want to change the value"
        )

    existing = await get_user_secret_names(user_id)
    written: list[str] = []
    for name, value in secrets.items():
        blueprint = declared[name]
        if name in existing:
            await update_user_secret(
                user_id, name, value=value, description=None
            )
        else:
            await create_user_secret(
                user_id,
                name,
                value,
                blueprint.description or f"Required by plugin {plugin['name']}",
            )
        written.append(name)
    # A filled blueprint is what makes a dangling ${vault:NAME} on an
    # already-enabled server resolve; the caches must not keep the old view.
    await after_secrets_changed(USER_TIER, user_id, written, user_id=user_id)
    return written
