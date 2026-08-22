"""Plugin install / uninstall orchestration.

Install validates everything it can before the first write (fatal manifest
or extension problems never touch the DB), creates the plugin row, then fans
components into the existing primitives. Install is deliberately NOT one
transaction — a crash leaves partial components that ``POST /{name}/update``
reconciles from the stored manifests, which is why the row's content_hash is
stamped last: it claims a tree is installed, and until the fan-out has run
that claim is false. Uninstall IS one transaction, through the component
delete helpers that own the side-effect purges, satisfying the RESTRICT FKs
before the plugin row drops.
"""

import logging
from typing import Any

from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    delete_catalog_server,
    list_catalog_servers,
)
from src.server.database.plugins import (
    create_plugin,
    delete_plugin_row,
    get_plugin,
    list_plugin_server_names,
    list_plugin_skill_names,
    lock_plugin_row,
    stamp_plugin_content_hash,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_skills import delete_user_skill
from src.server.models.plugin import (
    Diagnostic,
    InstallReport,
    UninstalledComponents,
)
from src.server.services.mcp_oauth.lifecycle import oauth_fence
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.grants import resolve_bind_grants
from src.server.services.plugins.package import (
    ValidatedPackage,
    pending_secret_declarations,
)
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.plugins.skill_fanout import fan_out_skills
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.vault_invalidation import (
    USER_TIER,
    after_secrets_changed,
)

logger = logging.getLogger(__name__)


async def _refuse_owned_names(user_id: str, package: ValidatedPackage) -> None:
    """Refuse a component name another plugin owns.

    Per-entry skips cover hand-made rows; a cross-plugin collision is an
    ownership conflict only the user can resolve.
    """
    by_name = {r["name"]: r for r in await list_catalog_servers(user_id)}
    for plan in package.entry_plans:
        if not plan.installable:
            continue
        owner = by_name.get(plan.name)
        if owner is not None and owner.get("plugin_id"):
            owner_name = owner.get("plugin_name") or "another plugin"
            raise ValueError(
                f"MCP server {plan.name!r} is owned by plugin "
                f"{owner_name!r}; uninstall it first"
            )


async def install_plugin_package(
    user_id: str,
    package: ValidatedPackage,
    *,
    source_type: str,
    source_ref: str | None,
) -> tuple[dict[str, Any], InstallReport]:
    """Create the plugin row and fan its components out.

    Raises PluginFatal (from the caller's validate_package), and ValueError
    for whole-install refusals: duplicate plugin, plugin cap, or a component
    name owned by another plugin. The duplicate check runs first, so a user
    reinstalling a plugin they already have is told to update it rather than
    told to uninstall one of its own servers.
    """
    if await get_plugin(user_id, package.name) is not None:
        raise ValueError(
            f"Plugin {package.name!r} is already installed; use update"
        )
    await _refuse_owned_names(user_id, package)

    plugin_row = await create_plugin(
        user_id,
        package.name,
        version=package.version,
        source_type=source_type,
        source_ref=source_ref,
        manifest=package.manifest,
        mcp_document=package.mcp_document,
    )

    report = InstallReport(
        diagnostics=list(package.diagnostics),
        dropped_files=list(package.dropped_files),
    )
    plugin_id = plugin_row["user_plugin_id"]
    # plugin_id=None: a first install owns no rows, so the vault alone decides
    # which declared names this package is allowed to reference.
    grants = await resolve_bind_grants(user_id, package.extension, plugin_id=None)
    materialize_binds(package.extension, package.entry_plans, grants.granted)
    if grants.refused:
        report.diagnostics.append(
            Diagnostic(
                level="warning", scope="plugin", code="secret_not_granted",
                message=grants.refusal_reason(),
            )
        )
    await fan_out_servers(user_id, plugin_id, package.entry_plans, report)
    await fan_out_skills(user_id, plugin_id, package.skill_plans, report)

    # Embedded literals the import loop vaulted: their refs may complete a
    # dangling ${vault:NAME} on an already-enabled server.
    await after_secrets_changed(
        USER_TIER, user_id, report.secrets_created, user_id=user_id
    )

    report.secrets_required = await pending_secret_declarations(user_id, package)

    if report.servers_created:
        # Components land enabled, so every workspace's effective set changed.
        await bump_user_workspaces_mcp_version(user_id)

    if report.landed_whole:
        plugin_row = (
            await stamp_plugin_content_hash(
                user_id, package.name, package.content_hash
            )
            or plugin_row
        )

    logger.info(
        f"[plugins] install user_id={user_id} name={package.name} "
        f"servers={report.servers_created} skills={report.skills_created} "
        f"complete={report.landed_whole}"
    )
    return plugin_row, report


async def uninstall_plugin(
    user_id: str, plugin: dict[str, Any]
) -> UninstalledComponents:
    """Delete a plugin and every component it still owns, in one transaction.

    Detached rows (plugin_id cleared by an edit) survive by design, which is
    why the enumeration and the deletes have to see the same snapshot: read on
    a separate connection first, a Customize landing in between would be read
    as still-owned and the user's forked row deleted anyway. The plugin row's
    write lock is taken before any of it, so a concurrent update cannot add a
    component behind the enumeration and leave the RESTRICT FK to abort a
    transaction whose OAuth fence has already cut live grants.

    The fence spans the whole drop: its teardown writes its own state on its
    own connection, so it cannot join this transaction.
    """
    plugin_id = plugin["user_plugin_id"]
    # Outside the transaction, only to name the fence. The authoritative
    # enumeration is the one under the lock below.
    fenced = [s["name"] for s in await list_plugin_server_names(user_id, plugin_id)]

    dropped_archives: list[str | None] = []
    servers: list[dict[str, Any]] = []
    skills: list[dict[str, Any]] = []
    async with oauth_fence(user_id, fenced):
        async with get_db_connection() as conn:
            async with conn.transaction():
                if await lock_plugin_row(user_id, plugin_id, conn=conn) is None:
                    return UninstalledComponents(servers=[], skills=[])
                servers = await list_plugin_server_names(
                    user_id, plugin_id, conn=conn
                )
                skills = await list_plugin_skill_names(
                    user_id, plugin_id, conn=conn
                )
                for server in servers:
                    await delete_catalog_server(
                        user_id, server["name"],
                        owned_by_plugin=plugin_id, conn=conn,
                    )
                for skill in skills:
                    row = await delete_user_skill(
                        user_id,
                        skill["name"],
                        workspace_id=skill.get("workspace_id"),
                        owned_by_plugin=plugin_id,
                        conn=conn,
                    )
                    if row:
                        dropped_archives.append(row.get("archive_key"))
                await delete_plugin_row(user_id, plugin_id, conn=conn)

    for key in dropped_archives:
        await drop_archive_if_unused(user_id, key)

    name = plugin["name"]

    logger.info(
        f"[plugins] uninstall user_id={user_id} name={name} "
        f"servers={len(servers)} skills={len(skills)}"
    )
    return UninstalledComponents(
        servers=[s["name"] for s in servers],
        skills=[s["name"] for s in skills],
    )
