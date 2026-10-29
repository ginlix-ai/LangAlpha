"""Plugin install / uninstall orchestration.

Install validates everything it can before the first write (fatal manifest
or extension problems never touch the DB) and checks every server the plugin
would create (``check_plugin_install``) with the secret values the request
supplied, before any of those values reach the vault. Only then does it write
the values, create the plugin row and fan components into the existing
primitives. Install is deliberately NOT one transaction — a crash leaves
partial components that ``POST /{name}/update`` reconciles from the stored
manifests, which is why the row's content_hash is stamped last: it claims a
tree is installed, and until the fan-out has run that claim is false.
Uninstall IS one transaction, through the component delete helpers that own
the side-effect purges, satisfying the RESTRICT FKs before the plugin row
drops.
"""

import copy
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    delete_catalog_server,
    list_catalog_servers,
)
from src.server.database.plugins import (
    MAX_PLUGINS_PER_USER,
    claim_plugin_secrets,
    create_plugin,
    delete_plugin_row,
    get_plugin,
    list_plugin_server_names,
    list_plugin_skill_names,
    list_plugins,
    lock_plugin_row,
    plugin_fan_out_lock,
    stamp_plugin_content_hash,
)
from src.server.database.plugin_subagents import (
    delete_plugin_subagents,
    list_user_plugin_subagent_names,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_skills import delete_user_skill, list_user_skills
from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.models.plugin import (
    Diagnostic,
    InstallReport,
    UninstalledComponents,
)
from src.server.services.mcp_oauth.lifecycle import oauth_fence
from src.server.services.mcp_validation import (
    McpCheckFailed,
    McpSignInRequired,
    SignInNeeded,
)
from src.server.services.plugins.bundled import bundled_names
from src.server.services.plugins.checks import (
    CheckedServers,
    Prospect,
    creatable_servers,
    sign_in_unsupported,
)
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.gate import (
    Due,
    check_due,
    readers,
    refuse_unapproved,
    secret_gate,
)
from src.server.services.plugins.grants import (
    BindGrants,
    disclose_undeclared_refs,
    disclose_vaulted_literals,
)
from src.server.services.plugins.mcp import McpEntryPlan
from src.server.services.plugins.package import (
    ValidatedPackage,
    pending_secret_declarations,
)
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.plugins.skill_fanout import (
    fan_out_skills,
    installs_under_dir,
)
from src.server.services.plugins.subagent_fanout import install_subagents
from src.server.services.plugins.supplied_secrets import (
    bind_placeholders,
    drop_secrets,
    refuse_vault_overflow,
    validate_supplied,
    write_supplied,
)
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.vault_invalidation import after_secrets_changed

logger = logging.getLogger(__name__)


async def _refuse_owned_names(
    user_id: str, package: ValidatedPackage
) -> dict[str, dict[str, Any]]:
    """Refuse a component name another plugin owns. Returns the catalog rows
    it read, by name.

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

    agents = {p.name for p in package.agent_plans if p.skip_code is None}
    if agents:
        for row in await list_user_plugin_subagent_names(user_id):
            if row["name"] in agents:
                raise ValueError(
                    f"Subagent {row['name']!r} is owned by plugin "
                    f"{row['plugin_name']!r}; uninstall it first"
                )

    skills = [p for p in package.skill_plans if p.skip_code is None]
    if not skills:
        return by_name
    # The fan-out installs a skill only under its directory name, so the
    # directory finds the collision, and only a colliding archive is unpacked.
    # One the fan-out would drop never takes the name, and uninstalling the
    # owner would not change that.
    by_skill = {r["name"]: r for r in await list_user_skills(user_id)}
    for plan in skills:
        owner = by_skill.get(plan.dir)
        if owner is None or not owner.get("plugin_id"):
            continue
        if not await installs_under_dir(plan):
            continue
        owner_name = owner.get("plugin_name") or "another plugin"
        raise ValueError(
            f"Skill {plan.dir!r} is owned by plugin {owner_name!r}; "
            "uninstall it first"
        )
    return by_name


async def _refuse_install(
    user_id: str, package: ValidatedPackage
) -> dict[str, dict[str, Any]]:
    """Every whole-install refusal, in the order a user can act on them.

    The shipped-name check runs first because it is the only one no user
    action resolves, and the duplicate check runs before the component scan so
    a user reinstalling a plugin they already have is told to update it rather
    than told to uninstall one of its own servers.

    That first check is also what keeps the plugin name unambiguous. A shipped
    bundle has no row here, so nothing else stops a user installing over one,
    and both would then answer to the same name on every route that addresses
    a package by it.

    Run once before the checks, so a plugin that cannot land never dials
    anything, and again under the fan-out lock, where it is binding.
    """
    if package.name in bundled_names():
        raise ValueError(
            f"{package.name!r} is the name of a package that already ships "
            "with the app; rename the plugin and install it again"
        )
    if await get_plugin(user_id, package.name) is not None:
        raise ValueError(
            f"Plugin {package.name!r} is already installed; use update"
        )
    by_name = await _refuse_owned_names(user_id, package)
    if len(await list_plugins(user_id)) >= MAX_PLUGINS_PER_USER:
        raise ValueError(
            f"Maximum of {MAX_PLUGINS_PER_USER} plugins per user reached"
        )
    return by_name


def _creatable(
    plans: list[McpEntryPlan],
    by_name: Mapping[str, Any],
    held: Iterable[str],
    supplied: Mapping[str, str],
) -> list[Prospect]:
    """The servers the install would create, refused whole past the cap.

    The import loop would land the ones that fit and report the rest, which
    is a plugin missing servers it was installed for.
    """
    prospects = creatable_servers(
        plans, existing_names=set(by_name), secret_names=set(held) | set(supplied)
    )
    room = MAX_CATALOG_SERVERS_PER_USER - len(by_name)
    if len(prospects) > room:
        raise ValueError(
            f"this plugin adds {len(prospects)} servers and your account has "
            f"room for {max(room, 0)} more (the limit is "
            f"{MAX_CATALOG_SERVERS_PER_USER})"
        )
    return prospects


@dataclass(frozen=True)
class _InstallPlan:
    """What an install would write, once every refusal has passed."""

    supplied: dict[str, str]
    grants: BindGrants
    # The package's entry plans with their binds written in. A copy: the
    # package itself is what each pass plans from.
    plans: list[McpEntryPlan]
    prospects: list[Prospect]
    # Rows already in the catalog that resolve a value the request supplies.
    readers: list[Due]

    @property
    def due(self) -> list[Due]:
        """Every server the install owes a check: the ones it creates, and
        the ones its values reach."""
        return [
            *(Due(p.server, moves=True, literals=p.literals) for p in self.prospects),
            *self.readers,
        ]


async def _plan_install(
    user_id: str, package: ValidatedPackage, secrets: Mapping[str, str] | None
) -> _InstallPlan:
    """Every refusal, cheapest first, then the servers the install would create.

    Run before the check, so a plugin that cannot land never dials a vendor,
    and again under the fan-out lock, where it is binding. ``plugin_id=None``
    because a first install owns no rows: the vault alone decides which
    declared names it may reference, read under the lock before the values
    are written, or a name this install introduces would read as one the user
    already held.
    """
    by_name = await _refuse_install(user_id, package)
    extension = package.extension
    plans = copy.deepcopy(package.entry_plans)
    bind_placeholders(plans, taken=await get_user_secret_names(user_id))
    supplied = validate_supplied(extension, secrets, entries=plans)
    grants = await secret_gate(
        user_id, extension, supplied, plugin_id=None, require_declared=True
    )
    materialize_binds(
        extension, plans, grants.granted,
        document_dropped=package.mcp_document_invalid,
    )
    prospects = _creatable(plans, by_name, grants.held, supplied)
    refuse_vault_overflow(
        grants.held, [*supplied, *(n for p in prospects for n in p.literals)]
    )
    return _InstallPlan(
        supplied, grants, plans, prospects, readers(by_name.values(), supplied)
    )


@dataclass(frozen=True)
class InstallCheck:
    """What a passed install check proved: the servers, and at most one
    sign-in still to happen (only when the caller allowed one)."""

    checked: CheckedServers
    sign_in: SignInNeeded | None = None


async def check_plugin_install(
    user_id: str,
    package: ValidatedPackage,
    *,
    secrets: Mapping[str, str] | None = None,
    allow_sign_in: bool = False,
) -> InstallCheck:
    """Prove the install can land, writing nothing.

    Every refusal first (``_plan_install``), then every server the install
    would create is checked, each as it would be saved, the supplied values
    standing in for the vault writes that follow, and so is every row already
    there that will read one of those values, whoever owns it: a value
    written for this plugin reaches them too. Raises McpCheckFailed with
    every failure.

    A sign-in is the one outcome a check cannot settle. Without
    ``allow_sign_in`` it raises McpSignInRequired, which the client answers by
    starting a pending sign-in; that start passes ``allow_sign_in`` and gets
    the server back, approved, for its callback to install. More than one is
    refused either way: a callback completes one sign-in.
    """
    plan = await _plan_install(user_id, package, secrets)
    due = plan.due
    if not due:
        return InstallCheck(CheckedServers())
    outcome = await check_due(user_id, due, supplied=plan.supplied)
    if outcome.failed:
        raise McpCheckFailed(outcome.failed)
    if len(outcome.sign_in) > 1:
        names = ", ".join(s.server for s in outcome.sign_in)
        raise sign_in_unsupported(
            outcome.sign_in,
            f"This plugin needs a sign-in to {len(outcome.sign_in)} servers "
            f"({names}), and an install can complete one. Add them one at a "
            "time instead.",
        )
    if outcome.sign_in and not allow_sign_in:
        raise McpSignInRequired(outcome.sign_in)
    sign_in = outcome.sign_in[0] if outcome.sign_in else None
    approved = [
        d.server
        for d in due
        if d.server.name in outcome.passed
        or (sign_in is not None and d.server.name == sign_in.server)
    ]
    return InstallCheck(CheckedServers.of(approved), sign_in)


@dataclass
class InstallProgress:
    """What an install has written so far, for a caller that must take it
    back when a later step of the install raises.

    Install is not one transaction (its module docstring says why), so a
    failure after the plugin row exists leaves the row, its secrets and the
    components that landed. The direct routes leave that for update to
    reconcile; a pending sign-in cannot, since its sign-in row would stay on
    with no connection.
    """

    plugin: dict[str, Any] | None = None
    report: InstallReport | None = None
    # Vault names this install created, claimed or not yet.
    introduced: list[str] = field(default_factory=list)


async def install_plugin_package(
    user_id: str,
    package: ValidatedPackage,
    *,
    source_type: str,
    source_ref: str | None,
    secrets: Mapping[str, str] | None = None,
    checked: CheckedServers | None = None,
    progress: InstallProgress | None = None,
) -> tuple[dict[str, Any], InstallReport]:
    """Check the plugin's servers, then write its secrets, row and components.

    ``secrets`` are values for the names the plugin declares or its entries
    reference (``validate_supplied``). ``checked`` is a check that already
    ran: the pending sign-in carries one from its start to its callback,
    minutes apart, and does not dial every vendor twice.
    Without it the check runs here first (``check_plugin_install``), so no
    caller writes a server that never passed. ``progress`` is filled in once
    the plugin row exists, so a caller can still find what was written when a
    later step raises.

    Raises PluginFatal (from the caller's validate_package), PluginRejected or
    PluginRefused for the request's secrets, McpCheckFailed or
    McpSignInRequired from the check, and ValueError for whole-install
    refusals: a name that ships with the app, duplicate plugin, plugin or
    server cap, or a component name owned by another plugin.
    """
    if checked is None:
        checked = (
            await check_plugin_install(user_id, package, secrets=secrets)
        ).checked
    async with plugin_fan_out_lock(user_id):
        # Asked again, for a caller whose check ran minutes ago: a name taken,
        # a secret deleted or the vault filled since would otherwise land.
        plan = await _plan_install(user_id, package, secrets)
        # The fan-out proves each server it creates; a row that came to read
        # one of the values while the check ran has only this.
        refuse_unapproved(plan.readers, checked)
        grants = plan.grants
        written, introduced = await write_supplied(
            user_id, package.name, package.extension, plan.supplied,
            held=grants.held,
        )
        try:
            plugin_row = await create_plugin(
                user_id,
                package.name,
                version=package.version,
                source_type=source_type,
                source_ref=source_ref,
                manifest=package.manifest,
                mcp_document=package.mcp_document,
            )
        except BaseException:
            await drop_secrets(user_id, introduced)
            raise
        plugin_id = plugin_row["user_plugin_id"]
        report = InstallReport(
            diagnostics=list(package.diagnostics),
            dropped_files=list(package.dropped_files),
        )
        if progress is not None:
            progress.plugin = plugin_row
            progress.report = report
            progress.introduced = list(introduced)
        # The claim is the grant record for values this install introduced,
        # which no row may reference yet (an entry held back as sse).
        await claim_plugin_secrets(user_id, plugin_id, introduced)

        disclose_undeclared_refs(plan.plans, grants)
        if grants.preexisting:
            report.diagnostics.append(
                Diagnostic(
                    level="warning", scope="plugin", code="secret_preexisting",
                    message=grants.disclosure_reason(),
                )
            )
        # Each server row commits enabled together with its version bump, so
        # a failure anywhere after this leaves no live row a session misses.
        await fan_out_servers(
            user_id, plugin_id, plan.plans, report, checked=checked
        )
        await fan_out_skills(user_id, plugin_id, package.skill_plans, report)
        await install_subagents(
            user_id, plugin_id, package.name, package.agent_plans, report
        )

        # Under the lock for the reason update writes its row there: an update
        # landing in between would otherwise have its tree stamped as this one.
        if report.landed_whole:
            plugin_row = (
                await stamp_plugin_content_hash(
                    user_id, package.name, package.content_hash
                )
                or plugin_row
            )

    # Embedded literals the import loop vaulted: their refs may complete a
    # dangling ${vault:NAME} on an already-enabled server.
    disclose_vaulted_literals(report)
    await after_secrets_changed(user_id, [*written, *report.secrets_created])

    report.secrets_required = await pending_secret_declarations(user_id, package)

    logger.info(
        f"[plugins] install user_id={user_id} name={package.name} "
        f"servers={report.servers_created} skills={report.skills_created} "
        f"subagents={report.subagents_created} complete={report.landed_whole}"
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

    All of it runs under ``plugin_fan_out_lock``, like install and update: an
    update checks its plugin is still there once under that lock and then
    writes components against it, so an uninstall in between would delete the
    plugin under those writes.
    """
    plugin_id = plugin["user_plugin_id"]
    dropped_archives: list[str | None] = []
    servers: list[dict[str, Any]] = []
    skills: list[dict[str, Any]] = []
    subagents: list[str] = []
    async with plugin_fan_out_lock(user_id):
        # Outside the transaction, only to name the fence. The authoritative
        # enumeration is the one under the row lock below.
        fenced = [
            s["name"] for s in await list_plugin_server_names(user_id, plugin_id)
        ]
        async with oauth_fence(user_id, fenced):
            async with get_db_connection() as conn:
                async with conn.transaction():
                    if await lock_plugin_row(user_id, plugin_id, conn=conn) is None:
                        return UninstalledComponents(servers=[], skills=[], subagents=[])
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
                    # With the user's changes to them, which name the plugin
                    # and so would otherwise outlive it.
                    subagents = await delete_plugin_subagents(
                        user_id, plugin_id, plugin["name"], conn=conn
                    )
                    await delete_plugin_row(user_id, plugin_id, conn=conn)

    for key in dropped_archives:
        await drop_archive_if_unused(user_id, key)

    name = plugin["name"]

    logger.info(
        f"[plugins] uninstall user_id={user_id} name={name} "
        f"servers={len(servers)} skills={len(skills)} subagents={len(subagents)}"
    )
    return UninstalledComponents(
        servers=[s["name"] for s in servers],
        skills=[s["name"] for s in skills],
        subagents=subagents,
    )
