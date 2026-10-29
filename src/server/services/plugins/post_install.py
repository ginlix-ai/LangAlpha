"""The two plugin writes that come after its install, replayed from the
stored manifests: an sse entry's consented upgrade, and a new value for a key
the plugin asks for.

Both write components or credentials after the plugin row already exists, so
both owe exactly what install owes: the check of every server they would
change, the fan-out, and the vault invalidation that follows anything a
``${vault:NAME}`` reference resolves through. They live here rather than in
the router because that invalidation is a step a second write path can
silently omit, and one already had.
"""

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    list_catalog_servers,
)
from src.server.database.plugins import claim_plugin_secrets, plugin_fan_out_lock
from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.models.plugin import (
    ComponentResult,
    Diagnostic,
    InstallReport,
)
from src.server.services.mcp_validation import McpCheckFailed
from src.server.services.plugins.checks import (
    CheckedServers,
    creatable_servers,
)
from src.server.services.plugins.extension import (
    NAMESPACE,
    LangalphaExtension,
    apply_server_metadata,
    materialize_binds,
    parse_extension,
)
from src.server.services.plugins.gate import (
    Due,
    check_due,
    readers,
    refuse_unapproved,
    secret_gate,
    still_installed,
)
from src.server.services.plugins.grants import (
    BindGrants,
    resolve_bind_grants,
    disclose_undeclared_refs,
    disclose_vaulted_literals,
)
from src.server.services.plugins.manifest import manifest_extension
from src.server.services.plugins.mcp import McpEntryPlan, validate_mcp_document
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.plugins.subagent_fanout import relink_subagents
from src.server.services.plugins.supplied_secrets import (
    validate_supplied,
    write_supplied,
)
from src.server.services.vault_invalidation import after_secrets_changed

logger = logging.getLogger(__name__)


def stored_extension(plugin: Mapping[str, Any]) -> LangalphaExtension:
    """The plugin's stored ``ai.langalpha`` namespace, read as install read it.

    Never raises: a part install dropped with a warning is dropped again here,
    silently, since install already reported it.
    """
    return parse_extension(
        manifest_extension(plugin.get("manifest") or {}, NAMESPACE)
    )


async def _stored_document(
    plugin: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, list[McpEntryPlan]]:
    """The stored mcp.json read back into entry plans, as install read it."""
    # Off the loop for the same reason install hops: serializing the stored
    # document and running it back through the schema is GIL-held work whose
    # size is the package's, not ours, and this path re-does all of it.
    doc, plans, _diags = await asyncio.to_thread(
        lambda: validate_mcp_document(
            json.dumps(plugin.get("mcp_document")).encode(),
            plugin_schema=(plugin.get("manifest") or {}).get("$schema"),
        )
    )
    return doc, plans


async def _stored_entry_plans(
    user_id: str, plugin: Mapping[str, Any]
) -> tuple[list[McpEntryPlan], BindGrants]:
    """Re-derive the entry plans, binds included, from what was installed.

    The grants come back with them because a refusal decided here is otherwise
    invisible: this path builds its own report, and an entry that installs
    without the credential it declared looks identical to one that installed
    with it until the first tool call fails.
    """
    doc, plans = await _stored_document(plugin)
    extension = stored_extension(plugin)
    apply_server_metadata(extension, plans, document_dropped=doc is None)
    grants = await resolve_bind_grants(
        user_id, extension, plugin_id=plugin["user_plugin_id"]
    )
    # A plugin stored with no usable MCP component re-reads as one here: the
    # column is NULL, which serializes to "null" and fails at document level.
    materialize_binds(
        extension, plans, grants.granted, document_dropped=doc is None
    )
    disclose_undeclared_refs(plans, grants)
    return plans, grants


async def _check_upgrades(
    user_id: str, plugin: Mapping[str, Any], keys: Sequence[str]
) -> tuple[CheckedServers, dict[str, str]]:
    """Check each consented entry as the upgrade would create it.

    Returns what passed, and why each entry that did not was refused, by
    entry key. An entry the fan-out would refuse on its own (a taken name, no
    room) is not dialled; it is reported there.
    """
    plans, _grants = await _stored_entry_plans(user_id, plugin)
    by_key = {
        p.key: p for p in plans if p.skip_code is None and p.transport == "sse"
    }
    wanted = [by_key[k] for k in dict.fromkeys(keys) if k in by_key]
    for plan in wanted:
        plan.transport = "http"
    rows = await list_catalog_servers(user_id)
    prospects = creatable_servers(
        wanted,
        existing_names={r["name"] for r in rows},
        secret_names=set(await get_user_secret_names(user_id)),
        room=MAX_CATALOG_SERVERS_PER_USER - len(rows),
    )
    if not prospects:
        return CheckedServers(), {}
    outcome = await check_due(
        user_id, [Due(p.server, moves=True, literals=p.literals) for p in prospects]
    )
    key_of = {p.server.name: p.plan.key for p in prospects}
    refused = {
        key_of[f.server]: f"did not pass its check: {f.reason}"
        for f in outcome.failed
    }
    for needed in outcome.sign_in:
        refused[key_of[needed.server]] = (
            "the server asks for a sign-in, which an upgrade cannot start"
        )
    return (
        CheckedServers.of(
            p.server for p in prospects if p.server.name in outcome.passed
        ),
        refused,
    )


async def apply_sse_upgrades(
    user_id: str, plugin: Mapping[str, Any], keys: Sequence[str]
) -> InstallReport:
    """Install consented held-back sse entries as streamable HTTP.

    Each one is checked first, from the row as the caller read it; one that
    fails is reported ``error`` and nothing is created for it, the others
    land. The plans written are re-derived from the stored manifests, so an
    upgrade months after install still lands the declared configuration.
    They are derived under the fan-out lock from the row as it stands then: an
    update that finished while this waited has replaced the package, an
    entry the new one no longer declares must not land, and one it changed is
    no longer what was checked (the fan-out refuses it).
    """
    report = InstallReport()
    checked, refused = await _check_upgrades(user_id, plugin, keys)
    async with plugin_fan_out_lock(user_id):
        stored = await still_installed(user_id, plugin, "this upgrade waited")
        plans, grants = await _stored_entry_plans(user_id, stored)
        sse_by_key = {
            p.key: p
            for p in plans
            if p.skip_code is None and p.transport == "sse"
        }
        if grants.preexisting:
            # Install and update both say this; an upgrade that quietly landed
            # the entry unbound would be the one path where the user is not told.
            report.diagnostics.append(
                Diagnostic(
                    level="warning", scope="plugin", code="secret_preexisting",
                    message=grants.disclosure_reason(),
                )
            )
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
            if key in refused:
                report.components.append(
                    ComponentResult.of(plan, "error", reason=refused[key])
                )
                continue
            # Consent recorded: install as the modern transport it probed for.
            plan.transport = "http"
            consented.append(plan)
        if not consented:
            return report
        # Each row commits with its own version bump, as on install.
        await fan_out_servers(
            user_id, plugin["user_plugin_id"], consented, report, checked=checked
        )
        await relink_subagents(user_id, plugin["user_plugin_id"], plugin["name"])
    disclose_vaulted_literals(report)
    await after_secrets_changed(user_id, report.secrets_created)
    logger.info(
        f"[plugins] sse upgrade user_id={user_id} name={plugin['name']} "
        f"servers={report.servers_created}"
    )
    return report


async def _check_rotation(
    user_id: str, secrets: Mapping[str, str]
) -> CheckedServers:
    """Check every server that reads one of these names, with the new values,
    before any of them is stored. Returns what passed; raises McpCheckFailed.

    Every row of the user's, whoever owns it. A server forked off the plugin
    keeps its references and drops the plugin, and a row written by hand can
    name the plugin's key; either sends the new value as surely as the
    plugin's own rows do, so filtering on the owner left both unchecked. None
    of them moves, so one that asks for a sign-in fails: the key was not what
    it wanted, and a sign-in is not something this request can start.
    """
    due = readers(await list_catalog_servers(user_id), secrets)
    if not due:
        return CheckedServers()
    outcome = await check_due(user_id, due, supplied=secrets)
    if outcome.failed:
        raise McpCheckFailed(outcome.failed)
    return CheckedServers.of(d.server for d in due)


async def apply_bindings(
    user_id: str, plugin: Mapping[str, Any], secrets: Mapping[str, str]
) -> list[str]:
    """Set new values for the keys an installed plugin asks for.

    Install takes a plugin's keys with the request and checks them, so this is
    for changing one afterwards: a rotated key, or a key a plugin installed
    before that rule never got. Every server that reads one of these names,
    whoever owns it, is checked with the new value first, and the vault is
    written only when they all pass.

    The check runs before the fan-out lock (a command server's check can start
    the user's computer) and the write under it, where the refusals are asked
    again: a secret the user created meanwhile is theirs, not the plugin's to
    overwrite, and a server that came to read one of these names meanwhile was
    never checked with the new value.

    Only names the plugin declares or its stored entries reference are
    accepted, the same two that install takes: this is not a general vault
    write. Raises PluginRejected for any other name, an unusable value or a
    name the user holds for something else, McpCheckFailed when a server
    rejects the new value, and ValueError when the plugin or a server changed
    under the check, or the vault itself refuses the write.
    """
    extension = stored_extension(plugin)
    _doc, entries = await _stored_document(plugin)
    secrets = validate_supplied(extension, secrets, entries=entries)
    if not secrets:
        return []
    plugin_id = plugin["user_plugin_id"]

    # Before the check too, so a value that could not be written dials nothing.
    await secret_gate(user_id, extension, secrets, plugin_id=plugin_id)
    checked = await _check_rotation(user_id, secrets)

    async with plugin_fan_out_lock(user_id):
        stored = await still_installed(
            user_id, plugin, "its keys were being checked"
        )
        # An update that landed meanwhile may have changed what is asked for.
        extension = stored_extension(stored)
        _doc, entries = await _stored_document(stored)
        secrets = validate_supplied(extension, secrets, entries=entries)
        grants = await secret_gate(user_id, extension, secrets, plugin_id=plugin_id)
        refuse_unapproved(
            readers(await list_catalog_servers(user_id), secrets), checked
        )
        written, introduced = await write_supplied(
            user_id, plugin["name"], extension, secrets, held=grants.held
        )
        # Only the creates are claimed. Writing a new value into a name that
        # was already there says nothing about whose it is, and a name that
        # survived the grant test was this plugin's already. Without this, a
        # plugin whose entries were all held back at install has no record of
        # the grant at all and is refused the key its own wizard just collected.
        await claim_plugin_secrets(user_id, plugin_id, introduced)
    # A new value is what a ${vault:NAME} on an already-enabled server
    # resolves to next; the caches must not keep the old view.
    await after_secrets_changed(user_id, written)
    return written
