"""Plugin update: re-validate a fresh package and reconcile owned components.

The diff is keyed by package identity (``plugin_server_key`` / the skills/
directory name), never by row name — renames in the package are a delete +
create. A component is deleted only when its key VANISHES from the package;
a key that is still declared but now invalid keeps its existing row (per-
entry isolation must not turn a package typo into a data loss). Rows the
user detached (plugin_id cleared by an edit) are never touched: an incoming
component whose name exists but is no longer owned is reported ``detached``
and skipped. User-set vault references survive in-place updates: where the
existing row holds a ``${vault:NAME}`` ref, the ref wins over whatever
literal the new package ships at that key.
"""

import asyncio
import logging
from typing import Any

from pydantic import ValidationError

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    delete_catalog_server,
    get_catalog_server,
)
from src.server.database.plugins import (
    list_plugin_server_names,
    list_plugin_skill_names,
    update_plugin_row,
)
from src.server.database.user_skills import (
    delete_user_skill,
    get_user_skill,
    upsert_user_skill,
)
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.models.mcp_server import (
    McpServerInput,
    _format_validation_error,
    coerce_mcp_name,
)
from src.server.models.plugin import ComponentResult, Diagnostic, InstallReport
from src.server.services.mcp_catalog import apply_catalog_edit
from src.server.services.mcp_import import plan_vault_extraction
from src.server.services.mcp_oauth.lifecycle import oauth_fence
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.grants import resolve_bind_grants
from src.server.services.plugins.package import (
    ValidatedPackage,
    pending_secret_declarations,
)
from src.server.services.plugins.reconcile import ReconcileArms, reconcile
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.plugins.skill_fanout import (
    fan_out_skills,
    store_skill_archive,
)
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.user_skills.validate import (
    SkillValidationError,
    validate_skill_archive,
)
from src.server.services.vault_invalidation import (
    USER_TIER,
    after_secrets_changed,
)

logger = logging.getLogger(__name__)


def _endpoint_moved(
    incoming: dict[str, Any], existing_row: dict[str, Any]
) -> bool:
    """True when the new version points the entry somewhere else."""
    if incoming.get("transport") != existing_row.get("transport"):
        return True
    return (incoming.get("url") or "") != (existing_row.get("url") or "")


def _preserve_vault_refs(
    incoming: dict[str, Any], existing_row: dict[str, Any]
) -> list[str]:
    """Keep the row's ``${vault:NAME}`` refs over the package's literals.

    Returns the names deliberately NOT carried over. A reference is consent to
    send that credential to the endpoint it was set up for, so a version that
    repoints ``url`` or switches transport does not get to bring it along: the
    package would otherwise only have to ship v1 honestly, then move the host
    in v2 and receive the key. The row is left needing the secret again, which
    is visible, instead of authenticating to a new host silently.
    """
    if _endpoint_moved(incoming, existing_row):
        return sorted(
            {
                name
                for section in ("env", "headers")
                for value in (existing_row.get(section) or {}).values()
                if isinstance(value, str)
                for name in VAULT_REF_RE.findall(value)
            }
        )
    for section in ("env", "headers"):
        stored = existing_row.get(section) or {}
        target = incoming.get(section)
        if not isinstance(target, dict):
            continue
        for key, value in stored.items():
            if key in target and isinstance(value, str) and VAULT_REF_RE.fullmatch(
                value
            ):
                target[key] = value
    return []


async def _update_servers(
    user_id: str,
    plugin_id: str,
    package: ValidatedPackage,
    report: InstallReport,
) -> bool:
    """Reconcile owned server rows against the package. True if the
    effective set changed (a bump is needed)."""
    if package.mcp_document_invalid:
        # The new tree ships an mcp.json that failed document-level
        # validation, so it yields no entry plans. That is not the same claim
        # as "the plugin removed every server", and reconciling against it
        # would delete every owned row over an upstream typo. The diagnostics
        # already say what is wrong; the rows stay until a readable document
        # says otherwise.
        return False
    owned = {
        s["plugin_server_key"] or s["name"]: s["name"]
        for s in await list_plugin_server_names(user_id, plugin_id)
    }
    incoming = {p.key: p for p in package.entry_plans}
    changed = False
    # Shared across the update arms for the same reason the import loop shares
    # them across entries: two entries shipping the same literal get one
    # secret, and no allocation collides with a name already in the vault.
    allocated: dict[str, str] = {}
    used_secret_names = set(await get_user_secret_names(user_id))

    async def delete(key: str, row_name: str) -> None:
        # Delete through the helper that owns the purges, inside the same
        # OAuth fence the catalog DELETE endpoint uses.
        nonlocal changed
        async with oauth_fence(user_id, [row_name]):
            # Only if still owned: the enumeration above and this write are
            # separate transactions, and a Customize in between makes the row
            # the user's, not the package's, to remove.
            if not await delete_catalog_server(
                user_id, row_name, owned_by_plugin=plugin_id
            ):
                return
        changed = True
        # renamed is a property of the key, never of the row: the component
        # must report the same flag here as it did at install.
        report.components.append(
            ComponentResult(
                kind="mcp", key=key, name=row_name,
                renamed=coerce_mcp_name(key)[1], status="deleted",
            )
        )

    async def update(key: str, row_name: str, plan) -> None:
        # In place, or keep the row when the new version of the entry doesn't
        # validate.
        nonlocal changed
        report.diagnostics.extend(plan.diagnostics)
        if not plan.installable:
            report.components.append(
                ComponentResult.of(
                    plan, "skipped", name=row_name,
                    reason=(
                        (plan.skip_reason or "held back (legacy sse transport)")
                        + "; existing row kept"
                    ),
                )
            )
            return
        existing = await get_catalog_server(user_id, row_name)
        if existing is None:
            return
        if str(existing.get("plugin_id") or "") != plugin_id:
            # Detached since the enumeration. The `detached` arm covers names
            # that were never ours; this covers ours becoming the user's
            # mid-run, and owes the same promise: never overwrite a fork.
            report.components.append(
                ComponentResult.of(
                    plan, "detached", name=row_name,
                    reason=(
                        "this server was customized while the update ran; "
                        "left untouched"
                    ),
                )
            )
            return
        config = dict(plan.config)
        # The row keeps its installed name — the key, not the name, is the
        # component's identity.
        config["name"] = row_name
        dropped_refs = _preserve_vault_refs(config, existing)
        if dropped_refs:
            report.diagnostics.append(
                Diagnostic(
                    level="warning", scope="entry", target=key,
                    code="secret_not_carried",
                    message=(
                        f"this version points {row_name!r} at a different "
                        f"endpoint, so it does not inherit the credentials the "
                        f"old one used ({', '.join(dropped_refs)}); set them "
                        f"again if the new endpoint should have them"
                    ),
                )
            )
        # Install gets this from run_mcp_import; the in-place arm writes
        # through apply_catalog_edit instead, so it owes the same extraction.
        # Without it a credential the package introduces in THIS version is
        # persisted as a plaintext literal — _preserve_vault_refs only defends
        # refs the row already holds. After the preserve, so a user's own ref
        # is what survives at a key the package still ships a literal for.
        entry_plan = plan_vault_extraction(
            row_name,
            config,
            allocated=allocated,
            used_secret_names=used_secret_names,
        )
        if config.get("transport") in ("http", "sse"):
            headers = config.get("headers") or {}
            if any(VAULT_REF_RE.search(str(v)) for v in headers.values()):
                config["discovery_uses_secrets"] = True
        try:
            server = McpServerInput(**config)
        except ValidationError as e:
            report.components.append(
                ComponentResult.of(
                    plan, "error", name=row_name,
                    reason=_format_validation_error(e),
                )
            )
            return
        # Before the edit, so the ref the row is about to hold already
        # resolves. An edit that then fails leaves the secret unreferenced,
        # which is inert — the reverse order would leave a live row pointing
        # at a name the vault does not have.
        try:
            for secret in entry_plan.secrets:
                await create_user_secret(
                    user_id, secret.name, secret.value, secret.description
                )
        except ValueError as e:
            # Vault cap or a raced name. Per-entry isolation, same as install:
            # the row keeps its working config rather than being rewritten to
            # point at a ref the vault never got.
            report.components.append(
                ComponentResult.of(plan, "error", name=row_name, reason=str(e))
            )
            return
        # Through the catalog edit policy, not the DB writer: an update that
        # repoints a server owes the same consent revoke and rediscovery as a
        # hand edit. detach_plugin=False — the plugin is editing its own row.
        edit = await apply_catalog_edit(
            user_id, row_name, server.to_catalog_fields(), detach_plugin=False
        )
        if edit is None:
            # The row went out from under us; the create arm of a later update
            # reinstates it, and claiming an update here would be a lie.
            return
        changed = True
        allocated.update(entry_plan.refs)
        used_secret_names.update(s.name for s in entry_plan.secrets)
        report.secrets_created.extend(s.name for s in entry_plan.secrets)
        report.components.append(
            ComponentResult.of(plan, "updated", name=row_name)
        )

    async def detached(key: str, plan) -> bool:
        # A name that exists but is not owned (detached or hand-made) is
        # reported and never overwritten.
        if not plan.installable:
            return False
        existing = await get_catalog_server(user_id, plan.name)
        if existing is None or str(existing.get("plugin_id") or "") == plugin_id:
            return False
        report.components.append(
            ComponentResult.of(
                plan, "detached",
                reason=(
                    "a server with this name exists outside the plugin; "
                    "left untouched"
                ),
            )
        )
        return True

    async def create(plans: list[Any]) -> None:
        # New keys go through the install fan-out, which also reports skipped
        # and sse-held-back plans.
        nonlocal changed
        before = report.servers_created
        await fan_out_servers(user_id, plugin_id, plans, report)
        changed = changed or report.servers_created > before

    await reconcile(
        owned,
        incoming,
        arms=ReconcileArms(
            delete=delete, update=update, create=create, detached=detached
        ),
    )
    return changed


async def _replace_skill(
    user_id: str,
    plugin_id: str,
    plan,
    row: dict[str, Any],
    validated,
    report: InstallReport,
) -> None:
    archive_key, archive_blob, error = await store_skill_archive(
        user_id, validated.canonical_zip, validated.content_hash
    )
    if error is not None:
        report.components.append(
            ComponentResult.of(plan, "error", name=row["name"], reason=error)
        )
        return
    try:
        _row, superseded = await upsert_user_skill(
            user_id,
            validated.name,
            description=validated.description,
            license=validated.license,
            frontmatter=validated.frontmatter,
            allowed_tools=validated.allowed_tools,
            confirmed=True,
            content_hash=validated.content_hash,
            archive_key=archive_key,
            archive_blob=archive_blob,
            archive_bytes=len(validated.canonical_zip),
            file_count=validated.file_count,
            workspace_id=None,
            plugin_id=plugin_id,
            plugin_skill_dir=plan.dir,
        )
    except ValueError as e:
        await drop_archive_if_unused(user_id, archive_key)
        report.components.append(
            ComponentResult.of(plan, "error", name=row["name"], reason=str(e))
        )
        return
    except BaseException:
        await drop_archive_if_unused(user_id, archive_key)
        raise
    await drop_archive_if_unused(user_id, superseded)
    report.components.append(
        ComponentResult.of(plan, "updated", name=validated.name)
    )


async def _update_skills(
    user_id: str,
    plugin_id: str,
    package: ValidatedPackage,
    report: InstallReport,
) -> None:
    owned = {
        s["plugin_skill_dir"] or s["name"]: s
        for s in await list_plugin_skill_names(user_id, plugin_id)
    }
    incoming = {p.dir: p for p in package.skill_plans}

    async def delete(directory: str, row_ref: dict[str, Any]) -> None:
        deleted = await delete_user_skill(
            user_id,
            row_ref["name"],
            workspace_id=row_ref.get("workspace_id"),
            owned_by_plugin=plugin_id,
        )
        if not deleted:
            # Detached or already gone since the enumeration; not ours to drop.
            return
        await drop_archive_if_unused(user_id, deleted.get("archive_key"))
        report.components.append(
            ComponentResult(
                kind="skill", key=directory, name=row_ref["name"],
                status="deleted",
            )
        )

    async def update(directory: str, row_ref: dict[str, Any], plan) -> None:
        report.diagnostics.extend(plan.diagnostics)
        if plan.skip_code is not None:
            report.components.append(
                ComponentResult.of(
                    plan, "skipped", name=row_ref["name"],
                    reason=(plan.skip_reason or "") + "; existing row kept",
                )
            )
            return
        row = await get_user_skill(
            user_id, row_ref["name"], workspace_id=row_ref.get("workspace_id")
        )
        if row is None:
            return
        if str(row.get("plugin_id") or "") != plugin_id:
            # Customized since the enumeration; the replace below would re-adopt
            # the user's fork under the package's content.
            report.components.append(
                ComponentResult.of(
                    plan, "detached", name=row["name"],
                    reason=(
                        "this skill was customized while the update ran; "
                        "left untouched"
                    ),
                )
            )
            return
        try:
            validated = await asyncio.to_thread(
                validate_skill_archive, plan.zip_bytes
            )
        except SkillValidationError as e:
            report.components.append(
                ComponentResult.of(
                    plan, "invalid", name=row["name"],
                    reason=f"{e}; existing row kept",
                )
            )
            return
        if validated.content_hash == row.get("content_hash"):
            report.components.append(
                ComponentResult.of(plan, "unchanged", name=row["name"])
            )
            return
        if validated.name != row["name"]:
            # Install enforces name == directory, so a plugin-owned row's name
            # IS its directory: reaching here means the new SKILL.md renamed
            # the skill without moving the directory, which the fan-out would
            # reject as a name mismatch. Deleting first and recreating after
            # would therefore always delete and never recreate. A genuine
            # rename moves the directory too, and lands as an ordinary
            # delete + create through the reconciler's other arms.
            report.components.append(
                ComponentResult.of(
                    plan, "invalid", name=row["name"],
                    reason=(
                        f"SKILL.md declares name {validated.name!r} but the "
                        f"directory is {plan.dir!r}; they must match. "
                        "Existing row kept."
                    ),
                )
            )
            return
        await _replace_skill(user_id, plugin_id, plan, row, validated, report)

    async def detached(directory: str, plan) -> bool:
        if plan.skip_code is not None:
            return False
        existing = await get_user_skill(user_id, directory)
        if existing is None or str(existing.get("plugin_id") or "") == plugin_id:
            return False
        report.components.append(
            ComponentResult.of(
                plan, "detached", name=directory,
                reason=(
                    "a skill with this name exists outside the plugin; "
                    "left untouched"
                ),
            )
        )
        return True

    async def create(plans: list[Any]) -> None:
        await fan_out_skills(user_id, plugin_id, plans, report)

    await reconcile(
        owned,
        incoming,
        arms=ReconcileArms(
            delete=delete, update=update, create=create, detached=detached
        ),
    )


async def update_plugin_package(
    user_id: str,
    plugin: dict[str, Any],
    package: ValidatedPackage,
    *,
    source_ref: str | None,
) -> tuple[dict[str, Any], InstallReport]:
    """Reconcile an installed plugin against a freshly fetched package.

    The caller has already matched ``package.name`` to the installed plugin.
    Always reconciles, even when the tree hash is unchanged: the hash says the
    package has not moved, not that its components are all present, and the
    two diverge exactly when it matters — a component deleted by hand, or one
    that landed ``exists`` against a name that has since been freed. The
    reconciler is idempotent, so the unchanged case costs a diff and reports
    everything ``unchanged``.
    """
    report = InstallReport(
        diagnostics=list(package.diagnostics),
        dropped_files=list(package.dropped_files),
    )
    report.secrets_required = await pending_secret_declarations(user_id, package)

    plugin_id = plugin["user_plugin_id"]
    # A name this plugin's rows already reference was granted at some earlier
    # install and carries forward; anything else the user holds does not.
    grants = await resolve_bind_grants(
        user_id, package.extension, plugin_id=plugin_id
    )
    materialize_binds(package.extension, package.entry_plans, grants.granted)
    if grants.refused:
        report.diagnostics.append(
            Diagnostic(
                level="warning", scope="plugin", code="secret_not_granted",
                message=grants.refusal_reason(),
            )
        )
    changed = await _update_servers(user_id, plugin_id, package, report)
    await _update_skills(user_id, plugin_id, package, report)

    await after_secrets_changed(
        USER_TIER, user_id, report.secrets_created, user_id=user_id
    )
    if changed:
        await bump_user_workspaces_mcp_version(user_id)

    # An unreadable mcp.json left the servers untouched above, so the stored
    # document is still the one that describes them — replacing it with NULL
    # would throw away the only copy over a fault we did not act on.
    settled = report.landed_whole and not package.mcp_document_invalid
    row = await update_plugin_row(
        user_id,
        plugin["name"],
        version=package.version,
        source_ref=source_ref,
        manifest=package.manifest,
        mcp_document=(
            plugin.get("mcp_document")
            if package.mcp_document_invalid
            else package.mcp_document
        ),
        # Same rule as install: the hash claims this tree is installed, so a
        # component left in error keeps the old one and stays reconcilable.
        content_hash=package.content_hash if settled else None,
    )
    logger.info(
        f"[plugins] update user_id={user_id} name={plugin['name']} "
        f"components={len(report.components)} complete={report.landed_whole}"
    )
    return row or plugin, report
