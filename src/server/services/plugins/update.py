"""Plugin update: re-validate a fresh package and reconcile owned components.

The diff is keyed by package identity (``plugin_server_key`` / the skills/
directory name), never by row name — renames in the package are a delete +
create. A component is deleted only when its key VANISHES from the package;
a key that is still declared but now invalid keeps its existing row (per-
entry isolation must not turn a package typo into a data loss). Rows the
user detached (plugin_id cleared by an edit) are never touched: an incoming
component whose name exists but is no longer owned is skipped and reported
``detached``, or ``exists`` when the row predates the install, as install
reports it. User-set vault references survive in-place updates: where the
existing row holds a ``${vault:NAME}`` ref, the ref wins over whatever
literal the new package ships at that key.

Every server the update would create, or rewrite with different dial fields,
is checked before anything is applied (``_check_update``), and so is every
server, whoever owns it, that will read a secret value the request supplies;
one failure and the update writes nothing. A server that would need a new
sign-in refuses the update, since an update has no callback to carry it
through.
"""

import asyncio
import copy
import logging
from collections.abc import Collection, Mapping, Sequence
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    delete_catalog_server,
    get_catalog_server,
    list_catalog_servers,
)
from src.server.database.plugins import (
    claim_plugin_secrets,
    list_plugin_server_names,
    list_plugin_skill_names,
    plugin_fan_out_lock,
    update_plugin_row,
)
from src.server.database.user_skills import (
    SkillNotOwned,
    delete_user_skill,
    get_user_skill,
    upsert_user_skill,
)
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.models.mcp_server import coerce_mcp_name
from src.server.models.plugin import ComponentResult, Diagnostic, InstallReport
from src.server.services.mcp_catalog import apply_catalog_edit
from src.server.services.mcp_dial import dial_moved
from src.server.services.mcp_import import saved_server
from src.server.services.mcp_oauth.lifecycle import oauth_fence
from src.server.services.mcp_validation import McpCheckFailed
from src.server.services.plugins.checks import (
    NOT_CHECKED_REASON,
    CheckedServers,
    asked_secrets,
    creatable_servers,
    missing_declarations,
    reads_any,
    row_server,
    secrets_required,
    server_secret_names,
    sign_in_unsupported,
)
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.gate import (
    Due,
    check_due,
    refuse_unapproved,
    secret_gate,
    still_installed,
)
from src.server.services.plugins.grants import (
    BindGrants,
    secret_names_in,
    strip_refs,
    disclose_undeclared_refs,
    disclose_vaulted_literals,
)
from src.server.services.plugins.mcp import McpEntryPlan
from src.server.services.plugins.package import (
    ValidatedPackage,
    pending_secret_declarations,
)
from src.server.services.plugins.reconcile import ReconcileArms, reconcile
from src.server.services.plugins.server_fanout import fan_out_servers
from src.server.services.plugins.subagent_fanout import reconcile_subagents
from src.server.services.plugins.skill_fanout import (
    fan_out_skills,
    store_skill_archive,
)
from src.server.services.plugins.supplied_secrets import (
    bind_placeholders,
    refuse_vault_overflow,
    validate_supplied,
    write_supplied,
)
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.user_skills.validate import (
    SkillValidationError,
    validate_skill_archive,
)
from src.server.services.vault_invalidation import after_secrets_changed
from src.server.utils.error_sanitization import validation_error_text

logger = logging.getLogger(__name__)


def _customized_mid_run(noun: str) -> str:
    """A row this update owned when it read it, and not when it writes.

    Both the pre-write ownership check and the write's own predicate report
    through this: a reader must not be able to read a wording difference as a
    difference in what happened to their fork.
    """
    return f"this {noun} was customized while the update ran; left untouched"


# Install's own wording for a name it found taken, so a collision reads the
# same through either door.
_SERVER_EXISTS_REASON = "a server with this name already exists; left untouched"
_SKILL_EXISTS_REASON = "a skill with this name already exists; left untouched"


def _predates(row: Mapping[str, Any], installed_at: str) -> bool:
    """Whether ``row`` was there before the plugin was installed.

    A component's name held by a row the plugin does not own is either the
    plugin's own row that the user customized, which Customize leaves with the
    creation time the plugin gave it, or a row of the user's that the plugin
    collided with. The first is a choice the update settles; the second is
    install's ``exists``, which keeps the version unsettled so the update is
    offered again once the name is free. A row of the user's made after the
    install reads as the first: nothing on a row records a Customize.
    """
    created = row.get("created_at")
    return bool(created) and (
        datetime.fromisoformat(created) < datetime.fromisoformat(installed_at)
    )


def _execution_identity(config: dict[str, Any]) -> tuple[Any, ...]:
    """What a credential on this entry was consented to: where it runs.

    For a remote server that is the transport and the url. For stdio it is the
    command and its arguments, because a stdio entry moves by changing the
    package it executes rather than a hostname, and comparing only url would
    call ``npx -y good-pkg`` and ``npx -y evil-pkg`` the same endpoint.

    Reference text is masked before comparing so that re-pointing a credential
    at a different vault name does not read as a move on its own; the identity
    is meant to be about the destination, not about which key goes there.
    """
    args = config.get("args")
    return (
        config.get("transport") or "",
        config.get("url") or "",
        config.get("command") or "",
        tuple(
            VAULT_REF_RE.sub("<ref>", a) if isinstance(a, str) else a
            for a in (args if isinstance(args, list) else ())
        ),
    )


def _endpoint_moved(
    incoming: dict[str, Any], existing_row: dict[str, Any]
) -> bool:
    """True when the new version points the entry somewhere else."""
    return _execution_identity(incoming) != _execution_identity(existing_row)


def _preserve_vault_refs(
    incoming: dict[str, Any],
    existing_row: dict[str, Any],
    *,
    consented: Collection[str] = (),
) -> list[str]:
    """Keep the row's ``${vault:NAME}`` refs over the package's literals.

    Returns the names deliberately NOT carried over. A reference is consent to
    send that credential to the endpoint it was set up for, so a version that
    repoints ``url`` or switches transport does not get to bring it along: the
    package would otherwise only have to ship v1 honestly, then move the host
    in v2 and receive the key. The row is left needing the secret again, which
    is visible, instead of authenticating to a new host silently. A name in
    ``consented`` is one the request hands a value for, which is the user
    giving it to the new endpoint, so the package's reference to it stays.

    On a move the incoming config is scrubbed, not merely reported on. Refusing
    to COPY the row's refs is not enough, because the incoming plan can already
    carry the same reference on its own: ``materialize_binds`` runs before this
    and writes every carried-forward grant, and the package can put the ref in
    the portable document besides. Reporting a name as dropped while leaving it
    in the config is exactly the silent authentication this is here to stop.
    """
    if _endpoint_moved(incoming, existing_row):
        dropped = (
            secret_names_in(existing_row) | secret_names_in(incoming)
        ) - set(consented)
        strip_refs(incoming, frozenset(dropped))
        return sorted(dropped)
    # A move is read with every reference masked, so past one the args differ
    # from the row's in vault names at most, and the row's stay for the reason
    # a header's does below: another name is not the same credential.
    stored_args = existing_row.get("args")
    if isinstance(stored_args, list) and isinstance(incoming.get("args"), list):
        incoming["args"] = list(stored_args)
    for section in ("env", "headers"):
        stored = existing_row.get(section) or {}
        target = incoming.get(section)
        if not isinstance(target, dict):
            continue
        for key, value in stored.items():
            if key not in target or not isinstance(value, str):
                continue
            held = set(VAULT_REF_RE.findall(value))
            if not held:
                continue
            # A reference need not fill its field (``Bearer ${vault:X}``), so
            # both sides are read by name, not by shape. When the package ships
            # the same credential in a new template, a ``prefix`` its bind
            # gained, the template is the fix and must land: keeping the row's
            # would pin v1's bare key and its 401 forever. A different name is
            # not the same credential, and the row's working one stays.
            shipped = set(VAULT_REF_RE.findall(str(target[key])))
            if not shipped or not shipped <= held:
                target[key] = value
    return []


def _rebuilt_config(
    plan: McpEntryPlan,
    existing: Mapping[str, Any],
    *,
    consented: Collection[str] = (),
) -> tuple[dict[str, Any], list[str]]:
    """The config the update arm rewrites ``existing`` into, with the row's
    own refs carried over, and the refs deliberately not carried.

    Shared with ``_check_update``, which has to check the very config this arm
    writes. Both then build it with ``saved_server``: install gets the vault
    extraction from the import, and without it a credential the package
    introduces in THIS version would be persisted as a plaintext literal, as
    ``_preserve_vault_refs`` only defends refs the row already holds. The
    preserve runs first, so a user's own ref is what survives at a key the
    package still ships a literal for.
    """
    config = copy.deepcopy(plan.config)
    dropped_refs = _preserve_vault_refs(config, dict(existing), consented=consented)
    return config, dropped_refs


def _withheld(
    rows: Sequence[Mapping[str, Any]],
    plugin_id: str,
    package: ValidatedPackage,
    supplied: Mapping[str, str],
    grants: BindGrants,
) -> set[str]:
    """The keys a moved server references that the request has no value for.

    The move strips them (``_preserve_vault_refs``), so unasked, the server is
    checked and saved without the key it had, and the update reads as landed.
    A name held for something else is left out: this form cannot take a value
    for it (``refuse_overwrites``), and the row reports it
    ``secret_not_carried``.
    """
    if package.mcp_document_invalid:
        return set()
    owned = {
        (r.get("plugin_server_key") or r["name"]): r
        for r in rows
        if str(r.get("plugin_id") or "") == plugin_id
    }
    withheld: set[str] = set()
    for plan in package.entry_plans:
        row = owned.get(plan.key)
        if row is None or not plan.installable:
            continue
        if _endpoint_moved(plan.config, dict(row)):
            withheld |= secret_names_in(plan.config)
    return {
        n for n in withheld - set(supplied) if not grants.is_preexisting(n)
    }


def _due_servers(
    rows: Sequence[Mapping[str, Any]],
    plugin_id: str,
    package: ValidatedPackage,
    supplied: Mapping[str, str],
    *,
    held: set[str],
) -> list[Due]:
    """Every server the update writes differently or leaves reading a value
    the request supplies, each built as it will be saved.

    Mirrors ``_update_servers``. An owned key the package dropped is deleted
    and leaves nothing. An owned key it keeps is rewritten in place, or kept
    as it stands when its new entry is held back or invalid. Every other row
    is left as it stands, whoever owns it. A new key is created as the
    fan-out would create it, against the names and the room the account has
    once the deletes have run, because the reconciler runs them first:
    counted before them, a key renamed onto its old row name, or a new key
    that fits only once an old one is gone, was never checked, and the
    fan-out then refused to create it after its predecessor was deleted.

    No I/O, so the check and the write under the lock derive the same answer
    from the rows each of them read.
    """
    names = set(supplied)
    owned = {
        (r.get("plugin_server_key") or r["name"]): r
        for r in rows
        if str(r.get("plugin_id") or "") == plugin_id
    }
    # An unreadable mcp.json leaves every row as it is (``_update_servers``).
    incoming = (
        {} if package.mcp_document_invalid else {p.key: p for p in package.entry_plans}
    )
    deleted = (
        set()
        if package.mcp_document_invalid
        else {r["name"] for key, r in owned.items() if key not in incoming}
    )
    allocated: dict[str, str] = {}
    used = set(held) | names
    standing: list[Due] = []
    rewritten: set[str] = set()

    def as_it_stands(row: Mapping[str, Any]) -> Due:
        return Due(
            row_server(row),
            moves=False,
            reads=reads_any(row, names),
            signed_in=True,
            stored=True,
        )

    for key in sorted(set(owned) & set(incoming)):
        row, plan = owned[key], incoming[key]
        rewritten.add(row["name"])
        server = None
        if plan.installable:
            config, _dropped = _rebuilt_config(plan, row, consented=names)
            try:
                # The row keeps its installed name: the key, not the name, is
                # the component's identity.
                server, extraction = saved_server(
                    row["name"], config, allocated=allocated, used_secret_names=used
                )
            except ValidationError:
                server = None
        if server is None:
            # The update arm keeps the row when its new entry cannot land.
            standing.append(as_it_stands(row))
            continue
        allocated.update(extraction.refs)
        used.update(extraction.literals)
        moves = dial_moved(row, server.to_catalog_fields())
        standing.append(
            Due(
                server,
                moves=moves,
                reads=reads_any(row, names) or bool(server_secret_names(server) & names),
                signed_in=True,
                literals=extraction.literals if moves else {},
            )
        )
    for row in rows:
        if row["name"] not in rewritten and row["name"] not in deleted:
            standing.append(as_it_stands(row))
    fresh = [incoming[key] for key in sorted(set(incoming) - set(owned))]
    for prospect in creatable_servers(
        fresh,
        existing_names={r["name"] for r in rows} - deleted,
        secret_names=used,
        room=MAX_CATALOG_SERVERS_PER_USER - (len(rows) - len(deleted)),
    ):
        standing.append(
            Due(
                prospect.server,
                moves=True,
                reads=bool(server_secret_names(prospect.server) & names),
                literals=prospect.literals,
            )
        )
    return [d for d in standing if d.moves or d.reads]


async def _check_update(
    user_id: str,
    plugin_id: str,
    package: ValidatedPackage,
    supplied: Mapping[str, str],
    *,
    grants: BindGrants,
) -> CheckedServers:
    """Check every server the update would write differently, and every
    server left reading a value the request supplies. Writes nothing.

    A new key is checked as the install fan-out would create it; an owned key
    when its dial fields move, which leaves a version bump that touches
    descriptions to the rows as they are. A supplied value is checked on every
    server that will read it, whoever owns the row and whether or not the
    update moves it: the value replaces what those servers send, so the rule
    the key form keeps (``apply_bindings``) holds here too. A row whose
    sign-in still holds for an unchanged address is checked with that
    sign-in's token.

    Declared secrets are asked for only where a server being checked uses
    them: a missing key on a row this update does not touch is that row's
    state from before, not something this update introduces. Every key a
    moved server would lose is asked for too (``_withheld``).
    """
    held = set(grants.held)
    rows = await list_catalog_servers(user_id)
    due = _due_servers(rows, plugin_id, package, supplied, held=held)
    withheld = _withheld(rows, plugin_id, package, supplied, grants)
    if not due and not withheld:
        return CheckedServers()
    referenced: set[str] = set()
    for d in due:
        referenced |= server_secret_names(d.server)
    missing = missing_declarations(
        package.extension, held, supplied, among=referenced
    )
    withheld -= {s.name for s in missing}
    if missing or withheld:
        raise secrets_required(
            [*missing, *asked_secrets(package.extension, withheld)]
        )
    refuse_vault_overflow(held, [*supplied, *(n for d in due for n in d.literals)])
    outcome = await check_due(user_id, due, supplied=supplied)
    if outcome.failed:
        raise McpCheckFailed(outcome.failed)
    if outcome.sign_in:
        names = ", ".join(s.server for s in outcome.sign_in)
        raise sign_in_unsupported(
            outcome.sign_in,
            f"This version needs a new sign-in to {names}, which an update "
            "cannot start. Remove the plugin and install it again to sign in.",
        )
    return CheckedServers.of(d.server for d in due)


async def _update_servers(
    user_id: str,
    plugin_id: str,
    package: ValidatedPackage,
    report: InstallReport,
    *,
    checked: CheckedServers,
    installed_at: str,
    consented: Collection[str] = (),
) -> None:
    """Reconcile owned server rows against the package.

    Each arm writes through a catalog helper that bumps every workspace's MCP
    config version in the same transaction as a live row, so no bump is left
    for after the update, where a later failure could skip it. A server whose
    dial fields this update moves is written only as ``checked`` holds it, and
    keeps across the move only the keys in ``consented``.
    """
    if package.mcp_document_invalid:
        # The new tree ships an mcp.json that failed document-level
        # validation, so it yields no entry plans. That is not the same claim
        # as "the plugin removed every server", and reconciling against it
        # would delete every owned row over an upstream typo. The diagnostics
        # already say what is wrong; the rows stay until a readable document
        # says otherwise.
        return
    owned = {
        s["plugin_server_key"] or s["name"]: s["name"]
        for s in await list_plugin_server_names(user_id, plugin_id)
    }
    incoming = {p.key: p for p in package.entry_plans}
    # Shared across the update arms for the same reason the import loop shares
    # them across entries: two entries shipping the same literal get one
    # secret, and no allocation collides with a name already in the vault.
    allocated: dict[str, str] = {}
    used_secret_names = set(await get_user_secret_names(user_id))

    async def delete(key: str, row_name: str) -> None:
        # Delete through the helper that owns the purges, inside the same
        # OAuth fence the catalog DELETE endpoint uses.
        # Ownership first, because entering the fence is itself destructive:
        # it disconnects the server's OAuth connection and drops its schema
        # snapshots. The predicate below would then correctly refuse to delete
        # a row the user forked, but their login for it would already be gone,
        # and silently — the refusal path reports nothing. Narrowing to this
        # read leaves the same residual race the predicate handles, minus the
        # half that cannot be undone.
        existing = await get_catalog_server(user_id, row_name)
        if existing is None:
            return
        if str(existing.get("plugin_id") or "") != plugin_id:
            report.components.append(
                ComponentResult(
                    kind="mcp", key=key, name=row_name,
                    renamed=coerce_mcp_name(key)[1], status="detached",
                    reason=_customized_mid_run("server"),
                )
            )
            return
        async with oauth_fence(user_id, [row_name]):
            # Only if still owned: the read above and this write are separate
            # transactions, and a Customize in between makes the row the
            # user's, not the package's, to remove.
            if not await delete_catalog_server(
                user_id, row_name, owned_by_plugin=plugin_id
            ):
                return
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
                    plan, "detached", name=row_name, reason=_customized_mid_run("server"),
                )
            )
            return
        config, dropped_refs = _rebuilt_config(
            plan, existing, consented=consented
        )
        if dropped_refs:
            report.diagnostics.append(
                Diagnostic(
                    level="warning", scope="entry", target=key,
                    code="secret_not_carried",
                    message=(
                        f"this version points {row_name!r} at a different "
                        f"endpoint, so it does not inherit the keys the "
                        f"old one used ({', '.join(dropped_refs)}); set them "
                        f"again if the new endpoint should have them"
                    ),
                )
            )
        try:
            server, entry_plan = saved_server(
                row_name, config,
                allocated=allocated, used_secret_names=used_secret_names,
            )
        except ValidationError as e:
            report.components.append(
                ComponentResult.of(
                    plan, "error", name=row_name,
                    reason=validation_error_text(e),
                )
            )
            return
        # An entry the package did not touch still arrives here with a fully
        # rebuilt config, and writing it regardless reported "updated" for
        # every component of every update forever — and bumped every
        # workspace's MCP version twice to deliver a row identical to the one
        # already there. The comparison surface is ``to_catalog_fields``
        # itself, not a list of names copied beside it, so a column the writer
        # gains is compared without anyone remembering to add it here.
        # Equality already implies no secret was minted (a fresh ref would not
        # match what the row holds), but the skip is guarded on it outright:
        # returning early must never strand a secret the config now points at.
        fields = server.to_catalog_fields()
        if not entry_plan.secrets and all(
            existing.get(field) == value for field, value in fields.items()
        ):
            report.components.append(
                ComponentResult.of(plan, "unchanged", name=row_name)
            )
            return
        if dial_moved(existing, fields) and not checked.approves(server):
            # The check proved the row as it stood before the lock; one that
            # moved since is a server nobody checked.
            report.components.append(
                ComponentResult.of(
                    plan, "error", name=row_name, reason=NOT_CHECKED_REASON
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
        # expect_plugin closes the window the ownership check above opens: the
        # secret writes between them are awaits a Customize can land inside,
        # and the check is worth nothing if the write that follows it is
        # unconditional.
        edit = await apply_catalog_edit(
            user_id, row_name, fields,
            detach_plugin=False, expect_plugin=plugin_id,
        )
        if edit is None:
            # Either the row went out from under us — the create arm of a later
            # update reinstates it, and claiming an update here would be a lie —
            # or it was customized mid-write and the predicate refused. Only the
            # second leaves something behind to tell the user about.
            if await get_catalog_server(user_id, row_name) is not None:
                report.components.append(
                    ComponentResult.of(
                        plan, "detached", name=row_name,
                        reason=_customized_mid_run("server"),
                    )
                )
            return
        allocated.update(entry_plan.refs)
        used_secret_names.update(entry_plan.literals)
        report.secrets_created.extend(entry_plan.literals)
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
            ComponentResult.of(plan, "exists", reason=_SERVER_EXISTS_REASON)
            if _predates(existing, installed_at)
            else ComponentResult.of(
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
        await fan_out_servers(user_id, plugin_id, plans, report, checked=checked)

    await reconcile(
        owned,
        incoming,
        arms=ReconcileArms(
            delete=delete, update=update, create=create, detached=detached
        ),
    )


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
            # The caller's ownership read is separated from this write by a zip
            # validation and an object PUT. Without the predicate a Customize
            # landing in between is re-adopted here and its archive dropped as
            # superseded, which no later step can put back.
            owned_by_plugin=plugin_id,
        )
    except SkillNotOwned:
        await drop_archive_if_unused(user_id, archive_key)
        report.components.append(
            ComponentResult.of(
                plan, "detached", name=row["name"],
                reason=_customized_mid_run("skill"),
            )
        )
        return
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
    *,
    installed_at: str,
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
                plan, "exists", name=directory, reason=_SKILL_EXISTS_REASON
            )
            if _predates(existing, installed_at)
            else ComponentResult.of(
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
    secrets: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], InstallReport]:
    """Reconcile an installed plugin against a freshly fetched package.

    The caller has already matched ``package.name`` to the installed plugin.
    Always reconciles, even when the tree hash is unchanged: the hash says the
    package has not moved, not that its components are all present, and the
    two diverge exactly when it matters — a component deleted by hand, or one
    that landed ``exists`` against a name that has since been freed. The
    reconciler is idempotent, so the unchanged case costs a diff and reports
    everything ``unchanged``.

    ``secrets`` are values for keys a new or moved server needs, declared or
    only referenced, or new values for keys the plugin already holds; they
    reach the vault only once every server the update writes, and every
    server that reads one of them, has passed its check. Raises what install
    raises for its secrets and its check, and ValueError when a server changed
    under the check.
    """
    report = InstallReport(
        diagnostics=list(package.diagnostics),
        dropped_files=list(package.dropped_files),
    )
    plugin_id = plugin["user_plugin_id"]
    # Onto no name already held, the plugin's own included: a row referencing
    # a held name keeps it over a new one (``_preserve_vault_refs``), so a
    # blank the row already answered stays answered, however it was wired.
    bind_placeholders(package.entry_plans, taken=await get_user_secret_names(user_id))
    supplied = validate_supplied(
        package.extension, secrets, entries=package.entry_plans
    )

    # A name this plugin's rows already reference was granted at some earlier
    # install and carries forward; anything else the user holds does not.
    grants = await secret_gate(
        user_id, package.extension, supplied, plugin_id=plugin_id
    )
    materialize_binds(
        package.extension, package.entry_plans, grants.granted,
        document_dropped=package.mcp_document_invalid,
    )
    disclose_undeclared_refs(package.entry_plans, grants)
    if grants.preexisting:
        report.diagnostics.append(
            Diagnostic(
                level="warning", scope="plugin", code="secret_preexisting",
                message=grants.disclosure_reason(),
            )
        )
    # Before the lock: a command server's check can start the user's computer,
    # and the write below re-proves each server against what passed.
    checked = await _check_update(user_id, plugin_id, package, supplied, grants=grants)
    # The row names the package its components came from, so it is written
    # under the same lock: released first, a second update could land its
    # components in between and this row would then claim the wrong package.
    async with plugin_fan_out_lock(user_id):
        # The plugin was read before the fetch; an uninstall, or one followed
        # by a reinstall under the same name, may have landed since.
        stored = await still_installed(user_id, plugin, "this update waited")
        written: list[str] = []
        if supplied:
            now = await secret_gate(
                user_id, package.extension, supplied, plugin_id=plugin_id
            )
            refuse_unapproved(
                _due_servers(
                    await list_catalog_servers(user_id), plugin_id, package,
                    supplied, held=set(now.held),
                ),
                checked,
            )
            written, introduced = await write_supplied(
                user_id, plugin["name"], package.extension, supplied,
                held=now.held,
            )
            await claim_plugin_secrets(user_id, plugin_id, introduced)
        await _update_servers(
            user_id, plugin_id, package, report,
            checked=checked, installed_at=stored["installed_at"], consented=supplied,
        )
        await _update_skills(
            user_id, plugin_id, package, report, installed_at=stored["installed_at"]
        )
        await reconcile_subagents(
            user_id, plugin_id, plugin["name"], package.agent_plans, report
        )

        # An unreadable mcp.json left the servers untouched above, so the stored
        # document is still the one that describes them. Replacing it with NULL
        # would throw away the only copy over a fault we did not act on, and
        # the caller's copy may predate an update that finished since.
        document = package.mcp_document
        if package.mcp_document_invalid:
            document = stored.get("mcp_document")
        settled = report.landed_whole and not package.mcp_document_invalid
        row = await update_plugin_row(
            user_id,
            plugin["name"],
            version=package.version,
            source_ref=source_ref,
            manifest=package.manifest,
            mcp_document=document,
            # Same rule as install: the hash claims this tree is installed, so
            # a component left in error leaves none and stays reconcilable.
            content_hash=package.content_hash if settled else "",
        )

    disclose_vaulted_literals(report)
    await after_secrets_changed(user_id, [*written, *report.secrets_created])
    report.secrets_required = await pending_secret_declarations(user_id, package)
    logger.info(
        f"[plugins] update user_id={user_id} name={plugin['name']} "
        f"components={len(report.components)} complete={report.landed_whole}"
    )
    return row or plugin, report
