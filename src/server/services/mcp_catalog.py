"""The policy an edit to a ``user_mcp_servers`` row owes, in one place.

The write itself is the small part. An OAuth token consented for the old
endpoint must not carry onto a new one, a moved discovery fingerprint strands
the cached tool snapshot, and a user edit of a plugin-owned row forks it away
from its plugin. Spreading those across each caller is what lets one of them
quietly skip a step, so every catalog edit goes through ``apply_catalog_edit``
and the DB layer stays a plain writer.

The check a new or edited row owes before any of that (``mcp_validation``) is
asked here too, for the doors that write a user's own server: no row is saved,
and no edit lands, on a configuration nothing has shown to work.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from src.server.database.mcp_oauth import SERVABLE, ConnectionStatus, get_connection
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_catalog_server,
    get_catalog_server,
    list_catalog_servers,
    set_catalog_server_enabled,
    update_catalog_server,
)
from src.server.models.mcp_server import McpServerInput, sandbox_name_error
from src.server.services.brokerages import brokerage_by_name, brokerage_names
from src.server.services.mcp_config import builtin_names, same_consented_url
from src.server.services.mcp_dial import consented_bearer, dial_moved
from src.server.services.mcp_validation import (
    McpSignInRequired,
    Passed,
    SignInNeeded,
    check_http_error,
    check_server,
)

logger = logging.getLogger(__name__)


def reject_reserved_catalog_name(name: str) -> None:
    """Names a hand-written catalog row may not claim, and why each is spoken for.

    Both are joined by name somewhere the user reads as an identity, so a row
    that takes one is presented as something it is not: a row named after a
    shipped brokerage is shown wearing that broker's label, tile and warnings.
    It lives here because the reservation is a property of the catalog, not of
    the door: the Plugins create, the workspace add and both imports all mint a
    row, and a name only counts as reserved if every one of them says so. The
    sandbox's own names are refused first, and only here at creation: an edit
    keeps whatever name the row was saved under.
    """
    if reason := sandbox_name_error(name):
        raise HTTPException(status_code=422, detail=reason)
    if name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{name!r} collides with a built-in server name",
        )
    if name in brokerage_names():
        raise HTTPException(
            status_code=409,
            detail=f"{name!r} is reserved for a brokerage connector this build ships",
        )


def detach_warning(plugin_name: str) -> str:
    """The one sentence for fork-on-edit, shared by every path that detaches.

    The Plugins edit and the workspace edit are the same operation on the same
    row, so a reader who meets both must not be able to read a wording
    difference as a difference in what happened.
    """
    return (
        f"This server was installed by the plugin {plugin_name!r}; your edit "
        "detaches it, so plugin updates will no longer change it."
    )


@dataclass(frozen=True, slots=True)
class CatalogEdit:
    """The committed row, plus the plugin the edit forked it away from."""

    row: dict[str, Any]
    detached_from_plugin: str | None


async def apply_catalog_edit(
    user_id: str,
    name: str,
    fields: Mapping[str, Any],
    *,
    detach_plugin: bool,
    expect_plugin: str | None = None,
    check: McpServerInput | None = None,
    workspace_id: str | None = None,
    conn=None,
) -> CatalogEdit | None:
    """Edit a catalog row and settle everything the edit moved. None if absent.

    ``detach_plugin`` is the fork-on-edit decision: a user edit clears the
    row's provenance so a later plugin update sees the name un-owned and skips
    it instead of overwriting the customization, while the plugin update path
    itself passes False to edit its own row in place.

    ``expect_plugin`` is what makes that skip hold under a race. A plugin
    update decides to write by reading ownership first, and a Customize landing
    between that read and this write hands the row to the user; the predicate
    turns the write into a no-op (None) instead of an overwrite of the fork.
    None means the write is unconditional, which is what a user edit wants.

    ``check`` is the edit as the server it makes, for a user's edit: when it
    moves a dial field the server is checked before anything is written, and
    a failed check raises the response (422, or 409 for a sign-in) with the
    row untouched. A connected server whose address stays put is checked with
    its own sign-in. None writes unchecked, which is for a plugin update (it
    checks its own servers) and a sign-in callback (the sign-in was the
    check). ``workspace_id`` picks the computer a command server starts on.

    ``conn`` lets a caller batch the catalog reads and the write onto its own
    connection. The OAuth teardown always runs on a separate one, so a caller
    inside an open transaction that has already bumped workspace versions would
    self-deadlock: pass ``conn`` only outside one.
    """
    # Pre-update read: the rediscovery decision below needs the OLD fingerprint,
    # and the update returns only the new row.
    prior = await get_catalog_server(user_id, name, conn=conn)
    if check is not None and prior is not None and dial_moved(prior, fields):
        await require_passing_check(
            user_id,
            check,
            workspace_id=workspace_id,
            bearer=await consented_bearer(user_id, check),
        )
    updates = dict(fields)
    if detach_plugin:
        updates["plugin_id"] = None
        updates["plugin_server_key"] = None
        if prior is not None and prior.get("plugin_enabled") is False:
            # Suppression lives in the join predicate, (plugin_id IS NULL OR
            # p.enabled), so clearing plugin_id turns a row the user had
            # switched off at the plugin into an unconditionally delivered one.
            # Editing a description is not consent to start running it, so the
            # OFF state moves onto the row itself and the user turns it back on
            # deliberately. Through the dedicated toggle rather than `updates`:
            # `enabled` is deliberately outside the writable set so a request
            # body can never carry it, and the toggle owns the version bump.
            # Before the write below, so the row it returns is already honest.
            await set_catalog_server_enabled(user_id, name, False)
    # The row write and its version fan-out are one transaction in the DB layer.
    row = await update_catalog_server(
        user_id, name, updates=updates, owned_by_plugin=expect_plugin, conn=conn
    )
    if row is None:
        return None
    from src.server.services.mcp_descriptions.prompt_text import users_own_words

    # Clearing the user's own description moves no tools, so nothing else
    # would queue the generated one it now falls back to.
    if prior is not None and users_own_words(prior) and not users_own_words(row):
        from src.server.services.mcp_descriptions.worker import requeue_server

        await requeue_server(user_id, name)

    # Force reconnect when the edit moves an OAuth-connected server off its
    # consented endpoint: the stored token was issued for the old host, so it
    # must not carry to the new one. The grant already pins to the connection's
    # server_url, so no token can leak in the meantime — this revokes the now-
    # stale connection so the UI shows a clean reconnect. The revoke writes only
    # OAuth state, never this catalog row, so callers can build their response
    # from the row returned here.
    from src.server.services.mcp_oauth.discovery import (
        schedule_post_edit_rediscovery,
    )
    from src.server.services.mcp_oauth.lifecycle import revoke_if_consent_moved

    # The consent check runs against a fresh read, never this request's own
    # values: the write above and the revoke below are separate transactions,
    # so a concurrent edit that already moved the row back onto the consented
    # endpoint would otherwise be revoked on values no row still holds. A row
    # deleted underneath us needs neither — DELETE revokes on both sides of its
    # own drop.
    committed = await get_catalog_server(user_id, name, conn=conn)
    if committed is not None and not await revoke_if_consent_moved(
        user_id, name, transport=committed["transport"], url=committed.get("url")
    ):
        # Consent survived the edit; if the discovery fingerprint moved, the
        # connection's cached snapshot just went stale under it.
        schedule_post_edit_rediscovery(user_id, name, prior=prior, updated=row)

    return CatalogEdit(
        row=row,
        detached_from_plugin=(
            prior.get("plugin_name") if detach_plugin and prior else None
        ),
    )


async def refuse_unplaceable(user_id: str, name: str, *, taken: str) -> None:
    """Refuse a new row whose name is taken or that the account has no room for.

    Asked before the check rather than left to the insert, which still enforces
    both under its lock: the check dials the server, and a dial whose answer
    the insert then throws away spends the user's wait on nothing.
    """
    rows = await list_catalog_servers(user_id)
    if any(r["name"] == name for r in rows):
        raise HTTPException(status_code=409, detail=taken)
    if len(rows) >= MAX_CATALOG_SERVERS_PER_USER:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Maximum of {MAX_CATALOG_SERVERS_PER_USER} MCP catalog "
                "servers per user reached"
            ),
        )


async def require_passing_check(
    user_id: str,
    server: McpServerInput,
    *,
    workspace_id: str | None = None,
    bearer: str | None = None,
) -> None:
    """Check a server as it is about to be saved, or raise the response.

    422 when it failed, 409 when only a sign-in can save it (see
    ``check_http_error``); either way the caller has written nothing yet.
    """
    result = await check_server(
        user_id, server, workspace_id=workspace_id, bearer=bearer
    )
    if not isinstance(result, Passed):
        raise check_http_error(result)


async def sign_in_missing(
    user_id: str, row: Mapping[str, Any]
) -> SignInNeeded | None:
    """The sign-in a catalog row lacks before it may be switched on, or None.

    A row needs one when it is a shipped brokerage, when its grant for the
    address it points at lapsed (``needs_reauth``: the user never let go of
    it), or when its server last answered a listing by asking for one. Such a
    row is switched on only while a connection that can be served stands for
    that address: on without one, the agent is handed a server every call to
    which fails.

    A revoked connection, or one granted for another address, is history,
    not a claim either way: the user disconnected, remade the row, or moved
    it, and the config resolver serves the row off its own headers. A revoked
    one at this address is asked about live, as the row is now, because
    nothing stored says whether the server still wants the sign-in the user
    let go of or takes the key they may have added since. A command or legacy
    SSE row has no sign-in to miss.
    """
    if row.get("transport") != "http":
        return None
    name = row["name"]
    url = row.get("url") or ""
    needed = SignInNeeded(server=name, url=url)
    # A plugin's row under a brokerage name is the plugin's server, not the
    # broker's (the brokerage switch refuses to adopt it for the same reason).
    brokerage = name in brokerage_names() and not row.get("plugin_id")
    connection = await get_connection(user_id, name)
    if connection is not None and same_consented_url(connection.server_url, url):
        if connection.status in SERVABLE:
            return None
        if connection.status != ConnectionStatus.REVOKED or brokerage:
            return needed
        return None if await _answers_without_sign_in(user_id, row) else needed
    if brokerage:
        return needed
    return needed if await _last_answer_was_sign_in(user_id, row) else None


def sign_in_to_enable(*needed: SignInNeeded) -> HTTPException:
    """The 409 a switch answers with when a row lacks its sign-in.

    The same shape a save answers with, so the client starts the sign-in the
    same way; only the sentence differs, since the row already exists.
    """
    error = check_http_error(McpSignInRequired(needed))
    error.detail = {**error.detail, "message": "Sign in to the server to switch it on."}
    return error


async def refuse_missing_sign_in(user_id: str, row: Mapping[str, Any]) -> None:
    """Refuse switching on a row that cannot answer without the sign-in it
    lacks (409), wherever the switch is: on Plugins or in a workspace."""
    if needed := await sign_in_missing(user_id, row):
        raise sign_in_to_enable(needed)


async def _answers_without_sign_in(user_id: str, row: Mapping[str, Any]) -> bool:
    """Whether the row, as saved, passes a check with no sign-in behind it.

    Anything short of a pass (a sign-in challenge, a refused key, no answer)
    keeps the switch off, since that is the state a failed check leaves.
    """
    try:
        server = McpServerInput(
            name=row["name"],
            transport="http",
            url=row.get("url"),
            headers=dict(row.get("headers") or {}),
        )
    except ValidationError:
        return False
    return isinstance(await check_server(user_id, server), Passed)


async def _last_answer_was_sign_in(user_id: str, row: Mapping[str, Any]) -> bool:
    """Whether the row's probe under its current configuration said ``oauth``.

    Read under the current fingerprint only: an edit since (a key added to its
    headers, say) moves the fingerprint, and the old answer stops describing
    the server the row now makes.
    """
    from src.server.database.mcp_tool_schemas import get_user_tool_schemas
    from src.server.models.mcp_server import snapshot_probe
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import ToolSnapshotIndex

    try:
        config = user_row_to_server_config(dict(row))
    except Exception:  # noqa: BLE001 — a row it cannot read was never probed either
        return False
    index = ToolSnapshotIndex(user_rows=await get_user_tool_schemas(user_id))
    probe = snapshot_probe(index.snapshot(config))
    return probe is not None and probe.verdict == "oauth"


async def _relay_execution_warning(user_id: str, name: str) -> str | None:
    """A row the relay has to carry runs only through it, and activation is the
    moment to tell the user their deployment cannot actually run it.

    Two shapes need the warning: an OAuth connection, whose token only the
    relay spends, and an ``http`` row with a tool on the direct path, which the
    model reaches by dialing the relay too. The second is read off the row's
    own binding map rather than its probe verdict: over-warning a row that is
    failing its probe costs a sentence, while waiting for a verdict would keep
    the warning from the silent case it exists for, where the sync binds
    nothing and the tool is gone from both agents.
    """
    from src.config.env import EGRESS_RELAY_SECRET
    from src.server.app import setup
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.egress.reachability import (
        effective_relay_base_url,
        relay_reachability_warning,
    )
    from src.server.services.tool_binding import inputs_from_row, resolve_plan

    if setup.agent_config is None:
        return None
    connection = await get_connection(user_id, name)
    # A revoked connection is history, not a claim on the row (the rule the
    # resolver and the relay apply), so the row is judged by its own binding.
    if connection is None or connection.status is ConnectionStatus.REVOKED:
        row = await get_catalog_server(user_id, name)
        if row is None or row.get("transport") != "http":
            return None
        # Planned off the row's own address and consent to nothing, the way
        # every other reader plans a row that has no connection.
        plan = resolve_plan(vendor_for_url(row.get("url")), (), inputs_from_row(row))
        if not plan.direct:
            return None
    if not EGRESS_RELAY_SECRET:
        return (
            "The egress relay is disabled (EGRESS_RELAY_SECRET is not set), so "
            "this server's tools cannot run in sandboxes. Set a strong "
            "EGRESS_RELAY_SECRET in the backend environment and restart."
        )
    provider = await _relay_provider_kind(user_id)
    return relay_reachability_warning(provider, effective_relay_base_url(provider))


async def _relay_provider_kind(user_id: str) -> str | None:
    """Which provider's relay reachability this user's warning is about.

    A user's own machine decides where their sandboxes run, so its kind beats
    the deployment default; a user with no computer yet gets the default.
    """
    from src.server.app import setup

    try:
        from src.server.database.computer import get_primary_computer

        computer = await get_primary_computer(user_id)
        if computer and computer.get("kind"):
            return str(computer["kind"])
    except Exception as e:
        logger.debug(f"Could not resolve the user's computer kind: {e}")
    if setup.agent_config is None:
        return None
    return setup.agent_config.sandbox.provider


async def apply_catalog_enabled(
    user_id: str, name: str, enabled: bool
) -> tuple[dict | None, str | None]:
    """The one place a *switch* flips a catalog row. Returns the row and
    whatever the user is owed about it, or ``(None, None)`` if it is gone.

    Every switch routes through here so no caller can end up with half of what
    another does: the DB layer bumps every workspace's ``mcp_config_version``
    in the same transaction (next-acquire convergence), and disable also has to
    bite now rather than at next acquire, which ``revoke_live_grants`` carries
    the reasoning for.

    The column itself has another writer, the disable an edit does before
    rewriting a row. It reaches the DB toggle directly and means to: it is not
    a user flipping a switch, so it owes no relay warning.

    Switching on a row that needs a sign-in it does not have raises the 409
    that starts one (``sign_in_missing``), and writes nothing. Switching off
    is always allowed.
    """
    from src.server.services.mcp_oauth.lifecycle import revoke_live_grants

    if enabled:
        current = await get_catalog_server(user_id, name)
        if current is None:
            return None, None
        await refuse_missing_sign_in(user_id, current)
    row = await set_catalog_server_enabled(user_id, name, enabled)
    if row is None:
        return None, None
    if enabled:
        from src.server.services.mcp_descriptions.worker import requeue_server

        # A row switched off drops its queued description, and switching it
        # back on moves no tools, so nothing else would queue it again.
        await requeue_server(user_id, name)
        return row, await _relay_execution_warning(user_id, name)
    await revoke_live_grants(user_id, [name])
    return row, None


def brokerage_server(name: str) -> McpServerInput:
    """A shipped brokerage as the server its row saves; raises the refusals.

    404 for a name the registry does not ship, so no door can be used to write
    an arbitrary server under it, and 409/422 for a name the row cannot take.
    """
    brokerage = brokerage_by_name(name)
    if brokerage is None:
        raise HTTPException(status_code=404, detail="Unknown brokerage")
    if brokerage.name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{brokerage.name!r} collides with a built-in server name",
        )
    # A create like any other, so the names the sandbox reserves hold here too.
    if reason := sandbox_name_error(brokerage.name):
        raise HTTPException(status_code=422, detail=reason)
    try:
        # Through the same validator every user-written row passes, so our own
        # definition cannot be the one payload that skips the URL policy. Its
        # ValidationError is a ValueError, so it answers here rather than
        # escaping as an untyped 500.
        return McpServerInput(
            name=brokerage.name,
            transport="http",
            url=brokerage.url,
            description=brokerage.description,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


async def create_brokerage_row(
    user_id: str, name: str, *, enabled: bool = True, conn=None
) -> dict[str, Any]:
    """Bring a shipped brokerage into the user's catalog; the new row.

    Its only caller is the brokerage's sign-in callback: a brokerage is saved
    by its sign-in, never by the switch, so it lands switched on by default.
    The callback stores the connection in its own transaction and deletes
    this row if that fails: the create bumps every workspace and the consent
    write takes the egress lock, the reverse of the order a workspace delete
    takes them in. Raises HTTPException: ``brokerage_server``'s refusals, and
    409 for a duplicate or a full account.
    """
    server = brokerage_server(name)
    try:
        return await create_catalog_server(
            user_id,
            server.name,
            enabled=enabled,
            conn=conn,
            **server.to_catalog_fields(),
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
