"""Saving a server, through its ordinary route or through its sign-in.

A server that asks for a sign-in is written by the sign-in's callback, never
before it: a row saved first and signed in to second is left behind whenever
the user walks away from the consent screen, switched on and failing every
call. So the start of such a sign-in proves everything it can without the
user's token (the name is free, every other server of a plugin passes its
check, the server really does ask for a sign-in) and parks what the save
needs; the callback, holding the token, checks the server with it and only
then writes the row and its connection together.

Each save is one object whichever door it comes through, so a refusal added
to a route cannot skip its sign-in: ``CatalogAdd`` (Plugins), ``WorkspaceAdd``,
``ServerEdit`` (from either) and ``BrokerageAdd``, which only a sign-in saves.
The fifth thing a sign-in saves is a plugin whose one remote server needs it,
installed by the plugin's own commit. A sign-in replaces a save's check,
never its decision about whether the row is switched on.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError

from src.server.database.mcp_oauth import ConnectionStatus, get_connection
from src.server.database.mcp_servers import (
    create_catalog_server,
    create_workspace_catalog_server,
    delete_catalog_server,
    get_catalog_server,
)
from src.server.database.workspace import get_workspace
from src.server.models.mcp_server import McpServerInput
from src.server.services.brokerages import brokerage_by_name, brokerage_names
from src.server.services.mcp_apply import schedule_proactive_apply
from src.server.services.mcp_catalog import (
    CatalogEdit,
    apply_catalog_edit,
    apply_catalog_enabled,
    brokerage_server,
    create_brokerage_row,
    refuse_unplaceable,
    reject_reserved_catalog_name,
    require_passing_check,
)
from src.server.services.mcp_config import (
    Origin,
    ServerRef,
    builtin_names,
    classify_server_name,
    same_consented_url,
)
from src.server.services.mcp_dial import consented_bearer, dial_moved, dial_of
from src.server.services.mcp_oauth.finish import (
    CallbackFailed,
    CallbackFinish,
    PreparedSignIn,
    StoreConnection,
    claim_parked,
    stale_sign_in,
)
from src.server.services.mcp_oauth.lifecycle import disconnect_server, oauth_fence
from src.server.services.mcp_oauth.redirects import CallbackError
from src.server.services.mcp_validation import (
    CheckFailure,
    Passed,
    check_http_error,
    check_server,
)
from src.server.services.plugins import (
    PendingPluginInstall,
    PluginRejected,
    commit_pending_plugin_install,
    refuse_pending_commit,
    undo_pending_plugin_install,
)
from src.server.utils.api import require_workspace_owner
from src.server.utils.error_sanitization import validation_error_text

logger = logging.getLogger(__name__)

NOT_FOUND = "MCP server not found"


class PendingKind(StrEnum):
    SERVER = "server"
    WORKSPACE_SERVER = "workspace_server"
    SERVER_EDIT = "server_edit"
    BROKERAGE = "brokerage"
    PLUGIN = "plugin"


def parse_server(raw: Mapping[str, Any]) -> McpServerInput:
    """A server body as every door reads it: 422 with each field's refusal,
    never the input it rejected, which can carry a credential."""
    try:
        return McpServerInput(**raw)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=validation_error_text(e))


def name_taken(name: str) -> str:
    return (
        f"A server named {name!r} already exists on your account. "
        "Choose another name."
    )


async def _owned_workspace(user_id: str, workspace_id: str) -> None:
    require_workspace_owner(await get_workspace(workspace_id), user_id=user_id)


# ---------------------------------------------------------------------------
# The saves
# ---------------------------------------------------------------------------


class _Add:
    """A new row: refused, checked, then written.

    ``write`` with a ``store`` is the sign-in's: the row and the connection
    cannot share a transaction (the row's write takes the user's write lock
    and bumps every workspace, the connection's takes the egress lock, which
    workspace deletion takes in the other order), so the row goes first and
    comes back off if the connection does not land.
    """

    kind: PendingKind
    workspace_id: str | None = None

    def __init__(self, user_id: str, server: McpServerInput) -> None:
        self.user_id = user_id
        self.server = server

    async def refuse(self) -> None:
        reject_reserved_catalog_name(self.server.name)
        await refuse_unplaceable(
            self.user_id, self.server.name, taken=name_taken(self.server.name)
        )

    async def check(self) -> None:
        """The ordinary route's check, which a sign-in replaces."""
        await require_passing_check(
            self.user_id, self.server, workspace_id=self.workspace_id
        )

    async def write(self, store: StoreConnection | None = None) -> dict[str, Any]:
        try:
            row = await self._insert()
        except ValueError as e:
            # The insert's own refusals, for a create that raced this one.
            raise HTTPException(status_code=409, detail=str(e))
        if store is not None:
            row_id = row["user_mcp_server_id"]
            await _store_or_take_back(
                store, lambda: self._take_back(row_id), self.server.name
            )
        return row

    async def after_write(self) -> None:
        return None

    async def _insert(self) -> dict[str, Any]:
        raise NotImplementedError

    async def _take_back(self, row_id: str) -> None:
        """Delete the row this save created, behind the OAuth fence.

        By its id, since the name may hold another request's row by now. The
        fence revokes a connection under the name should the store have
        committed after all, so no token outlives the row it was for.
        """
        async with oauth_fence(self.user_id, [self.server.name]):
            await delete_catalog_server(self.user_id, self.server.name, row_id=row_id)

    def prepared(self) -> PreparedSignIn:
        return PreparedSignIn(
            self.kind, self.server.name, self.server.url or "", self._parked()
        )

    def _parked(self) -> BaseModel:
        return _ParkedAdd(server=self.server)


class CatalogAdd(_Add):
    """``POST /api/v1/mcp/servers``. No ``enabled`` is written, so the row
    takes the insert's default."""

    kind = PendingKind.SERVER

    async def _insert(self) -> dict[str, Any]:
        return await create_catalog_server(
            self.user_id, self.server.name, **self.server.to_catalog_fields()
        )


class WorkspaceAdd(_Add):
    """``POST /api/v1/workspaces/{id}/mcp/servers``: on here, off everywhere
    else, with a command server checked on this workspace's computer."""

    kind = PendingKind.WORKSPACE_SERVER

    def __init__(self, user_id: str, server: McpServerInput, workspace_id: str) -> None:
        super().__init__(user_id, server)
        self.workspace_id = workspace_id

    async def refuse(self) -> None:
        await _owned_workspace(self.user_id, self.workspace_id)
        await super().refuse()

    async def _insert(self) -> dict[str, Any]:
        return await create_workspace_catalog_server(
            self.user_id,
            self.workspace_id,
            self.server.name,
            **self.server.to_catalog_fields(),
        )

    async def after_write(self) -> None:
        # Brings the workspace up to the new config before its next turn.
        schedule_proactive_apply(self.workspace_id, self.user_id)

    def _parked(self) -> BaseModel:
        return _ParkedWorkspaceAdd(server=self.server, workspace_id=self.workspace_id)


class BrokerageAdd(_Add):
    """A shipped brokerage's first connect, which is what saves its row.

    It lands switched on: connecting a brokerage is how it is turned on. A
    brokerage that already has a row reconnects that row instead.
    """

    kind = PendingKind.BROKERAGE

    def __init__(self, user_id: str, name: str) -> None:
        super().__init__(user_id, brokerage_server(name))

    async def refuse(self) -> None:
        brokerage = brokerage_by_name(self.server.name)
        label = brokerage.label if brokerage is not None else self.server.name
        await refuse_unplaceable(
            self.user_id,
            self.server.name,
            taken=f"{label} is already set up; connect it from its row.",
        )

    async def _insert(self) -> dict[str, Any]:
        return await create_brokerage_row(self.user_id, self.server.name, enabled=True)

    def _parked(self) -> BaseModel:
        return _ParkedBrokerage(name=self.server.name, url=self.server.url or "")


class ServerEdit:
    """An edit of a user's server, from Plugins or from a workspace: one row
    either way, through the same service.

    The path name is authoritative; a renamed body is refused rather than
    silently creating a second row under a different key. So an edit never
    introduces a name, which is why the sandbox's reserved names are not
    checked: a row saved before its name was reserved stays editable.
    """

    def __init__(
        self,
        user_id: str,
        name: str,
        server: McpServerInput,
        workspace_id: str | None = None,
    ) -> None:
        self.user_id = user_id
        self.name = name
        self.server = server
        self.workspace_id = workspace_id

    async def refuse(self) -> ServerRef | None:
        """Raise the edit's refusals; from a workspace, the row as it stands
        there."""
        if self.workspace_id is not None:
            await _owned_workspace(self.user_id, self.workspace_id)
            if self.name in builtin_names():
                raise HTTPException(
                    status_code=409, detail="Cannot edit a built-in server"
                )
            if self.name in brokerage_names():
                raise HTTPException(
                    status_code=409,
                    detail="Manage this brokerage connection from Plugins",
                )
        if self.server.name != self.name:
            raise HTTPException(
                status_code=409, detail="name in body must match the path name"
            )
        if self.workspace_id is None:
            return None
        ref = await classify_server_name(self.workspace_id, self.user_id, self.name)
        if ref is None or ref.origin is not Origin.USER:
            raise HTTPException(status_code=404, detail=NOT_FOUND)
        return ref

    async def write(self) -> CatalogEdit:
        """The ordinary route's write: checked when it moves how the server is
        reached, and forked off its plugin (the service owns both, with the
        consent revoke and the rediscovery kick)."""
        edit = await apply_catalog_edit(
            self.user_id,
            self.name,
            self.server.to_catalog_fields(),
            detach_plugin=True,
            check=self.server,
            workspace_id=self.workspace_id,
        )
        if edit is None:
            raise HTTPException(status_code=404, detail=NOT_FOUND)
        return edit

    async def after_write(self) -> None:
        if self.workspace_id is not None:
            schedule_proactive_apply(self.workspace_id, self.user_id)


# ---------------------------------------------------------------------------
# Start: refuse what can be refused, and require a sign-in
# ---------------------------------------------------------------------------


def _not_needed(message: str) -> HTTPException:
    """The answer for a server that saves through its ordinary route instead."""
    return HTTPException(
        status_code=422, detail={"code": "mcp_sign_in_not_needed", "message": message}
    )


async def _require_sign_in(
    user_id: str,
    server: McpServerInput,
    *,
    workspace_id: str | None = None,
    bearer: str | None = None,
) -> None:
    """Check the server as it would be saved; it must ask for a sign-in.

    One that passes, or that is not remote, saves through its ordinary route,
    and the client is told so rather than sent through a consent screen that
    proves nothing.
    """
    if server.transport != "http":
        raise _not_needed("Only a remote server signs in; save it directly.")
    result = await check_server(
        user_id, server, workspace_id=workspace_id, bearer=bearer
    )
    if isinstance(result, CheckFailure):
        raise check_http_error(result)
    if isinstance(result, Passed):
        raise _not_needed("This server does not ask for a sign-in; save it directly.")


async def start_add(save: CatalogAdd | WorkspaceAdd) -> PreparedSignIn:
    """An add whose server signs in, refused as its ordinary route would."""
    await save.refuse()
    await _require_sign_in(save.user_id, save.server, workspace_id=save.workspace_id)
    return save.prepared()


async def start_brokerage(save: BrokerageAdd) -> PreparedSignIn:
    """A brokerage's check may pass or ask for a sign-in, and either way its
    row is saved by the sign-in: a broker's tools are reached through the
    user's own account."""
    await save.refuse()
    result = await check_server(save.user_id, save.server)
    if isinstance(result, CheckFailure):
        raise check_http_error(result)
    return save.prepared()


async def start_edit(
    edit: ServerEdit, *, expected_url: str | None = None
) -> PreparedSignIn:
    """An edit whose new configuration signs in.

    ``expected_url`` is the address the page drew the row at; a row moved off
    it since is refused (409), the way the reconnect refuses one.
    """
    await edit.refuse()
    prior = await get_catalog_server(edit.user_id, edit.name)
    if prior is None:
        raise HTTPException(status_code=404, detail=NOT_FOUND)
    if expected_url is not None and not same_consented_url(
        prior.get("url"), expected_url
    ):
        raise HTTPException(
            status_code=409,
            detail="This server's address changed since the page was loaded",
        )
    if not dial_moved(prior, edit.server.to_catalog_fields()):
        raise _not_needed(
            "This edit does not change how the server is reached; save it directly."
        )
    await _require_sign_in(
        edit.user_id,
        edit.server,
        workspace_id=edit.workspace_id,
        bearer=await consented_bearer(edit.user_id, edit.server),
    )
    return PreparedSignIn(
        PendingKind.SERVER_EDIT,
        edit.name,
        edit.server.url or "",
        _ParkedEdit(
            server=edit.server, workspace_id=edit.workspace_id, prior=dial_of(prior)
        ),
    )


def start_plugin(pending: PendingPluginInstall) -> PreparedSignIn:
    """A plugin install prepared by ``plugins.prepare_pending_install``."""
    return PreparedSignIn(
        PendingKind.PLUGIN,
        pending.sign_in.server,
        pending.sign_in.url,
        pending,
        plugin=pending.plugin_name,
    )


# ---------------------------------------------------------------------------
# Callback: refuse again, check with the token, write around the connection
# ---------------------------------------------------------------------------

_NOT_SAVED = "the sign-in could not be saved; try again"


def _http_detail(e: HTTPException) -> str:
    detail = e.detail
    if isinstance(detail, Mapping):
        return str(detail.get("message") or detail.get("code") or "refused")
    return str(detail)


async def _refused(step: Awaitable[Any]) -> Any:
    """Run a save's step, its refusal becoming a reason the callback reports."""
    try:
        return await step
    except HTTPException as e:
        raise CallbackFailed(CallbackError.INSTALL_FAILED, _http_detail(e)) from None


async def _store_or_take_back(
    store: StoreConnection, take_back: Callable[[], Awaitable[None]], what: str
) -> None:
    try:
        await store()
    except Exception as e:
        logger.exception("[mcp_oauth] could not store the sign-in for %s", what)
        try:
            await take_back()
        except Exception:
            logger.exception("[mcp_oauth] could not take back %s", what)
        raise CallbackFailed(CallbackError.INSTALL_FAILED, _NOT_SAVED) from e


async def _verify(user_id: str, name: str, url: str, bearer: str) -> None:
    """Check the server with the token the sign-in just issued, which goes
    out alone (``probe_configured``)."""
    try:
        probe = McpServerInput(name=name, transport="http", url=url)
    except ValidationError as e:
        raise CallbackFailed(
            CallbackError.INSTALL_FAILED, validation_error_text(e)
        ) from None
    result = await check_server(user_id, probe, bearer=bearer)
    if isinstance(result, Passed):
        return
    reason = (
        result.reason
        if isinstance(result, CheckFailure)
        else "the server did not accept the sign-in"
    )
    raise CallbackFailed(CallbackError.INSTALL_FAILED, reason)


class _AddFinish:
    """An add, refused before and after the exchange as its route refuses it."""

    plugin = None

    def __init__(self, save: _Add) -> None:
        self.save = save

    async def before_exchange(self) -> None:
        await _refused(self.save.refuse())

    async def after_exchange(self, bearer: str) -> None:
        await self.before_exchange()
        server = self.save.server
        await _verify(self.save.user_id, server.name, server.url or "", bearer)

    async def write(self, store: StoreConnection) -> None:
        await _refused(self.save.write(store))

    async def after_connect(self) -> None:
        await self.save.after_write()


class _EditFinish:
    """An edit lands after its connection, not before.

    Stored first, a connection that fails takes nothing with it: the row and
    its old connection are untouched. The edit that follows then finds its
    connection already at the new address, so it revokes nothing. The row
    keeps the switch the edit leaves it with: nothing here turns it on, and
    an edit that detaches it from a switched-off plugin turns it off.

    The store does overwrite the old connection, so an edit that then fails
    cannot put the row back as it was. Whether it failed is read off the row,
    not the exception: one raised after the update committed (the consent
    check, the rediscovery it schedules) left the edit in place, which is a
    save that landed.
    """

    plugin = None

    def __init__(self, edit: ServerEdit, prior: Mapping[str, Any]) -> None:
        self.edit = edit
        self.prior = dict(prior)

    @property
    def _user_id(self) -> str:
        return self.edit.user_id

    @property
    def _name(self) -> str:
        return self.edit.name

    async def before_exchange(self) -> None:
        row = await get_catalog_server(self._user_id, self._name)
        if row is None or dial_of(row) != self.prior:
            raise CallbackFailed(CallbackError.SERVER_CHANGED)

    async def after_exchange(self, bearer: str) -> None:
        await self.before_exchange()
        await _verify(self._user_id, self._name, self.edit.server.url or "", bearer)

    async def write(self, store: StoreConnection) -> None:
        # Asked again: the check with the new token is a round trip to the
        # server, room enough for another edit to land that this one would
        # otherwise overwrite.
        await self.before_exchange()
        try:
            signed_in = await self._signed_in_where_it_was()
            await store()
        except Exception as e:
            logger.exception("[mcp_oauth] could not store the sign-in for %s", self._name)
            raise CallbackFailed(CallbackError.INSTALL_FAILED, _NOT_SAVED) from e
        fields = self.edit.server.to_catalog_fields()
        try:
            edit = await apply_catalog_edit(
                self._user_id, self._name, fields, detach_plugin=True
            )
        except Exception:
            logger.exception("[mcp_oauth] could not apply the edit to %s", self._name)
            edit = None
        if edit is not None:
            return
        landed = await self._landed(fields)
        if landed:
            return
        # Off first, so the row is never on behind a connection that is gone.
        # Off only when it stood on a sign-in the store overwrote (or where
        # it stands is unknown): a row that never had one still answers
        # without it, and turning it off would cost the user a working server.
        if signed_in or landed is None:
            await _switch(self._user_id, self._name, False)
        try:
            # The connection names an address the row never moved to.
            await disconnect_server(self._user_id, self._name)
        except Exception:
            logger.exception(
                "[mcp_oauth] could not take back the sign-in for %s", self._name
            )
        raise CallbackFailed(
            CallbackError.INSTALL_FAILED,
            "the server could not be updated; try the edit again",
        )

    async def after_connect(self) -> None:
        await self.edit.after_write()

    async def _signed_in_where_it_was(self) -> bool:
        """Whether the row is served through a sign-in (live or lapsed) at the
        address it holds now: the connection the store is about to overwrite."""
        connection = await get_connection(self._user_id, self._name)
        if connection is None or connection.status == ConnectionStatus.REVOKED:
            return False
        return same_consented_url(connection.server_url, self.prior.get("url"))

    async def _landed(self, fields: Mapping[str, Any]) -> bool | None:
        """Whether the row now carries the edit's dial fields; None if unread."""
        try:
            row = await get_catalog_server(self._user_id, self._name)
        except Exception:
            logger.exception("[mcp_oauth] could not read back %s", self._name)
            return None
        return row is not None and not dial_moved(row, fields)


class _PluginFinish:
    def __init__(self, user_id: str, pending: PendingPluginInstall) -> None:
        self.user_id = user_id
        self.pending = pending
        self.plugin = pending.plugin_name

    async def before_exchange(self) -> None:
        # The commit asks again under its lock; asked here so the code is not
        # spent on an install that cannot land.
        try:
            await refuse_pending_commit(self.user_id, self.pending)
        except ValueError as e:
            raise CallbackFailed(CallbackError.INSTALL_FAILED, str(e)) from None

    async def after_exchange(self, bearer: str) -> None:
        await self.before_exchange()
        sign_in = self.pending.sign_in
        await _verify(self.user_id, sign_in.server, sign_in.url, bearer)

    async def write(self, store: StoreConnection) -> None:
        try:
            committed = await commit_pending_plugin_install(self.user_id, self.pending)
        except (ValueError, PluginRejected) as e:
            raise CallbackFailed(CallbackError.INSTALL_FAILED, str(e)) from None
        except Exception as e:
            logger.exception(
                "[mcp_oauth] pending install of %s failed", self.pending.plugin_name
            )
            raise CallbackFailed(
                CallbackError.INSTALL_FAILED, "the plugin could not be installed"
            ) from e

        async def take_back() -> None:
            await undo_pending_plugin_install(self.user_id, committed.progress)

        await _store_or_take_back(store, take_back, self.pending.plugin_name)

    async def after_connect(self) -> None:
        return None


# What each kind parks, and the finish it is read back as.


class _ParkedAdd(BaseModel):
    server: McpServerInput

    def finish(self, user_id: str) -> CallbackFinish:
        return _AddFinish(CatalogAdd(user_id, self.server))


class _ParkedWorkspaceAdd(_ParkedAdd):
    workspace_id: str

    def finish(self, user_id: str) -> CallbackFinish:
        return _AddFinish(WorkspaceAdd(user_id, self.server, self.workspace_id))


class _ParkedEdit(BaseModel):
    server: McpServerInput
    workspace_id: str | None = None
    # The row's dial fields when the sign-in started: a row moved off them
    # since is not the row the user signed in for.
    prior: dict[str, Any]

    def finish(self, user_id: str) -> CallbackFinish:
        edit = ServerEdit(user_id, self.server.name, self.server, self.workspace_id)
        return _EditFinish(edit, self.prior)


class _ParkedBrokerage(BaseModel):
    name: str
    # The address the sign-in was started for, which its token is for.
    url: str

    def finish(self, user_id: str) -> CallbackFinish:
        try:
            save = BrokerageAdd(user_id, self.name)
        except HTTPException:
            # No longer a brokerage this build ships.
            raise stale_sign_in() from None
        if save.server.url != self.url:
            # A build that moved the brokerage since would save the row at an
            # address the sign-in was never for.
            raise stale_sign_in()
        return _AddFinish(save)


class _ParkedPlugin(PendingPluginInstall):
    def finish(self, user_id: str) -> CallbackFinish:
        return _PluginFinish(user_id, self)


_PARKED: dict[str, type[_ParkedAdd | _ParkedEdit | _ParkedBrokerage | _ParkedPlugin]] = {
    PendingKind.SERVER: _ParkedAdd,
    PendingKind.WORKSPACE_SERVER: _ParkedWorkspaceAdd,
    PendingKind.SERVER_EDIT: _ParkedEdit,
    PendingKind.BROKERAGE: _ParkedBrokerage,
    PendingKind.PLUGIN: _ParkedPlugin,
}


async def claim(state: str, user_id: str, server_name: str) -> CallbackFinish:
    """The save a pending sign-in's callback finishes, taken once with its
    state. Raises CallbackFailed for one that is gone or that this build
    cannot read, which the user starts again."""
    parked = await claim_parked(state, user_id, server_name)
    model = _PARKED.get(parked.kind)
    if model is None:
        raise stale_sign_in()
    try:
        # A plugin's package decodes from base64 here, off the event loop.
        body = await asyncio.to_thread(model.model_validate, parked.body)
    except ValidationError:
        raise stale_sign_in() from None
    return body.finish(user_id)


async def switch_on(user_id: str, name: str) -> None:
    """Switch a row on once its sign-in is stored.

    Connecting is how a user makes a server work, and their Disconnect is
    what switched it off, so a connect that lands brings it back. Best effort:
    the connection is stored either way, and a row left off is one the user
    can switch on.
    """
    await _switch(user_id, name, True)


async def _switch(user_id: str, name: str, enabled: bool) -> None:
    """Through the one switch, which bumps every workspace and reports nothing
    a callback could show. Best effort, logged."""
    try:
        await apply_catalog_enabled(user_id, name, enabled)
    except Exception:
        logger.warning(
            "[mcp_oauth] could not switch %s %s after its sign-in",
            "on" if enabled else "off", name, exc_info=True,
        )
