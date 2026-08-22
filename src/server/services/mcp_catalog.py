"""The policy an edit to a ``user_mcp_servers`` row owes, in one place.

The write itself is the small part. An OAuth token consented for the old
endpoint must not carry onto a new one, a moved discovery fingerprint strands
the cached tool snapshot, and a user edit of a plugin-owned row forks it away
from its plugin. Spreading those across each caller is what lets one of them
quietly skip a step, so every catalog edit goes through ``apply_catalog_edit``
and the DB layer stays a plain writer.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from src.server.database.mcp_servers import (
    get_catalog_server,
    set_catalog_server_enabled,
    update_catalog_server,
)


def detach_warning(plugin_name: str) -> str:
    """The one sentence for fork-on-edit, shared by every path that detaches.

    PUT and promote-with-overwrite are the same operation on the same row, so
    a reader who meets both must not be able to read a wording difference as a
    difference in what happened.
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

    ``conn`` lets a caller batch the catalog reads and the write onto its own
    connection. The OAuth teardown always runs on a separate one, so a caller
    inside an open transaction that has already bumped workspace versions would
    self-deadlock: pass ``conn`` only outside one.
    """
    # Pre-update read: the rediscovery decision below needs the OLD fingerprint,
    # and the update returns only the new row.
    prior = await get_catalog_server(user_id, name, conn=conn)
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
