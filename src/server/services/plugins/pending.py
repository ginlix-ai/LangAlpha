"""A plugin install that waits on one server's sign-in.

A plugin with a sign-in server cannot be checked whole until the user has
signed in, and a server is saved only by its sign-in. So the install is split
around the consent screen: ``prepare_pending_plugin_install`` does everything
an install does short of writing (the refusals, the secrets, every other
server's check) and returns what the sign-in's callback needs to finish;
``commit_pending_plugin_install`` is that finish, run by the callback once it
holds the user's tokens.

The pending install carries the package bytes rather than where they came
from. A fetch at the callback could return a different tree than the one the
user consented to and the checks passed, and an upload has nowhere to be
fetched from again.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, PlainSerializer

from src.server.database.mcp_servers import get_catalog_server
from src.server.database.plugins import get_plugin, list_plugin_owned_secrets
from src.server.models.plugin import InstallReport
from src.server.services.mcp_validation import SignInNeeded
from src.server.services.plugins.checks import CheckedServers
from src.server.services.plugins.errors import PluginRefused
from src.server.services.plugins.lifecycle import (
    InstallProgress,
    check_plugin_install,
    install_plugin_package,
    uninstall_plugin,
)
from src.server.services.plugins.package import ValidatedPackage, validate_package
from src.server.services.plugins.supplied_secrets import drop_secrets
from src.server.services.vault_invalidation import after_secrets_changed

logger = logging.getLogger(__name__)


def _decoded(value: Any) -> Any:
    return base64.b64decode(value, validate=True) if isinstance(value, str) else value


# Raw bytes in Python, base64 text in JSON. Not pydantic's Base64Bytes, which
# decodes a Python value too and so would garble the archive it is handed.
_Archive = Annotated[
    bytes,
    BeforeValidator(_decoded),
    PlainSerializer(
        lambda raw: base64.b64encode(raw).decode("ascii"),
        return_type=str,
        when_used="json",
    ),
]


class PendingPluginInstall(BaseModel):
    """Everything the callback needs to install what the user consented to.

    Holds secret values and the whole package, so it is parked encrypted, and
    only for as long as the sign-in it waits on.
    """

    model_config = ConfigDict(frozen=True)

    plugin_name: str
    archive: _Archive
    subdir: str | None
    name_hint: str | None
    source_type: str
    source_ref: str | None
    content_hash: str
    secrets: dict[str, str]
    checked: CheckedServers
    sign_in: SignInNeeded
    # The mcp.json key of the sign-in server, so a report can be read back
    # against it even if the entry was renamed on the way to its row name.
    sign_in_key: str


async def prepare_pending_plugin_install(
    user_id: str,
    raw: bytes,
    package: ValidatedPackage,
    *,
    subdir: str | None,
    name_hint: str | None,
    source_type: str,
    source_ref: str | None,
    secrets: Mapping[str, str] | None = None,
) -> PendingPluginInstall:
    """Check the install, allowing exactly one sign-in, and write nothing.

    ``package`` is ``raw`` validated with ``subdir`` and ``name_hint``, the
    way the install routes resolve it; the commit validates the bytes again
    with the same two, so it lands the same tree.

    Raises everything ``check_plugin_install`` raises (refusals, secrets
    required, McpCheckFailed, more than one sign-in), and PluginRefused
    ``plugin_sign_in_not_needed`` when no server asks for a sign-in: that
    plugin installs directly, through the ordinary install route.
    """
    result = await check_plugin_install(
        user_id, package, secrets=secrets, allow_sign_in=True
    )
    if result.sign_in is None:
        raise PluginRefused(
            "plugin_sign_in_not_needed",
            "None of this plugin's servers asks for a sign-in; install it directly.",
        )
    key = next(
        (p.key for p in package.entry_plans if p.name == result.sign_in.server),
        result.sign_in.server,
    )
    return PendingPluginInstall(
        plugin_name=package.name,
        archive=raw,
        subdir=subdir,
        name_hint=name_hint,
        source_type=source_type,
        source_ref=source_ref,
        content_hash=package.content_hash,
        secrets=dict(secrets or {}),
        checked=result.checked,
        sign_in=result.sign_in,
        sign_in_key=key,
    )


async def refuse_pending_commit(user_id: str, pending: PendingPluginInstall) -> None:
    """The refusals a sign-in started minutes ago is likeliest to meet.

    Install refuses both too, but only after the server checks, whose message
    would blame a server rather than the plugin a second callback finds. The
    callback asks before spending its code, and the commit again under it.
    Raises ValueError.
    """
    if await get_plugin(user_id, pending.plugin_name) is not None:
        raise ValueError(
            f"{pending.plugin_name} was installed while you were signing in"
        )
    if await get_catalog_server(user_id, pending.sign_in.server) is not None:
        raise ValueError(
            f"a server named {pending.sign_in.server!r} was added while you "
            "were signing in; remove it and install the plugin again"
        )


@dataclass(frozen=True)
class CommittedPluginInstall:
    """What the commit wrote, for the callback to take back if the connection
    it was for cannot be stored."""

    progress: InstallProgress


async def commit_pending_plugin_install(
    user_id: str, pending: PendingPluginInstall
) -> CommittedPluginInstall:
    """Install the package the sign-in was started for.

    Does not dial anything: every server but the sign-in one passed at
    prepare, and the sign-in one passed when its callback received tokens.
    It does re-run what is cheap and may have changed since (the plugin name
    is free, no other plugin owns a component name, the caps, the sign-in row
    name is free, the declared secrets still have values), and writes a
    server only as it was checked. Raises on any refusal with nothing
    written, and ValueError when the install did not land whole (the sign-in
    server's row, or any other component), after taking that install back.
    Any other failure once the install has begun writing (the skills'
    fan-out, the hash stamp, the vault invalidation) is raised after the same
    take-back, so the callback never meets a half-installed plugin whose
    sign-in row is on with no connection.
    """
    package = await asyncio.to_thread(
        validate_package,
        pending.archive,
        subdir=pending.subdir,
        name_hint=pending.name_hint,
    )
    if (
        package.name != pending.plugin_name
        or package.content_hash != pending.content_hash
    ):
        raise ValueError("the stored package no longer reads as the one signed in to")
    await refuse_pending_commit(user_id, pending)
    progress = InstallProgress()
    try:
        _plugin, report = await install_plugin_package(
            user_id,
            package,
            source_type=pending.source_type,
            source_ref=pending.source_ref,
            secrets=pending.secrets,
            checked=pending.checked,
            progress=progress,
        )
    except BaseException:
        if progress.plugin is not None:
            await _take_back_after_failure(user_id, progress)
        raise
    landed = any(
        c.kind == "mcp" and c.name == pending.sign_in.server and c.status == "created"
        for c in report.components
    )
    # The callback can only say connected or not, so an install that left out
    # any component is taken back rather than reported as one that worked.
    if not landed or not report.landed_whole:
        await undo_pending_plugin_install(user_id, progress)
        raise ValueError(_not_installed(report, pending, sign_in_landed=landed))
    return CommittedPluginInstall(progress)


def _not_installed(
    report: InstallReport, pending: PendingPluginInstall, *, sign_in_landed: bool
) -> str:
    """Why the commit took its install back, naming what did not land."""
    if sign_in_landed:
        missed = report.unsettled[0]
        name, reason = missed.name or missed.key, missed.reason
    else:
        name = pending.sign_in.server
        reason = next(
            (
                c.reason
                for c in report.components
                if c.kind == "mcp" and c.key == pending.sign_in_key and c.reason
            ),
            "",
        )
    return (
        f"{name} could not be added ({reason or 'it was not created'}), so the "
        "plugin was not installed"
    )


async def _take_back_after_failure(user_id: str, progress: InstallProgress) -> None:
    """Undo an install a later step failed, without masking that failure.

    The caller re-raises the install's own exception, which is what the
    callback reports; a take-back that fails too is logged here instead.
    """
    try:
        await undo_pending_plugin_install(user_id, progress)
    except Exception:
        logger.exception(
            "[plugins] could not take back failed pending install user_id=%s name=%s",
            user_id,
            (progress.plugin or {}).get("name"),
        )


async def undo_pending_plugin_install(user_id: str, progress: InstallProgress) -> None:
    """Take back an install that wrote its row: the row, its components and
    the secrets it introduced.

    For a callback that cannot finish after the commit, for instance when
    storing the connection fails. Values the request overwrote are not
    restored; the vault does not keep the previous one.
    """
    plugin = progress.plugin
    if plugin is None:
        return
    # The plugin's claimed names, plus what it vaulted or introduced before a
    # failure could claim them.
    introduced = set(
        await list_plugin_owned_secrets(user_id, plugin["user_plugin_id"])
    )
    introduced.update(progress.introduced)
    if progress.report is not None:
        introduced.update(progress.report.secrets_created)
    await uninstall_plugin(user_id, plugin)
    await drop_secrets(user_id, sorted(introduced))
    # The install pushed them to running sandboxes, and the uninstall's version
    # bump may already have been consumed with them still in the vault.
    await after_secrets_changed(user_id, sorted(introduced))
    logger.info(
        "[plugins] took back pending install user_id=%s name=%s",
        user_id,
        plugin.get("name"),
    )
