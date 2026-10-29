"""A plugin package as a request names it: fetched or uploaded, then validated.

The install and update routes and the sign-in that installs a plugin at its
callback all read a source this way, and answer its refusals through one
mapping (``refusals_as_http``), so the same package is refused alike at every
door.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass

from fastapi import HTTPException
from pydantic import TypeAdapter

from src.server.services.mcp_validation import (
    McpCheckFailed,
    McpSignInRequired,
    check_http_error,
)
from src.server.services.plugins.errors import (
    PluginAmbiguous,
    PluginFatal,
    PluginRefused,
    PluginRejected,
)
from src.server.services.plugins.fetch import (
    ARCHIVE_SUFFIXES,
    compose_subdir_url,
    fetch_plugin_source,
    package_site,
    parse_forge_url,
)
from src.server.services.plugins.package import ValidatedPackage, validate_package
from src.server.services.plugins.pending import (
    PendingPluginInstall,
    prepare_pending_plugin_install,
)


# The ``secrets`` field of a multipart install, update or sign-in, which
# holds the JSON body's object.
SECRETS_FORM = TypeAdapter(dict[str, str])
SECRETS_REFUSAL = "secrets must be a JSON object of names to string values"


@dataclass(frozen=True)
class PackageSource:
    """A package's bytes, and what reading them again needs."""

    raw: bytes
    subdir: str | None
    name_hint: str | None
    source_type: str
    source_ref: str | None


def name_hint(source: str | None, subdir: str | None) -> str | None:
    """What to call a package that does not name itself.

    Read only when the tree holds no plugin.json and its skills get wrapped
    into one (``validate_package``), so a package that declares a name never
    sees it. The folder a link points at says the most, then the repository,
    then an upload's file name.
    """
    if subdir and subdir.strip("/"):
        return subdir.strip("/").rsplit("/", 1)[-1]
    if not source:
        return None
    if "://" in source:
        parsed = parse_forge_url(source)
        return parsed.name if parsed is not None else None
    # An upload's file name, which some browsers send with a client path.
    segments = [s for s in re.split(r"[\\/]", source) if s]
    if not segments:
        return None
    name = segments[-1]
    for suffix in (*ARCHIVE_SUFFIXES, ".git"):
        if name.lower().endswith(suffix):
            return name[: -len(suffix)] or None
    return name


async def resolve_url_source(source_url: str, subdir: str | None) -> PackageSource:
    """Fetch a URL install's package.

    The URL itself may deep-link a subdirectory (a forge tree URL or a
    ``#subdir=`` fragment); an explicit ``subdir`` (the wizard's chooser
    answer) wins over it, and is folded into the stored source_ref so update
    lands on the same plugin. Raises PluginFatal.
    """
    # Imported here: the catalog reads name_hint from this module.
    from src.server.services.plugins.marketplace import lists_source

    source_ref = (
        compose_subdir_url(source_url, subdir) if subdir is not None else source_url
    )
    raw, url_subdir = await fetch_plugin_source(
        source_url, listed=await lists_source(source_ref)
    )
    chosen = subdir if subdir is not None else url_subdir
    return PackageSource(
        raw=raw,
        subdir=chosen,
        name_hint=name_hint(source_url, chosen),
        source_type="git",
        source_ref=source_ref,
    )


async def resolve_update_source(stored: str, source_url: str | None) -> PackageSource:
    """Fetch an update's package: the source the plugin was installed from,
    or ``source_url`` when that is the same package at another revision.

    A catalog entry from another repository pins a commit, and the commit it
    moves to is known only to the catalog, so updating to it hands the
    entry's URL in. Any other repository or folder is refused before
    anything is fetched: an update replaces what the user chose to install,
    and must not swap in a different package under its name. Raises
    PluginRefused and PluginFatal.
    """
    if source_url is not None:
        site, held = package_site(source_url), package_site(stored)
        if site is None or held is None or site.place != held.place:
            raise PluginRefused(
                "plugin_source_mismatch",
                "an update can only move this plugin to another revision of "
                "the repository and folder it was installed from",
            )
    return await resolve_url_source(source_url or stored, None)


def resolve_upload_source(
    raw: bytes, filename: str | None, subdir: str | None
) -> PackageSource:
    return PackageSource(
        raw=raw,
        subdir=subdir,
        name_hint=name_hint(filename, subdir),
        source_type="zip",
        source_ref=filename,
    )


async def validate_source(
    source: PackageSource, *, expected_name: str | None = None
) -> ValidatedPackage:
    """Validate a source's package; ``expected_name`` is the plugin an update
    means. Raises PluginFatal, and PluginAmbiguous for an archive of several
    plugins that ``expected_name`` does not settle."""
    # An upload's file name is whatever the browser saved it as, so an update
    # names a synthesized manifest after the plugin it updates, which is what
    # that plugin's install named it after. A guess from "download.zip" would
    # land in the stored description, and in the tree hash as a change.
    hint = source.name_hint
    if expected_name is not None and source.source_type == "zip":
        hint = expected_name
    return await _validated(
        source.raw,
        subdir=source.subdir,
        name_hint=hint,
        expected_name=expected_name,
    )


async def _validated(
    raw: bytes,
    *,
    subdir: str | None = None,
    name_hint: str | None = None,
    expected_name: str | None = None,
) -> ValidatedPackage:
    try:
        # Unzip + validate up to 50 MiB of tree: off the event loop.
        package = await asyncio.to_thread(
            validate_package, raw, subdir=subdir, name_hint=name_hint
        )
    except PluginAmbiguous as e:
        # Update knows which plugin it wants: a unique name match settles a
        # multi-plugin archive without re-asking the user.
        if expected_name is not None:
            matches = [c for c in e.candidates if c.name == expected_name]
            if len(matches) == 1:
                return await _validated(raw, subdir=matches[0].path)
            if e.fallback_path is not None:
                # The empty-package raise, which exists to offer a person a
                # better plugin. An update has no person to offer it to and
                # already means a specific plugin, so it reconciles the one
                # it installed and lets the report say it is empty.
                return await _validated(raw, subdir=e.fallback_path)
        raise
    if (
        expected_name is not None
        and package.synthesized
        and package.name != expected_name
    ):
        # A synthesized name is a guess from the link and the skill count, not
        # a claim the package makes, so it never turns an update into a
        # different plugin.
        package = package.renamed(expected_name)
    return package


async def prepare_pending_install(
    user_id: str, source: PackageSource, secrets: Mapping[str, str] | None
) -> PendingPluginInstall:
    """What a sign-in parks to install ``source`` at its callback.

    Read and refused as the install routes do, except that one sign-in server
    is what it expects rather than a 409.
    """
    return await prepare_pending_plugin_install(
        user_id,
        source.raw,
        await validate_source(source),
        subdir=source.subdir,
        name_hint=source.name_hint,
        source_type=source.source_type,
        source_ref=source.source_ref,
        secrets=secrets,
    )


def _fatal_to_http(e: PluginFatal) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "message": str(e),
            "diagnostics": [d.model_dump() for d in e.diagnostics],
        },
    )


def _ambiguous_to_http(e: PluginAmbiguous) -> HTTPException:
    """The chooser payload: which plugins the archive holds, and where."""
    return HTTPException(
        status_code=422,
        detail={
            "message": str(e),
            "code": "multiple_plugins",
            "discovered": [
                {
                    "path": c.path,
                    "dialect": c.dialect,
                    "name": c.name,
                    "description": c.description,
                    "version": c.version,
                    # Set when the plugin lives in another repo (a
                    # marketplace external entry): install via this URL, not
                    # via subdir.
                    "source_url": c.source_url,
                }
                for c in e.candidates
            ],
        },
    )


@contextmanager
def refusals_as_http() -> Iterator[None]:
    """Map a plugin source's or write's refusals to their responses.

    The checks' exceptions are not ValueErrors on purpose, so the routes'
    ``conflict_on_value_error`` cannot turn a failed check into a 409.
    """
    try:
        yield
    except (McpCheckFailed, McpSignInRequired) as e:
        raise check_http_error(e) from e
    except PluginRefused as e:
        raise HTTPException(status_code=422, detail=e.detail()) from e
    except PluginRejected as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except PluginAmbiguous as e:
        raise _ambiguous_to_http(e) from e
    except PluginFatal as e:
        raise _fatal_to_http(e) from e
