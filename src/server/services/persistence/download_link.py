"""Signed store links that let a browser download a workspace file directly.

A file with its own object is handed out as a short-lived GET on that
object, so its bytes never pass through this process however large it is.
Everything else (pack members, inline rows, a store that cannot sign, a file
this deployment cannot export) answers None and the caller serves the bytes
itself: a stopped workspace's from its persisted copy, a running one's by
streaming it from the sandbox.
"""

import asyncio
import logging
from typing import Any

from ptc_agent.core.paths import WorkspaceLayout
from src.server.database.blob_keys import blob_key
from src.server.database.workspace_file import get_file_locator
from src.server.services.persistence.blobs import _persist_blobs
from src.server.services.persistence.transfer import hash_one_file, scan_cap_bytes
from src.server.utils.http_headers import content_disposition
from src.utils.mime import resolve_content_type
from src.utils.storage import get_signed_url, is_storage_enabled

logger = logging.getLogger(__name__)

# Long enough to survive a slow click-through, short enough that a copied
# link is not a standing share: it cannot be revoked once issued.
DOWNLOAD_LINK_TTL_S = 300
# Below this, reading a running workspace's file through the sandbox is one
# round trip; above it, exporting the file to the store first is cheaper than
# carrying its bytes through this process.
LIVE_LINK_MIN_BYTES = 8 * 1024 * 1024


async def _sign(user_id: str, sha256: str, filename: str) -> str | None:
    return await asyncio.to_thread(
        get_signed_url,
        blob_key(user_id, sha256),
        DOWNLOAD_LINK_TTL_S,
        content_disposition=content_disposition(filename, disposition="attachment"),
        content_type=resolve_content_type(filename),
        for_browser=True,
    )


async def mirror_download_link(workspace: dict[str, Any], file_path: str) -> str | None:
    """A link to a stopped workspace's file, when the manifest gives it its own object."""
    if not is_storage_enabled():
        return None
    locator = await get_file_locator(str(workspace["workspace_id"]), file_path)
    if not locator or not locator.get("blob_sha256"):
        return None
    filename = locator.get("file_name") or file_path.rsplit("/", 1)[-1]
    return await _sign(str(workspace["user_id"]), locator["blob_sha256"], filename)


async def live_download_link(
    workspace: dict[str, Any],
    sandbox: Any,
    rel_path: str,
    *,
    layout: WorkspaceLayout,
) -> str | None:
    """A link to a running workspace's file, exporting its current bytes first.

    Only on the direct path, where the sandbox pushes the file itself: a relay
    export would read it whole into this process, while the download route
    streams it flat, so relay answers None. The file is hashed where it sits,
    never trusting the manifest's digest, since a link has to serve what is on
    disk now. No manifest row is written: the object is content-addressed and
    registered, so the next sync finds it already stored. Raises
    ``FileNotFoundError`` when no regular file is there.
    """
    if not is_storage_enabled() or scan_cap_bytes(sandbox, blobs_on=True) is not None:
        return None
    workspace_id = str(workspace["workspace_id"])
    user_id = str(workspace["user_id"])
    try:
        entry = await hash_one_file(sandbox, rel_path, prior=None, layout=layout)
    except Exception as e:
        # The download route reads the file its own way, so a hash that failed
        # here is no reason to fail the save.
        logger.warning(f"Hashing {rel_path} for download raised: {e}")
        return None
    if entry is None:
        raise FileNotFoundError(rel_path)
    if entry.size <= LIVE_LINK_MIN_BYTES:
        return None
    try:
        rows, unsaved = await _persist_blobs(
            user_id, workspace_id, sandbox, [entry], layout=layout
        )
        reason = (unsaved[0].reason if unsaved else "failed") if not rows else None
    except Exception as e:
        rows, reason = [], "failed"
        logger.warning(f"Exporting {rel_path} for download raised: {e}")
    url = (
        await _sign(user_id, entry.sha256 or "", rel_path.rsplit("/", 1)[-1])
        if rows
        else None
    )
    if url is not None:
        return url
    logger.warning(
        f"Could not export {rel_path} ({entry.size} bytes) from workspace "
        f"{workspace_id} for download: {reason or 'failed'}"
    )
    return None
