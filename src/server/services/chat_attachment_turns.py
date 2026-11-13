"""A turn's attachments, wherever their bytes are: inline in the request, or
uploaded beforehand and named by id (``services/chat_attachments``).

The request resolves once, early in the turn and for the turn's own user,
into :class:`TurnAttachments`, and the turn then sees every file alike. An
uploaded file reaches the workspace from where it waits, by the sandbox
fetching it from the store when it can, so the bytes never pass through this
process; only a file the model takes as a content block, or one the sandbox
cannot fetch, is read here.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from ptc_agent.core.project_context import ProjectContext, current_project
from src.server.database import chat_attachments as db
from src.server.database.blob_keys import RELAY_MAX_BYTES
from src.server.database.chat_attachments import ChatAttachmentRow
from src.server.database.workspace_file import WorkspaceSyncBusy, workspace_sync_lock
from src.server.models.chat_attachment import (
    MissingAttachment,
    StoredSource,
    TurnAttachment,
    TurnAttachments,
)
from src.server.services.chat_attachments import computer_takes_large
from src.server.services.persistence.transfer import (
    held_bytes_budget,
    pull_direct,
    sandbox_pulls_from_store,
    transfer_timeout_s,
)
from src.server.utils.error_sanitization import single_line
from src.server.utils.multimodal_context import (
    inline_attachment,
    parse_multimodal_contexts,
    upload_rel_path,
)
from src.utils.storage import get_bytes, get_signed_url

logger = logging.getLogger(__name__)

_MB = 1024 * 1024

# The largest uploaded file this process copies into a sandbox that cannot
# fetch from the store itself: the relay's own bound, which is server memory.
WORKER_COPY_MAX_BYTES = RELAY_MAX_BYTES
# A backup or restore holds the folder's sync lock, and the pull sweeps the
# staging names they use; a turn waits this long for it, then copies the bytes.
_PULL_LOCK_WAIT = "15s"

# Uploaded files one message may name. Each is read, kept and copied by the
# turn, so the count is what bounds a turn's memory: unlike inline data, an id
# costs the request nothing.
MAX_UPLOADED_PER_TURN = 10

_GONE = "it is no longer available: it expired or was removed before this message was sent"
_UNDECODABLE = "it did not arrive intact"
_OVER_CAP = f"a message carries at most {MAX_UPLOADED_PER_TURN} uploaded files"
_TOO_LARGE = (
    f"it is larger than this workspace's computer can receive "
    f"(up to {WORKER_COPY_MAX_BYTES // (1024 * 1024)} MB)"
)


# --- resolving -----------------------------------------------------------------


async def _load_bytes(row: ChatAttachmentRow) -> bytes | None:
    if row["has_content"]:
        return await db.get_content(row["attachment_id"])
    if row["object_key"]:
        return await asyncio.to_thread(get_bytes, row["object_key"])
    return None


def _from_row(row: ChatAttachmentRow, description: Optional[str]) -> TurnAttachment:
    async def load() -> bytes | None:
        return await _load_bytes(row)

    return TurnAttachment(
        name=description or row["name"],
        mime_type=row["mime_type"],
        size=row["size"],
        source=StoredSource(row["attachment_id"], row["object_key"], load=load),
    )


async def resolve_turn_attachments(
    additional_context: Optional[list[Any]], user_id: str
) -> TurnAttachments:
    """The request's attachments, uploaded ones resolved for ``user_id``.
    They are stamped as sent as the run is admitted (:func:`mark_sent`).

    An id that is unknown, another user's, or not ready, and inline data that
    does not decode, is missing rather than refusing the message: the rest of
    the turn still stands, and the agent tells the user which file it could
    not see. An id named again is the same file, taken once.
    """
    contexts = parse_multimodal_contexts(additional_context)
    ids = list(dict.fromkeys(str(ctx.attachment_id) for ctx in contexts if ctx.attachment_id))
    taken = set(ids[:MAX_UPLOADED_PER_TURN])
    rows = {r["attachment_id"]: r for r in await db.find_for_turn(list(taken), user_id)} if taken else {}
    seen: set[str] = set()
    over_cap = 0
    files: list[TurnAttachment] = []
    missing: list[MissingAttachment] = []
    for ctx in contexts:
        name = ctx.description or "file"
        if ctx.attachment_id is None:
            att = inline_attachment(ctx)
            if att is None:
                missing.append(MissingAttachment(name, ctx.type, _UNDECODABLE))
            else:
                files.append(att)
            continue
        attachment_id = str(ctx.attachment_id)
        if attachment_id in seen:
            continue
        seen.add(attachment_id)
        if attachment_id not in taken:
            over_cap += 1
            continue
        row = rows.get(attachment_id)
        # Without storage, a sent file's bytes leave Postgres a day on.
        if row is None or not (row["has_content"] or row["object_key"]):
            missing.append(MissingAttachment(name, ctx.type, _GONE))
        else:
            files.append(_from_row(row, ctx.description))
    if over_cap:
        # One note for the lot: the request sets how many there are.
        missing.append(MissingAttachment(f"{over_cap} more", "file", _OVER_CAP))
    if missing:
        logger.info(f"[PTC_CHAT] {len(missing)} attachment(s) not received")
    return TurnAttachments(files, missing)


async def refuse_too_large(
    attachments: TurnAttachments, user_id: str, workspace_id: str | None
) -> None:
    """Move to missing each uploaded file larger than the server copies when
    the turn's computer cannot fetch it from the store, before history records
    the turn: a composer may have staged it for another workspace, or before
    it knew which."""
    if not workspace_id:
        return
    large = [
        a
        for a in attachments.files
        if isinstance(a.source, StoredSource) and a.size > WORKER_COPY_MAX_BYTES
    ]
    if not large or await computer_takes_large(user_id, workspace_id):
        return
    for att in large:
        attachments.files.remove(att)
        attachments.missing.append(MissingAttachment(att.name, att.kind, _TOO_LARGE))


async def mark_sent(attachments: TurnAttachments, conn: Any) -> bool:
    """Stamp the turn's uploaded files as sent, in its run's START transaction.

    Not at resolution: a send that fails before its run exists leaves them
    unsent, so a discard or the sweep still removes them. Not after START
    either: a worker lost between the two would leave an admitted message
    naming files the sweep then removes. One discarded in between is no
    longer there to place, and is said so like any other. Whether any was.
    """
    stored = [a for a in attachments.files if isinstance(a.source, StoredSource)]
    if not stored:
        return False
    kept = await db.mark_sent([a.source.attachment_id for a in stored], conn)
    gone = [a for a in stored if a.source.attachment_id not in kept]
    for att in gone:
        attachments.files.remove(att)
        attachments.missing.append(MissingAttachment(att.name, att.kind, _GONE))
    return bool(gone)


async def read_for_model(att: TurnAttachment) -> bytes | None:
    """``att``'s bytes, kept for the turn's later reads; None when they cannot
    be read, which sends the file in by path."""
    try:
        if isinstance(att.source, StoredSource):
            async with held_bytes_budget().hold(att.size):
                return await att.read(keep=True)
        return await att.read(keep=True)
    except Exception as e:
        logger.warning(f"Reading an attachment for the model failed: {single_line(str(e))}")
        return None


# --- saving --------------------------------------------------------------------


async def save_attachments(
    files: list[TurnAttachment], sandbox: Any, *, project: ProjectContext | None
) -> None:
    """Save each file under the workspace's ``work/uploads`` and set its
    ``path``; one that could not be saved keeps none."""
    await pull_attachments(files, sandbox, project=project)
    await write_unsaved(files, sandbox, project=project)


async def pull_attachments(
    files: list[TurnAttachment], sandbox: Any, *, project: ProjectContext | None
) -> None:
    """Have the sandbox fetch the uploaded files it can from the store, in one
    pull; each placed gets its ``path``."""
    ctx = project if project is not None else current_project()
    fetchable = [
        a for a in files if isinstance(a.source, StoredSource) and a.source.object_key
    ]
    if fetchable and ctx is not None and sandbox_pulls_from_store(sandbox):
        await _pull(fetchable, sandbox, ctx)


async def write_unsaved(
    files: list[TurnAttachment], sandbox: Any, *, project: ProjectContext | None
) -> None:
    """Write from here every file the pull did not place."""
    ctx = project if project is not None else current_project()
    await asyncio.gather(*(_write(a, sandbox, ctx) for a in files if a.path is None))


async def _pull(files: list[TurnAttachment], sandbox: Any, ctx: ProjectContext) -> None:
    """Have the sandbox fetch the objects into place, as an upload into the
    folder is pulled (``workspace_upload_pull._pull_once``).

    One pull under one hold of the folder's sync lock: pulled apart, each
    would wait out the others' holds, and one waiting past the lock's wait
    would fall back to a copy through this process.
    """
    layout = sandbox.workspace(ctx)
    root = layout.workspace.rstrip("/")
    expires_in = transfer_timeout_s(sum(a.size for a in files)) + 300

    async def plan(att: TurnAttachment) -> tuple[TurnAttachment, str, dict[str, Any]] | None:
        assert isinstance(att.source, StoredSource) and att.source.object_key
        rel_path = upload_rel_path(att.name, att.mime_type)
        target = sandbox.normalize_path(rel_path, project=ctx)
        if not target.startswith(f"{root}/"):
            return None
        url = await asyncio.to_thread(get_signed_url, att.source.object_key, expires_in)
        if url is None:
            return None
        item = {
            "kind": "file",
            "path": target[len(root) + 1 :],
            "url": url,
            "size": att.size,
            "mode": 0o644,
        }
        return att, rel_path, item

    planned = [p for p in await asyncio.gather(*(plan(a) for a in files)) if p is not None]
    if not planned:
        return
    try:
        # The pull clears transfer leftovers at the folder's root, which a
        # backup or restore running beside it would still be using.
        async with workspace_sync_lock(ctx.workspace_id, wait=_PULL_LOCK_WAIT):
            results = await pull_direct(sandbox, [item for _, _, item in planned], layout=layout)
    except WorkspaceSyncBusy:
        logger.info(f"Attachment pull into {ctx.workspace_id} waited out a busy folder")
        return
    except Exception as e:
        logger.warning(f"Attachment pull failed: {type(e).__name__}: {single_line(str(e))}")
        return
    for att, rel_path, item in planned:
        result = results.get(item["path"]) or {}
        if result.get("status") == "ok":
            att.path = rel_path
        else:
            assert isinstance(att.source, StoredSource)
            logger.warning(
                f"Attachment {att.source.attachment_id} pull {result.get('status')}: "
                f"{result.get('error')}"
            )


async def _write(att: TurnAttachment, sandbox: Any, ctx: ProjectContext | None) -> None:
    rel_path = upload_rel_path(att.name, att.mime_type)
    try:
        abs_path = sandbox.normalize_path(rel_path, project=ctx)
        if isinstance(att.source, StoredSource):
            if att.size > WORKER_COPY_MAX_BYTES:
                logger.warning(
                    f"Attachment {att.source.attachment_id} is too large to copy "
                    f"through the server ({att.size // _MB} MB)"
                )
                return
            async with held_bytes_budget().hold(att.size):
                written = await sandbox.aupload_file_bytes(abs_path, await att.read())
        else:
            written = await sandbox.aupload_file_bytes(abs_path, await att.read())
    except Exception:
        logger.warning(f"Failed to save attachment '{att.name}' in the sandbox", exc_info=True)
        return
    if written:
        att.path = rel_path
    else:
        logger.warning(f"Saving attachment '{att.name}' in the sandbox returned False")
