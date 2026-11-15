"""The file routes' reads of a workspace's backup, which answers for the
machine while the workspace is stopped or its machine is away.

``files_from`` (``services/workspace_files``) decides when; these read the
stored copy, redacted as a live read is.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import Response

from src.server.services.persistence.download_link import mirror_download_link
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.served_paths import (
    is_always_hidden_path,
    is_text_content_type,
    is_utf8,
    panel_lists_stored_file,
)
from src.server.services.workspace_file_containment import (
    UTF8_SNIFF_BYTES,
    normalize_requested_path,
)
from src.server.utils.secret_redactor import (
    get_redactor,
    get_vault_secrets_for_redaction,
)

from ._shared import (
    _build_download_response,
    _range_length,
    _range_response,
    _requested_hidden_ok,
    _starts_as_text,
    _unsatisfiable,
    _virtual_file,
    http_file_byte_range,
    http_file_bytes,
    http_file_text,
)


async def backup_listing(
    workspace_id: str,
    path: str,
    include_system: bool,
    work_dir: str,
    previous_dirs: Sequence[str],
) -> list[str]:
    """The stored paths a listing of *path* shows, without the data files,
    which the route splices in for a live listing too."""
    file_tree = await FilePersistenceService.get_file_tree(workspace_id)
    # Filter by path prefix if specified
    normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
    if normalized_path:
        file_tree = [
            f
            for f in file_tree
            if f["path"].startswith(normalized_path + "/")
            or f["path"] == normalized_path
        ]
    allow_hidden = _requested_hidden_ok(path, work_dir, previous_dirs)
    return [
        f["path"]
        for f in file_tree
        if panel_lists_stored_file(
            f["path"], include_system=include_system, include_hidden=allow_hidden
        )
        # A row at a virtual path predates the backup skipping its folder;
        # a read answers from the DB, so only the splice may list one.
        and not _virtual_file(f["path"])
    ]


async def backup_paths_named(workspace_id: str, name: str) -> list[str]:
    file_tree = await FilePersistenceService.get_file_tree(workspace_id)
    return [f["path"] for f in file_tree if f["path"].rsplit("/", 1)[-1] == name]


async def read_from_backup(
    workspace_id: str,
    workspace: dict[str, Any],
    path: str,
    work_dir: str,
    previous_dirs: Sequence[str],
    offset: int,
    limit: int,
    unlimited: bool,
) -> dict[str, Any]:
    normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
    if not normalized_path:
        raise HTTPException(status_code=400, detail="File path is required")
    if is_always_hidden_path(normalized_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Parallel: fetch vault secrets + file content in one round-trip window
    vault_secrets, file_record = await asyncio.gather(
        get_vault_secrets_for_redaction(workspace["user_id"]),
        FilePersistenceService.get_file_content(workspace_id, normalized_path),
    )
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    if file_record.get("is_binary"):
        raise HTTPException(
            status_code=415,
            detail="Cannot read binary file as text. Use GET /files/download instead.",
        )

    # Never `.get("content_text", "")`: the key exists with a NULL value on
    # a blob-backed row, so the default never fires and redact() would be
    # handed None.
    text_content = await http_file_text(file_record, user_id=workspace["user_id"])
    text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)
    if unlimited:
        content = text_content
        truncated = False
    else:
        lines = text_content.splitlines()
        content = "\n".join(lines[offset : offset + limit])
        truncated = len(lines) > offset + limit
    mime = file_record.get("mime_type") or "text/plain"

    return {
        "workspace_id": workspace_id,
        "path": normalized_path,
        "offset": offset,
        "limit": limit,
        "content": content,
        "mime": mime,
        "truncated": truncated,
        "source": "database",
    }


async def _stored_range_response(
    file_record: dict[str, Any],
    byte_range: tuple[int, int],
    filename: str,
    mime: str,
    disposition: str,
    *,
    user_id: str,
) -> Response | None:
    """A range of a mirrored file, reading only that slice and the file's
    first bytes from storage.

    None for a row that goes whole: one stored inline or without a digest to
    version it, and one that may be text.
    """
    etag = file_record.get("content_hash") or file_record.get("blob_sha256")
    if not etag:
        return None
    first, last = byte_range
    length = _range_length(first, last)
    if first == 0 and length >= UTF8_SNIFF_BYTES:
        read = head_read = await http_file_byte_range(
            file_record, 0, length, user_id=user_id
        )
    else:
        head_read, read = await asyncio.gather(
            http_file_byte_range(file_record, 0, UTF8_SNIFF_BYTES, user_id=user_id),
            http_file_byte_range(file_record, first, length, user_id=user_id),
        )
    if read is None or head_read is None:
        return None
    content, size = read
    if not content:
        raise _unsatisfiable(size)
    if _starts_as_text(head_read[0][:UTF8_SNIFF_BYTES], size):
        return None
    return _range_response(content, first, size, etag, filename, mime, disposition)


async def download_from_backup(
    workspace_id: str,
    workspace: dict[str, Any],
    path: str,
    work_dir: str,
    previous_dirs: Sequence[str],
    byte_range: tuple[int, int] | None,
    request: Request,
    disposition: str,
) -> Response:
    normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
    if not normalized_path:
        raise HTTPException(status_code=400, detail="File path is required")
    if is_always_hidden_path(normalized_path):
        raise HTTPException(status_code=404, detail="File not found")

    if byte_range is None:
        # Parallel: fetch vault secrets + file content in one round-trip window
        vault_secrets, file_record = await asyncio.gather(
            get_vault_secrets_for_redaction(workspace["user_id"]),
            FilePersistenceService.get_file_content(workspace_id, normalized_path),
        )
    else:
        # A ranged answer is never redacted, so the secrets wait for the
        # row to say whether the range applies.
        file_record = await FilePersistenceService.get_file_content(
            workspace_id, normalized_path
        )
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    filename = file_record.get("file_name", "download")
    mime = file_record.get("mime_type") or "application/octet-stream"

    if byte_range is not None:
        if not is_text_content_type(mime):
            ranged = await _stored_range_response(
                file_record,
                byte_range,
                filename,
                mime,
                disposition,
                user_id=workspace["user_id"],
            )
            if ranged is not None:
                return ranged
        vault_secrets = await get_vault_secrets_for_redaction(workspace["user_id"])

    content = await http_file_bytes(file_record, user_id=workspace["user_id"])

    if is_text_content_type(mime) or is_utf8(content):
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    return _build_download_response(content, filename, mime, request, disposition)


async def backup_download_link(
    workspace: dict[str, Any], path: str, work_dir: str, previous_dirs: Sequence[str]
) -> str | None:
    normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
    if not normalized_path:
        raise HTTPException(status_code=400, detail="File path is required")
    if is_always_hidden_path(normalized_path):
        raise HTTPException(status_code=404, detail="File not found")
    return await mirror_download_link(workspace, normalized_path)
