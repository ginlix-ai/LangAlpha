"""User-managed memo API.

The user uploads markdown and PDF documents here. Each memo lands in the
LangGraph store under ``(user_id, "memos")`` keyed by a slugified filename.
PDFs are text-extracted server-side so the agent's `read_file` returns
readable text; the original bytes live in object storage (when configured)
or base64 inside the value.

Why ``"memos"`` (plural) and not ``"memo"``: ``AsyncPostgresStore`` does a
*string* prefix match (``WHERE prefix LIKE 'user.memo%'``) on the
period-joined namespace, so ``(user, "memo")`` would also match every row
under ``(user, "memory")``. The plural avoids that collision.

The agent has **read-only** access via the filesystem composite route added
in Step 2 — the routes live in the agent's `CompositeFilesystemBackend`;
this router is the write path.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
import re
from datetime import UTC, datetime
from hashlib import sha256
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
)
from pydantic import BaseModel

from ptc_agent.agent.backends import lock_for_namespace
from ptc_agent.agent.memo import (
    ACCEPTED_MIME_TYPES,
    MAX_COLLISION_SUFFIX,
    MEMO_MAX_CONTENT_BYTES,
    MEMO_MAX_UPLOAD_BYTES,
    METADATA_PLACEHOLDER_DESCRIPTION,
    MemoPdfExtractionError,
    candidate_slug,
    extract_pdf_text,
    random_collision_slug,
    slug_components,
)
from ptc_agent.agent.memo.content_types import is_pdf, resolve_mime_type
from ptc_agent.agent.memo.index import rebuild_memo_index
from ptc_agent.agent.memo.metadata import generate_memo_metadata
from ptc_agent.core.paths import MEMO_INDEX_FILENAME
from src.server.app import setup
from src.server.app._store_helpers import (
    MAX_LIST_LIMIT,
    adelete,
    aget,
    aput,
    asearch,
    coerce_str,
    paginate_namespace,
    require_store,
    validate_key,
)
from src.server.services import memo_binary_storage
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.utils.api import CurrentUserId, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/memo", tags=["Memo"])

_INDEX_KEY = MEMO_INDEX_FILENAME

# Per-key registry of in-flight metadata tasks so a delete or replace can
# cancel the prior task before issuing the new write. Without this, an LLM
# call that resolves AFTER the user deleted the row would resurrect it via
# the post-LLM ``_merge_metadata`` aput, or stamp stale description/summary
# onto newer content if the user re-uploaded mid-flight. Keyed by
# ``(namespace, key)`` so different memos do not interfere.
_TaskKey = tuple[tuple[str, ...], str]
_METADATA_TASKS: dict[_TaskKey, asyncio.Task[Any]] = {}


def _cancel_pending_metadata(namespace: tuple[str, ...], key: str) -> None:
    """Cancel any in-flight metadata task for this key (best effort)."""
    task = _METADATA_TASKS.pop((namespace, key), None)
    if task is not None and not task.done():
        task.cancel()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _namespace(user_id: str) -> tuple[str, ...]:
    return (user_id, "memos")


# Cap the sandbox-source path field before it lands in a memo row so a
# malicious 1 MB form value can't bloat every list/read response.
_SOURCE_PATH_MAX_CHARS: int = 1024
_UPLOAD_READ_CHUNK: int = 64 * 1024


async def _read_capped(file: UploadFile, max_bytes: int) -> bytes:
    """Read an uploaded file in chunks, raising 413 once the cap is exceeded.

    ``await file.read()`` would buffer the entire body to a SpooledTemporaryFile
    (1 MB threshold then disk) before any size check fires, turning a 100 MB
    adversarial upload into a disk-full DoS amplifier. Reading in chunks lets
    us reject early.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"File too large (>{max_bytes} bytes). Aborting before "
                    f"the full body is buffered."
                ),
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _reject_reserved_key(key: str) -> None:
    """Refuse user writes against the server-maintained catalog key.

    `memo.md` is rebuilt deterministically by `rebuild_memo_index` from the
    other rows in the namespace, so any user-driven write would be transient
    at best and corrupt the catalog at worst.
    """
    if key == _INDEX_KEY:
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{_INDEX_KEY}' is reserved for the memo catalog and cannot "
                "be written, deleted, or regenerated directly."
            ),
        )


# --- Response models -------------------------------------------------------


class MemoEntry(BaseModel):
    key: str
    original_filename: str | None = None
    mime_type: str | None = None
    size_bytes: int = 0
    description: str | None = None
    metadata_status: str | None = None
    created_at: str | None = None
    modified_at: str | None = None
    source_kind: str | None = None
    source_workspace_id: str | None = None
    source_path: str | None = None
    sha256: str | None = None


class MemoListResponse(BaseModel):
    entries: list[MemoEntry]
    truncated: bool = False


class MemoReadResponse(BaseModel):
    key: str
    original_filename: str | None = None
    mime_type: str | None = None
    content: str
    encoding: str
    description: str | None = None
    summary: str | None = None
    metadata_status: str | None = None
    metadata_error: str | None = None
    size_bytes: int = 0
    created_at: str | None = None
    modified_at: str | None = None
    source_kind: str | None = None
    source_workspace_id: str | None = None
    source_path: str | None = None


class MemoUploadResponse(BaseModel):
    key: str
    original_filename: str
    metadata_status: str
    replaced: bool = False


class MemoWriteRequest(BaseModel):
    key: str
    content: str


# --- Value mappers ---------------------------------------------------------


def _value_to_entry(key: str, value: Any) -> MemoEntry:
    if not isinstance(value, dict):
        return MemoEntry(key=key)
    return MemoEntry(
        key=key,
        original_filename=coerce_str(value.get("original_filename")) or None,
        mime_type=coerce_str(value.get("mime_type")) or None,
        size_bytes=int(value.get("size_bytes") or 0),
        description=coerce_str(value.get("description")) or None,
        metadata_status=coerce_str(value.get("metadata_status")) or None,
        created_at=coerce_str(value.get("created_at")) or None,
        modified_at=coerce_str(value.get("modified_at")) or None,
        source_kind=coerce_str(value.get("source_kind")) or None,
        source_workspace_id=coerce_str(value.get("source_workspace_id")) or None,
        source_path=coerce_str(value.get("source_path")) or None,
        sha256=coerce_str(value.get("sha256")) or None,
    )


def _value_to_read(key: str, value: Any) -> MemoReadResponse:
    if not isinstance(value, dict):
        logger.warning("memo entry has non-dict value", extra={"key": key})
        return MemoReadResponse(key=key, content="", encoding="utf-8")
    return MemoReadResponse(
        key=key,
        original_filename=coerce_str(value.get("original_filename")) or None,
        mime_type=coerce_str(value.get("mime_type")) or None,
        content=coerce_str(value.get("content")),
        encoding=coerce_str(value.get("encoding"), "utf-8") or "utf-8",
        description=coerce_str(value.get("description")) or None,
        summary=coerce_str(value.get("summary")) or None,
        metadata_status=coerce_str(value.get("metadata_status")) or None,
        metadata_error=coerce_str(value.get("metadata_error")) or None,
        size_bytes=int(value.get("size_bytes") or 0),
        created_at=coerce_str(value.get("created_at")) or None,
        modified_at=coerce_str(value.get("modified_at")) or None,
        source_kind=coerce_str(value.get("source_kind")) or None,
        source_workspace_id=coerce_str(value.get("source_workspace_id")) or None,
        source_path=coerce_str(value.get("source_path")) or None,
    )


# --- Helpers ---------------------------------------------------------------


async def _find_by_source(
    store: Any,
    namespace: tuple[str, ...],
    *,
    workspace_id: str,
    path: str,
) -> tuple[str, dict[str, Any]] | None:
    """Return the (key, value) of an existing memo whose source matches.

    Walks the namespace pages until a match is found. Hard-capped at
    ``MAX_LIST_LIMIT`` total rows so this runs in bounded time on the
    namespace-lock path even for users with thousands of memos. Beyond
    the cap, the next "Add to memo" allocates a fresh slug instead of
    deduping; eventual cleanup is acceptable for that edge.
    """
    page = 100
    offset = 0
    while offset < MAX_LIST_LIMIT:
        limit = min(page, MAX_LIST_LIMIT - offset)
        results = await asearch(store, namespace, limit=limit, offset=offset)
        if not results:
            return None
        for item in results:
            value = item.value
            if not isinstance(value, dict):
                continue
            if (
                value.get("source_kind") == "sandbox"
                and value.get("source_workspace_id") == workspace_id
                and value.get("source_path") == path
            ):
                return item.key, value
        if len(results) < limit:
            return None
        offset += limit
    return None


async def _resolve_unique_slug(
    store: Any, namespace: tuple[str, ...], original_filename: str
) -> str:
    """Pick a slug that is free in ``namespace`` using O(1) targeted lookups.

    Trades the previous full-namespace ``asearch`` (which serializes every
    row's value, up to MB per memo for PDFs) for a small handful of indexed
    primary-key probes. AsyncPostgresStore.aget is a single-row lookup; the
    postgres-side cost is microseconds regardless of memo count. After the
    linear cap (``MAX_COLLISION_SUFFIX``) we switch to random hex suffixes so
    the worst-case lock-hold stays bounded under pathological inputs.
    """
    base, suffix = slug_components(original_filename)
    for n in range(1, MAX_COLLISION_SUFFIX + 1):
        candidate = candidate_slug(base, suffix, n)
        item = await aget(store, namespace, candidate)
        if item is None:
            return candidate
    # Linear cap exhausted. Try a few random suffixes; collision odds with a
    # 2^16 bucket space are negligible until the namespace is densely
    # populated, so this almost always succeeds on the first probe.
    logger.warning(
        "memo: slug collision exhausted MAX_COLLISION_SUFFIX, falling back to random suffix",
        extra={"namespace": namespace, "base": base, "suffix": suffix},
    )
    for _ in range(8):
        candidate = random_collision_slug(base, suffix)
        item = await aget(store, namespace, candidate)
        if item is None:
            return candidate
    # Truly adversarial — return a fresh random slug and let the downstream
    # aput overwrite if it still collides.
    return random_collision_slug(base, suffix)


def _kickoff_metadata(
    *, user_id: str, namespace: tuple[str, ...], key: str
) -> bool:
    """Dispatch a background LLM call. Requires setup.llm_service to be wired.

    Cancels any in-flight task for the same ``(namespace, key)`` before
    scheduling — a fresh upload/write/regen supersedes a prior pending
    metadata generation, so we don't want a stale LLM result writing the
    older description over the new content.

    Returns ``True`` when a task was scheduled. Callers use the return value
    to decide whether to do their own ``rebuild_memo_index`` — when the
    metadata task is going to call rebuild after the LLM resolves, the
    caller's placeholder rebuild is redundant. When ``False`` (no LLM
    service configured), the caller MUST rebuild itself.
    """
    llm_service = getattr(setup, "llm_service", None)
    if llm_service is None:
        logger.warning(
            "memo: llm_service not configured; skipping metadata generation",
            extra={"key": key},
        )
        return False
    _cancel_pending_metadata(namespace, key)
    task = asyncio.create_task(
        generate_memo_metadata(
            store=setup.store,
            namespace=namespace,
            key=key,
            user_id=user_id,
            llm_service=llm_service,
        ),
        name=f"memo-metadata-{key}",
    )
    _METADATA_TASKS[(namespace, key)] = task

    def _cleanup(_t: asyncio.Task[Any]) -> None:
        # Only drop the entry if this is still the registered task — a newer
        # _kickoff_metadata may have replaced it under us.
        current = _METADATA_TASKS.get((namespace, key))
        if current is _t:
            _METADATA_TASKS.pop((namespace, key), None)

    task.add_done_callback(_cleanup)
    return True


# --- Endpoints -------------------------------------------------------------


@router.post(
    "/user/upload",
    response_model=MemoUploadResponse,
    status_code=202,
)
async def upload_user_memo(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
    source_kind: Annotated[str | None, Form()] = None,
    source_workspace_id: Annotated[str | None, Form()] = None,
    source_path: Annotated[str | None, Form()] = None,
) -> MemoUploadResponse:
    """Upload a markdown or PDF memo.

    When ``source_kind="sandbox"`` is supplied with ``source_workspace_id``
    and ``source_path``, an existing memo with the same triple is replaced
    in place (preserving its slug) instead of allocating a new one. The
    response sets ``replaced=True`` so the client can adapt its toast.

    Returns 202 immediately; description/summary generation happens in the
    background via ``LLMService.complete`` + ``rebuild_memo_index``.
    """
    store = require_store(setup.store)

    mime_type = resolve_mime_type(file.content_type, file.filename)
    if mime_type not in ACCEPTED_MIME_TYPES:
        raise HTTPException(
            status_code=415,
            detail=(
                f"Unsupported file type '{file.content_type or '<empty>'}'. "
                f"Accepted: {', '.join(sorted(ACCEPTED_MIME_TYPES))}."
            ),
        )

    if source_kind not in (None, "upload", "sandbox"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported source_kind '{source_kind}'.",
        )

    if source_path is not None and len(source_path) > _SOURCE_PATH_MAX_CHARS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"source_path is too long ({len(source_path)} chars); "
                f"max is {_SOURCE_PATH_MAX_CHARS}."
            ),
        )

    # Sandbox source must reference a workspace the caller actually owns.
    # Without this the dedup row records a workspace_id the user doesn't
    # control, and the UI's provenance subline misattributes the memo.
    if source_kind == "sandbox" and source_workspace_id:
        workspace = await db_get_workspace(source_workspace_id)
        require_workspace_owner(workspace, user_id=user_id)

    # Reject oversized uploads as soon as we cross the limit so a 100MB
    # adversarial body never gets fully buffered to disk.
    raw = await _read_capped(file, MEMO_MAX_UPLOAD_BYTES)

    # Text extraction -----------------------------------------------------
    if is_pdf(mime_type):
        try:
            content = await extract_pdf_text(raw)
        except MemoPdfExtractionError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    else:
        try:
            content = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Could not decode file as UTF-8. "
                    "Only UTF-8 text files are accepted."
                ),
            ) from exc

    content_bytes = len(content.encode("utf-8"))
    if content_bytes > MEMO_MAX_CONTENT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Extracted content is {content_bytes} bytes; max is "
                f"{MEMO_MAX_CONTENT_BYTES}. Shorten the document."
            ),
        )

    namespace = _namespace(user_id)
    original_filename = file.filename or "memo"

    # Slug allocation + dedup + aput run under the namespace lock so two
    # concurrent uploads with the same filename or same sandbox source can't
    # both observe an empty slot and both write — the prior code raced both
    # ``_find_by_source`` and ``_resolve_unique_slug``'s aget probes.
    async with lock_for_namespace(namespace):
        # Dedup-by-source: re-uploading the same sandbox file replaces the
        # existing entry instead of growing a new slug suffix.
        is_sandbox_source = (
            source_kind == "sandbox"
            and source_workspace_id
            and source_path
        )
        existing: tuple[str, dict[str, Any]] | None = None
        if is_sandbox_source:
            existing = await _find_by_source(
                store,
                namespace,
                workspace_id=source_workspace_id,
                path=source_path,
            )

        if existing is not None:
            key, prev_value = existing
            replaced = True
            # Preserve created_at across replacements; only modified_at advances.
            prior_binary_ref = prev_value.get("binary_ref")
            created_at = prev_value.get("created_at") or _now_iso()
        else:
            key = await _resolve_unique_slug(
                store, namespace, original_filename,
            )
            replaced = False
            prior_binary_ref = None
            created_at = _now_iso()

        # Final safety: the slug must pass the store's own validator.
        validate_key(key)
        if key == _INDEX_KEY:
            # Catalog file is server-maintained; refuse uploads that would clobber it.
            raise HTTPException(
                status_code=400,
                detail=(
                    f"'{_INDEX_KEY}' is a reserved key for the memo catalog. "
                    "Rename the file before uploading."
                ),
            )

        # Binary stash (PDF only) ----------------------------------------
        # We upload the original bytes to object storage BEFORE the row aput
        # so the row never points at a missing object. If the row aput then
        # fails, we revert the upload to avoid orphan bytes in the bucket.
        binary_ref: dict[str, Any] | None = None
        original_bytes_b64: str | None = None
        if is_pdf(mime_type):
            if memo_binary_storage.is_configured():
                try:
                    binary_ref = await memo_binary_storage.store_binary(
                        user_id=user_id,
                        key=key,
                        content=raw,
                        content_type=mime_type,
                    )
                except memo_binary_storage.MemoBinaryUploadError as exc:
                    logger.exception(
                        "memo binary upload failed",
                        extra={"user_id": user_id, "key": key},
                    )
                    raise HTTPException(
                        status_code=500,
                        detail="Could not store the original file — please retry.",
                    ) from exc
            else:
                original_bytes_b64 = base64.b64encode(raw).decode("ascii")

        # Build value and persist ----------------------------------------
        now = _now_iso()
        value: dict[str, Any] = {
            "content": content,
            "encoding": "utf-8",
            "mime_type": mime_type,
            "original_filename": original_filename,
            "key": key,
            "size_bytes": content_bytes,
            "sha256": sha256(content.encode("utf-8")).hexdigest(),
            "description": METADATA_PLACEHOLDER_DESCRIPTION,
            "summary": "",
            "metadata_status": "pending",
            "metadata_error": None,
            "binary_ref": binary_ref,
            "original_bytes_b64": original_bytes_b64,
            "created_at": created_at,
            "modified_at": now,
            "metadata_generated_at": None,
            "source_kind": source_kind,
            "source_workspace_id": source_workspace_id,
            "source_path": source_path,
        }
        try:
            await aput(store, namespace, key, value)
        except Exception:
            # Roll back the just-uploaded binary so we don't leave an orphan
            # blob in R2/S3 with no row referencing it. delete_binary is
            # best-effort; failing to revert just leaves the orphan, which
            # is the same end-state we'd have without this rollback.
            if binary_ref is not None:
                with contextlib.suppress(Exception):
                    await memo_binary_storage.delete_binary(binary_ref)
            raise

    # Replace-PDF-with-text: drop the orphaned object AFTER the row aput
    # succeeds. Doing this before would leave a row referencing a deleted
    # binary if aput later failed.
    if replaced and prior_binary_ref and not is_pdf(mime_type):
        if memo_binary_storage.is_configured():
            try:
                await memo_binary_storage.delete_binary(prior_binary_ref)
            except Exception:
                logger.exception(
                    "memo binary delete failed during replace",
                    extra={"user_id": user_id, "key": key},
                )

    # When metadata generation is dispatched, the background task rebuilds
    # the index after the LLM resolves — doing it here too writes memo.md
    # twice for every upload. Only rebuild eagerly when no LLM service is
    # wired (dev mode) so the catalog still updates.
    metadata_dispatched = _kickoff_metadata(
        user_id=user_id, namespace=namespace, key=key
    )
    if not metadata_dispatched:
        await rebuild_memo_index(store, namespace)

    return MemoUploadResponse(
        key=key,
        original_filename=original_filename,
        metadata_status="pending",
        replaced=replaced,
    )


@router.put("/user/write", response_model=MemoUploadResponse)
async def write_user_memo(
    user_id: CurrentUserId,
    body: MemoWriteRequest,
) -> MemoUploadResponse:
    """Overwrite the text content of an existing memo.

    Hash short-circuit: identical content → no LLM call, no index rebuild.
    Binary-backed memos (PDFs) are not editable via this endpoint; re-upload
    via multipart instead.
    """
    store = require_store(setup.store)
    validate_key(body.key)
    _reject_reserved_key(body.key)

    namespace = _namespace(user_id)
    content_bytes = len(body.content.encode("utf-8"))
    if content_bytes > MEMO_MAX_CONTENT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Content is {content_bytes} bytes; max is "
                f"{MEMO_MAX_CONTENT_BYTES}."
            ),
        )

    # Hold the namespace lock across the read-modify-write so a concurrent
    # upload (which also enters this lock) can't interleave its replace and
    # leave the row in a half-applied state.
    async with lock_for_namespace(namespace):
        item = await aget(store, namespace, body.key)
        if item is None:
            raise HTTPException(status_code=404, detail="Memo not found")
        if not isinstance(item.value, dict):
            raise HTTPException(status_code=500, detail="Malformed memo value")

        existing_value = item.value
        if existing_value.get("binary_ref") or existing_value.get("original_bytes_b64"):
            raise HTTPException(
                status_code=400,
                detail=(
                    "This memo is backed by an original binary (e.g. a PDF). "
                    "Re-upload the file to change its content."
                ),
            )

        new_hash = sha256(body.content.encode("utf-8")).hexdigest()
        if new_hash == existing_value.get("sha256"):
            # No-op — content unchanged. Don't rebuild, don't regenerate.
            return MemoUploadResponse(
                key=body.key,
                original_filename=existing_value.get("original_filename") or body.key,
                metadata_status=existing_value.get("metadata_status") or "ready",
            )

        now = _now_iso()
        updated = {
            **existing_value,
            "content": body.content,
            "sha256": new_hash,
            "size_bytes": content_bytes,
            "modified_at": now,
            "metadata_status": "pending",
            "metadata_error": None,
            "description": METADATA_PLACEHOLDER_DESCRIPTION,
        }
        await aput(store, namespace, body.key, updated)

    metadata_dispatched = _kickoff_metadata(
        user_id=user_id, namespace=namespace, key=body.key
    )
    if not metadata_dispatched:
        await rebuild_memo_index(store, namespace)

    return MemoUploadResponse(
        key=body.key,
        original_filename=updated.get("original_filename") or body.key,
        metadata_status="pending",
    )


@router.get("/user", response_model=MemoListResponse)
async def list_user_memos(user_id: CurrentUserId) -> MemoListResponse:
    """List all memos for the caller. Excludes memo.md itself."""
    store = require_store(setup.store)
    namespace = _namespace(user_id)
    raw_entries, truncated = await paginate_namespace(
        store, namespace, _value_to_entry
    )
    entries = [entry for entry in raw_entries if entry.key != _INDEX_KEY]
    return MemoListResponse(entries=entries, truncated=truncated)


@router.get("/user/read", response_model=MemoReadResponse)
async def read_user_memo(
    user_id: CurrentUserId,
    key: str = Query(..., description="Memo key (slug) relative to .agents/user/memo/"),
) -> MemoReadResponse:
    """Read one memo's text content and metadata. Never returns binary blob."""
    validate_key(key)
    store = require_store(setup.store)
    item = await aget(store, _namespace(user_id), key)
    if item is None:
        raise HTTPException(status_code=404, detail="Memo not found")
    return _value_to_read(key, item.value)


@router.get("/user/download")
async def download_user_memo(
    user_id: CurrentUserId,
    key: str = Query(..., description="Memo key (slug) to download"),
) -> Response:
    """Stream the original file bytes (PDF) — or the text content if non-binary."""
    validate_key(key)
    store = require_store(setup.store)
    item = await aget(store, _namespace(user_id), key)
    if item is None:
        raise HTTPException(status_code=404, detail="Memo not found")
    value = item.value if isinstance(item.value, dict) else {}

    filename = value.get("original_filename") or key
    mime_type = value.get("mime_type") or "application/octet-stream"
    headers = {
        "Content-Disposition": _content_disposition(filename),
    }

    binary_ref = value.get("binary_ref")
    if isinstance(binary_ref, dict):
        try:
            data = await memo_binary_storage.fetch_binary(binary_ref)
        except memo_binary_storage.MemoBinaryFetchError as exc:
            logger.exception(
                "memo binary fetch failed",
                extra={"user_id": user_id, "key": key},
            )
            raise HTTPException(
                status_code=502,
                detail="Could not retrieve the original file — please retry.",
            ) from exc
        return Response(content=data, media_type=mime_type, headers=headers)

    b64 = value.get("original_bytes_b64")
    if isinstance(b64, str) and b64:
        try:
            data = base64.b64decode(b64)
        except (binascii.Error, ValueError) as exc:
            logger.exception(
                "memo b64 decode failed",
                extra={"user_id": user_id, "key": key},
            )
            raise HTTPException(
                status_code=500,
                detail="Could not decode the original file — please retry.",
            ) from exc
        return Response(content=data, media_type=mime_type, headers=headers)

    # Text memo — return content as-is.
    content = coerce_str(value.get("content"))
    return Response(
        content=content.encode("utf-8"),
        media_type=mime_type or "text/markdown",
        headers=headers,
    )


@router.delete("/user")
async def delete_user_memo(
    user_id: CurrentUserId,
    key: str = Query(..., description="Memo key (slug) to delete"),
) -> dict[str, str]:
    """Delete a memo and rebuild memo.md."""
    validate_key(key)
    _reject_reserved_key(key)
    store = require_store(setup.store)
    namespace = _namespace(user_id)

    # Pair the lock with upload's _find_by_source → aput window. Without it,
    # a sandbox-source upload that already saw key=foo can aput a fresh value
    # after our adelete completes — silently resurrecting a row the user
    # asked us to remove. The S3 cleanup and index rebuild stay outside the
    # lock since they're best-effort and don't need to serialize with writes.
    async with lock_for_namespace(namespace):
        item = await aget(store, namespace, key)
        if item is None:
            raise HTTPException(status_code=404, detail="Memo not found")

        # Cancel any in-flight metadata task before deleting the row —
        # otherwise a post-LLM ``_merge_metadata`` aput could resurrect this
        # key after it is gone from the store.
        _cancel_pending_metadata(namespace, key)

        # Capture the binary_ref BEFORE deleting the store row — once the row
        # is gone we can't recover where the bytes lived.
        binary_ref = (
            item.value.get("binary_ref") if isinstance(item.value, dict) else None
        )

        await adelete(store, namespace, key)

    # Best-effort cleanup of the original PDF in object storage. Failure is
    # logged but not surfaced to the caller — the user's memo is gone from
    # the catalog either way; the storage hygiene matters for compliance and
    # bucket size, not correctness.
    if isinstance(binary_ref, dict):
        await memo_binary_storage.delete_binary(binary_ref)

    await rebuild_memo_index(store, namespace)
    return {"status": "deleted", "key": key}


@router.post("/user/regenerate", response_model=MemoUploadResponse)
async def regenerate_user_memo_metadata(
    user_id: CurrentUserId,
    key: str = Query(..., description="Memo key (slug) to regenerate metadata for"),
) -> MemoUploadResponse:
    """Retry metadata generation for a memo (typically after a 'failed' status)."""
    validate_key(key)
    _reject_reserved_key(key)
    store = require_store(setup.store)
    namespace = _namespace(user_id)

    # Same lock as upload/write so the regenerate's RMW can't interleave with
    # a concurrent edit that rotates the row's modified_at.
    async with lock_for_namespace(namespace):
        item = await aget(store, namespace, key)
        if item is None:
            raise HTTPException(status_code=404, detail="Memo not found")
        if not isinstance(item.value, dict):
            raise HTTPException(status_code=500, detail="Malformed memo value")

        now = _now_iso()
        updated = {
            **item.value,
            "metadata_status": "pending",
            "metadata_error": None,
            "modified_at": now,
            "description": METADATA_PLACEHOLDER_DESCRIPTION,
        }
        await aput(store, namespace, key, updated)

    metadata_dispatched = _kickoff_metadata(
        user_id=user_id, namespace=namespace, key=key
    )
    if not metadata_dispatched:
        await rebuild_memo_index(store, namespace)
    return MemoUploadResponse(
        key=key,
        original_filename=updated.get("original_filename") or key,
        metadata_status="pending",
    )


# --- helpers ---------------------------------------------------------------


# Characters that are safe inside the ASCII ``filename="..."`` token: printable
# ASCII minus quote, backslash, and the separators that break either Starlette's
# latin-1 header encoder or downstream parsers. Any other char is replaced with
# an underscore in the ASCII fallback; the original (unicode) name flows through
# the ``filename*`` parameter per RFC 6266.
_ASCII_FILENAME_RE = re.compile(r"[^\w.\-+@~ ]", re.ASCII)


def _content_disposition(filename: str) -> str:
    """Build an RFC 6266-compliant Content-Disposition header.

    Pure-ASCII clients see ``filename="..."``; modern clients prefer the
    ``filename*=UTF-8''<percent-encoded>`` parameter and recover the original
    unicode name (e.g. CJK or emoji).

    Without this we'd hit Starlette's latin-1 header encoder and 500 the
    download endpoint for every memo whose filename contains a non-latin-1
    character — and we'd accept whatever quote/CR/LF the user injected via
    upload metadata.
    """
    cleaned = filename.replace("\r", "").replace("\n", "").replace('"', "")
    ascii_fallback = _ASCII_FILENAME_RE.sub("_", cleaned)
    if not ascii_fallback or ascii_fallback.isspace():
        ascii_fallback = "memo"
    encoded = quote(cleaned, safe="")
    return (
        f'attachment; filename="{ascii_fallback}"; '
        f"filename*=UTF-8''{encoded}"
    )
