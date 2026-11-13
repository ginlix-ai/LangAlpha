"""
Multimodal context utilities for chat endpoint.

Parses MultimodalContext items from additional_context, and turns a turn's
attachments into what the model reads: image/PDF content blocks merged into
the user message, and notes naming the files it reaches by path.
"""

import asyncio
import base64
import io
import logging
import uuid
from urllib.parse import quote
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from src.llms.llm import COMMON_IMAGE_TYPES
from src.server.models.additional_context import MultimodalContext
from src.server.models.chat_attachment import (
    NATIVE_MAX_BYTES,
    AttachmentRoute,
    InlineSource,
    MissingAttachment,
    TurnAttachment,
    TurnAttachments,
    normalize_mime,
    served_type,
)
from src.utils.storage import get_public_url, is_storage_enabled, sanitize_storage_key, upload_bytes

logger = logging.getLogger(__name__)

_MULTIMODAL_TYPES = ("image", "pdf", "file")


def parse_multimodal_contexts(
    additional_context: Optional[List[Any]],
) -> List[MultimodalContext]:
    """The attachment items of ``additional_context``.

    A raw item that does not validate (one naming neither inline data nor an
    uploaded id, say) is dropped with a log line: a client sending junk
    beside a message loses that item, not the turn.
    """
    contexts: List[MultimodalContext] = []
    for ctx in additional_context or []:
        if isinstance(ctx, MultimodalContext):
            contexts.append(ctx)
        elif isinstance(ctx, dict) and ctx.get("type") in _MULTIMODAL_TYPES:
            try:
                contexts.append(MultimodalContext.model_validate(ctx))
            except ValidationError as e:
                logger.warning(f"Dropped an attachment item that does not validate: {e.error_count()} error(s)")
    return contexts


def inline_attachment(ctx: MultimodalContext) -> Optional[TurnAttachment]:
    """An inline item as the turn's attachment, its data URL decoded once;
    None when it does not decode."""
    assert ctx.data is not None
    header, sep, payload = ctx.data.partition(",")
    if not sep:
        return None
    try:
        raw = base64.b64decode(payload)
    except ValueError:
        return None
    mime = header.split(":")[1].split(";")[0] if ":" in header else ""
    return TurnAttachment(
        name=ctx.description or "file",
        mime_type=mime,
        size=len(raw),
        source=InlineSource(raw),
    )


# -- routing -------------------------------------------------------------------


def wants_bytes(att: TurnAttachment, modalities: List[str]) -> bool:
    """Whether routing ``att`` needs its bytes read first: an image or PDF
    the model takes, read for its block, its check or its page count."""
    return att.size <= NATIVE_MAX_BYTES and att.kind != "file" and att.kind in modalities


def route_for(
    att: TurnAttachment, modalities: List[str], *, readable: bool = True
) -> AttachmentRoute:
    """How ``att`` reaches a model taking ``modalities``.

    Only an image or PDF small enough for a request goes in as a content
    block; one whose bytes could not be read or, for an image, given to the
    route goes in as a file. One the model does not take is a file the agent
    is told it cannot view. Everything else is always a file.
    """
    if att.kind == "file" or att.size > NATIVE_MAX_BYTES or not readable:
        return "file"
    if att.kind in modalities:
        return "native"
    # Told it cannot view an image, the agent suggests a model that can; for a
    # type no model takes as is, another model would not help either.
    if att.kind == "image" and att.mime_type not in COMMON_IMAGE_TYPES:
        return "file"
    return "unsupported"


def pdf_page_count(raw: bytes) -> Optional[int]:
    """Pages in a PDF, or None when it cannot be read (encrypted or corrupt).

    Blocking: parsing a large PDF takes long enough to stall the event loop,
    so async callers run it in a thread.
    """
    try:
        import pypdf

        # A broken page tree can parse as zero pages without raising, and
        # "0 pages" would read as an empty document rather than an unreadable one.
        return len(pypdf.PdfReader(io.BytesIO(raw)).pages) or None
    except Exception:
        return None


# -- history ---------------------------------------------------------------------


def _object_url(key: str) -> str:
    """The link to a stored attachment. The key keeps the file's name as
    given, so a ``#`` or ``?`` in it would end the path the browser asks for."""
    return get_public_url(quote(key, safe="/"))


async def build_attachment_metadata(
    attachments: TurnAttachments,
    thread_id: str = "",
) -> List[Dict[str, Any]]:
    """The history's entry for each attachment: ``{name, type, size, url?}``.

    An inline file is stored for its link when storage is enabled, all at
    once. An uploaded one's object is already the stored copy, so nothing is
    stored again; one whose bytes Postgres holds has no link, as an
    attachment has none without storage.
    """
    batch_id = uuid.uuid4().hex[:12]
    prefix = f"attachments/{thread_id}/{batch_id}" if thread_id else f"attachments/{batch_id}"

    async def entry(att: TurnAttachment) -> Dict[str, Any]:
        meta: Dict[str, Any] = {"name": att.name, "type": att.kind, "size": att.size}
        source = att.source
        if not isinstance(source, InlineSource):
            if source.object_key:
                meta["url"] = _object_url(source.object_key)
            return meta
        if is_storage_enabled():
            # The key's extension comes off the type as a data URL states it.
            safe_key = sanitize_storage_key(att.name, f"data:{att.mime_type};")
            # Its own folder: two files of one name in a turn are two objects.
            storage_key = f"{prefix}/{uuid.uuid4().hex[:8]}/{safe_key}"
            try:
                stored = await asyncio.to_thread(
                    upload_bytes,
                    storage_key,
                    source.data,
                    # The type the request claims, served only where it is safe
                    # to render from the store's origin.
                    content_type=served_type(normalize_mime(att.mime_type)),
                )
                if stored:
                    meta["url"] = _object_url(storage_key)
            except Exception:
                logger.warning("Failed to upload attachment %r", safe_key, exc_info=True)
        return meta

    files = await asyncio.gather(*(entry(att) for att in attachments.files))
    return [*files, *(_missing_entry(m) for m in attachments.missing)]


def drop_gone_metadata(
    entries: List[Dict[str, Any]],
    listed: List[TurnAttachment],
    attachments: TurnAttachments,
) -> List[Dict[str, Any]]:
    """The history's entries again once admission found some of ``listed``
    (the files ``entries`` was built from, in order) no longer there: those
    show as missing, as the agent is told they are."""
    present = {id(a) for a in attachments.files}
    files = [e for a, e in zip(listed, entries) if id(a) in present]
    return [*files, *(_missing_entry(m) for m in attachments.missing)]


def _missing_entry(missing: MissingAttachment) -> Dict[str, Any]:
    return {"name": missing.name, "type": missing.kind, "size": 0, "missing": True}


# -- what the model reads --------------------------------------------------------


def inject_multimodal_context(
    messages: List[Dict[str, Any]],
    attachments: List[TurnAttachment],
) -> List[Dict[str, Any]]:
    """Merge the content blocks of the attachments routed ``native`` into the
    last user message, ahead of its text so the model sees them first.

    A PDF saved to the sandbox goes in as a note with its page count and path,
    not as a document: inlined whole, a long one fills the context window on
    its first turn and every turn after, while from the path the agent can
    search it with code and Read only the pages it needs. With no saved path
    the document is the only way in, so it is inlined, stamped with its page
    count so the read side can hold it to the model's page ceiling, and as
    unsaved so what replaces it there offers no copy to extract from.

    Mutates ``messages`` in place.
    """
    blocks: List[Dict[str, Any]] = []
    for att in attachments:
        if att.route != "native":
            continue
        path_note = f" (saved to {att.path})" if att.path else ""
        if att.kind == "pdf":
            if att.path:
                pages = att.pages
                size = f", {pages} page{'' if pages == 1 else 's'}" if pages else ""
                blocks.append({"type": "text", "text": f"[Attached PDF: {att.name}{size}{path_note}]"})
                continue
            blocks.append({"type": "text", "text": f"[Attached PDF: {att.name}]"})
            file_block: Dict[str, Any] = {
                "type": "file",
                "base64": base64.b64encode(att.in_hand()).decode(),
                "mime_type": "application/pdf",
                "filename": att.name,
            }
            if att.pages:
                file_block["pages"] = att.pages
            # Read off by the strip, which must not point the model at a
            # workspace copy when the page ceiling or a model switch drops it.
            file_block["unsaved"] = True
            blocks.append(file_block)
        else:
            data_url = f"data:{att.mime_type};base64,{base64.b64encode(att.in_hand()).decode()}"
            blocks.append({"type": "text", "text": f"[Attached image: {att.name}{path_note}]"})
            blocks.append({"type": "image_url", "image_url": {"url": data_url}})

    if not blocks:
        return messages

    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == "user":
            content = messages[i]["content"]
            if isinstance(content, str):
                # A turn that opens with no words of the user's (the onboarding
                # kickoff) gets no empty text block: some providers refuse one.
                text = [{"type": "text", "text": content}] if content.strip() else []
                messages[i]["content"] = blocks + text
            elif isinstance(content, list):
                messages[i]["content"] = blocks + content
            break

    return messages


def file_note(att: TurnAttachment) -> str:
    """A file the agent reaches by path, or is only told of when it could not
    be saved."""
    mime = att.mime_type or "unknown"
    if att.path:
        return (
            f"The user attached a file ({att.name}, {mime}). "
            f"It has been saved to {att.path}. "
            f"Use Python to process it."
        )
    # Not just "attached": told only that, the agent looks for the file, or
    # answers as if it had read it.
    return (
        f"The user attached a file ({att.name}, {mime}), but it could not be "
        "saved where you can open it. If it is needed, ask the user to attach it again."
    )


def missing_note(missing: MissingAttachment) -> str:
    """Said, so the agent can ask for the file again rather than answer as if
    it saw it."""
    return (
        f"The user attached a file ({missing.name}), but {missing.why}. "
        "If it is needed, ask the user to attach it again."
    )


# -- Unsupported-attachment reminder -----------------------------------------

_UNSUPPORTED_AGENT_INSTRUCTION = (
    "You cannot view the attached file(s) directly because the current model "
    "does not support this input type. Be transparent with the user about this "
    "limitation and suggest they try switching to a model that supports these "
    "input types (e.g. one with vision/PDF support). Work in best effort to "
    "answer the user's query."
)


def build_unsupported_reminder(notes: list[str]) -> str:
    """Build a ``<system-reminder>`` for unsupported attachments.

    Wraps one or more file-description *notes* with standard agent
    instructions, matching the ``build_directive_reminder`` pattern.

    Args:
        notes: Per-file descriptions (e.g. from the PTC workflow).

    Returns:
        Formatted ``<system-reminder>`` string ready for
        ``_append_to_last_user_message``.
    """
    body = "\n".join(notes)
    return (
        "\n\n<system-reminder>\n"
        f"{body}\n"
        f"{_UNSUPPORTED_AGENT_INSTRUCTION}\n"
        "</system-reminder>"
    )


def build_file_reminder(notes: list[str]) -> str:
    """Build a ``<system-reminder>`` for file-only attachments.

    Unlike :func:`build_unsupported_reminder`, this does **not** include the
    "cannot view" warning because no model can natively consume these file
    types: they are always processed via sandbox + Python.
    """
    body = "\n".join(notes)
    return f"\n\n<system-reminder>\n{body}\n</system-reminder>"


# -- Where a file is saved -----------------------------------------------------

# Mapping from MIME type to file extension
_MIME_EXTENSIONS = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "text/csv": ".csv",
    "text/plain": ".txt",
    "application/json": ".json",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
}


def _ext_from_desc(desc: str) -> str:
    """Extract file extension from description/filename, or empty string."""
    if "." in desc:
        return "." + desc.rsplit(".", 1)[1].lower()[:10]
    return ""


def upload_rel_path(desc: str, mime_type: str, upload_dir: str = "uploads") -> str:
    """Where an attachment is saved, relative to the workspace folder: its name
    made safe, a short unique id, and an extension from its type or its name."""
    ext = _MIME_EXTENSIONS.get(mime_type, _ext_from_desc(desc))
    unique_id = uuid.uuid4().hex[:8]
    stem = desc
    if ext and _ext_from_desc(desc) in {ext, ".jpeg" if ext == ".jpg" else ext}:
        stem = desc.rsplit(".", 1)[0]
    safe_desc = "".join(
        c if c.isalnum() or c in "-_." else "_" for c in stem
    ).strip("_")[:60]
    filename = f"{safe_desc}_{unique_id}{ext}" if safe_desc else f"{unique_id}{ext}"
    return f"work/{upload_dir}/{filename}"
