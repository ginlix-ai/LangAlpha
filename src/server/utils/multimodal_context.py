"""
Multimodal context utilities for chat endpoint.

Parses MultimodalContext items from additional_context and injects image/PDF
content blocks into user messages so the LLM receives native multimodal input.
"""

import asyncio
import base64
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from src.server.models.additional_context import MultimodalContext
from src.utils.storage import get_public_url, is_storage_enabled, sanitize_storage_key, upload_base64

logger = logging.getLogger(__name__)


def parse_multimodal_contexts(
    additional_context: Optional[List[Any]],
) -> List[MultimodalContext]:
    """Extract MultimodalContext items from additional_context list.

    Args:
        additional_context: List of context items from ChatRequest

    Returns:
        List of MultimodalContext objects
    """
    if not additional_context:
        return []

    contexts = []

    _multimodal_types = ("image", "pdf", "file")

    for ctx in additional_context:
        if isinstance(ctx, dict):
            if ctx.get("type") in _multimodal_types:
                contexts.append(
                    MultimodalContext(
                        type=ctx.get("type", "image"),
                        data=ctx.get("data", ""),
                        description=ctx.get("description"),
                    )
                )
        elif isinstance(ctx, MultimodalContext):
            contexts.append(ctx)
        elif hasattr(ctx, "type") and ctx.type in _multimodal_types:
            contexts.append(
                MultimodalContext(
                    type=ctx.type,
                    data=getattr(ctx, "data", ""),
                    description=getattr(ctx, "description", None),
                )
            )

    return contexts


async def build_attachment_metadata(
    contexts: List[MultimodalContext],
    thread_id: str = "",
) -> List[Dict[str, Any]]:
    """Build attachment metadata dicts, uploading to storage when enabled.

    Returns list of {name, type, size, url?} dicts.
    Uploads run concurrently via asyncio.gather.
    """
    batch_id = uuid.uuid4().hex[:12]
    prefix = f"attachments/{thread_id}/{batch_id}" if thread_id else f"attachments/{batch_id}"

    async def _process(ctx: MultimodalContext) -> Dict[str, Any]:
        is_pdf = ctx.data.startswith("data:application/pdf")
        is_image = ctx.data.startswith("data:image/")
        name = ctx.description or "file"
        meta: Dict[str, Any] = {
            "name": name,
            "type": "pdf" if is_pdf else "image" if is_image else "file",
            "size": len(ctx.data.split(",", 1)[1]) * 3 // 4 if "," in ctx.data else 0,
        }
        if is_storage_enabled():
            safe_key = sanitize_storage_key(name, ctx.data)
            storage_key = f"{prefix}/{safe_key}"
            try:
                success = await asyncio.to_thread(upload_base64, storage_key, ctx.data)
                if success:
                    meta["url"] = get_public_url(storage_key)
            except Exception:
                logger.warning("Failed to upload attachment %r", safe_key, exc_info=True)
        return meta

    return list(await asyncio.gather(*(_process(ctx) for ctx in contexts)))


def inject_multimodal_context(
    messages: List[Dict[str, Any]],
    multimodal_contexts: List[MultimodalContext],
    file_paths: Optional[List[Optional[str]]] = None,
) -> List[Dict[str, Any]]:
    """Merge image/PDF content blocks into the last user message.

    Prepends content blocks before the user's text content so the LLM sees
    the attachment context first. Non-image/non-PDF contexts are skipped
    (they are handled via system-reminder with sandbox path only).

    Args:
        messages: List of message dicts (role + content)
        multimodal_contexts: List of MultimodalContext objects to inject
        file_paths: Optional parallel list of sandbox virtual paths. When
            provided, path references are included in the label text.

    Returns:
        Modified messages list with content blocks merged into user message
    """
    if not multimodal_contexts or not messages:
        return messages

    # Build content blocks from contexts
    blocks: List[Dict[str, Any]] = []
    for idx, ctx in enumerate(multimodal_contexts):
        data_url = ctx.data
        desc = ctx.description or "file"
        path_note = ""
        if file_paths and idx < len(file_paths) and file_paths[idx]:
            path_note = f" (saved to {file_paths[idx]})"

        if data_url.startswith("data:application/pdf"):
            raw_b64 = data_url.split(",", 1)[1] if "," in data_url else data_url
            blocks.append({"type": "text", "text": f"[Attached PDF: {desc}{path_note}]"})
            blocks.append({
                "type": "file",
                "base64": raw_b64,
                "mime_type": "application/pdf",
                "filename": desc,
            })
        elif data_url.startswith("data:image/"):
            blocks.append({"type": "text", "text": f"[Attached image: {desc}{path_note}]"})
            blocks.append({"type": "image_url", "image_url": {"url": data_url}})
        else:
            # Non-image/non-PDF: skip content block injection.
            # These are handled via system-reminder with sandbox path only.
            continue

    if not blocks:
        return messages

    # Find last user message and prepend blocks into its content
    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == "user":
            content = messages[i]["content"]
            if isinstance(content, str):
                messages[i]["content"] = blocks + [{"type": "text", "text": content}]
            elif isinstance(content, list):
                messages[i]["content"] = blocks + content
            break

    return messages


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
        notes: Per-file descriptions (e.g. from the PTC workflow
            or a simple type summary for Flash mode).

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
    types — they are always processed via sandbox + Python.
    """
    body = "\n".join(notes)
    return f"\n\n<system-reminder>\n{body}\n</system-reminder>"


# -- Capability-aware helpers ------------------------------------------------

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


def filter_multimodal_by_capability(
    contexts: list,
    modalities: list[str],
) -> Tuple[list, list, list]:
    """Filter multimodal contexts by model capabilities.

    Image and PDF items are checked against the model's supported modalities.
    All other file types (xlsx, csv, etc.) bypass capability checking entirely
    because no model can consume them natively — they always require sandbox
    processing via Python.

    Args:
        contexts: List of MultimodalContext objects (or dicts with a ``data`` key).
        modalities: List of modality strings the model supports (e.g.
            ``["text", "image", "pdf"]``).

    Returns:
        Tuple of (supported, unsupported, file_only).

        - **supported**: image/PDF items the model handles natively.
        - **unsupported**: image/PDF items the model cannot handle.
        - **file_only**: non-image/non-PDF items (always need sandbox + Python).
    """
    supported: list = []
    unsupported: list = []
    file_only: list = []
    for ctx in contexts:
        data = ctx.data if hasattr(ctx, "data") else ctx.get("data", "")
        is_pdf = data.startswith("data:application/pdf")
        is_image = data.startswith("data:image/")
        if is_pdf or is_image:
            needed = "pdf" if is_pdf else "image"
            if needed in modalities:
                supported.append(ctx)
            else:
                unsupported.append(ctx)
        else:
            file_only.append(ctx)
    return supported, unsupported, file_only


async def upload_to_sandbox(
    contexts: list,
    sandbox,
    upload_dir: str = "uploads",
) -> List[Optional[str]]:
    """Upload multimodal files to the sandbox filesystem.

    For each context item the base64 payload is decoded and written into the
    sandbox's ``work/{upload_dir}/`` directory.

    Args:
        contexts: List of MultimodalContext objects to upload.
        sandbox: The sandbox instance (must expose ``aupload_file_bytes``,
            ``normalize_path``, and optionally ``virtualize_path``).
        upload_dir: Sub-directory under ``work/`` to store uploads.

    Returns:
        List of virtual paths parallel to the input list (``None`` on failure).
    """
    paths: List[Optional[str]] = []

    for ctx in contexts:
        data_url = ctx.data if hasattr(ctx, "data") else ctx.get("data", "")
        desc = (
            ctx.description if hasattr(ctx, "description") else ctx.get("description")
        ) or "file"

        try:
            header, b64_content = data_url.split(",", 1)
            mime_type = header.split(":")[1].split(";")[0] if ":" in header else ""

            file_bytes = base64.b64decode(b64_content)

            ext = _MIME_EXTENSIONS.get(mime_type, _ext_from_desc(desc))
            unique_id = uuid.uuid4().hex[:8]
            safe_desc = "".join(
                c if c.isalnum() or c in "-_." else "_" for c in desc
            ).strip("_")[:60]
            filename = f"{safe_desc}_{unique_id}{ext}" if safe_desc else f"{unique_id}{ext}"

            rel_path = f"work/{upload_dir}/{filename}"
            abs_path = sandbox.normalize_path(rel_path)

            ok = await sandbox.aupload_file_bytes(abs_path, file_bytes)
            if ok:
                # Return relative path (agent convention per workspace_paths template)
                paths.append(rel_path)
            else:
                logger.warning(f"Upload returned False for '{desc}'")
                paths.append(None)

        except Exception:
            logger.warning(
                f"Failed to upload attachment '{desc}' to sandbox",
                exc_info=True,
            )
            paths.append(None)

    return paths
