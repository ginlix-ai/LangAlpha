"""Turning a turn's attachments into something the model can act on.

Every file the user attached takes one of three routes, decided by what this
turn's model accepts: a native content block, a sandbox path the agent opens
with Python, or a note saying the file exists and cannot be read.
"""

from __future__ import annotations

from typing import Any

from ptc_agent.core.project_context import ProjectContext

from src.llms.llm import get_image_types, get_input_modalities, image_route
from src.server.models.chat import ChatRequest
from src.server.utils.multimodal_context import (
    build_file_reminder,
    build_unsupported_reminder,
    filter_multimodal_by_capability,
    find_unreadable_images,
    inject_multimodal_context,
    parse_multimodal_contexts,
    pdf_page_counts,
    upload_to_sandbox,
)

from .request_prep import _append_to_last_user_message, logger


def _modalities(effective_model: str | None, config: Any) -> list[str]:
    """What this turn's model accepts natively, text alone when it is unresolved."""
    return (
        get_input_modalities(
            effective_model, custom_modalities=config.input_modalities
        )
        if effective_model
        else ["text"]
    )


async def attach_request_files(
    messages: list[dict],
    request: ChatRequest,
    session: Any | None,
    effective_model: str | None,
    config: Any,
    *,
    project: ProjectContext | None = None,
) -> list[dict]:
    """Upload the turn's attachments and describe them to the PTC agent.

    All attachments are uploaded to sandbox (when available) so the agent always
    has file access. Images the model accepts also go in as native content
    blocks; a PDF it accepts goes in as a note with its page count once it is
    saved, and inline only when it is not.
    """
    multimodal_contexts = parse_multimodal_contexts(request.additional_context)
    if not multimodal_contexts or request.hitl_response:
        return messages

    # 1. Upload ALL files to sandbox
    file_paths: list = []
    if session and session.sandbox:
        file_paths = await upload_to_sandbox(
            multimodal_contexts, session.sandbox, project=project
        )
        logger.info(
            f"[PTC_CHAT] Uploaded {len(multimodal_contexts)} attachment(s) to sandbox"
        )

    # 2. Filter by model capability for native content blocks. An image the
    # turn's route can't be given takes the file-only route on every model.
    modalities = _modalities(effective_model, config)
    client = getattr(config, "llm_client", None)
    route = image_route(
        getattr(client, "metadata", None) or {"manifest_model": effective_model}
    )
    supported, unsupported, file_only = filter_multimodal_by_capability(
        multimodal_contexts,
        modalities,
        unreadable=await find_unreadable_images(
            multimodal_contexts, get_image_types(route)
        ),
    )

    # 3. Inject supported as native content blocks (merged into user message)
    if supported:
        supported_paths = [
            file_paths[i]
            for i, ctx in enumerate(multimodal_contexts)
            if ctx in supported
        ] if file_paths else None
        messages = inject_multimodal_context(
            messages,
            supported,
            file_paths=supported_paths,
            pdf_pages=await pdf_page_counts(supported),
        )
        logger.info(
            f"[PTC_CHAT] Multimodal context injected: "
            f"{len(supported)} supported attachment(s)"
        )

    # Helper to build per-file path notes
    def _file_note(ctx, idx):
        desc = ctx.description or "file"
        data = ctx.data
        mime = data.split(":")[1].split(";")[0] if ":" in data else "unknown"
        fpath = file_paths[idx] if file_paths and idx < len(file_paths) else None
        if fpath:
            return (
                f"The user attached a file ({desc}, {mime}). "
                f"It has been saved to {fpath}. "
                f"Use Python to process it."
            )
        return f"The user attached a file ({desc}, {mime})."

    # 4. Unsupported image/PDF: "cannot view" warning + file paths
    if unsupported:
        notes = [
            _file_note(ctx, i)
            for i, ctx in enumerate(multimodal_contexts)
            if ctx in unsupported
        ]
        _append_to_last_user_message(messages, build_unsupported_reminder(notes))

    # 5. File-only (xlsx, csv, etc.): path notes only, no "cannot view"
    if file_only:
        notes = [
            _file_note(ctx, i)
            for i, ctx in enumerate(multimodal_contexts)
            if ctx in file_only
        ]
        _append_to_last_user_message(messages, build_file_reminder(notes))
        logger.info(
            f"[PTC_CHAT] {len(file_only)} file-only attachment(s) "
            f"uploaded to sandbox for {effective_model}"
        )

    return messages

