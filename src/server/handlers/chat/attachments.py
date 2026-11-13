"""Turning a turn's attachments into something the model can act on.

Every file the user attached takes one of three routes, decided by what this
turn's model accepts: a native content block, a sandbox path the agent opens
with Python, or a note saying the file exists and cannot be read.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ptc_agent.agent.middleware.file_operations.image_fit import usable_image
from ptc_agent.core.project_context import ProjectContext

from src.llms.llm import get_image_types, get_input_modalities, image_route
from src.server.models.chat_attachment import (
    NATIVE_TURN_MAX_BYTES,
    TurnAttachment,
    TurnAttachments,
)
from src.server.services.chat_attachment_turns import (
    pull_attachments,
    read_for_model,
    write_unsaved,
)
from src.server.utils.multimodal_context import (
    build_file_reminder,
    build_unsupported_reminder,
    file_note,
    inject_multimodal_context,
    missing_note,
    pdf_page_count,
    route_for,
    wants_bytes,
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


def _accepted_images(effective_model: str | None, config: Any) -> frozenset[str]:
    """The image types the route being called takes, read off its client."""
    client = getattr(config, "llm_client", None)
    return get_image_types(
        image_route(getattr(client, "metadata", None) or {"manifest_model": effective_model})
    )


def _check(
    read: list[tuple[TurnAttachment, bytes | None]],
    modalities: list[str],
    accepted: frozenset[str],
) -> None:
    """Route the files read for the model by what their bytes allow, and count
    a PDF's pages.

    Blocking, since each file is decoded in full, and in one thread for the
    lot: a thread per file would take as many of the shared executor's
    workers as a request has files.
    """
    for att, raw in read:
        if raw is not None and att.kind == "pdf":
            att.pages = pdf_page_count(raw)
        readable = raw is not None and (att.kind != "image" or usable_image(raw, accepted))
        att.route = route_for(att, modalities, readable=readable)


async def attach_request_files(
    messages: list[dict],
    attachments: TurnAttachments,
    session: Any | None,
    effective_model: str | None,
    config: Any,
    *,
    project: ProjectContext | None,
) -> list[dict]:
    """Save the turn's attachments to the sandbox and describe them to the PTC agent.

    All attachments are saved to sandbox (when available) so the agent always
    has file access. Images the model accepts also go in as native content
    blocks; a PDF it accepts goes in as a note with its page count once it is
    saved, and inline only when it is not. ``attachments`` are the request's,
    resolved for its user (``resolve_turn_attachments``).
    """
    files = attachments.files
    if files:
        await _route_and_save(files, session, effective_model, config, project)
        messages = inject_multimodal_context(messages, files)
        for att in files:
            att.release()
        native = sum(1 for a in files if a.route == "native")
        if native:
            logger.info(f"[PTC_CHAT] Multimodal context injected: {native} supported attachment(s)")
        unsupported = [file_note(a) for a in files if a.route == "unsupported"]
        if unsupported:
            _append_to_last_user_message(messages, build_unsupported_reminder(unsupported))
        file_only = [file_note(a) for a in files if a.route == "file"]
        if file_only:
            _append_to_last_user_message(messages, build_file_reminder(file_only))
            logger.info(
                f"[PTC_CHAT] {len(file_only)} file-only attachment(s) for {effective_model}"
            )
    if attachments.missing:
        _append_to_last_user_message(
            messages, build_file_reminder([missing_note(m) for m in attachments.missing])
        )
    return messages


async def _route_and_save(
    files: list[TurnAttachment],
    session: Any | None,
    effective_model: str | None,
    config: Any,
    project: ProjectContext | None,
) -> None:
    """Set each file's route and, with a sandbox, its saved path.

    The files whose route depends on their bytes are read and kept, up to
    the turn's budget for content blocks, while the sandbox fetches the
    uploaded ones from the store. The rest are then written from here, which
    the kept bytes save a second read for, beside the check.
    """
    modalities = _modalities(effective_model, config)
    for att in files:
        att.route = route_for(att, modalities)
    wanted: list[TurnAttachment] = []
    budget = NATIVE_TURN_MAX_BYTES
    for att in files:
        if not wants_bytes(att, modalities):
            continue
        if att.size > budget:
            att.route = "file"
            continue
        budget -= att.size
        wanted.append(att)
    reading = asyncio.gather(*(read_for_model(a) for a in wanted))
    sandbox = session.sandbox if session else None
    if sandbox is None:
        read = await reading
    else:
        read, _ = await asyncio.gather(
            reading, pull_attachments(files, sandbox, project=project)
        )
    checking = asyncio.to_thread(
        _check, list(zip(wanted, read)), modalities, _accepted_images(effective_model, config)
    )
    if sandbox is None:
        await checking
        return
    await asyncio.gather(write_unsaved(files, sandbox, project=project), checking)
    logger.info(f"[PTC_CHAT] Saved {sum(1 for a in files if a.path)} of {len(files)} attachment(s) to sandbox")
