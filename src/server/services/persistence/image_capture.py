"""Post-stream safety net: capture sandbox images referenced in SSE events.

``ImageCaptureMiddleware`` rewrites checkpointed messages at model time; the
streamed sse_events still carry sandbox paths, so this hook rewrites them at
persistence time. Keys are content-addressed (shared helpers), so both passes
converge on identical URLs — re-uploads are idempotent PUTs of the same bytes.

No-op when storage is disabled (storage.provider = "none").
"""

import logging

from ptc_agent.agent.middleware.image_capture import (
    IMAGE_MD_RE,
    capture_sandbox_images,
    is_sandbox_image_path,
    rewrite_image_paths,
)
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)


async def capture_and_rewrite_images(
    sse_events: list[dict],
    sandbox,
    thread_id: str = "",
) -> int:
    """Scan SSE events for sandbox image paths, upload to storage, rewrite in-place.

    Returns number of images captured. No-op if storage is disabled.
    Non-fatal: logs warnings on failure, never raises.
    """
    if not is_storage_enabled() or not sse_events:
        return 0

    work_dir = sandbox.working_dir

    # Collect all unique sandbox image paths from text message_chunks
    image_paths: set[str] = set()
    for evt in sse_events:
        if evt.get("event") != "message_chunk":
            continue
        data = evt.get("data", {})
        if data.get("content_type") != "text":
            continue
        content = data.get("content", "")
        for match in IMAGE_MD_RE.finditer(content):
            if is_sandbox_image_path(match.group(2), work_dir):
                image_paths.add(match.group(2))

    if not image_paths:
        return 0

    path_to_url = await capture_sandbox_images(sandbox, image_paths, thread_id)
    if not path_to_url:
        return 0

    # Persist the path→URL map as a ui record so checkpoint-sourced replay of
    # pre-middleware turns can resolve sandbox image paths without the
    # (long-gone) sandbox. New turns don't need it — the middleware rewrites
    # the checkpointed message itself — but the record is harmless and keeps
    # this hook a complete fallback while it remains in place.
    if thread_id:
        try:
            from src.server.services.history.reader import CheckpointHistoryReader

            await CheckpointHistoryReader.get_instance().append_ui_record(
                thread_id, "image_capture", {"path_to_url": path_to_url}
            )
        except Exception as e:
            logger.warning(f"[IMAGE_CAPTURE] Failed to persist ui record: {e}")

    # Rewrite image paths in SSE events in-place
    for evt in sse_events:
        if evt.get("event") != "message_chunk":
            continue
        data = evt.get("data", {})
        if data.get("content_type") != "text":
            continue
        content = data.get("content", "")
        if content:
            data["content"] = rewrite_image_paths(content, path_to_url)

    return len(path_to_url)
