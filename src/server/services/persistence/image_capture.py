"""Late image capture: sandbox images a turn's text still names at its end.

``ImageCaptureMiddleware`` rewrites checkpointed messages at model time and
leaves a path in place when its capture fails or times out. The main turn's
leftovers are captured as its run ends and their URLs recorded on its
response row; a subagent lane's, from its captured events, onto its runs. Keys
are content-addressed (shared helpers), so every pass converges on identical
URLs and re-uploads are idempotent PUTs of the same bytes.

No-op when storage is disabled (storage.provider = "none").
"""

import logging

from ptc_agent.core.project_context import ProjectContext

from ptc_agent.agent.middleware.image_capture import (
    IMAGE_MD_RE,
    capture_sandbox_images,
    is_sandbox_image_path,
)
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)


def sandbox_image_paths(events: list[dict]) -> set[str]:
    """Every sandbox image path the events' text references.

    A message's text streams in pieces that can split a reference anywhere,
    so each message's pieces are joined before they are read.
    """
    texts: dict[tuple, list[str]] = {}
    for evt in events:
        if evt.get("event") != "message_chunk":
            continue
        data = evt.get("data") or {}
        content = data.get("content")
        if data.get("content_type") != "text" or not isinstance(content, str):
            continue
        texts.setdefault((data.get("agent"), data.get("id")), []).append(content)
    image_paths: set[str] = set()
    for pieces in texts.values():
        for match in IMAGE_MD_RE.finditer("".join(pieces)):
            if is_sandbox_image_path(match.group(2)):
                image_paths.add(match.group(2))
    return image_paths


async def capture_images(
    image_paths: set[str],
    sandbox,
    thread_id: str = "",
    *,
    project: ProjectContext | None = None,
    workspace_id: str | None = None,
) -> dict[str, str]:
    """Upload ``image_paths`` from the sandbox, returning each captured
    path's URL.

    Empty if storage is disabled. Non-fatal: logs warnings on failure,
    never raises.
    """
    if not is_storage_enabled() or not image_paths:
        return {}
    if project is None and workspace_id:
        return await _capture_from_held_folder(
            sandbox, image_paths, thread_id, workspace_id
        )
    return await capture_sandbox_images(
        sandbox, image_paths, thread_id, project=project
    )


async def _capture_from_held_folder(
    sandbox, image_paths: set[str], thread_id: str, workspace_id: str
) -> dict[str, str]:
    """Late, the run is terminal and a settle may move the folder: the images
    are read from the one the row names under the folder hold, and a folder
    mid-move gives none rather than whatever holds its old name."""
    from src.server.database.workspace_folders import (
        WorkspaceFolderMoving,
        is_top_level,
        workspace_folder_in_use,
    )
    from src.server.services.workspace_layout import resolve_project_placement

    try:
        async with workspace_folder_in_use(workspace_id):
            try:
                placement = await resolve_project_placement(
                    workspace_id, root=sandbox.working_dir
                )
            except Exception:
                logger.warning("[IMAGE_CAPTURE] Workspace placement unavailable", exc_info=True)
                return {}
            if placement.dir_name and not is_top_level(placement.dir_name):
                return {}
            project = ProjectContext(
                workspace_id, placement.dir_name,
                placement.sibling_dir_names, placement.layout_origin,
                placement.previous_dir_names,
            )
            return await capture_sandbox_images(
                sandbox, image_paths, thread_id, project=project
            )
    except WorkspaceFolderMoving:
        logger.info(f"[IMAGE_CAPTURE] Folder of {workspace_id} is moving; images left as paths")
        return {}
