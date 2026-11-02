"""The owner's side of artifacts: a workspace file's record, its published
versions, where its comments go, and the comments themselves.

Endpoints, owner-only (contract: the artifacts API build note):
- GET|POST /api/v1/workspaces/{workspace_id}/artifacts   - find by path, or ensure
- GET|PATCH /api/v1/artifacts/{artifact_id}               - the record; keep updated
- PUT|DELETE /api/v1/artifacts/{artifact_id}/binding      - bind or unbind a thread
- GET  /api/v1/artifacts/{artifact_id}/destinations       - where a comment goes
- GET  /api/v1/artifacts/{artifact_id}/status             - the live file against v<latest>
- POST /api/v1/artifacts/{artifact_id}/publish            - share and cut a version
- POST /api/v1/artifacts/{artifact_id}/unshare
- GET  /api/v1/artifacts/{artifact_id}/versions
- POST /api/v1/artifacts/{artifact_id}/grant              - where a version is framed from
- GET|POST /api/v1/artifacts/{artifact_id}/comments
- POST /api/v1/artifacts/{artifact_id}/comments/{comment_id}/messages
- PATCH|DELETE /api/v1/artifacts/{artifact_id}/comments/{comment_id}
- POST /api/v1/artifacts/{artifact_id}/comments/{comment_id}/read
- POST /api/v1/artifacts/{artifact_id}/comments/read
- GET  /api/v1/artifacts/threads/{thread_id}/queued-comments

``records`` holds the record, destination and version routes, ``comments``
the comment routes, ``models`` their shapes. A comment reaches the agent only
with a chat send (``artifact_comment_ids``, ``services/artifacts/comments``).
Every change publishes a user event, so the owner's other tabs read the
artifact again.
"""

from fastapi import APIRouter

from src.server.app.artifacts import comments, records
from src.server.app.artifacts.records import workspace_router

router = APIRouter(prefix="/api/v1/artifacts", tags=["Artifacts"])
router.include_router(records.router)
router.include_router(comments.router)

__all__ = ["router", "workspace_router"]
