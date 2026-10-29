"""Workspace Files API Router.

Provides file operations against a workspace's Daytona sandbox, with DB
fallback for stopped workspaces (offline file access).

Design goals:
- Proxy all file access through the backend (UI clients never talk to Daytona directly).
- Auto-start stopped workspaces for write operations.
- Serve files from PostgreSQL when sandbox is stopped (read-only).
- Support both virtual paths ("work/task/foo.txt") and absolute sandbox paths
  ("/home/workspace/work/task/foo.txt").
- Return virtual paths to clients for a consistent UX.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/files
- GET    /api/v1/workspaces/{workspace_id}/files/read
- POST   /api/v1/workspaces/{workspace_id}/files/resolve
- PUT    /api/v1/workspaces/{workspace_id}/files/write
- GET    /api/v1/workspaces/{workspace_id}/files/download
- POST   /api/v1/workspaces/{workspace_id}/files/upload
- DELETE /api/v1/workspaces/{workspace_id}/files

Uploads sent through object storage (``uploads``):
- POST   /api/v1/workspaces/{workspace_id}/files/uploads
- POST   /api/v1/workspaces/{workspace_id}/files/uploads/{upload_id}/parts
- POST   /api/v1/workspaces/{workspace_id}/files/uploads/{upload_id}/complete
- GET    /api/v1/workspaces/{workspace_id}/files/uploads/{upload_id}
- DELETE /api/v1/workspaces/{workspace_id}/files/uploads/{upload_id}

Plus a path-style serving router whose credential is a signed, expiring
owner grant (``services/file_grants``):
- GET    /api/v1/wsfiles/g/{grant}/{path:path}
"""

from .crud import router
from .serve import wsfiles_router
from .uploads import router as _uploads_router

router.include_router(_uploads_router)

__all__ = ["router", "wsfiles_router"]
