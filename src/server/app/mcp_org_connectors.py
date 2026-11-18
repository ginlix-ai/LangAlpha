"""The connectors a user's organization sets up, as the member sees them.

Not catalog rows: the organization defines them and the auth service sends
the definitions with the membership. A member switches one off per workspace
through the workspace toggle and signs in or out of one that members sign in
to (``app/mcp_oauth.py``); nothing here edits or deletes one.

Endpoints (user-scoped):
- GET /api/v1/mcp/org-connectors
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from src.server.models.mcp_server import OrgConnectorList
from src.server.services.mcp_config import member_listing
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])


@router.get("/org-connectors")
@handle_api_exceptions("list organization connectors", logger)
async def list_org_connectors(user_id: CurrentUserId) -> OrgConnectorList:
    """The organization's connectors this member may use, with their own
    sign-in and the workspaces each is switched off in; 503 while the account
    cannot be read. Reads nothing where the deployment serves none."""
    return OrgConnectorList(connectors=await member_listing(user_id))
