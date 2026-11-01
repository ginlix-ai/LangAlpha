"""An artifact version's files on the API origin, for a deployment with no
preview base.

The grant in the path is the credential (``services/artifacts/grants``), and
the same loader serves an artifact's own host (``preview_proxy/artifacts.py``).
With a preview base set, versions run on their own hosts and this route
serves nothing, so a grant opens a version on its artifact's origin alone.

Endpoint:
- GET|HEAD /api/v1/artifacts/g/{grant}/v/{seq}/{path}
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import Response

from src.config.env import PREVIEW_PROXY_FRAME_ANCESTORS
from src.server.preview_proxy.artifacts import (
    ArtifactFileUnavailable,
    load_artifact_file,
)
from src.server.services.preview_base import preview_base

logger = logging.getLogger(__name__)

frame_router = APIRouter(prefix="/api/v1/artifacts", tags=["Artifact Serving"])


@frame_router.api_route("/g/{grant}/v/{seq}/{path:path}", methods=["GET", "HEAD"])
async def serve_artifact_file(
    grant: str,
    seq: str,
    path: str,
    if_none_match: Annotated[str | None, Header()] = None,
    host: Annotated[str | None, Header()] = None,
) -> Response:
    """A version's file, sandboxed to an opaque origin. Uniform 404 for a bad
    or expired grant, an unshared link, a version that is no longer the
    latest for a visitor, a missing version and a missing file."""
    if preview_base() is not None:
        raise HTTPException(status_code=404, detail="Not found")
    try:
        served = await load_artifact_file(
            grant,
            seq,
            path,
            sandboxed=True,
            frame_ancestors=PREVIEW_PROXY_FRAME_ANCESTORS,
            if_none_match=if_none_match,
            host=host,
        )
    except ArtifactFileUnavailable as e:
        logger.warning("Artifact file could not be served: %s", e)
        raise HTTPException(status_code=503, detail="File unavailable") from None
    if served is None:
        raise HTTPException(status_code=404, detail="Not found")
    return Response(
        content=served.body, status_code=served.status, headers=dict(served.headers)
    )
