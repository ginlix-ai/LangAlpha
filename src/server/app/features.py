"""User-facing feature flags: discovery and per-user opt-in/out.

The catalog and resolution semantics live in ``src/config/features.py``;
this router only exposes the resolved per-user state and validates writes.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.server.services.features import (
    FeatureNotOverridableError,
    FeatureState,
    UnknownFeatureError,
    list_user_features,
    set_feature_override,
)
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1", tags=["features"])


class FeatureListResponse(BaseModel):
    features: list[FeatureState]


class FeatureOverrideRequest(BaseModel):
    enabled: bool | None = None  # None (or omitted) clears the override


@router.get("/features", response_model=FeatureListResponse)
async def get_features(x_user_id: CurrentUserId):
    """Resolved feature flags for the current user."""
    return FeatureListResponse(features=await list_user_features(x_user_id))


@router.put("/features/{key}", response_model=FeatureListResponse)
async def put_feature_override(
    key: str, request: FeatureOverrideRequest, x_user_id: CurrentUserId
):
    """Set or clear the current user's override for one feature."""
    try:
        features = await set_feature_override(x_user_id, key, request.enabled)
    except UnknownFeatureError:
        raise HTTPException(status_code=404, detail=f"Unknown feature: {key}")
    except FeatureNotOverridableError:
        raise HTTPException(
            status_code=403,
            detail=f"Feature gate does not accept user overrides: {key}",
        )
    return FeatureListResponse(features=features)
