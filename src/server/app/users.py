"""
User Management API Router.

Provides REST endpoints for user profile and preferences management.

Endpoints:
- POST /api/v1/auth/sync - Sync Supabase user to backend (create/migrate)
- POST /api/v1/users - Create new user
- GET /api/v1/users/me - Get current user (by Bearer token)
- PUT /api/v1/users/me - Update current user profile
- GET /api/v1/users/me/preferences - Get user preferences
- PUT /api/v1/users/me/preferences - Update user preferences
"""

import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import File, UploadFile
from pydantic import BaseModel
from src.utils.storage import get_public_url, upload_bytes

from src.server.auth.jwt_bearer import get_current_auth_info, AuthInfo
from src.server.services.workspace_manager import WorkspaceManager
from src.server.database.user import (
    create_user as db_create_user,
    create_user_from_auth,
    delete_user_preferences as db_delete_user_preferences,
    find_user_by_email,
    get_user as db_get_user,
    get_user_preferences as db_get_user_preferences,
    get_user_with_preferences,
    invalidate_user_prefs_cache,
    migrate_user_id,
    update_user as db_update_user,
    upsert_user_preferences,
)
from src.ptc_agent.agent.graph import invalidate_user_profile_cache
from src.server.services.onboarding import maybe_complete_onboarding
from src.server.models.user import (
    UserBase,
    UserPreferencesResponse,
    UserPreferencesUpdate,
    UserResponse,
    UserUpdate,
    UserWithPreferencesResponse,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions, raise_not_found

logger = logging.getLogger(__name__)

_VALID_MODALITIES = frozenset({"text", "image", "pdf"})

router = APIRouter(prefix="/api/v1", tags=["Users"])


# ==================== Auth Sync ====================


class AuthSyncRequest(BaseModel):
    email: Optional[str] = None
    name: Optional[str] = None
    avatar_url: Optional[str] = None
    timezone: Optional[str] = None
    locale: Optional[str] = None


@router.post("/auth/sync", response_model=UserWithPreferencesResponse)
@handle_api_exceptions("sync user", logger)
async def sync_user(
    body: AuthSyncRequest,
    auth_info: AuthInfo = Depends(get_current_auth_info),
):
    """
    Sync Supabase user to backend after OAuth/email login.

    Called by frontend immediately after Supabase auth succeeds.
    Uses ``get_current_auth_info`` to extract both ``user_id`` and
    ``auth_provider`` from the JWT so the provider can be persisted.

    Logic:
      1. user_id already exists -> lazy-backfill auth_provider if NULL, return profile
      2. email matches a legacy user -> migrate PK to UUID, return profile
      3. No match -> create new user with auth_provider, return profile
    """
    user_id = auth_info.user_id
    auth_provider = auth_info.auth_provider

    # 1. Already exists by UUID?
    existing = await db_get_user(user_id)
    if existing:
        updates = {}

        # Lazy-backfill NULL fields
        if auth_provider and not existing.get("auth_provider"):
            updates["auth_provider"] = auth_provider
        if body.timezone and not existing.get("timezone"):
            updates["timezone"] = body.timezone
        if body.locale and not existing.get("locale"):
            updates["locale"] = body.locale

        # Throttle last_login_at writes — only update if stale (>1 hour)
        last_login = existing.get("last_login_at")
        now = datetime.now(tz=last_login.tzinfo if last_login else None)
        if not last_login or (now - last_login).total_seconds() > 3600:
            updates["last_login_at"] = now

        if updates:
            await db_update_user(user_id=user_id, **updates)

        result = await get_user_with_preferences(user_id)
        if not result:
            raise_not_found("User")
        user_resp = UserResponse.model_validate(result["user"])
        pref_resp = None
        if result.get("preferences"):
            pref_resp = UserPreferencesResponse.model_validate(result["preferences"])
        return UserWithPreferencesResponse(user=user_resp, preferences=pref_resp)

    # 2. Legacy email-based user?
    if body.email:
        legacy = await find_user_by_email(body.email)
        if legacy:
            migrated = await migrate_user_id(legacy["user_id"], user_id)
            if migrated:
                logger.info(f"Migrated legacy user {legacy['user_id']} -> {user_id}")
                result = await get_user_with_preferences(user_id)
                if not result:
                    raise_not_found("User")
                user_resp = UserResponse.model_validate(result["user"])
                pref_resp = None
                if result.get("preferences"):
                    pref_resp = UserPreferencesResponse.model_validate(result["preferences"])
                return UserWithPreferencesResponse(user=user_resp, preferences=pref_resp)

    # 3. Brand-new user
    user = await create_user_from_auth(
        user_id=user_id,
        email=body.email,
        name=body.name,
        avatar_url=body.avatar_url,
        auth_provider=auth_provider,
        timezone=body.timezone,
        locale=body.locale,
    )
    user_resp = UserResponse.model_validate(user)
    return UserWithPreferencesResponse(user=user_resp, preferences=None)


# ==================== User CRUD ====================


@router.post("/users", response_model=UserResponse, status_code=201)
@handle_api_exceptions("create user", logger, conflict_on_value_error=True)
async def create_user(
    request: UserBase,
    user_id: CurrentUserId,
):
    """
    Create a new user.

    Called on first authentication to register the user in the system.

    Args:
        request: User creation data (email, name, etc.)
        user_id: User ID from authentication header

    Returns:
        Created user details

    Raises:
        409: User already exists
    """
    user = await db_create_user(
        user_id=user_id,
        email=request.email,
        name=request.name,
        avatar_url=request.avatar_url,
        timezone=request.timezone,
        locale=request.locale,
    )

    logger.info(f"Created user {user_id}")
    return UserResponse.model_validate(user)


@router.get("/users/me", response_model=UserWithPreferencesResponse)
@handle_api_exceptions("get user", logger)
async def get_current_user(
    user_id: CurrentUserId,
    refresh_tier: bool = Query(False, description="Bust cached platform tier (use after invitation redemption)"),
):
    """
    Get current user profile and preferences.

    Returns the user profile along with their preferences in a single response.

    Args:
        user_id: User ID from authentication header
        refresh_tier: When true, deletes cached platform tier before fetching

    Returns:
        User profile and preferences

    Raises:
        404: User not found
    """
    result = await get_user_with_preferences(user_id)

    if not result:
        raise_not_found("User")

    user_response = UserResponse.model_validate(result["user"])

    # Populate platform access tier (cached, SaaS mode only)
    from src.server.dependencies.usage_limits import _fetch_platform_tier, platform_tier_cache_key
    if refresh_tier:
        from src.utils.cache.redis_cache import get_cache_client
        cache = get_cache_client()
        await cache.delete(platform_tier_cache_key(user_id))
    user_response.access_tier = await _fetch_platform_tier(user_id)

    preferences_response = None
    if result["preferences"]:
        preferences_response = UserPreferencesResponse.model_validate(result["preferences"])

    return UserWithPreferencesResponse(
        user=user_response,
        preferences=preferences_response,
    )


@router.put("/users/me", response_model=UserWithPreferencesResponse)
@handle_api_exceptions("update user", logger)
async def update_current_user(
    request: UserUpdate,
    user_id: CurrentUserId,
):
    """
    Update current user profile.

    Updates user profile fields (not preferences). Only provided fields are updated.

    Args:
        request: Fields to update
        user_id: User ID from authentication header

    Returns:
        Updated user profile and preferences

    Raises:
        404: User not found
    """
    # Check user exists
    existing = await db_get_user(user_id)
    if not existing:
        raise_not_found("User")

    # Update user
    user = await db_update_user(
        user_id=user_id,
        email=request.email,
        name=request.name,
        avatar_url=request.avatar_url,
        timezone=request.timezone,
        locale=request.locale,
        onboarding_completed=request.onboarding_completed,
        personalization_completed=request.personalization_completed,
    )

    await invalidate_user_profile_cache(user_id)
    WorkspaceManager.mark_user_data_stale(user_id)

    if not user:
        raise_not_found("User")

    # Get preferences for combined response
    preferences = await db_get_user_preferences(user_id)

    user_response = UserResponse.model_validate(user)
    preferences_response = None
    if preferences:
        preferences_response = UserPreferencesResponse.model_validate(preferences)

    logger.info(f"Updated user {user_id}")
    return UserWithPreferencesResponse(
        user=user_response,
        preferences=preferences_response,
    )


@router.get("/users/me/preferences", response_model=UserPreferencesResponse)
@handle_api_exceptions("get preferences", logger)
async def get_preferences(user_id: CurrentUserId):
    """
    Get user preferences only.

    Args:
        user_id: User ID from authentication header

    Returns:
        User preferences

    Raises:
        404: User or preferences not found
    """
    # Verify user exists
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    preferences = await db_get_user_preferences(user_id)
    if not preferences:
        raise_not_found("Preferences")

    return UserPreferencesResponse.model_validate(preferences)


def _validate_custom_models(custom_models: list, custom_providers: list | None = None) -> None:
    """Validate custom_models list before persisting.

    Raises HTTPException 400 on invalid data.
    """
    from src.llms.llm import ModelConfig, CUSTOM_MODEL_NAME_RE

    if not isinstance(custom_models, list):
        raise HTTPException(status_code=400, detail="custom_models must be a list")

    mc = ModelConfig()
    name_re = re.compile(CUSTOM_MODEL_NAME_RE)
    seen_names: set[str] = set()

    # Build valid provider set: all known flat providers + custom providers
    valid_providers = {
        k for k, v in mc.flat_providers.items()
        if not v.get("platform")
    }
    if custom_providers:
        valid_providers.update(
            cp["name"] for cp in custom_providers if isinstance(cp, dict) and cp.get("name")
        )

    for idx, cm in enumerate(custom_models):
        if not isinstance(cm, dict):
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: must be an object")

        name = cm.get("name")
        model_id = cm.get("model_id")
        provider = cm.get("provider")

        # Required fields
        if not name:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: name is required")
        if not model_id:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: model_id is required")
        if not provider:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: provider is required")

        # Name format
        if not name_re.match(name):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: name '{name}' is invalid (alphanumeric start, max 63 chars, only .-_:/ allowed)",
            )

        # No collision with system models
        if mc.get_model_config(name):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: name '{name}' conflicts with a system model",
            )

        # No duplicate names
        if name in seen_names:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: duplicate name '{name}'",
            )
        seen_names.add(name)

        # Provider format: must be non-empty string
        if not isinstance(provider, str) or not provider.strip():
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider must be a non-empty string",
            )

        # Provider must reference a known BYOK-eligible or custom provider
        if provider not in valid_providers:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider '{provider}' is not a known BYOK-eligible or custom provider",
            )

        # Validate JSON fields are dicts if present
        for field in ("parameters", "extra_body"):
            val = cm.get(field)
            if val is not None and not isinstance(val, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: {field} must be a JSON object",
                )

        # Validate input_modalities if present
        modalities = cm.get("input_modalities")
        if modalities is not None:
            if not isinstance(modalities, list) or len(modalities) == 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: input_modalities must be a non-empty list",
                )
            for m in modalities:
                if not isinstance(m, str) or m not in _VALID_MODALITIES:
                    raise HTTPException(
                        status_code=400,
                        detail=f"custom_models[{idx}]: invalid modality '{m}', allowed: {sorted(_VALID_MODALITIES)}",
                    )
            # Ensure "text" is always present
            if "text" not in modalities:
                cm["input_modalities"] = ["text"] + modalities


def _validate_custom_providers(custom_providers: list) -> None:
    """Validate custom_providers list before persisting."""
    if not isinstance(custom_providers, list):
        raise HTTPException(status_code=400, detail="custom_providers must be a list")

    from src.llms.llm import ModelConfig

    mc = ModelConfig()
    builtin = set(mc.get_byok_eligible_providers())
    name_re = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")
    seen: set[str] = set()

    for idx, cp in enumerate(custom_providers):
        if not isinstance(cp, dict):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: must be an object")

        name = cp.get("name")
        parent = cp.get("parent_provider")

        if not name:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: name is required")
        if not parent:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: parent_provider is required")
        if not name_re.match(name):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: invalid name '{name}'")
        if name in builtin:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: name '{name}' conflicts with built-in provider")
        if name in seen:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: duplicate name '{name}'")
        if parent not in builtin:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: parent_provider '{parent}' is not a BYOK-eligible provider")
        seen.add(name)

        ura = cp.get("use_response_api")
        if ura is not None and not isinstance(ura, bool):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: use_response_api must be a boolean")



@router.put("/users/me/preferences", response_model=UserPreferencesResponse)
@handle_api_exceptions("update preferences", logger)
async def update_preferences(
    request: UserPreferencesUpdate,
    user_id: CurrentUserId,
):
    """
    Update user preferences.

    Partial update supported - only provided fields are updated.
    JSONB fields are merged with existing values.

    Args:
        request: Preferences to update
        user_id: User ID from authentication header

    Returns:
        Updated preferences

    Raises:
        404: User not found
    """
    # Verify user exists
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    # Convert Pydantic models to dicts for JSONB storage.
    # Use exclude_unset=True (not exclude_none=True) so explicitly-sent null
    # values are preserved — _split_updates_and_deletes uses None to signal
    # key deletion from the JSONB column.
    risk_pref = request.risk_preference.model_dump(exclude_unset=True) if request.risk_preference else None
    investment_pref = request.investment_preference.model_dump(exclude_unset=True) if request.investment_preference else None
    agent_pref = request.agent_preference.model_dump(exclude_unset=True) if request.agent_preference else None
    other_pref = request.other_preference.model_dump(exclude_unset=True) if request.other_preference else None

    # Validate custom_providers BEFORE custom_models (models may reference providers)
    if other_pref and "custom_providers" in other_pref:
        custom_providers = other_pref["custom_providers"]
        if custom_providers is not None:
            _validate_custom_providers(custom_providers)

    # Validate custom_models if present in other_preference
    if other_pref and "custom_models" in other_pref:
        custom_models = other_pref["custom_models"]
        if custom_models is not None:
            # Resolve custom_providers for validation:
            # - If in this request → use them (even if empty/null → means being deleted)
            # - Otherwise → load existing from DB
            if "custom_providers" in other_pref:
                cp_for_validation = other_pref.get("custom_providers") or []
            else:
                existing = await db_get_user_preferences(user_id)
                cp_for_validation = (existing or {}).get("other_preference", {}).get("custom_providers") or []
            _validate_custom_models(custom_models, cp_for_validation)

    preferences = await upsert_user_preferences(
        user_id=user_id,
        risk_preference=risk_pref,
        investment_preference=investment_pref,
        agent_preference=agent_pref,
        other_preference=other_pref,
    )

    await invalidate_user_prefs_cache(user_id)
    await invalidate_user_profile_cache(user_id)
    WorkspaceManager.mark_user_data_stale(user_id)

    await maybe_complete_onboarding(user_id)

    logger.info(f"Updated preferences for user {user_id}")
    return UserPreferencesResponse.model_validate(preferences)

@router.delete("/users/me/preferences", status_code=200)
@handle_api_exceptions("delete preferences", logger)
async def delete_preferences(user_id: CurrentUserId):
    """
    Delete all user preferences (reset to blank).

    Used by the "Reset & Re-onboard" flow to clear all preference data
    and reset onboarding_completed to false.

    Args:
        user_id: User ID from authentication header

    Returns:
        Confirmation message
    """
    # Verify user exists
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    await db_delete_user_preferences(user_id)
    await invalidate_user_prefs_cache(user_id)
    await invalidate_user_profile_cache(user_id)
    WorkspaceManager.mark_user_data_stale(user_id)
    await db_update_user(user_id=user_id, onboarding_completed=False)

    logger.info(f"Cleared preferences and reset onboarding for user {user_id}")
    return {"success": True, "message": "Preferences cleared"}


@router.post("/users/me/avatar", response_model=dict)
@handle_api_exceptions("upload avatar", logger)
async def upload_avatar(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
):
    """
    Upload user avatar image.

    Accepts image file, uploads to R2 storage, and updates user's avatar_url.

    Args:
        user_id: User ID from authentication header
        file: Image file to upload

    Returns:
        {"avatar_url": "https://..."}

    Raises:
        400: Invalid file type or upload failed
        404: User not found
    """
    # Verify user exists
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    # Validate file type
    allowed_types = {"image/jpeg", "image/png", "image/gif", "image/webp"}
    if file.content_type not in allowed_types:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid file type: {file.content_type}")

    # Read file content
    content = await file.read()

    # Generate R2 key: avatars/{user_id}.{ext}
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "png"
    key = f"avatars/{user_id}.{ext}"

    # Upload to R2
    success = upload_bytes(key, content, content_type=file.content_type)
    if not success:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail="Failed to upload avatar")

    # Get public URL
    avatar_url = get_public_url(key)

    # Update user's avatar_url
    await db_update_user(user_id=user_id, avatar_url=avatar_url)

    logger.info(f"Uploaded avatar for user {user_id}: {avatar_url}")
    return {"avatar_url": avatar_url}