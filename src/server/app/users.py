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
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi import File, UploadFile
from pydantic import BaseModel
from src.ptc_agent.utils.storage.r2_uploader import upload_bytes, get_public_url

from src.server.auth.jwt_bearer import verify_jwt_token
from src.server.database.user import (
    create_user as db_create_user,
    create_user_from_auth,
    find_user_by_email,
    get_user as db_get_user,
    get_user_preferences as db_get_user_preferences,
    get_user_with_preferences,
    migrate_user_id,
    update_user as db_update_user,
    upsert_user_preferences,
)
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

router = APIRouter(prefix="/api/v1", tags=["Users"])

# ==================== Auth Sync ====================


class AuthSyncRequest(BaseModel):
    email: Optional[str] = None
    name: Optional[str] = None
    avatar_url: Optional[str] = None


@router.post("/auth/sync", response_model=UserWithPreferencesResponse)
@handle_api_exceptions("sync user", logger)
async def sync_user(
    body: AuthSyncRequest,
    user_id: str = Depends(verify_jwt_token),
):
    """
    Sync Supabase user to backend after OAuth/email login.

    Called by frontend immediately after Supabase auth succeeds.
    Uses its own JWT extraction (does not use CurrentUserId) so it
    can handle first-time users who don't yet exist in the DB.

    Logic:
      1. user_id already exists -> return profile
      2. email matches a legacy user -> migrate PK to UUID, return profile
      3. No match -> create new user, return profile
    """

    # 1. Already exists by UUID?
    existing = await db_get_user(user_id)
    if existing:
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
async def get_current_user(user_id: CurrentUserId):
    """
    Get current user profile and preferences.

    Returns the user profile along with their preferences in a single response.

    Args:
        user_id: User ID from authentication header

    Returns:
        User profile and preferences

    Raises:
        404: User not found
    """
    result = await get_user_with_preferences(user_id)

    if not result:
        raise_not_found("User")

    user_response = UserResponse.model_validate(result["user"])
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
    )

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

    # Convert Pydantic models to dicts for JSONB storage
    risk_pref = request.risk_preference.model_dump(exclude_none=True) if request.risk_preference else None
    investment_pref = request.investment_preference.model_dump(exclude_none=True) if request.investment_preference else None
    agent_pref = request.agent_preference.model_dump(exclude_none=True) if request.agent_preference else None
    other_pref = request.other_preference.model_dump(exclude_none=True) if request.other_preference else None

    preferences = await upsert_user_preferences(
        user_id=user_id,
        risk_preference=risk_pref,
        investment_preference=investment_pref,
        agent_preference=agent_pref,
        other_preference=other_pref,
    )

    await maybe_complete_onboarding(user_id)

    logger.info(f"Updated preferences for user {user_id}")
    return UserPreferencesResponse.model_validate(preferences)

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