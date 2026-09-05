"""User Management API Router — user profile and preferences endpoints."""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import File, UploadFile
from pydantic import BaseModel
from src.llms.preferences import (
    MOVED_MODEL_KEYS,
    TUNING_FIELDS,
    TuningError,
    validate_tuning,
)
from src.utils.storage import get_public_url, upload_bytes

from src.server.auth.jwt_bearer import get_current_auth_info, AuthInfo
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
from ptc_agent.agent.graph import invalidate_user_profile_cache
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

    Three cases: existing UUID match (backfill auth_provider/timezone), legacy
    email match (migrate PK to UUID), or new user (create with auth_provider).
    ``locale`` is never backfilled here — NULL means "no explicit preference".
    """
    user_id = auth_info.user_id
    auth_provider = auth_info.auth_provider

    # 1. Already exists by UUID?
    existing = await db_get_user(user_id)
    if existing:
        updates = {}

        # Lazy-backfill NULL fields. Deliberately skip `locale` — that column
        # encodes the user's explicit Settings preference. A NULL row means
        # "no preference; use browser locale via the frontend detector".
        if auth_provider and not existing.get("auth_provider"):
            updates["auth_provider"] = auth_provider
        if body.timezone and not existing.get("timezone"):
            updates["timezone"] = body.timezone

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

    # 3. Brand-new user. `locale` is left NULL — only set when the user
    # explicitly picks a language in Settings. The frontend detector handles
    # browser-locale and English-fallback at render time.
    user = await create_user_from_auth(
        user_id=user_id,
        email=body.email,
        name=body.name,
        avatar_url=body.avatar_url,
        auth_provider=auth_provider,
        timezone=body.timezone,
        locale=None,
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
    """Create a new user. Raises 409 if user_id already exists."""
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
    """Get current user profile and preferences.

    Set ``refresh_tier=true`` to bust the cached platform tier (e.g. after
    invitation redemption). Access tier and plan display name are cached 5 min.
    """
    result = await get_user_with_preferences(user_id)

    if not result:
        raise_not_found("User")

    user_response = UserResponse.model_validate(result["user"])

    # Populate platform membership: access tier + plan display name.
    # Both fields share a single Redis cache entry (5 min TTL) so this never
    # costs more than one platform-service round-trip per user per 5 minutes.
    from src.server.dependencies.usage_limits import (
        _fetch_platform_membership,
        platform_membership_cache_key,
    )
    if refresh_tier:
        from src.utils.cache.redis_cache import get_cache_client
        cache = get_cache_client()
        await cache.delete(platform_membership_cache_key(user_id))
    membership = await _fetch_platform_membership(user_id)
    user_response.access_tier = int(membership.get("access_tier", -1))
    user_response.plan_display_name = membership.get("plan_display_name")

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
    """Update current user profile fields (not preferences). Partial update."""
    existing = await db_get_user(user_id)
    if not existing:
        raise_not_found("User")

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

    if not user:
        raise_not_found("User")

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
    """Get user preferences. Raises 404 if user or preferences are not found."""
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    preferences = await db_get_user_preferences(user_id)
    if not preferences:
        raise_not_found("Preferences")

    return UserPreferencesResponse.model_validate(preferences)


def _validate_custom_models(custom_models: list, custom_providers: list | None = None) -> None:
    """Validate custom_models list before persisting. Raises HTTPException 400 on invalid data."""
    from ptc_agent.agent.prompts.guidance import VALID_GUIDANCE

    from src.llms.llm import LLM, CUSTOM_MODEL_NAME_RE
    from src.llms.model_spec import reasoning_block
    from src.llms.reasoning import (
        REASONING_LEVELS,
        ReasoningSurfaceError,
        validate_surface,
    )

    if not isinstance(custom_models, list):
        raise HTTPException(status_code=400, detail="custom_models must be a list")

    # Reuse the process-wide singleton — building a fresh ModelConfig on every
    # preferences PUT re-parses models.json + re-scans _flat_providers for
    # nothing (the manifest is static).
    mc = LLM.get_model_config()
    name_re = re.compile(CUSTOM_MODEL_NAME_RE)
    seen_names: set[str] = set()

    # Shadow semantics: a custom ``name`` MAY collide with a built-in. The
    # resolver checks custom first, so the user's entry wins. This supports
    # "route built-in model X through my variant's key" without inventing a
    # prefix format for preferences.

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

        if not name:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: name is required")
        if not model_id:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: model_id is required")
        if not provider:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: provider is required")

        if not name_re.match(name):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: name '{name}' is invalid (alphanumeric start, max 63 chars, only .-_:/ allowed)",
            )

        if name in seen_names:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: duplicate name '{name}'",
            )
        seen_names.add(name)

        if not isinstance(provider, str) or not provider.strip():
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider must be a non-empty string",
            )

        if provider not in valid_providers:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider '{provider}' is not a known BYOK-eligible or custom provider",
            )

        for field in ("parameters", "extra_body", "reasoning"):
            val = cm.get(field)
            if val is not None and not isinstance(val, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: {field} must be a JSON object",
                )

        # Rejected here rather than at request time: an unknown write path is
        # accepted by the vendor and ignored, so the entry would render an
        # effort control that reports a level and sends nothing.
        if isinstance(cm.get("reasoning"), dict) and cm["reasoning"]:
            try:
                validate_surface(f"custom_models[{idx}]", cm["reasoning"])
            except ReasoningSurfaceError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

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

        # A custom model has no manifest entry, so these are the only place it
        # can declare what it honors. Without them every custom model resolves
        # to the fail-safe guidance level and gets no effort selector at all.
        guidance = cm.get("prompt_guidance")
        if guidance is not None and guidance not in VALID_GUIDANCE:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: prompt_guidance must be one of {sorted(VALID_GUIDANCE)}",
            )

        # Either shape: a `reasoning` block, or the flat keys entries saved
        # before it existed still carry. Whichever it used is written back to,
        # which is why the shapes are read apart here rather than through
        # `reasoning_block`: its normalized view cannot say which key to
        # rewrite. Empty means unused, on the same terms as that function.
        block = cm.get("reasoning")
        declared = block if isinstance(block, dict) and block else cm
        efforts_key = "efforts" if declared is not cm else "reasoning_efforts"
        default_key = "default" if declared is not cm else "reasoning_effort_default"

        efforts = declared.get(efforts_key)
        if efforts is not None:
            if not isinstance(efforts, list) or any(e not in REASONING_LEVELS for e in efforts):
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: {efforts_key} must be a list drawn from {list(REASONING_LEVELS)}",
                )
            # Store in canonical order so the UI renders a ladder, not the
            # order the client happened to send.
            declared[efforts_key] = [lv for lv in REASONING_LEVELS if lv in set(efforts)]

        # An entry shadowing a built-in inherits that model's ladder, so a
        # default may name a level this entry does not list itself.
        effective_efforts = declared.get(efforts_key)
        if effective_efforts is None:
            effective_efforts = reasoning_block(mc.get_model_config(name)).get("efforts") or []
        default_effort = declared.get(default_key)
        if default_effort is not None and default_effort not in effective_efforts:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: {default_key} must be one of {sorted(effective_efforts)}",
            )


def _validate_custom_providers(custom_providers: list) -> None:
    """Validate custom_providers list before persisting."""
    if not isinstance(custom_providers, list):
        raise HTTPException(status_code=400, detail="custom_providers must be a list")

    from src.llms.llm import LLM

    mc = LLM.get_model_config()
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


_VALID_OUTPUT_FORMATS = {"markdown", "html"}

# The keys that moved out of ``other_preference``. Read on input only, to
# re-route a stale client's write; drop with the resolver's legacy read.
LEGACY_MODEL_KEYS = frozenset(MOVED_MODEL_KEYS)

def _tuning_400(exc: TuningError) -> HTTPException:
    """The 400 for a tuning value the stored shape does not allow.

    ``TuningError`` already leads its message with the offending field, so the
    detail does not repeat ``exc.field``.
    """
    return HTTPException(status_code=400, detail=str(exc))


def _effective_model_preference(stored: dict, patch: dict) -> dict:
    """The column as it will look once this patch lands, one level deep.

    A cross-key invariant has to hold on the merged state: a patch that only
    deletes a provider names no model at all, so checking the request body alone
    lets it through and leaves a dangling reference behind it.
    """
    merged = dict(stored)
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    return merged


def _validate_model_tuning(model_pref: dict, effective: dict) -> None:
    """Check tuning on both levels — account-wide and per model. Raises 400.

    The account level is checked against the global vocabulary because it is
    chosen with no model in hand and clamped per model at resolve time; a
    profile is checked against its own model's ladder, which is the whole point
    of the per-model layer.
    """
    from src.llms.model_spec import canonical_reasoning_efforts, reasoning_block
    from src.server.services.llm.user_models import model_entry

    try:
        validate_tuning(model_pref, where="model_preference")

        profiles = model_pref.get("profiles")
        if profiles is None:
            return
        if not isinstance(profiles, dict):
            raise HTTPException(
                status_code=400, detail="profiles must be an object keyed by model name"
            )

        for model, profile in profiles.items():
            if profile is None:  # deletes the model's profile
                continue
            if not isinstance(profile, dict):
                raise HTTPException(
                    status_code=400, detail=f"profiles[{model}] must be an object"
                )
            # Anything outside the tuning set is a typo that would sit in the
            # row forever doing nothing, so it is rejected at the boundary.
            unknown = set(profile) - set(TUNING_FIELDS)
            if unknown:
                raise HTTPException(
                    status_code=400,
                    detail=f"profiles[{model}]: unknown settings {sorted(unknown)}; "
                           f"allowed: {sorted(TUNING_FIELDS)}",
                )
            # One entry, resolved the way the resolver resolves it: a custom
            # model shadows a built-in of the same name, so reading the
            # manifest first would check a shadowed ladder the turn never runs.
            entry = model_entry(effective, model) or {}
            efforts = list(canonical_reasoning_efforts(reasoning_block(entry).get("efforts")))
            validate_tuning(profile, where=f"profiles[{model}]", reasoning_efforts=efforts)
    except TuningError as exc:
        raise _tuning_400(exc) from exc


def _clear_unhonored_efforts(
    model_pref: dict, effective: dict, stored_profiles: dict | None
) -> None:
    """Drop stored efforts the changed catalog no longer offers, in this write.

    ``custom_models`` is the one thing that can narrow a ladder under a profile
    the patch never mentions, and nothing else revisits those, so the level
    would sit in Settings naming a step no turn runs.
    """
    from src.llms.model_spec import canonical_reasoning_efforts, reasoning_block
    from src.server.services.llm.user_models import model_entry

    # Merged by hand: ``_effective_model_preference`` replaces ``profiles``
    # wholesale, so a patch carrying its own would hide the stored ones this
    # exists to reach.
    merged = {**(stored_profiles or {}), **(model_pref.get("profiles") or {})}
    for model, profile in merged.items():
        if not isinstance(profile, dict):
            continue  # already a delete
        effort = profile.get("reasoning_effort")
        if effort is None:
            continue
        entry = model_entry(effective, model) or {}
        if effort in canonical_reasoning_efforts(reasoning_block(entry).get("efforts")):
            continue
        patch = model_pref.setdefault("profiles", {})
        patch[model] = {**(patch.get(model) or {}), "reasoning_effort": None}


def _validate_agent_preference(agent_pref: dict) -> None:
    """Validate agent_preference before persisting. Raises HTTPException 400 on invalid data."""
    # output_format may be absent or None (delete/default); else must be a
    # known format. Not a Literal on the model so None survives the JSONB merge
    # as a key deletion rather than being stripped at parse time.
    if "output_format" in agent_pref:
        of = agent_pref["output_format"]
        if of is not None and (not isinstance(of, str) or of not in _VALID_OUTPUT_FORMATS):
            raise HTTPException(
                status_code=400,
                detail=f"output_format must be one of {sorted(_VALID_OUTPUT_FORMATS)}",
            )


@router.put("/users/me/preferences", response_model=UserPreferencesResponse)
@handle_api_exceptions("update preferences", logger)
async def update_preferences(
    request: UserPreferencesUpdate,
    user_id: CurrentUserId,
):
    """Update user preferences (partial, JSONB merge). Raises 404 if user not found."""
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    # Convert Pydantic models to dicts for JSONB storage.
    # Use exclude_unset=True (not exclude_none=True) so explicitly-sent null
    # values are preserved — the DB layer reads None as a key deletion.
    risk_pref = request.risk_preference.model_dump(exclude_unset=True) if request.risk_preference else None
    investment_pref = request.investment_preference.model_dump(exclude_unset=True) if request.investment_preference else None
    agent_pref = request.agent_preference.model_dump(exclude_unset=True) if request.agent_preference else None
    other_pref = request.other_preference.model_dump(exclude_unset=True) if request.other_preference else None
    model_pref = request.model_preference.model_dump(exclude_unset=True) if request.model_preference else None

    # The model keys live in their own column now. A client still
    # sending them under other_preference is honored for one release so an old
    # tab does not silently drop the user's settings on the floor.
    if other_pref:
        legacy = {k: other_pref.pop(k) for k in list(other_pref) if k in LEGACY_MODEL_KEYS}
        if legacy:
            model_pref = {**legacy, **(model_pref or {})}

    if model_pref:
        stored = await db_get_user_preferences(user_id)
        stored_model_pref = (stored or {}).get("model_preference") or {}
        effective = _effective_model_preference(stored_model_pref, model_pref)

        # Providers before models: a model names the provider it runs on.
        if model_pref.get("custom_providers") is not None:
            _validate_custom_providers(model_pref["custom_providers"])

        # Checked against the merged catalog rather than the request, because a
        # patch that only drops a provider names no model and would otherwise
        # skip this. ``effective`` holds the request's own list by reference, so
        # the normalization this does still reaches the row.
        if "custom_models" in model_pref or "custom_providers" in model_pref:
            _validate_custom_models(
                effective.get("custom_models") or [],
                effective.get("custom_providers") or [],
            )

        _validate_model_tuning(model_pref, effective)

        # After validation, so a bad level in the patch still gets its 400 and
        # only levels already in the row are cleared.
        if "custom_models" in model_pref:
            _clear_unhonored_efforts(
                model_pref, effective, stored_model_pref.get("profiles")
            )

    # Validate search_provider / search_depth if present (None = key deletion,
    # allowed). Shape validation only — tier gating happens at resolve time.
    if other_pref and (
        other_pref.get("search_provider") is not None
        or other_pref.get("search_depth") is not None
    ):
        from src.tools.web.manifest import CAPABILITY_SEARCH, providers_with_capability

        providers = providers_with_capability(CAPABILITY_SEARCH)

        sp = other_pref.get("search_provider")
        if sp is not None and (not isinstance(sp, str) or sp not in providers):
            raise HTTPException(
                status_code=400,
                detail=f"search_provider must be one of {sorted(providers)}",
            )

        sd = other_pref.get("search_depth")
        if sd is not None:
            # Depth names are provider-scoped: validate against the provider
            # in this payload, or any provider when none is being set (the
            # effective provider isn't known at write time).
            if isinstance(sp, str) and sp in providers:
                valid_depths = {
                    lv.name for lv in providers[sp].capability(CAPABILITY_SEARCH).levels
                }
            else:
                valid_depths = {
                    lv.name
                    for spec in providers.values()
                    for lv in spec.capability(CAPABILITY_SEARCH).levels
                }
            if not isinstance(sd, str) or sd not in valid_depths:
                raise HTTPException(
                    status_code=400,
                    detail=f"search_depth must be one of {sorted(valid_depths)}",
                )

    # Validate agent_preference (output_format shape). None = key deletion.
    if agent_pref:
        _validate_agent_preference(agent_pref)

    try:
        preferences = await upsert_user_preferences(
            user_id=user_id,
            risk_preference=risk_pref,
            investment_preference=investment_pref,
            agent_preference=agent_pref,
            other_preference=other_pref,
            model_preference=model_pref,
        )
    except TuningError as exc:
        raise _tuning_400(exc) from exc

    await invalidate_user_prefs_cache(user_id)
    await invalidate_user_profile_cache(user_id)

    await maybe_complete_onboarding(user_id)

    logger.info(f"Updated preferences for user {user_id}")
    return UserPreferencesResponse.model_validate(preferences)

@router.delete("/users/me/preferences", status_code=200)
@handle_api_exceptions("delete preferences", logger)
async def delete_preferences(user_id: CurrentUserId):
    """Delete all user preferences and reset onboarding_completed to false."""
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    await db_delete_user_preferences(user_id)
    await invalidate_user_prefs_cache(user_id)
    await invalidate_user_profile_cache(user_id)
    await db_update_user(user_id=user_id, onboarding_completed=False)

    logger.info(f"Cleared preferences and reset onboarding for user {user_id}")
    return {"success": True, "message": "Preferences cleared"}


@router.post("/users/me/avatar", response_model=dict)
@handle_api_exceptions("upload avatar", logger)
async def upload_avatar(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
):
    """Upload user avatar to R2 storage and update avatar_url. Returns ``{"avatar_url": "..."}``."""
    user = await db_get_user(user_id)
    if not user:
        raise_not_found("User")

    # Validate file type
    allowed_types = {"image/jpeg", "image/png", "image/gif", "image/webp"}
    if file.content_type not in allowed_types:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid file type: {file.content_type}")

    content = await file.read()

    # Generate R2 key: avatars/{user_id}.{ext}
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "png"
    key = f"avatars/{user_id}.{ext}"

    # upload_bytes is a synchronous boto3 call; offload it so it doesn't block
    # the event loop during the network round-trip to object storage.
    success = await asyncio.to_thread(upload_bytes, key, content, content_type=file.content_type)
    if not success:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail="Failed to upload avatar")

    avatar_url = get_public_url(key)
    await db_update_user(user_id=user_id, avatar_url=avatar_url)

    logger.info(f"Uploaded avatar for user {user_id}: {avatar_url}")
    return {"avatar_url": avatar_url}