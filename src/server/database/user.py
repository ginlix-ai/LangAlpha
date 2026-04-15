"""
Database utility functions for user management.

Provides functions for creating, retrieving, and managing users and
user preferences in PostgreSQL.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.conversation import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)

# Computed columns appended to user SELECT queries for gate logic.
def _gate_cols(alias: str = "users") -> str:
    """Return the has_api_key + has_oauth_token EXISTS subqueries for a given table alias."""
    return (
        f"EXISTS (SELECT 1 FROM user_api_keys WHERE user_api_keys.user_id = {alias}.user_id) AS has_api_key,\n"
        f"                    EXISTS (SELECT 1 FROM user_oauth_tokens WHERE user_oauth_tokens.user_id = {alias}.user_id) AS has_oauth_token"
    )

_HAS_API_KEY = "EXISTS (SELECT 1 FROM user_api_keys WHERE user_api_keys.user_id = users.user_id) AS has_api_key"
_HAS_OAUTH = "EXISTS (SELECT 1 FROM user_oauth_tokens WHERE user_oauth_tokens.user_id = users.user_id) AS has_oauth_token"


# ==================== User Operations ====================


async def create_user(
    user_id: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    timezone: Optional[str] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new user.

    Args:
        user_id: External auth ID (e.g., from Clerk, Auth0)
        email: User email
        name: User display name
        avatar_url: URL to user avatar
        timezone: User timezone (e.g., 'America/New_York')
        locale: User locale (e.g., 'en-US')

    Returns:
        Created user dict

    Raises:
        ValueError: If user already exists
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check if user already exists
            await cur.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user_id,)
            )
            existing = await cur.fetchone()
            if existing:
                raise ValueError(f"User {user_id} already exists")

            # Insert new user
            await cur.execute("""
                INSERT INTO users (
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, FALSE, NOW(), NOW())
                RETURNING
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
            """, (user_id, email, name, avatar_url, timezone, locale))

            result = await cur.fetchone()

            # Ensure a preferences row exists so the user can configure
            # models/BYOK without completing onboarding first.
            await cur.execute("""
                INSERT INTO user_preferences (user_preference_id, user_id, created_at, updated_at)
                VALUES (gen_random_uuid(), %s, NOW(), NOW())
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))

            logger.info(f"[user_db] create_user user_id={user_id}")
            return dict(result)


async def find_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Find a user by email address (for legacy migration lookup)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
                FROM users
                WHERE email = %s
                LIMIT 1
            """, (email,))
            result = await cur.fetchone()
            return dict(result) if result else None


async def migrate_user_id(old_user_id: str, new_user_id: str) -> Optional[Dict[str, Any]]:
    """Update a user's PK from old_user_id to new_user_id.

    Requires ON UPDATE CASCADE on all FK constraints so child tables
    (workspaces, watchlists, etc.) update automatically.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                UPDATE users SET user_id = %s, updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
            """, (new_user_id, old_user_id))
            result = await cur.fetchone()
            if result:
                logger.info(f"[user_db] migrate_user_id {old_user_id} -> {new_user_id}")
            return dict(result) if result else None


async def create_user_from_auth(
    user_id: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    auth_provider: Optional[str] = None,
    timezone: Optional[str] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new user from Supabase auth data.

    Uses ON CONFLICT DO UPDATE so it's idempotent — if the user already
    exists it just refreshes their profile fields.  ``auth_provider``,
    ``timezone``, and ``locale`` are only written when the existing value
    is NULL (lazy backfill on next login).
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                INSERT INTO users (
                    user_id, email, name, avatar_url, auth_provider,
                    timezone, locale,
                    onboarding_completed, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    email = COALESCE(EXCLUDED.email, users.email),
                    name = COALESCE(EXCLUDED.name, users.name),
                    avatar_url = COALESCE(EXCLUDED.avatar_url, users.avatar_url),
                    auth_provider = COALESCE(users.auth_provider, EXCLUDED.auth_provider),
                    timezone = COALESCE(EXCLUDED.timezone, users.timezone),
                    locale = COALESCE(EXCLUDED.locale, users.locale),
                    updated_at = NOW()
                RETURNING
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
            """, (user_id, email, name, avatar_url, auth_provider, timezone, locale))
            result = await cur.fetchone()

            # Ensure a preferences row exists so the user can configure
            # models/BYOK without completing onboarding first.
            await cur.execute("""
                INSERT INTO user_preferences (user_preference_id, user_id, created_at, updated_at)
                VALUES (gen_random_uuid(), %s, NOW(), NOW())
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))

            logger.info(f"[user_db] create_user_from_auth user_id={user_id}")
            return dict(result)


async def get_user(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user by ID.

    Args:
        user_id: User ID

    Returns:
        User dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
                FROM users
                WHERE user_id = %s
            """, (user_id,))

            result = await cur.fetchone()
            return dict(result) if result else None


async def update_user(
    user_id: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    timezone: Optional[str] = None,
    locale: Optional[str] = None,
    onboarding_completed: Optional[bool] = None,
    personalization_completed: Optional[bool] = None,
    last_login_at: Optional[datetime] = None,
    auth_provider: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update user profile fields.

    Only updates fields that are provided (not None).

    Args:
        user_id: User ID
        email: New email
        name: New name
        avatar_url: New avatar URL
        timezone: New timezone
        locale: New locale
        onboarding_completed: New onboarding status
        personalization_completed: New personalization status
        last_login_at: New last login timestamp
        auth_provider: Authentication provider (e.g. google, github, email)

    Returns:
        Updated user dict or None if user not found
    """
    builder = UpdateQueryBuilder()
    builder.add_field("email", email)
    builder.add_field("name", name)
    builder.add_field("avatar_url", avatar_url)
    builder.add_field("timezone", timezone)
    builder.add_field("locale", locale)
    builder.add_field("onboarding_completed", onboarding_completed)
    builder.add_field("personalization_completed", personalization_completed)
    builder.add_field("last_login_at", last_login_at)
    builder.add_field("auth_provider", auth_provider)

    if not builder.has_updates():
        return await get_user(user_id)

    returning_columns = [
        "user_id", "email", "name", "avatar_url", "timezone", "locale",
        "onboarding_completed",
        "COALESCE(personalization_completed, FALSE) AS personalization_completed",
        "auth_provider",
        "created_at", "updated_at", "last_login_at",
        _HAS_API_KEY,
        _HAS_OAUTH,
    ]

    query, params = builder.build(
        table="users",
        where_clause="user_id = %s",
        where_params=[user_id],
        returning_columns=returning_columns,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)

            result = await cur.fetchone()
            if result:
                logger.info(f"[user_db] update_user user_id={user_id}")
            return dict(result) if result else None


async def upsert_user(
    user_id: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    timezone: Optional[str] = None,
    locale: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create or update user (upsert).

    If user exists, updates their profile. If not, creates a new user.

    Args:
        user_id: External auth ID
        email: User email
        name: User display name
        avatar_url: URL to user avatar
        timezone: User timezone
        locale: User locale

    Returns:
        User dict (created or updated)
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                INSERT INTO users (
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, FALSE, NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    email = COALESCE(EXCLUDED.email, users.email),
                    name = COALESCE(EXCLUDED.name, users.name),
                    avatar_url = COALESCE(EXCLUDED.avatar_url, users.avatar_url),
                    timezone = COALESCE(EXCLUDED.timezone, users.timezone),
                    locale = COALESCE(EXCLUDED.locale, users.locale),
                    updated_at = NOW()
                RETURNING
                    user_id, email, name, avatar_url, timezone, locale,
                    onboarding_completed,
                    COALESCE(personalization_completed, FALSE) AS personalization_completed,
                    auth_provider,
                    created_at, updated_at, last_login_at,
                    """ + _HAS_API_KEY + """,
                    """ + _HAS_OAUTH + """
            """, (user_id, email, name, avatar_url, timezone, locale))

            result = await cur.fetchone()
            logger.info(f"[user_db] upsert_user user_id={user_id}")
            return dict(result)


# ==================== User Preferences Operations ====================


_USER_PREFS_TTL = 86400  # 24h — freshness via explicit invalidation


async def get_user_preferences(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user preferences (cached in Redis).

    Result is cached for up to ``_USER_PREFS_TTL`` seconds.  The cache is
    explicitly invalidated by ``invalidate_user_prefs_cache`` whenever
    preferences are written or deleted.

    Args:
        user_id: User ID

    Returns:
        Preferences dict or None if not found
    """
    import json as _json
    from src.utils.cache.redis_cache import get_cache_client

    cache_key = f"user_prefs:{user_id}"
    cache = get_cache_client()
    if cache.enabled and cache.client:
        try:
            cached = await cache.client.get(cache_key)
            if cached is not None:
                return _json.loads(cached) if cached != b"null" else None
        except Exception:
            pass  # Redis down — fall through to DB

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    user_preference_id, user_id,
                    risk_preference, investment_preference,
                    agent_preference, other_preference,
                    created_at, updated_at
                FROM user_preferences
                WHERE user_id = %s
            """, (user_id,))

            row = await cur.fetchone()
            result = dict(row) if row else None

    if cache.enabled and cache.client:
        try:
            await cache.client.set(
                cache_key,
                _json.dumps(result, default=str) if result else b"null",
                ex=_USER_PREFS_TTL,
            )
        except Exception:
            pass

    return result


async def invalidate_user_prefs_cache(user_id: str) -> None:
    """Delete the cached ``get_user_preferences`` result so the next call hits the DB."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if cache.enabled and cache.client:
        try:
            await cache.client.delete(f"user_prefs:{user_id}")
        except Exception:
            pass


def _split_updates_and_deletes(data: Optional[Dict[str, Any]]) -> tuple[Dict[str, Any], list[str]]:
    """Split data into updates (non-None values) and deletes (None values).

    Args:
        data: Dict that may contain None values to signal deletion

    Returns:
        Tuple of (updates_dict, delete_keys_list)
    """
    if not data:
        return {}, []
    updates = {}
    deletes = []
    for key, value in data.items():
        if value is None:
            deletes.append(key)
        else:
            updates[key] = value
    return updates, deletes


async def upsert_user_preferences(
    user_id: str,
    risk_preference: Optional[Dict[str, Any]] = None,
    investment_preference: Optional[Dict[str, Any]] = None,
    agent_preference: Optional[Dict[str, Any]] = None,
    other_preference: Optional[Dict[str, Any]] = None,
    replace: bool = False,
) -> Dict[str, Any]:
    """
    Create or update user preferences (upsert with merge or replace).

    For existing preferences, merges the JSONB fields by default.
    To delete a field within a preference, pass it with a None value (e.g., {"notes": None}).
    Use replace=True to completely replace the preference instead of merging.

    Args:
        user_id: User ID
        risk_preference: Risk settings dict (None values = delete field)
        investment_preference: Investment settings dict (None values = delete field)
        agent_preference: Agent behavior settings dict (None values = delete field)
        other_preference: Miscellaneous settings dict (None values = delete field)
        replace: If True, replace entire preference instead of merging

    Returns:
        Updated preferences dict
    """
    user_preference_id = str(uuid4())

    # Split each preference into updates and deletes
    risk_updates, risk_deletes = _split_updates_and_deletes(risk_preference)
    inv_updates, inv_deletes = _split_updates_and_deletes(investment_preference)
    agent_updates, agent_deletes = _split_updates_and_deletes(agent_preference)
    other_updates, other_deletes = _split_updates_and_deletes(other_preference)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            if replace:
                # Replace mode: completely replace the JSONB field (only for provided preferences)
                await cur.execute("""
                    INSERT INTO user_preferences (
                        user_preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        risk_preference = CASE WHEN %s THEN %s::jsonb ELSE user_preferences.risk_preference END,
                        investment_preference = CASE WHEN %s THEN %s::jsonb ELSE user_preferences.investment_preference END,
                        agent_preference = CASE WHEN %s THEN %s::jsonb ELSE user_preferences.agent_preference END,
                        other_preference = CASE WHEN %s THEN %s::jsonb ELSE user_preferences.other_preference END,
                        updated_at = NOW()
                    RETURNING
                        user_preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                """, (
                    user_preference_id, user_id,
                    Json(risk_updates or {}),
                    Json(inv_updates or {}),
                    Json(agent_updates or {}),
                    Json(other_updates or {}),
                    # For UPDATE: flag if provided, then the value
                    risk_preference is not None, Json(risk_updates or {}),
                    investment_preference is not None, Json(inv_updates or {}),
                    agent_preference is not None, Json(agent_updates or {}),
                    other_preference is not None, Json(other_updates or {}),
                ))
            else:
                # Merge mode: use JSONB merge (||) to add/update, remove deleted keys (- text[])
                await cur.execute("""
                    INSERT INTO user_preferences (
                        user_preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        risk_preference = (COALESCE(user_preferences.risk_preference, '{}'::jsonb) - %s::text[]) || COALESCE(%s::jsonb, '{}'::jsonb),
                        investment_preference = (COALESCE(user_preferences.investment_preference, '{}'::jsonb) - %s::text[]) || COALESCE(%s::jsonb, '{}'::jsonb),
                        agent_preference = (COALESCE(user_preferences.agent_preference, '{}'::jsonb) - %s::text[]) || COALESCE(%s::jsonb, '{}'::jsonb),
                        other_preference = (COALESCE(user_preferences.other_preference, '{}'::jsonb) - %s::text[]) || COALESCE(%s::jsonb, '{}'::jsonb),
                        updated_at = NOW()
                    RETURNING
                        user_preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                """, (
                    user_preference_id, user_id,
                    Json(risk_updates or {}),
                    Json(inv_updates or {}),
                    Json(agent_updates or {}),
                    Json(other_updates or {}),
                    # For the UPDATE clause: deletes then updates for each column
                    risk_deletes, Json(risk_updates) if risk_updates else None,
                    inv_deletes, Json(inv_updates) if inv_updates else None,
                    agent_deletes, Json(agent_updates) if agent_updates else None,
                    other_deletes, Json(other_updates) if other_updates else None,
                ))

            result = await cur.fetchone()
            logger.info(f"[user_db] upsert_user_preferences user_id={user_id} replace={replace}")
            return dict(result)


async def delete_user_preferences(user_id: str) -> bool:
    """
    Delete all preferences for a user.

    Args:
        user_id: User ID

    Returns:
        True if a row was deleted, False if no preferences existed
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_preferences WHERE user_id = %s",
                (user_id,),
            )
            deleted = cur.rowcount > 0
            logger.info(f"[user_db] delete_user_preferences user_id={user_id} deleted={deleted}")
            return deleted


async def get_user_with_preferences(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user with their preferences in a single query.

    Args:
        user_id: User ID

    Returns:
        Dict with 'user' and 'preferences' keys, or None if user not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    u.user_id, u.email, u.name, u.avatar_url, u.timezone, u.locale,
                    u.onboarding_completed,
                    COALESCE(u.personalization_completed, FALSE) AS personalization_completed,
                    u.auth_provider,
                    u.created_at, u.updated_at, u.last_login_at,
                    """ + _gate_cols("u") + """,
                    p.user_preference_id, p.risk_preference, p.investment_preference,
                    p.agent_preference, p.other_preference,
                    p.created_at as pref_created_at, p.updated_at as pref_updated_at
                FROM users u
                LEFT JOIN user_preferences p ON u.user_id = p.user_id
                WHERE u.user_id = %s
            """, (user_id,))

            result = await cur.fetchone()
            if not result:
                return None

            # Split into user and preferences
            user = {
                'user_id': result['user_id'],
                'email': result['email'],
                'name': result['name'],
                'avatar_url': result['avatar_url'],
                'timezone': result['timezone'],
                'locale': result['locale'],
                'onboarding_completed': result['onboarding_completed'],
                'personalization_completed': result['personalization_completed'],
                'has_api_key': result['has_api_key'],
                'has_oauth_token': result['has_oauth_token'],
                'auth_provider': result['auth_provider'],
                'created_at': result['created_at'],
                'updated_at': result['updated_at'],
                'last_login_at': result['last_login_at'],
            }

            preferences = None
            if result['user_preference_id']:
                preferences = {
                    'user_preference_id': result['user_preference_id'],
                    'user_id': result['user_id'],
                    'risk_preference': result['risk_preference'],
                    'investment_preference': result['investment_preference'],
                    'agent_preference': result['agent_preference'],
                    'other_preference': result['other_preference'],
                    'created_at': result['pref_created_at'],
                    'updated_at': result['pref_updated_at'],
                }

            return {'user': user, 'preferences': preferences}
