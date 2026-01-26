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

from src.server.database.conversation_db import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


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
                    onboarding_completed, created_at, updated_at, last_login_at
            """, (user_id, email, name, avatar_url, timezone, locale))

            result = await cur.fetchone()
            logger.info(f"[user_db] create_user user_id={user_id}")
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
                    onboarding_completed, created_at, updated_at, last_login_at
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
    last_login_at: Optional[datetime] = None,
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
        last_login_at: New last login timestamp

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
    builder.add_field("last_login_at", last_login_at)

    if not builder.has_updates():
        return await get_user(user_id)

    returning_columns = [
        "user_id", "email", "name", "avatar_url", "timezone", "locale",
        "onboarding_completed", "created_at", "updated_at", "last_login_at",
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
                    onboarding_completed, created_at, updated_at, last_login_at
            """, (user_id, email, name, avatar_url, timezone, locale))

            result = await cur.fetchone()
            logger.info(f"[user_db] upsert_user user_id={user_id}")
            return dict(result)


# ==================== User Preferences Operations ====================


async def get_user_preferences(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user preferences.

    Args:
        user_id: User ID

    Returns:
        Preferences dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    preference_id, user_id,
                    risk_preference, investment_preference,
                    agent_preference, other_preference,
                    created_at, updated_at
                FROM user_preferences
                WHERE user_id = %s
            """, (user_id,))

            result = await cur.fetchone()
            return dict(result) if result else None


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
    preference_id = str(uuid4())

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
                        preference_id, user_id,
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
                        preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                """, (
                    preference_id, user_id,
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
                        preference_id, user_id,
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
                        preference_id, user_id,
                        risk_preference, investment_preference,
                        agent_preference, other_preference,
                        created_at, updated_at
                """, (
                    preference_id, user_id,
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
                    u.onboarding_completed, u.created_at, u.updated_at, u.last_login_at,
                    p.preference_id, p.risk_preference, p.investment_preference,
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
                'created_at': result['created_at'],
                'updated_at': result['updated_at'],
                'last_login_at': result['last_login_at'],
            }

            preferences = None
            if result['preference_id']:
                preferences = {
                    'preference_id': result['preference_id'],
                    'user_id': result['user_id'],
                    'risk_preference': result['risk_preference'],
                    'investment_preference': result['investment_preference'],
                    'agent_preference': result['agent_preference'],
                    'other_preference': result['other_preference'],
                    'created_at': result['pref_created_at'],
                    'updated_at': result['pref_updated_at'],
                }

            return {'user': user, 'preferences': preferences}
