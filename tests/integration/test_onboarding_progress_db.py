"""The onboarding progress record against real PostgreSQL (migration 135's
``onboarding_progress``): a partial upsert, a null that clears, a stamp that
keeps the first time it was set, and a row that follows its user."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def user(seed_user, patched_get_db_connection):
    yield seed_user["user_id"]
    async with patched_get_db_connection() as conn:
        await conn.execute("DELETE FROM onboarding_progress WHERE user_id = %s", (seed_user["user_id"],))


async def test_a_patch_writes_only_its_keys_and_a_null_clears_one(user):
    from src.server.database.onboarding_progress import get_onboarding_progress, patch_onboarding_progress

    assert await get_onboarding_progress(user) is None
    await patch_onboarding_progress(user, {"models": "claude_code", "runs_on": "cloud"})
    row = await patch_onboarding_progress(user, {"setup_on": "browser", "runs_on": None})

    assert row["models"] == "claude_code"
    assert row["runs_on"] is None
    assert row["setup_on"] == "browser"
    assert row == await get_onboarding_progress(user)


async def test_a_stamp_keeps_its_first_time_and_false_clears_it(user):
    from src.server.database.onboarding_progress import patch_onboarding_progress

    first = await patch_onboarding_progress(user, {"setup_completed": True})
    again = await patch_onboarding_progress(user, {"setup_completed": True, "setup_step": "default"})
    cleared = await patch_onboarding_progress(user, {"setup_completed": False})

    assert first["setup_completed_at"] is not None
    assert again["setup_completed_at"] == first["setup_completed_at"]
    assert again["setup_step"] == "default"
    assert cleared["setup_completed_at"] is None
    assert cleared["setup_step"] == "default"


async def test_writers_racing_to_stamp_keep_one_time(user):
    """A hand-off between onboarding surfaces and a retry of it can land together."""
    from src.server.database.onboarding_progress import patch_onboarding_progress

    rows = await asyncio.gather(
        *(patch_onboarding_progress(user, {"plan_step_done": True}) for _ in range(8))
    )
    final = await patch_onboarding_progress(user, {"plan_step_done": True})

    assert {row["plan_step_done_at"] for row in rows} == {final["plan_step_done_at"]}


async def test_a_value_outside_the_vocabulary_is_refused_by_the_table(user):
    import psycopg

    from src.server.database.onboarding_progress import patch_onboarding_progress

    with pytest.raises(psycopg.errors.CheckViolation):
        await patch_onboarding_progress(user, {"runs_on": "laptop"})


async def test_no_user_row_answers_none(patched_get_db_connection):
    from src.server.database.onboarding_progress import patch_onboarding_progress

    assert await patch_onboarding_progress("nobody-onboarding-135", {"runs_on": "cloud"}) is None


async def test_the_row_follows_its_user_to_a_new_id_and_goes_with_them(patched_get_db_connection):
    """A legacy user with nothing else stored, since several older tables
    refuse a new id for a user they hold rows for."""
    from src.server.database.onboarding_progress import get_onboarding_progress, patch_onboarding_progress
    from src.server.database.user import migrate_user_id

    user = "legacy-onboarding-135"
    moved = f"{user}-moved"
    async with patched_get_db_connection() as conn:
        await conn.execute("INSERT INTO users (user_id, email) VALUES (%s, %s)", (user, "legacy@example.com"))
    await patch_onboarding_progress(user, {"models": "langalpha_models"})
    await migrate_user_id(user, moved)
    try:
        assert (await get_onboarding_progress(moved))["models"] == "langalpha_models"
        assert await get_onboarding_progress(user) is None
    finally:
        async with patched_get_db_connection() as conn:
            await conn.execute("DELETE FROM users WHERE user_id IN (%s, %s)", (user, moved))
    assert await get_onboarding_progress(moved) is None


async def test_a_preferences_reset_keeps_the_account_setup_stamp(user):
    """Reset clears the user's choices beside the stamp, which it keeps."""
    from src.server.database.user import (
        delete_user_preferences,
        get_user_preferences,
        upsert_user_preferences,
    )

    await upsert_user_preferences(
        user_id=user,
        other_preference={"theme": "dark", "account_setup_completed_at": "2026-11-15T09:30:00Z"},
    )

    assert await delete_user_preferences(user) is True
    kept = await get_user_preferences(user, cached=False)

    assert kept["other_preference"] == {"account_setup_completed_at": "2026-11-15T09:30:00Z"}
