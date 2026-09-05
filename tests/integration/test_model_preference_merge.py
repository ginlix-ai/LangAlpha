"""``profiles`` merges one level deeper than everything beside it.

Sibling keys ride a shallow ``||``, which is right for them and destructive for
``profiles``: a client tuning one model would otherwise replace the whole map
and wipe every other model's settings. The merge is SQL, so these run against
real PostgreSQL. A fake would only prove that the fake merges.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _model_pref(user_id: str) -> dict:
    from src.server.database.user import get_user_preferences

    row = await get_user_preferences(user_id)
    return (row or {}).get("model_preference") or {}


async def _patch(user_id: str, model_preference: dict) -> None:
    from src.server.database.user import upsert_user_preferences

    await upsert_user_preferences(user_id=user_id, model_preference=model_preference)


class TestProfilesMergePerModel:
    async def test_patching_one_model_leaves_the_others_intact(
        self, seed_user, patched_get_db_connection
    ):
        """The whole reason profiles gets its own merge."""
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": "high"}, "m2": {"fast_mode": True}}})
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": "low"}}})

        profiles = (await _model_pref(uid))["profiles"]
        assert profiles["m1"] == {"reasoning_effort": "low"}
        assert profiles["m2"] == {"fast_mode": True}

    async def test_a_model_present_in_both_is_field_merged(
        self, seed_user, patched_get_db_connection
    ):
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": "high", "fast_mode": True}}})
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": "low"}}})

        assert (await _model_pref(uid))["profiles"]["m1"] == {
            "reasoning_effort": "low",
            "fast_mode": True,
        }

    async def test_a_null_model_deletes_only_that_model(
        self, seed_user, patched_get_db_connection
    ):
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"fast_mode": True}, "m2": {"fast_mode": False}}})
        await _patch(uid, {"profiles": {"m1": None}})

        profiles = (await _model_pref(uid))["profiles"]
        assert "m1" not in profiles
        assert profiles["m2"] == {"fast_mode": False}

    async def test_a_null_field_inside_a_profile_is_stripped(
        self, seed_user, patched_get_db_connection
    ):
        """Null deletes at the field level too, not just the model level."""
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": "high", "fast_mode": True}}})
        await _patch(uid, {"profiles": {"m1": {"reasoning_effort": None}}})

        assert (await _model_pref(uid))["profiles"]["m1"] == {"fast_mode": True}

    async def test_a_null_profiles_deletes_the_whole_map(
        self, seed_user, patched_get_db_connection
    ):
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"fast_mode": True}}})
        await _patch(uid, {"profiles": None})

        assert "profiles" not in await _model_pref(uid)

    async def test_profiles_survives_a_patch_that_does_not_mention_it(
        self, seed_user, patched_get_db_connection
    ):
        uid = seed_user["user_id"]
        await _patch(uid, {"profiles": {"m1": {"fast_mode": True}}})
        await _patch(uid, {"preferred_model": "some-model"})

        got = await _model_pref(uid)
        assert got["profiles"]["m1"] == {"fast_mode": True}
        assert got["preferred_model"] == "some-model"


class TestSiblingsKeepTheShallowMerge:
    async def test_scalar_siblings_merge_alongside_a_profiles_patch(
        self, seed_user, patched_get_db_connection
    ):
        """Peeling profiles out must not cost the rest of the patch its merge."""
        uid = seed_user["user_id"]
        await _patch(uid, {"preferred_model": "m1", "fetch_model": "m2"})
        await _patch(uid, {"fetch_model": "m3", "profiles": {"m1": {"fast_mode": True}}})

        got = await _model_pref(uid)
        assert got["preferred_model"] == "m1"
        assert got["fetch_model"] == "m3"
        assert got["profiles"]["m1"] == {"fast_mode": True}

    async def test_a_null_sibling_still_deletes_its_key(
        self, seed_user, patched_get_db_connection
    ):
        uid = seed_user["user_id"]
        await _patch(uid, {"preferred_model": "m1", "profiles": {"m1": {"fast_mode": True}}})
        await _patch(uid, {"preferred_model": None})

        got = await _model_pref(uid)
        assert "preferred_model" not in got
        assert got["profiles"]["m1"] == {"fast_mode": True}
