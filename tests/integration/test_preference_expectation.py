"""A key of ``other_preference`` written whole lands only on the value it was
built from, so a write from a stale read cannot drop another device's change.

The compare is SQL inside the write's own statement, so these run against real
PostgreSQL.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

KEY = "app_scopes"


async def _stored(user_id: str):
    from src.server.database.user import get_user_preferences

    row = await get_user_preferences(user_id, cached=False)
    return ((row or {}).get("other_preference") or {}).get(KEY)


async def _write(user_id: str, value, **kwargs) -> None:
    from src.server.database.user import upsert_user_preferences

    await upsert_user_preferences(
        user_id=user_id, other_preference={KEY: value, "beside": value}, **kwargs
    )


async def test_a_write_from_a_stale_read_is_refused_whole(seed_user, patched_get_db_connection):
    from src.server.database.user import PreferenceChanged

    uid = seed_user["user_id"]

    # No row yet: every key reads as absent, so only an expectation of None fits.
    with pytest.raises(PreferenceChanged):
        await _write(uid, {"a": ["x"]}, expect_other_preference={KEY: {"a": []}})
    await _write(uid, {"a": ["x"]}, expect_other_preference={KEY: None})

    # Two devices read {"a": ["x"]}; one revokes, the other adds from its stale copy.
    await _write(uid, {"a": []}, expect_other_preference={KEY: {"a": ["x"]}})
    with pytest.raises(PreferenceChanged) as refused:
        await _write(uid, {"a": ["x"], "b": ["y"]}, expect_other_preference={KEY: {"a": ["x"]}})
    assert refused.value.keys == [KEY]

    from src.server.database.user import get_user_preferences

    other = (await get_user_preferences(uid, cached=False))["other_preference"]
    assert other == {KEY: {"a": []}, "beside": {"a": []}}


async def test_a_write_without_an_expectation_applies_as_before(seed_user, patched_get_db_connection):
    uid = seed_user["user_id"]
    await _write(uid, {"a": ["x"]})
    await _write(uid, {"b": ["y"]})
    assert await _stored(uid) == {"b": ["y"]}
