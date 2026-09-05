"""One generated statement writes every preference column, at the depth each needs.

The clause this replaced was concatenated by hand, and its operand order was
load-bearing: swap the two terms and a one-model patch starts wiping every other
model while every test still passes. What is pinned here is the generation --
which column merges deep, which stays shallow, and that the placeholders and the
parameters still line up.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.llms.preferences import TuningError
from src.server.database.user import _PREF_COLUMNS, upsert_user_preferences

pytestmark = pytest.mark.asyncio


@pytest.fixture
def executed():
    """Run the upsert against a cursor that only records what it was handed."""
    calls: list[tuple[str, list]] = []

    async def _execute(sql, params):
        calls.append((sql, list(params)))

    cursor = AsyncMock()
    cursor.execute = _execute
    cursor.fetchone = AsyncMock(return_value={"user_id": "u"})

    @asynccontextmanager
    async def _cursor(**kwargs):
        yield cursor

    conn = AsyncMock()
    conn.cursor = _cursor

    @asynccontextmanager
    async def _conn():
        yield conn

    with patch("src.server.database.user.get_db_connection", new=_conn):
        yield calls


def _values(params: list) -> list:
    """Parameters with the JSON wrappers unwrapped, for comparison by value."""
    return [p.obj if hasattr(p, "obj") else p for p in params]


class TestGeneratedStatement:
    @pytest.mark.parametrize("replace", [False, True])
    async def test_every_placeholder_has_a_parameter(self, executed, replace):
        """The failure the hand-spelled version could not rule out."""
        await upsert_user_preferences(
            user_id="u",
            risk_preference={"a": 1, "b": None},
            other_preference={"c": 2},
            model_preference={"profiles": {"m1": {"fast_mode": True}}},
            replace=replace,
        )
        sql, params = executed[0]
        assert sql.count("%s") == len(params)

    async def test_only_model_preference_merges_at_depth(self, executed):
        await upsert_user_preferences(user_id="u", model_preference={"x": 1})
        sql, _ = executed[0]
        for column, deep in _PREF_COLUMNS:
            clause = f"{column} = jsonb_deep_merge(COALESCE(user_preferences.{column}"
            assert (clause in sql) is deep, column

    async def test_a_deep_patch_keeps_its_nulls_and_a_shallow_one_splits_them(
        self, executed
    ):
        """The two ways a key is deleted, and which column gets which."""
        await upsert_user_preferences(
            user_id="u",
            other_preference={"gone": None},
            model_preference={"profiles": {"m1": None}},
        )
        _, params = executed[0]
        values = _values(params)
        assert {"profiles": {"m1": None}} in values  # read as a delete by the merge
        assert ["gone"] in values  # subtracted as a text[] instead


class TestRejectedShapes:
    """A shape the merge cannot interpret, refused before it reaches the DB."""

    @pytest.mark.parametrize("profiles", ["nope", 3, [], True])
    async def test_a_non_map_profiles_bag_is_rejected(self, executed, profiles):
        with pytest.raises(TuningError) as err:
            await upsert_user_preferences(
                user_id="u", model_preference={"profiles": profiles}
            )
        assert err.value.field == "profiles"

    @pytest.mark.parametrize("entry", ["nope", 3, [], True])
    async def test_a_non_map_profile_entry_is_rejected(self, executed, entry):
        with pytest.raises(TuningError):
            await upsert_user_preferences(
                user_id="u", model_preference={"profiles": {"m1": entry}}
            )

    async def test_a_null_entry_is_a_per_model_delete_and_is_allowed(self, executed):
        await upsert_user_preferences(
            user_id="u", model_preference={"profiles": {"m1": None}}
        )
        assert executed


class TestMovedKeyDeletes:
    """A clear reaches both columns, because the merge cannot store a tombstone.

    ``jsonb_deep_merge`` reads a null as a delete rather than storing it, and
    ``get_model_preference`` reads ``other_preference`` underneath the model
    column for exactly the keys 034 moved. Clearing one column alone therefore
    is not a clear at all: the pre-move value answers the next read.
    """

    async def test_a_cleared_moved_key_is_subtracted_from_the_legacy_column(
        self, executed
    ):
        await upsert_user_preferences(
            user_id="u", model_preference={"preferred_model": None}
        )
        values = _values(executed[0][1])
        assert {"preferred_model": None} in values  # the merge reads this as a delete
        assert ["preferred_model"] in values  # and the shallow column subtracts it

    async def test_a_key_that_never_moved_is_left_alone(self, executed):
        """``profiles`` has no pre-move copy, and a dict offered to the shallow
        column would replace the bag rather than merge into it."""
        await upsert_user_preferences(
            user_id="u", model_preference={"profiles": {"m1": None}, "fast_mode": None}
        )
        assert ["fast_mode"] in _values(executed[0][1])
        assert not any(
            isinstance(v, list) and "profiles" in v for v in _values(executed[0][1])
        )

    async def test_setting_a_moved_key_touches_only_its_own_column(self, executed):
        await upsert_user_preferences(
            user_id="u", model_preference={"preferred_model": "m1"}
        )
        assert not any(
            isinstance(v, list) and "preferred_model" in v
            for v in _values(executed[0][1])
        )

    async def test_an_explicit_legacy_value_from_the_caller_wins(self, executed):
        """The mirror is a default, not an override: a caller that named the
        old column outright meant it."""
        await upsert_user_preferences(
            user_id="u",
            model_preference={"preferred_model": None},
            other_preference={"preferred_model": "kept"},
        )
        assert {"preferred_model": "kept"} in _values(executed[0][1])
