"""Precedence for the per-model tuning fields.

One rule serves every tuning field, so a caller that knows which model is
running resolves a compaction profile the same way it resolves a guidance
level. The edges that fail quietly are here: a profile storing the field as
``None`` is an answer rather than a miss, and a malformed ``profiles`` bag must
not take the account-wide value down with it.
"""

import pytest

from src.llms.preferences import TUNING_FIELDS, resolve_tuning_field


class TestPrecedence:
    def test_profile_beats_the_account_wide_value(self):
        pref = {"reasoning_effort": "low", "profiles": {"m1": {"reasoning_effort": "high"}}}
        assert resolve_tuning_field(pref, "m1", "reasoning_effort") == "high"

    def test_a_profile_governs_only_its_own_model(self):
        pref = {"reasoning_effort": "low", "profiles": {"m1": {"reasoning_effort": "high"}}}
        assert resolve_tuning_field(pref, "m2", "reasoning_effort") == "low"

    def test_account_wide_value_when_no_profiles_exist(self):
        assert resolve_tuning_field({"fast_mode": True}, "m1", "fast_mode") is True

    def test_a_profile_silent_on_the_field_falls_through(self):
        pref = {"reasoning_effort": "low", "profiles": {"m1": {"fast_mode": True}}}
        assert resolve_tuning_field(pref, "m1", "reasoning_effort") == "low"

    def test_no_model_cannot_select_a_profile(self):
        pref = {"reasoning_effort": "low", "profiles": {"m1": {"reasoning_effort": "high"}}}
        assert resolve_tuning_field(pref, None, "reasoning_effort") == "low"

    def test_missing_everywhere_is_none(self):
        assert resolve_tuning_field({}, "m1", "reasoning_effort") is None


class TestAStoredNullIsAnAnswer:
    """Membership is the test, not truthiness. A profile that stores ``None``
    is the user turning the field off for that one model, so falling through to
    the account-wide value would silently re-enable it."""

    def test_a_null_in_a_profile_wins(self):
        pref = {"reasoning_effort": "high", "profiles": {"m1": {"reasoning_effort": None}}}
        assert resolve_tuning_field(pref, "m1", "reasoning_effort") is None

    def test_a_false_in_a_profile_wins(self):
        pref = {"fast_mode": True, "profiles": {"m1": {"fast_mode": False}}}
        assert resolve_tuning_field(pref, "m1", "fast_mode") is False


class TestMalformedBagsDegradeToAccountWide:
    @pytest.mark.parametrize("profiles", ["nope", 3, [], None])
    def test_a_non_map_profiles_bag_is_ignored(self, profiles):
        pref = {"reasoning_effort": "low", "profiles": profiles}
        assert resolve_tuning_field(pref, "m1", "reasoning_effort") == "low"

    @pytest.mark.parametrize("profile", ["nope", 3, [], None])
    def test_a_non_map_profile_entry_is_ignored(self, profile):
        pref = {"reasoning_effort": "low", "profiles": {"m1": profile}}
        assert resolve_tuning_field(pref, "m1", "reasoning_effort") == "low"


class TestEveryTuningFieldUsesTheSameRule:
    @pytest.mark.parametrize("field", TUNING_FIELDS)
    def test_each_declared_field_resolves_through_the_profile(self, field):
        pref = {field: "account-wide", "profiles": {"m1": {field: "per-model"}}}
        assert resolve_tuning_field(pref, "m1", field) == "per-model"
