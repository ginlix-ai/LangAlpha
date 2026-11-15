"""``plan_spec_tier``: the size a computer sized for the user takes on their plan.

It reads the platform the other way from the spec gate: only a definitive
answer opens a size, and everything short of one is None, which a caller must
not take for standard.
"""

from unittest.mock import AsyncMock, patch

import pytest

MODULE = "src.server.dependencies.usage_limits"
OFFERED = ("standard", "performance", "max")
ALL_SCOPES = ["workspace:spec:performance", "workspace:spec:max", "workspace:spec:xlarge"]


def _platform(scopes, quotas=None, *, raises=None):
    """A validate call answering ``scopes``, and each count quota from ``quotas``."""

    async def validate(user_id, check_quota=None, **_):
        if raises is not None:
            raise raises
        if check_quota is None:
            return None if scopes is None else {"scopes": scopes}
        quota = (quotas or {}).get(check_quota)
        return {"scopes": scopes, "quota": quota}

    return AsyncMock(side_effect=validate)


async def _tier(validate, *, gating=True, offered=OFFERED, current_tier=None):
    from src.server.dependencies.usage_limits import plan_spec_tier

    with (
        patch(f"{MODULE}.platform_gating_active", return_value=gating),
        patch(f"{MODULE}._call_validate_for_user", validate),
    ):
        return await plan_spec_tier("user-1", offered, current_tier=current_tier)


@pytest.mark.asyncio
async def test_oss_is_standard_without_asking_anyone():
    validate = _platform(ALL_SCOPES)
    assert await _tier(validate, gating=False) == "standard"
    validate.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_largest_size_the_provider_offers_and_the_plan_opens():
    """Extra Large is on the plan, but this provider never builds it."""
    validate = _platform(ALL_SCOPES, {"spec_max": {"allowed": True}})
    assert await _tier(validate) == "max"


@pytest.mark.asyncio
async def test_a_size_with_no_free_slot_steps_down_to_one_with_a_slot():
    validate = _platform(
        ALL_SCOPES,
        {"spec_max": {"allowed": False}, "spec_performance": {"allowed": True}},
    )
    assert await _tier(validate) == "performance"


@pytest.mark.asyncio
async def test_a_plan_with_no_elevated_scope_is_standard():
    validate = _platform(["workspace:always_on"])
    assert await _tier(validate) == "standard"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "validate",
    [
        _platform(None),
        _platform(ALL_SCOPES, {}),
        _platform(ALL_SCOPES, raises=RuntimeError("boom")),
    ],
    ids=["unreachable", "no_quota", "error"],
)
async def test_anything_short_of_a_definitive_answer_is_unread(validate):
    assert await _tier(validate) is None


@pytest.mark.asyncio
async def test_the_scope_list_is_read_past_the_cache():
    """A plan bought a minute ago: the cached list from before it must not decide."""
    from src.server.dependencies import usage_limits

    validate = _platform(ALL_SCOPES, {"spec_max": {"allowed": True}})
    with patch.dict(usage_limits._scope_cache, {"user-1": ([], 1e18)}):
        assert await _tier(validate) == "max"


@pytest.mark.asyncio
async def test_a_late_read_from_before_a_downgrade_leaves_the_gates_denial():
    """Two reads overlap a downgrade: the gate caches the later one's list,
    then the earlier one lands with the old grant. The gate keeps refusing."""
    from src.server.dependencies import usage_limits

    downgraded = _platform([], {})
    stale = _platform(ALL_SCOPES, {"spec_max": {"allowed": True}})
    with (
        patch.dict(usage_limits._scope_cache, clear=True),
        patch(f"{MODULE}.platform_gating_active", return_value=True),
    ):
        with patch(f"{MODULE}._call_validate_for_user", downgraded):
            assert not await usage_limits.spec_grantable("user-1", "max", current_tier="standard")
        with patch(f"{MODULE}._call_validate_for_user", stale):
            assert await usage_limits.plan_spec_tier("user-1", OFFERED) == "max"
        with patch(f"{MODULE}._call_validate_for_user", downgraded):
            assert not await usage_limits.spec_grantable("user-1", "max", current_tier="standard")
        assert usage_limits._scope_cache["user-1"][0] == []


@pytest.mark.asyncio
async def test_a_computer_moving_up_gives_back_the_slot_it_holds():
    """Home at Medium fills the plan's one slot, which Large would take over."""
    validate = _platform(
        ALL_SCOPES,
        {"spec_max": {"allowed": False, "capacity_used": 1, "capacity_limit": 1}},
    )
    assert await _tier(validate, current_tier="performance") == "max"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "quota, current_tier",
    [
        ({"message": "pending"}, None),
        ({"message": "pending"}, "standard"),
        ({"message": "pending"}, "performance"),
        ({"message": "pending", "capacity_used": 1}, "performance"),
        ({"allowed": "yes", "capacity_limit": 1}, "performance"),
    ],
    ids=["new", "from-standard", "returning-slot", "returning-slot-no-limit", "returning-slot-no-used"],
)
async def test_a_quota_with_no_verdict_is_unread(quota, current_tier):
    """Neither a boolean ``allowed`` nor both counts for a returned slot."""
    validate = _platform(ALL_SCOPES, {"spec_max": quota})
    assert await _tier(validate, current_tier=current_tier) is None


@pytest.mark.asyncio
async def test_a_returned_slot_refused_without_counts_steps_down():
    validate = _platform(
        ALL_SCOPES,
        {"spec_max": {"allowed": False}, "spec_performance": {"allowed": True}},
    )
    assert await _tier(validate, current_tier="performance") == "performance"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("later", "tier"),
    [(["workspace:spec:performance"], "performance"), (None, None)],
    ids=["newer-list-tries-smaller", "no-list-unread"],
)
async def test_a_count_that_approves_a_size_whose_scope_has_gone_does_not_open_it(later, tier):
    """The count approves a slot, not the plan's scope, which can go between
    the two reads: the answer that approves it must still carry it, and one
    whose newer list grants only a smaller size gives that one."""

    async def validate(user_id, check_quota=None, **_):
        if check_quota is None:
            return {"scopes": ALL_SCOPES}
        answer = {"quota": {"allowed": True}}
        return answer if later is None else {"scopes": later, **answer}

    assert await _tier(AsyncMock(side_effect=validate)) == tier


@pytest.mark.asyncio
async def test_a_computer_already_at_the_plans_size_keeps_it_without_a_count():
    validate = _platform(["workspace:spec:performance"])
    assert await _tier(validate, current_tier="performance") == "performance"
    assert [c.kwargs.get("check_quota") for c in validate.await_args_list] == [None]


async def _slot(validate, *, gating=True, running=False):
    from src.server.dependencies.usage_limits import computer_slot_granted

    with (
        patch(f"{MODULE}.platform_gating_active", return_value=gating),
        patch(f"{MODULE}._call_validate_for_user", validate),
    ):
        return await computer_slot_granted("user-1", running=running)


@pytest.mark.asyncio
async def test_a_running_slot_is_read_off_the_running_limit():
    validate = _platform([], {"workspace": {"allowed": True}, "computer_count": {"allowed": False}})
    assert await _slot(validate, running=True) is True
    assert await _slot(validate) is False


@pytest.mark.asyncio
async def test_a_computer_slot_is_granted_only_on_a_definitive_yes():
    assert await _slot(_platform([], {"computer_count": {"allowed": True}})) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "validate, gating",
    [
        (_platform([], {"computer_count": {"allowed": False}}), True),
        (_platform([], {}), True),
        (AsyncMock(return_value=None), True),
        (_platform([], raises=RuntimeError("down")), True),
        (_platform([], {"computer_count": {"allowed": True}}), False),
    ],
    ids=["full", "no-quota", "no-answer", "unreachable", "oss"],
)
async def test_anything_short_of_a_yes_grants_no_computer_slot(validate, gating):
    assert await _slot(validate, gating=gating) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("later", "tier"),
    [([], "standard"), (["workspace:spec:performance"], "performance")],
    ids=["downgraded", "still-granted"],
)
async def test_a_denied_larger_size_hands_its_newer_scopes_to_the_smaller_ones(later, tier):
    """The plan was downgraded between the reads: the count that refuses Max
    carries the newer scope list, so the size held is kept only while that
    list still grants it."""

    async def validate(user_id, check_quota=None, **_):
        if check_quota is None:
            return {"scopes": ALL_SCOPES}
        return {"scopes": later, "quota": {"allowed": False}}

    assert await _tier(AsyncMock(side_effect=validate), current_tier="performance") == tier
