"""A thread's own model and the account default it falls back to.

Locks the rules the composer shares with the server: the effective default per
mode, which threads a changed default moves (only those still on the old name,
per mode), when a write moves them at all (``existing_threads`` only), and which
model a turn runs (a named one, else the thread's own; a held model that no
longer resolves is cleared and the default runs, a named one is left for the
resolver to refuse).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.llm import thread_model

_MOD = "src.server.services.llm.thread_model"
SYSTEM = {"default_model": "sys-main", "flash_model": "sys-flash"}


def _catalog(names):
    mc = MagicMock()
    mc.llm_config = {n: {"model_id": n} for n in names}
    mc.get_model_config.side_effect = lambda n: mc.llm_config.get(n)
    return mc


# ---------------------------------------------------------------------------
# Effective default per mode (the composer's rule)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pref, system, expected",
    [
        ({}, SYSTEM, {"ptc": "sys-main", "flash": "sys-flash"}),
        # A PTC preference is also the flash default when no flash one is saved.
        ({"preferred_model": "m-a"}, SYSTEM, {"ptc": "m-a", "flash": "m-a"}),
        (
            {"preferred_model": "m-a", "preferred_flash_model": "m-f"},
            SYSTEM,
            {"ptc": "m-a", "flash": "m-f"},
        ),
        ({"preferred_flash_model": "m-f"}, SYSTEM, {"ptc": "sys-main", "flash": "m-f"}),
        # Auto for flash keeps the deployment's flash model beside a saved primary,
        # and a saved flash model still beats it.
        (
            {"preferred_model": "m-a", "flash_follows": "deployment"},
            SYSTEM,
            {"ptc": "m-a", "flash": "sys-flash"},
        ),
        (
            {"preferred_model": "m-a", "preferred_flash_model": "m-f", "flash_follows": "deployment"},
            SYSTEM,
            {"ptc": "m-a", "flash": "m-f"},
        ),
        (
            {"preferred_model": "m-a", "flash_follows": "primary"},
            SYSTEM,
            {"ptc": "m-a", "flash": "m-a"},
        ),
        # No deployment flash model: flash runs the deployment's main one.
        ({}, {"default_model": "sys-main", "flash_model": ""}, {"ptc": "sys-main", "flash": "sys-main"}),
        ({}, {"default_model": "", "flash_model": ""}, {"ptc": None, "flash": None}),
    ],
)
def test_effective_default_follows_the_composer_rule(pref, system, expected):
    with patch(f"{_MOD}.system_default_models", return_value=system):
        assert thread_model.effective_default_models(pref) == expected


def test_only_modes_whose_default_changed_move():
    before = {"ptc": "m-a", "flash": "m-f"}
    assert thread_model.default_moves(before, {"ptc": "m-b", "flash": "m-f"}) == {
        "ptc": ("m-a", "m-b")
    }
    # No old default means no thread can be on it.
    assert thread_model.default_moves({"ptc": None, "flash": "m-f"}, {"ptc": "m-b", "flash": "m-f"}) == {}


# ---------------------------------------------------------------------------
# The model a turn runs
# ---------------------------------------------------------------------------


async def _turn_model(catalog, *, named=None, held=None, pref=None, cleared=True, now=None):
    """``cleared`` is whether the conditional clear won; ``now`` is what the
    thread holds when it is read again after losing."""
    clear = AsyncMock(return_value=cleared)
    get_pref = AsyncMock(return_value=pref or {})
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_catalog(catalog)),
        patch(f"{_MOD}.user_models.get_model_preference", get_pref),
        patch(f"{_MOD}.threads_write.clear_thread_llm_model", clear),
        patch(
            f"{_MOD}.threads_read.get_thread_auth_meta",
            AsyncMock(return_value={"llm_model": now}),
        ),
    ):
        result = await thread_model.turn_model(
            "user-1", "thread-1", named=named, held=held
        )
    return result, clear, get_pref


@pytest.mark.asyncio
async def test_a_live_thread_model_runs_the_turn():
    result, clear, _ = await _turn_model(["m-a", "other"], held="m-a")
    assert result == "m-a"
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_custom_model_counts_as_live():
    pref = {"custom_models": [{"name": "my-model", "model_id": "x", "provider": "p"}]}
    result, clear, _ = await _turn_model(["other"], held="my-model", pref=pref)
    assert result == "my-model"
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_dead_thread_model_is_cleared_and_the_default_runs():
    result, clear, _ = await _turn_model(["m-a"], held="retired")
    assert result is None
    # Conditional on the name read, so a model named since is never lost.
    clear.assert_awaited_once_with("thread-1", "retired")


@pytest.mark.asyncio
async def test_a_clear_lost_to_a_newer_pick_runs_the_pick():
    """A pick saved after the send read the dead model wins, as it does over
    the send's own write."""
    result, _, _ = await _turn_model(["m-b"], held="retired", cleared=False, now="m-b")
    assert result == "m-b"


@pytest.mark.asyncio
async def test_a_clear_lost_to_another_dead_model_runs_the_default():
    result, _, _ = await _turn_model(["m-b"], held="retired", cleared=False, now="gone")
    assert result is None


@pytest.mark.asyncio
async def test_an_unloaded_manifest_keeps_the_thread_model():
    result, clear, _ = await _turn_model([], held="m-a")
    assert result == "m-a"
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_model_reads_nothing():
    result, clear, get_pref = await _turn_model(["m-a"])
    assert result is None
    get_pref.assert_not_awaited()
    clear.assert_not_awaited()


@pytest.mark.parametrize("held", [None, "m-a"], ids=["new_thread", "other_model"])
@pytest.mark.asyncio
async def test_a_named_model_beats_the_threads_and_is_judged_downstream(held):
    """Nothing of the thread's is at stake, so even a dead name goes to the
    resolver untouched, which refuses it as ``model_removed``."""
    result, clear, get_pref = await _turn_model(["m-a"], named="retired", held=held)
    assert result == "retired"
    get_pref.assert_not_awaited()
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_dead_named_model_the_thread_holds_is_cleared_and_still_refused():
    """The client re-sent the thread's dead model: the thread forgets it, so a
    refetch shows the default, and the name still reaches the resolver."""
    result, clear, _ = await _turn_model(["m-a"], named="retired", held="retired")
    assert result == "retired"
    clear.assert_awaited_once_with("thread-1", "retired")


@pytest.mark.asyncio
async def test_a_live_named_model_the_thread_holds_runs():
    result, clear, _ = await _turn_model(["m-a"], named="m-a", held="m-a")
    assert result == "m-a"
    clear.assert_not_awaited()


# ---------------------------------------------------------------------------
# PATCH validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unknown_model_is_refused_as_unavailable():
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_catalog(["m-a"])),
        patch(f"{_MOD}.user_models.get_model_preference", AsyncMock(return_value={})),
        pytest.raises(HTTPException) as exc,
    ):
        await thread_model.require_selectable_model("user-1", "not-a-model")
    assert exc.value.status_code == 400
    assert exc.value.detail["type"] == "model_unavailable"


@pytest.mark.parametrize(
    "name, pref",
    [
        ("m-a", {}),
        ("my-model", {"custom_models": [{"name": "my-model", "model_id": "x", "provider": "p"}]}),
        ("my-gw", {"custom_providers": [{"name": "my-gw", "parent_provider": "openai"}]}),
    ],
    ids=["manifest", "custom_model", "custom_provider"],
)
@pytest.mark.asyncio
async def test_a_runnable_model_is_accepted(name, pref):
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_catalog(["m-a"])),
        patch(f"{_MOD}.user_models.get_model_preference", AsyncMock(return_value=pref)),
    ):
        await thread_model.require_selectable_model("user-1", name)


# ---------------------------------------------------------------------------
# What a changed default does to existing threads
# ---------------------------------------------------------------------------


async def _carry(after, apply_to, before=None):
    reassign = AsyncMock(return_value=3)
    with (
        patch(f"{_MOD}.system_default_models", return_value=SYSTEM),
        patch(f"{_MOD}.threads_write.reassign_thread_llm_models", reassign),
    ):
        moved = await thread_model.carry_default_change(
            "user-1",
            before=before or {"preferred_model": "m-a"},
            after=after,
            apply_to=apply_to,
        )
    return moved, reassign


@pytest.mark.parametrize(
    "saved_scope, apply_to",
    [
        (None, None),
        ("ask", None),
        ("new_threads", None),
        ("existing_threads", "new_threads"),
    ],
)
@pytest.mark.asyncio
async def test_only_existing_threads_scope_moves_anything(saved_scope, apply_to):
    after = {"preferred_model": "m-b"}
    if saved_scope:
        after["default_model_scope"] = saved_scope
    moved, reassign = await _carry(after, apply_to)
    assert moved == 0
    reassign.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_saved_scope_moves_threads_when_the_write_names_none():
    after = {"preferred_model": "m-b", "default_model_scope": "existing_threads"}
    moved, reassign = await _carry(after, None)
    assert moved == 3
    reassign.assert_awaited_once_with(
        "user-1", {"ptc": ("m-a", "m-b"), "flash": ("m-a", "m-b")}, conn=None
    )


@pytest.mark.asyncio
async def test_the_one_shot_answer_overrides_the_saved_scope():
    after = {
        "preferred_model": "m-b",
        "preferred_flash_model": "m-a",
        "default_model_scope": "new_threads",
    }
    moved, reassign = await _carry(after, "existing_threads")
    assert moved == 3
    # The flash default did not change, so flash threads stay.
    reassign.assert_awaited_once_with("user-1", {"ptc": ("m-a", "m-b")}, conn=None)


@pytest.mark.asyncio
async def test_an_unchanged_default_moves_nothing():
    moved, reassign = await _carry({"preferred_model": "m-a"}, "existing_threads")
    assert moved == 0
    reassign.assert_not_awaited()
