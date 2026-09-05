"""The stale-model-preference scrubber must not act on an empty catalog.

Regression: ``resolvable()`` asks the manifest whether a model name still
exists. When the manifest fails to load, every name answers "no" and the
scrubber deletes the user's whole model preference set through a merge-upsert
that keeps no copy. There is no undo, so the guard is the fix.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.llm.availability import cleanup_stale_model_preferences

STORED_PREFS = {
    "preferred_model": "some-model",
    "preferred_flash_model": "some-flash",
    "fetch_model": "some-fetch",
    "compaction_model": "some-compaction",
    "fallback_models": ["fallback-a", "fallback-b"],
    "profiles": {"some-model": {"reasoning_effort": "high"}},
}


def _catalog(names):
    mc = MagicMock()
    mc.llm_config = {n: {"model_id": n} for n in names}
    mc.get_model_config.side_effect = lambda n: mc.llm_config.get(n)
    return mc


async def _run(catalog_names):
    upsert = AsyncMock()
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_catalog(catalog_names)),
        patch(
            "src.server.services.llm.user_models.get_model_preference",
            AsyncMock(return_value=dict(STORED_PREFS)),
        ),
        patch("src.server.database.user.invalidate_user_prefs_cache", AsyncMock()),
        patch("src.server.database.user.upsert_user_preferences", upsert),
    ):
        removed = await cleanup_stale_model_preferences("user-1")
    return removed, upsert


@pytest.mark.asyncio
async def test_empty_manifest_scrubs_nothing_and_writes_nothing():
    """The destructive case. An empty catalog means "not loaded", not "all gone"."""
    removed, upsert = await _run([])
    assert removed == []
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_loaded_manifest_still_scrubs_what_really_vanished():
    """The guard must not disable the feature it is protecting."""
    removed, upsert = await _run(["something-else"])
    assert removed, "a populated catalog should still scrub names it does not contain"
    upsert.assert_awaited_once()
    written = upsert.await_args.kwargs["model_preference"]
    assert written["preferred_model"] is None


@pytest.mark.asyncio
async def test_the_scrub_states_the_delete_and_leaves_the_columns_to_the_merge():
    """Regression: a delete that only clears the model column is not a delete.

    ``get_model_preference`` reads ``other_preference`` underneath that column,
    so a stale name left there reappears on the very next read, is raised on,
    is scrubbed again, a loop that repeats every turn. Carrying the delete
    across to the pre-move copy is the merge-upsert's job for every writer
    (``TestMovedKeyDeletes`` in the database suite), so what is pinned here is
    that the scrub asks for the delete and does not hand-roll the other half.
    """
    _, upsert = await _run(["something-else"])
    kwargs = upsert.await_args.kwargs
    assert kwargs["model_preference"]["preferred_model"] is None
    assert "other_preference" not in kwargs


@pytest.mark.asyncio
async def test_the_legacy_column_still_answers_for_a_key_the_column_never_set():
    """The rollback window the legacy read exists for stays open."""
    from src.server.services.llm.user_models import get_model_preference

    with patch(
        "src.server.database.user.get_user_preferences",
        AsyncMock(
            return_value={
                "other_preference": {"preferred_model": "pre-move-model"},
                "model_preference": {"preferred_flash_model": "flash"},
            }
        ),
    ):
        pref = await get_model_preference("user-1")
    assert pref["preferred_model"] == "pre-move-model"
    assert pref["preferred_flash_model"] == "flash"
