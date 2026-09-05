"""The profile cache's reader and its invalidator must address the same key.

They were two copies of the same f-string, one in each function, which is a
silent failure if they ever drift: invalidation would delete a key nothing
reads and the stale profile would serve for a full 24h TTL. The key also
carries a shape version, so a change to the cached dict retires the entries
written under the old shape instead of relying on a manual flush.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.graph import (
    _USER_PROFILE_SHAPE,
    _user_profile_cache_key,
    get_user_profile_for_prompt,
    invalidate_user_profile_cache,
)


class _FakeCache:
    """Minimal stand-in that records the keys it is asked for."""

    def __init__(self, store=None):
        self.enabled = True
        self.store = store or {}
        self.deleted: list[str] = []
        self.client = MagicMock()
        self.client.get = AsyncMock(side_effect=lambda k: self.store.get(k))
        self.client.set = AsyncMock(side_effect=self._set)
        self.client.delete = AsyncMock(side_effect=self.deleted.append)

    async def _set(self, key, value, ex=None):
        self.store[key] = value


def _patch_cache(cache):
    return patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache)


def test_key_carries_the_shape_version():
    key = _user_profile_cache_key("user-1")
    assert key == f"user_profile_prompt:v{_USER_PROFILE_SHAPE}:user-1"


@pytest.mark.asyncio
async def test_invalidate_deletes_exactly_the_key_the_reader_reads():
    """The drift guard. Asserted against the reader's own key, not a literal."""
    cache = _FakeCache()
    with _patch_cache(cache):
        await invalidate_user_profile_cache("user-1")
    assert cache.deleted == [_user_profile_cache_key("user-1")]


@pytest.mark.asyncio
async def test_reader_serves_and_stores_under_that_same_key():
    cache = _FakeCache({_user_profile_cache_key("user-1"): b'{"name": "cached"}'})
    with _patch_cache(cache):
        profile = await get_user_profile_for_prompt("user-1")
    assert profile == {"name": "cached"}


@pytest.mark.asyncio
async def test_the_profile_carries_no_model_preferences():
    """Model-scoped settings are resolved server-side and stamped on the config,
    so this dict never carries them. It is handed to the prompt renderer, whose
    profile template loops over ``agent_preference`` verbatim."""
    cache = _FakeCache()
    with (
        _patch_cache(cache),
        patch(
            "src.server.database.user.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value={
                "user": {"name": "Ada", "timezone": "UTC", "locale": "en-US"},
                "preferences": {
                    "agent_preference": {"proactive_questions": "auto"},
                    "model_preference": {"prompt_guidance": "lean"},
                },
            },
        ),
    ):
        profile = await get_user_profile_for_prompt("user-1")
    assert set(profile) == {"name", "timezone", "locale", "agent_preference"}


@pytest.mark.asyncio
async def test_an_entry_from_the_previous_shape_is_not_served():
    """What replaces a manual flush: the old key is simply never addressed."""
    cache = _FakeCache({"user_profile_prompt:user-1": b'{"name": "pre-shape"}'})
    with (
        _patch_cache(cache),
        patch(
            "src.server.database.user.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value={"user": {"name": "fresh"}, "preferences": {}},
        ),
    ):
        profile = await get_user_profile_for_prompt("user-1")
    assert profile["name"] == "fresh"
    assert cache.store["user_profile_prompt:user-1"] == b'{"name": "pre-shape"}'
