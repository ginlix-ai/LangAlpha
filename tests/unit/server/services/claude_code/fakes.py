"""What the Claude Code token tests share: the cache every worker reads, and
the one patch that points the token services at it."""

from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

from src.server.services.claude_code import checks, flow, machine, saved

#: Every module of the token services that reads the cache.
_READERS = (checks, flow, saved, machine)


class Cache:
    """The cache's get, set and lock, with Redis up unless ``redis`` is False."""

    def __init__(self, redis=True):
        self.redis = self.enabled = redis
        self.values, self.ttls, self.locks, self.lock_ttls = {}, {}, {}, {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ttl=None):
        if self.redis:
            self.values[key], self.ttls[key] = value, ttl
        return self.redis

    async def acquire_lock(self, key, token, ttl_ms):
        if not self.redis:
            return None
        if self.locks.setdefault(key, token) != token:
            return False
        self.lock_ttls[key] = ttl_ms
        return True

    async def release_lock(self, key, token):
        if self.locks.get(key) == token:
            del self.locks[key]

    async def delete(self, key):
        self.values.pop(key, None)

    async def claim(self, key, value):
        if self.values.get(key) != value:
            return False
        del self.values[key]
        return True

    @property
    def client(self):
        """The raw client the sign-in's flow is kept through."""
        cache = self

        class _Client:
            async def get(self, key):
                return cache.values.get(key, cache.locks.get(key))

            async def set(self, key, value, ex=None):
                cache.values[key], cache.ttls[key] = value, ex

            async def eval(self, script, numkeys, key, value, ttl, since):
                """``machine._UNLESS_NEWER_LUA``, whose Lua the integration suite runs on Redis."""
                kept = cache.values.get(key)
                if isinstance(kept, dict) and float(kept.get("checked_at", 0)) > float(since):
                    return 0
                if value == "":
                    cache.values.pop(key, None)
                else:
                    cache.values[key], cache.ttls[key] = json.loads(value), ttl
                return 1

        return _Client()


@contextmanager
def serving(cache: Cache):
    """The token services on ``cache``, a sign-in's pending state included, checking on one model."""
    with ExitStack() as stack:
        for module in _READERS:
            stack.enter_context(patch.object(module, "get_cache_client", return_value=cache))
        stack.enter_context(patch("src.server.services.oauth_signin.require_cache", return_value=cache))
        stack.enter_context(patch.object(checks, "token_check_model", return_value="claude-haiku-5-5"))
        yield cache
