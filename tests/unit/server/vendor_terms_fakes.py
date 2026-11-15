"""An in-memory stand-in for the Redis calls ``services.vendor_terms`` makes."""

from __future__ import annotations

from types import SimpleNamespace


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def pipeline(self, transaction: bool = True) -> _Pipe:
        return _Pipe(self)

    async def mget(self, keys):
        return [None if k not in self.values else str(self.values[k]).encode() for k in keys]

    async def delete(self, key):
        return 1 if self.values.pop(key, None) is not None else 0


class _Pipe:
    def __init__(self, redis: FakeRedis) -> None:
        self.redis, self.ops = redis, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def set(self, key, value, nx=False, px=None):
        self.ops.append(("set", key, value, nx))

    def pexpire(self, key, ms):
        self.ops.append(("pexpire", key))

    async def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "set":
                if op[3] and op[1] in self.redis.values:
                    out.append(None)
                else:
                    self.redis.values[op[1]] = op[2]
                    out.append(True)
            else:
                out.append(op[1] in self.redis.values)
        return out


def fake_cache(redis: FakeRedis):
    return lambda: SimpleNamespace(enabled=True, client=redis)
