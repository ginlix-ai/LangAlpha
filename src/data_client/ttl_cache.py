"""A per-process cache with a size bound and an age limit, for answers a tool pages through."""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Callable, Generic, Hashable, TypeVar

V = TypeVar("V")


class TtlCache(Generic[V]):
    """At most *size* entries, each kept *ttl* seconds; when full, the least recently read goes first."""

    def __init__(self, size: int, ttl: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._size, self._ttl, self._clock = size, ttl, clock
        self._entries: OrderedDict[Hashable, tuple[float, V]] = OrderedDict()

    def get(self, key: Hashable) -> V | None:
        hit = self._entries.get(key)
        if hit is None:
            return None
        if self._clock() - hit[0] > self._ttl:
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return hit[1]

    def put(self, key: Hashable, value: V) -> None:
        """Keep *value*, dropping expired entries and then the least recently read past *size*. None is refused,
        since get reads it as a miss."""
        if value is None:
            raise ValueError("a TtlCache keeps no None")
        now = self._clock()
        self._entries[key] = (now, value)
        self._entries.move_to_end(key)
        for stale in [k for k, (at, _) in self._entries.items() if now - at > self._ttl]:
            del self._entries[stale]
        while len(self._entries) > self._size:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)
