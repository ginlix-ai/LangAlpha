"""The file bytes the daemon keeps across commands, within a budget."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .views import View

_Key = tuple[str, str, "View | None"]


def _unindex(index: dict, name: object, key: _Key) -> None:
    keys = index[name]
    keys.discard(key)
    if not keys:
        del index[name]


class Contents:
    """File bytes by (path, version), bounded by their total size; the one
    used longest ago goes first.

    Bytes whose version is their digest are any command's. A route may keep
    a version while its content moves (an automation file's run state), so
    other bytes are kept per command too, and one command's never replace or
    stand for another's.
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._items: OrderedDict[_Key, bytes] = OrderedDict()
        self._paths: dict[str, set[_Key]] = {}
        self._owners: dict[View, set[_Key]] = {}
        self._size = 0

    def get(self, path: str, version: str | None, view: View) -> bytes | None:
        if not version:
            return None
        for key in ((path, version, None), (path, version, view)):
            data = self._items.get(key)
            if data is not None:
                self._items.move_to_end(key)
                return data
        return None

    def put(
        self, path: str, version: str | None, data: bytes, owner: View | None, exact: bool
    ) -> None:
        if not version or len(data) > self.limit or not (exact or owner is not None):
            return
        key = (path, version, None if exact else owner)
        self._discard(key)
        self._items[key] = data
        self._paths.setdefault(path, set()).add(key)
        if key[2] is not None:
            self._owners.setdefault(key[2], set()).add(key)
        self._size += len(data)
        while self._size > self.limit:
            self._discard(next(iter(self._items)))

    def drop(self, path: str) -> None:
        """Forget every version of the file, whoever read it."""
        for key in list(self._paths.get(path, ())):
            self._discard(key)

    def forget(self, owner: View) -> None:
        """Forget the bytes kept for one command alone, once its view goes:
        no command can be shown them again."""
        for key in list(self._owners.get(owner, ())):
            self._discard(key)

    def _discard(self, key: _Key) -> None:
        data = self._items.pop(key, None)
        if data is None:
            return
        self._size -= len(data)
        _unindex(self._paths, key[0], key)
        if key[2] is not None:
            _unindex(self._owners, key[2], key)
