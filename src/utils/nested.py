"""Helpers for nested JSON-shaped values."""

from __future__ import annotations

from collections.abc import Set
from typing import Any


def without_keys(value: Any, keys: Set[str]) -> Any:
    """A copy of ``value`` less every dict entry under one of ``keys``, at any depth.

    Every dict and list is rebuilt, so the caller may change the copy without
    touching a stored or cached original.
    """
    if isinstance(value, dict):
        return {k: without_keys(v, keys) for k, v in value.items() if k not in keys}
    if isinstance(value, list):
        return [without_keys(v, keys) for v in value]
    return value
