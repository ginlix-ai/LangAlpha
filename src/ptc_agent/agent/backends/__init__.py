"""Filesystem backends behind the agent's read/write/glob/grep tools.

``CompositeFilesystemBackend`` prefix-routes between ``SandboxBackend``
(default, Daytona-backed) and one or more ``StoreBackend`` instances —
the latter wrap a LangGraph ``BaseStore`` to host the cross-workspace
memory and memo tiers under ``.agents/user/`` and ``.agents/workspace/``.
``RequestScopedStoreCache`` is shared across the store-backed routes and
the read-side middleware so per-turn lookups dedupe to one round-trip.
"""

from typing import Union

from .composite import CompositeFilesystemBackend
from .langgraph_store import (
    InvalidStoreKeyError,
    StoreContentTooLargeError,
    NamespaceFactory,
    ReadOnlyStoreError,
    StoreBackend,
    lock_for_namespace,
    validate_store_key,
)
from .sandbox import SandboxBackend
from .store_cache import RequestScopedStoreCache

# Backward-compat alias
DaytonaBackend = SandboxBackend

FilesystemBackend = Union[SandboxBackend, CompositeFilesystemBackend]

__all__ = [
    "CompositeFilesystemBackend",
    "DaytonaBackend",
    "FilesystemBackend",
    "InvalidStoreKeyError",
    "StoreContentTooLargeError",
    "NamespaceFactory",
    "ReadOnlyStoreError",
    "RequestScopedStoreCache",
    "SandboxBackend",
    "StoreBackend",
    "lock_for_namespace",
    "validate_store_key",
]
