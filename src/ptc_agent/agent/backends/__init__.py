"""Backend implementations for deepagent middleware."""

from typing import Union

from .composite import CompositeFilesystemBackend
from .memory import (
    InvalidMemoryKeyError,
    MemoryContentTooLargeError,
    NamespaceFactory,
    StoreMemoryBackend,
    validate_memory_key,
)
from .sandbox import SandboxBackend

# Backward-compat alias
DaytonaBackend = SandboxBackend

FilesystemBackend = Union[SandboxBackend, CompositeFilesystemBackend]

__all__ = [
    "CompositeFilesystemBackend",
    "DaytonaBackend",
    "FilesystemBackend",
    "InvalidMemoryKeyError",
    "MemoryContentTooLargeError",
    "NamespaceFactory",
    "SandboxBackend",
    "StoreMemoryBackend",
    "validate_memory_key",
]
