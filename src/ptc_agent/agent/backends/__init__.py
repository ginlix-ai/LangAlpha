"""Backend implementations for deepagent middleware."""

from .sandbox import SandboxBackend

# Backward-compat alias
DaytonaBackend = SandboxBackend

__all__ = ["DaytonaBackend", "SandboxBackend"]
