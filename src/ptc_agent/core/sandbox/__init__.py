"""PTC Sandbox package — backward-compatible re-exports."""

from ptc_agent.core.sandbox.ptc_sandbox import (
    ChartData,
    ExecutionResult,
    PTCSandbox,
    SyncResult,
    _hash_dict,
    _resolve_local_path,
    _sha256_file,
)
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError

# Backward-compat alias: _DaytonaRetryPolicy was renamed to RetryPolicy
_DaytonaRetryPolicy = RetryPolicy

__all__ = [
    "PTCSandbox",
    "ChartData",
    "ExecutionResult",
    "SyncResult",
    "SandboxGoneError",
    "SandboxTransientError",
    "_DaytonaRetryPolicy",
    "_hash_dict",
    "_resolve_local_path",
    "_sha256_file",
]
