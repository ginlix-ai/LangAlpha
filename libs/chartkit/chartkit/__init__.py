"""chartkit: publish explainable chart layers onto the user's chart from sandbox Python.

Ships into the sandbox as ``chartkit`` (``_internal/src/chartkit/``). Submodules load on first
use, so ``import chartkit`` stays cheap and pandas/numpy are only imported by the code that needs
them. The agent-facing reference is the chart-annotation skill's ``chartkit.md``.
"""

from __future__ import annotations

import importlib

__all__ = ["Chart", "ChartError", "analysis", "bars", "future_times", "snapshot", "ta"]

_LAZY = {
    "Chart": ("chart", "Chart"),
    "ChartError": ("_util", "ChartError"),
    "bars": ("data", "bars"),
    "future_times": ("analysis", "future_times"),
    "snapshot": ("_snapshot", "snapshot"),
    "analysis": ("analysis", None),
    "ta": ("ta", None),
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module 'chartkit' has no attribute {name!r}; it has {__all__}")
    module, attr = _LAZY[name]
    mod = importlib.import_module(f".{module}", __name__)
    value = mod if attr is None else getattr(mod, attr)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
