"""Reach the repo root, then hand back the real bootstrap.

A launched server gets its own directory as ``sys.path[0]``, which used to be
``mcp_servers/`` and is now this bundle, so the sibling ``_bootstrap`` it
imports first has to exist here. Delegating rather than copying is the point:
the sandbox flattens every server back into one ``mcp_servers/`` directory and
ships a single ``_bootstrap.py``, so a real copy per bundle would be three
files where only one of them ever runs there.
"""

import sys
from pathlib import Path

_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file())
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from mcp_servers._bootstrap import MCPServer  # noqa: E402

__all__ = ["MCPServer"]
