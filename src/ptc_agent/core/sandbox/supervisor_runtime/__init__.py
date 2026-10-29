"""MCP supervisor: one daemon per computer owning every MCP server process.

Ships verbatim into ``_internal/src/supervisor/`` and runs there as
``python3 -m supervisor <computer_root>``. Kept on the host inside ``src/`` so
it is linted and unit-testable like any other module; nothing on the host
imports it at runtime.
"""

from .daemon import Supervisor, main

__all__ = ["Supervisor", "main"]
