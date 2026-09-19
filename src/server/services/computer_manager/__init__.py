"""Machine-owned sandbox sessions and runtime lifecycle.

Caches are keyed by machine so sibling workspaces share one session.
WorkspaceManager provides the project-facing facade.

The implementation is split across mixin modules by concern, assembled into
one class in ``manager``. The mixins share ``self`` and are not usable on
their own.
"""

from src.server.services.computer_manager._types import (
    ComputerBinding,
    SessionMetadata,
)
from src.server.services.computer_manager.manager import ComputerManager

__all__ = [
    "ComputerBinding",
    "ComputerManager",
    "SessionMetadata",
]
