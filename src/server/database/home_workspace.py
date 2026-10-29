"""Which row is a user's Home, where work in no particular workspace runs.

Identity is the row's id, never its status: once bound to a computer the row
takes that computer's lifecycle like any workspace on it, so only a Home that
was never bound still reads 'unbound'. The status test stays for a former
Home at another id, which an account merge can leave beside the user's own.

Nothing here imports the project, so every database module can ask.
"""

from __future__ import annotations

import uuid
from typing import Any, Mapping, Optional

# Every stored Home id derives from this value, so it never changes.
HOME_WORKSPACE_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")

# A Home's status until its first computer.
UNBOUND = "unbound"


def get_home_workspace_id(user_id: str) -> str:
    return str(uuid.uuid5(HOME_WORKSPACE_NAMESPACE, user_id))


def is_home_id(user_id: Any, workspace_id: Any) -> bool:
    """Whether ``workspace_id`` names ``user_id``'s Home, in any case a client spells it."""
    if not user_id or not workspace_id:
        return False
    return str(workspace_id).lower() == get_home_workspace_id(str(user_id))


def is_unbound(workspace: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``workspace`` is a Home on no computer yet."""
    return bool(workspace) and workspace.get("status") == UNBOUND


def is_home_row(workspace: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``workspace`` is its owner's Home, bound or not."""
    if not workspace:
        return False
    if is_unbound(workspace):
        return True
    return is_home_id(workspace.get("user_id"), workspace.get("workspace_id"))
