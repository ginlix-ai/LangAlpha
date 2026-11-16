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

# The config key that marks Home's stand-in, so a listing leaves it out by the
# row alone (``home_stand_in.EXCLUDE_STAND_IN``) without knowing whose it is.
HOME_STAND_IN_KEY = "home_stand_in"

# What a user's id is suffixed with to name their stand-in's id.
_STAND_IN_NAME = "/stand-in"

# The key a run in Home's stand-in names the stand-in's computer under, on its
# turn's run row and on the task runs that turn starts: the thread is Home's,
# on another computer, so a machine-scoped read of live work (the idle sweep)
# finds the run only by this.
STAND_IN_COMPUTER = "stand_in_computer"


def get_home_workspace_id(user_id: str) -> str:
    return str(uuid.uuid5(HOME_WORKSPACE_NAMESPACE, user_id))


def _uuid5_sql(name_sql: str) -> str:
    """``uuid.uuid5(HOME_WORKSPACE_NAMESPACE, name)`` of the SQL text
    ``name_sql``: a version 5 uuid over pgcrypto's sha1 (migration 001)."""
    hashed = (
        f"substring(digest(decode('{HOME_WORKSPACE_NAMESPACE.hex}', 'hex')"
        f" || convert_to({name_sql}, 'UTF8'), 'sha1') from 1 for 16)"
    )
    return (
        "(SELECT encode(set_byte(set_byte(h, 6, (get_byte(h, 6) & 15) | 80),"
        " 8, (get_byte(h, 8) & 63) | 128), 'hex')::uuid"
        f" FROM (SELECT {hashed} AS h) AS name_hash)"
    )


def home_id_sql(user_id_sql: str) -> str:
    """``get_home_workspace_id`` of the SQL expression ``user_id_sql``, so a
    statement can tell a user's Home by its id, which no row marks."""
    return _uuid5_sql(f"({user_id_sql})::text")


def get_home_stand_in_id(user_id: str) -> str:
    """Home's stand-in: a folder on one of the user's cloud computers where a
    Home turn runs while the machine Home lives on can't be reached. A hidden
    workspace row, placed once and again only when its computer can no longer
    keep it (``home_stand_in.place_home_stand_in``); never Home itself, so
    callers that mean it name it (``is_home_stand_in_id``)."""
    return str(uuid.uuid5(HOME_WORKSPACE_NAMESPACE, f"{user_id}{_STAND_IN_NAME}"))


def stand_in_id_sql(user_id_sql: str) -> str:
    """``get_home_stand_in_id`` of the SQL expression ``user_id_sql``."""
    return _uuid5_sql(f"({user_id_sql})::text || '{_STAND_IN_NAME}'")


def is_home_stand_in_id(user_id: Any, workspace_id: Any) -> bool:
    """Whether ``workspace_id`` is ``user_id``'s Home stand-in, in any case a client spells it."""
    if not user_id or not workspace_id:
        return False
    return str(workspace_id).lower() == get_home_stand_in_id(str(user_id))


def is_home_stand_in_row(workspace: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``workspace`` is a Home stand-in, by its mark."""
    return bool(workspace) and bool(
        (workspace.get("config") or {}).get(HOME_STAND_IN_KEY)
    )


def acting_for(user_id: Any, workspace_id: str) -> str:
    """The workspace ``workspace_id``'s folder works for: Home's for its
    stand-in, else its own. The stand-in runs only Home's turns, so it reads
    Home's tool selection, not a copy that would miss a later change, and
    serves Home's thread transcripts, having none of its own."""
    if is_home_stand_in_id(user_id, workspace_id):
        return get_home_workspace_id(str(user_id))
    return workspace_id


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


def is_reserved_row(workspace: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``workspace`` is its owner's Home or Home's stand-in, which no
    one renames, duplicates or deletes: to the user and the agent the stand-in
    is Home, where a Home turn runs while Home's machine is away."""
    return is_home_row(workspace) or is_home_stand_in_row(workspace)
