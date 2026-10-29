"""A user's subagents as the pane and the agent's files see them.

Two writers share this package: REST for the pane (``app/subagents.py``) and
the file route over ``.agents/user/agents/``. Both read the same view
(``view``), refuse the same values (``rules``) and save through the same
steps (``writes``: ``prepare_save`` before the user's lock, ``apply_save``
under it), so a value one of them refuses the other refuses too, and a save
from either moves the version the other checks.

The per-turn merge that turns rows into the roster a build compiles stays in
``ptc_agent.agent.subagents.merge``; nothing here runs during a turn.
"""

from .context import SubagentsUnavailable
from .rules import DESCRIPTION_MAX, INSTRUCTIONS_MAX_BYTES, MAX_CUSTOM, SubagentInvalid
from .writes import (
    SubagentNotFound,
    VersionConflict,
    delete_subagent,
    list_subagents,
    save_subagent,
)

__all__ = [
    "DESCRIPTION_MAX",
    "INSTRUCTIONS_MAX_BYTES",
    "MAX_CUSTOM",
    "SubagentInvalid",
    "SubagentNotFound",
    "SubagentsUnavailable",
    "VersionConflict",
    "delete_subagent",
    "list_subagents",
    "save_subagent",
]
