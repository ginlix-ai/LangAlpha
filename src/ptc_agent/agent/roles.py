"""The roles one agent runs in.

Its own module, importing nothing from the package, so the subagent definitions
and the runtime-context baseline can say what differs by role without importing
the agent that builds them.
"""

from __future__ import annotations

from typing import Literal, get_args

# The analyst works inside one workspace; the Chief of Staff works from Home and
# hands workspace work to the analysts.
AgentRole = Literal["analyst", "chief_of_staff"]

ALL_ROLES: frozenset[AgentRole] = frozenset(get_args(AgentRole))
