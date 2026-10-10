"""The main agent's state schema: every field its middleware keep, declared
whichever of them a build includes.

A graph writes back only the channels it declares, and the Postgres saver
keeps a primitive value (a count, a flag) inside the checkpoint row rather
than beside it, so one write through a graph that lacks such a field erases
it. The main agent's checkpoints are written by builds that differ (a turn's,
and thread maintenance's, which leaves the subagent switch out) and by the
server's history reader appending a ui record, so each declares this schema.
"""

from __future__ import annotations

from ptc_agent.agent.middleware.compaction.notes import NotesDueState
from ptc_agent.agent.middleware.compaction.types import CompactionState
from ptc_agent.agent.middleware.skills.middleware import LoadedSkillsState
from ptc_agent.agent.middleware.subagent_switch import SubagentSwitchState
from ptc_agent.agent.state import DeltaAgentState


# DeltaAgentState last: a TypedDict takes each field from the last base that
# declares it, and only DeltaAgentState gives ``messages`` its DeltaChannel.
class MainAgentState(
    CompactionState,
    NotesDueState,
    SubagentSwitchState,
    LoadedSkillsState,
    DeltaAgentState,
):
    """The main agent's checkpointed state; see the module docstring."""
