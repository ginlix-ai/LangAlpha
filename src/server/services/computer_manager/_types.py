"""Value types and module constants shared by the ComputerManager mixins."""

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Set, Tuple

from ptc_agent.core.session import Session


# This cache stores which projects this process has already materialised on a
# sandbox, not authority over any of it: every step behind it is guarded by its
# own on-disk marker.
_PROJECTS_ATTACHED_CAP = 4096
_WORKSPACE_TOOL_VIEWS_CAP = 64

# The decision key is shared with egress grant sync and platform-secret sweeps,
# so brief contention is expected.
_MACHINE_DECISION_LOCK_TIMEOUT_MS = 30_000

_SEEDED_AGENT_MD = """# Workspace Notes

<!--
This is a starter template. Replace these comments with real content
as you work. The system prompt has full guidelines on what to maintain.
-->

## Thread Index

## Key Findings

## File Index
"""


class WorkspaceNotOnComputer(ValueError):
    """Nothing below the edge resolve has a second addressing mode to fall back to.

    Reachable for a flash workspace, which has no sandbox lifecycle at all, and
    for a row that disappeared between two reads."""

    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id
        super().__init__(
            f"Workspace {workspace_id} is not on a computer, so it has no "
            "sandbox lifecycle; flash workspaces use agent_mode='flash'"
        )


@dataclass(frozen=True)
class ComputerBinding:
    """Which machine a project is on, and how to reach that machine's backend.

    An empty workspace_id is a machine operation that names no project, and
    such a binding carries no folder either. dir_name is the project's folder
    on the machine, resolved in the same read as the machine itself: a bound
    project's folder never moves, so a holder of this binding never has to ask
    again. None on a project binding means only that this read did not answer
    for the folder, so the layout resolves it from the row; a caller that
    builds a project binding out of a machine row has to supply it."""

    workspace_id: str
    computer_id: str
    dir_name: Optional[str] = None
    kind: Optional[str] = None
    root_dir: Optional[str] = None
    provider_ref: Optional[str] = None
    resource_tier: Optional[str] = None
    is_always_on: bool = False
    provider_config: Optional[Dict[str, Any]] = None

    @property
    def provider_identity(self) -> Tuple[Optional[str], str]:
        """Row-level provider overrides distinguish backends of the same kind."""
        return (self.kind, json.dumps(self.provider_config or {}, sort_keys=True))


@dataclass(frozen=True)
class WorkspaceToolView:
    """One project's tool configuration, frozen for the turn that follows.

    The session is the machine's, and its MCP fields are rewritten by whichever
    sibling resolved last, outside the machine lock. A turn that read them at
    build time could carry a sibling's registry and direct tools; it reads
    this instead, taken the moment this project's composite was installed."""

    workspace_id: str
    session: Session
    mcp_registry: Any
    mcp_tool_summary: Optional[str]
    direct_mcp_tools: Mapping[str, Any]
    egress_binding: Any
    mcp_config_version: Optional[int]
    mcp_servers: tuple[Any, ...] = ()
    mcp_settled_servers: frozenset[str] = frozenset()
    built_at: float = field(default_factory=time.monotonic)

    @classmethod
    def from_session(cls, workspace_id: str, session: Session) -> "WorkspaceToolView":
        return cls(
            workspace_id=workspace_id,
            session=session,
            mcp_registry=session.mcp_registry,
            mcp_tool_summary=session.mcp_tool_summary,
            direct_mcp_tools=MappingProxyType(dict(session.direct_mcp_tools or {})),
            egress_binding=session.egress_binding,
            mcp_config_version=session.mcp_config_version,
            mcp_servers=tuple(getattr(session.config.mcp, "servers", ()) or ()),
            mcp_settled_servers=frozenset(
                getattr(session, "mcp_settled_servers", ()) or ()
            ),
        )

    @classmethod
    def builtin_only(cls, workspace_id: str, session: Session) -> "WorkspaceToolView":
        """What a project gets when its own composite never installed: the
        built-in servers and no directly bound tools, never a sibling's set."""
        return cls(
            workspace_id=workspace_id,
            session=session,
            mcp_registry=session._builtin_mcp_registry or session.mcp_registry,
            mcp_tool_summary=None,
            direct_mcp_tools=MappingProxyType({}),
            egress_binding=None,
            mcp_config_version=None,
            mcp_servers=tuple(getattr(session, "_pristine_mcp_servers", ()) or ()),
            mcp_settled_servers=frozenset(),
        )


@dataclass
class SessionMetadata:
    """A NULL workspace_id represents a computer with no project."""

    workspace_id: Optional[str] = None
    computer_id: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sandbox_id: Optional[str] = None
    request_count: int = 0

    def touch(self) -> None:
        self.last_active = datetime.now(timezone.utc)
        self.request_count += 1


@dataclass
class MachineState:
    """This worker's execution context for one machine, never authority over it.

    One record per machine, so a teardown drops the whole context in a single
    move. The twelve maps this replaces had to be remembered site by site, and
    whichever one a path forgot outlived the session it described."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    session: Optional[Session] = None
    meta: Optional[SessionMetadata] = None
    # Which folder holds the machine's root files; see _layout_root_owner_dir.
    # A machine's root owner never changes, so one read per machine per process
    # is enough. root_owner_read distinguishes "read, has none" from "unread",
    # which keeps a failed read out of the cache.
    root_owner_dir: Optional[str] = None
    root_owner_read: bool = False
    # This worker owns an unpromoted lazy start: it promotes on success and
    # reverts on failure, so no other path may retire or reap the row.
    pending_lazy_sync: bool = False
    # Elevated tier seen at restart; Phase 2 rechecks the entitlement without an
    # extra read for standard-tier machines.
    pending_tier_recheck: bool = False
    # A withheld version stamp is read only on the slow path, so this marker
    # bypasses cooldown rather than leaving a superseded session live for 30s.
    resolve_superseded: Set[str] = field(default_factory=set)
    # Phase 2 runs outside the machine lock; racing warm and chat callers wait
    # on the owner's event instead of duplicating readiness and asset sync.
    phase2_event: Optional[asyncio.Event] = None
    last_sync_at: Optional[float] = None
    # What the idle sweep could not stop, so it stops retrying and re-logging
    # every cycle. Execution context, never read as truth about the machine.
    idle_reap_failures: int = 0
    idle_reap_backoff_until: float = 0.0
    # Discovery runs outside the response path and the machine lock because
    # stdio cold starts take up to 30s. Tracked per machine so teardown cancels
    # the probes before they write orphan schemas.
    discovery_tasks: Set[asyncio.Task] = field(default_factory=set)
    # Each project's frozen tool configuration, keyed by workspace; see
    # WorkspaceToolView. Execution context, dropped with the session.
    tool_views: OrderedDict[str, WorkspaceToolView] = field(default_factory=OrderedDict)

    def forget_session(self) -> None:
        """Drop everything the session owned, keeping the lock its caller may hold.

        The lock belongs to the machine and outlives any session on it:
        replacing it under a holder would let the next caller build a second one
        and run alongside."""
        self.session = None
        self.meta = None
        self.pending_lazy_sync = False
        self.pending_tier_recheck = False
        self.resolve_superseded.clear()
        self.phase2_event = None
        self.last_sync_at = None
        self.tool_views.clear()
