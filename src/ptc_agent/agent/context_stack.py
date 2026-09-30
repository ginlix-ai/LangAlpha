"""The three runtime-context middlewares, built once for both agent flavors.

PTC and Flash assemble different tools and different baselines, but the three
middlewares that tell the model what world it is running in are the same three
in the same relative order. Built here, a change to that wiring cannot land in
one stack and miss the other.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ptc_agent.agent.middleware.runtime_context import (
    BaselineContextMiddleware,
    BaselineSources,
    BlockReader,
    TailEnvelopeMiddleware,
    TurnContext,
    TurnContextMiddleware,
    resolve_preferred_market,
)
from ptc_agent.agent.middleware.runtime_context.profile import profile_read_answered
from ptc_agent.agent.roles import AgentRole


@dataclass(frozen=True, slots=True)
class ContextMiddleware:
    """The three middlewares, each named for the position it has to take.

    ``turn`` and ``baseline`` both write at the turn boundary, right after the
    turn's user message, and the order they appear in the stack is the order
    their rows land in: the turn row first (when the turn opened, the market
    session, the gap since the last turn, the run context), then the baseline's
    rows for whatever moved underneath it. ``baseline`` must also follow the
    caching middlewares so its block lands after their system breakpoint and
    carries its own, and ``tail`` is innermost overall so what it carries is the
    true tail of the request and the breakpoint it pins stays on the last block.
    """

    turn: TurnContextMiddleware
    baseline: BaselineContextMiddleware
    tail: TailEnvelopeMiddleware


def build_context_middleware(
    *,
    now: datetime,
    guidance: str | None,
    model_name: str | None,
    turn_context: TurnContext | None = None,
    user_profile: dict | None = None,
    sandbox_enabled: bool,
    session: Any | None = None,
    workspace_name: str | None = None,
    workspace_description: str | None = None,
    sources: BaselineSources | None = None,
    blocks: dict[str, BlockReader] | None = None,
    user_data_counts: dict[str, Any] | None = None,
    files_mounted: bool | None = None,
    role: AgentRole = "analyst",
) -> ContextMiddleware:
    """Wire the turn row, the per-thread baseline and the tail envelope.

    Everything from ``session`` down is the baseline's own material, which is
    where the two flavors differ: PTC reads a workspace and a sandbox, Flash
    carries the user's identity and the memory index alone. ``blocks`` is the
    same split for the harness-authored text: both flavors state their skills
    manifest, only PTC has an MCP roster to state. ``role`` reaches the blocks
    whose wording differs by role; a build that names none is the analyst.
    """
    turn = turn_context or TurnContext()
    # Both platform reads answer None on failure, and a market or a zone
    # derived from a failed read is the product default rather than this
    # user's. None here, for the zone as for the market, lets the turn row
    # take what the frozen identity block states and the baseline carry the
    # previous identity forward. ``turn.timezone`` keeps a zone the request
    # names, which is this user's whatever the profile read did; the locale
    # default its tools fall back to is ``tool_timezone`` and never comes here.
    preferred_market = (
        resolve_preferred_market(user_profile, user_data_counts)
        if profile_read_answered(user_profile) and user_data_counts is not None
        else None
    )
    return ContextMiddleware(
        turn=TurnContextMiddleware(
            now=now,
            timezone=turn.timezone,
            preferred_market=preferred_market,
            last_turn_at=turn.last_turn_at,
            platform=turn.platform,
            origin=turn.origin,
            surface_rules=turn.surface_rules,
            inherits_rules=turn.inherits_rules,
            disk_free_mb=turn.disk_free_mb,
            disk_known=turn.disk_known,
        ),
        baseline=BaselineContextMiddleware(
            session=session,
            workspace_name=workspace_name,
            workspace_description=workspace_description,
            sources=sources,
            blocks=blocks,
            user_profile=user_profile,
            user_data_counts=user_data_counts,
            sandbox_enabled=sandbox_enabled,
            preferred_market=preferred_market,
            timezone=turn.timezone,
            guidance=guidance,
            model_name=model_name,
            files_mounted=files_mounted,
            role=role,
        ),
        tail=TailEnvelopeMiddleware(
            now=now,
            guidance=guidance,
            model_name=model_name,
        ),
    )
