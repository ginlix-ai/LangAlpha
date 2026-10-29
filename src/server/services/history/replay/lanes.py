"""The lane a replayed row belongs to: the main agent's, or the ``task:…`` agent that wrote it."""

from __future__ import annotations

from typing import Any

MAIN_LANE = "main"


def agent_lane(agent: Any) -> str:
    return agent if isinstance(agent, str) and agent.startswith("task:") else MAIN_LANE
