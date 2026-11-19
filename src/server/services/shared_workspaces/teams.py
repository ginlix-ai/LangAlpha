"""The team a shared workspace names, as the auth service keeps it.

langalpha stores only the team's id on the workspace. Who may name a team
reads the caller's teams from their account; the team's name and people are
read from the auth service's roster when a client asks for them, and never
kept.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

from fastapi import HTTPException

from src.config.settings import AUTH_SERVICE_URL
from src.server.services import platform_client
from src.server.services.account_contract import Team
from src.server.utils.http_client import get_http_client

logger = logging.getLogger(__name__)

#: The ``type`` of a refusal to name a team the caller is not in.
TEAM_NOT_YOURS = "team_not_yours"
#: The ``type`` of a refusal to name a team the auth service does not know.
TEAM_NOT_FOUND = "team_not_found"


@dataclass(frozen=True)
class RosterMember:
    user_id: str
    name: Optional[str]
    email: Optional[str]
    role: str
    #: An active member of the organization now.
    active: bool


@dataclass(frozen=True)
class Roster:
    id: str
    name: str
    org_id: str
    members: tuple[RosterMember, ...]


class TeamNotFound(LookupError):
    """The auth service has no such team in the organization."""


class RosterUnavailable(RuntimeError):
    """The auth service could not be asked, or answered something else."""


def _member(raw: Any) -> RosterMember:
    if not (
        isinstance(raw, dict)
        and isinstance(raw.get("user_id"), str)
        and raw["user_id"]
        and raw.get("role") in ("admin", "member")
        and isinstance(raw.get("active"), bool)
        and isinstance(raw.get("name"), (str, type(None)))
        and isinstance(raw.get("email"), (str, type(None)))
    ):
        raise ValueError(f"roster member: {raw!r}")
    return RosterMember(raw["user_id"], raw.get("name"), raw.get("email"), raw["role"], raw["active"])


def _roster(body: Any) -> Roster:
    team = body.get("team") if isinstance(body, dict) else None
    members = body.get("members") if isinstance(body, dict) else None
    if not (
        isinstance(team, dict)
        and isinstance(team.get("id"), str)
        and isinstance(team.get("name"), str)
        and isinstance(team.get("organization_id"), str)
        and isinstance(members, list)
    ):
        raise ValueError("roster")
    return Roster(
        team["id"], team["name"], team["organization_id"], tuple(_member(m) for m in members)
    )


async def read_roster(team_id: str, org_id: str) -> Roster:
    """The team and its people, from the auth service. Raises
    :class:`TeamNotFound` for a team it does not have in ``org_id``, and
    :class:`RosterUnavailable` for anything but an answer."""
    if not platform_client.platform_gating_active() or platform_client.service_token_missing():
        raise RosterUnavailable("no auth service to ask")
    url = (
        f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/teams/"
        f"{quote(team_id, safe='')}/roster"
    )
    try:
        client = await get_http_client()
        resp = await client.get(
            url, params={"org_id": org_id}, headers=platform_client.service_headers()
        )
    except Exception as e:
        logger.warning("[teams] roster read failed team=%s: %s", team_id, e)
        raise RosterUnavailable(str(e)) from None
    if resp.status_code == 404:
        raise TeamNotFound(team_id)
    if resp.status_code != 200:
        logger.warning("[teams] roster read answered %d team=%s", resp.status_code, team_id)
        raise RosterUnavailable(f"HTTP {resp.status_code}")
    try:
        roster = _roster(resp.json())
    except ValueError as e:
        logger.warning("[teams] unreadable roster team=%s: %s", team_id, e)
        raise RosterUnavailable("unreadable roster") from None
    if roster.id != team_id or roster.org_id != org_id:
        # Not the team asked for: as good as none.
        raise TeamNotFound(team_id)
    return roster


def _refusal(status: int, kind: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"type": kind, "message": message})


async def require_nameable(teams: tuple[Team, ...], team_id: str, org_id: str) -> Roster:
    """The roster of a team the caller may name on a workspace: one they
    are in, since a member sees only their own teams, and one the auth
    service still has, so a team deleted a moment ago is never named."""
    if not any(t.id == team_id for t in teams):
        raise _refusal(403, TEAM_NOT_YOURS, "You can only choose a team you are in.")
    try:
        return await read_roster(team_id, org_id)
    except TeamNotFound:
        raise _refusal(400, TEAM_NOT_FOUND, "Team not found.") from None
    except RosterUnavailable:
        raise platform_client.quota_service_unavailable() from None
