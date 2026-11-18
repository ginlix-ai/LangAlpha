"""The account as the auth service sends it, and how this build reads it.

The organization, its groups, its feature settings, its policy layer, its
connectors and the account's teams in it ride the cached membership (``services.membership``) as blocks of
the validate answer; :func:`parse_account` reads them strictly, so a block
this build cannot read makes the account unknown rather than half read.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, get_args
from urllib.parse import urlsplit

from pydantic import ValidationError

from src.config.models import PolicyLayer
from src.server.models.mcp_names import (
    DESCRIPTION_MAX,
    INSTRUCTION_MAX,
    NAME_RE,
    sandbox_name_error,
)

GroupLevel = Literal["read", "write", "manage"]
_LEVELS = frozenset(get_args(GroupLevel))

_EMPTY: Mapping[str, Any] = MappingProxyType({})

#: The version of the account this build reads, declared to the auth service
#: on the membership fetch: the blocks in ``MEMBERSHIP_ACCOUNT_KEYS``, the
#: rules of :class:`PolicyLayer` and the connector fields parsed below. A key
#: this build cannot read makes the account unknown and refuses every turn, so
#: the auth service sends only what the declared version reads, and any change
#: to what is read here moves the version, as does a feature an organization
#: can be granted (one that ``requires`` ``teams``).
#:
#: 2: the ``org_library.publish`` rule and the ``org_library`` feature.
#: 3: the ``shared_workspaces.create`` rule and the ``shared_workspaces``
#: feature.
#: 4: the ``teams`` block, the account's teams in its organization.
ACCOUNT_CONTRACT = 4


@dataclass(frozen=True)
class Org:
    id: str
    name: str
    #: The account's access through this organization has ended: the person
    #: was removed or suspended, or the organization was. Its content stays
    #: for the organization, and the person can neither work in it nor read it.
    suspended: bool = False


@dataclass(frozen=True)
class Group:
    id: str
    level: GroupLevel


TeamRole = Literal["admin", "member"]
_TEAM_ROLES = frozenset(get_args(TeamRole))


@dataclass(frozen=True)
class Team:
    """A team of the organization the account is in, with its role there."""

    id: str
    name: str
    role: TeamRole


ConnectorAuth = Literal["shared", "member"]
_CONNECTOR_AUTHS = frozenset(get_args(ConnectorAuth))


@dataclass(frozen=True)
class OrgConnector:
    """An MCP server the organization sets up for its members."""

    #: Opaque and stable across the admin's edits; keys everything stored.
    id: str
    #: The module its tools are generated into in the sandbox.
    name: str
    url: str
    #: ``shared``: one credential the auth service holds for everyone;
    #: ``member``: each member signs in on their own.
    auth: ConnectorAuth
    #: Changes whenever the admin edits the url, auth or credential.
    revision: str
    description: str = ""
    instruction: str = ""


@dataclass(frozen=True)
class Account:
    org: Org | None = None
    groups: tuple[Group, ...] = ()
    #: The organization's feature settings, the org tier of feature resolution.
    features: Mapping[str, bool] = field(default=_EMPTY)
    #: The organization's layer of the policy, None when it sent none.
    policy: PolicyLayer | None = None
    #: The connectors the organization sets up, as it defines them.
    connectors: tuple[OrgConnector, ...] = ()
    #: The account's teams in its organization. Empty from an auth service
    #: that sends no teams, which then reach nothing.
    teams: tuple[Team, ...] = ()

    @property
    def team_ids(self) -> frozenset[str]:
        return frozenset(t.id for t in self.teams)

    @property
    def suspended(self) -> bool:
        return self.org is not None and self.org.suspended


#: An account langalpha does not treat as in any organization.
NO_ORG = Account()


class MalformedAccount(ValueError):
    """The auth service sent an account block of the wrong shape."""


def _parse_org(raw: object) -> Org | None:
    if raw is None:
        return None
    if not (
        isinstance(raw, dict)
        and isinstance(raw.get("id"), str)
        and raw["id"]
        and isinstance(raw.get("name"), str)
        and isinstance(raw.get("suspended", False), bool)
    ):
        raise MalformedAccount(f"org: {raw!r}")
    return Org(id=raw["id"], name=raw["name"], suspended=raw.get("suspended", False))


def _parse_groups(raw: object) -> tuple[Group, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise MalformedAccount(f"groups: {raw!r}")
    groups = []
    for item in raw:
        if not (
            isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and item["id"]
            and item.get("level") in _LEVELS
        ):
            raise MalformedAccount(f"group: {item!r}")
        groups.append(Group(id=item["id"], level=item["level"]))
    return tuple(groups)


def _parse_features(raw: object) -> Mapping[str, bool]:
    if raw is None:
        return _EMPTY
    if not isinstance(raw, dict) or not all(
        isinstance(k, str) and isinstance(v, bool) for k, v in raw.items()
    ):
        raise MalformedAccount(f"features: {raw!r}")
    # Keys the catalog does not know are kept: effective_flags reads only its own.
    return MappingProxyType(dict(raw))


def _parse_policy(raw: object) -> PolicyLayer | None:
    """The organization's policy layer, validated here with the rest of the
    account so a layer this version cannot read makes the whole account
    unknown, its feature tier included, rather than the policy alone."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise MalformedAccount(f"policy: {raw!r}")
    try:
        return PolicyLayer.model_validate(raw)
    except ValidationError as e:
        raise MalformedAccount(
            f"policy: {e.errors(include_url=False, include_input=False)}"
        ) from None


_CONNECTOR_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
_CONNECTOR_KEYS = frozenset(
    {"id", "name", "url", "auth", "revision", "description", "instruction"}
)
MAX_CONNECTORS = 50


def _parse_connector(raw: object) -> OrgConnector:
    if not isinstance(raw, dict) or not raw.keys() <= _CONNECTOR_KEYS:
        raise MalformedAccount(f"connector: {raw!r}")
    cid, name, url, auth, revision = (
        raw.get(k) for k in ("id", "name", "url", "auth", "revision")
    )
    description = raw.get("description", "")
    instruction = raw.get("instruction", "")
    ok = (
        isinstance(cid, str)
        and _CONNECTOR_ID_RE.fullmatch(cid)
        and isinstance(name, str)
        and NAME_RE.match(name)
        and sandbox_name_error(name) is None
        and isinstance(url, str)
        and _is_https(url)
        and auth in _CONNECTOR_AUTHS
        and isinstance(revision, str)
        and revision
        and isinstance(description, str)
        and len(description) <= DESCRIPTION_MAX
        and isinstance(instruction, str)
        and len(instruction) <= INSTRUCTION_MAX
    )
    if not ok:
        raise MalformedAccount(f"connector: {raw!r}")
    return OrgConnector(
        id=cid, name=name, url=url, auth=auth, revision=revision,
        description=description, instruction=instruction,
    )


def _is_https(url: str) -> bool:
    try:
        parts = urlsplit(url)
        return parts.scheme == "https" and bool(parts.hostname)
    except ValueError:
        return False


def _parse_connectors(raw: object) -> tuple[OrgConnector, ...]:
    """Strict, so a definition this version cannot run makes the account
    unknown rather than a connector that half works; two that share an id or
    a name could never be told apart where they are stored or imported."""
    if raw is None:
        return ()
    if not isinstance(raw, list) or len(raw) > MAX_CONNECTORS:
        raise MalformedAccount(f"connectors: {raw!r}")
    connectors = tuple(_parse_connector(item) for item in raw)
    for attr in ("id", "name"):
        values = [getattr(c, attr) for c in connectors]
        if len(set(values)) != len(values):
            raise MalformedAccount(f"connectors: duplicate {attr}")
    return connectors


_TEAM_KEYS = frozenset({"id", "name", "role"})
MAX_TEAMS = 500


def _parse_teams(raw: object) -> tuple[Team, ...]:
    """Strict, as connectors are: a team id decides who reaches a shared
    workspace, so a list this version cannot read makes the account unknown
    rather than one that reaches less or more than it should."""
    if raw is None:
        return ()
    if not isinstance(raw, list) or len(raw) > MAX_TEAMS:
        raise MalformedAccount(f"teams: {raw!r}")
    teams = []
    for item in raw:
        if not (
            isinstance(item, dict)
            and item.keys() <= _TEAM_KEYS
            and isinstance(item.get("id"), str)
            and item["id"]
            and isinstance(item.get("name"), str)
            and item.get("role") in _TEAM_ROLES
        ):
            raise MalformedAccount(f"team: {item!r}")
        teams.append(Team(id=item["id"], name=item["name"], role=item["role"]))
    if len({t.id for t in teams}) != len(teams):
        raise MalformedAccount("teams: duplicate id")
    return tuple(teams)


def parse_account(answer: Mapping[str, Any]) -> Account:
    """The account a cached membership describes, before the door is applied.

    An entry an older worker wrote carries none of the blocks and so reads as
    no organization. Raises :class:`MalformedAccount` on a block of the wrong
    shape.
    """
    return Account(
        org=_parse_org(answer.get("org")),
        groups=_parse_groups(answer.get("groups")),
        features=_parse_features(answer.get("features")),
        policy=_parse_policy(answer.get("policy")),
        connectors=_parse_connectors(answer.get("connectors")),
        teams=_parse_teams(answer.get("teams")),
    )
