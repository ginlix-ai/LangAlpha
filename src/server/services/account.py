"""Which organization an account belongs to, as the auth service says, and
what that lets the account do.

The auth service decides who is in an organization, in which groups at which
level, and what the organization allows. langalpha only reads that answer,
from the membership the server already fetches and caches
(``services.membership.read_membership``), and asks the policy it implies
(``services.policy``) wherever work is gated.
"""

import asyncio
import logging
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType

from fastapi import HTTPException

from ptc_agent.core.sandbox.providers import OWN_MACHINE_KINDS
from src.config.features import is_feature_enabled_system
from src.config.models import OrgLibraryPolicy, PolicyLayer, SharedWorkspacesPolicy
from src.server.database import mcp_config_version
from src.server.database import api_keys, oauth_tokens
from src.server.database import shared_workspaces as shared_db
from src.server.database.shared_workspaces import is_holder
from src.server.services import account_suspension, membership, platform_client
from src.server.services.account_contract import (
    NO_ORG,
    Account,
    MalformedAccount,
    Org,
    parse_account,
)
from src.server.services.policy import (
    OWN_MACHINES,
    PUBLIC_LINKS,
    USER_CONNECTORS,
    Policy,
    deployment_policy,
    governed_by_user_connectors,
    merge,
    policy_denied,
)
from src.server.services.refusals import Refusal

logger = logging.getLogger(__name__)


class AccountUnknown(HTTPException):
    """The auth service could not be asked about an account, and nothing it
    said since the last change notice is on record. A route answers it as
    the outage it is (``quota_service_unavailable``), which a client retries."""

    def __init__(self) -> None:
        outage = platform_client.quota_service_unavailable()
        super().__init__(outage.status_code, outage.detail, outage.headers)


#: The ``type`` of the refusal a suspended account gets.
ACCOUNT_SUSPENDED = "account_suspended"


class AccountSuspended(Refusal):
    """The refusal for an account whose access through its organization has
    ended."""


@dataclass(frozen=True)
class Standing:
    """An account as :func:`read_standing` reads it: one read answers who it
    is, whether it is suspended and what its policy allows."""

    #: As :func:`account_for` gives it; None when unknown.
    account: Account | None
    #: The account as the auth service sent it, before an organization that
    #: does not allow ``teams`` is dropped; None when unknown.
    sent: Account | None
    #: The last word on record from the auth service, carried by a change
    #: notice or its last answer, is that the account is suspended. It counts
    #: only while the account is unknown, as it is right after that notice
    #: when the auth service cannot then be asked: a known answer is newer.
    suspended_hint: bool = False

    @cached_property
    def policy(self) -> Policy | None:
        """The deployment's policy narrowed by the account's; None when the
        account is unknown, which is never "no restriction": an outage, or a
        policy this version cannot read, must not lift what is withheld."""
        if self.account is None:
            return None
        return merge(deployment_policy(), self.account.policy)

    @property
    def suspended(self) -> bool:
        if self.account is None:
            return self.suspended_hint
        return self.account.suspended

    @property
    def member_org(self) -> Org | None:
        """The organization the auth service names the account a member of,
        suspended members included, whatever that organization's feature
        settings say: an account in an organization is wholly its, so this is
        whose its work is. Raises :class:`AccountUnknown` rather than guess."""
        if self.sent is None:
            raise AccountUnknown()
        return self.sent.org

    def refusal(self) -> AccountSuspended | None:
        """The refusal a suspended account gets. An unknown account is not one
        unless the hint says so: an outage must not lock every member out of
        their own work."""
        if not self.suspended:
            return None
        if self.account is None:
            # Only the hint is known, not the organization's name.
            message, name = (
                "This account's access through its organization has ended. Ask "
                "an admin of your organization if this is unexpected.",
                None,
            )
        else:
            name = self.account.org.name
            message = (
                f"This account's access to {name} has ended. Ask an admin of "
                "your organization if this is unexpected."
            )
        return AccountSuspended(
            status_code=403,
            detail={"message": message, "type": ACCOUNT_SUSPENDED, "organization": name},
        )

    def refuse_if_suspended(self) -> None:
        if (refusal := self.refusal()) is not None:
            raise refusal

    def require_policy(self) -> Policy:
        """The policy for work to run under: a suspended account is refused
        (403) whatever its policy says, and an unknown one as an unknown
        membership is refused, a 503 the client retries."""
        self.refuse_if_suspended()
        if self.policy is None:
            raise platform_client.quota_service_unavailable()
        return self.policy


async def read_standing(
    user_id: str, *, unknown_on_cache_error: bool = False
) -> Standing:
    """The user's standing: the one place that decides whether langalpha acts
    on an account's organization at all.

    The door is shut, giving :data:`NO_ORG` without a cache read or a request,
    unless the deployment enables ``teams`` and asks the auth service about
    its users; and shut for an account whose organization does not allow
    ``teams``, unless the account is suspended, which holds whatever the
    organization's features say. An unknown account means the auth service
    could not be asked and nothing it said since the last change notice is on
    record, or what it said could not be read. ``unknown_on_cache_error`` is
    for a check of the suspension alone (``read_membership``).

    Another user's account is read the way the caller's own is, off the
    cached membership (one MGET on a hit, asked of the auth service on a miss
    and coalesced per process); the entry follows every change notice.
    """
    if is_holder(user_id):
        holder = await _holder_account(user_id)
        return Standing(holder, holder)
    if not membership.teams_door_open():
        return Standing(NO_ORG, NO_ORG)
    answer = await membership.read_membership(
        user_id, unknown_on_cache_error=unknown_on_cache_error
    )
    hinted = bool(answer and answer.get(membership.MEMBERSHIP_SUSPENDED_HINT))
    # A record standing in from before a change notice says what the
    # organization was, not what it is.
    if answer is None or answer.get(membership.MEMBERSHIP_ACCOUNT_UNKNOWN):
        return Standing(None, None, hinted)
    try:
        account = parse_account(answer)
    except MalformedAccount as e:
        logger.warning("[Account] unreadable account for user_id=%s: %s", user_id, e)
        return Standing(None, None, hinted)
    if account.org is None:
        return Standing(NO_ORG, account)
    if not account.suspended and account.features.get("teams") is not True:
        return Standing(NO_ORG, account)
    return Standing(account, account)


async def account_for(user_id: str) -> Account | None:
    """The user's account (:func:`read_standing`), or None when it is unknown."""
    return (await read_standing(user_id)).account


async def member_of(user_id: str) -> Org | None:
    """:attr:`Standing.member_org` for ``user_id``; None for an account in no
    organization, and with the door shut, which reads nothing."""
    return (await read_standing(user_id)).member_org


async def active_in(user_id: str, org_id: str) -> bool | None:
    """Whether ``user_id`` is an active member of ``org_id``; None when their
    account cannot be read, which each caller answers its own way."""
    try:
        org = await member_of(user_id)
    except AccountUnknown:
        return None
    return org is not None and org.id == org_id and not org.suspended


#: The organization layer of a holder's policy: closed on everything a
#: machine could be used for outside a person's turn. No organization can
#: loosen it; each member's own rules govern their turn.
HOLDER_POLICY = PolicyLayer(
    public_links=False,
    trading=False,
    own_machines=False,
    user_connectors=False,
    org_library=OrgLibraryPolicy(publish=False),
    shared_workspaces=SharedWorkspacesPolicy(create=False),
)


async def _holder_account(holder_id: str) -> Account:
    """A shared workspace holder's account, answered here and never asked of
    the auth service, which has no account by that id: in the workspace's
    organization, never suspended, under a closed policy no organization can
    loosen. A holder whose workspace row is gone keeps the closed policy."""
    shared = await shared_db.shared_of_holder(holder_id)
    return Account(
        org=Org(id=shared.org_id, name="") if shared is not None else None,
        features=MappingProxyType({"teams": True}),
        policy=HOLDER_POLICY,
    )


async def admit(user_id: str) -> None:
    """The gate every request that names a user passes, refusing a suspended
    account before the route reads anything it holds.

    The read is shared with the rest of the request (``share_membership_read``)
    so a route that reads the membership anyway pays nothing more for the
    gate. Nothing is read with the door shut.
    """
    membership.share_membership_read(user_id)
    (await read_standing(user_id, unknown_on_cache_error=True)).refuse_if_suspended()


async def on_membership_changed(user_id: str, suspended: bool | None) -> None:
    """Act on the auth service's notice that the user's plan, organization or
    teams changed: their next read asks again, an account the change
    suspended, or left unable to use a shared workspace (out of its
    organization, or of the team that let them in), has what it runs there
    stopped in the background, and their open feeds read their reach again.
    ``suspended`` is the account's suspension after the change, None when the
    notice said nothing about it."""
    epoch = await membership.forget_membership(user_id, suspended=suspended)
    if is_feature_enabled_system("teams"):
        # What the organization allows decides which servers a computer's
        # tools are built from, and a warm computer rebuilds them only when
        # the version moves.
        await mcp_config_version.bump_user_workspaces_mcp_version(user_id)
    if membership.teams_door_open():
        account_suspension.stop_if_suspended_soon(user_id, epoch)
        if is_feature_enabled_system("shared_workspaces"):
            # Late: shared workspaces are built on accounts and import this module.
            from src.server.services.shared_workspaces.feed import publish_reach_changed
            from src.server.services.shared_workspaces.turns import (
                stop_shared_work_if_gone_soon,
            )

            stop_shared_work_if_gone_soon(user_id)
            # A team change moves no workspace's row, so no workspace event
            # tells their open feeds; this one does.
            await publish_reach_changed(user_id)


async def policy_for(user_id: str) -> Policy | None:
    """The user's effective policy (:attr:`Standing.policy`), or None when
    their account is unknown. With the ``teams`` flag off, or no auth service
    to ask, the deployment's layer alone applies and nothing is read."""
    return (await read_standing(user_id)).policy


async def require_policy(user_id: str) -> Policy:
    """The policy for work to run under (:meth:`Standing.require_policy`):
    every place work is asked about asks here."""
    return (await read_standing(user_id)).require_policy()


@dataclass(frozen=True)
class OwnAccess:
    """The user's own access that counts under ``policy``: stored and allowed."""

    policy: Policy
    api_keys: bool
    #: None when the caller did not ask (``own_access(sign_ins=False)``).
    subscriptions: bool | None

    @property
    def any(self) -> bool:
        return self.api_keys or bool(self.subscriptions)


async def own_access(
    user_id: str, *, sign_ins: bool = True, policy: Policy | None = None
) -> OwnAccess:
    """Which of the user's own keys and sign-ins a turn may use.

    The one place their stored state is read to decide a turn's path, so no
    call site can count access the policy denies by reading it raw. The
    policy is looked up when not given, an unknown one refused as
    :func:`require_policy` refuses it; ``sign_ins=False`` skips that read
    for a caller that only needs the keys.
    """
    if policy is None:
        policy = await require_policy(user_id)
    if sign_ins:
        is_byok, has_oauth = await asyncio.gather(
            api_keys.is_byok_active(user_id), oauth_tokens.has_any_oauth_token(user_id)
        )
        return OwnAccess(policy, policy.byok(is_byok), policy.oauth(has_oauth))
    return OwnAccess(policy, policy.byok(await api_keys.is_byok_active(user_id)), None)


async def serves_public_links(owner_id: str | None) -> bool:
    """Whether the owner's links open for anyone but the owner.

    Asked on every visitor's request rather than once at sharing, so a link
    shared before the policy stops serving while it stands and serves again,
    unchanged, once it is lifted. An unknown owner or policy serves nothing.
    With no policy configured and the ``teams`` flag off nothing is read.
    """
    if not owner_id:
        return False
    # Not refused when suspended: a suspended owner's links are the
    # organization's published content now, and keep serving under its
    # ``public_links``.
    policy = await policy_for(owner_id)
    return policy is not None and policy.allows(PUBLIC_LINKS)


async def refuse_machine_use(kind: str | None, owner_id: str | None) -> None:
    """Refuse running the owner's work on a computer of ``kind``: any kind
    for a suspended owner, and one they brought under the ``own_machines``
    rule.

    Asked wherever a computer is taken up for work, and when one is added,
    so a machine added before the policy, or a computer started before a
    suspension, stops being used while it stands; stopping, renaming and
    removing one never ask. Nothing is read with the ``teams`` flag off for
    a kind the deployment provides, or with no policy configured either. An
    unknown owner or policy refuses a machine they brought as
    :func:`require_policy` refuses it, and lets one the deployment provides
    be used, as it was before organizations.
    """
    if kind not in OWN_MACHINE_KINDS:
        if owner_id:
            (await read_standing(owner_id)).refuse_if_suspended()
        return
    if not owner_id:
        raise platform_client.quota_service_unavailable()
    policy = await require_policy(owner_id)
    if policy.allows(OWN_MACHINES):
        return
    raise policy_denied(
        OWN_MACHINES,
        f"Your own machines cannot be used: {policy.reason(OWN_MACHINES)}. Use "
        "another computer instead.",
        policy.set_by(OWN_MACHINES),
        status_code=403,
    )


async def refuse_user_connector(
    user_id: str, what: str, *, url: str | None, policy: Policy | None = None
) -> None:
    """Refuse putting one of the user's own MCP servers in use (403).

    ``what`` names the act, as in "{what} is not allowed"; ``url`` is the
    server's address, None for one launched by command, and exempts a
    brokerage. Asked where a server is added or turned on, never where one
    is turned off, edited or deleted, so leaving one is always possible.
    Nothing is read for a brokerage, or with no policy configured and the
    ``teams`` flag off.
    """
    if governed_by_user_connectors(url):
        (policy or await require_policy(user_id)).require(USER_CONNECTORS, what)
