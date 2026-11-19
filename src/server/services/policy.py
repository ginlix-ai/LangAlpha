"""What a user may do: the deployment's policy narrowed by their organization's.

A policy is a set of restrictions in two layers of one schema
(``src.config.models.PolicyLayer``): the deployment's, from ``policy:`` in
config.yaml, and the organization's, as the auth service sends it
(``services.account``). The effective policy is the most restrictive
combination, so an organization can only narrow what the deployment allows.

A later yes-or-no rule is a field of ``PolicyLayer``, an entry in
:data:`RULES` saying how its refusals read, and its enforcement. An
organization's layer naming a key this version lacks makes the account
unknown, so a rule is never lifted by a build that cannot enforce it.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from operator import attrgetter
from typing import Any, Literal, NoReturn

from fastapi import HTTPException
from src.config.core import get_infrastructure_config
from src.config.models import PolicyLayer
from src.server.services.brokerages import brokerage_for_url
from src.server.services.refusals import Refusal

Setter = Literal["deployment", "organization"]

#: The ``type`` of every refusal a policy makes; ``rule`` says which.
POLICY_DENIED = "policy_denied"

MODELS_ALLOWED = "models.allowed"
OWN_API_KEYS = "own_model_access.api_keys"
OWN_SUBSCRIPTIONS = "own_model_access.subscriptions"
PUBLIC_LINKS = "public_links"
TRADING = "trading"
OWN_MACHINES = "own_machines"
USER_CONNECTORS = "user_connectors"
ORG_LIBRARY_PUBLISH = "org_library.publish"
SHARED_WORKSPACES_CREATE = "shared_workspaces.create"


@dataclass(frozen=True)
class Rule:
    """A yes-or-no rule, keyed by its dotted path in ``PolicyLayer``."""

    key: str
    #: What a refusal says is not allowed: "{setter} does not allow {denies}".
    denies: str
    #: The act a refusal names when its caller names none: "{act} is not
    #: allowed".
    act: str | None = None
    #: In ``/users/me``'s body; a rule only a member of an organization granted
    #: the feature can use is reported by its own route, and named in
    #: ``set_by`` only when it restricts.
    reported: bool = True
    #: What the agent reads when a call is withheld: the denial, naming its
    #: setter as ``{by}``, and the text for a policy that cannot be read. Read
    #: as a tool result, so neither advises trying again, which would only
    #: repeat the refusal.
    withheld: tuple[str, str] | None = None


RULES: dict[str, Rule] = {
    rule.key: rule
    for rule in (
        Rule(OWN_API_KEYS, "your own API keys, custom models or custom providers"),
        Rule(OWN_SUBSCRIPTIONS, "your own model sign-ins or a Claude Code subscription"),
        Rule(PUBLIC_LINKS, "links that open without signing in", act="Sharing a public link"),
        Rule(
            TRADING,
            "trading",
            withheld=(
                "No order was sent: {by} does not allow trading, so live orders "
                "and brokerage tools not known to be safe are withheld. Tell the user.",
                "No order was sent: live orders and brokerage tools not known to "
                "be safe are withheld while this account's policy cannot be read. "
                "Tell the user.",
            ),
        ),
        Rule(OWN_MACHINES, "work on a machine you bring"),
        Rule(
            USER_CONNECTORS,
            "your own connectors",
            withheld=(
                "No call was made: {by} does not allow the user's own connectors, "
                "so this one is withheld. Tell the user.",
                "No call was made: the user's own connectors are withheld while "
                "this account's policy cannot be read. Tell the user.",
            ),
        ),
        Rule(ORG_LIBRARY_PUBLISH, "it", act="Publishing to the library", reported=False),
        Rule(
            SHARED_WORKSPACES_CREATE, "it", act="Creating a shared workspace", reported=False
        ),
    )
}


def _by(setter: Setter) -> str:
    return "your organization" if setter == "organization" else "this deployment"


@dataclass(frozen=True)
class Policy:
    """The effective policy, merged; ``layers`` keeps who set what, so a
    refusal can name it."""

    models_allowed: frozenset[str] | None = None
    #: ``models_allowed`` in the order its list was written: the
    #: organization's when it set one, else the deployment's. The first entry
    #: a user can be served is their default where the deployment's is not
    #: allowed.
    allowed_in_order: tuple[str, ...] = ()
    #: The :data:`RULES` some layer refuses.
    denied: frozenset[str] = frozenset()
    layers: tuple[tuple[Setter, PolicyLayer], ...] = ()

    def allows(self, rule: str) -> bool:
        return rule not in self.denied

    @property
    def unrestricted(self) -> bool:
        """Nothing restricts which models run or whose access runs them; the
        rules about other things do not count here."""
        return (
            self.models_allowed is None
            and self.allows(OWN_API_KEYS)
            and self.allows(OWN_SUBSCRIPTIONS)
        )

    def allows_model(self, name: str) -> bool:
        return self.models_allowed is None or name in self.models_allowed

    def byok(self, is_byok: bool) -> bool:
        """Whether the user's own keys count, given whether they hold any."""
        return is_byok and self.allows(OWN_API_KEYS)

    def oauth(self, has_oauth: bool) -> bool:
        """Whether the user's own sign-ins count, given whether they hold any."""
        return has_oauth and self.allows(OWN_SUBSCRIPTIONS)

    def _organization_first(self) -> Iterator[tuple[Setter, PolicyLayer]]:
        return iter(sorted(self.layers, key=lambda layer: layer[0] != "organization"))

    def set_by(self, rule: str, model: str | None = None) -> Setter:
        """Which layer refuses ``rule`` (for ``models.allowed``, ``model``),
        the organization's when both do."""
        for setter, layer in self._organization_first():
            if rule == MODELS_ALLOWED:
                allowed = layer.models.allowed
                if allowed is not None and (model is None or model not in allowed):
                    return setter
            elif rule in RULES and not attrgetter(rule)(layer):
                return setter
        if rule == MODELS_ALLOWED and model is not None:
            # Every list names it, yet it is refused (a custom entry under
            # that name): any layer with a list refuses it.
            return self.set_by(rule)
        return "deployment"

    def reason(self, rule: str) -> str:
        """Why ``rule`` refuses, as every refusal of it ends:
        "{setter} does not allow {what}"."""
        return f"{_by(self.set_by(rule))} does not allow {RULES[rule].denies}"

    def require(self, rule: str, act: str | None = None) -> None:
        """Refuse ``act`` (the rule's own when None) unless ``rule`` allows
        it (403)."""
        if self.allows(rule):
            return
        raise policy_denied(
            rule,
            f"{act or RULES[rule].act} is not allowed: {self.reason(rule)}.",
            self.set_by(rule),
            status_code=403,
        )

    def for_client(self) -> dict[str, Any]:
        """The policy as ``/users/me`` reports it, for a client to grey out
        what a turn or a connect route would refuse."""
        body: dict[str, Any] = {
            # In the order the server picks a default from, so a client can
            # show which model that is.
            "models": {
                "allowed": list(self.allowed_in_order)
                if self.models_allowed is not None
                else None
            },
        }
        for rule in RULES.values():
            if rule.reported:
                *path, leaf = rule.key.split(".")
                node = body
                for part in path:
                    node = node.setdefault(part, {})
                node[leaf] = self.allows(rule.key)
        restricted = ([MODELS_ALLOWED] if self.models_allowed is not None else []) + [
            rule for rule in RULES if not self.allows(rule)
        ]
        body["set_by"] = {rule: self.set_by(rule) for rule in restricted}
        return body


#: No deployment policy and no organization: nothing is restricted.
ALLOW_ALL = Policy()


def merge(deployment: PolicyLayer | None, organization: PolicyLayer | None) -> Policy:
    """The most restrictive combination of the two layers."""
    layers: tuple[tuple[Setter, PolicyLayer], ...] = tuple(
        (setter, layer)
        for setter, layer in (("deployment", deployment), ("organization", organization))
        if layer is not None
    )
    if not layers:
        return ALLOW_ALL
    allowed: frozenset[str] | None = None
    written: list[str] = []
    for _, layer in layers:
        if layer.models.allowed is not None:
            named = frozenset(layer.models.allowed)
            allowed = named if allowed is None else allowed & named
            written = layer.models.allowed  # the organization's, layered last, wins
    return Policy(
        models_allowed=allowed,
        allowed_in_order=tuple(
            dict.fromkeys(m for m in written if allowed is not None and m in allowed)
        ),
        denied=frozenset(
            rule for rule in RULES if any(not attrgetter(rule)(layer) for _, layer in layers)
        ),
        layers=layers,
    )


def withheld_text(policy: Policy | None, rule: str) -> str | None:
    """What the agent reads for a call ``rule`` withholds under ``policy``, or
    None when it may make the call. An unknown policy withholds."""
    denied, unknown = RULES[rule].withheld
    if policy is None:
        return unknown
    if policy.allows(rule):
        return None
    return denied.format(by=_by(policy.set_by(rule)))


def deployment_policy() -> PolicyLayer | None:
    return get_infrastructure_config().policy


class PolicyDenied(Refusal):
    """A refusal by one of the policy's rules."""


def policy_denied(
    rule: str, message: str, setter: Setter, *, status_code: int = 400, **extra: Any
) -> HTTPException:
    """A refusal by policy: one ``type`` for every rule, ``rule`` naming it."""
    return PolicyDenied(
        status_code=status_code,
        detail={
            "message": message,
            "type": POLICY_DENIED,
            "rule": rule,
            "set_by": setter,
            **extra,
        },
    )


def is_policy_refusal(exc: BaseException) -> bool:
    """Whether ``exc`` is a refusal by policy or of a suspended account, or
    ``account.require_policy``'s for an account whose policy is unknown: none is
    to be worked around.

    The last is the plain 503 every unanswered question to the auth service
    raises, so it is recognised by its ``type``, and an outage elsewhere in
    the same call is held to as firmly."""
    if isinstance(exc, Refusal):
        return True
    return (
        isinstance(exc, HTTPException)
        and isinstance(exc.detail, dict)
        and exc.detail.get("type") == "service_unavailable"
    )


_SETTINGS_LINK = {"url": "/settings?tab=model", "label": "Open Settings"}


def raise_model_denied(policy: Policy, rule: str, model: str) -> NoReturn:
    """A turn's model the policy refuses, by ``rule``."""
    setter = policy.set_by(rule, model)
    if rule == MODELS_ALLOWED:
        message = (
            f"Model '{model}' is not allowed: {_by(setter)} limits which models "
            "can be used. Pick another model."
        )
        raise policy_denied(
            rule, message, setter, model=model,
            allowed=list(policy.allowed_in_order), link=_SETTINGS_LINK,
        )
    raise policy_denied(
        rule,
        f"Model '{model}' cannot be used: {policy.reason(rule)}. Pick another model.",
        setter, model=model, link=_SETTINGS_LINK,
    )


def raise_no_model_allowed(policy: Policy, model: str) -> NoReturn:
    """Nothing the turn could fall back to is allowed: ``model`` is the last
    candidate, the deployment's default."""
    setter = policy.set_by(MODELS_ALLOWED, model)
    raise policy_denied(
        MODELS_ALLOWED,
        f"No model this turn could run is allowed: {_by(setter)} limits which "
        "models can be used. Pick an allowed model.",
        setter,
        model=model,
        allowed=list(policy.allowed_in_order),
        link=_SETTINGS_LINK,
    )


def governed_by_user_connectors(url: str | None) -> bool:
    """Whether the ``user_connectors`` rule governs a server at ``url``.

    A known brokerage is the ``trading`` rule's, told apart by its address
    as everywhere else, so an organization that withholds the user's own
    connectors does not also take their brokerage. The one predicate both
    resolution and every gate ask, so the two cannot disagree on a server.
    """
    return brokerage_for_url(url) is None
