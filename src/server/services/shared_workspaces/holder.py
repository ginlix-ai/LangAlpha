"""The holder: the account that owns a shared workspace and its computer.

A ``users`` row that is not a person, made for one shared workspace. It owns
the rows that key the machine and the workspace (``computers.user_id``,
``workspaces.user_id``), so whatever the shared computer is given is read for
an account that holds nothing: no vault, no connectors, no sign-ins, no
personal memory. It never signs in, never acts and is never named to the auth
service, which has no account by its id. Code recognises it by its id alone,
with no read.
"""

from __future__ import annotations

from typing import Any, Optional

from src.server.database.shared_workspaces import HolderNotAPerson, refuse_holder

__all__ = ["HolderNotAPerson", "SHARED_PROFILE_FIELDS", "refuse_holder", "shared_profile"]


#: What a shared workspace's turn may say of its sender: who is speaking and
#: their clock, and none of what they told the agent of themselves (their
#: preferences and investment profile), which other members would read.
SHARED_PROFILE_FIELDS = ("name", "timezone", "locale")


def shared_profile(profile: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """The sender's profile as a shared workspace's build may carry it."""
    return {k: profile.get(k) for k in SHARED_PROFILE_FIELDS} if profile else profile


def withheld_from_holder(server: Any) -> bool:
    """Whether a shared workspace's computer goes without this built-in.

    One the relay reaches with a vendor account the host keeps per user
    (``relay_bound``) needs a person to own that account and accept its terms,
    and the holder is none, so every call there would be refused while its
    tools were offered. Its grant, tools and wording go with it.
    """
    return bool(server.relay_bound)


def holder_withheld_skills() -> frozenset[str]:
    """The skills shipped in the same bundle as a built-in the holder goes
    without (:func:`withheld_from_holder`): they teach that server's use, so
    a computer without it carries none of them."""
    from src.server.app import setup
    from src.server.services.plugins.bundled import enforcement_owners

    config = setup.agent_config
    if config is None:
        return frozenset()
    owners = enforcement_owners()
    bundles = {
        owners.servers[s.name]
        for s in config.mcp.servers
        if withheld_from_holder(s) and s.name in owners.servers
    }
    return frozenset(name for name, bundle in owners.skills.items() if bundle in bundles)
