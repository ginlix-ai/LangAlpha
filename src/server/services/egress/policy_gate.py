"""What the user's policy withholds from one call through a connection,
judged on every call by the relay and the direct tool path alike, so a
sandbox or a turn that outlived a change to the policy or the account
reaches nothing it now withholds."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from src.server.services import account, membership
from src.server.services.account import Standing
from src.server.services.activity import record_withheld_call
from src.server.services.brokerage_capabilities import trading_rule_withholds
from src.server.services.policy import (
    TRADING,
    USER_CONNECTORS,
    governed_by_user_connectors,
    withheld_text,
)

logger = logging.getLogger(__name__)


@dataclass
class PolicyGate:
    """One call's judge. The account is read once, when a check first needs
    it, and each withheld call is recorded for the organization's activity
    log; the caller words the refusal for its own path."""

    user_id: str
    via: Literal["relay", "direct"]
    #: Who dialled the call: sandbox code or the host.
    caller: str | None
    workspace_id: Any = None
    _standing: Standing | None = field(default=None, init=False)

    async def _read(self) -> Standing:
        if self._standing is None:
            # The rest of the request reuses this read, as the credential
            # resolution of an organization's connector does.
            membership.share_membership_read(self.user_id)
            self._standing = await account.read_standing(self.user_id)
        return self._standing

    async def suspended(self) -> bool:
        """Whether the account's access through its organization has ended.
        Nothing is read with the ``teams`` flag off."""
        if not membership.teams_door_open() or not (await self._read()).suspended:
            return False
        logger.warning("[egress] %s call refused for a suspended account: user=%s",
                       self.via, self.user_id)
        return True

    async def own_server(
        self, url: str | None, *, server: str, tool: str | None, **detail: Any
    ) -> str | None:
        """Why the ``user_connectors`` rule withholds one of the user's own
        servers at ``url``, judged on the address the call dials; a brokerage
        is the ``trading`` rule's."""
        if not governed_by_user_connectors(url):
            return None
        return await self._withheld(USER_CONNECTORS, server, tool, detail)

    async def live_order(
        self, vendor: str | None, tool: str, *, server: str, **detail: Any
    ) -> str | None:
        """Why the ``trading`` rule withholds a brokerage tool. The account is
        read only for a tool the rule could withhold."""
        if not trading_rule_withholds(vendor, tool):
            return None
        return await self._withheld(TRADING, server, tool, {"vendor": vendor, **detail})

    async def _withheld(
        self, rule: str, server: str, tool: str | None, detail: dict[str, Any]
    ) -> str | None:
        policy = (await self._read()).policy
        text = withheld_text(policy, rule)
        if text is not None:
            logger.warning(
                "[egress] %s call withheld by the %s rule: server=%r tool=%r user=%s",
                self.via, rule, server, tool, self.user_id,
            )
            record_withheld_call(
                self.user_id, policy, rule, via=self.via, caller=self.caller,
                server=server, tool=tool, workspace_id=self.workspace_id,
                detail=detail,
            )
        return text

