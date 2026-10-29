"""What a description is cached under: whose it is, and which tools it covers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from src.server.services.brokerage_capabilities import (
    denied_tools,
    effective_capabilities,
    group_keys_for,
    vendor_for_url,
)
from src.server.services.egress import fold_tool_name


@dataclass(frozen=True)
class Scope:
    """Whose description it is.

    A shipped brokerage's is shared by every user whose grant puts the same
    groups in force, since their agents are offered the same tools. Any other
    server's is its owner's alone, so one user's server cannot write what
    another user's agent reads, and is keyed by its address too, so a row
    pointed at a different server never reads the old one's description.
    """

    brokerage: str | None
    groups: tuple[str, ...] = ()
    owner: str = ""

    @classmethod
    def at_brokerage(cls, brokerage: str, granted: Iterable[str]) -> Scope:
        in_force = set(effective_capabilities(brokerage, granted)) & set(group_keys_for(brokerage))
        return cls(brokerage, tuple(sorted(in_force)))

    @classmethod
    def of(
        cls,
        user_id: str,
        server_name: str,
        url: str | None,
        connection: Mapping[str, Any] | None,
        dials: str = "",
    ) -> Scope:
        """The scope a catalog row reads, by the identity the denial reads: the
        consented URL and its grant, or the row's own URL and consent to
        nothing when there is no connection. A connection consented at another
        URL than the row's binds nothing, as ``resolve_mcp_config`` reads it.

        A user's own server is keyed by a hash of its discovery fingerprint
        (``dial_identity``), so any edit that can change what it lists reads
        none of the old description, and a credential in its URL never
        reaches the key, which is stored and logged."""
        from src.server.services.mcp_config import same_consented_url

        consented = (connection or {}).get("server_url")
        if consented and not same_consented_url(consented, url):
            connection = None
        if connection is not None:
            url = consented
        vendor = vendor_for_url(url)
        if vendor is None:
            address = _sha(dials or url or "")[:16]
            return cls(None, owner=f"{user_id}:{server_name}:{address}")
        return cls.at_brokerage(vendor, (connection or {}).get("granted_capabilities") or ())

    @classmethod
    def parse(cls, key: str) -> Scope:
        if key.startswith("brokerage:"):
            brokerage, _, groups = key.removeprefix("brokerage:").partition(":")
            return cls(brokerage, tuple(g for g in groups.split(",") if g))
        return cls(None, owner=key.removeprefix("user:"))

    @property
    def key(self) -> str:
        if self.brokerage is None:
            return f"user:{self.owner}"
        return f"brokerage:{self.brokerage}:{','.join(self.groups)}"


def dial_identity(server_name: str, row: Mapping[str, Any]) -> str:
    """A catalog row's discovery fingerprint: everything that can change what
    its server lists, and never a secret value."""
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    return mcp_discovery_fingerprint(user_row_to_server_config({**row, "name": server_name}))


def platform_model() -> str | None:
    """The model shared brokerage descriptions are written with."""
    from src.server.app import setup
    from src.server.services.llm.config import slot_model

    if setup.agent_config is None or setup.agent_config.llm is None:
        return None
    return slot_model(setup.agent_config.llm, "background")


def described_tools(scope: Scope, tools: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """The tools a server's description covers.

    At a shipped brokerage, the tools the scope's grant leaves the agent: what
    ``denied_tools`` refuses is never offered, so the description never says
    the server does what the agent cannot call. A release that changes the
    curation changes the set, and with it the digest.
    """
    refused = {fold_tool_name(t) for t in denied_tools(scope.brokerage, scope.groups) or ()}
    return [t for t in tools if fold_tool_name(t.get("name") or "") not in refused]


def _sha(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def server_digest(tools: Iterable[dict[str, Any]]) -> str:
    """Hash of what the description is written from, order-independent: each
    tool's name, description and input schema."""
    return _sha(
        sorted(
            _sha(
                {
                    "name": t.get("name") or "",
                    "description": t.get("description") or "",
                    "inputSchema": t.get("input_schema") or {},
                }
            )
            for t in tools
        )
    )
