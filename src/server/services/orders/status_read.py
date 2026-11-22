"""Reading a brokerage's own order list from the host, through the user's grant.

Two readers need it: the reconciler, which settles the ledger's open rows, and
a watch, which proves an order placed elsewhere is listed before the ledger
records it. Both walk the same chain to reach the vendor (the connection, its
state, the vendor's adapter, the grant), both page through a list the same
way, and both must never send a tool that trades. That is all here, so the two
cannot disagree about what a status read is.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from src.config.env import EGRESS_RELAY_SECRET
from src.server.database.mcp_oauth import SERVABLE, get_connection, list_connections
from src.server.database.order_reconciliation import active_grant_for_connection
from src.server.services.brokerage_capabilities import order_tool, vendor_for_url
from src.server.services.brokerage_orders import (
    ListingIncomplete,
    OrderAdapter,
    StatusQuery,
    VendorOrder,
    adapter_for,
)
from src.server.services.egress.direct_tools import relay_mcp_client
from src.server.services.egress.relay_jwt import CALLER_HOST, mint_relay_jwt

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_LISTING_PAGES",
    "READER_SWEEP",
    "READER_WATCH",
    "Listing",
    "NotAStatusRead",
    "StatusReader",
    "Unreachable",
    "status_reader",
]

# Who reads, as the sandbox_id claim on the relay JWT. Audit-only at the relay,
# which authorizes on the (user, workspace) pair, but the claim must be
# non-empty, and in a relay log line it is what says which host job called.
READER_SWEEP = "reconcile"
READER_WATCH = "watch"

# Pages one status read may turn before absence stops counting as evidence.
# At a hundred rows a page this is two thousand orders of history, which is
# more than the match window can hold; a list still going past it is read as
# incomplete, and an attempt missing from it is left open rather than failed.
MAX_LISTING_PAGES = 20


@dataclass(frozen=True)
class Listing:
    """What one status read returned, and whether the vendor had more.

    ``complete`` is what turns absence into evidence: an attempt missing from
    a list the vendor cut short may be on the page not read.
    """

    orders: list[VendorOrder]
    complete: bool


class NotAStatusRead(Exception):
    """A read that names an order tool. Refused here rather than at the relay,
    because a host job must not be the thing that discovers an adapter naming
    a mutating tool as its status read."""


@dataclass(frozen=True)
class Unreachable:
    """Why the vendor cannot be read for this connection, in chain order."""

    reason: Literal["no_connection", "not_connected", "unsupported", "no_grant"]
    #: The connection's own name, or the name asked for when none matched.
    server: str
    vendor: str | None = None


def _result_body(result: Any) -> Any:
    """The vendor's payload out of an MCP call result, however the SDK wrapped it.

    Text first: every brokerage answers with its own JSON as text, and the
    structured field is what an SDK synthesizes when a server declares an
    output schema. Reading the synthesized one first would hand the adapter a
    re-wrapped shape instead of the envelope it parses.
    """
    blocks = getattr(result, "content", None) or []
    text = "".join(str(getattr(block, "text", "") or "") for block in blocks).strip()
    if text:
        return text
    structured = getattr(result, "structured_content", None)
    return structured if structured is not None else ""


def is_status_read(vendor: str, query: StatusQuery) -> bool:
    """Whether ``query`` only reads: the capability map names no order tool for it."""
    return order_tool(vendor, query.tool) is None


@dataclass(frozen=True)
class StatusReader:
    """One user's reads at one connected brokerage, on that user's own grant."""

    user_id: str
    server: str
    vendor: str
    adapter: OrderAdapter
    grant: dict[str, str]
    reader: str

    def client(self) -> Any:
        """An MCP client for this reader's calls, made by the host rather than
        a sandbox."""
        minted = mint_relay_jwt(
            EGRESS_RELAY_SECRET,
            user_id=self.user_id,
            workspace_id=self.grant["workspace_id"],
            sandbox_id=self.reader,
            caller=CALLER_HOST,
        )
        return relay_mcp_client(self.grant["grant_id"], token=minted.token)

    async def read(self, client: Any, query: StatusQuery) -> Listing:
        """Every page the vendor offers, up to the bound, and whether that was all."""
        if not is_status_read(self.vendor, query):
            raise NotAStatusRead(query.tool)
        orders: list[VendorOrder] = []
        page: StatusQuery | None = query
        for _ in range(MAX_LISTING_PAGES):
            result = await client.call_tool(page.tool, dict(page.args))
            body = _result_body(result)
            orders.extend(self.adapter.parse_status(page, body))
            try:
                page = self.adapter.next_page(page, body)
            except ListingIncomplete as e:
                # The vendor stopped answering mid-list. What was read still
                # settles an order found in it; absence from it settles nothing.
                logger.warning(
                    "[OrderStatus] %s for %s: %s; absence from this list is not "
                    "evidence",
                    query.tool, self.reader, e,
                )
                return Listing(orders, complete=False)
            if page is None:
                return Listing(orders, complete=True)
        logger.warning(
            "[OrderStatus] %s for %s: %d pages read and the vendor still has "
            "more; absence from this list is not evidence",
            query.tool, self.reader, MAX_LISTING_PAGES,
        )
        return Listing(orders, complete=False)


async def _connection(user_id: str, server: str, *, by_vendor: bool) -> Any:
    """The connection named ``server`` or, with ``by_vendor``, the user's one
    connection to the brokerage of that name when there is exactly one."""
    named = await get_connection(user_id, server)
    if named is not None or not by_vendor:
        return named
    wanted = server.strip().casefold()
    matches = [
        row
        for row in await list_connections(user_id)
        if (vendor_for_url(row.get("server_url")) or "") == wanted
    ]
    if len(matches) == 1:
        return await get_connection(user_id, matches[0]["server_name"])
    return None


async def status_reader(
    user_id: str,
    server: str,
    *,
    reader: str,
    by_vendor: bool = False,
    serves: Callable[[str, OrderAdapter], bool] | None = None,
) -> StatusReader | Unreachable:
    """The reader for ``server``, or the first link of the chain that is missing.

    ``serves`` is the caller's own test of the adapter, run before the grant is
    looked up, so a brokerage that cannot do what the caller needs is told as
    that rather than as whatever the grant says.
    """
    connection = await _connection(user_id, server, by_vendor=by_vendor)
    if connection is None:
        return Unreachable("no_connection", server)
    if connection.status not in SERVABLE:
        return Unreachable("not_connected", connection.server_name)
    vendor = vendor_for_url(connection.server_url) or ""
    adapter = adapter_for(vendor)
    if adapter is None or (serves is not None and not serves(vendor, adapter)):
        return Unreachable("unsupported", connection.server_name, vendor or None)
    grant = await active_grant_for_connection(user_id, str(connection.connection_id))
    if grant is None:
        return Unreachable("no_grant", connection.server_name, vendor)
    return StatusReader(
        user_id=user_id,
        server=connection.server_name,
        vendor=vendor,
        adapter=adapter,
        grant=grant,
        reader=reader,
    )
