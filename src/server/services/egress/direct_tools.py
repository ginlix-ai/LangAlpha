"""Directly bound MCP tools: JSON tool calls from the model, through the relay.

The sandbox path hands the model a Python wrapper per MCP tool and lets it
compose them in ``ExecuteCode``. The direct path binds the tool itself, with
its vendor schema verbatim, so a call is one typed tool call the middleware
sees and the UI can render. Both paths reach the vendor through the same
egress grant and the same relay route: the host dials its own relay on
loopback with a relay JWT, so policy, token attach and audit are one code
path however the call arrived.

The relay sessions are opened inside the run, not the request. The graph is
driven by the run manager's task and outlives the request's generator when
the client drops, and the transport's session lives in the task that entered
it, so the sessions are entered and closed by ``drive`` around the run
stream itself. Between them the client is held open, so each call reuses
one session instead of paying a handshake through the relay.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import warnings
from collections.abc import AsyncIterator, Awaitable, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from hashlib import sha1
from typing import TYPE_CHECKING, Any, Mapping

from langchain_core.tools import BaseTool

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import execution_token
from src.config.env import EGRESS_RELAY_LOOPBACK_URL, EGRESS_RELAY_SECRET
from src.server.database.mcp_oauth import SERVABLE, ConnectionStatus, get_connection
from src.server.services.brokerage_capabilities import (
    denied_tools,
    order_tool,
    vendor_for_url,
)
from src.server.services.egress import folded_contains
from src.server.services.egress.execution_token import EXECUTION_HEADER
from src.server.services.egress.relay_jwt import (
    CALLER_HOST,
    identity_claim,
    mint_relay_jwt,
)
from src.server.services.mcp_tool_split import DirectServerTools, DirectTool
from src.server.services.tool_binding import (
    inputs_from_row,
    order_payload,
    order_policy,
    resolve_plan,
)

if TYPE_CHECKING:
    from ptc_agent.core.session import Session
    from src.server.services.computer_manager._types import WorkspaceToolView
    from src.server.services.egress.order_ledger import OrderAttemptLedger

logger = logging.getLogger(__name__)

_NAME_SAFE = re.compile(r"[^A-Za-z0-9_-]")
# The strictest provider limit on a tool name.
_MAX_TOOL_NAME = 64

# The sandbox_id claim on a host-only turn. Audit-only at the relay, which
# authorizes on the (user, workspace) pair, but the claim must be non-empty.
FLASH_SANDBOX_ID = "flash"


def direct_tool_name(server: str, tool: str) -> str:
    """``mcp__<server>__<tool>``: distinct per server so two vendors' ``account_*``
    tools cannot collide, and prefixed so the frontend can route on it.

    The plain form is kept only while it is unambiguous. A segment the provider
    would reject, a server name carrying the ``__`` separator, or an overflow of
    the limit each fall back to a digest of the *pair*, because two tools bound
    under one name are not merely mislabelled: the tools node keeps whichever
    came last, and ``DirectMcpPolicyMiddleware`` then gates the call against the
    other tool's connection.

    Over the limit it is the *server* that gives way, never the tool: a long
    server name would otherwise eat the whole budget and leave ``read_order``
    and ``cancel_order`` the same name. Only a tool long enough to overrun on
    its own is trimmed, and the digest sits ahead of the cut so identity
    survives it.
    """
    safe_server = _NAME_SAFE.sub("_", server).replace("__", "_")
    safe_tool = _NAME_SAFE.sub("_", tool)
    plain = f"mcp__{safe_server}__{safe_tool}"
    if plain == f"mcp__{server}__{tool}" and len(plain) <= _MAX_TOOL_NAME:
        return plain
    digest = sha1(f"{server}\x00{tool}".encode()).hexdigest()[:8]
    tail = f"_{digest}__{safe_tool}"
    keep = _MAX_TOOL_NAME - len("mcp__") - len(tail)
    if keep >= 1:
        return f"mcp__{safe_server[:keep]}{tail}"
    return f"mcp__{safe_server[:1]}_{digest}__{safe_tool}"[:_MAX_TOOL_NAME]


# A turn's whole direct budget, across every server it binds. The per-server
# discovery caps sit at 128 tools and 400,000 schema characters, but those
# size the cached JSON and the sandbox wrapper module; nothing there bounds
# what a provider is asked to accept in one request. These do. The agent
# already carries roughly 25 to 40 tools of its own, so 64 direct ones keep a
# turn near a hundred definitions, and 120,000 characters of schema is about
# thirty thousand tokens, which is a large but survivable slice of context.
MAX_DIRECT_TOOLS = 64
MAX_DIRECT_SCHEMA_CHARS = 120_000


@dataclass
class _Budget:
    """What a turn has left to spend on direct tool definitions."""

    tools: int = 0
    chars: int = 0

    def take(self, tool: DirectTool) -> bool:
        if self.tools >= MAX_DIRECT_TOOLS:
            return False
        size = len(json.dumps(tool.schema, ensure_ascii=False, default=str))
        if self.chars + size > MAX_DIRECT_SCHEMA_CHARS:
            return False
        self.tools += 1
        self.chars += size
        return True


def admit_within_budget(
    by_server: Mapping[str, Any],
) -> tuple[dict[str, list[DirectTool]], list[tuple[str, DirectTool]]]:
    """Split each server's tools into the ones a turn can afford, and the rest.

    Order tools are seated first, at every server, because losing one to a
    quote tool the user also bound direct is not a smaller toolset but a
    brokerage whose reads work and whose orders are gone. Everything else is
    taken one per server in rotation rather than server by server, so a broker
    publishing eighty tools cannot spend the whole budget before a second
    connection is reached at all: every server keeps a usable share, and a user
    who wants more of one narrows the others on the Plugins page.
    """
    queues = {name: list(entry.tools or ()) for name, entry in by_server.items()}
    admitted: dict[str, list[DirectTool]] = {name: [] for name in queues}
    dropped: list[tuple[str, DirectTool]] = []
    budget = _Budget()

    for name in queues:
        rest: list[DirectTool] = []
        for tool in queues[name]:
            if tool.resolved.order is None:
                rest.append(tool)
            elif budget.take(tool):
                admitted[name].append(tool)
            else:
                dropped.append((name, tool))
        queues[name] = rest

    while any(queues.values()):
        for name, queue in queues.items():
            if not queue:
                continue
            tool = queue.pop(0)
            if budget.take(tool):
                admitted[name].append(tool)
            else:
                dropped.append((name, tool))
    return admitted, dropped


def relay_http_client(**kwargs: Any) -> Any:
    """The MCP client's HTTP client, carrying the order grant when there is one.

    The grant is per call and the session is per turn, so it cannot be a header
    on the transport: it is read from a contextvar at send time. The MCP client
    runs each POST inside the context of whoever sent the message, which is
    what makes a value set around one ``await`` visible to the task that dials.

    Built by the SDK's own factory: a bare ``AsyncClient`` would drop the read
    timeout to five seconds and cut every slow vendor call.
    """
    from mcp.shared._httpx_utils import create_mcp_http_client

    async def attach_grant(request: Any) -> None:
        token = execution_token.get()
        if token:
            request.headers[EXECUTION_HEADER] = token

    # ``follow_redirects`` is what the factory already does and not one of its
    # parameters, so the transport passing it explicitly has to be dropped.
    kwargs.pop("follow_redirects", None)
    client = create_mcp_http_client(**{k: v for k, v in kwargs.items() if v is not None})
    client.event_hooks.setdefault("request", []).append(attach_grant)
    return client


def relay_mcp_client(grant_id: str, *, token: str) -> Any:
    """One MCP client dialling the host's own relay on loopback for one grant.

    The only construction of it, because everything that makes a host-side call
    a host-side call is here: the loopback address, the grant in the path, the
    relay JWT, and the HTTP client that attaches an execution grant when the
    caller holds one. A second copy would be a second answer to "does this call
    carry the order token".
    """
    from fastmcp import Client
    from fastmcp.client.transports import StreamableHttpTransport

    base = EGRESS_RELAY_LOOPBACK_URL.rstrip("/")
    return Client(
        StreamableHttpTransport(
            url=f"{base}/v1/egress/{grant_id}",
            headers={"Authorization": f"Bearer {token}"},
            httpx_client_factory=relay_http_client,
        )
    )


# How long a turn waits for one relay session before giving up on opening it
# ahead of time. Opening early only buys the first call's handshake, so a
# session that is not ready quickly is worth less than the delay it adds in
# front of the first token. A server that times out here is treated exactly
# like one that refused to open: warned about, left closed, and reopened per
# call by the tools that need it.
SESSION_OPEN_TIMEOUT_S = 5.0


@dataclass
class DirectMCPBinding:
    """The tools bound for one turn, the clients behind them, and the policy.

    ``check`` is the port ``DirectMcpPolicyMiddleware`` calls per tool call. It
    reads the server's own row again rather than the set bound at turn start:
    consent withdrawn on the Plugins page mid-turn, a connection that went to
    ``needs_reauth``, a row or its plugin switched off, a tool moved back to
    the sandbox, or an order gate switched on, refuses the next call instead
    of the next turn.
    """

    user_id: str | None = None
    tools: list[BaseTool] = field(default_factory=list)
    _clients: list[tuple[str, Any]] = field(default_factory=list)

    def add_server(self, name: str, client: Any) -> None:
        self._clients.append((name, client))

    async def check(
        self, server: str, tool: str, approval: bool = False
    ) -> str | None:
        if not self.user_id:
            return "no user is attached to this turn"
        connection = await get_connection(self.user_id, server)
        # A revoked record is history, not a claim on the row: the user
        # disconnected, and the row's own headers may authenticate it now.
        if connection is not None and connection.status is ConnectionStatus.REVOKED:
            connection = None
        if connection is not None and connection.status not in SERVABLE:
            return f"{server} needs to be reconnected before it can be used"
        from src.server.database.mcp_servers import get_catalog_server

        try:
            row = await get_catalog_server(self.user_id, server)
        except Exception:
            # Fails closed, because the alternative is judging the call against
            # a row nobody could read: the consent, the binding and the gate all
            # come off this one read.
            logger.warning(
                "[DIRECT_MCP] %r/%r: catalog re-read failed",
                server,
                tool,
                exc_info=True,
            )
            return f"{server} could not be read just now; send this again"
        identity = self._identity(connection, row)
        if identity is None:
            return f"{server} is switched off or no longer connected"
        vendor, granted = identity
        denied = denied_tools(vendor, granted)
        if denied and folded_contains(denied, tool):
            return f"the connection to {server} does not permit {tool}"
        # Resolved the way the binder resolved it, so a tool the user moved
        # back to the sandbox mid-turn stops being callable here. The relay's
        # own direct-only gate cannot catch this one: it refuses a sandbox
        # caller reaching a direct tool, and this call arrives as the host.
        plan = resolve_plan(vendor, granted, inputs_from_row(row))
        if not folded_contains(plan.direct, tool):
            return (
                f"{tool} is no longer bound directly; "
                "send this again to reach it from the sandbox"
            )
        # The interrupt is wired from the stamp made at turn start, so a switch
        # to asking about orders would otherwise not reach a tool already bound:
        # the call would run ungated for the rest of the turn. Refusing sends
        # the user back through a turn whose tools carry the current answer.
        if not approval and self._now_needs_approval(tool, vendor, row):
            return (
                f"{tool} now needs your approval on every call; "
                "send this again so it can ask you first"
            )
        return None

    def _identity(
        self, connection: Any, row: Mapping[str, Any] | None
    ) -> tuple[str | None, Sequence[str]] | None:
        """Whose rules judge this call and what was consented to, or None when
        the server is gone or out of delivery.

        A connection answers both, and its own address outranks the row's:
        consent was given for what the token was issued for. A
        header-authenticated row answers with its own address and consent to
        nothing, which is what denies a curated vendor's whole curation to a
        row that never went through a consent screen.
        """
        # The row governs either credential kind: a live token says nothing
        # about a server the user has taken out of delivery. A plugin's disable
        # leaves its rows' own flag alone, and the plugin's is what takes them
        # out of delivery -- the relay reads both the same way.
        if row is None or not row.get("enabled") or row.get("plugin_enabled") is False:
            return None
        if connection is not None:
            return (
                vendor_for_url(connection.server_url),
                connection.granted_capabilities or (),
            )
        return vendor_for_url(row.get("url")), ()

    def _now_needs_approval(
        self, tool: str, vendor: str | None, row: Mapping[str, Any] | None
    ) -> bool:
        """Whether the row gates this tool right now, not at turn start.

        The order map answers first because it answers alone: a tool that
        mutates no order is gated by nothing a row can say, so the row's
        switches are read only for one that does.
        """
        if order_tool(vendor, tool) is None:
            return False
        policy = order_policy(
            vendor, tool, order_approval=inputs_from_row(row).order_approval
        )
        return policy is not None and policy.approval

    async def drive(self, stream: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Run ``stream`` with every relay session open around it.

        A server whose session cannot be opened is left closed with a warning
        rather than failing the turn: its tools then open a session per call,
        or fail per call with the same reason, and the rest of the toolset is
        unaffected.
        """
        if not self._clients:
            async for event in stream:
                yield event
            return
        async with AsyncExitStack() as stack:

            async def _open(server: str, client: Any) -> None:
                try:
                    await asyncio.wait_for(
                        stack.enter_async_context(client), SESSION_OPEN_TIMEOUT_S
                    )
                except TimeoutError:
                    logger.warning(
                        "[DIRECT_MCP] %r: relay session did not open within %ss; "
                        "its tools will open one per call",
                        server,
                        SESSION_OPEN_TIMEOUT_S,
                    )
                except Exception as e:
                    logger.warning(
                        "[DIRECT_MCP] %r: relay session failed to open: %s", server, e
                    )

            # Concurrently, because these handshakes stand between the user and
            # the first token: opened in sequence, one slow server delays every
            # server behind it and the waits add up across the whole set, so a
            # turn that would never call those tools still looks hung.
            await asyncio.gather(*(_open(s, c) for s, c in self._clients))
            async for event in stream:
                yield event


async def prepare_direct_mcp_tools(
    *,
    user_id: str | None,
    workspace_id: str,
    sandbox_id: str | None = None,
    computer_id: str | None = None,
    grants: Mapping[str, str],
    by_server: Mapping[str, DirectServerTools],
) -> DirectMCPBinding:
    """Build the tools for one turn; nothing is dialled until ``drive``."""
    binding = DirectMCPBinding(user_id=user_id)
    if not by_server or not grants or not EGRESS_RELAY_SECRET or not user_id:
        return binding

    from mcp.types import Tool

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from langchain.mcp import as_langchain_tool

    minted = mint_relay_jwt(
        EGRESS_RELAY_SECRET,
        user_id=user_id,
        workspace_id=workspace_id,
        sandbox_id=sandbox_id,
        computer_id=computer_id,
        caller=CALLER_HOST,
    )

    # Only a granted server can be reached, so only a granted server may spend
    # the budget: one that lost its grant would otherwise displace tools from a
    # healthy connection and then be skipped anyway, leaving the capacity spent
    # on nothing.
    grantable = {name: entry for name, entry in by_server.items() if grants.get(name)}
    affordable, dropped = admit_within_budget(grantable)
    if dropped:
        logger.warning(
            "[DIRECT_MCP] over the turn budget of %d tools / %d schema chars: "
            "dropped %d tool(s): %s",
            MAX_DIRECT_TOOLS,
            MAX_DIRECT_SCHEMA_CHARS,
            len(dropped),
            ", ".join(f"{s}/{t.name}" for s, t in dropped[:20]),
        )
    # An order tool that could not be seated even with priority means the whole
    # server stays unbound: a brokerage whose reads answer and whose orders
    # quietly are not there reads as working, and the model would compose an
    # order out of the tools that remain.
    starved: dict[str, list[str]] = {}
    for server, tool in dropped:
        if tool.resolved.order is not None:
            starved.setdefault(server, []).append(tool.name)
    for server, order_tools in starved.items():
        logger.warning(
            "[DIRECT_MCP] %r: order tools over budget (%s); none of its tools "
            "are bound directly this turn",
            server,
            ", ".join(sorted(order_tools)),
        )
        affordable[server] = []

    for server, entry in by_server.items():
        grant_id = grants.get(server)
        affordable_tools = affordable.get(server) or ()
        if not grant_id or not affordable_tools:
            continue
        client = relay_mcp_client(grant_id, token=minted.token)
        bound = 0
        for direct in affordable_tools:
            schema = direct.schema
            try:
                tool = Tool(
                    name=schema["name"],
                    description=schema.get("description"),
                    input_schema=schema.get("input_schema") or {"type": "object"},
                )
                # No I/O for a client that is not entered: the session opens
                # in ``drive``.
                lc_tool = await as_langchain_tool(tool, client)
            except Exception as e:
                logger.warning(
                    "[DIRECT_MCP] %r/%r: could not bind: %s",
                    server,
                    schema.get("name"),
                    e,
                )
                continue
            lc_tool.name = direct_tool_name(server, tool.name)
            # Everything but the vendor comes off the resolution the plan
            # already made, so the composite, the relay and this stamp cannot
            # disagree about what a call is or which calls stop.
            lc_tool.metadata = {
                **(lc_tool.metadata or {}),
                METADATA_KEY: {
                    "server": server,
                    "tool": tool.name,
                    # The brokerage whose rules this call is judged by, from
                    # the address the grant was issued for. The server name is
                    # the user's to choose, so it says nothing about a vendor.
                    "vendor": entry.vendor,
                    # What this call does to an order, or null when it does
                    # nothing to one. Read by the surface that renders the
                    # call and by whatever governs it.
                    "order": order_payload(direct.resolved.order),
                    # A `both` tool keeps its sandbox wrapper, so the prompt
                    # must not tell the model this one is call-only.
                    "sandboxed": direct.sandboxed,
                    "approval": direct.resolved.approval,
                },
            }
            binding.tools.append(lc_tool)
            bound += 1
        if bound:
            binding.add_server(server, client)
            logger.info(
                "[DIRECT_MCP] %r: bound %d tool(s) through grant %s",
                server,
                bound,
                grant_id,
            )
    return binding


async def direct_tools_for_turn(
    binding: Awaitable[DirectMCPBinding],
    *,
    user_id: str | None,
    workspace_id: str,
    thread_id: str,
    run_id: str,
    turn_index: int,
) -> tuple[DirectMCPBinding, "OrderAttemptLedger | None"]:
    """This turn's direct toolset, and the ledger its orders are written to.

    A binder that fails costs the turn its direct tools and not the turn: on
    PTC the rest of the toolset is still reachable through the sandbox, and on
    Flash the agent still answers. The ledger carries ids only, because every
    transition it makes is a guarded statement against the row: the worker that
    ends up holding the tool call is never assumed to be the one that proposed
    it.
    """
    from src.server.services.egress.order_ledger import OrderAttemptLedger

    try:
        bound = await binding
    except Exception:
        logger.warning("[DIRECT_MCP] binding failed; running without", exc_info=True)
        bound = DirectMCPBinding(user_id=user_id)
    if not user_id:
        return bound, None
    return bound, OrderAttemptLedger(
        user_id=user_id,
        workspace_id=workspace_id,
        thread_id=thread_id,
        run_id=run_id,
        turn_index=turn_index,
    )


async def bind_direct_mcp_tools(
    session: "Session",
    *,
    user_id: str | None,
    workspace_id: str,
    view: "WorkspaceToolView | None" = None,
) -> DirectMCPBinding:
    """The PTC shape: the split and the grants come from the project's frozen
    view when the caller holds one.

    The project comes from the caller, never from the session: one session
    serves every project on its machine, so its own labels name whichever
    project built it and a turn on any sibling would mint a token claiming
    that one. The same goes for the split: the session's copy is whichever
    sibling resolved last, so a turn reads the view frozen for its project.
    """
    by_server = view.direct_mcp_tools if view is not None else session.direct_mcp_tools
    egress = view.egress_binding if view is not None else session.egress_binding
    if not by_server or egress is None:
        return DirectMCPBinding(user_id=user_id)
    return await prepare_direct_mcp_tools(
        user_id=user_id,
        workspace_id=workspace_id,
        # Absent, not "": the host path runs for a Flash turn and for a session
        # whose sandbox is not provisioned, and the validator refuses an empty
        # identity claim.
        sandbox_id=identity_claim(getattr(session.sandbox, "sandbox_id", None)),
        computer_id=identity_claim(getattr(session, "computer_id", None)),
        grants=egress.grants,
        by_server=by_server,
    )
