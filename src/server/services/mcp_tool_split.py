"""Turn one server's discovered tools into what each path gets.

The composite install and the Flash binder both start from the same three
inputs, the vendor's list, the consent's denial and the row's binding plan,
and must land on the same split, or a tool could be wrapped for the sandbox
on one path and bound directly on the other. Folded names throughout, the way
the relay matches them, so a vendor that recases a name is treated the same
at every gate.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from src.server.services.brokerage_capabilities import vendor_for_url
from src.server.services.brokerage_tool_overlays import overlay_tool_schemas
from src.server.services.egress import fold_tool_name, folded_contains
from src.server.services.tool_binding import BindingPlan, Resolved


@dataclass(frozen=True)
class DirectTool:
    """One directly bound tool: the vendor's schema, and how the row resolved it.

    The resolution travels with the schema so the turn budget, the prompt hint
    and the tool stamp all read one answer instead of each re-deriving it from
    a name.
    """

    schema: dict
    resolved: Resolved

    @property
    def name(self) -> str:
        return str(self.schema.get("name") or "")

    @property
    def sandboxed(self) -> bool:
        """Whether this tool also keeps its sandbox wrapper (bound ``both``).

        The prompt has to say so: the blanket rule is that a direct tool is
        called and not imported, and a ``both`` tool that reads as direct-only
        loses the Python path it was bound ``both`` to keep.
        """
        return self.resolved.binding == "both"


@dataclass(frozen=True)
class DirectServerTools:
    """One server's directly bound tools.

    ``vendor`` rides along because the binder has the server name and nothing
    else, and the brokerage whose rules a call is judged by is a question only
    the address can answer.
    """

    tools: tuple[DirectTool, ...]
    vendor: str | None = None


def split_server_tools(
    tools: list[dict],
    *,
    denied: frozenset[str] | None,
    plan: BindingPlan | None,
    vendor: str | None = None,
) -> tuple[list[dict], DirectServerTools | None]:
    """``(sandbox_tools, direct)``.

    ``sandbox_tools`` is what the wrappers and docs are generated from;
    ``direct`` is None when nothing on this server is bound directly.
    """
    if denied:
        tools = [t for t in tools if not folded_contains(denied, t.get("name"))]
    if plan is None or not plan.direct:
        return tools, None
    # Folded, because the plan is keyed by the curated or stored spelling while
    # a schema carries whatever the vendor publishes. Read off ``by_tool``
    # rather than off ``direct``, so which tools are bound and how they
    # resolved are one answer and not two sets that can disagree.
    by_name = {fold_tool_name(k): r for k, r in sorted(plan.by_tool.items())}
    bound = tuple(
        DirectTool(t, resolved)
        for t in tools
        if (resolved := by_name.get(fold_tool_name(t.get("name")))) is not None
        and resolved.binding in ("direct", "both")
    )
    direct = DirectServerTools(tools=bound, vendor=vendor) if bound else None
    sandbox_tools = [
        t for t in tools if not folded_contains(plan.sandbox_excluded, t.get("name"))
    ]
    return sandbox_tools, direct


def build_direct_entries(
    servers: Iterable[Any],
    snapshots: Any,
    *,
    denied: Mapping[str, frozenset[str]],
    plans: Mapping[str, BindingPlan],
) -> tuple[dict[str, list[dict]], dict[str, DirectServerTools]]:
    """Split every server with a current snapshot, keyed by server name.

    A server with no usable snapshot appears in neither result, which is also
    how the composite install reads settlement.
    """
    sandbox_by_server: dict[str, list[dict]] = {}
    direct_by_server: dict[str, DirectServerTools] = {}
    for server in servers:
        snapshot = snapshots.ok(server)
        if snapshot is None:
            continue
        vendor = vendor_for_url(server.url)
        tools = overlay_tool_schemas(vendor, snapshot.get("tools") or [])
        sandbox_tools, direct = split_server_tools(
            tools,
            denied=denied.get(server.name),
            plan=plans.get(server.name),
            vendor=vendor,
        )
        sandbox_by_server[server.name] = sandbox_tools
        if direct is not None:
            direct_by_server[server.name] = direct
    return sandbox_by_server, direct_by_server
