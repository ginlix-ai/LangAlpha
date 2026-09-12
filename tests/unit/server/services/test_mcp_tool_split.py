"""The composite and the Flash binder split one server's tools the same way."""

from __future__ import annotations

from src.server.services.mcp_tool_split import (
    build_direct_entries,
    split_server_tools,
)
from src.server.services.tool_binding import BindingPlan, OrderPolicy, Resolved


def _tool(name: str, **props) -> dict:
    return {"name": name, "input_schema": {"type": "object", "properties": props}}


def _plan(**bindings: str) -> BindingPlan:
    """A plan shaped the way ``resolve_plan`` builds one: every set is a
    projection of ``by_tool``, so the split cannot read two of them apart."""
    by_tool = {
        name: Resolved(
            binding,
            "policy",
            order=(
                OrderPolicy("place", "live", True)
                if name == "trading_order_place"
                else None
            ),
        )
        for name, binding in bindings.items()
    }
    return BindingPlan(
        direct=frozenset(
            t for t, r in by_tool.items() if r.binding in ("direct", "both")
        ),
        sandbox_excluded=frozenset(
            t for t, r in by_tool.items() if r.binding == "direct"
        ),
        by_tool=by_tool,
    )


TOOLS = [
    _tool("account_list"),
    _tool("Trading_Order_Place"),
    _tool("sim_trade_input_order"),
]


def test_no_plan_means_everything_goes_to_the_sandbox():
    sandbox, direct = split_server_tools(TOOLS, denied=None, plan=None)
    assert [t["name"] for t in sandbox] == [t["name"] for t in TOOLS]
    assert direct is None


def test_direct_only_tools_leave_the_sandbox_and_both_keeps_them():
    plan = _plan(trading_order_place="direct", sim_trade_input_order="both")
    sandbox, direct = split_server_tools(TOOLS, denied=None, plan=plan)
    assert [t["name"] for t in sandbox] == ["account_list", "sim_trade_input_order"]
    # Folded matching: the vendor's recased name still lands in the direct set.
    assert [t.name for t in direct.tools] == [
        "Trading_Order_Place",
        "sim_trade_input_order",
    ]
    # A ``both`` tool keeps the wrapper the prompt may still point the model at.
    assert [t.sandboxed for t in direct.tools] == [False, True]
    # Folded once, here, so no consumer has to remember to fold it again: the
    # recased schema carries the resolution stored under the plain spelling.
    assert [t.resolved.approval for t in direct.tools] == [True, False]


def test_a_denied_tool_reaches_neither_path():
    plan = _plan(trading_order_place="direct")
    sandbox, direct = split_server_tools(
        TOOLS, denied=frozenset({"trading_order_place"}), plan=plan
    )
    assert [t["name"] for t in sandbox] == ["account_list", "sim_trade_input_order"]
    assert direct is None


class _Server:
    def __init__(self, name: str, url: str = "https://vendor.example.com/mcp") -> None:
        self.name = name
        self.url = url


class _Snapshots:
    def __init__(self, by_name: dict[str, list[dict] | None]) -> None:
        self.by_name = by_name

    def ok(self, server) -> dict | None:
        tools = self.by_name.get(server.name)
        return None if tools is None else {"tools": tools}


def test_build_direct_entries_keys_both_halves_by_server_name():
    servers = [_Server("moomoo"), _Server("nosnapshot")]
    plan = _plan(trading_order_place="direct")
    sandbox, direct = build_direct_entries(
        servers,
        _Snapshots({"moomoo": TOOLS, "nosnapshot": None}),
        denied={},
        plans={"moomoo": plan},
    )
    # A server with no usable snapshot is in neither half, which is how the
    # composite install reads settlement.
    assert set(sandbox) == {"moomoo"}
    assert [t["name"] for t in sandbox["moomoo"]] == [
        "account_list",
        "sim_trade_input_order",
    ]
    assert [t.name for t in direct["moomoo"].tools] == ["Trading_Order_Place"]


def test_build_direct_entries_keeps_a_planless_server_out_of_the_direct_half():
    sandbox, direct = build_direct_entries(
        [_Server("moomoo")], _Snapshots({"moomoo": TOOLS}), denied={}, plans={}
    )
    assert [t["name"] for t in sandbox["moomoo"]] == [t["name"] for t in TOOLS]
    assert direct == {}
