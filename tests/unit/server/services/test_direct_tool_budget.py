"""The whole-turn budget over directly bound MCP tools.

Per-server discovery caps size the cached JSON; these size what one model
request is asked to carry, which nothing bounded before.
"""

import pytest

from src.server.services.egress.direct_tools import (
    MAX_DIRECT_SCHEMA_CHARS,
    MAX_DIRECT_TOOLS,
    admit_within_budget,
)
from src.server.services.egress import direct_tools
from src.server.services.mcp_tool_split import DirectServerTools, DirectTool
from src.server.services.tool_binding import OrderPolicy, Resolved


def _entry(*tools: DirectTool, vendor: str | None = None) -> DirectServerTools:
    return DirectServerTools(tools=tools, vendor=vendor)


def _read(name: str, description: str = "d") -> DirectTool:
    schema = {"name": name, "description": description, "input_schema": {}}
    return DirectTool(schema, Resolved("direct", "policy"))


def _order(name: str) -> DirectTool:
    schema = {"name": name, "description": "d", "input_schema": {}}
    policy = OrderPolicy("place", "live", True)
    return DirectTool(schema, Resolved("direct", "policy", order=policy))


def _reads(prefix: str, n: int, description: str = "d") -> tuple[DirectTool, ...]:
    return tuple(_read(f"{prefix}_{i}", description) for i in range(n))


def test_everything_fits_when_the_set_is_small():
    by_server = {"a": _entry(*_reads("a", 3)), "b": _entry(*_reads("b", 2))}
    admitted, dropped = admit_within_budget(by_server)
    assert dropped == []
    assert [t.name for t in admitted["a"]] == ["a_0", "a_1", "a_2"]
    assert [t.name for t in admitted["b"]] == ["b_0", "b_1"]


def test_a_crowded_server_cannot_starve_a_small_one():
    by_server = {"big": _entry(*_reads("big", 200)), "small": _entry(*_reads("small", 4))}
    admitted, dropped = admit_within_budget(by_server)
    # Round-robin: the small server is reached on the first four rotations, so
    # it keeps every tool even though the big one alone overruns the cap.
    assert [t.name for t in admitted["small"]] == ["small_0", "small_1", "small_2", "small_3"]
    assert len(admitted["big"]) == MAX_DIRECT_TOOLS - 4
    assert dropped and all(server == "big" for server, _ in dropped)


def test_the_tool_count_cap_is_the_total_not_the_per_server_one():
    by_server = {name: _entry(*_reads(name, 40)) for name in ("a", "b", "c")}
    admitted, _ = admit_within_budget(by_server)
    assert sum(len(v) for v in admitted.values()) == MAX_DIRECT_TOOLS


def test_a_fat_schema_is_charged_by_size_not_by_count():
    fat = "x" * (MAX_DIRECT_SCHEMA_CHARS // 4)
    by_server = {"a": _entry(*_reads("a", 8, description=fat))}
    admitted, dropped = admit_within_budget(by_server)
    assert len(admitted["a"]) < 8
    assert dropped
    # Well under the count cap, so size is what stopped it.
    assert len(admitted["a"]) < MAX_DIRECT_TOOLS


def test_a_server_with_no_schemas_is_not_an_error():
    by_server = {"a": _entry(), "b": _entry(*_reads("b", 2))}
    admitted, dropped = admit_within_budget(by_server)
    assert admitted["a"] == []
    assert len(admitted["b"]) == 2
    assert dropped == []


@pytest.mark.asyncio
async def test_a_server_without_a_grant_does_not_spend_the_budget(monkeypatch):
    """A connection can go away between the split and the bind.

    Its schemas are still in ``by_server`` but it has no grant, so it is
    skipped moments later. Charging it first would displace tools from a
    healthy connection and leave the capacity spent on nothing.
    """
    monkeypatch.setattr(direct_tools, "EGRESS_RELAY_SECRET", "s")
    seen: dict = {}

    def _spy(by_server):
        seen["names"] = sorted(by_server)
        return {name: [] for name in by_server}, []

    monkeypatch.setattr(direct_tools, "admit_within_budget", _spy)

    await direct_tools.prepare_direct_mcp_tools(
        user_id="u",
        workspace_id="w",
        sandbox_id="sb",
        grants={"live": "grant-1"},
        by_server={"live": _entry(*_reads("live", 2)), "dead": _entry(*_reads("dead", 99))},
    )

    assert seen["names"] == ["live"]


MOOMOO_ORDERS = ("trading_order_place", "sim_trade_input_order")


def _moomoo(order_names=MOOMOO_ORDERS, reads=80):
    """A moomoo entry whose order tools sit at the very back of the queue."""
    return _entry(
        *_reads("quote", reads), *(_order(n) for n in order_names), vendor="moomoo"
    )


def test_an_order_tool_is_seated_before_everything_else():
    """Round-robin alone let a ``trading_order_place`` at the back of an
    eighty-tool list lose its seat to a quote tool, which is a brokerage whose
    reads work and whose orders are gone."""
    by_server = {"moomoo": _moomoo(), "other": _entry(*_reads("other", 80))}
    admitted, dropped = admit_within_budget(by_server)
    seated = [t.name for t in admitted["moomoo"]]
    assert seated[: len(MOOMOO_ORDERS)] == list(MOOMOO_ORDERS)
    assert not any(t.name in MOOMOO_ORDERS for _, t in dropped)
    assert sum(len(v) for v in admitted.values()) == MAX_DIRECT_TOOLS


def test_priority_does_not_change_what_a_turn_can_afford():
    by_server = {"moomoo": _moomoo(), "other": _entry(*_reads("other", 80))}
    admitted, dropped = admit_within_budget(by_server)
    total = sum(len(v) for v in admitted.values())
    assert total == MAX_DIRECT_TOOLS
    assert total + len(dropped) == 82 + 80


def test_a_vendor_with_no_order_tools_is_still_round_robined():
    by_server = {
        "moomoo": _entry(*_reads("quote", 200), vendor="moomoo"),
        "small": _entry(*_reads("small", 4)),
    }
    admitted, _ = admit_within_budget(by_server)
    assert [t.name for t in admitted["small"]] == [
        "small_0", "small_1", "small_2", "small_3"
    ]


@pytest.mark.asyncio
async def test_a_starved_order_tool_takes_the_whole_server_with_it(monkeypatch, caplog):
    """Binding the reads and not the orders is the failure that looks healthy:
    the model composes an order out of what is left and the user never learns
    the order tool was not there."""
    monkeypatch.setattr(direct_tools, "EGRESS_RELAY_SECRET", "s")
    seen: dict = {}

    def _spy(by_server):
        seen["called"] = True
        return (
            {name: [] for name in by_server},
            [("moomoo", _order("trading_order_place")), ("moomoo", _read("quote_stock_quote"))],
        )

    monkeypatch.setattr(direct_tools, "admit_within_budget", _spy)
    with caplog.at_level("WARNING"):
        binding = await direct_tools.prepare_direct_mcp_tools(
            user_id="u",
            workspace_id="w",
            sandbox_id="sb",
            grants={"moomoo": "grant-1"},
            by_server={"moomoo": _moomoo()},
        )
    assert seen["called"]
    assert binding.tools == []
    assert "order tools over budget" in caplog.text
    assert "trading_order_place" in caplog.text
