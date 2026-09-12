"""The per-call gate: what the port refuses when the row changed under a turn."""

from types import SimpleNamespace

import pytest

from src.server.services.egress.direct_tools import DirectMCPBinding

MOOMOO_URL = "https://mcp.moomoo.com/mcp"
LIVE = "trading_order_place"
PAPER_ORDER = "sim_trade_input_order"
PAPER_READ = "sim_trade_position_list"


def _connected(monkeypatch, reads: list, order_approval: dict | None = None):
    async def _get_connection(user_id, server):
        return SimpleNamespace(
            server_url=MOOMOO_URL,
            status="connected",
            granted_capabilities=("account", "trading", "paper_trading"),
        )

    async def _get_catalog_server(user_id, name):
        reads.append(name)
        return {"order_approval": order_approval or {}}

    monkeypatch.setattr(
        "src.server.services.egress.direct_tools.get_connection", _get_connection
    )
    monkeypatch.setattr(
        "src.server.database.mcp_servers.get_catalog_server", _get_catalog_server
    )


@pytest.mark.asyncio
async def test_a_call_bound_before_the_row_gated_it_is_refused(monkeypatch):
    """The interrupt is wired from the stamp made at turn start, so a turn that
    bound this tool ungated has no stop to raise. Refusing sends the user back
    through a turn whose tools carry the current answer."""
    _connected(monkeypatch, [])
    binding = DirectMCPBinding(user_id="u1")
    reason = await binding.check("moomoo", LIVE, False)
    assert reason and "approval" in reason


@pytest.mark.asyncio
async def test_a_call_the_turn_already_gated_is_not_refused_again(monkeypatch):
    """The stamp is the turn's own answer. A gated call is stopped by the
    interrupt, not by this port, or the order could never be placed at all."""
    _connected(monkeypatch, [])
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", LIVE, True) is None


@pytest.mark.asyncio
async def test_a_tool_that_mutates_no_order_does_not_read_the_row(monkeypatch):
    """The order map decides the question alone, so a position list must not
    pay a catalog read on every call to be told nothing gates it."""
    reads: list[str] = []
    _connected(monkeypatch, reads)
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", PAPER_READ, False) is None
    assert reads == []


@pytest.mark.asyncio
async def test_a_paper_order_is_ungated_by_default_and_gated_when_asked(monkeypatch):
    """The switch is per mode now. A paper order runs without a stop unless
    this connection asked to be asked about paper, and then an ungated call
    bound before the change is refused the way a live one is."""
    reads: list[str] = []
    _connected(monkeypatch, reads)
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", PAPER_ORDER, False) is None
    assert reads == ["moomoo"], "an order tool reads the row for its mode"

    _connected(monkeypatch, reads, order_approval={"paper": True})
    reason = await binding.check("moomoo", PAPER_ORDER, False)
    assert reason and "approval" in reason


@pytest.mark.asyncio
async def test_a_live_order_the_row_stopped_asking_about_is_not_refused(monkeypatch):
    """Turning the mode's switch off is the one way to stop being asked, and
    it has to reach the per-call re-read too, or the call the turn bound
    ungated would be refused for not having been gated."""
    _connected(monkeypatch, [], order_approval={"live": False})
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", LIVE, False) is None
