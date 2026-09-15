"""The per-call gate: what the port refuses when the row changed under a turn."""

from types import SimpleNamespace

import pytest

from src.server.services.egress.direct_tools import DirectMCPBinding

MOOMOO_URL = "https://mcp.moomoo.com/mcp"
HEADER_URL = "https://example.com/mcp"
LIVE = "trading_order_place"
PAPER_ORDER = "sim_trade_input_order"
PAPER_READ = "sim_trade_position_list"
HEADER_TOOL = "search_docs"


def _row(url: str, **fields) -> dict:
    """A catalog row as the reader hands it back. ``transport`` is load-bearing:
    the relay dials streamable HTTP, so anything else clamps every tool back to
    the sandbox."""
    return {"transport": "http", "url": url, "enabled": True, **fields}


def _connected(
    monkeypatch, reads: list, order_approval: dict | None = None, **row_fields
):
    async def _get_connection(user_id, server):
        return SimpleNamespace(
            server_url=MOOMOO_URL,
            status="connected",
            granted_capabilities=("account", "trading", "paper_trading"),
        )

    async def _get_catalog_server(user_id, name):
        reads.append(name)
        return _row(MOOMOO_URL, order_approval=order_approval or {}, **row_fields)

    monkeypatch.setattr(
        "src.server.services.egress.direct_tools.get_connection", _get_connection
    )
    monkeypatch.setattr(
        "src.server.database.mcp_servers.get_catalog_server", _get_catalog_server
    )


def _header(monkeypatch, tool_binding: dict):
    """A row that authenticates with its own headers: no connection, no consent,
    and an uncurated address, so its bindings are the row's alone."""

    async def _get_connection(user_id, server):
        return None

    async def _get_catalog_server(user_id, name):
        return _row(HEADER_URL, tool_binding=dict(tool_binding))

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
async def test_every_call_reads_the_row_once(monkeypatch):
    """Consent, binding and gate all come off one read, so a position list pays
    a catalog read like an order does, and no call pays two."""
    reads: list[str] = []
    _connected(monkeypatch, reads)
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", PAPER_READ, False) is None
    assert reads == ["moomoo"]


@pytest.mark.asyncio
async def test_a_curated_direct_tool_passes_with_nothing_stored_on_the_row(monkeypatch):
    """An untouched row is the common case: the paper group binds direct on its
    own, so the binding check must pass a tool no override ever named."""
    _connected(monkeypatch, [])
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", PAPER_READ, False) is None


@pytest.mark.asyncio
async def test_a_paper_order_is_ungated_by_default_and_gated_when_asked(monkeypatch):
    """The switch is per mode now. A paper order runs without a stop unless
    this connection asked to be asked about paper, and then an ungated call
    bound before the change is refused the way a live one is."""
    reads: list[str] = []
    _connected(monkeypatch, reads)
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("moomoo", PAPER_ORDER, False) is None
    assert reads == ["moomoo"], "the call reads the row it is judged against"

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


@pytest.mark.asyncio
async def test_a_tool_moved_back_to_the_sandbox_mid_turn_is_refused(monkeypatch):
    """The relay's direct-only gate refuses a sandbox caller reaching a direct
    tool, which is the other direction: this call arrives as the host, so
    nothing downstream would stop a tool the user just sent back to the
    sandbox."""
    _header(monkeypatch, {HEADER_TOOL: "ptc"})
    binding = DirectMCPBinding(user_id="u1")
    reason = await binding.check("docs", HEADER_TOOL, False)
    assert reason and "no longer bound directly" in reason


@pytest.mark.asyncio
async def test_a_tool_the_row_still_binds_directly_is_not_refused(monkeypatch):
    """The same row one edit earlier: the binding check must not cost the turn
    the tools it was right to bind."""
    _header(monkeypatch, {HEADER_TOOL: "direct"})
    binding = DirectMCPBinding(user_id="u1")
    assert await binding.check("docs", HEADER_TOOL, False) is None


@pytest.mark.asyncio
async def test_a_connected_row_switched_off_mid_turn_is_refused(monkeypatch):
    """The row is where a server is taken out of delivery, and a live token
    says nothing about that: read only on the header branch, the switch left
    every tool of an OAuth-connected server callable for the rest of the
    turn."""
    _connected(monkeypatch, [], enabled=False)
    binding = DirectMCPBinding(user_id="u1")
    reason = await binding.check("moomoo", PAPER_READ, False)
    assert reason and "switched off" in reason


@pytest.mark.asyncio
async def test_a_connected_row_whose_plugin_is_switched_off_is_refused(monkeypatch):
    """A plugin's disable leaves its rows' own flag alone, so the plugin's flag
    is the only one saying these tools are gone."""
    _connected(monkeypatch, [], plugin_enabled=False)
    binding = DirectMCPBinding(user_id="u1")
    reason = await binding.check("moomoo", PAPER_READ, False)
    assert reason and "switched off" in reason
