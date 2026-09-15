"""Which resolved servers earn an egress grant, and of which kind.

One rule, read by the sandbox bind, the Flash bind and the config mutations
that retire a grant before replying. A server that earns a grant on one path
and not another is either a tool the model cannot call or an authorization
overhang, so the rule is pinned here rather than at each caller.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ptc_agent.config.core import MCPServerConfig
from src.server.database.egress_grants import (
    GRANT_KIND_HEADER_MCP,
    GRANT_KIND_OAUTH_MCP,
)
from src.server.services.egress.grant_scope import grant_refs
from src.server.services.mcp_config import (
    Origin,
    ResolvedMCP,
    ResolvedServer,
    State,
)
from src.server.services.mcp_discovery import (
    ToolSnapshotIndex,
    mcp_discovery_fingerprint,
)
from src.server.services.tool_binding import BindingInputs, resolve_plan

USER = "usr-grant-scope-0001"
TOOL = "list_funds"


def _server(
    name: str = "fund_desk",
    *,
    connection_id: str | None = None,
    transport: str = "http",
    url: str | None = "https://vendor.example.test/mcp",
) -> MCPServerConfig:
    return MCPServerConfig(
        name=name,
        transport=transport,
        url=url,
        command="run" if transport == "stdio" else None,
        source="user",
        oauth_connection_id=connection_id,
    )


def _plan(server: MCPServerConfig):
    # The map lives on the catalog row, not the config; the resolver clamps it
    # to what the transport can reach, which is what keeps stdio off direct.
    return resolve_plan(
        None,
        (),
        BindingInputs(
            overrides={TOOL: "direct"}, preset=None, relayable=bool(server.url)
        ),
        candidates=[TOOL],
    )


def _entry(
    server: MCPServerConfig,
    *,
    state: State = State.ACTIVE,
    origin: Origin = Origin.USER,
    bound: bool = True,
    awaiting_probe: bool = False,
) -> ResolvedServer:
    return ResolvedServer(
        config=server,
        origin=origin,
        state=state,
        binding_plan=_plan(server) if bound else None,
        awaiting_probe=awaiting_probe,
    )


def _resolved(*entries: ResolvedServer) -> ResolvedMCP:
    return ResolvedMCP(entries=tuple(entries), version=1)


def _snapshots(*pairs: tuple[MCPServerConfig, str | None]) -> ToolSnapshotIndex:
    """An index holding one snapshot per server, at the verdict given."""
    return ToolSnapshotIndex(
        user_rows=[
            {
                "server_name": s.name,
                "config_hash": mcp_discovery_fingerprint(s),
                "status": "ok",
                "tools": [{"name": TOOL}],
                "last_probe": {} if verdict is None else {"verdict": verdict},
            }
            for s, verdict in pairs
        ]
    )


class TestHeaderKind:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("verdict", ["ok", "ok_authed"])
    async def test_a_clean_remote_row_with_a_direct_tool_earns_one(self, verdict):
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server)),
            user_id=USER,
            snapshots=_snapshots((server, verdict)),
        )

        assert [(r.kind, r.server_name, r.connection_id) for r in refs] == [
            (GRANT_KIND_HEADER_MCP, "fund_desk", None)
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "verdict",
        ["needs_credential", "credential_rejected", "oauth", "missing_secrets",
         "unreachable", None],
    )
    async def test_any_other_verdict_earns_none(self, verdict):
        # A grant for a server that is refusing authorizes a call that can only
        # fail, and the verdict is re-read every sync, so a key that stops
        # working costs the row its grant on the next turn.
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server)),
            user_id=USER,
            snapshots=_snapshots((server, verdict)),
        )

        assert refs == []

    @pytest.mark.asyncio
    async def test_a_row_nothing_has_probed_earns_none(self):
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server)), user_id=USER, snapshots=_snapshots()
        )

        assert refs == []

    @pytest.mark.asyncio
    async def test_a_row_with_no_directly_bound_tool_earns_none(self):
        # The sandbox reaches a header row with the row's own headers and needs
        # no grant, so only a directly bound tool spends one.
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server, bound=False)),
            user_id=USER,
            snapshots=_snapshots((server, "ok")),
        )

        assert refs == []

    @pytest.mark.asyncio
    async def test_a_stdio_row_never_earns_one(self):
        # It cannot be dialled through the relay at all, so tool_binding asking
        # for direct changes nothing.
        server = _server(transport="stdio", url=None)
        refs = await grant_refs(
            _resolved(_entry(server)),
            user_id=USER,
            snapshots=_snapshots((server, "ok")),
        )

        assert refs == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs",
        [{"state": State.DISABLED}, {"origin": Origin.WORKSPACE}],
        ids=["disabled", "workspace-local"],
    )
    async def test_only_an_active_user_tier_row_earns_one(self, kwargs):
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server, **kwargs)),
            user_id=USER,
            snapshots=_snapshots((server, "ok")),
        )

        assert refs == []


class TestOAuthKind:
    @pytest.mark.asyncio
    async def test_a_connection_outranks_the_rows_own_headers(self):
        # One address, one credential: while the connection is servable the
        # relay has a token to spend, and the probe verdict does not enter it.
        server = _server(connection_id="conn-1")
        refs = await grant_refs(
            _resolved(_entry(server)),
            user_id=USER,
            snapshots=_snapshots((server, "credential_rejected")),
        )

        assert [(r.kind, r.connection_id) for r in refs] == [
            (GRANT_KIND_OAUTH_MCP, "conn-1")
        ]

    @pytest.mark.asyncio
    async def test_the_sandbox_path_grants_an_unbound_connection_too(self):
        # Its generated wrappers dial the relay for every OAuth server, so
        # narrowing this set to the directly bound ones would cut the sandbox
        # off from the rest.
        server = _server(connection_id="conn-1")
        refs = await grant_refs(
            _resolved(_entry(server, bound=False)), user_id=USER, snapshots=_snapshots()
        )

        assert [r.kind for r in refs] == [GRANT_KIND_OAUTH_MCP]

    @pytest.mark.asyncio
    async def test_the_flash_shape_drops_it(self):
        # Flash has no sandbox, so a grant for a server it binds no tool of is
        # authority nothing spends.
        server = _server(connection_id="conn-1")
        refs = await grant_refs(
            _resolved(_entry(server, bound=False)),
            user_id=USER,
            direct_only=True,
            snapshots=_snapshots(),
        )

        assert refs == []


@pytest.mark.asyncio
async def test_the_two_kinds_are_decided_per_row_in_one_pass():
    oauth = _server("broker", connection_id="conn-1")
    header = _server("fund_desk")
    refs = await grant_refs(
        _resolved(_entry(oauth), _entry(header)),
        user_id=USER,
        snapshots=_snapshots((header, "ok_authed")),
    )

    assert {r.server_name: r.kind for r in refs} == {
        "broker": GRANT_KIND_OAUTH_MCP,
        "fund_desk": GRANT_KIND_HEADER_MCP,
    }


@pytest.mark.asyncio
async def test_a_workspace_with_no_header_candidate_never_reads_the_probe_index():
    # The index is a query on the request path, and every OAuth-only workspace
    # would pay for it.
    class _Exploding(ToolSnapshotIndex):
        def snapshot(self, *a, **kw):  # pragma: no cover - must not be called
            raise AssertionError("the probe index was read")

    server = _server("broker", connection_id="conn-1")
    refs = await grant_refs(
        _resolved(_entry(server)), user_id=USER, snapshots=_Exploding()
    )

    assert [r.kind for r in refs] == [GRANT_KIND_OAUTH_MCP]


class TestUnprobedKick:
    """A header row whose direct bindings are waiting on a first verdict.

    The resolver clamps those tools back into the sandbox until a probe
    answers, and nothing on the turn path asks for one, so the row would stay
    clamped until the user next opened the Plugins page.
    """

    @pytest.fixture
    def kick(self):
        with patch(
            "src.server.services.mcp_oauth.discovery.schedule_catalog_discovery"
        ) as sched:
            yield sched

    @pytest.mark.asyncio
    async def test_an_unprobed_row_asks_for_one_throttled_probe(self, kick):
        server = _server()
        refs = await grant_refs(
            _resolved(_entry(server, bound=False, awaiting_probe=True)),
            user_id=USER,
            snapshots=_snapshots(),
        )

        assert refs == []
        kick.assert_called_once_with(
            USER, "fund_desk", reason="resolve", throttle=True
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("verdict", ["ok_authed", "credential_rejected"])
    async def test_a_row_that_has_an_answer_is_left_alone(self, kick, verdict):
        # Including a refusal: that is the server's answer, and re-dialling it
        # every workspace resolve buys nothing.
        server = _server()
        await grant_refs(
            _resolved(_entry(server)),
            user_id=USER,
            snapshots=_snapshots((server, verdict)),
        )

        kick.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stdio_row_is_never_kicked(self, kick):
        # No address the relay could dial, so no verdict could release it.
        server = _server(transport="stdio", url=None)
        await grant_refs(
            _resolved(_entry(server, bound=False, awaiting_probe=True)),
            user_id=USER,
            snapshots=_snapshots(),
        )

        kick.assert_not_called()

