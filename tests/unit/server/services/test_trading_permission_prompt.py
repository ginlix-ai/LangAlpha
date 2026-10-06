"""What the agent is told about trading: the user's settings, never this turn's binding."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.runtime_context import (
    BaselineContextMiddleware,
    is_runtime_update_message,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.profile import (
    ProfileSnapshot,
    profile_diff_lines,
)
from ptc_agent.agent.prompts import get_loader
from ptc_agent.config.core import MCPConfig, MCPServerConfig
from src.server.database.egress_grants import GRANT_KIND_OAUTH_MCP, GrantRef
from src.server.services import trading_rule as trading_rule_module
from src.server.services.brokerage_capabilities import vendor_for_url
from src.server.services.egress import direct_tools, flash_binding
from src.server.services.egress.direct_tools import METADATA_KEY, direct_tools_for_turn
from src.server.services.mcp_config import (
    Origin,
    ResolvedMCP,
    ResolvedServer,
    State,
    resolve_mcp_config,
)
from src.server.services.tool_binding import inputs_from_row, resolve_plan
from src.server.services.trading_permission import (
    TRADING_AGREEMENT_VERSION,
    TradingPermission,
)
from src.server.services.trading_rule import (
    TRADING_SETTINGS_URL,
    TradingRule,
    trading_rule,
    with_trading_rule,
)

AUTONOMOUS = TradingPermission.AUTONOMOUS
STILL_ASKS = "still stop for the user's approval"
AUTONOMY = "**Full autonomy.**"
URLS = {
    "moomoo": "https://mcp.moomoo.com/mcp",
    "robinhood": "https://agent.robinhood.com/mcp/trading",
    "ibkr": "https://api.ibkr.com/v1/api/mcp-public",
}
# Consent that reaches each brokerage's live or staged order tools.
TRADES = {
    "moomoo": ("account", "trading", "paper_trading"),
    "robinhood": ("account", "trading"),
    "ibkr": ("account", "staged_orders"),
}


@pytest.fixture(autouse=True)
def _relay(monkeypatch):
    # This deployment binds direct tools; nothing here opens a relay session.
    for module in (trading_rule_module, direct_tools, flash_binding):
        monkeypatch.setattr(module, "EGRESS_RELAY_SECRET", "s" * 32)


def _row(server: str, level: TradingPermission, approval: dict) -> dict:
    """A catalog row as the read returns it, the user's level joined on."""
    return {
        "name": server,
        "transport": "http",
        "url": URLS[server],
        "order_approval": approval,
        "trading_level": level.value,
        "trading_agreement_version": TRADING_AGREEMENT_VERSION,
    }


def _plan(server, level, granted=None, **approval):
    row = _row(server, level, approval)
    granted = TRADES[server] if granted is None else granted
    return resolve_plan(vendor_for_url(row["url"]), granted, inputs_from_row(row))


def _config(name: str) -> MCPServerConfig:
    return MCPServerConfig(name=name, transport="http", url=URLS[name], source="user")


def _resolved(**plans) -> ResolvedMCP:
    """The workspace's resolve: one active catalog server per plan."""
    return ResolvedMCP(
        entries=tuple(
            ResolvedServer(
                config=_config(name),
                origin=Origin.USER,
                state=State.ACTIVE,
                binding_plan=plan,
            )
            for name, plan in plans.items()
        ),
        version=1,
    )


def _profile(resolved) -> dict:
    return with_trading_rule({"name": "Demo"}, trading_rule(resolved))


def _section(profile: dict) -> str:
    out = get_loader().render(
        "components/user_profile.md.j2",
        user_profile=profile,
        user_data_counts=None,
        sandbox_enabled=False,
    )
    return out.split("## Trading Permission", 1)[1]


def _diff(frozen: dict, current: dict) -> list[str]:
    """The ``profile_changed`` row a turn files for this move, if any."""
    return profile_diff_lines(
        ProfileSnapshot(user_profile=frozen), ProfileSnapshot(user_profile=current)
    )


# ---------------------------------------------------------------------------
# What the rule says
# ---------------------------------------------------------------------------


def test_a_brokerage_whose_switch_asks_is_named_under_a_level_that_does_not():
    profile = _profile(
        _resolved(
            moomoo=_plan("moomoo", AUTONOMOUS, live=True),
            robinhood=_plan("robinhood", AUTONOMOUS, live=False),
        )
    )
    assert profile["trading_permission"] == "autonomous"
    assert profile["trading_asks_on"] == ["moomoo"]
    assert f"Orders through moomoo {STILL_ASKS}" in _section(profile)


def test_nothing_is_named_when_every_brokerage_follows_the_level():
    profile = _profile(_resolved(moomoo=_plan("moomoo", TradingPermission.PLAN_FIRST)))
    assert profile["trading_permission"] == "plan_first"
    assert "trading_asks_on" not in profile
    assert STILL_ASKS not in _section(profile)


def test_nothing_is_named_at_a_level_that_asks_for_every_order():
    level = TradingPermission.APPROVE_EACH
    profile = _profile(_resolved(moomoo=_plan("moomoo", level, live=True)))
    assert profile["trading_permission"] == "approve_each"
    assert "trading_asks_on" not in profile


def test_a_paper_switch_that_asks_names_nothing():
    # Paper orders sit outside the permission, so their switch says nothing
    # about whether real money goes out unasked.
    profile = _profile(
        _resolved(moomoo=_plan("moomoo", AUTONOMOUS, live=False, paper=True))
    )
    assert profile["trading_permission"] == "autonomous"
    assert "trading_asks_on" not in profile


def test_paper_only_brokerages_produce_no_rule():
    resolved = _resolved(
        moomoo=_plan("moomoo", AUTONOMOUS, ("account", "paper_trading"), paper=True)
    )
    # Its paper order tools are bound directly; they spend nothing.
    assert "moomoo" in resolved.binding_plans_by_name
    assert trading_rule(resolved) is None
    assert "trading_permission" not in _profile(resolved)


def test_an_order_tool_the_relay_cannot_reach_produces_no_rule():
    row = {**_row("moomoo", AUTONOMOUS, {}), "transport": "sse"}
    plan = resolve_plan("moomoo", TRADES["moomoo"], inputs_from_row(row))
    assert trading_rule(_resolved(moomoo=plan)) is None


def test_a_deployment_that_binds_nothing_directly_has_no_rule(monkeypatch):
    monkeypatch.setattr(trading_rule_module, "EGRESS_RELAY_SECRET", "")
    assert trading_rule(_resolved(moomoo=_plan("moomoo", AUTONOMOUS))) is None


def test_several_brokerages_read_as_one_list_in_the_block_and_in_a_row():
    profile = _profile(
        _resolved(
            robinhood=_plan("robinhood", AUTONOMOUS, live=True),
            ibkr=_plan("ibkr", AUTONOMOUS, staged=True),
            moomoo=_plan("moomoo", AUTONOMOUS, live=True),
        )
    )
    line = f"Orders through ibkr, moomoo and robinhood {STILL_ASKS}"
    assert line in _section(profile)
    snapshot = ProfileSnapshot(user_profile=profile)
    assert line in snapshot.fields()["Trading permission"]


@pytest.mark.parametrize("level", list(TradingPermission))
def test_every_level_links_where_the_user_changes_it(level):
    # The agent hands this to the user, often outside the app, so it is the
    # absolute address rather than the path the web app links with.
    profile = _profile(_resolved(moomoo=_plan("moomoo", level)))
    place = f"Settings > Preferences > Trading permission ({TRADING_SETTINGS_URL})"
    assert TRADING_SETTINGS_URL.endswith("/settings?tab=preferences#trading-permission")
    assert place in _section(profile)
    assert place in ProfileSnapshot(user_profile=profile).fields()["Trading permission"]


# ---------------------------------------------------------------------------
# A setting the user changes moves the rule
# ---------------------------------------------------------------------------


def test_a_level_change_files_a_changed_rule():
    before = _profile(_resolved(moomoo=_plan("moomoo", TradingPermission.APPROVE_EACH)))
    after = _profile(_resolved(moomoo=_plan("moomoo", AUTONOMOUS)))
    [line] = _diff(before, after)
    assert line.startswith("Trading permission: ")
    assert AUTONOMY in line


def test_an_approval_switch_files_a_changed_rule():
    before = _profile(_resolved(moomoo=_plan("moomoo", AUTONOMOUS, live=False)))
    after = _profile(_resolved(moomoo=_plan("moomoo", AUTONOMOUS, live=True)))
    [line] = _diff(before, after)
    assert f"Orders through moomoo {STILL_ASKS}" in line.split("(the frozen block")[0]


def test_revoking_the_trading_capability_files_a_removed_rule():
    before = _profile(_resolved(moomoo=_plan("moomoo", AUTONOMOUS)))
    after = _profile(_resolved(moomoo=_plan("moomoo", AUTONOMOUS, ("account",))))
    [line] = _diff(before, after)
    assert line.startswith("Trading permission: no longer set")


# ---------------------------------------------------------------------------
# What the turn managed to bind does not
# ---------------------------------------------------------------------------


class _Snapshots:
    """Discovery found every tool each configured server's plan names."""

    def __init__(self, resolved: ResolvedMCP):
        self._plans = resolved.binding_plans_by_name

    def ok(self, server):
        plan = self._plans.get(server.name)
        return {"tools": [{"name": n} for n in sorted(plan.by_tool)]} if plan else None


async def _flash_turn(monkeypatch, resolved: ResolvedMCP, lost: str | None = None):
    """One Flash turn through the resolve, the grant sync and the binder, losing
    its tools the way ``lost`` says, with the profile composed as the runner
    composes it. The stored level is unreadable to anything that asks again."""
    refs = [
        GrantRef(GRANT_KIND_OAUTH_MCP, name, connection_id=f"conn-{name}")
        for name in resolved.binding_plans_by_name
    ]
    grants = {ref.key: f"grant-{ref.server_name}" for ref in refs}
    reads: list = []

    async def _resolve(*a, **kw):
        return resolved

    async def _snapshots(user_id):
        if lost == "binder raises":
            raise ConnectionError("discovery snapshots unreadable")
        return _Snapshots(resolved)

    async def _refs(*a, **kw):
        return refs

    async def _sync(**kw):
        if lost == "superseded":
            return None
        # A disconnect racing the sync leaves the server with no grant.
        return SimpleNamespace(grants={} if lost == "no grant" else grants)

    async def _unreadable(user_id, *a, **kw):
        reads.append(user_id)
        raise ConnectionError("database unavailable")

    monkeypatch.setattr(flash_binding, "resolve_mcp_config", _resolve)
    monkeypatch.setattr(flash_binding, "user_snapshots", _snapshots)
    monkeypatch.setattr(flash_binding, "grant_refs", _refs)
    monkeypatch.setattr(flash_binding, "sync_egress_grants", _sync)
    monkeypatch.setattr(
        "src.server.database.trading_permission.get_trading_permission_row",
        _unreadable,
    )
    if lost == "starved":
        # Every brokerage publishes more order tools than this.
        monkeypatch.setattr(direct_tools, "MAX_DIRECT_TOOLS", 1)

    mcp = await flash_binding.resolve_flash_mcp(object(), user_id="u", workspace_id="w")
    bound, _ = await direct_tools_for_turn(
        flash_binding.bind_flash_direct_tools(mcp, user_id="u", workspace_id="w"),
        user_id="u",
        workspace_id="w",
        thread_id="t",
        run_id="r",
        turn_index=0,
    )
    assert not reads
    return bound, with_trading_rule({"name": "Demo"}, trading_rule(mcp))


def _live_stamps(bound) -> dict[str, list[bool]]:
    """Per server, the approval each bound live order tool was stamped with."""
    stamps: dict[str, list[bool]] = {}
    for tool in bound.tools:
        stamp = tool.metadata[METADATA_KEY]
        if (stamp["order"] or {}).get("mode") == "live":
            stamps.setdefault(stamp["server"], []).append(stamp["approval"])
    return stamps


def _two_brokerages() -> ResolvedMCP:
    return _resolved(
        moomoo=_plan("moomoo", AUTONOMOUS, live=True),
        robinhood=_plan("robinhood", AUTONOMOUS),
    )


@pytest.mark.asyncio
async def test_the_rule_and_the_stamps_read_one_resolution(monkeypatch):
    bound, profile = await _flash_turn(monkeypatch, _two_brokerages())
    stamps = _live_stamps(bound)
    assert stamps["moomoo"] and all(stamps["moomoo"])
    assert stamps["robinhood"] and not any(stamps["robinhood"])
    assert profile["trading_permission"] == "autonomous"
    assert profile["trading_asks_on"] == ["moomoo"]


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["no grant", "superseded", "starved", "binder raises"])
async def test_a_turn_that_bound_no_order_tool_files_no_change(monkeypatch, lost):
    resolved = _two_brokerages()
    bound, frozen = await _flash_turn(monkeypatch, resolved)
    assert _live_stamps(bound)

    unbound, current = await _flash_turn(monkeypatch, resolved, lost=lost)
    assert not _live_stamps(unbound)
    assert current == frozen
    assert (
        ProfileSnapshot(user_profile=current).fields()["Trading permission"]
        == ProfileSnapshot(user_profile=frozen).fields()["Trading permission"]
    )
    assert _diff(frozen, current) == []


async def _resolve_with_connection(status: str) -> ResolvedMCP:
    """moomoo through the real resolver, its connection in ``status``."""
    row = _row("moomoo", AUTONOMOUS, {"live": True})
    connection = {
        "connection_id": "conn-moomoo",
        "server_name": "moomoo",
        "server_url": URLS["moomoo"],
        "status": status,
        "granted_capabilities": list(TRADES["moomoo"]),
    }
    db = "src.server.database"
    with (
        patch(
            f"{db}.mcp_servers.get_workspace_servers_and_version",
            new=AsyncMock(return_value=([], 1)),
        ),
        patch(
            f"{db}.mcp_servers.list_enabled_user_servers",
            new=AsyncMock(return_value=[row]),
        ),
        patch(
            f"{db}.mcp_oauth.list_connections",
            new=AsyncMock(return_value=[connection]),
        ),
        patch(
            f"{db}.mcp_tool_schemas.get_user_tool_schemas",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.mcp_config.account_disabled_builtins",
            new=AsyncMock(return_value=frozenset()),
        ),
    ):
        return await resolve_mcp_config(
            SimpleNamespace(mcp=MCPConfig(servers=[])), "u", "w"
        )


@pytest.mark.asyncio
async def test_a_connection_waiting_on_reauth_keeps_the_rule():
    """The resolve keeps consent for any connection that still claims its row;
    only one the user revoked stops counting."""
    connected = _profile(await _resolve_with_connection("connected"))
    assert connected["trading_asks_on"] == ["moomoo"]

    reauth = _profile(await _resolve_with_connection("needs_reauth"))
    assert _diff(connected, reauth) == []

    revoked = _profile(await _resolve_with_connection("revoked"))
    assert "trading_permission" not in revoked


@pytest.mark.asyncio
async def test_the_ptc_view_carries_the_rule_without_a_tool_snapshot():
    """No snapshot binds nothing directly, and the settings still say the rule.
    A view rebuilt from the session once this one is evicted keeps it."""
    from src.server.services.computer_manager import ComputerManager
    from src.server.services.computer_manager._types import WorkspaceToolView

    ComputerManager.reset_instance()
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider="daytona")
    config.filesystem = SimpleNamespace(working_directory="/home/workspace")
    session = MagicMock()
    resolved = _resolved(moomoo=_plan("moomoo", AUTONOMOUS, live=True))
    try:
        manager = ComputerManager.get_instance(config=config)
        with (
            patch(
                "src.server.database.mcp_tool_schemas.get_tool_schemas",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "src.server.database.mcp_tool_schemas.get_user_tool_schemas",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="S",
            ),
        ):
            view = await manager._install_session_composite(
                session, resolved, user_id="u", workspace_id="ws"
            )
    finally:
        ComputerManager.reset_instance()

    assert dict(view.direct_mcp_tools) == {}
    assert view.trading_rule == TradingRule(AUTONOMOUS, ("moomoo",))
    rebuilt = WorkspaceToolView.from_session("ws", session)
    assert rebuilt.trading_rule == view.trading_rule


# ---------------------------------------------------------------------------
# A profile read that did not answer does not hold the rule back
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("unanswered", ["profile", "counts"])
async def test_a_raised_level_reaches_the_model_when_a_profile_read_did_not_answer(
    unanswered,
):
    """The order tools follow the new level this turn, so the row stating it
    cannot wait on reads the rule does not come from."""

    def turn(level, unanswered=None):
        rule = trading_rule(_resolved(moomoo=_plan("moomoo", level)))
        profile = None if unanswered == "profile" else {"name": "Demo"}
        return BaselineContextMiddleware(
            user_profile=with_trading_rule(profile, rule),
            user_data_counts=None if unanswered == "counts" else {"portfolio_count": 2},
            guidance="lean",
        )

    state = await turn(TradingPermission.APPROVE_EACH).abefore_agent({}, None)
    update = await turn(AUTONOMOUS, unanswered).abefore_agent(state, None)

    rows = [
        runtime_update_from_message(m)
        for m in (update or {}).get("messages") or []
        if is_runtime_update_message(m)
    ]
    assert [row.kind for row in rows] == ["profile_changed"]
    # The rule alone moved: the rest of the profile is restated as last seen.
    [line] = rows[0].text.rsplit("\n\n", 1)[1].splitlines()
    assert line.startswith("Trading permission: ")
    assert AUTONOMY in line.split("(the frozen block")[0]
