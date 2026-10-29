"""One user's subagents for a turn: no rows is the deployment's roster, the request narrows it, and a pin the turn cannot serve runs as Inherit."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.subagents import create_subagents
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import ResolvedSubagents, narrow_to_request, turn_roster
from ptc_agent.config.agent import AgentConfig, LLMConfig, SubagentConfig, SubagentsConfig
from src.server.services.llm import subagents as turn_subagents
from src.server.services.llm.subagents import (
    model_on_plan,
    demote_clientless,
    read_user_subagents,
    resolve_user_subagents,
)
from src.server.services.llm.user_models import ModelSource

USER = "user-1"


def _deployment() -> SubagentsConfig:
    return SubagentsConfig(
        enabled=["research", "data-prep", "macro"],
        definitions={
            "data-prep": SubagentConfig(description="Data desk", tools=["execute_code", "filesystem"]),
            "macro": SubagentConfig(description="Macro desk", model="m-deploy"),
        },
    )


def _override(name: str, key: str, **fields) -> dict:
    return {"kind": "override", "name": name, "base_key": key, "fields": fields,
            "instructions": "", "roles": None, "workspace_id": None}


def _custom(name: str, **fields) -> dict:
    return {"kind": "custom", "name": name, "fields": fields, "instructions": "Do the work.",
            "roles": None, "workspace_id": None}


def _plugin_row(name: str, *, plugin: str = "acme", on: bool = True, **fields) -> dict:
    """A ``plugin_subagents`` row joined to its plugin, as the reader returns it."""
    return {
        "plugin_name": plugin, "name": name, "plugin_enabled": on,
        "fields": {"description": "Package desk", "tools": ["web"], **fields},
        "instructions": "Package text.", "roles": None,
    }


async def _deny(_tier: int) -> bool:
    return False


@pytest.fixture
def rows(monkeypatch) -> list[dict]:
    """The user's ``user_subagents`` rows the next resolve reads."""
    from src.server.database import user_subagents

    stored: list[dict] = []

    async def _list(user_id, *, conn=None):
        return [dict(r) for r in stored]

    monkeypatch.setattr(user_subagents, "list_user_subagents", _list)
    return stored


@pytest.fixture
def plugin_rows(monkeypatch) -> list[dict]:
    """The user's installed plugins' subagents the next resolve reads."""
    from src.server.database import plugin_subagents

    stored: list[dict] = []

    async def _list(user_id, *, conn=None):
        return [dict(r) for r in stored]

    monkeypatch.setattr(plugin_subagents, "list_user_plugin_subagents", _list)
    return stored


@pytest.fixture(autouse=True)
def _no_bundled_agents(monkeypatch):
    """Keep these rosters to what each test writes, whatever bundles ship later."""
    from src.server.services.plugins import bundled

    monkeypatch.setattr(bundled, "bundled_agent_plans", lambda: [])


@pytest.fixture(autouse=True)
def unknown(monkeypatch) -> set[str]:
    """Model names the manifest lacks; every other name resolves."""
    names: set[str] = set()
    monkeypatch.setattr(turn_subagents, "model_resolves", lambda _pref, name: name not in names)
    return names


@pytest.fixture
def on_plan(monkeypatch) -> AsyncMock:
    """The plan check, refusing every model unless a test says otherwise."""
    check = AsyncMock(return_value=False)
    monkeypatch.setattr(turn_subagents, "model_on_plan", check)
    return check


async def _resolve(deployment: SubagentsConfig | None = None, *, requested=None) -> ResolvedSubagents:
    reads = await read_user_subagents(USER, deployment or _deployment())
    return await resolve_user_subagents(
        USER, reads, model_pref={}, permits=_deny, is_byok=False,
        search_api="tavily", search_depth="standard", requested=requested,
    )


# ---------------------------------------------------------------------------
# The no-row identity and the request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", [None, [], ["macro", "research"]])
async def test_a_user_with_no_rows_compiles_what_the_deployment_alone_would(
    rows, plugin_rows, on_plan, requested
):
    """The feature must be invisible until a user touches it: same names, same
    order, same definitions, for either role, and the deployment's own pin is
    never put to the plan check."""
    deployment = _deployment()

    resolved = await _resolve(deployment, requested=requested)

    per_turn = deployment.model_copy(update={"resolved": resolved})
    as_built = SimpleNamespace(compile=lambda defn: defn)
    for role in ("analyst", "chief_of_staff"):
        alone, _ = narrow_to_request(turn_roster(deployment), requested)
        before = create_subagents(alone, as_built, role)
        after = create_subagents(turn_roster(per_turn), as_built, role)
        assert after == before
    assert (resolved.orphaned, resolved.shadowed) == ((), ())
    on_plan.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested", "enabled"),
    [
        (None, ("data-prep", "macro", "macro-desk")),
        ([], ("data-prep", "macro", "macro-desk")),
        # A name the user switched off stays off even when a stored request names it.
        (["research", "macro-desk", "data-prep"], ("macro-desk", "data-prep")),
    ],
)
async def test_the_roster_is_the_users_enabled_list_narrowed_by_the_request(
    rows, plugin_rows, requested, enabled
):
    rows += [_override("research", "builtin:research", enabled=False), _custom("macro-desk")]

    assert (await _resolve(requested=requested)).enabled == enabled


@pytest.mark.asyncio
async def test_a_user_who_turns_every_subagent_off_gets_none(rows, plugin_rows):
    rows += [
        _override("research", "builtin:research", enabled=False),
        _override("data-prep", "builtin:data-prep", enabled=False),
        _override("macro", "deployment:macro", enabled=False),
    ]
    deployment = _deployment()

    resolved = await _resolve(deployment)

    assert resolved.enabled == ()
    assert turn_roster(deployment.model_copy(update={"resolved": resolved})).enabled == ()


@pytest.mark.asyncio
async def test_a_failed_read_of_the_rows_fails_the_turn(monkeypatch, plugin_rows):
    """An off-switch is a restriction; resolving without it would run a
    subagent the user turned off."""
    from src.server.database import user_subagents

    monkeypatch.setattr(
        user_subagents, "list_user_subagents", AsyncMock(side_effect=RuntimeError("pool down"))
    )

    with pytest.raises(RuntimeError):
        await _resolve()


@pytest.mark.asyncio
async def test_a_plugins_subagent_joins_under_its_plugin_key_while_the_plugin_is_on(
    rows, plugin_rows
):
    plugin_rows += [_plugin_row("desk"), _plugin_row("quiet", plugin="zeta", on=False)]
    rows += [
        _override("desk", "plugin:acme/desk", effort="high"),
        _override("quiet", "plugin:zeta/quiet", effort="high"),
    ]

    resolved = await _resolve()

    assert resolved.enabled == ("research", "data-prep", "macro", "desk")
    assert resolved.definitions["desk"].reasoning_effort == "high"
    assert "quiet" not in resolved.definitions


@contextlib.contextmanager
def _offline_turn():
    """Every credential and catalog read ``resolve_llm_config`` makes, answered
    with a system model and no key."""
    with contextlib.ExitStack() as stack:
        for target, value in [
            ("src.server.services.llm.user_models.classify_model", (ModelSource.SYSTEM, {})),
            ("src.server.services.llm.user_models.get_custom_provider_config", None),
            ("src.server.services.llm.user_models.get_model_preference", {}),
            ("src.server.services.llm.clients.resolve_oauth_llm_client", None),
            ("src.server.services.llm.clients.resolve_byok_llm_client", None),
            ("src.server.database.api_keys.get_byok_configs_for_providers", {}),
        ]:
            stack.enter_context(patch(target, AsyncMock(return_value=value)))
        yield


def _agent_config() -> AgentConfig:
    cfg = AgentConfig.create(llm=MagicMock(name="placeholder-llm"))
    cfg.llm = LLMConfig(name="main-model")
    return cfg


@pytest.mark.asyncio
@pytest.mark.parametrize(("slot", "user_facing"), [("background", True), ("primary", False)])
async def test_a_build_that_compiles_no_subagents_never_reads_them(
    monkeypatch, plugin_rows, slot, user_facing
):
    from src.server.database import user_subagents
    from src.server.services.llm.config import resolve_llm_config

    reads = AsyncMock(return_value=[])
    monkeypatch.setattr(user_subagents, "list_user_subagents", reads)

    with _offline_turn():
        config = await resolve_llm_config(
            _agent_config(), USER, None, False, slot=slot, user_facing=user_facing,
        )

    reads.assert_not_awaited()
    assert config.subagents.resolved is None


# ---------------------------------------------------------------------------
# Pass 1: name and plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "rows_in", "plugins_in", "model"),
    [
        ("macro", [_override("macro", "deployment:macro", model="m-user")], [], "m-user"),
        ("research", [_override("research", "builtin:research", model="m-user")], [], "m-user"),
        ("macro-desk", [_custom("macro-desk", model="m-user")], [], "m-user"),
        # The user installed the plugin, so its pin is gated as theirs.
        ("desk", [], [_plugin_row("desk", model="m-pkg")], "m-pkg"),
    ],
)
@pytest.mark.parametrize("permitted", [True, False])
async def test_a_model_the_user_chose_runs_only_if_their_plan_reaches_it(
    rows, plugin_rows, on_plan, name, rows_in, plugins_in, model, permitted
):
    rows += rows_in
    plugin_rows += plugins_in
    on_plan.return_value = permitted

    resolved = await _resolve()

    # Only the user's choice is checked; the deployment's own m-deploy never is.
    assert [c.args[1] for c in on_plan.await_args_list] == [model]
    assert resolved.definitions[name].model == (model if permitted else None)


@pytest.mark.asyncio
async def test_a_pin_the_manifest_lacks_runs_as_inherit_whoever_set_it(
    rows, plugin_rows, on_plan, unknown
):
    """A name the manifest lacks fails the build, so this check covers the
    deployment's own pins too, and short-circuits the plan check."""
    unknown |= {"m-deploy", "m-gone"}
    rows += [_custom("macro-desk", model="m-gone")]

    resolved = await _resolve()

    assert resolved.definitions["macro"].model is None
    assert resolved.definitions["macro-desk"].model is None
    on_plan.assert_not_awaited()


def _system(tier=None, provider="p-1") -> tuple[str, dict]:
    entry = {"provider": provider} if provider else {}
    if tier is not None:
        entry["tier"] = tier
    return ModelSource.SYSTEM, entry


async def _plan_one(tier: int) -> bool:
    return tier <= 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host_mode", "classified", "is_byok", "has_key", "access_type", "expected"),
    [
        pytest.param("oss", _system(tier=3), False, False, None, True, id="oss-never-gated"),
        pytest.param("platform", (ModelSource.CUSTOM, {}), False, False, None, True,
                     id="custom-model-left-to-the-client-pass"),
        pytest.param("platform", _system(tier=1), False, False, None, True, id="on-plan"),
        pytest.param("platform", _system(tier=3), False, False, None, False, id="above-plan"),
        pytest.param("platform", _system(tier=3), True, True, None, True, id="above-plan-own-key"),
        pytest.param("platform", _system(tier=3), True, False, None, False,
                     id="above-plan-byok-but-no-key-for-it"),
        pytest.param("platform", _system(tier=3), False, False, "oauth", True,
                     id="above-plan-subscription-provider"),
        pytest.param("platform", _system(), False, False, None, False, id="no-tier-no-key"),
        pytest.param("platform", _system(), True, True, None, True, id="no-tier-own-key"),
        pytest.param("platform", _system(tier=3, provider=None), True, True, None, False,
                     id="no-provider"),
    ],
)
async def test_the_plan_check_exempts_a_model_the_user_brings_a_key_or_subscription_for(
    monkeypatch, host_mode, classified, is_byok, has_key, access_type, expected
):
    monkeypatch.setattr("src.config.settings.HOST_MODE", host_mode)
    monkeypatch.setattr("src.server.dependencies.usage_limits.platform_gating_active", lambda: host_mode == "platform")
    mc = MagicMock()
    mc.get_provider_info.return_value = {"access_type": access_type} if access_type else {}
    mc.get_model_config.return_value = None  # no built-in under the custom model's name
    walk = AsyncMock(return_value=({"key": "k"} if has_key else None, None))

    with (
        patch("src.server.services.llm.user_models.classify_model",
              AsyncMock(return_value=classified)),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch("src.server.services.llm.clients.walk_byok_candidates", walk),
    ):
        allowed = await model_on_plan(
            USER, "m-1", model_pref={}, permits=_plan_one, is_byok=is_byok, byok_cache={},
        )

    assert allowed is expected


# ---------------------------------------------------------------------------
# Pass 2: after the clients are built
# ---------------------------------------------------------------------------


def _defn(name: str, model: str | None) -> SubagentDefinition:
    return SubagentDefinition(name=name, description="", model=model, source="custom")


def test_a_pin_that_got_no_client_runs_as_inherit():
    """Guidance stamped from the pinned model goes with it, since the
    subagent now runs the turn's."""
    resolved = ResolvedSubagents(
        definitions={
            "own": _defn("own", "m-own"),
            "keyless": _defn("keyless", "m-keyless"),
            "inherit": _defn("inherit", None),
            "off": _defn("off", "m-off"),
        },
        enabled=("own", "keyless", "inherit"),
    )
    config = SimpleNamespace(
        subagents=SubagentsConfig(resolved=resolved),
        subsidiary_llm_clients={"subagent:own": object(), "subagent:inherit": object()},
        role_prompt_guidance={f"subagent:{n}": "g" for n in ("own", "keyless", "inherit", "off")},
    )

    demote_clientless(config)

    after = config.subagents.resolved
    assert {n: d.model for n, d in after.definitions.items()} == {
        "own": "m-own", "keyless": None, "inherit": None, "off": "m-off",
    }
    assert set(config.role_prompt_guidance) == {"subagent:own", "subagent:inherit", "subagent:off"}


@pytest.mark.asyncio
async def test_a_turn_reads_its_rows_and_its_plugin_bases_from_one_snapshot(monkeypatch):
    # Uninstall drops a plugin's subagents and the user's overrides of them in
    # one transaction; reads on two connections could straddle it and run a
    # subagent the user switched off.
    from src.server.database import plugin_subagents, user_subagents

    snapshot = object()
    seen: dict[str, object] = {}

    @contextlib.asynccontextmanager
    async def one_snapshot():
        yield snapshot

    def reader(table: str):
        async def _list(user_id, *, conn=None):
            seen[table] = conn
            return []
        return _list

    monkeypatch.setattr(plugin_subagents, "subagents_snapshot", one_snapshot)
    monkeypatch.setattr(user_subagents, "list_user_subagents", reader("user_subagents"))
    monkeypatch.setattr(plugin_subagents, "list_user_plugin_subagents", reader("plugin_subagents"))
    await read_user_subagents(USER, _deployment())
    assert seen == {"user_subagents": snapshot, "plugin_subagents": snapshot}
