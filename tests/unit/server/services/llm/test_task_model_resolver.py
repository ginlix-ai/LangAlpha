"""A model a Task names is gated as a turn's own model is, in order, and
refused rather than replaced when it cannot run."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from ptc_agent.config.agent import Admitted, CredentialSource, TaskModel, TaskModelRefused
from src.server.services.llm import task_model as tm
from src.server.services.llm import user_models
from src.server.services.llm.clients import ResolvedClient

ModelSource = user_models.ModelSource

_ENTRIES = {
    "turn-model": {"provider": "acme", "model_id": "turn-model"},
    "other-model": {"provider": "acme", "display_name": "Other Model", "model_id": "other-model"},
    "turn-twin": {"provider": "acme-plan", "model_id": "turn-model"},
    "pinned-model": {"provider": "acme", "model_id": "pinned-model"},
    "cc-row": {"provider": "claude-code", "model_id": "cc-row"},
    "own-model": {"provider": "acme", "model_id": "own-model"},
}


class _Client(SimpleNamespace):
    metadata: dict | None = None


@pytest.fixture
def world(monkeypatch):
    """A catalog of models, every gate open, and a record of the client
    built."""
    state = SimpleNamespace(
        on_plan=set(_ENTRIES),
        tier=1,
        placement=Admitted(hosted="saved_token"),
        placement_reads=0,
        refreshes=0,
        client_calls=[],
        client=lambda name: _Client(metadata={"manifest_model": name}, model=name),
        raises=None,
        source=ModelSource.SYSTEM,
        definitions={
            "research": SimpleNamespace(model=None, reasoning_effort=None),
            "desk": SimpleNamespace(model="pinned-model", reasoning_effort="minimal"),
        },
    )

    async def classify(_user, name, _pref_cache=None):
        entry = _ENTRIES.get(name)
        return (state.source, entry) if entry else (ModelSource.UNKNOWN, {})

    async def on_plan(_user, name, **_):
        return name in state.on_plan

    async def placement(_config, *, user_id, workspace_id):
        state.placement_reads += 1
        return state.placement

    async def resolve_client(user_id, name, **kwargs):
        state.client_calls.append((name, kwargs))
        if state.raises is not None:
            raise state.raises
        client = state.client(name)
        return ResolvedClient(
            client, state.source,
            CredentialSource.PLATFORM if client is not None else CredentialSource.NONE,
        )

    monkeypatch.setattr(user_models, "classify_model", classify)
    monkeypatch.setattr(user_models, "model_entry", lambda _pref, name: _ENTRIES.get(name))
    monkeypatch.setattr(user_models, "guidance_for", lambda _pref, name: f"guidance-for-{name}")
    monkeypatch.setattr(tm, "model_resolves", lambda _pref, name: name in _ENTRIES)
    monkeypatch.setattr("src.server.services.llm.availability.model_on_plan", on_plan)
    monkeypatch.setattr(tm, "cli_admission", placement)
    monkeypatch.setattr(tm, "is_claude_code_model", lambda name: name == "cc-row")
    monkeypatch.setattr(tm, "resolve_model_client", resolve_client)
    monkeypatch.setattr(
        tm, "turn_roster", lambda _subagents: SimpleNamespace(definitions=state.definitions)
    )
    monkeypatch.setattr(
        tm.LLMFactory, "get_model_config",
        classmethod(lambda _cls: SimpleNamespace(get_display_name=lambda p: p.upper())),
    )
    return state


class _Permits:
    def __init__(self, state):
        self.state = state

    async def refresh(self):
        self.state.refreshes += 1

    async def tier(self):
        return self.state.tier


def _config(**overrides):
    return SimpleNamespace(**{
        "credential_source": CredentialSource.PLATFORM,
        "compaction": SimpleNamespace(profile="standard"),
        "claude_code": False,
        "claude_code_admitted": None,
        "claude_code_pins": None,
        "subsidiary_llm_clients": {},
        "subagents": None,
        **overrides,
    })


def _gate(world, *, config=None, **overrides):
    kwargs = {
        "user_id": "u-1",
        "workspace_id": "w-1",
        "thread_id": "t-1",
        "model_pref": {"reasoning_effort": "low", "profiles": {"other-model": {"reasoning_effort": "high"}}},
        "permits": _Permits(world),
        "is_byok": False,
        "turn_model": "turn-model",
        "plan_exempt": frozenset(),
        **overrides,
    }
    return tm.TaskModelGate(config or _config(), **kwargs)


async def _resolve(world, name, subagent_type="research", **kwargs):
    return await _gate(world, **kwargs).resolve(name, subagent_type)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["no-such-model", ""])
async def test_a_name_the_catalog_lacks_is_refused_as_unknown(world, name):
    refused = await _resolve(world, name)

    assert isinstance(refused, TaskModelRefused)
    assert refused.message == (
        f'Error: there is no model named "{name}". Run `la models list` for the names '
        "you can pass, or leave model out to use the subagent's own."
    )
    assert world.client_calls == []


@pytest.mark.asyncio
async def test_a_model_off_plan_is_refused_after_the_plan_is_read_again(world):
    world.on_plan.discard("other-model")

    refused = await _resolve(world, "other-model")

    assert refused.message == (
        "Error: other-model is not on the user's plan. Pick another from "
        "`la models list`, or leave model out."
    )
    assert world.refreshes == 1
    assert world.client_calls == []


@pytest.mark.asyncio
async def test_an_unreadable_tier_is_not_read_as_off_plan(world):
    """A quota-service outage leaves no tier to read, which is not the plan's answer."""
    world.on_plan.discard("other-model")
    world.tier = None

    refused = await _resolve(world, "other-model")

    assert refused.message == (
        "Error: other-model could not be checked just now. Try again, or leave model out."
    )


@pytest.mark.asyncio
async def test_the_deployments_own_model_passes_whatever_the_plan(world):
    world.on_plan.clear()

    resolved = await _resolve(world, "other-model", plan_exempt=frozenset({"other-model"}))

    assert isinstance(resolved, TaskModel)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "credential"),
    [
        (ModelSource.CUSTOM, CredentialSource.PLATFORM),
        (ModelSource.SYSTEM, CredentialSource.BYOK),
    ],
)
async def test_a_model_with_no_client_needs_the_users_key_and_is_never_replaced(
    world, source, credential
):
    """A custom model, and any model on a turn the user's own key pays for."""
    world.client = lambda _name: None
    world.source = source

    refused = await _resolve(world, "other-model", config=_config(credential_source=credential))

    assert refused.message == (
        "Error: other-model needs the user's own API key for ACME, which they have not "
        "added. Pick another from `la models list`, or leave model out."
    )


@pytest.mark.asyncio
async def test_a_catalog_model_the_server_holds_no_key_for_says_so(world):
    world.client = lambda _name: None

    refused = await _resolve(world, "other-model")

    assert refused.message == (
        "Error: other-model cannot run now: this server has no API key for ACME. "
        "Pick another from `la models list`, or leave model out."
    )
    assert refused.reason == "this server has no API key for ACME"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "platform_ok"),
    [
        (CredentialSource.BYOK, False),
        (CredentialSource.OAUTH, False),
        (CredentialSource.PLATFORM, True),
        (CredentialSource.NONE, True),
    ],
)
async def test_an_own_key_turn_never_moves_a_task_onto_the_platform_key(world, source, platform_ok):
    await _resolve(world, "other-model", config=_config(credential_source=source))

    ((_, kwargs),) = world.client_calls
    assert kwargs["allow_platform_fallback"] is platform_ok


@pytest.mark.asyncio
async def test_the_task_runs_at_the_models_own_saved_effort(world):
    await _resolve(world, "other-model")

    ((name, kwargs),) = world.client_calls
    assert (name, kwargs["reasoning_effort"], kwargs["cache_key"]) == ("other-model", "high", "t-1")


@pytest.mark.asyncio
async def test_a_subagents_own_set_effort_applies_on_the_model_its_task_names(world):
    """Its set effort is the user's choice for that subagent, whatever serves it."""
    gate = _gate(world)

    await gate.resolve("other-model", "desk")
    await gate.resolve("other-model", "research")

    assert [kwargs["reasoning_effort"] for _, kwargs in world.client_calls] == ["minimal", "high"]


@pytest.mark.asyncio
async def test_a_resolved_model_is_kept_for_the_turn_by_type_and_name(world):
    gate = _gate(world)

    first = await gate.resolve("other-model", "research")
    again = await gate.resolve("other-model", "research")

    assert first is again
    assert len(world.client_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("subagent_type", "name", "clients"),
    [
        ("research", "turn-model", {}),
        ("desk", "pinned-model", {}),
        ("desk", "own-model", {"subagent:desk": _Client(metadata={"manifest_model": "own-model"})}),
    ],
)
async def test_a_subagents_own_model_needs_no_model_of_its_own(world, subagent_type, name, clients):
    """Its own model is the turn's, its pin, or what its client was built on."""
    resolved = await _resolve(
        world, name, subagent_type, config=_config(subsidiary_llm_clients=clients),
    )

    assert resolved is None
    assert world.client_calls == []


@pytest.mark.asyncio
async def test_a_model_kept_from_the_last_run_resolves_though_it_is_the_subagents_own(world):
    resolved = await _gate(world).resolve("turn-model", "research", keep=True)

    assert resolved.name == "turn-model"


@pytest.mark.asyncio
async def test_a_resolved_model_is_stamped_and_named_for_its_card(world):
    resolved = await _resolve(world, "other-model")

    assert isinstance(resolved, TaskModel)
    assert (resolved.name, resolved.display_name) == ("other-model", "Other Model")
    assert resolved.guidance == "guidance-for-other-model"
    assert resolved.model == {"id": "other-model", "provider": "acme"}
    assert resolved.claude_code is None
    assert resolved.client.metadata["prompt_guidance"] == "guidance-for-other-model"
    assert resolved.client.metadata["compaction_profile"] == "standard"


@pytest.mark.asyncio
async def test_the_turns_own_model_by_another_name_shows_no_model_on_its_card(world):
    resolved = await _resolve(world, "turn-twin", "desk")

    assert resolved.model is None


@pytest.mark.asyncio
async def test_claude_code_is_refused_where_it_does_not_run_in_admissions_words(world):
    world.placement = "Add a Claude Code setup token in Settings under Model access to use this model."

    refused = await _resolve(world, "cc-row")

    assert refused.message == (
        "Error: cc-row cannot run here: Add a Claude Code setup token in Settings under "
        "Model access to use this model. Leave model out, or pick a model that is not "
        "Claude Code."
    )
    assert refused.reason == (
        "Add a Claude Code setup token in Settings under Model access to use this model"
    )
    assert world.client_calls == []


@pytest.mark.asyncio
async def test_a_custom_model_shadowing_a_claude_code_row_runs_as_the_custom_model(world):
    world.placement = "Add a Claude Code setup token in Settings under Model access to use this model."
    world.source = ModelSource.CUSTOM

    resolved = await _resolve(world, "cc-row")

    assert resolved.claude_code is None
    assert not resolved.on_claude_code
    assert world.placement_reads == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("claude_code_turn", [True, False])
async def test_claude_code_runs_where_the_turn_placed_it_without_reading_again(world, claude_code_turn):
    at = Admitted(cli_computer="c-2")
    config = _config(
        claude_code=claude_code_turn,
        claude_code_admitted=at if claude_code_turn else None,
        claude_code_pins=None if claude_code_turn else at,
    )

    resolved = await _resolve(world, "cc-row", config=config)

    assert resolved.claude_code == at
    assert resolved.on_claude_code
    assert world.placement_reads == 0


@pytest.mark.asyncio
async def test_claude_code_placement_is_read_once_per_turn(world):
    world.placement = "Claude Code is off for your account."
    gate = _gate(world)

    await gate.resolve("cc-row", "research")
    await gate.resolve("cc-row", "research")

    assert world.placement_reads == 1


@pytest.mark.asyncio
async def test_a_placement_that_could_not_be_read_is_read_again(world):
    world.placement = tm.PLACEMENT_UNREAD
    gate = _gate(world)

    await gate.resolve("cc-row", "research")
    world.placement = Admitted(hosted="saved_token")
    resolved = await gate.resolve("cc-row", "research")

    assert world.placement_reads == 2
    assert resolved.claude_code == Admitted(hosted="saved_token")


@pytest.mark.asyncio
async def test_a_sign_in_that_needs_renewing_is_refused_by_the_account_name(world, monkeypatch):
    world.raises = HTTPException(400, detail={"type": "oauth_reauth_required", "message": "x"})
    monkeypatch.setattr(
        tm, "subscription_route",
        lambda *_a, **_k: SimpleNamespace(subscription=SimpleNamespace(account_name="Acme Plan")),
    )

    refused = await _resolve(world, "other-model")

    assert refused.message == (
        "Error: other-model runs on the user's Acme Plan sign-in, which needs signing in "
        "again. Ask the user to sign in again in Settings, or leave model out."
    )
    assert refused.reason == "the user's Acme Plan sign-in needs signing in again"
