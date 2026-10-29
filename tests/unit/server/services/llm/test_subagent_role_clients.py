"""A subagent's model and effort as an LLM role: which model at which level, and on whose key, each turn builds for it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import ResolvedSubagents
from ptc_agent.config.agent import CredentialSource
from src.server.services.llm import clients
from src.server.services.llm.clients import LLMRole, ResolvedClient
from src.server.services.llm.subagents import subagent_roles
from src.server.services.llm.user_models import ModelSource

MAIN = "main-model"
OWN_KEY = (CredentialSource.BYOK, CredentialSource.OAUTH)
NO_OWN_KEY = (CredentialSource.PLATFORM, CredentialSource.NONE)


# ---------------------------------------------------------------------------
# The role list
# ---------------------------------------------------------------------------


# Both models carry a saved level of their own, which a set effort must beat
# and an unset one must not reach past.
_PREF = {"profiles": {"sub-model": {"reasoning_effort": "low"}, MAIN: {"reasoning_effort": "max"}}}


@pytest.mark.parametrize(
    ("model", "effort", "turn_model", "expected"),
    [
        # A set effort is the subagent's, and beats the model's saved one.
        ("sub-model", "high", MAIN, ("sub-model", "high", True)),
        # Unset: the pinned model at its own saved level.
        ("sub-model", None, MAIN, ("sub-model", "low", False)),
        # Inherit model + set effort: the turn's model at the set effort.
        (None, "high", MAIN, (MAIN, "high", True)),
        # Inherit both: no role of its own, so the subagent shares the turn's client.
        (None, None, MAIN, None),
        (None, "high", None, None),
    ],
)
def test_a_subagent_gets_a_role_for_its_model_or_its_effort(model, effort, turn_model, expected):
    defn = SubagentDefinition(name="desk", description="", model=model, reasoning_effort=effort)

    roster = ResolvedSubagents(definitions={"desk": defn}, enabled=("desk",))

    roles = [
        (r.key, r.model, r.reasoning_effort, r.effort_follows, r.own_client)
        for r in subagent_roles(roster, _PREF, turn_model)
    ]

    assert roles == ([("subagent:desk", *expected, True)] if expected else [])


@pytest.mark.parametrize(("role", "expected"), [("analyst", ["both", "desk"]), ("chief_of_staff", ["both", "home"])])
def test_only_the_turns_agent_gets_roles_for_its_subagents(role, expected):
    """A subagent the turn's agent never compiles gets no client built for it."""
    defns = {
        name: SubagentDefinition(name=name, description="", model="sub-model", roles=frozenset(roles))
        for name, roles in {
            "both": {"analyst", "chief_of_staff"},
            "desk": {"analyst"},
            "home": {"chief_of_staff"},
        }.items()
    }
    roster = ResolvedSubagents(definitions=defns, enabled=tuple(defns))

    keys = [r.key for r in subagent_roles(roster, _PREF, MAIN, role=role)]

    assert keys == [f"subagent:{name}" for name in expected]


# ---------------------------------------------------------------------------
# The client each role gets
# ---------------------------------------------------------------------------


class _Client:
    def __init__(self, model: str, effort: str | None, credential: CredentialSource):
        self.model, self.effort, self.credential = model, effort, credential
        self.copied_from = None

    def model_copy(self):
        copy = _Client(self.model, self.effort, self.credential)
        copy.copied_from = self
        return copy


@pytest.fixture
def ladder(monkeypatch):
    """``resolve_model_client`` over a fixed table of which models the user
    holds a key for. The platform builds a client only when it is allowed to
    and the model is not in ``no_platform``."""
    state = SimpleNamespace(own={}, no_platform=set(), calls=[])

    async def _resolve(user_id, model_name, *, is_byok, cache_key=None, reasoning_effort=None,
                       service_tier=None, allow_platform_fallback=False, _pref_cache=None,
                       _byok_cache=None):
        state.calls.append((model_name, reasoning_effort, allow_platform_fallback))
        credential = state.own.get(model_name)
        if credential is None and allow_platform_fallback and model_name not in state.no_platform:
            credential = CredentialSource.PLATFORM
        if credential is None:
            return ResolvedClient(None, ModelSource.SYSTEM, CredentialSource.NONE)
        return ResolvedClient(
            _Client(model_name, reasoning_effort, credential), ModelSource.SYSTEM, credential,
        )

    monkeypatch.setattr(clients, "resolve_model_client", _resolve)
    monkeypatch.setattr(clients, "_stamp", lambda *_a, **_k: None)
    monkeypatch.setattr(clients.user_models, "guidance_for", lambda _pref, model: f"guidance:{model}")
    return state


def _turn(credential: CredentialSource) -> SimpleNamespace:
    main = _Client(MAIN, None, credential) if credential in OWN_KEY else None
    return SimpleNamespace(
        credential_source=credential, llm_client=main, prompt_guidance=f"guidance:{MAIN}",
        subsidiary_llm_clients={}, role_prompt_guidance={},
    )


async def _resolve_roles(config, *roles: LLMRole) -> None:
    await clients._resolve_role_clients(
        config, "user-1", list(roles), {}, {}, is_byok=True, cache_key="t-1", main_model=MAIN,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", OWN_KEY)
@pytest.mark.parametrize(
    ("set_effort", "expected_effort"),
    [("high", "high"), (None, None)],
)
async def test_an_own_key_turn_runs_a_keyless_subagent_on_the_main_model_never_the_platform(
    ladder, credential, set_effort, expected_effort
):
    """The user's own key paid for the turn, so a subagent pinned to a model
    they hold no key for runs the main model on that key: at the set effort
    when there is one, else as a copy of the main client."""
    ladder.own = {MAIN: credential}
    config = _turn(credential)
    role = LLMRole("subagent:desk", "sub-model", reasoning_effort=set_effort or "low",
                   effort_follows=set_effort is not None, own_client=True)

    await _resolve_roles(config, role)

    client = config.subsidiary_llm_clients["subagent:desk"]
    assert (client.model, client.effort, client.credential) == (MAIN, expected_effort, credential)
    assert (client.copied_from is config.llm_client) is (set_effort is None)
    assert not any(platform for _, _, platform in ladder.calls)
    assert config.role_prompt_guidance["subagent:desk"] == f"guidance:{MAIN}"


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", OWN_KEY)
async def test_inherit_with_a_set_effort_is_the_turns_model_at_that_effort(ladder, credential):
    ladder.own = {MAIN: credential}
    config = _turn(credential)

    await _resolve_roles(
        config,
        LLMRole("subagent:desk", MAIN, reasoning_effort="high", effort_follows=True,
                own_client=True),
    )

    client = config.subsidiary_llm_clients["subagent:desk"]
    assert (client.model, client.effort, client.credential) == (MAIN, "high", credential)
    assert client.copied_from is None


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", NO_OWN_KEY)
async def test_every_subagent_role_gets_its_own_client_for_platform_and_keyless_users(
    ladder, credential
):
    """A subagent's bare model name never reaches the manifest's providers, so
    it is built here even untuned; an untuned non-subagent role keeps the
    cheap name path."""
    config = _turn(credential)

    await _resolve_roles(
        config,
        LLMRole("fetch", "fetch-model"),
        LLMRole("subagent:plain", "sub-model", own_client=True),
        LLMRole("subagent:tuned", "sub-model", reasoning_effort="high", effort_follows=True,
                own_client=True),
    )

    assert set(config.subsidiary_llm_clients) == {"subagent:plain", "subagent:tuned"}
    built = {
        key: (c.model, c.effort, c.credential) for key, c in config.subsidiary_llm_clients.items()
    }
    assert built == {
        "subagent:plain": ("sub-model", None, CredentialSource.PLATFORM),
        "subagent:tuned": ("sub-model", "high", CredentialSource.PLATFORM),
    }
    assert ("fetch-model", None, False) in ladder.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", NO_OWN_KEY)
async def test_a_set_effort_follows_the_subagent_onto_the_main_model(ladder, credential):
    """Its pinned model got no client at all, so the set effort is kept on the
    model the turn runs rather than dropped."""
    ladder.no_platform = {"sub-model"}
    config = _turn(credential)

    await _resolve_roles(
        config,
        LLMRole("subagent:desk", "sub-model", reasoning_effort="high", effort_follows=True,
                own_client=True),
        LLMRole("subagent:untuned", "sub-model", own_client=True),
    )

    client = config.subsidiary_llm_clients["subagent:desk"]
    assert (client.model, client.effort) == (MAIN, "high")
    # No set effort and no own key: no copy of a main client to hand it.
    assert "subagent:untuned" not in config.subsidiary_llm_clients


# ---------------------------------------------------------------------------
# The model each subagent runs, as its runs record it
# ---------------------------------------------------------------------------


_ENTRIES = {
    MAIN: {"model_id": "main-v1", "provider": "deepseek"},
    "main-model-oauth": {"model_id": "main-v1", "provider": "claude-oauth"},
    "sub-model": {"model_id": "sub-v1", "provider": "z-ai-cn"},
    "my-proxy-model": {"model_id": "sub-v1", "provider": "my-proxy"},
}


def _client(manifest_model: str) -> SimpleNamespace:
    return SimpleNamespace(metadata={"manifest_model": manifest_model})


@pytest.fixture
def entries(monkeypatch):
    monkeypatch.setattr(
        "src.server.services.llm.subagents.user_models.model_entry",
        lambda _pref, name: _ENTRIES.get(name),
    )


@pytest.mark.usefixtures("entries")
@pytest.mark.parametrize(
    ("pin", "client", "expected"),
    [
        # Its own client on another model: news.
        ("sub-model", _client("sub-model"), {"id": "sub-model", "provider": "z-ai-cn"}),
        # A pin with no key of its own runs a copy of the main client.
        ("sub-model", _client(MAIN), None),
        # An effort alone is the turn's model at that effort.
        (None, _client(MAIN), None),
        # The turn's model on another route is still the model the user picked.
        ("main-model-oauth", _client("main-model-oauth"), None),
        # No client: the compiler hands the pin over by name.
        ("sub-model", None, {"id": "sub-model", "provider": "z-ai-cn"}),
        # Inherit: the main agent's own model.
        (None, None, None),
    ],
)
def test_a_subagent_records_its_model_only_when_it_is_not_the_turns(pin, client, expected):
    from src.server.services.llm.subagents import subagent_models

    defn = SubagentDefinition(name="desk", description="", model=pin)
    config = SimpleNamespace(
        subagents=SimpleNamespace(
            resolved=ResolvedSubagents(definitions={"desk": defn}, enabled=("desk",))
        ),
        subsidiary_llm_clients={"subagent:desk": client} if client else {},
    )

    models = subagent_models(config, {}, MAIN)

    assert models == ({"desk": expected} if expected else {})


@pytest.mark.usefixtures("entries")
def test_a_users_own_provider_records_the_catalog_provider_it_is_a_variant_of():
    from src.server.services.llm.subagents import subagent_models

    defn = SubagentDefinition(name="desk", description="", model="my-proxy-model")
    config = SimpleNamespace(
        subagents=SimpleNamespace(
            resolved=ResolvedSubagents(definitions={"desk": defn}, enabled=("desk",))
        ),
        subsidiary_llm_clients={"subagent:desk": _client("my-proxy-model")},
    )
    pref = {"custom_providers": [{"name": "my-proxy", "parent_provider": "openai"}]}

    assert subagent_models(config, pref, MAIN) == {
        "desk": {"id": "my-proxy-model", "provider": "openai"}
    }
