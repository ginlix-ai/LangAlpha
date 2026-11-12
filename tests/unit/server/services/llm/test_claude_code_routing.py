"""A Claude Code row resolves to its runtime target for a user who can run it,
is refused with a reason for anyone else, and is never a role, a fallback or a
Background model."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.config.agent import Admitted, AgentConfig, CredentialSource, LLMConfig
from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig
from src.llms.claude_code import ClaudeCodeTarget, ClaudeTurnFailed
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.saved import token_check_key
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF
from tests.unit.server.services.claude_code.fakes import Cache, serving
from src.server.services.llm.config import resolve_llm_config, saved_default_model

HANDLER = "src.server.services.llm.config"
CODE = "claude-sonnet-5-5-code"


def _base() -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(name="claude-sonnet-5-5", background="claude-haiku-5-5", fallback=["claude-haiku-5-5"]),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


class _Status(Cache):
    """The status check's kept answer for the one token: works, refused, or None when not checked yet."""

    def __init__(self, answer):
        super().__init__()
        self.answer = answer

    async def get(self, key):
        return self.answer


async def _resolve(
    pref: dict, *, runs: bool = True, opted_in: bool = True, role: str = "analyst", request_model=None,
    token: str | None = "sk-ant-oat01-tok", checked: str | None = "works", slot: str = "primary", **turn,
):
    cleanup = AsyncMock(return_value=[])
    with (
        serving(_Status(checked)),
        patch("src.server.services.claude_code.saved.stored_row", new_callable=AsyncMock, return_value={"access_token": token} if token else None),
        patch("src.server.services.llm.user_models.get_model_preference", new_callable=AsyncMock, return_value=pref),
        patch("src.server.services.llm.clients.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch(f"{HANDLER}.effective_flags_for_user", new_callable=AsyncMock, return_value={"claude_code_runtime": opted_in}),
        patch("src.server.services.llm.claude_code.rows_available", return_value=runs),
        patch(f"{HANDLER}.rows_available", return_value=runs),
        patch("src.server.services.llm.claude_code.live_cloud_turns", new_callable=AsyncMock, return_value=0),
        patch("src.server.services.claude_code.hosted.backend_available", return_value=runs),
        patch(f"{HANDLER}.cleanup_stale_model_preferences", new=cleanup),
    ):
        try:
            return await resolve_llm_config(_base(), "user-1", request_model, False, slot=slot, role=role, **turn)
        finally:
            cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("runs", "opted_in", "reason"), [(False, True, "server"), (True, False, "account")])
async def test_a_turn_that_cannot_run_claude_code_is_refused_and_nothing_saved_is_cleared(runs, opted_in, reason):
    with pytest.raises(HTTPException) as refused:
        await _resolve({"preferred_model": CODE}, runs=runs, opted_in=opted_in)
    assert refused.value.status_code == 400
    assert refused.value.detail == {"message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": CODE, "reason": reason}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "checked", "reason"),
    [(None, None, "signed_out"), ("sk-ant-oat01-tok", "refused", "token_refused"), ("sk-ant-oat01-tok", False, "token_refused")],
)
async def test_a_turn_on_a_token_known_not_to_work_is_refused_saying_whether_one_is_saved(token, checked, reason):
    with pytest.raises(HTTPException) as refused:
        await _resolve({"preferred_model": CODE}, token=token, checked=checked)
    assert refused.value.detail == {"message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": CODE, "reason": reason}


@pytest.mark.asyncio
async def test_a_token_a_turn_found_refused_is_refused_from_the_next_turn_on():
    cache = Cache()
    with (
        serving(cache),
        patch("src.server.services.claude_code.saved.mark_access_token_refused", new_callable=AsyncMock, return_value=True),
        patch("src.server.services.claude_code.hosted.backend_available", return_value=True),
        patch(
            "src.server.services.claude_code.launchers.stored_row", new_callable=AsyncMock,
            return_value={"access_token": "sk-ant-oat01-tok", "updated_at": None},
        ),
        pytest.raises(ClaudeTurnFailed),
    ):
        token = HostedCredential("user-1", "saved_token", None)
        await token.read()
        await token.refused()
    assert cache.values == {token_check_key("sk-ant-oat01-tok"): False}
    with pytest.raises(HTTPException) as refused:
        await _resolve({"preferred_model": CODE}, checked=cache.values[token_check_key("sk-ant-oat01-tok")])
    assert refused.value.detail["reason"] == "token_refused"


@pytest.mark.asyncio
async def test_a_token_not_checked_yet_runs_without_waiting_on_the_check(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    config = await _resolve({"preferred_model": CODE}, checked=None)
    assert isinstance(config.llm_client, ClaudeCodeTarget)


@pytest.mark.asyncio
async def test_chief_of_staff_runs_claude_code_named_or_by_default_where_its_cli_has_somewhere_to_run(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    for named in (None, CODE):
        config = await _resolve({"preferred_model": CODE}, role="chief_of_staff", request_model=named)
        assert isinstance(config.llm_client, ClaudeCodeTarget)


@pytest.mark.asyncio
async def test_chief_of_staff_runs_the_deployment_model_beside_a_claude_code_default_with_nowhere_to_run_it(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    with patch(f"{HANDLER}.chief_of_staff_default_runs", new_callable=AsyncMock, return_value=None) as placed:
        config = await _resolve({"preferred_model": CODE}, role="chief_of_staff")
    placed.assert_awaited_once_with("user-1", None)
    assert config.llm.name == "claude-sonnet-5-5" and not isinstance(config.llm_client, ClaudeCodeTarget)


@pytest.mark.asyncio
@pytest.mark.parametrize("named", [CODE, "claude-haiku-5-5"], ids=["claude-code", "another-model"])
async def test_chief_of_staff_naming_a_model_never_reads_where_its_claude_code_default_would_run(monkeypatch, named):
    """The default is the saved choice of a turn that names none; one that
    names its model runs that model, and admission reads the placement."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    with patch(f"{HANDLER}.chief_of_staff_default_runs", new_callable=AsyncMock, return_value=None) as placed:
        config = await _resolve({"preferred_model": CODE}, role="chief_of_staff", request_model=named)
    placed.assert_not_awaited()
    assert config.llm.name == named

@pytest.mark.asyncio
@pytest.mark.parametrize(("runs", "opted_in"), [(False, True), (True, False)], ids=["server", "account"])
async def test_chief_of_staff_runs_the_deployment_model_beside_a_claude_code_default_while_claude_code_is_off(
    monkeypatch, runs, opted_in,
):
    """Not refused: a turn naming no model never chose that default."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    config = await _resolve({"preferred_model": CODE}, runs=runs, opted_in=opted_in, role="chief_of_staff")
    assert config.llm.name == "claude-sonnet-5-5" and not isinstance(config.llm_client, ClaudeCodeTarget)


@pytest.mark.asyncio
async def test_a_named_claude_code_model_is_refused_the_same_way():
    with pytest.raises(HTTPException) as refused:
        await _resolve({}, opted_in=False, request_model=CODE)
    assert refused.value.detail["type"] == "claude_code_off"


@pytest.mark.asyncio
async def test_an_opted_in_turn_resolves_to_the_runtime_target_on_the_users_own_credential(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")  # a fallback could be built, and is not
    config = await _resolve({"preferred_model": CODE, "fallback_models": ["claude-haiku-5-5"]})
    assert isinstance(config.llm_client, ClaudeCodeTarget) and config.llm_client.model == "claude-sonnet-5-5"
    assert config.credential_source == CredentialSource.OAUTH and config.claude_code_admitted == Admitted("saved_token", "saved_token")
    assert config.fallback_llm_clients == [] and config.fallback_llm_names == []
    assert not any(isinstance(c, ClaudeCodeTarget) for c in config.subsidiary_llm_clients.values())
    assert config.llm.background == "claude-haiku-5-5", "Background follows the deployment, not a Claude Code primary"


@pytest.mark.asyncio
async def test_a_claude_code_row_saved_as_a_role_or_fallback_reads_as_unset(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")  # the deployment's key builds the fallback that stays
    config = await _resolve({
        "preferred_model": "claude-sonnet-5-5",
        "compaction_model": "claude-opus-5-5-code",
        "fetch_model": "claude-haiku-5-5-code",
        "preferred_background_model": CODE,
        "fallback_models": ["claude-haiku-5-5-code", "claude-haiku-5-5"],
    })
    # Each reads as if never saved: compaction runs the turn's model, fetch the
    # Background model, and Background follows the primary.
    assert (config.llm.compaction, config.llm.fetch, config.llm.background) == ("claude-sonnet-5-5", None, "claude-sonnet-5-5")
    assert config.fallback_llm_names == ["claude-haiku-5-5"]


@pytest.mark.asyncio
async def test_a_claude_code_row_saved_as_the_background_default_reads_as_unset():
    """A Background job is one chat model call, which such a row does not make."""
    config = await _resolve({"preferred_background_model": CODE}, slot="background")
    assert config.llm.background == "claude-haiku-5-5"


def test_background_follows_the_primary_unless_the_primary_is_claude_code():
    assert saved_default_model({"preferred_model": "claude-sonnet-5-5"}, "background") == "claude-sonnet-5-5"
    assert saved_default_model({"preferred_model": CODE}, "background") is None


@pytest.mark.asyncio
async def test_a_subagent_can_be_pinned_to_a_claude_code_row_with_no_key_of_its_own():
    """Each turn runs it where its workspace has Claude Code, on the setup
    token kept there, so the pane neither refuses it nor marks it unavailable."""
    from src.server.services.subagents.context import SubagentContext

    ctx = SubagentContext(
        user_id="user-1", bases={}, offered=(), model_pref={}, is_byok=False, permits=None,
        search_provider="", search_depth="", skills=frozenset(), connectors=(),
        brokerages_connected=False, oauth_accounts={},
    )
    assert ctx.model_known("claude-sonnet-5-5") and ctx.model_known(CODE)
    assert await ctx.model_has_key(CODE)
