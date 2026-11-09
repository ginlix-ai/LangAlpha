"""A subagent's Claude Code pin on a thread of another model: it runs on the
user's Claude plan where the turn's workspace has Claude Code at turn start,
else on the turn's model, and never on the setup token the user stored."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.llms.claude_code import ClaudeCodeTarget
from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.claude_code.machine import answer_key
from src.server.services.llm.claude_code import Admitted, claude_code_placement
from tests.unit.server.services.claude_code.fakes import Cache, serving
from tests.unit.server.services.llm.test_claude_code_placement import HOSTED, OWN, _answer
from tests.unit.server.services.llm.test_claude_code_routing import _base

CODE = "claude-haiku-5-5-code"
HANDLER = "src.server.services.llm.config"


async def _place(computer, *, opted_in=True, backend=True, saved="sk-ant-oat01-saved", kept=None):
    cache = Cache()
    cache.values.update(kept or {})
    config = SimpleNamespace(feature_enabled=lambda _: opted_in)
    with (
        serving(cache),
        patch("src.server.services.llm.claude_code.rows_available", return_value=True),
        patch("src.server.services.llm.claude_code.live_cloud_turns", new_callable=AsyncMock, return_value=99),
        patch("src.server.services.claude_code.hosted.backend_available", return_value=backend),
        patch("src.server.database.computer.get_computer_for_workspace", new_callable=AsyncMock, return_value=computer),
        patch("src.server.services.claude_code.saved.stored_row", new_callable=AsyncMock,
              return_value={"access_token": saved} if saved else None),
    ):
        return await claude_code_placement(config, user_id="user-1", workspace_id="ws-1")


@pytest.mark.asyncio
async def test_a_workspace_runs_claude_code_where_admission_would_let_a_turn_there():
    """Read as admission reads it, but answered rather than raised, and with
    no regard to the user's count of cloud turns, which counts turns only."""
    assert await _place(OWN, backend=False, saved=None) == Admitted()
    assert await _place(HOSTED) == Admitted("saved_token", "saved_token")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("computer", "kw"),
    [
        (HOSTED, {"backend": False}),
        (HOSTED, {"saved": None}),
        (OWN, {"kept": {answer_key("c-1"): _answer("refused")}}),
        (OWN, {"opted_in": False}),
    ],
)
async def test_a_workspace_where_a_claude_code_turn_would_be_refused_runs_none(computer, kw):
    assert await _place(computer, **kw) is None


@pytest.mark.asyncio
async def test_a_placement_that_cannot_be_read_runs_the_pin_on_the_turns_model():
    """The turn itself never fails for a pin's Claude Code state."""
    with patch("src.server.services.llm.claude_code.cli_placement", new_callable=AsyncMock, side_effect=OSError("db down")):
        assert await _place(HOSTED) is None


def _custom(name: str, model: str) -> dict:
    return {"kind": "custom", "name": name, "fields": {"model": model}, "instructions": "Do the work.",
            "roles": None, "workspace_id": None}


async def _resolve(monkeypatch, pref: dict, rows: list[dict], *, placed: Admitted | None, signed_in=()):
    """A turn on ``pref``'s model, the user signed in to each model in
    ``signed_in`` with their own account."""
    from src.llms.llm import create_llm
    from src.server.database import user_subagents
    from src.server.services.llm.config import resolve_llm_config

    async def _rows(user_id, *, conn=None):
        return rows

    async def _own(user_id, model_name, *args, **kwargs):
        return create_llm(model_name) if model_name in signed_in else None

    monkeypatch.setattr(user_subagents, "list_user_subagents", _rows)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    placement = AsyncMock(return_value=placed)
    with (
        patch("src.server.services.llm.user_models.get_model_preference", new_callable=AsyncMock, return_value=pref),
        patch("src.server.services.llm.clients.resolve_oauth_llm_client", new=_own),
        patch(f"{HANDLER}.effective_flags_for_user", new_callable=AsyncMock, return_value={"claude_code_runtime": True}),
        patch(f"{HANDLER}.require_runnable", new_callable=AsyncMock, return_value=Admitted()),
        patch(f"{HANDLER}.claude_code_placement", new=placement),
    ):
        config = await resolve_llm_config(_base(), "user-1", None, False, workspace_id="ws-1")
    return config, placement


@pytest.mark.asyncio
async def test_a_claude_code_pin_on_a_thread_of_another_model_keeps_its_pin_where_the_workspace_has_claude_code(
    monkeypatch,
):
    config, placement = await _resolve(
        monkeypatch, {}, [_custom("desk", CODE)], placed=Admitted("saved_token", "saved_token"),
    )
    assert placement.await_args.kwargs == {"user_id": "user-1", "workspace_id": "ws-1"}
    assert config.claude_code_pins == Admitted("saved_token", "saved_token") and not config.claude_code
    assert config.subagents.resolved.definitions["desk"].model == CODE
    assert isinstance(config.subsidiary_llm_clients["subagent:desk"], ClaudeCodeTarget)
    assert config.subagent_models["desk"]["id"] == CODE


@pytest.mark.asyncio
async def test_a_claude_code_pin_runs_the_threads_model_where_the_workspace_has_no_claude_code(monkeypatch):
    config, _ = await _resolve(monkeypatch, {}, [_custom("desk", CODE)], placed=None)
    assert config.claude_code_pins is None
    assert config.subagents.resolved.definitions["desk"].model is None
    assert "subagent:desk" not in config.subsidiary_llm_clients and "desk" not in config.subagent_models


@pytest.mark.asyncio
async def test_a_pin_of_another_model_on_a_claude_code_thread_runs_as_before(monkeypatch):
    """On the user's own account for it, as before. Nor is where Claude Code
    runs asked again for a Claude Code turn, or asked at all for a user with
    no Claude Code pin."""
    config, placement = await _resolve(
        monkeypatch, {"preferred_model": "claude-sonnet-5-5-code"}, [_custom("desk", "claude-haiku-5-5")], placed=None,
        signed_in={"claude-haiku-5-5"},
    )
    assert config.claude_code and config.subagents.resolved.definitions["desk"].model == "claude-haiku-5-5"
    assert config.subagent_models["desk"]["id"] == "claude-haiku-5-5"
    _, never = await _resolve(monkeypatch, {}, [_custom("desk", "claude-haiku-5-5")], placed=None)
    placement.assert_not_awaited()
    never.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("own", "token", "placed"),
    [
        pytest.param(False, "stored_token", None, id="a-cloud-computer-on-the-stored-token"),
        pytest.param(True, "stored_token", None, id="the-stored-token-relayed-to-an-own-computer"),
        pytest.param(False, "saved_token", Admitted("saved_token", "saved_token"), id="the-servers-saved-token"),
        pytest.param(True, None, Admitted(), id="an-own-computers-own-token"),
    ],
)
async def test_a_pin_never_signs_in_with_the_stored_token(own, token, placed):
    """Nothing on a turn of another model tells whether the user started the
    work it carries on, so a pin runs on the plan only where a token the
    user's own computer holds, or the server's, signs it in."""
    from src.server.services.claude_code.hosted import Hosted, OnCloud, OnOwn

    where = OnOwn(OWN) if own else OnCloud(Hosted(runs_on=token))
    with (
        patch("src.server.services.llm.claude_code.rows_available", return_value=True),
        patch("src.server.services.llm.claude_code.cli_placement", new_callable=AsyncMock, return_value=where),
        patch("src.server.services.llm.claude_code.read_answer", new_callable=AsyncMock, return_value=None),
        patch("src.server.services.llm.claude_code.require_computer_signed_in", new_callable=AsyncMock,
              return_value=token),
    ):
        assert await claude_code_placement(
            SimpleNamespace(feature_enabled=lambda _: True), user_id="user-1", workspace_id="ws-1",
        ) == placed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "roles", "pinned"),
    [
        pytest.param("claude-sonnet-5-5-code", None, True, id="a-claude-code-pin"),
        pytest.param("gpt-6.1-terra", None, False, id="changed-to-another-model"),
        pytest.param(None, None, False, id="removed"),
        pytest.param("claude-sonnet-5-5-code", ["chief_of_staff"], False, id="moved-off-the-analyst"),
    ],
)
async def test_a_subagent_counts_as_pinned_only_by_the_pin_it_has_now(model, roles, pinned):
    """A steer waiting on a task asks what its subagent is pinned to now,
    as a turn's merge would read it, not what its last run ran on."""
    from src.server.services.llm.subagents import pinned_to_claude_code
    from tests.unit.ptc_agent.agent.test_subagent_merge import OFFERED, _bases

    rows = [{**_custom("macro-desk", model), "roles": roles}] if model else []
    reads = AsyncMock(return_value=(rows, _bases(), OFFERED))
    with patch("src.server.services.llm.subagents.read_user_subagents", new=reads):
        assert await pinned_to_claude_code("user-1", "macro-desk", "analyst") is pinned


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", ["ws-1", get_home_workspace_id("user-1")], ids=["workspace", "home"])
async def test_a_pin_where_cloud_computers_run_none_runs_on_the_picked_own_computers_own_token(workspace):
    """Its CLI goes where a Claude Code turn's there would, the computer
    carried so its launcher, count and room follow it; the Chief of Staff's
    pins too, since it runs Claude Code."""
    from tests.unit.server.services.claude_code.test_hosted import STUDIO, platform

    config = SimpleNamespace(feature_enabled=lambda _: True)
    with platform(mode="local", cli=(STUDIO, None)):
        placed = await claude_code_placement(config, user_id="user-1", workspace_id=workspace)
    with platform(mode="local", cli=(None, "Studio")):
        offline = await claude_code_placement(config, user_id="user-1", workspace_id=workspace)
    assert (placed, offline) == (Admitted(cli_computer="c-9"), None)
