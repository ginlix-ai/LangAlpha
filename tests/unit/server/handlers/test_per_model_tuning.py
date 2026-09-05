"""Per-model tuning profiles beat the account-wide value, keyed on the model that runs.

This is the behaviour the ``model_preference`` column exists for. Before it, the
three tuning call sites in ``resolve_llm_config`` read the account-wide value
directly, so one compaction threshold was applied to a 200k-context model and a
1M-context one alike. The fix threads ``effective_model`` through ``_tuning``;
these tests pin that thread, because nothing else does.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, COMPACTION_PROFILES, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)

HANDLER = "src.server.services.llm.config"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"

BIG = "big-context-model"
SMALL = "small-context-model"


def _make_config(**llm_overrides) -> AgentConfig:
    llm_defaults = {"name": "system-default-model", "flash": "system-flash-model"}
    llm_defaults.update(llm_overrides)
    return AgentConfig(
        llm=LLMConfig(**llm_defaults),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


def _mock_model_config():
    known = {"system-default-model", "system-flash-model", BIG, SMALL}
    mc = MagicMock()
    mc.get_model_config.side_effect = lambda name: {"provider": "openai"} if name in known else None
    mc.get_provider_info.return_value = {}
    mc.get_parent_provider.return_value = "openai"
    return mc


async def _resolve(model_pref, request_model=None, mode="ptc"):
    from src.server.services.llm.config import resolve_llm_config

    with (
        patch(
            f"{USER_MODELS}.get_model_preference",
            new_callable=AsyncMock,
            return_value=model_pref,
        ),
        patch(f"{CLIENTS}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch("src.llms.llm.LLM.get_model_config", return_value=_mock_model_config()),
    ):
        return await resolve_llm_config(
            _make_config(), "user-1", request_model, False, mode=mode
        )


@pytest.fixture
def two_profiles():
    """One account-wide setting, one model overriding it."""
    return {
        "compaction_profile": "relaxed",
        "profiles": {BIG: {"compaction_profile": "aggressive"}},
    }


class TestCompactionProfileIsPerModel:
    @pytest.mark.asyncio
    async def test_profiled_model_uses_its_own_threshold(self, two_profiles):
        config = await _resolve(two_profiles, request_model=BIG)
        assert config.compaction.token_threshold == COMPACTION_PROFILES["aggressive"]["token_threshold"]

    @pytest.mark.asyncio
    async def test_unprofiled_model_falls_through_to_the_account_value(self, two_profiles):
        config = await _resolve(two_profiles, request_model=SMALL)
        assert config.compaction.token_threshold == COMPACTION_PROFILES["relaxed"]["token_threshold"]

    @pytest.mark.asyncio
    async def test_one_preference_dict_yields_two_thresholds(self, two_profiles):
        """The regression itself: same account, same request, two models.

        Asserted as a difference rather than two values, so it keeps failing if
        the profile lookup silently stops discriminating.
        """
        big = await _resolve(two_profiles, request_model=BIG)
        small = await _resolve(two_profiles, request_model=SMALL)
        assert big.compaction.token_threshold != small.compaction.token_threshold

    @pytest.mark.asyncio
    async def test_whole_preset_is_applied_not_only_the_threshold(self, two_profiles):
        config = await _resolve(two_profiles, request_model=BIG)
        preset = COMPACTION_PROFILES["aggressive"]
        assert config.compaction.keep_messages == preset["keep_messages"]
        assert (
            config.compaction.truncate_args_trigger_messages
            == preset["truncate_args_trigger_messages"]
        )


class TestProfileIsKeyedOnTheModelThatRuns:
    @pytest.mark.asyncio
    async def test_preferred_model_selects_the_profile(self):
        """No per-request model: the profile must key on the stored preference."""
        config = await _resolve(
            {
                "preferred_model": BIG,
                "compaction_profile": "relaxed",
                "profiles": {BIG: {"compaction_profile": "aggressive"}},
            }
        )
        assert config.compaction.token_threshold == COMPACTION_PROFILES["aggressive"]["token_threshold"]

    @pytest.mark.asyncio
    async def test_flash_mode_keys_on_the_flash_model(self):
        """A profile on the primary model must not tune a flash turn."""
        config = await _resolve(
            {
                "preferred_model": BIG,
                "preferred_flash_model": SMALL,
                "compaction_profile": "relaxed",
                "profiles": {BIG: {"compaction_profile": "aggressive"}},
            },
            mode="flash",
        )
        assert config.compaction.token_threshold == COMPACTION_PROFILES["relaxed"]["token_threshold"]

    @pytest.mark.asyncio
    async def test_flash_profile_applies_on_a_flash_turn(self):
        config = await _resolve(
            {
                "preferred_model": BIG,
                "preferred_flash_model": SMALL,
                "compaction_profile": "relaxed",
                "profiles": {SMALL: {"compaction_profile": "aggressive"}},
            },
            mode="flash",
        )
        assert config.compaction.token_threshold == COMPACTION_PROFILES["aggressive"]["token_threshold"]


class TestProfileShapesThatMustNotCrash:
    @pytest.mark.asyncio
    async def test_profile_for_another_model_is_ignored(self):
        config = await _resolve(
            {"compaction_profile": "relaxed", "profiles": {SMALL: {"compaction_profile": "aggressive"}}},
            request_model=BIG,
        )
        assert config.compaction.token_threshold == COMPACTION_PROFILES["relaxed"]["token_threshold"]

    @pytest.mark.asyncio
    async def test_unknown_profile_name_falls_through_to_yaml_defaults(self):
        untouched = _make_config().compaction.token_threshold
        config = await _resolve(
            {"profiles": {BIG: {"compaction_profile": "not-a-preset"}}}, request_model=BIG
        )
        assert config.compaction.token_threshold == untouched

    @pytest.mark.asyncio
    @pytest.mark.parametrize("profiles", [None, {}, "not-a-dict", {BIG: "not-a-dict"}])
    async def test_malformed_profiles_do_not_break_resolution(self, profiles):
        config = await _resolve({"compaction_profile": "aggressive", "profiles": profiles})
        assert config.compaction.token_threshold == COMPACTION_PROFILES["aggressive"]["token_threshold"]
