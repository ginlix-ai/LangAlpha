"""
Tests for src/server/handlers/chat/request_prep.py: what a turn's run is wired
with.

Covers:
- init_tracking: returns (TokenTrackingManager, ToolUsageTracker)
- apply_fetch_override: sets the fetch context vars, mocked and real
- build_graph_config: mode parameterization, optional fields
- setup_steering_tracking: wires callback on handler
"""

from unittest.mock import MagicMock, patch

import pytest

from ptc_agent.config import LLMConfig

PREP = "src.server.handlers.chat.request_prep"


# ---------------------------------------------------------------------------
# init_tracking
# ---------------------------------------------------------------------------


class TestInitTracking:
    def test_returns_tuple(self):
        from src.server.handlers.chat.request_prep import init_tracking

        with (
            patch(
                f"{PREP}.TokenTrackingManager.initialize_tracking",
                return_value=MagicMock(),
            ) as mock_token,
            patch(
                f"{PREP}.ToolUsageTracker",
                return_value=MagicMock(),
            ) as mock_tool,
        ):
            token_cb, tool_tr = init_tracking("thread-1")

        mock_token.assert_called_once_with(thread_id="thread-1", track_tokens=True)
        mock_tool.assert_called_once_with(thread_id="thread-1")
        assert token_cb is not None
        assert tool_tr is not None


# ---------------------------------------------------------------------------
# apply_fetch_override
# ---------------------------------------------------------------------------


class TestApplyFetchOverride:
    def test_sets_context_vars(self):
        from src.server.handlers.chat.request_prep import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="gpt-4o-mini")
        config.subsidiary_llm_clients = {"fetch": MagicMock()}

        with (
            patch(f"{PREP}.fetch_model_override") as mock_model_var,
            patch(f"{PREP}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once_with("gpt-4o-mini")
        mock_client_var.set.assert_called_once_with(
            config.subsidiary_llm_clients["fetch"]
        )

    def test_blank_fetch_defaults_to_flash(self):
        from src.server.handlers.chat.request_prep import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, flash="gpt-4o-mini")
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{PREP}.fetch_model_override") as mock_model_var,
            patch(f"{PREP}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once_with("gpt-4o-mini")
        mock_client_var.set.assert_not_called()

    def test_skips_when_no_fetch_or_flash(self):
        from src.server.handlers.chat.request_prep import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, flash=None)
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{PREP}.fetch_model_override") as mock_model_var,
            patch(f"{PREP}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_not_called()
        mock_client_var.set.assert_not_called()

    def test_skips_client_when_not_in_subsidiary(self):
        from src.server.handlers.chat.request_prep import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="gpt-4o-mini")
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{PREP}.fetch_model_override") as mock_model_var,
            patch(f"{PREP}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once()
        mock_client_var.set.assert_not_called()


# ---------------------------------------------------------------------------
# apply_fetch_override — real context-var contract (regression lock)
#
# These tests assert the actual ContextVar state transitions rather than mock
# call counts. Each case is isolated in its own copy_context().run() so no
# override leaks into other tests or the module-global vars.
# ---------------------------------------------------------------------------


class TestApplyFetchOverrideContextVars:
    """Contract tests using real ContextVars (no mocking of the vars themselves).

    Isolation: every case runs inside ``contextvars.copy_context().run(...)``
    so the process-global vars are untouched outside each sub-run.
    """

    def _run_and_capture(self, config):
        """Run apply_fetch_override in an isolated context; return snapshot."""
        import contextvars
        from src.server.handlers.chat.request_prep import apply_fetch_override
        from src.tools.web.fetch import fetch_model_override, fetch_llm_client_override

        results = {}

        def _inner():
            apply_fetch_override(config)
            results["model"] = fetch_model_override.get()
            results["client"] = fetch_llm_client_override.get()

        contextvars.copy_context().run(_inner)
        return results

    def test_credentialed_user_sets_both_vars(self):
        """Credentialed (BYOK/OAuth) path: subsidiary_llm_clients['fetch'] is
        pre-populated by resolve_llm_config — apply_fetch_override must forward
        it verbatim into fetch_llm_client_override (fetch.py copies at use time).
        """
        fake_client = MagicMock(name="byok-fetch-client")
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="claude-haiku-4-5")
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-haiku-4-5"
        assert snap["client"] is fake_client

    def test_platform_user_leaves_client_var_unset(self):
        """Platform/system path: no entry in subsidiary_llm_clients → the
        client context var must remain None so fetch.py uses LLM(model).get_llm().
        """
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="claude-haiku-4-5")
        config.subsidiary_llm_clients = {}  # platform user — nothing materialized

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-haiku-4-5"
        assert snap["client"] is None  # default — fetch.py takes the platform path

    def test_blank_fetch_forwards_flash_role_client(self):
        """A blank fetch means the flash model, and role_registry resolves a
        client for it. That client must reach web_fetch, or an OAuth user's
        extraction goes out on a server key it does not have.
        """
        fake_client = MagicMock(name="oauth-fetch-client")
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, flash="claude-sonnet-4-6-oauth")
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-sonnet-4-6-oauth"
        assert snap["client"] is fake_client

    def test_no_fetch_or_flash_leaves_both_vars_unset(self):
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, flash=None)
        config.subsidiary_llm_clients = {}

        snap = self._run_and_capture(config)

        assert snap["model"] is None   # ContextVar default
        assert snap["client"] is None  # ContextVar default

    def test_stored_client_is_not_copied_by_apply_fetch_override(self):
        """apply_fetch_override must store the raw shared instance (not a copy).
        fetch.py performs the .model_copy() at consumption time — this test
        guards against a double-copy regression.
        """
        fake_client = MagicMock(name="shared-client")
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="claude-haiku-4-5")
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        # Must be the exact same object — no copy performed here.
        assert snap["client"] is fake_client
        fake_client.model_copy.assert_not_called()

    def test_context_isolation_across_cases(self):
        """Override set in one isolated run must not bleed into the next run."""
        import contextvars
        from src.server.handlers.chat.request_prep import apply_fetch_override
        from src.tools.web.fetch import fetch_llm_client_override

        leak_sentinel = MagicMock(name="leaked-client")

        config_with = MagicMock()
        config_with.llm = LLMConfig(name="main-model", fetch="some-model")
        config_with.subsidiary_llm_clients = {"fetch": leak_sentinel}

        # First run sets the client in its own context copy.
        def _first():
            apply_fetch_override(config_with)
            assert fetch_llm_client_override.get() is leak_sentinel

        contextvars.copy_context().run(_first)

        # Process-global var must still be None.
        assert fetch_llm_client_override.get() is None


# ---------------------------------------------------------------------------
# build_graph_config
# ---------------------------------------------------------------------------


class TestBuildGraphConfig:
    def _build(self, **kwargs):
        from src.server.handlers.chat.request_prep import build_graph_config

        defaults = dict(
            thread_id="t-1",
            user_id="u-1",
            workspace_id="ws-1",
            mode="flash",
            timezone_str="America/New_York",
            token_callback=MagicMock(),
            request=MagicMock(
                locale="en-US",
                checkpoint_id=None,
                reasoning_effort=None,
                fast_mode=None,
                platform=None,
            ),
            effective_model="gpt-4o",
            recursion_limit=100,
        )
        defaults.update(kwargs)
        with (
            patch(f"{PREP}.get_langsmith_tags", return_value=["tag1"]),
            patch(f"{PREP}.get_langsmith_metadata", return_value={"k": "v"}),
        ):
            return build_graph_config(**defaults)

    def test_basic_flash_config(self):
        config = self._build(mode="flash", recursion_limit=500)
        assert config["configurable"]["agent_mode"] == "flash"
        assert config["configurable"]["thread_id"] == "t-1"
        assert config["recursion_limit"] == 500

    def test_ptc_config_with_plan_mode(self):
        config = self._build(mode="ptc", plan_mode=True, recursion_limit=2000)
        assert config["configurable"]["agent_mode"] == "ptc"
        assert config["recursion_limit"] == 2000

    def test_checkpoint_id_added(self):
        request = MagicMock(
            locale="en-US",
            checkpoint_id="cp-123",
            reasoning_effort=None,
            fast_mode=None,
            platform=None,
        )
        config = self._build(request=request)
        assert config["configurable"]["checkpoint_id"] == "cp-123"

    def test_no_checkpoint_id(self):
        config = self._build()
        assert "checkpoint_id" not in config["configurable"]

    def test_token_callback_in_callbacks(self):
        cb = MagicMock()
        config = self._build(token_callback=cb)
        assert config["callbacks"] == [cb]

    def test_no_callbacks_when_none(self):
        config = self._build(token_callback=None)
        assert "callbacks" not in config

    def test_extra_configurable_merged(self):
        config = self._build(extra_configurable={"plan_mode": True})
        assert config["configurable"]["plan_mode"] is True

    def test_graph_metadata_stays_turn_identity(self):
        """A subagent inherits this dict wholesale while running its own model at
        its own effort, so anything the LLM owns is asserted for calls it was
        never true of. Those keys live on the client (see ``LLM.get_llm``)."""
        from src.config.settings import get_langsmith_metadata
        from src.server.handlers.chat.request_prep import build_graph_config

        with (
            patch(f"{PREP}.get_langsmith_tags", return_value=[]),
            patch(f"{PREP}.get_langsmith_metadata", side_effect=get_langsmith_metadata),
        ):
            config = build_graph_config(
                thread_id="t-1",
                user_id="u-1",
                workspace_id="ws-1",
                mode="ptc",
                timezone_str="UTC",
                token_callback=None,
                request=MagicMock(
                    locale=None, checkpoint_id=None, reasoning_effort="high",
                    fast_mode=True, platform=None,
                ),
                effective_model="claude-sonnet-5",
                recursion_limit=100,
            )

        metadata = config["metadata"]
        assert metadata["user_id"] == "u-1"
        # The turn's own selection, which is a different question from what any
        # one call hit; cost and latency charts group on it.
        assert metadata["llm_model"] == "claude-sonnet-5"
        for llm_owned in (
            "reasoning_effort",
            "prompt_guidance",
            "compaction_profile",
            "fast_mode",
            "is_byok",
        ):
            assert llm_owned not in metadata

    def test_timezone_in_configurable(self):
        config = self._build(timezone_str="UTC")
        assert config["configurable"]["timezone"] == "UTC"

    def test_skill_contexts_and_dirs_threaded_to_configurable(self):
        """The server→middleware handoff: both skill_contexts and skill_dirs must
        land in ``configurable`` so SkillsMiddleware can inject + locate bodies."""
        config = self._build(
            skill_contexts=[{"name": "chart-annotation", "instruction": "AAPL:1d"}],
            skill_dirs=["/skills"],
        )
        assert config["configurable"]["skill_contexts"] == [
            {"name": "chart-annotation", "instruction": "AAPL:1d"}
        ]
        assert config["configurable"]["skill_dirs"] == ["/skills"]

    def test_skill_contexts_without_dirs_omits_dirs(self):
        """skill_dirs is nested under the skill_contexts gate; contexts may be
        present while dirs default (middleware falls back to project_root/skills)."""
        config = self._build(
            skill_contexts=[{"name": "research"}], skill_dirs=None
        )
        assert config["configurable"]["skill_contexts"] == [{"name": "research"}]
        assert "skill_dirs" not in config["configurable"]

    def test_skill_dirs_without_contexts_is_dropped(self):
        """No skills requested → neither key is set, even if skill_dirs is passed
        (the ``if skill_contexts`` gate guards the whole block)."""
        config = self._build(skill_contexts=None, skill_dirs=["/skills"])
        assert "skill_contexts" not in config["configurable"]
        assert "skill_dirs" not in config["configurable"]


# ---------------------------------------------------------------------------
# setup_steering_tracking
# ---------------------------------------------------------------------------


class TestSetupSteeringTracking:
    @pytest.mark.asyncio
    async def test_wires_callback(self):
        from src.server.handlers.chat.request_prep import setup_steering_tracking

        handler = MagicMock()
        handler.injected_steerings = []
        handler.on_steering_delivered = None

        setup_steering_tracking(handler)

        assert handler.on_steering_delivered is not None

    @pytest.mark.asyncio
    async def test_callback_filters_empty_content(self):
        from src.server.handlers.chat.request_prep import setup_steering_tracking

        handler = MagicMock()
        handler.injected_steerings = []

        setup_steering_tracking(handler)

        # Call the wired callback
        callback = handler.on_steering_delivered
        await callback([
            {"content": "hello", "role": "user"},
            {"content": "", "role": "user"},
            {"role": "user"},  # no content key
            {"content": "world", "role": "user"},
        ])

        assert len(handler.injected_steerings) == 2
        assert handler.injected_steerings[0]["content"] == "hello"
        assert handler.injected_steerings[1]["content"] == "world"
