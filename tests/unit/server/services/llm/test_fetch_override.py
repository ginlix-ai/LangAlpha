"""
Tests for apply_fetch_override in src/server/services/llm/config.py: how a
resolved config hands its fetch model and client to WebFetch.
"""

from unittest.mock import MagicMock, patch

from ptc_agent.config import LLMConfig

CONFIG = "src.server.services.llm.config"


# ---------------------------------------------------------------------------
# apply_fetch_override
# ---------------------------------------------------------------------------


class TestApplyFetchOverride:
    def test_sets_context_vars(self):
        from src.server.services.llm.config import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="gpt-4o-mini")
        config.subsidiary_llm_clients = {"fetch": MagicMock()}

        with (
            patch(f"{CONFIG}.fetch_model_override") as mock_model_var,
            patch(f"{CONFIG}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once_with("gpt-4o-mini")
        mock_client_var.set.assert_called_once_with(
            config.subsidiary_llm_clients["fetch"]
        )

    def test_blank_fetch_defaults_to_background(self):
        from src.server.services.llm.config import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, background="gpt-4o-mini")
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{CONFIG}.fetch_model_override") as mock_model_var,
            patch(f"{CONFIG}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once_with("gpt-4o-mini")
        mock_client_var.set.assert_not_called()

    def test_skips_when_no_fetch_or_background(self):
        from src.server.services.llm.config import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, background=None)
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{CONFIG}.fetch_model_override") as mock_model_var,
            patch(f"{CONFIG}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_not_called()
        mock_client_var.set.assert_not_called()

    def test_skips_client_when_not_in_subsidiary(self):
        from src.server.services.llm.config import apply_fetch_override

        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch="gpt-4o-mini")
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{CONFIG}.fetch_model_override") as mock_model_var,
            patch(f"{CONFIG}.fetch_llm_client_override") as mock_client_var,
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
        from src.server.services.llm.config import apply_fetch_override
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

    def test_blank_fetch_forwards_background_role_client(self):
        """A blank fetch means the background model, and role_registry resolves a
        client for it. That client must reach web_fetch, or an OAuth user's
        extraction goes out on a server key it does not have.
        """
        fake_client = MagicMock(name="oauth-fetch-client")
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, background="claude-sonnet-4-6-oauth")
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-sonnet-4-6-oauth"
        assert snap["client"] is fake_client

    def test_no_fetch_or_background_leaves_both_vars_unset(self):
        config = MagicMock()
        config.llm = LLMConfig(name="main-model", fetch=None, background=None)
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
        from src.server.services.llm.config import apply_fetch_override
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
