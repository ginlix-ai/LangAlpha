"""
Integration tests for src/server/handlers/chat/ package structure.

Verifies:
- All public symbols importable from the package
- No circular import issues at package load time
- Internal module paths resolve correctly
- Caller sites (threads.py, automation_executor.py) can import
"""

import importlib
import sys

import pytest


class TestPackagePublicAPI:
    """All 6 public symbols must be importable from src.server.handlers.chat."""

    def test_import_astream_flash_workflow(self):
        from src.server.handlers.chat import astream_flash_workflow

        assert callable(astream_flash_workflow)

    def test_import_astream_ptc_workflow(self):
        from src.server.handlers.chat import astream_ptc_workflow

        assert callable(astream_ptc_workflow)

    def test_import_resolve_llm_config(self):
        from src.server.handlers.chat import resolve_llm_config

        assert callable(resolve_llm_config)

    def test_import_reconnect_to_workflow_stream(self):
        from src.server.handlers.chat import reconnect_to_workflow_stream

        assert callable(reconnect_to_workflow_stream)

    def test_import_stream_subagent_task_events(self):
        from src.server.handlers.chat import stream_subagent_task_events

        assert callable(stream_subagent_task_events)

    def test_import_steer_subagent(self):
        from src.server.handlers.chat import steer_subagent

        assert callable(steer_subagent)

    def test_all_matches_exports(self):
        import src.server.handlers.chat as pkg

        expected = {
            "astream_flash_workflow",
            "astream_ptc_workflow",
            "steer_subagent",
            "reconnect_to_workflow_stream",
            "resolve_llm_config",
            "stream_subagent_task_events",
        }
        assert set(pkg.__all__) == expected


class TestSubmoduleImports:
    """Internal modules can be imported directly without errors."""

    def test_import_common(self):
        import src.server.handlers.chat._common as mod

        assert hasattr(mod, "classify_error")
        assert hasattr(mod, "process_hitl_response")
        assert hasattr(mod, "normalize_request_messages")
        assert hasattr(mod, "init_tracking")
        assert hasattr(mod, "apply_fetch_override")
        assert hasattr(mod, "ensure_thread")
        assert hasattr(mod, "persist_or_skip_replay")
        assert hasattr(mod, "inject_skills")
        assert hasattr(mod, "build_graph_config")
        assert hasattr(mod, "wait_or_steer")
        assert hasattr(mod, "stream_live_events")
        assert hasattr(mod, "handle_workflow_error")
        assert hasattr(mod, "serialize_context_metadata")
        assert hasattr(mod, "setup_steering_tracking")

    def test_import_llm_config(self):
        import src.server.handlers.chat.llm_config as mod

        assert hasattr(mod, "resolve_llm_config")
        assert hasattr(mod, "resolve_byok_llm_client")
        assert hasattr(mod, "resolve_oauth_llm_client")

    def test_import_steering(self):
        import src.server.handlers.chat.steering as mod

        assert hasattr(mod, "steer_thread")
        assert hasattr(mod, "steer_subagent")
        assert hasattr(mod, "drain_pending_steerings")
        assert hasattr(mod, "backfill_steering_queries")

    def test_import_flash_workflow(self):
        import src.server.handlers.chat.flash_workflow as mod

        assert hasattr(mod, "astream_flash_workflow")

    def test_import_ptc_workflow(self):
        import src.server.handlers.chat.ptc_workflow as mod

        assert hasattr(mod, "astream_ptc_workflow")

    def test_import_stream_reconnect(self):
        import src.server.handlers.chat.stream_reconnect as mod

        assert hasattr(mod, "reconnect_to_workflow_stream")
        assert hasattr(mod, "stream_subagent_task_events")


class TestNoCircularImports:
    """Package loads cleanly when all modules are freshly imported."""

    def test_fresh_import_succeeds(self):
        """Force reimport to detect any circular dependency at load time."""
        # Collect all chat submodule keys before removal
        chat_modules = [
            k
            for k in sys.modules
            if k.startswith("src.server.handlers.chat")
        ]
        saved = {}
        for k in chat_modules:
            saved[k] = sys.modules.pop(k)

        try:
            mod = importlib.import_module("src.server.handlers.chat")
            assert hasattr(mod, "astream_flash_workflow")
            assert hasattr(mod, "astream_ptc_workflow")
        finally:
            # Restore original modules to avoid polluting other tests
            for k, v in saved.items():
                sys.modules[k] = v


class TestLoggerName:
    """Logger name is preserved for backward compatibility."""

    def test_common_logger_name(self):
        from src.server.handlers.chat._common import logger

        assert logger.name == "src.server.handlers.chat_handler"


class TestOldModulePathRemoved:
    """The old monolithic chat_handler.py no longer exists as importable module."""

    def test_old_path_not_importable(self):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("src.server.handlers.chat_handler")
