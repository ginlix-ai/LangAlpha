"""The Claude Code runtime's tests need the ``claude-code`` extra (the SDK and its CLI); without it they are not collected."""

import importlib.util

import pytest

if importlib.util.find_spec("claude_agent_sdk") is None:
    collect_ignore_glob = ["*.py"]

TEST_KEY = "claude-code-test-key"


@pytest.fixture(autouse=True)
def _at_rest_key(monkeypatch):
    """A turn derives its device id from the deployment's at-rest key."""
    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", TEST_KEY)
