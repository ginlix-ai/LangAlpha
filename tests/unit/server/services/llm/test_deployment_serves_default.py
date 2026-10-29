"""Whether a user with no credential of their own can run a turn.

The web's setup gate reads this from ``/users/me``. Without it a self-hosted
stack whose keys live in its config sent its user to the setup wizard on every
visit, though every turn already ran on those keys.
"""

from unittest.mock import patch

import pytest

from src.server.services.llm import config as llm_config

MODEL = "deepseek-flash"


@pytest.fixture
def default_model():
    with patch.object(llm_config, "system_default_models", return_value={"default_model": MODEL, "background_model": ""}):
        yield


@pytest.fixture
def env_key():
    from src.llms.llm import LLM

    mc = LLM.get_model_config()
    entry = mc.get_model_config(MODEL)
    return mc.get_provider_info(entry.get("system_provider") or entry["provider"])["env_key"]


def test_self_hosted_with_the_key_in_its_config_is_served(default_model, env_key, monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.setenv(env_key, "k")
    assert llm_config.deployment_serves_default()


def test_self_hosted_without_the_key_still_needs_setup(default_model, env_key, monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.delenv(env_key, raising=False)
    assert not llm_config.deployment_serves_default()


def test_platform_mode_leaves_it_to_the_plan(default_model, env_key, monkeypatch):
    """The deployment's key there is the platform's, which only a plan reaches."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setenv(env_key, "k")
    assert not llm_config.deployment_serves_default()
