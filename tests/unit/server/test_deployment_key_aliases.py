"""``deployment_key`` must honour a provider's ``env_key_aliases`` like ``LLM._resolve_api_key``.

Otherwise an operator who sets only the alias (e.g. ``ATLAS_CLOUD_API_KEY``) gets working
requests but a key status that reports the provider as unconfigured.
"""

from unittest.mock import patch

import pytest

from src.llms.llm import provider_env_keys
from src.server.services.llm.config import deployment_key

ATLAS = {
    "env_key": "ATLASCLOUD_API_KEY",
    "env_key_aliases": ["ATLAS_CLOUD_API_KEY"],
}


@pytest.fixture(autouse=True)
def _oss_mode_and_clean_env(monkeypatch):
    monkeypatch.delenv("ATLASCLOUD_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_CLOUD_API_KEY", raising=False)
    with patch("src.config.settings.HOST_MODE", "oss"):
        yield


def test_provider_env_keys_orders_primary_then_aliases_without_duplicates():
    info = {"env_key": "A", "env_key_aliases": ["B", "A", "", "C", "B"]}
    assert provider_env_keys(info) == ["A", "B", "C"]
    assert provider_env_keys({}) == []


def test_alias_only_is_reported_as_a_deployment_key(monkeypatch):
    monkeypatch.setenv("ATLAS_CLOUD_API_KEY", "alias-key")
    assert deployment_key(ATLAS) == "alias-key"


def test_primary_wins_over_alias(monkeypatch):
    monkeypatch.setenv("ATLASCLOUD_API_KEY", "primary-key")
    monkeypatch.setenv("ATLAS_CLOUD_API_KEY", "alias-key")
    assert deployment_key(ATLAS) == "primary-key"


def test_no_key_set_is_none():
    assert deployment_key(ATLAS) is None


def test_platform_mode_never_exposes_the_deployment_key(monkeypatch):
    monkeypatch.setenv("ATLAS_CLOUD_API_KEY", "alias-key")
    with patch("src.config.settings.HOST_MODE", "platform"):
        assert deployment_key(ATLAS) is None
