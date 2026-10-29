"""Custom models and providers on a provider the manifest no longer has.

No client can be built for one, so readers must treat the name as gone (the
``model_removed`` path) while the stored row stays as it was.
"""

from unittest.mock import MagicMock, patch

from src.server.services.llm.user_models import without_removed_providers

PREF = {
    "preferred_model": "my-doubao",
    "custom_providers": [
        {"name": "my-ark", "parent_provider": "volcengine"},
        {"name": "my-openai", "parent_provider": "openai"},
    ],
    "custom_models": [
        {"name": "my-doubao", "model_id": "doubao-seed", "provider": "volcengine"},
        {"name": "via-ark", "model_id": "doubao-seed", "provider": "my-ark"},
        {"name": "via-openai", "model_id": "gpt-4o", "provider": "my-openai"},
        {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai"},
        {"name": "odd", "model_id": "x", "provider": ["openai"]},
    ],
}


def _live(providers):
    mc = MagicMock()
    mc.flat_providers = {p: {} for p in providers}
    with patch("src.llms.llm.LLM.get_model_config", return_value=mc):
        return without_removed_providers(PREF)


def test_drops_what_runs_on_a_removed_provider():
    live = _live(["openai"])
    assert [cp["name"] for cp in live["custom_providers"]] == ["my-openai"]
    assert [cm["name"] for cm in live["custom_models"]] == ["via-openai", "my-gpt"]
    assert live["preferred_model"] == "my-doubao"  # the scrub's to clear, on a pick
    assert len(PREF["custom_models"]) == 5  # the stored bag is not touched


def test_an_empty_manifest_removes_nothing():
    """A load failure, not a catalog of nothing."""
    assert _live([]) is PREF
