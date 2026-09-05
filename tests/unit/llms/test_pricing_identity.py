"""Pricing identity resolution: a manifest key must outrank a shared model_id.

Model ids are not unique across the manifest. A region or plan variant shares its
base model's id while declaring a different provider, and those providers carry
different rates, so resolving by id alone decides pricing by manifest order.

The pairs are discovered from the live manifest rather than named, so retiring a
model churns the coverage instead of breaking the test. The ordering contract
itself is pinned against a synthetic manifest, because shipped data is not a
place to keep a regression: an entry that stops declaring ``system_provider``
takes the assertion with it, and the test keeps passing on nothing.
"""

from unittest.mock import MagicMock, patch

from src.llms.llm import LLM
from src.llms.pricing_utils import detect_provider_for_model, resolve_pricing_identity


def _manifest():
    return LLM.get_model_config().llm_config


def _synthetic(entries):
    """Patch a hand-built manifest in, so a branch is covered regardless of data."""
    config = MagicMock()
    config.llm_config = entries
    return patch.object(LLM, "get_model_config", return_value=config)


def _shared_id_pairs():
    """Manifest keys grouped by the model_id they share, where providers differ."""
    by_id = {}
    for key, cfg in _manifest().items():
        by_id.setdefault(cfg.get("model_id"), []).append(key)
    manifest = _manifest()
    return [
        (model_id, keys)
        for model_id, keys in by_id.items()
        if len(keys) > 1 and len({manifest[k].get("provider") for k in keys}) > 1
    ]


class TestTheOrderingContract:
    """Pinned against injected data, so shipped models can come and go."""

    _TWO_KEYS_ONE_ID = {
        "model-base": {"model_id": "shared-id", "provider": "provider-a"},
        "model-regional": {"model_id": "shared-id", "provider": "provider-b"},
    }

    def test_a_key_outranks_another_entrys_matching_id(self):
        """Resolving both in one pass would price by whichever is listed first."""
        with _synthetic(self._TWO_KEYS_ONE_ID):
            assert resolve_pricing_identity("model-regional", billing_type="byok") == (
                "provider-b",
                "shared-id",
            )
            assert resolve_pricing_identity("model-base", billing_type="byok") == (
                "provider-a",
                "shared-id",
            )

    def test_platform_billing_prefers_the_system_route(self):
        """system_provider decides what the call costs, and an entry is free not to
        declare one, so asserting against live data risks never running this at all."""
        entries = {
            "glm-x-cn": {
                "model_id": "glm-x",
                "provider": "z-ai",
                "system_provider": "z-ai-dashscope",
            }
        }
        with _synthetic(entries):
            assert resolve_pricing_identity("glm-x-cn", billing_type="platform") == (
                "z-ai-dashscope",
                "glm-x",
            )
            for user_paid in ("byok", "oauth"):
                assert resolve_pricing_identity("glm-x-cn", billing_type=user_paid) == (
                    "z-ai",
                    "glm-x",
                )


class TestResolvePricingIdentity:
    def test_each_key_resolves_to_its_own_declared_provider(self):
        manifest = _manifest()
        for _model_id, keys in _shared_id_pairs():
            for key in keys:
                provider, _ = resolve_pricing_identity(key, billing_type="byok")
                assert provider == manifest[key]["provider"], (
                    f"{key} resolved to {provider}, not its declared "
                    f"{manifest[key]['provider']}"
                )

    def test_a_key_resolves_to_the_id_its_rates_are_filed_under(self):
        manifest = _manifest()
        for key, cfg in manifest.items():
            _, pricing_id = resolve_pricing_identity(key)
            assert pricing_id == cfg["model_id"]

    def test_platform_billing_prefers_the_system_route_in_shipped_data(self):
        """Sweeps whatever the shipped manifest declares; asserts nothing about how much."""
        manifest = _manifest()
        for key, cfg in manifest.items():
            if not cfg.get("system_provider"):
                continue
            provider, _ = resolve_pricing_identity(key, billing_type="platform")
            assert provider == cfg["system_provider"]

    def test_a_bare_model_id_still_resolves(self):
        """Records written before keys were stamped carry the id, not the key."""
        model_id = next(iter(_manifest().values()))["model_id"]
        provider, pricing_id = resolve_pricing_identity(model_id, billing_type="byok")
        assert provider is not None
        assert pricing_id == model_id

    def test_an_unknown_name_returns_no_provider_and_echoes_the_name(self):
        provider, pricing_id = resolve_pricing_identity("not-a-real-model-xyz")
        assert provider is None
        assert pricing_id == "not-a-real-model-xyz"

    def test_detect_provider_still_answers_for_its_callers(self):
        key = next(iter(_manifest()))
        assert detect_provider_for_model(key, billing_type="byok") == (
            _manifest()[key]["provider"]
        )
