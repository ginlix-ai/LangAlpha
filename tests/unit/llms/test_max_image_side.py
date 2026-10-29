import pytest

from src.llms import llm
from src.llms.llm import (
    COMMON_IMAGE_TYPES,
    get_image_types,
    get_max_image_side,
    image_route,
)


class TestImageRoute:
    """Which vendor's image rules a client has to meet, read off its stamp.

    Pinned per provider family rather than per model, so a new model on a known
    route inherits its vendor's rule and a variant route (a region, a plan, an
    OAuth login) is judged as the vendor it serves.
    """

    @pytest.mark.parametrize(
        ("model", "route"),
        [
            ("claude-sonnet-5-5", "anthropic"),
            ("claude-haiku-5-5-oauth", "anthropic"),
            ("deepseek-flash", "deepseek"),
            ("glm-5.3-flash", "z-ai"),
            ("glm-5.3-flash-cn", "z-ai"),
            ("gpt-6.1-sol", "openai"),
            ("gpt-6.1-sol-oauth", "openai"),
            ("gemini-3.8-flash", "gemini"),
            ("qwen3.8-max-intl", "dashscope"),
        ],
    )
    def test_a_manifest_model_is_judged_as_its_vendor(self, model, route):
        assert image_route({"manifest_model": model}) == route

    def test_a_user_defined_model_is_judged_by_the_provider_it_was_built_for(self):
        """No manifest row to read: the provider stamp is all there is."""
        assert image_route({"manifest_model": "My GLM", "pricing_provider": "z-ai-cn"}) == "z-ai"

    def test_the_provider_called_outranks_the_manifest_models_own(self):
        """A platform route replaces the manifest provider when the caller
        brings no key, and the stamp records the replacement."""
        metadata = {"manifest_model": "claude-sonnet-5-5", "pricing_provider": "deepseek"}
        assert image_route(metadata) == "deepseek"

    def test_a_known_vendors_endpoint_outranks_the_provider_slot(self):
        """A BYOK base URL can point the Anthropic slot at another vendor."""
        metadata = {
            "manifest_model": "claude-sonnet-5-5",
            "pricing_provider": "anthropic",
            "provider_route": "anthropic@https://api.z.ai/api/anthropic",
        }
        assert image_route(metadata) == "z-ai"

    def test_an_unknown_endpoint_falls_back_to_the_provider(self):
        metadata = {
            "manifest_model": "claude-sonnet-5-5",
            "pricing_provider": "anthropic",
            "provider_route": "anthropic@https://proxy.example.com/v1",
        }
        assert image_route(metadata) == "anthropic"

    @pytest.mark.parametrize("metadata", [None, {}, {"manifest_model": "not-a-real-model"}])
    def test_nothing_known_is_no_route(self, metadata):
        assert image_route(metadata) == ""


class TestGetImageTypes:
    def test_gemini_and_dashscope_take_more_than_the_common_four(self):
        assert {"image/heic", "image/heif"} <= get_image_types("gemini")
        assert {"image/bmp", "image/tiff", "image/heic"} <= get_image_types("dashscope")

    @pytest.mark.parametrize("route", ["anthropic", "openai", "deepseek", "z-ai", ""])
    def test_the_rest_take_the_common_four(self, route):
        assert get_image_types(route) == COMMON_IMAGE_TYPES


class TestGetMaxImageSide:
    """The published per-side pixel limits per route."""

    def test_anthropic_holds_every_image_to_its_many_image_side(self):
        """Its side tightens above 20 images, and re-fitting an image already
        sent would invalidate the signed thinking after it, so the count
        never moves the side."""
        assert get_max_image_side("anthropic", 1) == 2000
        assert get_max_image_side("anthropic", 20) == 2000
        assert get_max_image_side("anthropic", 21) == 2000

    def test_deepseek_tightens_at_15(self):
        assert get_max_image_side("deepseek", 14) == 8192
        assert get_max_image_side("deepseek", 15) == 4096

    def test_glm_has_one_limit(self):
        assert get_max_image_side("z-ai", 1) == 6000
        assert get_max_image_side("z-ai", 50) == 6000

    @pytest.mark.parametrize("route", ["openai", "gemini", "dashscope"])
    def test_a_provider_that_resizes_oversized_images_reports_none(self, route):
        assert get_max_image_side(route, 1) is None
        assert get_max_image_side(route, 500) is None

    @pytest.mark.parametrize(
        ("count", "side"), [(1, 6000), (14, 6000), (15, 4096), (20, 4096), (21, 2000)]
    )
    @pytest.mark.parametrize("route", ["", "moonshot"])
    def test_a_route_with_no_rule_gets_the_tightest_known_one_at_its_count(
        self, route, count, side
    ):
        """This gates transmission: too generous a guess is a 400 that every
        later call on the thread repeats, too tight a one only costs detail.
        No one vendor is tightest at every count: GLM's 6000 up to 14 images,
        DeepSeek's 4096 from 15, Anthropic's 2000 above 20."""
        assert get_max_image_side(route, count) == side

    def test_a_new_vendor_rule_tightens_it(self, monkeypatch):
        monkeypatch.setitem(llm._IMAGE_SIDE_RULES, "new-vendor", (1500, ()))
        assert get_max_image_side("", 1) == 1500
