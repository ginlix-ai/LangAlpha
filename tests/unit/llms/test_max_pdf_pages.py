import pytest

from src.llms.llm import get_max_pdf_pages


class TestGetMaxPdfPages:
    """The published per-request PDF page ceilings, read off the manifest.

    These pin the *shape* of the rule, not vendor numbers: that Anthropic's
    ceiling moves with the context window, that a provider documenting no page
    limit reports None rather than a guess, and that an unknown model fails
    closed. A vendor raising a limit should update the constant and these move
    with it; a vendor being read the wrong way should fail here.
    """

    @pytest.mark.parametrize(
        "model",
        ["claude-sonnet-5", "claude-opus-5", "claude-opus-4-8-oauth-1m"],
    )
    def test_a_1m_context_anthropic_route_gets_the_higher_ceiling(self, model):
        assert get_max_pdf_pages(model) == 600

    @pytest.mark.parametrize(
        "model",
        ["claude-sonnet-4-6", "claude-haiku-4-5", "claude-sonnet-4-6-oauth"],
    )
    def test_a_sub_1m_anthropic_route_gets_the_tighter_one(self, model):
        """The pair that makes a single global cap impossible: same vendor, same
        modality support, six-fold difference in what a request may carry."""
        assert get_max_pdf_pages(model) == 100

    def test_an_anthropic_sdk_endpoint_we_have_no_docs_for_is_treated_as_anthropic(self):
        """Volcengine's Anthropic-compatible route publishes no page limit. It
        speaks the same protocol, so it inherits the same reading rather than
        being assumed unbounded."""
        assert get_max_pdf_pages("doubao-seed-2.0-pro-anthropic") == 100

    def test_a_provider_with_no_documented_page_limit_reports_none(self):
        """None means 'not bounded by pages', which is a different claim from
        'we don't know' — the latter has to fail closed instead."""
        assert get_max_pdf_pages("gpt-5.5") is None

    def test_an_unknown_model_fails_closed(self):
        """This gates transmission, so an over-generous guess becomes a 400 the
        caller cannot recover; an over-tight one only costs a placeholder."""
        assert get_max_pdf_pages("not-a-real-model") == 100
