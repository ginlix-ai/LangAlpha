import pytest

from src.llms.llm import LLM, get_max_pdf_pages


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
        ["claude-sonnet-5-5", "claude-opus-5-5", "claude-opus-5-5-oauth"],
    )
    def test_a_1m_context_anthropic_route_gets_the_higher_ceiling(self, model):
        assert get_max_pdf_pages(model) == 600

    @pytest.mark.parametrize("provider", ["anthropic", "claude-oauth"])
    def test_a_sub_1m_anthropic_route_gets_the_tighter_one(self, monkeypatch, provider):
        """The pair that makes a single global cap impossible: same vendor, same
        modality support, six-fold difference in what a request may carry.

        Synthetic entries, because no shipped Anthropic route sits below 1M any
        more and a model that is not in the manifest also gets 100. The 1M twin
        answering 600 is what shows the entries were read at all."""
        models = LLM.get_model_config().llm_config
        for name, context in (("_wide", 1_000_000), ("_narrow", 200_000)):
            monkeypatch.setitem(
                models, name, {"model_id": name, "provider": provider, "context": context}
            )
        assert get_max_pdf_pages("_wide") == 600
        assert get_max_pdf_pages("_narrow") == 100

    def test_a_provider_with_no_documented_page_limit_reports_none(self):
        """None means 'not bounded by pages', which is a different claim from
        'we don't know' — the latter has to fail closed instead."""
        assert get_max_pdf_pages("gpt-6.1-sol") is None

    def test_an_unknown_model_fails_closed(self):
        """This gates transmission, so an over-generous guess becomes a 400 the
        caller cannot recover; an over-tight one only costs a placeholder."""
        assert get_max_pdf_pages("not-a-real-model") == 100
