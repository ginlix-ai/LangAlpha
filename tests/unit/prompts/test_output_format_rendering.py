"""How the ``output_format`` ranking reaches the prompt.

``output_format`` is a list, most preferred first, of the forms an answer with
substance can take. It renders as one paragraph that lists them in the user's
order, in the main agent's copy only, and the agent still picks per answer. A
profile in the two keys the ranking replaced (``deliverable``, and
``output_format: "html"``) reads as the ranking migration 096 wrote for it.
"""

import pytest

from ptc_agent.agent.middleware.runtime_context.profile import (
    ProfileSnapshot,
    render_user_profile,
)
from ptc_agent.agent.prompts import reset_loader
from src.server.models.profile_vocabulary import upgraded_agent_preference

LEAD = "Fit how you hand back an answer to the work behind it."
CLOSE = "Use the highest one that suits this answer and this turn's surface"
VISUAL = "1. A visual answer."
LINES = {
    "visual": "A visual answer.",
    "markdown": "The full findings in a Markdown file",
    "text": "Text in the chat, with tables where they help.",
}


def _render(agent_preference, main_agent_steering=True):
    reset_loader()
    profile = {"name": "Demo", "timezone": "UTC", "locale": "en-US"}
    if agent_preference is not None:
        profile["agent_preference"] = agent_preference
    return render_user_profile(profile, main_agent_steering=main_agent_steering)


def _section(out):
    if "## Agent Preferences" not in out:
        return ""
    return out.split("## Agent Preferences", 1)[1].split("\n## ", 1)[0]


def _order(section):
    """The ranked forms as the paragraph lists them."""
    found = [(section.find(f". {line}"), form) for form, line in LINES.items()]
    return [form for at, form in sorted(found) if at >= 0]


@pytest.fixture(autouse=True)
def _reset_loader():
    reset_loader()
    yield
    reset_loader()


class TestTheRanking:
    @pytest.mark.parametrize(
        "ranking",
        [
            ["visual", "markdown", "text"],
            ["text", "markdown", "visual"],
            ["markdown", "text", "visual"],
            ["markdown"],
            ["text", "visual"],
        ],
    )
    def test_the_forms_are_listed_in_the_user_s_order(self, ranking):
        section = _section(_render({"output_format": ranking}))
        assert LEAD in section and CLOSE in section
        assert _order(section) == ranking
        for index, form in enumerate(ranking, start=1):
            assert f"\n{index}. {LINES[form]}" in section
        assert "- **output_format**:" not in section

    def test_a_simple_question_is_answered_in_the_chat_whatever_the_order(self):
        section = _section(_render({"output_format": ["visual"]}))
        assert "A simple question gets a reply in the chat as soon as you have the answer." in section

    def test_the_visual_form_picks_a_widget_or_a_report_by_the_answer(self):
        section = _section(_render({"output_format": ["visual"]}))
        assert "A short result, or one for a quick look, is an interactive chart or dashboard" in section
        assert "A long result, or one the user will keep or share, is a styled HTML report at `<task>/<name>.html`" in section
        assert "where this turn's surface shows widgets" in section

    def test_a_format_the_user_asks_for_wins(self):
        section = _section(_render({"output_format": ["text"]}))
        assert "When the user asks for a format, use it." in section

    def test_a_repeated_form_is_listed_once(self):
        section = _section(_render({"output_format": ["text", "text", "visual"]}))
        assert _order(section) == ["text", "visual"]
        assert "\n3. " not in section

    def test_its_paragraphs_stand_apart(self):
        section = _section(
            _render(
                {
                    "tone": "terse",
                    "research_depth": "light",
                    "output_format": ["visual", "markdown", "text"],
                    "proactivity": "on_request",
                }
            )
        )
        assert f"do it.\n\n{LEAD}" in section
        assert f"most preferred first:\n\n{VISUAL}" in section
        assert f"tables where they help.\n\n{CLOSE}" in section
        assert "use it.\n\nThe user wants to hear from you only when they ask" in section

    def test_a_subagent_gets_no_paragraph(self):
        out = _render({"output_format": ["visual"], "tone": "concise"}, main_agent_steering=False)
        assert LEAD not in out
        assert "- **tone**: concise" in out
        assert "- **output_format**:" not in out

    def test_no_ranking_renders_no_section(self):
        assert "## Agent Preferences" not in _render({"output_format": []})
        assert "## Agent Preferences" not in _render(None)


class TestOutsideTheVocabulary:
    @pytest.mark.parametrize("value", ["pdf", ["visual", "pdf"]])
    def test_a_value_outside_it_prints_as_the_user_wrote_it(self, value):
        """Anything else is the user's own words and reaches the model as
        they wrote it."""
        section = _section(_render({"output_format": value}))
        assert LEAD not in section
        assert "- **output_format**: " in section


class TestTheKeysItReplaced:
    @pytest.mark.parametrize(
        "legacy, ranking",
        [
            ({"output_format": "html"}, ["visual", "markdown", "text"]),
            ({"output_format": "html", "deliverable": "text"}, ["visual", "markdown", "text"]),
            ({"deliverable": "visual"}, ["visual", "markdown", "text"]),
            ({"deliverable": "text"}, ["text", "markdown", "visual"]),
            ({"deliverable": "text", "output_format": "markdown"}, ["text", "markdown", "visual"]),
        ],
    )
    def test_an_old_profile_reads_as_the_ranking_096_wrote(self, legacy, ranking):
        assert upgraded_agent_preference(legacy) == {"output_format": ranking}
        section = _section(_render(legacy))
        assert _order(section) == ranking
        assert "- **deliverable**:" not in section
        assert "Output Format: Styled HTML" not in section

    def test_markdown_alone_meant_nothing_chosen(self):
        assert upgraded_agent_preference({"output_format": "markdown", "tone": "x"}) == {"tone": "x"}
        assert "## Agent Preferences" not in _render({"output_format": "markdown"})

    def test_a_ranking_is_left_as_it_is(self):
        prefs = {"output_format": ["text"], "tone": "terse"}
        assert upgraded_agent_preference(prefs) == prefs

    def test_old_words_of_the_user_s_own_are_left_as_they_are(self):
        prefs = {"deliverable": "report", "output_format": "bullet points"}
        assert upgraded_agent_preference(prefs) == prefs

    def test_a_frozen_old_profile_compares_equal_to_its_migrated_row(self):
        """A thread frozen before 096 would otherwise get a change row for a
        preference nobody changed."""
        frozen = ProfileSnapshot(user_profile={"agent_preference": {"output_format": "html", "deliverable": "visual"}})
        migrated = ProfileSnapshot(user_profile={"agent_preference": {"output_format": ["visual", "markdown", "text"]}})
        assert frozen.fields() == migrated.fields()
