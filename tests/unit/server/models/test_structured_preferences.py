"""The structured profile's one check, and how the prompt reads the agent keys.

``check_profile`` is what the preferences endpoint and the agent's
``preference.json`` both run, so its rules are pinned here once; each caller's
own tests pin only how it turns the problems into its error.
"""

import pytest

from src.server.models.profile_vocabulary import (
    MAX_SECTOR_LENGTH,
    MAX_SECTORS,
    STRUCTURED_PREFERENCES,
    check_profile,
    prompt_preferences,
)


def _paths(problems):
    return [path for path, _ in problems]


class TestStoredSpelling:
    def test_every_key_is_kept_in_its_stored_spelling(self):
        checked, problems = check_profile(
            {
                "risk_preference": {"risk_level": " Moderate "},
                "investment_preference": {
                    "investor_type": "Professional",
                    "experience": " Under_1Y ",
                    "help_with": ["Portfolio", "ideas", "portfolio"],
                    "familiar_instruments": ["stock", "Future", "stock"],
                    "markets": ["us", "HK", "us"],
                    "asset_classes": ["ETF", "etf", "option"],
                    "approaches": ["Macro", "event_driven"],
                    "styles": ["growth"],
                    "horizon": "intraday",
                    "sectors": ["Technology", "real estate", "technology"],
                    "brokerages": [" Fidelity ", "IBKR", "other", "fidelity"],
                },
                "agent_preference": {
                    "research_depth": "Light",
                    "proactivity": "When_Needed",
                    "output_format": [" Visual ", "markdown", "visual"],
                },
            },
            None,
        )

        assert problems == []
        assert checked == {
            "risk_preference": {"risk_level": "moderate"},
            "investment_preference": {
                "investor_type": "professional",
                "experience": "under_1y",
                "help_with": ["portfolio", "ideas"],
                "familiar_instruments": ["stock", "future"],
                "markets": ["us", "hk"],
                "asset_classes": ["etf", "option"],
                "approaches": ["macro", "event_driven"],
                "styles": ["growth"],
                "horizon": "intraday",
                "sectors": ["technology", "real_estate"],
                "brokerages": ["fidelity", "ibkr", "other"],
            },
            "agent_preference": {
                "research_depth": "light",
                "proactivity": "when_needed",
                "output_format": ["visual", "markdown"],
            },
        }

    def test_a_brokerage_named_twice_is_kept_once(self):
        checked, problems = check_profile(
            {
                "investment_preference": {
                    "brokerages": ["moomoo", "Futu", "MooMoo", "futu"]
                }
            },
            None,
        )
        assert problems == []
        assert checked["investment_preference"]["brokerages"] == ["moomoo", "futu"]


class TestOutsideTheVocabulary:
    @pytest.mark.parametrize(
        "patches, path",
        [
            ({"risk_preference": {"risk_level": "yolo"}}, "risk_preference.risk_level"),
            ({"risk_preference": {"risk_level": 3}}, "risk_preference.risk_level"),
            (
                {"investment_preference": {"investor_type": "trader"}},
                "investment_preference.investor_type",
            ),
            (
                {"investment_preference": {"experience": "5y"}},
                "investment_preference.experience",
            ),
            (
                {"investment_preference": {"horizon": "decades"}},
                "investment_preference.horizon",
            ),
            (
                {"investment_preference": {"horizon": 5}},
                "investment_preference.horizon",
            ),
            (
                {"investment_preference": {"markets": ["us", "uk"]}},
                "investment_preference.markets[1]",
            ),
            (
                {"investment_preference": {"markets": "us"}},
                "investment_preference.markets",
            ),
            (
                {"investment_preference": {"markets": ["us", 1]}},
                "investment_preference.markets[1]",
            ),
            # The plural is not the stored spelling.
            (
                {"investment_preference": {"asset_classes": ["futures"]}},
                "investment_preference.asset_classes[0]",
            ),
            (
                {"investment_preference": {"help_with": ["ideas", "gossip"]}},
                "investment_preference.help_with[1]",
            ),
            (
                {
                    "investment_preference": {
                        "familiar_instruments": ["stock", "warrant"]
                    }
                },
                "investment_preference.familiar_instruments[1]",
            ),
            (
                {"investment_preference": {"approaches": ["macro", "sentiment"]}},
                "investment_preference.approaches[1]",
            ),
            (
                {"investment_preference": {"styles": ["momentum"]}},
                "investment_preference.styles[0]",
            ),
            # A brokerage not listed is ``other``; its name is the user's to
            # say, not a key.
            (
                {"investment_preference": {"brokerages": ["fidelity", "huatai"]}},
                "investment_preference.brokerages[1]",
            ),
            (
                {"agent_preference": {"research_depth": "deep"}},
                "agent_preference.research_depth",
            ),
            # A report is the visual answer's long form, never a rank of its own.
            (
                {"agent_preference": {"output_format": ["visual", "html"]}},
                "agent_preference.output_format[1]",
            ),
            (
                {"agent_preference": {"proactivity": "always"}},
                "agent_preference.proactivity",
            ),
            (
                {"agent_preference": {"output_format": "pdf"}},
                "agent_preference.output_format",
            ),
            (
                {"agent_preference": {"output_format": 123}},
                "agent_preference.output_format",
            ),
            (
                {"agent_preference": {"output_format": True}},
                "agent_preference.output_format",
            ),
        ],
    )
    def test_a_value_outside_its_vocabulary_is_named(self, patches, path):
        _checked, problems = check_profile(patches, None)
        assert _paths(problems) == [path]

    def test_the_problem_says_what_the_key_takes(self):
        _checked, problems = check_profile(
            {
                "investment_preference": {
                    "investor_type": "trader",
                    "brokerages": ["huatai"],
                },
                "agent_preference": {"output_format": ["report"]},
            },
            None,
        )
        messages = dict(problems)
        assert messages["investment_preference.investor_type"] == (
            "'trader' is not one of individual, professional, student"
        )
        assert messages["investment_preference.brokerages[0]"].startswith(
            "'huatai' is not one of fidelity, "
        )
        assert (
            messages["agent_preference.output_format[0]"]
            == "'report' is not one of visual, markdown, text"
        )

    def test_every_problem_in_every_column_is_named_in_order(self):
        """One refusal names them all, so one retry can fix them all."""
        _checked, problems = check_profile(
            {
                "risk_preference": {"risk_level": "yolo"},
                "investment_preference": {
                    "investor_type": "trader",
                    "approaches": ["macro", "sentiment"],
                    "horizon": 5,
                },
                "agent_preference": {"proactivity": "daily", "output_format": ["report"]},
            },
            None,
        )
        assert _paths(problems) == [
            "risk_preference.risk_level",
            "investment_preference.investor_type",
            "investment_preference.approaches[1]",
            "investment_preference.horizon",
            "agent_preference.proactivity",
            "agent_preference.output_format[0]",
        ]


class TestSectors:
    """Sectors are the user's own words, bounded rather than listed."""

    def test_words_become_the_slug_the_form_would_send(self):
        checked, problems = check_profile(
            {
                "investment_preference": {
                    "sectors": ["Real Estate", "real_estate", "AI-chips"]
                }
            },
            None,
        )
        assert problems == []
        assert checked["investment_preference"]["sectors"] == [
            "real_estate",
            "ai-chips",
        ]

    @pytest.mark.parametrize(
        "sector", ["tech/AI", "x" * (MAX_SECTOR_LENGTH + 1), "_tech", ""]
    )
    def test_a_sector_that_is_not_a_short_slug_is_named(self, sector):
        _checked, problems = check_profile(
            {"investment_preference": {"sectors": [sector]}}, None
        )
        assert _paths(problems) == ["investment_preference.sectors[0]"]

    def test_the_longest_sector_and_the_most_sectors_pass(self):
        sectors = ["x" * MAX_SECTOR_LENGTH, *(f"s{i}" for i in range(MAX_SECTORS - 1))]
        _checked, problems = check_profile(
            {"investment_preference": {"sectors": sectors}}, None
        )
        assert problems == []

    def test_one_sector_too_many_is_named_on_the_list(self):
        sectors = [f"s{i}" for i in range(MAX_SECTORS + 1)]
        _checked, problems = check_profile(
            {"investment_preference": {"sectors": sectors}}, None
        )
        assert _paths(problems) == ["investment_preference.sectors"]


class TestWhatPasses:
    def test_every_choice_passes_as_it_is(self):
        for column, keys in STRUCTURED_PREFERENCES.items():
            for key, vocab in keys.items():
                for choice in vocab.choices or ():
                    value = [choice] if vocab.many else choice
                    patches = {column: {key: value}}
                    assert check_profile(patches, None) == (patches, []), (
                        f"{column}.{key}={choice}"
                    )

    def test_a_key_outside_the_vocabulary_is_free_text(self):
        """``drawdown_reaction`` had a vocabulary once and is checked no longer."""
        patches = {
            "risk_preference": {
                "risk_level": "growth",
                "drawdown_reaction": "Panic",
                "tolerance": 7,
            },
            "investment_preference": {"holding_period": "a few years"},
            "agent_preference": {"tone": "Plain words"},
        }
        assert check_profile(patches, None) == (patches, [])

    def test_null_passes_as_a_deletion(self):
        patches = {
            "risk_preference": {"risk_level": None},
            "investment_preference": {"markets": None, "sectors": None},
            "agent_preference": {"research_depth": None, "output_format": None},
        }
        assert check_profile(patches, None) == (patches, [])

    @pytest.mark.parametrize("patch", [None, {}])
    def test_a_column_with_no_patch_is_left_as_it_came(self, patch):
        patches = {"risk_preference": patch, "other_preference": {"x": "anything"}}
        assert check_profile(patches, None) == (patches, [])

    def test_a_value_the_row_holds_survives_a_save_that_leaves_it(self):
        """``horizon`` and ``sectors`` were free-form before they had a
        vocabulary; a save that sends the stored value back is not refused."""
        stored = {
            "investment_preference": {"horizon": "long_term", "sectors": ["Tech & AI"]},
            "agent_preference": {"research_depth": "deep"},
        }
        patches = {
            "investment_preference": {"horizon": "long_term", "sectors": ["Tech & AI"]},
            "agent_preference": {"research_depth": "deep"},
        }
        assert check_profile(patches, stored) == (patches, [])

    def test_a_list_item_the_row_holds_passes_beside_a_new_one(self):
        """The form sends the whole list when one sector is toggled, so a
        sector saved before the vocabulary rides along as written."""
        stored = {"investment_preference": {"sectors": ["Tech & AI"]}}
        checked, problems = check_profile(
            {"investment_preference": {"sectors": ["Tech & AI", "energy"]}}, stored
        )
        assert problems == []
        assert checked["investment_preference"]["sectors"] == ["Tech & AI", "energy"]

    def test_a_new_list_item_is_still_checked_beside_one_the_row_holds(self):
        stored = {"investment_preference": {"sectors": ["Tech & AI"]}}
        _checked, problems = check_profile(
            {"investment_preference": {"sectors": ["Tech & AI", "Not & Valid"]}}, stored
        )
        assert _paths(problems) == ["investment_preference.sectors[1]"]

    def test_changing_a_value_the_row_held_is_checked(self):
        stored = {"investment_preference": {"horizon": "long_term"}}
        _checked, problems = check_profile(
            {"investment_preference": {"horizon": "short_term"}}, stored
        )
        assert _paths(problems) == ["investment_preference.horizon"]

    @pytest.mark.parametrize(
        "stored",
        [{"investment_preference": "long_term"}, {"investment_preference": None}],
    )
    def test_a_stored_column_that_is_not_an_object_holds_nothing(self, stored):
        _checked, problems = check_profile(
            {"investment_preference": {"horizon": "long_term"}}, stored
        )
        assert _paths(problems) == ["investment_preference.horizon"]


def test_every_key_documents_itself():
    """The profile README's rows and the API schema's field descriptions are
    the vocabulary's ``doc``, so an empty one ships an empty row."""
    for column, keys in STRUCTURED_PREFERENCES.items():
        for key, vocab in keys.items():
            assert vocab.doc.strip(), f"{column}.{key}"


class TestPromptPreferences:
    def test_a_value_in_its_vocabulary_steers_and_the_rest_prints(self):
        printed, steering = prompt_preferences(
            {
                "research_depth": "thorough",
                "output_format": ["markdown", "text"],
                "proactivity": "sometimes",
                "tone": "terse",
            }
        )
        assert steering == {"research_depth": "thorough", "output_format": ["markdown", "text"]}
        assert printed == {"proactivity": "sometimes", "tone": "terse"}

    def test_a_ranking_with_a_format_outside_it_prints(self):
        printed, steering = prompt_preferences({"output_format": ["visual", "pdf"]})
        assert steering == {}
        assert printed == {"output_format": ["visual", "pdf"]}

    def test_a_deliverable_outside_the_old_vocabulary_is_the_user_s_words(self):
        """Only text and visual were ever deliverables; "html" was an output format."""
        printed, steering = prompt_preferences({"deliverable": "html"})
        assert steering == {}
        assert printed == {"deliverable": "html"}


class TestTheValuesBeforeTheRanking:
    """A tab still on the build before migration 096 sends the old strings."""

    def test_html_is_saved_as_the_ranking_096_wrote(self):
        checked, problems = check_profile({"agent_preference": {"output_format": "html"}}, None)
        assert problems == []
        assert checked["agent_preference"] == {"output_format": ["visual", "markdown", "text"]}

    def test_markdown_is_saved_as_no_ranking(self):
        checked, problems = check_profile({"agent_preference": {"output_format": "markdown"}}, None)
        assert problems == []
        assert checked["agent_preference"] == {"output_format": None}

    def test_a_deliverable_alone_replaces_the_stored_ranking(self):
        """An old onboarding tab sends only the answer it changed."""
        stored = {"agent_preference": {"output_format": ["visual", "markdown", "text"]}}
        checked, problems = check_profile({"agent_preference": {"deliverable": "text"}}, stored)
        assert problems == []
        assert checked["agent_preference"] == {
            "output_format": ["text", "markdown", "visual"],
            "deliverable": None,
        }

    def test_a_deliverable_in_the_user_s_words_is_kept(self):
        checked, problems = check_profile(
            {"agent_preference": {"output_format": "html", "deliverable": "slides"}}, None
        )
        assert problems == []
        assert checked["agent_preference"] == {
            "output_format": ["visual", "markdown", "text"],
            "deliverable": "slides",
        }

    def test_a_deliverable_beside_a_list_in_the_user_s_words_stays_theirs(self):
        printed, steering = prompt_preferences(
            {"output_format": ["visual", "pdf"], "deliverable": "text"}
        )
        assert steering == {}
        assert printed == {"output_format": ["visual", "pdf"], "deliverable": "text"}

    def test_a_deliverable_beside_a_ranking_never_reaches_the_prompt(self):
        printed, steering = prompt_preferences(
            {"output_format": ["markdown", "text", "visual"], "deliverable": "visual"}
        )
        assert steering == {"output_format": ["markdown", "text", "visual"]}
        assert printed == {}

    def test_the_row_is_left_as_it_was(self):
        row = {"proactivity": "proactive"}
        prompt_preferences(row)
        assert row == {"proactivity": "proactive"}

    @pytest.mark.parametrize("row", [None, "terse", ["tone"]])
    def test_a_row_that_is_not_an_object_holds_nothing(self, row):
        assert prompt_preferences(row) == ({}, {})
