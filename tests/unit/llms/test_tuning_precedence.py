"""One precedence rule, two implementations, one fixture.

``resolve_tuning_field`` and ``resolveTuningField`` in
``web/src/lib/modelTuning.ts`` answer the same question on either side of the
wire, so the cases live in JSON and a change on one side fails the other's
suite. Locating the profile is not part of that contract: the TypeScript
resolver is handed both layers already selected, so the selection and
malformed-shape cases below are Python's alone.
"""

import json
from pathlib import Path

import pytest

from src.llms.preferences import TUNING_FIELDS, Tuning, resolve_tuning, resolve_tuning_field

CONTRACT = json.loads(
    (Path(__file__).parents[3] / "tests/fixtures/tuning_precedence.json").read_text()
)

MODEL = "m1"


def test_contract_covers_every_tuning_field():
    assert sorted(CONTRACT["fields"]) == sorted(TUNING_FIELDS)


@pytest.mark.parametrize("case", CONTRACT["cases"], ids=lambda c: c["name"])
def test_shared_precedence(case):
    model_pref = {**case["account"], "profiles": {MODEL: case["profile"]}}
    assert resolve_tuning_field(model_pref, MODEL, case["field"]) == case["expected"]


@pytest.mark.parametrize(
    "model_pref,model,expected",
    [
        # Another model's profile is not this model's.
        ({"reasoning_effort": "low", "profiles": {"other": {"reasoning_effort": "high"}}}, MODEL, "low"),
        # No model in hand: the account value is the only answer available.
        ({"reasoning_effort": "low", "profiles": {MODEL: {"reasoning_effort": "high"}}}, None, "low"),
        # No profiles map at all.
        ({"reasoning_effort": "low"}, MODEL, "low"),
    ],
)
def test_profile_selection(model_pref, model, expected):
    assert resolve_tuning_field(model_pref, model, "reasoning_effort") == expected


@pytest.mark.parametrize(
    "profiles",
    ["not-a-map", 42, [], {MODEL: "not-a-map"}, {MODEL: None}],
    ids=["str", "int", "list", "entry-str", "entry-none"],
)
def test_malformed_profiles_fall_through_rather_than_raising(profiles):
    """A stored shape the writer should have rejected must not break a turn.

    The write path validates, but rows outlive the code that wrote them.
    """
    model_pref = {"reasoning_effort": "low", "profiles": profiles}
    assert resolve_tuning_field(model_pref, MODEL, "reasoning_effort") == "low"


def test_resolve_tuning_agrees_with_the_single_field_resolver():
    model_pref = {
        "prompt_guidance": "lean",
        "compaction_profile": "moderate",
        "reasoning_effort": "low",
        "fast_mode": True,
        "profiles": {MODEL: {"reasoning_effort": "high", "fast_mode": False}},
    }
    tuning = resolve_tuning(model_pref, MODEL)

    assert tuning == Tuning(
        prompt_guidance="lean",
        compaction_profile="moderate",
        reasoning_effort="high",
        fast_mode=False,
    )
    for field in TUNING_FIELDS:
        assert getattr(tuning, field) == resolve_tuning_field(model_pref, MODEL, field)
