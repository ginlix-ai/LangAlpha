"""The shared validator: what either writer may save, refused field by field the same way."""

from __future__ import annotations

from typing import Any

import pytest

from src.server.services.subagents.rules import (
    MAX_CUSTOM,
    NUL_MESSAGE,
    SURROGATE_MESSAGE,
    WRITABLE_KEYS,
    FieldError,
    SubagentInvalid,
    content_errors,
    description_matches,
    validate_fields,
    validate_new_name,
)
from tests.unit.server.services.subagents._fakes import World

ROLES = ["analyst", "chief_of_staff"]

# A custom subagent's values before the save, on a model with three levels.
CURRENT: dict[str, Any] = {
    "enabled": True, "model": "model-a", "effort": None, "tools": ["web"],
    "search_provider": None, "search_depth": None, "connectors": True, "brokerages": "on",
    "memory": True, "preferred_connectors": [], "preferred_skills": [], "description": "Handles work.",
    "instructions": "", "available_to": ROLES,
}
# A base pinning a model the user's plan doesn't carry, with two levels.
PINNED_BASE: dict[str, Any] = {**CURRENT, "model": "model-pinned", "effort": "low"}

# Not pinned: the row only says the field was refused.
ANY = object()


async def _validate(fields: dict[str, Any], *, kind: str = "custom", base: dict[str, Any] | None = None):
    current = dict(base) if base is not None else CURRENT
    return await validate_fields(World().ctx(), fields, kind=kind, current=current, base=base)


@pytest.mark.parametrize(
    ("fields", "kind", "field", "options"),
    [
        ({"model": "nope"}, "custom", "model", ["model-a", "model-flat"]),
        # Known, but off the plan: the turn would not run it.
        ({"model": "model-off-plan"}, "custom", "model", ["model-a", "model-flat"]),
        ({"effort": "turbo"}, "custom", "effort", ["low", "medium", "high"]),
        # Held to the ladder of the model it runs on.
        ({"effort": "max"}, "custom", "effort", ["low", "medium", "high"]),
        ({"model": "model-flat", "effort": "high"}, "custom", "effort", []),
        ({"tools": ["web", "telepathy"]}, "custom", "tools", ["web", "market_data", "code", "shell"]),
        ({"tools": "web"}, "custom", "tools", None),
        ({"search_provider": "nope"}, "custom", "search_provider", ANY),
        ({"search_provider": "serper", "search_depth": "deep"}, "custom", "search_depth", ["standard"]),
        ({"search_depth": "bottomless"}, "custom", "search_depth", ANY),
        ({"connectors": "maybe"}, "custom", "connectors", ["on", "off"]),
        ({"enabled": "yes"}, "custom", "enabled", ["true", "false"]),
        ({"connectors": "true"}, "custom", "connectors", ["on", "off"]),
        ({"memory": "sometimes"}, "custom", "memory", ["on", "off"]),
        ({"brokerages": "all"}, "custom", "brokerages", ["off", "on"]),
        ({"preferred_connectors": ["ghost"]}, "custom", "preferred_connectors", ["fundamentals", "my-broker"]),
        ({"preferred_skills": ["ghost"]}, "custom", "preferred_skills", ["dcf", "sec-filings"]),
        ({"available_to": ["boss"]}, "custom", "available_to", ROLES),
        ({"available_to": []}, "custom", "available_to", ROLES),
        ({"description": "x" * 401}, "custom", "description", None),
        # Bytes, not characters: 8193 two-byte characters are over 16 KiB.
        ({"instructions": "é" * 8193}, "custom", "instructions", None),
        # Postgres stores no NUL; a YAML "\0" escape writes one from plain text.
        ({"description": "A\x00B"}, "custom", "description", None),
        ({"instructions": "A\x00B"}, "custom", "instructions", None),
        ({"description": "A\ud800B"}, "custom", "description", None),
        ({"instructions": "A\ud800B"}, "custom", "instructions", None),
        ({"colour": "blue"}, "custom", "colour", list(WRITABLE_KEYS)),
        ({"description": "Mine now."}, "override", "description", None),
        ({"instructions": "Mine now."}, "override", "instructions", None),
        ({"available_to": ["analyst"]}, "override", "available_to", None),
    ],
)
@pytest.mark.asyncio
async def test_a_bad_value_is_refused_on_its_field(fields, kind, field, options):
    base = CURRENT if kind == "override" else None
    with pytest.raises(SubagentInvalid) as exc:
        await _validate(fields, kind=kind, base=base)

    assert [e.field for e in exc.value.errors] == [field]
    error = exc.value.errors[0].as_dict()
    assert set(error) == ({"field", "message"} if options is None else {"field", "message", "options"})
    if options is not ANY:
        assert error.get("options") == options


@pytest.mark.asyncio
async def test_every_refused_field_is_reported_at_once():
    with pytest.raises(SubagentInvalid) as exc:
        await _validate({"model": "nope", "tools": ["telepathy"], "brokerages": "all", "enabled": False})

    assert {e.field for e in exc.value.errors} == {"model", "tools", "brokerages"}


@pytest.mark.parametrize(
    ("fields", "saved"),
    [
        ({"enabled": "off", "connectors": "on"}, {"enabled": False, "connectors": True}),
        ({"memory": "off"}, {"memory": False}),
        ({"enabled": "true", "memory": "on"}, {"enabled": True, "memory": True}),
        ({"enabled": "false"}, {"enabled": False}),
        ({"memory": True}, {"memory": True}),
        # YAML reads a bare on/off as a boolean.
        ({"brokerages": True}, {"brokerages": "on"}),
        ({"brokerages": False}, {"brokerages": "off"}),
        ({"tools": ["shell", "web", "web"]}, {"tools": ["web", "shell"]}),
        ({"model": "Inherit", "effort": None}, {"model": "inherit", "effort": None}),
        ({"available_to": ["chief_of_staff", "analyst"]}, {"available_to": ROLES}),
        ({"preferred_skills": ["dcf", " dcf ", ""]}, {"preferred_skills": ["dcf"]}),
        ({"search_provider": "serper", "search_depth": "standard"},
         {"search_provider": "serper", "search_depth": "standard"}),
        # With the provider inherited, any level some provider offers passes.
        ({"search_depth": "deep"}, {"search_depth": "deep"}),
        ({"description": "  Padded.  "}, {"description": "Padded."}),
        ({"description": "x" * 400, "instructions": "é" * 8192},
         {"description": "x" * 400, "instructions": "é" * 8192}),
    ],
)
@pytest.mark.asyncio
async def test_an_accepted_value_is_saved_in_canonical_form(fields, saved):
    assert await _validate(fields) == saved


@pytest.mark.asyncio
async def test_only_the_keys_sent_are_judged():
    """A skill deleted since, or a model since dropped from the plan, must not
    block an unrelated edit; the view flags them instead."""
    stale = {**CURRENT, "model": "model-off-plan", "preferred_skills": ["gone"], "preferred_connectors": ["gone"]}

    patch = await validate_fields(World().ctx(), {"enabled": False}, kind="custom", current=stale)

    assert patch == {"enabled": False}


@pytest.mark.parametrize(
    ("fields", "refused"),
    [
        # Writing the base's own pin back is the operator's choice, never plan-gated.
        ({"model": "model-pinned"}, None),
        ({"model": "model-pinned", "effort": "high"}, None),
        # A model the user picks is.
        ({"model": "model-off-plan"}, "model"),
        # An effort alone is held to the ladder of the base's pin.
        ({"effort": "medium"}, "effort"),
    ],
)
@pytest.mark.asyncio
async def test_the_plan_check_applies_to_models_the_user_chooses(fields, refused):
    if refused is None:
        assert await _validate(fields, kind="override", base=PINNED_BASE) == fields
        return
    with pytest.raises(SubagentInvalid) as exc:
        await _validate(fields, kind="override", base=PINNED_BASE)
    assert [e.field for e in exc.value.errors] == [refused]


@pytest.mark.parametrize(
    ("saved", "model", "refused"),
    [
        ("high", "model-flat", True),
        ("high", "model-a", False),
        (None, "model-flat", False),
        # The thread's model is not known until the turn, which steps a level
        # down to what that model offers.
        ("high", "inherit", False),
    ],
)
@pytest.mark.asyncio
async def test_a_new_model_holds_the_saved_effort_to_its_ladder(saved, model, refused):
    # A model-only save would otherwise keep a pair the same call refuses
    # when the two are written together.
    current = {**CURRENT, "effort": saved}
    if not refused:
        await validate_fields(World().ctx(), {"model": model}, kind="custom", current=current)
        return
    with pytest.raises(SubagentInvalid) as exc:
        await validate_fields(World().ctx(), {"model": model}, kind="custom", current=current)
    assert [(e.field, e.message) for e in exc.value.errors] == [
        ("effort", "model-flat has no effort setting. Use inherit.")
    ]


@pytest.mark.parametrize(
    ("saved", "provider", "refused"),
    [
        ("deep", "serper", True),
        ("deep", "exa", False),
        (None, "serper", False),
        # Which provider runs on inherit is not known until the turn.
        ("deep", "inherit", False),
    ],
)
@pytest.mark.asyncio
async def test_a_new_provider_holds_the_saved_depth_to_its_levels(saved, provider, refused):
    current = {**CURRENT, "search_provider": "tavily", "search_depth": saved}
    fields = {"search_provider": provider}
    if not refused:
        await validate_fields(World().ctx(), fields, kind="custom", current=current)
        return
    with pytest.raises(SubagentInvalid) as exc:
        await validate_fields(World().ctx(), fields, kind="custom", current=current)
    assert [(e.field, e.message) for e in exc.value.errors] == [
        ("search_depth", "serper doesn't offer deep depth.")
    ]


@pytest.mark.parametrize(
    ("pinned", "fields", "message"),
    [
        # Cleared, the provider is the base's again, under the saved depth.
        (("serper", None), {"search_provider": None}, "serper doesn't offer deep depth."),
        # Cleared, the depth is the base's again, under the new provider.
        (("tavily", "deep"), {"search_provider": "serper", "search_depth": None},
         "serper doesn't offer deep depth."),
        (("tavily", "ultra_fast"), {"search_depth": None}, "exa doesn't offer ultra_fast depth."),
        (("serper", None), {"search_provider": None, "search_depth": None}, None),
        ((None, None), {"search_provider": None}, None),
        (("serper", None), {"search_provider": "inherit"}, None),
    ],
)
@pytest.mark.asyncio
async def test_a_cleared_search_key_holds_the_pair_the_base_puts_back(pinned, fields, message):
    base = {**CURRENT, "search_provider": pinned[0], "search_depth": pinned[1]}
    current = {**CURRENT, "search_provider": "exa", "search_depth": "deep"}
    if message is None:
        await validate_fields(World().ctx(), fields, kind="override", current=current, base=base)
        return
    with pytest.raises(SubagentInvalid) as exc:
        await validate_fields(World().ctx(), fields, kind="override", current=current, base=base)
    assert [(e.field, e.message) for e in exc.value.errors] == [("search_depth", message)]


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"effort": None}, "model-pinned doesn't offer medium effort."),
        ({"model": "model-flat", "effort": None}, "model-flat has no effort setting. Use inherit."),
    ],
)
@pytest.mark.asyncio
async def test_a_cleared_effort_is_held_to_the_model_it_runs_on(fields, message):
    base = {**CURRENT, "model": "model-a", "effort": "medium"}
    current = {**CURRENT, "model": "model-pinned", "effort": "high"}
    with pytest.raises(SubagentInvalid) as exc:
        await validate_fields(World().ctx(), fields, kind="override", current=current, base=base)
    assert [(e.field, e.message) for e in exc.value.errors] == [("effort", message)]


@pytest.mark.parametrize(
    ("base", "current", "fields"),
    [
        ({"search_provider": "serper", "search_depth": "deep"},
         {"search_provider": "tavily", "search_depth": "deep"}, {"search_provider": None}),
        ({"model": "model-pinned", "effort": "medium"},
         {"model": "model-a", "effort": "medium"}, {"model": None}),
    ],
    ids=["search", "effort"],
)
@pytest.mark.asyncio
async def test_landing_back_on_the_bases_own_pair_is_allowed(base, current, fields):
    """The base's pair is the operator's call, as its pin is, so a reset to it
    passes even when the manifest no longer offers it; the view flags that."""
    await validate_fields(
        World().ctx(), fields, kind="override", current={**CURRENT, **current}, base={**CURRENT, **base}
    )


@pytest.mark.parametrize(
    ("mine", "roles", "others", "matches"),
    [
        ("Reads  credit\nfilings.", ["analyst"], [("other", "reads credit filings.", ["analyst"])], ["other"]),
        # Subagents no role picks between can say the same thing.
        ("Reads credit filings.", ["analyst"], [("other", "Reads credit filings.", ["chief_of_staff"])], []),
        ("", ROLES, [("other", "", ROLES)], []),
    ],
)
def test_description_matches_ignores_case_and_spacing_within_a_shared_role(mine, roles, others, matches):
    assert description_matches(mine, roles, others) == matches


@pytest.mark.parametrize(
    ("name", "custom_count", "refused"),
    [
        ("credit-analyst", 0, False),
        ("Credit_Analyst", 0, True),
        ("-leading-hyphen", 0, True),
        ("a" * 49, 0, True),
        ("main", 0, True),
        ("research", 0, True),
        ("credit-analyst", MAX_CUSTOM, True),
    ],
)
def test_a_new_name(name, custom_count, refused):
    errors = validate_new_name(name, base_names={"research"}, custom_count=custom_count)

    assert [e.field for e in errors] == (["name"] if refused else [])


@pytest.mark.parametrize("field", ["description", "instructions"])
@pytest.mark.parametrize(
    ("text", "message"),
    [("A\x00B", NUL_MESSAGE), ("A\ud800B", SURROGATE_MESSAGE), ("A\udfffB", SURROGATE_MESSAGE)],
)
def test_a_lone_surrogate_is_refused_like_a_nul(field, text, message):
    assert [(e.field, e.message) for e in content_errors({field: text})] == [(field, message)]


@pytest.mark.parametrize("field", ["description", "instructions"])
def test_a_valid_surrogate_pair_passes(field):
    assert content_errors({field: "Up \U0001f4c8 only."}) == []


@pytest.mark.parametrize(
    ("size", "message"),
    [
        (16384, None),
        # Rounded up, so a size just over the limit never reads as 16.0.
        (16385, "Keep instructions to 16 KB or less (this is 16.1 KB)."),
    ],
)
def test_the_instructions_limit_is_stated_and_rounded_up(size, message):
    errors = content_errors({"instructions": "a" * size})

    assert [e.message for e in errors] == ([] if message is None else [message])


def test_a_field_error_escapes_a_lone_surrogate_it_echoes():
    error = FieldError("model", "Unknown model: x\ud800.")

    assert error.message == "Unknown model: x\\ud800."
    error.as_dict()["message"].encode("utf-8")
