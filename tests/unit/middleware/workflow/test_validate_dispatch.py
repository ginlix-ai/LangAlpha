from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from ptc_agent.agent.middleware.background_subagent.workflow.validation import (
    DispatchValidationError,
    compose_child_prompt,
    parse_schema_result,
    validate_dispatch,
)
from src.config.models import WorkflowOrchestrationConfig


def _validate(
    *,
    prompt: str = "research this",
    opts: dict[str, Any] | None = None,
    known: list[str] | None = None,
    default: str = "general",
    caps: WorkflowOrchestrationConfig | None = None,
) -> dict[str, Any]:
    return validate_dispatch(
        prompt=prompt,
        opts=opts or {},
        known_subagent_types=known or ["general", "research"],
        default_subagent_type=default,
        caps=caps or WorkflowOrchestrationConfig(),
    )


def test_defaults_and_unknown_options_are_ignored() -> None:
    assert _validate(opts={"futureOption": True}) == {
        "subagent_type": "general",
        "prompt": "research this",
        "label": None,
        "phase": None,
        "schema": None,
    }


@pytest.mark.parametrize("prompt", ["", None, 7])
def test_prompt_must_be_a_non_empty_string(prompt: Any) -> None:
    with pytest.raises(DispatchValidationError, match="prompt"):
        _validate(prompt=prompt)


def test_prompt_length_cap_is_enforced() -> None:
    caps = WorkflowOrchestrationConfig(max_prompt_chars=3)
    with pytest.raises(DispatchValidationError, match="max_prompt_chars cap is 3"):
        _validate(prompt="four", caps=caps)


@pytest.mark.parametrize("key", ["model", "effort", "isolation"])
def test_unsupported_options_are_rejected(key: str) -> None:
    with pytest.raises(
        DispatchValidationError,
        match=rf"opts\.{key} is not supported in this environment",
    ):
        _validate(opts={key: "value"})


def test_agent_type_is_selected_and_validated() -> None:
    assert _validate(opts={"agentType": "research"})["subagent_type"] == "research"
    with pytest.raises(DispatchValidationError, match="Available: alpha, zeta"):
        _validate(opts={"agentType": "missing"}, known=["zeta", "alpha"])


def test_label_and_phase_are_coerced_and_clipped() -> None:
    result = _validate(opts={"label": 42, "phase": "p" * 130})
    assert result["label"] == "42"
    assert result["phase"] == "p" * 120


@pytest.mark.parametrize("opts", [[], "opts", 7, None])
def test_non_object_opts_is_a_dispatch_error(opts: Any) -> None:
    """``opts`` arrives from JavaScript, so its shape is the script's choice.
    Anything but a mapping has to fail as a dispatch error the script can
    catch — an AttributeError here would escape as a server exception."""
    with pytest.raises(DispatchValidationError, match="plain object"):
        validate_dispatch(
            prompt="research this",
            opts=opts,
            known_subagent_types=["general"],
            default_subagent_type="general",
            caps=WorkflowOrchestrationConfig(),
        )


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "string", "pattern": "^(([a-z])+.)+[A-Z]([a-z])+$"},
        {
            "type": "object",
            "properties": {"s": {"type": "string", "pattern": "^(a+)+$"}},
        },
        {"type": "object", "patternProperties": {"^(a+)+$": {"type": "string"}}},
    ],
)
def test_regex_schema_keywords_are_rejected_at_any_depth(
    schema: dict[str, Any],
) -> None:
    """`re` backtracks, so a schema well inside every size cap can burn minutes
    of server CPU validating a short reply. The keywords are refused outright
    rather than bounded."""
    with pytest.raises(DispatchValidationError, match="must not use"):
        _validate(opts={"schema": schema})


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "array", "uniqueItems": True},
        {
            "type": "object",
            "properties": {"rows": {"type": "array", "uniqueItems": True}},
        },
    ],
)
def test_uniqueitems_is_rejected_at_any_depth(schema: dict[str, Any]) -> None:
    """Unhashable members make `jsonschema` compare pairwise, so the cost is
    quadratic in the *reply* — which nothing bounds, since validation runs
    before truncation and on a thread a cancelled run cannot reclaim."""
    with pytest.raises(DispatchValidationError, match="quadratic"):
        _validate(opts={"schema": schema})


def test_uniqueitems_false_is_admitted() -> None:
    """The default costs nothing; refusing it would only confuse."""
    _validate(opts={"schema": {"type": "array", "uniqueItems": False}})


def test_schema_must_be_an_object() -> None:
    with pytest.raises(DispatchValidationError, match="opts.schema must be an object"):
        _validate(opts={"schema": []})


def test_schema_serialized_size_cap_is_enforced() -> None:
    caps = WorkflowOrchestrationConfig(schema_max_bytes=256)
    schema = {"type": "string", "description": "x" * 300}
    with pytest.raises(DispatchValidationError, match="schema_max_bytes cap 256"):
        _validate(opts={"schema": schema}, caps=caps)


def test_schema_depth_cap_is_enforced() -> None:
    caps = WorkflowOrchestrationConfig(schema_max_depth=2)
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    with pytest.raises(DispatchValidationError, match="schema_max_depth cap 2"):
        _validate(opts={"schema": schema}, caps=caps)


def test_schema_property_cap_is_enforced_at_any_level() -> None:
    caps = WorkflowOrchestrationConfig(schema_max_properties=2)
    schema = {"type": "object", "properties": {}, "required": []}
    with pytest.raises(DispatchValidationError, match="schema_max_properties cap is 2"):
        _validate(opts={"schema": schema}, caps=caps)


def test_invalid_json_schema_is_rejected() -> None:
    with pytest.raises(DispatchValidationError, match="valid JSON Schema"):
        _validate(opts={"schema": {"type": "not-a-real-type"}})


def test_valid_schema_is_returned_unchanged() -> None:
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
    }
    assert _validate(opts={"schema": schema})["schema"] is schema


def test_new_config_field_bounds_are_active() -> None:
    """Only the floors — the defaults themselves are tunable, and pinning them
    reddens the suite on a capacity retune that breaks nothing."""
    with pytest.raises(ValidationError):
        WorkflowOrchestrationConfig(memory_limit_mb=15)
    with pytest.raises(ValidationError):
        WorkflowOrchestrationConfig(max_dispatches_per_run=0)


def test_the_cap_governs_the_prompt_that_is_actually_sent() -> None:
    """A schema adds its response-format contract to every dispatch, so a base
    prompt that fits can still put the real prompt over the operator's cap."""
    schema = {"type": "object", "properties": {"ok": {"type": "boolean"}}}
    base = "p" * 150
    caps = WorkflowOrchestrationConfig(max_prompt_chars=200)

    # Fits on its own — and would have passed when only the base was checked.
    assert len(base) <= caps.max_prompt_chars
    with pytest.raises(DispatchValidationError, match="max_prompt_chars"):
        _validate(prompt=base, opts={"schema": schema}, caps=caps)


def test_the_returned_prompt_is_the_composed_one() -> None:
    rec = _validate(
        prompt="summarize it",
        opts={"schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}},
    )
    assert rec["prompt"].startswith("summarize it")
    assert "RESPONSE FORMAT REQUIREMENT" in rec["prompt"]


def test_a_retry_composes_longer_than_the_dispatch_it_corrects() -> None:
    """Why the retry gets its own cap check: it carries everything the first
    dispatch did, plus the reason the first one failed."""
    schema = {"type": "object", "properties": {"ok": {"type": "boolean"}}}
    first = compose_child_prompt("do it", schema)
    retry = compose_child_prompt(
        "do it", schema, validation_error="'ok' is a required property"
    )

    assert retry.startswith("do it")
    assert "'ok' is a required property" in retry
    assert "RESPONSE FORMAT REQUIREMENT" in retry
    assert len(retry) > len(first)


def test_a_reply_too_deep_to_decode_fails_its_own_child() -> None:
    """`parse_schema_result` runs in a worker thread whose caller unwinds the
    whole run on any exception it does not name, so an unreadable reply has to
    come back as a mismatch. Nesting deep enough to exhaust the interpreter
    stack is unreadable like any other malformed reply.
    """
    deep = "[" * 20_000 + "1" + "]" * 20_000
    valid, parsed, error = parse_schema_result(deep, {"type": "object"})
    assert valid is False
    assert parsed is None
    assert error

    shallow = "[" * 100 + "1" + "]" * 100
    assert parse_schema_result(shallow, {"type": "object"})[0] is False
