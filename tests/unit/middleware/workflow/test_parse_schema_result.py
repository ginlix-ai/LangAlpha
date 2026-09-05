"""Reading a schema'd child's reply back.

The child is instructed to answer with bare JSON, but it is a model, so the
reply routinely arrives fenced or wrapped in a sentence. Anything the child
actually got right has to survive that packaging — a discarded reply costs a
corrective re-dispatch and then a null in the script.
"""

from __future__ import annotations

import json
import sys
import tracemalloc

import pytest

from ptc_agent.agent.middleware.background_subagent.workflow.validation import (
    _MAX_REASON_CHARS,
    parse_schema_result,
)

SCHEMA = {
    "type": "object",
    "properties": {"ok": {"type": "boolean"}},
    "required": ["ok"],
}


@pytest.mark.parametrize(
    ("content", "case"),
    [
        ('{"ok": true}', "bare"),
        ('```json\n{"ok": true}\n```', "fenced with a language tag"),
        ("```\n{\"ok\": true}\n```", "fenced bare"),
        ('  \n {"ok": true}\n  ', "surrounding whitespace"),
        ('Here are the results:\n{"ok": true}', "prose before"),
        ('{"ok": true}\nThat is the summary.', "prose after"),
        ('Results [see below]:\n{"ok": true}', "a bracket in the prose before"),
        ('{"ok": true}\nSee the [docs] for more.', "a bracket in the prose after"),
        ('I found [1, 2] items:\n{"ok": true}', "valid JSON in the prose before"),
    ],
)
def test_a_schema_satisfying_reply_survives_its_packaging(
    content: str, case: str
) -> None:
    valid, parsed, error = parse_schema_result(content, SCHEMA)
    assert valid, f"{case}: {error}"
    assert parsed == {"ok": True}


def test_a_top_level_array_is_read() -> None:
    valid, parsed, _ = parse_schema_result(
        'The list:\n[1, 2, 3]', {"type": "array"}
    )
    assert valid
    assert parsed == [1, 2, 3]


@pytest.mark.parametrize(
    ("content", "case"),
    [
        ("no JSON here at all", "nothing to parse"),
        ("", "empty reply"),
        ('{"ok": "yes"}', "parses but violates the schema"),
        ('{"missing": true}', "parses but omits a required key"),
        ('{"ok": tru', "truncated mid-value"),
    ],
)
def test_a_reply_that_does_not_satisfy_the_schema_is_rejected(
    content: str, case: str
) -> None:
    valid, parsed, error = parse_schema_result(content, SCHEMA)
    assert not valid, case
    assert parsed is None
    assert error


@pytest.mark.parametrize(
    ("content", "schema", "expected"),
    [
        ("42", {"type": "number"}, 42),
        ("  42  ", {"type": "number"}, 42),
        ("```json\n42\n```", {"type": "number"}, 42),
        ('"done"', {"type": "string"}, "done"),
        ("true", {"type": "boolean"}, True),
        ("null", {"type": "null"}, None),
    ],
)
def test_a_scalar_schema_accepts_a_scalar_reply(
    content: str, schema: dict[str, object], expected: object
) -> None:
    """A scalar is a legal dispatch schema, and a scalar reply carries no
    delimiter to scan from. Scanning alone rejected every one of these, so the
    script got ``null`` no matter what the child answered."""
    valid, parsed, error = parse_schema_result(content, schema)
    assert valid, error
    assert parsed == expected


def test_a_scalar_schema_still_rejects_a_non_conforming_reply() -> None:
    valid, parsed, error = parse_schema_result("about seven", {"type": "number"})
    assert not valid
    assert parsed is None
    assert error


def test_the_candidate_that_satisfies_the_schema_wins() -> None:
    """A bracketed aside can itself be valid JSON. Whichever candidate matches
    the schema is the reply; an earlier one that merely parses is not."""
    valid, parsed, _ = parse_schema_result(
        'Checked [{"ok": "not-a-boolean"}] first.\n{"ok": true}', SCHEMA
    )
    assert valid
    assert parsed == {"ok": True}


def test_a_rejection_reports_the_reason_not_the_reply() -> None:
    """The reason travels back to the child in its retry prompt, so it carries
    the diagnosis only. ``str(ValidationError)`` pretty-prints the offending
    instance, which for a large reply is ~100KB of the child's own words — big
    enough on its own to overrun the dispatch prompt cap.
    """
    reply = json.dumps({"rows": [{"note": "n" * 200} for _ in range(400)]})

    valid, parsed, error = parse_schema_result(reply, SCHEMA)

    assert not valid
    assert parsed is None
    assert "'ok' is a required property" in error
    # The reply is already on the record as `result` / `full_result_ref`.
    assert "n" * 200 not in error
    assert len(error) < 200, f"reason echoed the reply back ({len(error)} chars)"


@pytest.mark.parametrize(
    ("schema", "reply", "expectation"),
    [
        ({"type": "integer"}, '"{}"', "is not of type 'integer'"),
        (
            {"type": "string", "maxLength": 8},
            '"{}"',
            "is too long",
        ),
        ({"enum": ["yes", "no"]}, '"{}"', "is not one of"),
    ],
    ids=["type", "maxLength", "enum"],
)
def test_a_bounded_reason_survives_a_validator_that_embeds_the_instance(
    schema: dict, reply: str, expectation: str
) -> None:
    """``required`` names the missing key, so its message is small no matter
    how big the reply is — which is why it cannot stand in for this. The
    common validators interpolate ``instance!r`` into ``.message`` itself, so
    a rejected reply comes back as its own reason unless it is clipped.

    Clipped in the middle, because the expectation lives at the tail: a child
    told only what it wrote cannot fix anything.
    """
    body = "z" * 90_000
    valid, parsed, error = parse_schema_result(reply.format(body), schema)

    assert not valid
    assert parsed is None
    assert len(error) <= _MAX_REASON_CHARS
    assert body not in error
    assert expectation in error


def test_the_candidate_scan_is_bounded_by_the_cap_not_the_reply() -> None:
    """The reply is validated before it is truncated, so a scan that collects
    every delimiter up front is sized by the child's output rather than by
    ``_MAX_JSON_CANDIDATES``.
    """
    reply = "{" * 2_000_000

    tracemalloc.start()
    try:
        valid, _, _ = parse_schema_result(reply, SCHEMA)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert not valid
    # One int per delimiter would be tens of MB; the cap admits 32 of them.
    assert peak < 8 * 1024 * 1024, f"scan allocated {peak / 1e6:.1f}MB"


def test_an_integer_past_the_digit_limit_fails_its_own_child() -> None:
    """`json` raises a bare ``ValueError`` — not ``JSONDecodeError`` — for a
    number past ``sys.get_int_max_str_digits()``. Uncaught it escapes the
    worker thread and unwinds the whole run, so one child's malformed reply
    would take every sibling with it.
    """
    reply = '{"ok": ' + "1" * (sys.get_int_max_str_digits() + 100) + "}"

    valid, parsed, error = parse_schema_result(reply, SCHEMA)

    assert not valid
    assert parsed is None
    assert error
