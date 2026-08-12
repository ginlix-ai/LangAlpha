"""Server-side validation for JavaScript workflow dispatch calls."""

from __future__ import annotations

import json
from itertools import islice
from typing import TYPE_CHECKING, Any

from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for
from referencing import Registry
from referencing.exceptions import Unresolvable

if TYPE_CHECKING:
    from src.config.models import WorkflowOrchestrationConfig


# Every JSON Schema keyword whose value is a regex. `jsonschema` compiles these
# with `re`, which backtracks — a 101-byte schema well inside every size cap can
# take minutes on a short string, and validation runs in the server process.
# Nothing a response schema legitimately needs is worth reintroducing that.
_REGEX_KEYWORDS = ("pattern", "patternProperties")
# Every keyword whose value is a URI reference. `jsonschema`'s default registry
# retrieves an unknown URI over the network, so a one-key schema of
# {"$ref": "http://…"} turns each child reply into an outbound request the
# server makes on the script's behalf, with the response echoed back in the
# validation error. A response schema only ever refers within itself.
_REF_KEYWORDS = ("$ref", "$schema", "$dynamicRef", "$recursiveRef")
# Keywords whose cost scales with the *reply*, not the schema. `uniqueItems`
# compares array members pairwise, and object members are unhashable, so
# `jsonschema` falls back to O(n²) equality: 4000 members measured ~6.5s. The
# reply is validated before it is truncated, so nothing upstream bounds n, and
# validation runs in a worker thread that a cancelled run cannot reclaim.
_INSTANCE_COST_KEYWORDS = ("uniqueItems",)
# An empty registry resolves nothing outside the schema document, so the
# retrieval path cannot be reached even if a ref keyword slips past the gate.
_NO_REMOTE_REFS = Registry()
# Prose around the JSON is tolerated (below), but each candidate start costs a
# decode attempt, so only the first few are tried.
_MAX_JSON_CANDIDATES = 32
# A reply is only ever text a model wrote, so anything the decoder refuses is
# a malformed reply like any other — and this runs in a worker thread whose
# caller unwinds the whole run on any exception it does not name. Deliberately
# `ValueError` rather than its `JSONDecodeError` subclass: a number past
# `sys.get_int_max_str_digits()` raises the bare parent, and nesting deep
# enough to exhaust the stack raises neither (`json` recurses per level on
# both the decoder and the validator).
_UNREADABLE_JSON = (ValueError, RecursionError)
# A validation reason is sized by the schema, never by the reply. `jsonschema`
# interpolates `instance!r` into `ValidationError.message` itself for the
# common validators (`type`, `enum`, `maxLength`), so a rejected 90KB reply
# yields a 90KB reason — which `compose_child_prompt` then echoes back at the
# child that just wrote it, spending the retry's context on its own output and,
# for a large enough reply, overrunning `max_prompt_chars` outright. That turns
# a mismatch the script was promised a retry for into a throw.
_MAX_REASON_CHARS = 400
_REASON_ELISION = " …[clipped]… "


class DispatchValidationError(Exception):
    """A workflow dispatch violates the server-side contract."""


def validate_dispatch(
    *,
    prompt: str,
    opts: dict[str, Any],
    known_subagent_types: list[str],
    default_subagent_type: str,
    caps: WorkflowOrchestrationConfig,
) -> dict[str, Any]:
    """Validate and normalize one JavaScript workflow dispatch.

    Shape and schema only — the per-run dispatch cap is the driver's, which is
    where the count it would be checked against lives.
    """
    if not isinstance(opts, dict):
        raise DispatchValidationError("opts must be a plain object")
    if not isinstance(prompt, str) or not prompt:
        raise DispatchValidationError("prompt must be a non-empty string")

    for key in ("model", "effort", "isolation"):
        if key in opts:
            raise DispatchValidationError(
                f"opts.{key} is not supported in this environment"
            )

    subagent_type = opts.get("agentType", default_subagent_type)
    if subagent_type not in known_subagent_types:
        available = ", ".join(sorted(known_subagent_types))
        raise DispatchValidationError(
            f"opts.agentType '{subagent_type}' is unknown. Available: {available}"
        )

    label = opts.get("label")
    normalized_label = str(label)[:120] if label is not None else None
    phase = opts.get("phase")
    normalized_phase = str(phase)[:120] if phase is not None else None
    schema = opts.get("schema")
    if schema is not None:
        _validate_schema(schema, caps)

    effective_prompt = compose_child_prompt(prompt, schema)
    check_prompt_cap(effective_prompt, caps)

    return {
        "subagent_type": subagent_type,
        "prompt": effective_prompt,
        "label": normalized_label,
        "phase": normalized_phase,
        "schema": schema,
    }


def compose_child_prompt(
    prompt: str,
    schema: dict[str, Any] | None,
    *,
    validation_error: str | None = None,
) -> str:
    """The text a child is actually sent.

    One composer for the first dispatch and the corrective retry, so the cap
    below governs what goes out rather than the caller's fragment of it.
    """
    parts = [prompt]
    if validation_error:
        parts.append(f"Your previous response failed validation: {validation_error}")
    if schema is not None:
        parts.append(schema_instruction(schema))
    return "\n\n".join(parts)


def check_prompt_cap(prompt: str, caps: WorkflowOrchestrationConfig) -> None:
    """Admit a composed prompt against the operator's cap.

    Refuses rather than clips: a silently shortened prompt drops instructions
    the script believed it sent, and the child's answer then reflects a task
    nobody can reconstruct from the run record.
    """
    if len(prompt) > caps.max_prompt_chars:
        # Measures the composed text, so the count can exceed what the script
        # passed — a schema's response-format contract rides along with it.
        raise DispatchValidationError(
            f"prompt is {len(prompt)} chars as sent; max_prompt_chars cap is "
            f"{caps.max_prompt_chars}"
        )


def schema_instruction(schema: dict[str, Any]) -> str:
    """The response-format contract appended to a schema'd child's prompt."""
    rendered = json.dumps(schema, ensure_ascii=False)
    return (
        "--- RESPONSE FORMAT REQUIREMENT ---\n"
        "Reply with ONLY a JSON value valid against this JSON Schema. "
        "Do not include prose or markdown fences.\n"
        f"{rendered}\n"
        "--- END RESPONSE FORMAT REQUIREMENT ---"
    )


def _clip_reason(text: str) -> str:
    """Bound a validation reason without discarding what it was measured against.

    Clips the middle rather than the tail: every validator that embeds the
    instance puts it *first* (``<instance> is not of type 'integer'``), so a
    tail clip would keep only the reply and drop the actual expectation.
    """
    if len(text) <= _MAX_REASON_CHARS:
        return text
    keep = (_MAX_REASON_CHARS - len(_REASON_ELISION)) // 2
    return f"{text[:keep]}{_REASON_ELISION}{text[-keep:]}"


def parse_schema_result(
    content: str, schema: dict[str, Any]
) -> tuple[bool, Any, str | None]:
    """Read a child's reply back against its schema, tolerating fences and
    surrounding prose the instruction above asked it not to write."""
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    decoder = json.JSONDecoder()
    first_error: str | None = None

    def matches(instance: Any) -> str | None:
        """None if ``instance`` satisfies the schema, else the reason."""
        try:
            validator_for(schema)(schema, registry=_NO_REMOTE_REFS).validate(instance)
        except jsonschema_exceptions.ValidationError as error:
            # `str(error)` pretty-prints the failing instance *and* the schema;
            # `.message` still interpolates the instance for most validators.
            # Neither is bounded by anything we control, so the reason is
            # clipped before it can become a retry prompt. The instance is
            # already on the record as `result` and `full_result_ref`.
            location = error.json_path
            reason = _clip_reason(error.message)
            if location and location != "$":
                return f"{reason} (at {location})"
            return reason
        except RecursionError:
            return "response nests too deeply to validate"
        except Unresolvable as error:
            # A ref the gate did not catch: unresolvable rather than fetched.
            # Reported like a mismatch so one bad schema fails its own child
            # instead of escaping to_thread and unwinding the whole run.
            return f"opts.schema has an unresolvable reference: {error}"
        return None

    # The whole text first. A scalar schema is a legal dispatch schema, and a
    # scalar reply ("42", "true") carries no delimiter, so the scan below can
    # never see one — without this every such child resolves to null forever.
    # A decode failure here just means the reply is wrapped in something; only
    # a schema mismatch is worth reporting over what the scan finds.
    try:
        whole = json.loads(text)
    except _UNREADABLE_JSON:
        pass
    else:
        reason = matches(whole)
        if reason is None:
            return True, whole, None
        first_error = reason

    # Decoding from a delimiter rather than slicing to the last one keeps a
    # stray bracket in the surrounding prose from swallowing the real value.
    # The first candidate that also satisfies the schema wins, so a bracketed
    # aside that happens to be valid JSON cannot shadow the actual reply.
    # Lazily: the reply is validated before it is truncated, so materializing
    # every delimiter offset would size an allocation by the reply rather than
    # by the cap that is supposed to bound this scan.
    starts = (index for index, char in enumerate(text) if char in "{[")
    for index in islice(starts, _MAX_JSON_CANDIDATES):
        try:
            parsed, _ = decoder.raw_decode(text, index)
        except _UNREADABLE_JSON as error:
            first_error = first_error or str(error) or type(error).__name__
            continue
        reason = matches(parsed)
        if reason is None:
            return True, parsed, None
        first_error = first_error or reason
    return False, None, first_error or "response does not contain JSON matching the schema"


def _validate_schema(schema: Any, caps: WorkflowOrchestrationConfig) -> None:
    if not isinstance(schema, dict):
        raise DispatchValidationError("opts.schema must be an object")
    try:
        serialized = json.dumps(schema)
    except (TypeError, ValueError) as error:
        raise DispatchValidationError(
            f"opts.schema must be JSON-serializable: {error}"
        ) from error
    if len(serialized) > caps.schema_max_bytes:
        raise DispatchValidationError(
            f"opts.schema serialized size exceeds schema_max_bytes cap "
            f"{caps.schema_max_bytes}"
        )
    _check_schema_shape(schema, caps, depth=1)
    try:
        validator_for(schema).check_schema(schema)
    except jsonschema_exceptions.SchemaError as error:
        raise DispatchValidationError(f"opts.schema is not a valid JSON Schema: {error.message}") from error


def _check_schema_shape(
    value: Any,
    caps: WorkflowOrchestrationConfig,
    *,
    depth: int,
) -> None:
    if isinstance(value, (dict, list)) and depth > caps.schema_max_depth:
        raise DispatchValidationError(
            f"opts.schema nesting depth exceeds schema_max_depth cap "
            f"{caps.schema_max_depth}"
        )
    if isinstance(value, dict):
        if len(value) > caps.schema_max_properties:
            raise DispatchValidationError(
                f"opts.schema object has {len(value)} keys; schema_max_properties cap "
                f"is {caps.schema_max_properties}"
            )
        for keyword in _REGEX_KEYWORDS:
            if keyword in value:
                raise DispatchValidationError(
                    f"opts.schema must not use '{keyword}'. Constrain the shape with "
                    "type/enum/required instead, and check string formatting in the "
                    "workflow script."
                )
        for keyword in _INSTANCE_COST_KEYWORDS:
            # Truthiness, not presence: `uniqueItems: false` is the default and
            # costs nothing, so refusing it would only confuse.
            if value.get(keyword):
                raise DispatchValidationError(
                    f"opts.schema must not use '{keyword}'. Validating it is "
                    "quadratic in the length of the child's reply; check "
                    "uniqueness in the workflow script instead."
                )
        for keyword in _REF_KEYWORDS:
            reference = value.get(keyword)
            if isinstance(reference, str) and not reference.startswith("#"):
                raise DispatchValidationError(
                    f"opts.schema '{keyword}' must be a local fragment starting with "
                    "'#'. A response schema cannot reference another document."
                )
        for child in value.values():
            if isinstance(child, (dict, list)):
                _check_schema_shape(child, caps, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            if isinstance(child, (dict, list)):
                _check_schema_shape(child, caps, depth=depth + 1)
