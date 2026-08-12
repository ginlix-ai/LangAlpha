"""Backend↔frontend type-parity guard for chart annotations.

The chart-annotation data shape is defined TWICE, by hand:

- Backend (source of truth): the pydantic discriminated union in
  ``src/tools/chart_annotation/schemas.py`` (``Annotation`` = union of
  PriceLineAnnotation, TrendlineAnnotation, MarkerAnnotation,
  VerticalLineAnnotation, RectangleAnnotation, TextAnnotation,
  EventAnnotation, FibRetracementAnnotation; discriminator ``type``).
- Frontend (hand-mirrored): the ``StoredAnnotation`` / ``ChartInstance``
  types in
  ``web/src/pages/MarketView/stores/chartAnnotationStore.ts``.

These drift independently — a backend field change won't fail any test
until something breaks at runtime in the browser. This test pins an
explicit, committed expectation of the backend contract (the exact set of
discriminator ``type`` values, and per-variant the field names, which are
required, and their JSON types). Any backend annotation-shape change makes
this fail loudly, forcing a conscious update.

WHEN THIS TEST FAILS: you changed the pydantic annotation schema. Update
``EXPECTED_ANNOTATION_CONTRACT`` below to match AND update the hand-mirrored
TypeScript types in
``web/src/pages/MarketView/stores/chartAnnotationStore.ts`` in lockstep
(the ``StoredAnnotation`` union and its member interfaces), or the frontend
will silently disagree with the wire format the agent emits.

The expectation is encoded against the JSON schema (the actual wire shape),
so ``float`` reads as ``number``, ``str | None`` as ``string|null``,
``Literal[...]`` enums as ``string``, and nested models (``TimePricePoint``)
as ``object`` — matching how the frontend declares the same fields.
"""

from typing import get_args

from pydantic import TypeAdapter

from src.tools.chart_annotation.schemas import Annotation

# ---------------------------------------------------------------------------
# Committed expectation of the backend contract.
#
# Shape: { discriminator_type: { field_name: (json_type, required) } }
#   json_type ∈ {"string", "number", "string|null", "object"}
#   required  ∈ {True, False}
#
# `object` denotes a nested TimePricePoint anchor ({time: string, price:
# number}); its inner shape is asserted separately in
# EXPECTED_TIME_PRICE_POINT below.
# ---------------------------------------------------------------------------
EXPECTED_ANNOTATION_CONTRACT: dict[str, dict[str, tuple[str, bool]]] = {
    "price_line": {
        "type": ("string", True),
        "price": ("number", True),
        "label": ("string|null", False),
        "color": ("string|null", False),
        "style": ("string", False),
    },
    "trendline": {
        "type": ("string", True),
        "point1": ("object", True),
        "point2": ("object", True),
        "label": ("string|null", False),
        "color": ("string|null", False),
    },
    "marker": {
        "type": ("string", True),
        "time": ("string", True),
        "shape": ("string", True),
        "position": ("string", False),
        "text": ("string|null", False),
        "color": ("string|null", False),
    },
    "vertical_line": {
        "type": ("string", True),
        "time": ("string", True),
        "label": ("string|null", False),
        "color": ("string|null", False),
        "style": ("string", False),
    },
    "rectangle": {
        "type": ("string", True),
        "point1": ("object", True),
        "point2": ("object", True),
        "label": ("string|null", False),
        "color": ("string|null", False),
    },
    "text": {
        "type": ("string", True),
        "time": ("string", True),
        "price": ("number", True),
        "text": ("string", True),
        "color": ("string|null", False),
    },
    "event": {
        "type": ("string", True),
        "time": ("string", True),
        "price": ("number", True),
        "title": ("string", True),
        "detail": ("string", True),
        "color": ("string|null", False),
    },
    "fib_retracement": {
        "type": ("string", True),
        "point1": ("object", True),
        "point2": ("object", True),
        "label": ("string|null", False),
        "color": ("string|null", False),
    },
}

# Inner shape of the (time, price) anchor shared by trendline / rectangle /
# fib_retracement. Mirrors the frontend `TimePricePoint` interface.
EXPECTED_TIME_PRICE_POINT: dict[str, tuple[str, bool]] = {
    "time": ("string", True),
    "price": ("number", True),
}


def _json_type_token(field_schema: dict) -> str:
    """Collapse one JSON-schema property into a parity token.

    Maps the actual wire shape (not the python annotation) so the
    expectation reads the way the frontend declares the same field:
      - ``{"type": "string"}`` (incl. const/enum strings) → ``"string"``
      - ``{"type": "number"}`` → ``"number"``
      - ``{"anyOf": [{"type": "string"}, {"type": "null"}]}`` → ``"string|null"``
      - ``{"$ref": ".../TimePricePoint"}`` (nested model) → ``"object"``
    """
    if "$ref" in field_schema:
        return "object"

    if "anyOf" in field_schema:
        inner = {
            opt.get("type")
            for opt in field_schema["anyOf"]
        }
        if inner == {"string", "null"}:
            return "string|null"
        if inner == {"number", "null"}:
            return "number|null"
        # Surface any unexpected optional combination explicitly so it can't
        # silently pass as a known token.
        return "anyOf:" + "|".join(sorted(t or "?" for t in inner))

    # const (single-value Literal) and enum (multi-value Literal) both carry
    # an explicit "type" — for annotations that is always "string".
    return field_schema.get("type", "unknown")


def _variant_schemas() -> dict[str, dict]:
    """Resolve each union member's full JSON schema, keyed by discriminator.

    Walks the discriminated-union JSON schema: the ``discriminator.mapping``
    maps each ``type`` value to a ``$ref`` into ``$defs``.
    """
    schema = TypeAdapter(Annotation).json_schema()
    defs = schema["$defs"]
    mapping = schema["discriminator"]["mapping"]
    resolved: dict[str, dict] = {}
    for type_value, ref in mapping.items():
        def_name = ref.split("/")[-1]
        resolved[type_value] = defs[def_name]
    return resolved


def _contract_from_schema(variant_schema: dict) -> dict[str, tuple[str, bool]]:
    """Reduce one variant's JSON schema to {field: (json_type, required)}."""
    required = set(variant_schema.get("required", []))
    props = variant_schema["properties"]
    return {
        name: (_json_type_token(prop), name in required)
        for name, prop in props.items()
    }


def test_discriminator_type_values_match_expectation():
    """The exact set of `type` discriminator values is pinned."""
    schema = TypeAdapter(Annotation).json_schema()
    assert schema["discriminator"]["propertyName"] == "type"
    live_types = set(schema["discriminator"]["mapping"])
    assert live_types == set(EXPECTED_ANNOTATION_CONTRACT), (
        "Annotation discriminator `type` values drifted. Update "
        "EXPECTED_ANNOTATION_CONTRACT and the frontend StoredAnnotation "
        "union (chartAnnotationStore.ts) in lockstep."
    )


def test_union_member_count_matches_discriminator():
    """No union member can be added without a discriminator entry."""
    union, _field = get_args(Annotation)
    members = get_args(union)
    assert len(members) == len(EXPECTED_ANNOTATION_CONTRACT)


def test_each_variant_field_contract_matches_expectation():
    """Per-variant: field names, required flags, and JSON types are pinned.

    This is the core drift guard — any added/removed/renamed field, any
    required↔optional flip, or any type change on the backend fails here.
    """
    variant_schemas = _variant_schemas()
    for type_value, expected_fields in EXPECTED_ANNOTATION_CONTRACT.items():
        live = _contract_from_schema(variant_schemas[type_value])
        assert live == expected_fields, (
            f"Annotation variant '{type_value}' drifted from the committed "
            f"contract. Update EXPECTED_ANNOTATION_CONTRACT and the matching "
            f"frontend interface in chartAnnotationStore.ts.\n"
            f"  expected: {expected_fields}\n"
            f"  actual:   {live}"
        )


def test_time_price_point_shape_matches_expectation():
    """The shared (time, price) anchor shape is pinned.

    `object`-typed fields (point1/point2) reference this nested model; pin it
    explicitly so a change to the anchor shape can't slip past the `object`
    token.
    """
    schema = TypeAdapter(Annotation).json_schema()
    tpp = schema["$defs"]["TimePricePoint"]
    assert _contract_from_schema(tpp) == EXPECTED_TIME_PRICE_POINT, (
        "TimePricePoint anchor shape drifted. Update "
        "EXPECTED_TIME_PRICE_POINT and the frontend TimePricePoint "
        "interface in chartAnnotationStore.ts."
    )
