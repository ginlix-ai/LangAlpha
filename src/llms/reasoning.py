"""Unified reasoning effort mapper.

Translates a level the manifest has **already guaranteed** the model accepts into
that provider's native parameter. Each entry names its own surface in its
``reasoning`` block, so the mapping is a declaration rather than a guess about
which key the entry happened to carry.

Nothing is clamped here. A level outside the model's declared
``reasoning_efforts`` is resolved upstream by ``clamp_reasoning_effort`` in
``llm.py``, the only place that can see the enum, which steps down to the nearest
level the model does offer. By the time a request reaches this function the level
is known-good, so a lossy fallback here would only hide a bug.
"""

from typing import Literal, get_args

#: Canonical ordered vocabulary. Ordering is meaningful: the UI renders the
#: model's declared subset in this order, and it matches langchain's upstream
#: ``ModelProfile`` levels so the two stay comparable.
#:
#: Declared as a ``Literal`` first so the request models can annotate against the
#: same vocabulary rather than restating it. A hand-copied list drifted once
#: already: the API rejected ``none``/``minimal``/``max`` for every model whose
#: manifest entry offered them, including ones whose own default was ``max``.
ReasoningLevel = Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"]

REASONING_LEVELS: tuple[ReasoningLevel, ...] = get_args(ReasoningLevel)

#: Levels that mean "do not think". Every binary surface keys off this rather
#: than off ``low``, which is a real thinking level everywhere that grades.
OFF_LEVELS = frozenset({"none"})

#: Paths a ``write`` may target: graded dials that take a level name verbatim.
#: Closed on purpose. A dotted string makes a typo look structurally valid, and
#: a write to a misspelled path lands somewhere the vendor ignores and returns
#: 200 for, which is the exact silent failure this block exists to remove.
#:
#: Ordered, not a set: :func:`infer_surface` takes the first seed it finds, so
#: an entry seeded on two dials resolves to the ``parameters`` lane, where a
#: typed SDK field lives, rather than to whichever path sorts first.
WRITE_PATHS = (
    "parameters.reasoning.effort",
    "parameters.output_config.effort",
    "parameters.thinking_level",
    "parameters.reasoning_effort",
    "extra_body.reasoning.effort",
    "extra_body.reasoning_effort",
)

#: Paths an ``on``/``off`` patch may target: mode switches, which take a vendor
#: literal rather than a level. Disjoint from :data:`WRITE_PATHS` on purpose --
#: a patch that could name a dial is how an ``off`` block ends up restating the
#: entry's own graded write, which then contradicts the switch beside it.
PATCH_PATHS = frozenset(
    {
        "parameters.thinking.type",
        "extra_body.thinking.type",
        "extra_body.thinking.clear_thinking",
    }
)


#: The keys that say *where* a level goes, as opposed to which levels exist.
#: Beside the allowlists they partition, because a key added to the mapper and
#: not to this tuple is dropped by ``_checked_surface`` without a word.
SURFACE_KEYS = ("write", "on", "off")


class ReasoningSurfaceError(ValueError):
    """A ``reasoning`` block names a path outside the allowlists."""


def validate_surface(name: str, surface: dict) -> None:
    """Reject a block that cannot do what its ladder advertises.

    Runs where the author is still holding it: the manifest suite builds every
    entry, and the preferences endpoint checks a custom one on save. Past here
    a bad path is a 200 the vendor ignores, and a wrongly typed one is an
    exception raised on every turn from data that was accepted once.
    """
    for key, expected in (("write", str), ("on", dict), ("off", dict), ("efforts", list)):
        value = surface.get(key)
        if value is not None and not isinstance(value, expected):
            raise ReasoningSurfaceError(
                f"{name}: reasoning.{key} must be a {expected.__name__}, "
                f"got {type(value).__name__}"
            )
    write = surface.get("write")
    if write is not None and write not in WRITE_PATHS:
        raise ReasoningSurfaceError(
            f"{name}: reasoning.write={write!r} is not a known write path"
        )
    for key in ("on", "off"):
        for path in surface.get(key) or {}:
            if path not in PATCH_PATHS:
                raise ReasoningSurfaceError(
                    f"{name}: reasoning.{key} path {path!r} is not a known patch path"
                )
    # The rest is the block checked against its own ladder. Only a full block
    # carries one -- an inferred surface has no efforts and no rung to be wrong
    # about -- and a surface with no levels behind it writes nothing anyway.
    efforts = surface.get("efforts") or ()
    if not efforts:
        return
    # Before any set operation: the levels arrive from a stored preferences bag,
    # and one unhashable element would raise out of the intersection below,
    # turning a save this function exists to answer with a 400 into a 500.
    unknown = [lv for lv in efforts if not isinstance(lv, str) or lv not in REASONING_LEVELS]
    if unknown:
        raise ReasoningSurfaceError(
            f"{name}: reasoning.efforts must be drawn from {list(REASONING_LEVELS)}, "
            f"got {unknown!r}"
        )
    if write is None and not (surface.get("on") or surface.get("off")):
        raise ReasoningSurfaceError(
            f"{name}: reasoning declares efforts with nowhere to write them"
        )
    # A patch is one payload, so a surface with no graded write can tell exactly
    # two states apart. More rungs than that is the lie the block exists to
    # remove: buttons the UI renders as distinct that emit the same request.
    on_rungs = [lv for lv in efforts if lv not in OFF_LEVELS]
    if write is None and len(on_rungs) > 1:
        raise ReasoningSurfaceError(
            f"{name}: reasoning declares no `write`, so {on_rungs} all apply the "
            f"same `on` patch and reach the provider identically"
        )
    off_rungs = sorted(OFF_LEVELS.intersection(efforts))
    if surface.get("on") and not surface.get("off") and off_rungs:
        raise ReasoningSurfaceError(
            f"{name}: reasoning offers {off_rungs} but declares no `off`, so the "
            f"off rung would apply `on` and turn thinking on"
        )
    if surface.get("off") and not off_rungs:
        raise ReasoningSurfaceError(
            f"{name}: reasoning declares `off` but no level in efforts means off, "
            f"so the patch can never be applied"
        )


def infer_surface(parameters: dict | None, extra_body: dict | None) -> dict:
    """Guess the surface of a user-supplied entry that declared none.

    Only for BYOK entries. A manifest row states its surface outright; a user
    pasting an OpenAI-compatible config cannot be asked to name a write path, and
    entries stored before the block existed carry only the seed. Graded dials
    only — a mode switch or a token budget has to be declared, because there is
    no seed value that distinguishes one from a dial's starting point.
    """
    lanes = {"parameters": parameters or {}, "extra_body": extra_body or {}}
    for path in WRITE_PATHS:
        lane, *rest = path.split(".")
        node = lanes[lane]
        for segment in rest[:-1]:
            node = node.get(segment) if isinstance(node, dict) else None
        if isinstance(node, dict) and rest[-1] in node:
            return {"write": path}
    return {}


def _put(lanes: dict[str, dict], path: str, value) -> None:
    lane, *rest = path.split(".")
    node = lanes[lane]
    for segment in rest[:-1]:
        child = node.get(segment)
        if not isinstance(child, dict):
            child = {}
            node[segment] = child
        node = child
    node[rest[-1]] = value


def apply_reasoning_effort(
    level: str,
    parameters: dict,
    extra_body: dict,
    surface: dict | None = None,
) -> tuple[dict, dict]:
    """Apply a reasoning effort level to a model's request parameters.

    ``off`` replaces the graded write rather than layering over it: on a surface
    carrying both a switch and a dial, only the switch reliably means off, and
    the two would otherwise contradict each other in the same payload.

    Args:
        level: A level from :data:`REASONING_LEVELS`, already validated against
            the model's declared ``reasoning_efforts``.
        parameters: Model parameters dict (mutated in place).
        extra_body: Extra body dict (mutated in place).
        surface: The write half of the model's ``reasoning`` block. Absent means
            the model offers no effort control, and nothing is written.

    Returns:
        Tuple of (parameters, extra_body) — the same objects, mutated.
    """
    if not surface or level not in REASONING_LEVELS:
        return parameters, extra_body

    lanes = {"parameters": parameters, "extra_body": extra_body}
    off_patch = surface.get("off")

    if level in OFF_LEVELS and off_patch:
        # The graded write goes first, wherever it came from -- the entry's own
        # seed or a caller override merged in above. Leaving it is the payload
        # this branch exists to prevent: a live effort beside the instruction
        # not to think. Only that one key, since its container is the entry's
        # own transport config.
        write = surface.get("write")
        if write:
            lane, *rest = write.split(".")
            node = lanes[lane]
            for segment in rest[:-1]:
                node = node.get(segment) if isinstance(node, dict) else None
            if isinstance(node, dict):
                node.pop(rest[-1], None)
        # Each container the patch touches is reset, not merged into: a mode
        # switch sits in a discriminated union whose disabled variant rejects
        # the siblings the enabled one requires, so a caller-supplied
        # `budget_tokens` must not survive next to it. Reset in its own pass
        # first, or two paths sharing a container would wipe each other's write.
        for path in off_patch:
            lane, *rest = path.split(".")
            if len(rest) > 1:
                lanes[lane][rest[0]] = {}
        for path, value in off_patch.items():
            _put(lanes, path, value)
        return parameters, extra_body

    for path, value in (surface.get("on") or {}).items():
        _put(lanes, path, value)
    if write := surface.get("write"):
        _put(lanes, write, level)

    return parameters, extra_body
