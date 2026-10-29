"""What a subagent save may hold, checked the same way for both writers.

``validate_fields`` is the one gate the pane's REST route and the agent's file
route share, so a value one refuses the other refuses too. Rules that belong
to one writer stay out of it and are exposed for that writer to apply: the
file route ignores read-only keys rather than refusing them.

Values arrive in either writer's spelling (YAML reads ``on`` as true) and leave
in one canonical form: ``None`` clears the user's value, ``INHERIT`` chooses
the thread's (or the user's) value explicitly, anything else is the value.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from ptc_agent.agent.roles import ALL_ROLES
from ptc_agent.agent.subagents.merge import BROKERAGE_ACCESS, TOOL_WORDS
from ptc_agent.core.paths import SUBAGENT_NAME

from .frontmatter import UNSAVABLE

if TYPE_CHECKING:
    from .context import SubagentContext

NAME_RE = re.compile(SUBAGENT_NAME)
RESERVED_NAMES = frozenset({"main"})
MAX_CUSTOM = 20
DESCRIPTION_MAX = 400
INSTRUCTIONS_MAX_BYTES = 16 * 1024

#: Explicit "same as the thread's" for a model or effort, and "same as yours"
#: for search, as distinct from clearing the key back to the base's value.
INHERIT = "inherit"

#: Keys any subagent's row may set.
EDITABLE_KEYS = (
    "enabled",
    "model",
    "effort",
    "tools",
    "search_provider",
    "search_depth",
    "connectors",
    "brokerages",
    "memory",
    "preferred_connectors",
    "preferred_skills",
)
#: Keys only a custom subagent sets; a base's are read-only.
CUSTOM_KEYS = ("description", "instructions", "available_to")
WRITABLE_KEYS = EDITABLE_KEYS + CUSTOM_KEYS
INHERITABLE_KEYS = frozenset({"model", "effort", "search_provider", "search_depth"})
ROLE_ORDER = tuple(sorted(ALL_ROLES))

NUL_MESSAGE = "Remove the NUL character (\\x00). Saved text can't hold one."
SURROGATE_MESSAGE = "Remove the unpaired surrogate (\\ud800 to \\udfff). Saved text can't hold one."

Kind = Literal["custom", "override"]


@dataclass(frozen=True)
class FieldError:
    field: str
    message: str
    options: list[str] | None = None

    def __post_init__(self) -> None:
        # A message may echo what was sent (an unknown model, a key), and a lone
        # surrogate there would fail the response's UTF-8 encoding.
        for name in ("field", "message"):
            value = getattr(self, name)
            if isinstance(value, str):
                object.__setattr__(self, name, value.encode("utf-8", "backslashreplace").decode("utf-8"))

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"field": self.field, "message": self.message}
        if self.options is not None:
            out["options"] = self.options
        return out


class SubagentInvalid(Exception):
    """A save refused, with every field that refused it."""

    def __init__(self, errors: list[FieldError]):
        super().__init__("; ".join(f"{e.field}: {e.message}" for e in errors))
        self.errors = errors


class _Bad(ValueError):
    def __init__(self, message: str, options: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.options = options


# Each switch's words as the README and the rendered file spell them. A
# quoted on or off still reads for any of them, as it always has.
_SWITCH_WORDS = {"enabled": ("true", "false"), "connectors": ("on", "off"), "memory": ("on", "off")}


def _switch(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    on, off = _SWITCH_WORDS[key]
    if value in (on, "on"):
        return True
    if value in (off, "off"):
        return False
    raise _Bad(f"Must be {on} or {off}.", [on, off])


def _names(value: Any, what: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise _Bad(f"Must be a list of {what} names.")
    return list(dict.fromkeys(v.strip() for v in value if v.strip()))


def normalize_value(key: str, value: Any) -> Any:
    """``value`` in canonical form for ``key``, or raises with a user-facing message.

    Shape only: whether a model, skill or connector exists is the context's
    question, asked in ``validate_fields``.
    """
    if value is None:
        return None
    if key in INHERITABLE_KEYS:
        if not isinstance(value, str) or not value.strip():
            raise _Bad("Must be a name, or inherit.")
        value = value.strip()
        return INHERIT if value.lower() == INHERIT else value
    if key in _SWITCH_WORDS:
        return _switch(key, value)
    if key == "brokerages":
        # YAML reads a bare on/off as a boolean.
        if isinstance(value, bool):
            return "on" if value else "off"
        if value not in BROKERAGE_ACCESS:
            raise _Bad("Must be off or on.", list(BROKERAGE_ACCESS))
        return value
    if key == "tools":
        words = _names(value, "tool")
        unknown = [w for w in words if w not in TOOL_WORDS]
        if unknown:
            raise _Bad(
                f"Unknown tool: {', '.join(unknown)}. Use web, market_data, code or shell.",
                list(TOOL_WORDS),
            )
        return [w for w in TOOL_WORDS if w in words]
    if key in ("preferred_connectors", "preferred_skills"):
        return _names(value, "connector" if key == "preferred_connectors" else "skill")
    if key == "description":
        if not isinstance(value, str):
            raise _Bad("Must be text.")
        return value.strip()
    if key == "instructions":
        if not isinstance(value, str):
            raise _Bad("Must be text.")
        return value
    if key == "available_to":
        roles = _names(value, "role")
        unknown = [r for r in roles if r not in ROLE_ORDER]
        if unknown:
            raise _Bad(f"Unknown role: {', '.join(unknown)}.", list(ROLE_ORDER))
        if not roles:
            raise _Bad("Pick at least one: analyst or chief_of_staff.", list(ROLE_ORDER))
        return [r for r in ROLE_ORDER if r in roles]
    raise _Bad("Not a setting a subagent has.", list(WRITABLE_KEYS))


def validate_new_name(
    name: str, *, base_names: Iterable[str], custom_count: int
) -> list[FieldError]:
    """Why ``name`` can't be a new custom subagent, if it can't."""
    errors: list[FieldError] = []
    if not NAME_RE.fullmatch(name):
        errors.append(FieldError(
            "name",
            "Use 1 to 48 lowercase letters, digits or hyphens, starting with a letter or digit.",
        ))
    elif name in RESERVED_NAMES:
        errors.append(FieldError("name", f"{name} is reserved for the agent itself."))
    elif name in set(base_names):
        errors.append(FieldError("name", f"{name} is a built-in subagent. Pick another name."))
    if custom_count >= MAX_CUSTOM:
        errors.append(FieldError(
            "name",
            f"You already have {MAX_CUSTOM} subagents of your own. Delete one to add another.",
        ))
    return errors


def _after(key: str, patch: Mapping[str, Any], current: Mapping[str, Any], base: Mapping[str, Any] | None) -> Any:
    """The effective value of ``key`` once ``patch`` is saved."""
    if key not in patch:
        return current.get(key)
    value = patch[key]
    if value is None:
        return base.get(key) if base is not None else None
    return None if value == INHERIT else value


def _pair_after(
    keys: tuple[str, str], patch: Mapping[str, Any], current: Mapping[str, Any], base: Mapping[str, Any] | None
) -> tuple[Any, Any] | None:
    """The pair ``keys`` will hold once ``patch`` is saved, when a save that
    writes either half has to answer for it.

    A cleared half is the base's again, so it is held to the other half as a
    new value is. The base's own pair is the operator's call, as its pin is
    below, so landing back on it is always allowed.
    """
    if not any(k in patch for k in keys):
        return None
    pair = (_after(keys[0], patch, current, base), _after(keys[1], patch, current, base))
    if base is not None and pair == (base.get(keys[0]), base.get(keys[1])):
        return None
    return pair


async def validate_fields(
    ctx: SubagentContext,
    fields: Mapping[str, Any],
    *,
    kind: Kind,
    current: Mapping[str, Any],
    base: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """``fields`` in canonical form, or raises ``SubagentInvalid`` naming every bad one.

    Only the keys being written are judged, so a stale value elsewhere (a
    skill since deleted) never blocks an unrelated edit; the view flags those.
    ``current`` holds the subagent's effective values before the save and
    ``base`` its base's (None for a custom one), which the cross-field checks
    read: an effort is held to the ladder of the model it will run on.
    """
    errors: list[FieldError] = []
    patch: dict[str, Any] = {}
    for key, raw in fields.items():
        if kind == "override" and key in CUSTOM_KEYS:
            errors.append(FieldError(
                key, "Read-only on a built-in subagent. Duplicate it to write your own."
            ))
            continue
        try:
            patch[key] = normalize_value(key, raw)
        except _Bad as bad:
            errors.append(FieldError(key, bad.message, bad.options))

    model = patch.get("model")
    if model not in (None, INHERIT) and not (base is not None and base.get("model") == model):
        # A base's own pin is the operator's call and never plan-gated, so
        # writing it back is always allowed.
        if not ctx.model_known(model):
            errors.append(FieldError(
                "model", f"Unknown model: {model}. Pick one from the list.",
                await ctx.model_options(),
            ))
        elif not await ctx.model_on_plan(model):
            errors.append(FieldError(
                "model",
                f"{model} isn't on your plan. Add your own key for its provider, "
                "or pick another model.",
                await ctx.model_options(),
            ))

    # A new model holds the saved effort to its ladder too, so a save never
    # keeps a pair this call refuses when the two are written together.
    runs_on, effort = _pair_after(("model", "effort"), patch, current, base) or (None, None)
    if effort is not None:
        from src.llms.reasoning import REASONING_LEVELS

        ladder = ctx.effort_ladder(runs_on) if runs_on and ctx.model_known(runs_on) else None
        if effort not in REASONING_LEVELS:
            errors.append(FieldError(
                "effort", f"Unknown effort: {effort}.", list(ladder or REASONING_LEVELS)
            ))
        elif ladder is not None and not ladder:
            errors.append(FieldError(
                "effort", f"{runs_on} has no effort setting. Use inherit.", []
            ))
        elif ladder is not None and effort not in ladder:
            errors.append(FieldError(
                "effort", f"{runs_on} doesn't offer {effort} effort.", list(ladder)
            ))

    search_pair = _pair_after(("search_provider", "search_depth"), patch, current, base)
    if search_pair is not None:
        from src.tools.web.manifest import (
            CAPABILITY_SEARCH,
            providers_with_capability,
            search_levels,
        )

        providers = providers_with_capability(CAPABILITY_SEARCH)
        provider = patch.get("search_provider")
        if provider not in (None, INHERIT) and provider not in providers:
            errors.append(FieldError(
                "search_provider", f"Unknown search provider: {provider}.", list(providers)
            ))
        # A new provider holds the saved depth to its levels, as a new model
        # holds the saved effort above.
        runs_on, depth = search_pair
        if depth is not None:
            # The rule Settings applies to the user's own pair: a set provider
            # holds the depth to its levels; on inherit any level passes, since
            # which provider runs is not known until the turn.
            levels = search_levels(runs_on)
            if runs_on in providers:
                message = f"{runs_on} doesn't offer {depth} depth."
            else:
                message = f"Unknown search depth: {depth}."
            if depth not in levels:
                errors.append(FieldError("search_depth", message, levels))

    unknown = [c for c in patch.get("preferred_connectors") or () if c not in ctx.connector_names]
    if unknown:
        errors.append(FieldError(
            "preferred_connectors", f"Unknown connector: {', '.join(unknown)}.",
            sorted(ctx.connector_names),
        ))
    unknown = [s for s in patch.get("preferred_skills") or () if s not in ctx.skills]
    if unknown:
        errors.append(FieldError(
            "preferred_skills", f"Unknown skill: {', '.join(unknown)}.", sorted(ctx.skills)
        ))

    errors += content_errors(patch)
    if errors:
        raise SubagentInvalid(errors)
    return patch


def unsavable_message(text: str) -> str | None:
    """Why Postgres can't store ``text`` (see ``UNSAVABLE``), or None."""
    found = UNSAVABLE.search(text)
    if found is None:
        return None
    return NUL_MESSAGE if found.group() == "\x00" else SURROGATE_MESSAGE


def content_errors(values: Mapping[str, Any]) -> list[FieldError]:
    """The limits on a custom subagent's own text: its size, and no character
    Postgres can't store. Its other values are names checked against what
    exists, so only these two are saved as sent."""
    errors: list[FieldError] = []
    description = values.get("description")
    unsavable = unsavable_message(description) if isinstance(description, str) else None
    if unsavable is not None:
        errors.append(FieldError("description", unsavable))
    elif isinstance(description, str) and len(description) > DESCRIPTION_MAX:
        errors.append(FieldError(
            "description",
            f"Keep it to {DESCRIPTION_MAX} characters (this is {len(description)}).",
        ))
    instructions = values.get("instructions")
    unsavable = unsavable_message(instructions) if isinstance(instructions, str) else None
    if unsavable is not None:
        errors.append(FieldError("instructions", unsavable))
    elif isinstance(instructions, str):
        size = len(instructions.encode("utf-8"))
        if size > INSTRUCTIONS_MAX_BYTES:
            # Rounded up, so a size just over the limit never reads as the limit.
            errors.append(FieldError(
                "instructions",
                f"Keep instructions to {INSTRUCTIONS_MAX_BYTES // 1024} KB or less "
                f"(this is {math.ceil(size / 102.4) / 10:.1f} KB).",
            ))
    return errors


def custom_text_errors(final: Mapping[str, Any], sent: Mapping[str, Any]) -> list[FieldError]:
    """A custom subagent's text once saved: limits on what ``sent`` didn't carry
    (a copied description), and a description to route on, which it needs."""
    errors = content_errors({k: final.get(k) for k in ("description", "instructions") if k not in sent})
    if not (final.get("description") or "").strip() and not any(
        e.field == "description" for e in errors
    ):
        errors.append(FieldError(
            "description", "Say when the agent should hand work to this subagent."
        ))
    return errors


def _collapse(text: str) -> str:
    return " ".join(text.split()).casefold()


def description_matches(
    description: str,
    roles: Iterable[str],
    others: Iterable[tuple[str, str, Iterable[str]]],
) -> list[str]:
    """Names among ``others`` whose description reads the same as ``description``.

    ``others`` are ``(name, description, roles)`` of the other subagents that
    are on; only pairs sharing a role count, since those are the ones the main
    agent would pick between at random. Whitespace and case don't count.
    """
    mine = _collapse(description or "")
    if not mine:
        return []
    roles = set(roles)
    return [
        name
        for name, text, their_roles in others
        if roles & set(their_roles) and _collapse(text or "") == mine
    ]


def description_match_note(other: str) -> str:
    """The save report's warning for a copied description (design wording)."""
    return f"Its description matches {other}'s; say when to pick this one over it"


def changed_fields(current: Mapping[str, Any], values: Mapping[str, Any]) -> dict[str, Any]:
    """The keys of ``values`` that would change ``current`` (a view's values).

    For a writer that sends every key (a file), so only what the save changes
    is validated and stored. A value that can't be normalized counts as
    changed, so the validator reports it.
    """
    out: dict[str, Any] = {}
    for key, raw in values.items():
        try:
            value = normalize_value(key, raw)
        except _Bad:
            out[key] = raw
            continue
        now = current.get(key)
        if key in INHERITABLE_KEYS:
            value = None if value == INHERIT else value
        if value != now:
            out[key] = raw
    return out
