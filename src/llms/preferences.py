"""Shape of the stored ``model_preference`` bag.

Lives under ``src.llms`` because both layers that read it — the server's config
resolver and the agent's prompt renderer — already reach here, and neither may
import the other. Nothing in this module imports either, or ``llm.py``: it is
the vocabulary both sides agree on, so it has to stay cheap to import.
"""

from dataclasses import dataclass, fields, replace
from typing import Any

#: Keys that moved out of ``other_preference`` into their own column, and so
#: may still be found there on a row written before the move.
#:
#: Migration 034 carries its own copy on purpose, and the two are no longer
#: identical: it also names ``prompt_guidance``, which never lived in the old
#: column and appears there only if that revision's downgrade puts it back.
#: Naming it there closes the return trip; naming it here would claim a
#: history it does not have.
MOVED_MODEL_KEYS: tuple[str, ...] = (
    "preferred_model",
    "preferred_flash_model",
    "compaction_model",
    "summarization_model",
    "fetch_model",
    "fallback_models",
    "custom_models",
    "custom_providers",
    "compaction_profile",
    "reasoning_effort",
    "fast_mode",
)

#: How much prompt scaffolding a model gets. Lean is a strict subset of
#: detailed, so an unannotated model gets more, never less.
GUIDANCE_LEVELS: tuple[str, ...] = ("lean", "detailed")
DEFAULT_GUIDANCE: str = "detailed"

#: Context window (inclusive lower bound) -> compaction profile, widest first.
#: A model that declares no profile of its own gets the one its window can
#: afford: the presets were sized against these same bands, so this reads the
#: intent back out rather than inventing a second policy. Names, not bundles —
#: the bundles are agent config and this module must stay importable from both
#: sides without dragging one into the other.
COMPACTION_PROFILE_BANDS: tuple[tuple[int, str], ...] = (
    (1_000_000, "relaxed"),
    (400_000, "extended"),
    (200_000, "moderate"),
    (0, "aggressive"),
)

COMPACTION_PROFILE_NAMES: tuple[str, ...] = tuple(
    name for _, name in COMPACTION_PROFILE_BANDS
)


def compaction_profile_for(entry: dict[str, Any] | None) -> str | None:
    """Which compaction preset a model entry gets when nobody has chosen one:
    its own declaration, else the band its context window falls in.

    Takes the entry rather than a name so a custom model, which has no manifest
    row, resolves by the same rule as a built-in one.
    """
    if not entry:
        return None
    declared = entry.get("compaction_profile")
    if isinstance(declared, str):
        return declared
    context = entry.get("context")
    if not isinstance(context, int):
        return None
    for floor, profile in COMPACTION_PROFILE_BANDS:
        if context >= floor:
            return profile
    return None


@dataclass(frozen=True)
class Tuning:
    """The four settings a user may set per account or per model.

    Typed rather than a loose bag because every read site was re-checking the
    same invariants the write site had already enforced.
    """

    prompt_guidance: str | None = None
    compaction_profile: str | None = None
    reasoning_effort: str | None = None
    fast_mode: bool | None = None


#: Settings a per-model profile may override. Derived from :class:`Tuning` so
#: the field list and the type cannot drift apart.
TUNING_FIELDS: tuple[str, ...] = tuple(f.name for f in fields(Tuning))


class TuningError(ValueError):
    """A tuning value the stored shape does not allow.

    Carries the offending field so the HTTP layer can name it in a 400 without
    re-deriving what went wrong, and so a non-HTTP writer raising this is not
    reduced to a 500.
    """

    def __init__(self, field: str, message: str):
        self.field = field
        super().__init__(message)


def validate_tuning(
    values: dict[str, Any],
    *,
    where: str,
    reasoning_efforts: list[str] | None = None,
) -> None:
    """Check tuning values against their vocabularies. Raises :class:`TuningError`.

    ``reasoning_efforts`` is the running model's ladder when the caller knows
    which model these settings are for; ``None`` means no model in hand (the
    account-wide level), where any level in the global vocabulary is allowed
    because it is clamped per model at resolve time.
    """
    from src.llms.reasoning import REASONING_LEVELS

    for field, valid in (
        ("prompt_guidance", GUIDANCE_LEVELS),
        ("compaction_profile", COMPACTION_PROFILE_NAMES),
    ):
        value = values.get(field)
        if value is not None and value not in valid:
            raise TuningError(
                field, f"{where}.{field} must be one of {sorted(valid)}"
            )

    fast = values.get("fast_mode")
    if fast is not None and not isinstance(fast, bool):
        raise TuningError("fast_mode", f"{where}.fast_mode must be a boolean")

    effort = values.get("reasoning_effort")
    if effort is not None:
        # ``None`` and ``[]`` are different answers: no model in hand, versus a
        # model that declares no ladder at all. Reading this as truthiness let a
        # profile store a level for a model with no reasoning control, which
        # ``LLM`` then resolves to None -- configured on screen, ignored at run.
        allowed = list(REASONING_LEVELS) if reasoning_efforts is None else reasoning_efforts
        if effort not in allowed:
            raise TuningError(
                "reasoning_effort",
                f"{where}.reasoning_effort must be one of {allowed}",
            )


def custom_model(model_pref: dict[str, Any], name: str | None) -> dict[str, Any] | None:
    """The user's own entry for ``name``, if they defined one.

    Custom models shadow built-ins by name, so this is checked before the
    manifest everywhere a model's own declaration matters.
    """
    if not name:
        return None
    for entry in model_pref.get("custom_models") or []:
        if isinstance(entry, dict) and entry.get("name") == name:
            return entry
    return None


def custom_model_names(model_pref: dict[str, Any]) -> set[str]:
    """Names of every custom model the user has defined."""
    return {
        entry["name"]
        for entry in model_pref.get("custom_models") or []
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }


def resolve_tuning_field(model_pref: dict[str, Any], model: str | None, field: str) -> Any:
    """Per-model override, else the account-wide value.

    Every tuning field resolves this way, so a caller that knows which model is
    running gets the same precedence whether it is picking a compaction
    threshold or a prompt guidance level.
    """
    profiles = model_pref.get("profiles")
    if model and isinstance(profiles, dict):
        profile = profiles.get(model)
        if isinstance(profile, dict) and field in profile:
            return profile[field]
    return model_pref.get(field)


def resolve_tuning(model_pref: dict[str, Any], model: str | None) -> Tuning:
    """All four fields at once, with per-model overrides applied.

    The typed twin of ``resolveTuning`` in ``web/src/lib/modelTuning.ts``; the
    two implement one precedence rule and are pinned together by
    ``tests/fixtures/tuning_precedence.json``.
    """
    return Tuning(
        **{f: resolve_tuning_field(model_pref, model, f) for f in TUNING_FIELDS}
    )


def resolve_turn_tuning(
    model_pref: dict[str, Any],
    model: str | None,
    *,
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
) -> Tuning:
    """:func:`resolve_tuning` with what this turn asked for on top.

    The third rung of one ladder: the request beats the model's own profile,
    which beats the account value. Only the model the request named takes the
    override, so a subagent running a different model resolves on its own
    settings rather than inheriting a level chosen for the composer's pick.
    """
    tuning = resolve_tuning(model_pref, model)
    return replace(
        tuning,
        reasoning_effort=reasoning_effort or tuning.reasoning_effort,
        fast_mode=fast_mode if fast_mode is not None else tuning.fast_mode,
    )
