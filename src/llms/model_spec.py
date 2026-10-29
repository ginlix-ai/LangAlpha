"""A model resolved down to what building a client needs.

A manifest entry and a user-defined one carry the same facts in different
shapes, so they are normalized to one type here and ``LLM.__init__`` runs a
single body for both. Imports nothing from ``llm``: the config class arrives as
an argument, which is what keeps the dependency pointing one way.
"""

import copy
from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Protocol

from .gateway_names import gateway_slug, manifest_slug_index, names_models_its_own_way
from .reasoning import (
    OFF_LEVELS,
    REASONING_LEVELS,
    SURFACE_KEYS,
    infer_surface,
    validate_surface,
)


class ManifestSource(Protocol):
    """The lookups a spec needs from the config."""

    llm_config: Mapping[str, dict]

    def get_model_config(self, model_id: str) -> dict | None: ...

    def get_provider_info(self, provider: str) -> dict: ...


def canonical_reasoning_efforts(declared: Any) -> tuple[str, ...]:
    """Declared reasoning levels, deduped and in canonical order.

    Anything outside :data:`REASONING_LEVELS` is dropped, so a typo in a
    manifest entry or a user-defined model costs one button instead of offering
    a level the provider will reject.
    """
    if not isinstance(declared, list):
        return ()
    seen = {v for v in declared if v in REASONING_LEVELS}
    return tuple(level for level in REASONING_LEVELS if level in seen)


def default_reasoning_effort(
    efforts: tuple[str, ...] | list[str], declared: Any
) -> str | None:
    """The level used when the user has not chosen one.

    Falls back to the middle of the model's own range rather than a fixed name,
    because "medium" is not in every model's vocabulary: a binary model's two
    levels are off and on.
    """
    if not efforts:
        return None
    if declared in efforts:
        return declared
    return efforts[len(efforts) // 2]


def summary_reasoning_effort(efforts: tuple[str, ...] | list[str]) -> str | None:
    """The level a compaction summary runs at: ``medium``, else ``high``.

    A summary reads a long stretch of history inside a fixed time budget, so
    it takes neither the model's default, which can be its deepest level, nor
    a level picked for conversations: at its deepest a model can reason for
    minutes before writing a word of the summary. A ladder offering neither
    leaves the model's own default.
    """
    return next((level for level in ("medium", "high") if level in efforts), None)


def reasoning_block(entry: dict | None) -> dict:
    """One model's reasoning declaration, from either shape it may be stored in.

    The manifest states it as a single ``reasoning`` block. Custom models saved
    before that block existed carry the same facts as flat ``reasoning_efforts``
    / ``reasoning_effort_default`` keys, so both are read here rather than
    migrating a preferences column. An empty block declares nothing and so does
    not answer for the entry: preferring it would strand a stored entry's flat
    keys, unread by everything downstream of a save that validated them.
    """
    entry = entry or {}
    block = entry.get("reasoning")
    if isinstance(block, dict) and block:
        return block
    flat = {}
    if "reasoning_efforts" in entry:
        flat["efforts"] = entry["reasoning_efforts"]
    if "reasoning_effort_default" in entry:
        flat["default"] = entry["reasoning_effort_default"]
    return flat


def profile_overrides(entry: dict) -> dict[str, Any]:
    """Map a model entry onto ``ModelProfile`` keys (the entry is the SoT)."""
    overrides: dict[str, Any] = {}
    context = entry.get("context")
    if isinstance(context, int):
        overrides["max_input_tokens"] = context
    max_tokens = (entry.get("parameters") or {}).get("max_tokens")
    if isinstance(max_tokens, int):
        overrides["max_output_tokens"] = max_tokens
    modalities = entry.get("input_modalities")
    if isinstance(modalities, list):
        overrides["text_inputs"] = "text" in modalities
        overrides["image_inputs"] = "image" in modalities
        overrides["audio_inputs"] = "audio" in modalities
        overrides["video_inputs"] = "video" in modalities
    return overrides


def clamp_reasoning_effort(
    efforts: tuple[str, ...] | list[str], default: str | None, requested: str
) -> str | None:
    """The level to actually send, or None to leave the model's own default.

    Takes the ladder rather than a model name because a user-defined model has
    no manifest row to look one up in. An unhonored level steps *down* to the
    nearest one the ladder offers, so the request is never overshot; below its
    floor there is nowhere to step but the lowest level, and a level outside
    the vocabulary takes the default.
    """
    if not efforts:
        return None
    if requested in efforts:
        return requested
    if requested not in REASONING_LEVELS:
        return default
    ceiling = REASONING_LEVELS.index(requested)
    at_or_below = [e for e in efforts if REASONING_LEVELS.index(e) <= ceiling]
    return at_or_below[-1] if at_or_below else efforts[0]


def _seeded_level(surface: dict | None, parameters: dict, extra_body: dict) -> Any:
    """The level already sitting at a surface's own write path, if any."""
    write = (surface or {}).get("write")
    if not isinstance(write, str):
        return None
    lane, *rest = write.split(".")
    node = {"parameters": parameters, "extra_body": extra_body}.get(lane)
    for segment in rest:
        node = node.get(segment) if isinstance(node, dict) else None
    return node


def _checked_surface(name: str, reasoning: dict | None) -> dict:
    """Just the write half of a ``reasoning`` block, validated.

    Raises rather than dropping the bad path, so a typo is a loud error naming
    the model instead of a silent no-op on the wire. Spec-building is per
    request, so for manifest rows the suite is what catches one first:
    ``test_reasoning_efforts_manifest`` builds every entry.
    """
    if not reasoning:
        return {}
    validate_surface(name, reasoning)
    return {k: reasoning[k] for k in SURFACE_KEYS if k in reasoning}


#: What a custom entry inherits from a built-in whose name it takes. Routing --
#: model_id, provider, parameters -- is the entry's whole reason to exist and
#: never inherits. These say what the model honors, and a shadow is the same
#: model reached through the user's own key. ``compaction_profile_for`` reads
#: the named profile first and the window's band second, so both travel:
#: Claude and GPT rows name a later profile than their window's band.
#: ``reasoning`` is
#: handled separately by :func:`with_inherited_declarations`, because it is the
#: one declaration that is really two.
SHADOW_INHERITED = (
    "prompt_guidance",
    "compaction_profile",
    "context",
)


#: SDKs whose client can call the Responses API as well as Chat Completions.
_RESPONSES_SDKS = ("openai", "codex", "dashscope")


def client_wire(
    provider_info: dict | None, response_api: bool | None = None, sdk_fallback: str | None = "openai"
) -> tuple[str | None, bool]:
    """The SDK a provider is called through, and whether its client takes the
    Responses API. ``response_api`` is an entry's own opt-in, over the provider's."""
    info = provider_info or {}
    sdk = info.get("sdk") or sdk_fallback
    on = info.get("use_response_api", False) if response_api is None else response_api
    return sdk, bool(on) and sdk in _RESPONSES_SDKS


def wire_api(provider_info: dict | None, response_api: bool | None = None) -> str:
    """The API a provider is called through: its SDK, and for an SDK with both
    OpenAI endpoints, which one."""
    sdk, responses = client_wire(provider_info, response_api)
    if sdk not in ("openai", "dashscope"):
        return sdk
    return f"{sdk}-responses" if responses else f"{sdk}-chat"


def borrows_reasoning(shadowed: dict | None, route_api: str, shadowed_api: str) -> bool:
    """Whether a custom entry reached through ``route_api`` can take the effort
    control of the built-in it shadows.

    A level written to ``parameters`` is a keyword of one SDK's client:
    ``output_config`` is Anthropic's, ``reasoning`` the Responses API's,
    ``thinking_level`` Gemini's. Through another API's client it is refused
    (every call fails) or moves the call to another endpoint. A control written
    only into the body goes through any client and leaves the endpoint to
    decide, which is how a coding plan already reaches a vendor's models
    through its Anthropic route.
    """
    surface = reasoning_block(shadowed)
    if not surface:
        return False
    if route_api == shadowed_api:
        return True
    paths = [surface["write"]] if isinstance(surface.get("write"), str) else []
    for switch in ("on", "off"):
        paths += list(surface.get(switch) or {})
    return bool(paths) and all(path.startswith("extra_body.") for path in paths)


def routed_provider_info(entry: dict, manifest: ManifestSource) -> dict:
    """The manifest's declarations for a routed entry's ``provider``, which are
    none when the resolver marked it ``_own_provider``: the user's own provider
    under a name the manifest also gives one of its own."""
    provider = entry.get("provider")
    if entry.get("_own_provider") or not isinstance(provider, str):
        return {}
    return manifest.get_provider_info(provider)


def shadowed_model(entry: dict, manifest: ManifestSource) -> dict | None:
    """The built-in a custom entry stands for: the one whose name it took, else,
    for an entry routed through a gateway that names models its own way
    (OpenRouter, or one with a ``model_catalog``), the one its id matches. That
    match is what the model list called it known by, so the entry is tuned as
    the model it was picked as."""
    name = entry.get("name")
    found = manifest.get_model_config(name) if isinstance(name, str) and name else None
    route = routed_provider_info(entry, manifest)
    if found is not None or not names_models_its_own_way(route):
        return found
    model_id = entry.get("model_id")
    key = manifest_slug_index(manifest).get(gateway_slug(model_id)) if isinstance(model_id, str) else None
    return manifest.get_model_config(key) if key else None


def _borrows_from(entry: dict, shadowed: dict, manifest: ManifestSource) -> bool:
    """:func:`borrows_reasoning` for a routed entry: its ``provider`` the one
    whose SDK it speaks and ``_use_response_api`` its own opt-in, as the
    resolver leaves them."""
    return borrows_reasoning(
        shadowed,
        wire_api(routed_provider_info(entry, manifest), entry.get("_use_response_api")),
        wire_api(manifest.get_provider_info(shadowed.get("provider"))),
    )


def _merged_reasoning(own: dict, borrowed: dict) -> dict:
    """``own`` over ``borrowed``, with the write half taken whole from one side.

    A surface is one declaration: an ``off`` patch is only coherent beside the
    ``write`` and ``on`` it was written with. An entry naming any of
    :data:`SURFACE_KEYS` owns all of them, else a borrowed ``off`` sits beside a
    ladder of the entry's own with no off rung and every turn fails validation.
    Borrowing the surface for a ladder of the entry's own, the narrowed one a
    flat-shape entry stores, drops an ``off`` none of its rungs can apply.
    """
    if any(k in own for k in SURFACE_KEYS):
        borrowed = {k: v for k, v in borrowed.items() if k not in SURFACE_KEYS}
    merged = {**borrowed, **own}
    efforts = merged.get("efforts")
    if (
        "off" in borrowed
        and "efforts" in own
        and isinstance(efforts, list)
        and not OFF_LEVELS.intersection(lv for lv in efforts if isinstance(lv, str))
    ):
        del merged["off"]
    return merged


def inherited_declarations(entry: dict, shadowed: dict | None, manifest: ManifestSource) -> dict:
    """What a routed custom entry takes from the built-in whose name it took.

    Presence, not truthiness: an entry declaring ``reasoning_efforts: []`` is
    saying the model honors no levels, and the shadowed ladder must not
    overwrite that answer.

    ``reasoning`` fills in by half rather than whole, because the block
    bundles two declarations -- which levels exist, and where the chosen one is
    written -- and an entry that names one still needs the other. Inheriting it
    whole discards a ladder stored in the flat shape, which carries no
    ``reasoning`` key to block the inheritance; inheriting nothing when the
    entry names any key at all strands a partial block with no surface. The
    surface itself comes whole from one side (``_merged_reasoning``). An entry
    on another API than the built-in's keeps only its own (``borrows_reasoning``).
    """
    if not shadowed:
        return {}
    inherited = {k: shadowed[k] for k in SHADOW_INHERITED if k not in entry and k in shadowed}
    borrowed = reasoning_block(shadowed) if _borrows_from(entry, shadowed, manifest) else {}
    reasoning = _merged_reasoning(reasoning_block(entry), borrowed)
    if reasoning:
        inherited["reasoning"] = reasoning
    return inherited


def with_inherited_declarations(entry: dict, shadowed: dict | None, manifest: ManifestSource) -> dict:
    """A routed custom entry filled in from the built-in whose name it took."""
    inherited = inherited_declarations(entry, shadowed, manifest)
    return {**entry, **inherited} if inherited else entry


@dataclass(frozen=True)
class ModelSpec:
    """One model's client-building facts, whatever declared them."""

    name: str
    model_id: str
    provider: str
    parameters: dict[str, Any] = field(default_factory=dict)
    extra_body: dict[str, Any] = field(default_factory=dict)
    additional_betas: tuple[str, ...] = ()
    reasoning_efforts: tuple[str, ...] = ()
    reasoning_effort_default: str | None = None
    #: Where this model's effort level is written. Empty means no effort control.
    reasoning_surface: dict[str, Any] = field(default_factory=dict)
    #: Platform proxy to route through when the caller brings no key of its own.
    system_provider: str | None = None
    #: SDK to assume when the provider is not in the manifest at all. A manifest
    #: model has no such guess to make; a custom endpoint is usually
    #: OpenAI-compatible.
    sdk_fallback: str | None = None
    #: Response-API opt-in the entry states outright, overriding the provider's.
    use_response_api_override: bool | None = None
    #: ``provider`` is the user's own under a name the manifest also uses, so
    #: none of the manifest's declarations for that name apply to it.
    own_provider: bool = False
    #: ``ModelProfile`` keys the entry declares, laid over the SDK's own table,
    #: which most models are missing from. Compaction sizes its summary input
    #: to ``max_input_tokens``.
    profile: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_manifest(cls, model_config: ManifestSource, model_name: str) -> "ModelSpec":
        info = model_config.get_model_config(model_name)
        if not info:
            raise ValueError(f"Model {model_name} not found in models.json")
        reasoning = reasoning_block(info)
        efforts = canonical_reasoning_efforts(reasoning.get("efforts"))
        return cls(
            name=model_name,
            model_id=info["model_id"],
            provider=info["provider"],
            parameters=copy.deepcopy(info.get("parameters") or {}),
            extra_body=copy.deepcopy(info.get("extra_body") or {}),
            additional_betas=tuple(info.get("additional_betas") or ()),
            reasoning_efforts=efforts,
            reasoning_effort_default=default_reasoning_effort(
                efforts, reasoning.get("default")
            ),
            reasoning_surface=_checked_surface(model_name, reasoning),
            system_provider=info.get("system_provider"),
            profile=profile_overrides(info),
        )

    @classmethod
    def from_custom(cls, config: dict, model_config: ManifestSource) -> "ModelSpec":
        """A user-defined entry, routed, which carries its own model_id and ladder.

        When it took a manifest model's name it still routes the call, but a
        level it does not list itself is the built-in's to declare.
        """
        shadowed = shadowed_model(config, model_config)
        declared = with_inherited_declarations(config, shadowed, model_config)
        reasoning = reasoning_block(declared)
        # The entry's own half of that, kept apart: what it says about itself
        # outranks anything it borrowed from the name it took.
        own = reasoning_block(config)
        efforts = canonical_reasoning_efforts(reasoning.get("efforts"))
        declared_response_api = config.get("_use_response_api")
        # Deep-copied because the mapper writes into these in place, and they
        # would otherwise be the caller's own stored preference dicts.
        parameters = copy.deepcopy(config.get("parameters") or {})
        extra_body = copy.deepcopy(config.get("extra_body") or {})
        # Gated on the ladder: an entry declaring no levels wants no effort
        # control at all, not the shadowed model's surface.
        surface = reasoning if efforts else None
        if efforts and not any(k in own for k in SURFACE_KEYS):
            # Read against the entry's own block, not the merged one, so the
            # order below is seed first and borrowed path second. An entry that
            # spelled a control in its own parameters is routing somewhere that
            # reads it; writing the level to the built-in's path instead leaves
            # that seed stale and puts a second control on the wire beside it.
            surface = infer_surface(parameters, extra_body)
            if not surface and any(k in reasoning for k in SURFACE_KEYS):
                surface = reasoning
        # An entry that seeded a level into its own parameters and named no
        # default has already said which rung it runs: the mapper used to skip
        # the no-request turn entirely, so that seed was what went out. Reading
        # it back keeps that turn on the same rung instead of the ladder's
        # midpoint. Only graded dials seed a level name, which is the whole of
        # WRITE_PATHS, so there is nothing here to misread.
        # It is read before the shadowed default for the same reason the surface
        # is: a borrowed default belongs to the wider ladder this entry narrowed,
        # so taking it first moves a default turn off the rung the entry ran.
        declared_default = own.get("default")
        if declared_default is None:
            declared_default = _seeded_level(surface, parameters, extra_body)
        if declared_default is None:
            declared_default = reasoning.get("default")
        default = default_reasoning_effort(efforts, declared_default)
        return cls(
            name=config.get("name", config["model_id"]),
            model_id=config["model_id"],
            provider=config["provider"],
            parameters=parameters,
            extra_body=extra_body,
            additional_betas=tuple(config.get("additional_betas") or ()),
            reasoning_efforts=efforts,
            reasoning_effort_default=default,
            reasoning_surface=_checked_surface(
                config.get("name", config["model_id"]), surface
            ),
            sdk_fallback="openai",
            use_response_api_override=(
                None if declared_response_api is None else bool(declared_response_api)
            ),
            own_provider=bool(config.get("_own_provider")),
            profile=profile_overrides(declared),
        )
