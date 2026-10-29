"""Save-time checks on the models and providers a user defines.

Run by the preferences route before a write. Each refusal is a 400 whose detail
names the offending entry, which the settings UI shows as is.
"""

from __future__ import annotations

import re

from fastapi import HTTPException

_VALID_MODALITIES = frozenset({"text", "image", "pdf"})
_MIN_CONTEXT = 1_000
_MAX_CONTEXT = 100_000_000


def validate_custom_models(
    custom_models: list,
    custom_providers: list | None = None,
    stored: dict | None = None,
) -> None:
    """Validate custom_models list before persisting. Raises HTTPException 400 on invalid data.

    The provider check exempts what ``_saved_pairs`` names, for the reason it
    gives. Checks added since entries were first saved hold only an entry this
    save adds or changes, for the same reason: a save carries the whole list.
    """
    from ptc_agent.agent.prompts.guidance import VALID_GUIDANCE

    from src.llms.llm import LLM, CUSTOM_MODEL_NAME_RE
    from src.llms.model_spec import ModelSpec, reasoning_block
    from src.server.services.llm.user_models import custom_provider, route_custom_entry
    from src.llms.reasoning import (
        REASONING_LEVELS,
        ReasoningSurfaceError,
        validate_surface,
    )

    if not isinstance(custom_models, list):
        raise HTTPException(status_code=400, detail="custom_models must be a list")

    # Reuse the process-wide singleton — building a fresh ModelConfig on every
    # preferences PUT re-parses models.json + re-scans _flat_providers for
    # nothing (the manifest is static).
    mc = LLM.get_model_config()
    name_re = re.compile(CUSTOM_MODEL_NAME_RE)
    seen_names: set[str] = set()

    # Shadow semantics: a custom ``name`` MAY collide with a built-in. The
    # resolver checks custom first, so the user's entry wins. This supports
    # "route built-in model X through my variant's key" without inventing a
    # prefix format for preferences.

    valid_providers = {
        k for k, v in mc.flat_providers.items()
        if not v.get("platform")
    }
    if custom_providers:
        valid_providers.update(
            cp["name"] for cp in custom_providers if isinstance(cp, dict) and cp.get("name")
        )

    stored = stored or {}
    # Models on the user's own providers are judged against this save's
    # providers instead, so removing a provider takes its models with it.
    own = {name for name, _ in _saved_pairs(stored.get("custom_providers"), "parent_provider")}
    kept = {pair for pair in _saved_pairs(stored.get("custom_models"), "provider") if pair[1] not in own}
    saved_models = [cm for cm in stored.get("custom_models") or [] if isinstance(cm, dict)]

    for idx, cm in enumerate(custom_models):
        if not isinstance(cm, dict):
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: must be an object")
        # Read before anything below normalizes ``cm`` in place; a stored entry
        # was saved normalized, so one this save leaves alone compares equal.
        unchanged = cm in saved_models

        name = cm.get("name")
        model_id = cm.get("model_id")
        provider = cm.get("provider")

        # Strings before the regex and set lookups below, which would raise a
        # TypeError (a 500) on anything else.
        if not isinstance(name, str) or not name:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: name is required")
        if not isinstance(model_id, str) or not model_id:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: model_id is required")
        if not provider:
            raise HTTPException(status_code=400, detail=f"custom_models[{idx}]: provider is required")

        if not name_re.match(name):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: name '{name}' is invalid (alphanumeric start, max 63 chars, only .-_:/ allowed)",
            )

        if name in seen_names:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: duplicate name '{name}'",
            )
        seen_names.add(name)

        if not isinstance(provider, str) or not provider.strip():
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider must be a non-empty string",
            )

        if provider not in valid_providers and (name, provider) not in kept:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: provider '{provider}' is not a known BYOK-eligible or custom provider",
            )

        for field in ("parameters", "extra_body", "reasoning"):
            val = cm.get(field)
            if val is not None and not isinstance(val, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: {field} must be a JSON object",
                )

        # Rejected here rather than at request time: an unknown write path is
        # accepted by the vendor and ignored, so the entry would render an
        # effort control that reports a level and sends nothing.
        if isinstance(cm.get("reasoning"), dict) and cm["reasoning"]:
            try:
                validate_surface(f"custom_models[{idx}]", cm["reasoning"])
            except ReasoningSurfaceError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        modalities = cm.get("input_modalities")
        if modalities is not None:
            if not isinstance(modalities, list) or len(modalities) == 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: input_modalities must be a non-empty list",
                )
            for m in modalities:
                if not isinstance(m, str) or m not in _VALID_MODALITIES:
                    raise HTTPException(
                        status_code=400,
                        detail=f"custom_models[{idx}]: invalid modality '{m}', allowed: {sorted(_VALID_MODALITIES)}",
                    )
            # Ensure "text" is always present
            if "text" not in modalities:
                cm["input_modalities"] = ["text"] + modalities

        # Read as the window compaction triggers under, so a wrong type would
        # silently fall back to the default band rather than fail.
        context = cm.get("context")
        if not unchanged and context is not None and (
            isinstance(context, bool) or not isinstance(context, int)
            or not _MIN_CONTEXT <= context <= _MAX_CONTEXT
        ):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: context must be a whole number of tokens from {_MIN_CONTEXT} to {_MAX_CONTEXT}",
            )

        # A custom model has no manifest entry, so these are the only place it
        # can declare what it honors. Without them every custom model resolves
        # to the fail-safe guidance level and gets no effort selector at all.
        guidance = cm.get("prompt_guidance")
        if guidance is not None and guidance not in VALID_GUIDANCE:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: prompt_guidance must be one of {sorted(VALID_GUIDANCE)}",
            )

        # Either shape: a `reasoning` block, or the flat keys entries saved
        # before it existed still carry. Whichever it used is written back to,
        # which is why the shapes are read apart here rather than through
        # `reasoning_block`: its normalized view cannot say which key to
        # rewrite. Empty means unused, on the same terms as that function.
        block = cm.get("reasoning")
        declared = block if isinstance(block, dict) and block else cm
        efforts_key = "efforts" if declared is not cm else "reasoning_efforts"
        default_key = "default" if declared is not cm else "reasoning_effort_default"

        efforts = declared.get(efforts_key)
        if efforts is not None:
            if not isinstance(efforts, list) or any(e not in REASONING_LEVELS for e in efforts):
                raise HTTPException(
                    status_code=400,
                    detail=f"custom_models[{idx}]: {efforts_key} must be a list drawn from {list(REASONING_LEVELS)}",
                )
            # Store in canonical order so the UI renders a ladder, not the
            # order the client happened to send.
            declared[efforts_key] = [lv for lv in REASONING_LEVELS if lv in set(efforts)]

        # An entry shadowing a built-in inherits that model's ladder, so a
        # default may name a level this entry does not list itself. With no
        # ladder at all no level is ever sent and the default is inert: that is
        # a shadow whose built-in has since left the manifest, and refusing it
        # would 400 every later save of the whole list.
        effective_efforts = declared.get(efforts_key)
        if effective_efforts is None:
            effective_efforts = reasoning_block(mc.get_model_config(name)).get("efforts") or []
        default_effort = declared.get(default_key)
        if (
            not unchanged
            and default_effort is not None
            and effective_efforts
            and default_effort not in effective_efforts
        ):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: {default_key} must be one of {sorted(effective_efforts)}",
            )

        # The block above is only the entry's own half. What runs is that half
        # laid over the built-in it shadows, on the route this save's providers
        # give it, so the spec a turn builds is built here.
        if not unchanged:
            routed = cm
            provider_def = custom_provider({"custom_providers": custom_providers}, provider)
            if provider_def is not None and isinstance(provider_def.get("parent_provider"), str):
                routed = route_custom_entry(cm, provider_def, mc)
            try:
                ModelSpec.from_custom(routed, mc)
            except ReasoningSurfaceError as exc:
                raise HTTPException(
                    status_code=400, detail=f"custom_models[{idx}]: {exc}"
                ) from exc


async def refuse_custom_model_endpoints(custom_models: list, stored: list | None = None) -> None:
    """Refuse a subscription's model that sets an endpoint or connection, its
    token being for its own provider only, and, in platform mode, a custom
    model whose parameters point its client somewhere the server must not
    dial, or route it another way.

    Only an entry that is new, or whose provider or routing differs from the
    ``stored`` entry of its name, is checked. A save carries the whole list, so one entry
    saved before the policy, or whose host has stopped resolving, would
    otherwise block every later change. Such an entry is still refused, or
    held, where its client is built.
    """
    from src.server.services.llm import egress
    from src.server.services.llm.availability import on_subscription

    def route(cm: dict) -> tuple:
        # The provider too: moving onto a subscription changes what may be set.
        return cm.get("provider"), egress.routing_params(cm.get("parameters"))

    saved = {cm.get("name"): route(cm) for cm in stored or [] if isinstance(cm, dict)}
    changed = [
        (idx, cm) for idx, cm in enumerate(custom_models)
        if cm.get("name") not in saved or saved[cm.get("name")] != route(cm)
    ]
    for idx, cm in changed:
        if on_subscription(cm) and egress.routing_params(cm.get("parameters")):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: a subscription's model cannot set an endpoint or connection.",
            )
    if not egress.enforced():
        return
    to_check: list[tuple[int, str]] = []
    for idx, cm in changed:
        params = cm.get("parameters")
        for key in (*egress.connection_params(params), *egress.vertex_params(params)):
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: parameters.{key} cannot be set. Remove it.",
            )
        to_check.extend((idx, url) for url in egress.parameter_urls(params))
    reasons = await egress.save_refusals(url for _, url in to_check)
    for idx, url in to_check:
        if reasons[url]:
            raise HTTPException(
                status_code=400,
                detail=f"custom_models[{idx}]: the base URL in parameters cannot be used: {reasons[url]}",
            )


def validate_custom_providers(custom_providers: list, stored: dict | None = None) -> None:
    """Validate custom_providers list before persisting.

    The parent check exempts what ``_saved_pairs`` names, for the reason it
    gives.
    """
    if not isinstance(custom_providers, list):
        raise HTTPException(status_code=400, detail="custom_providers must be a list")

    from src.llms.llm import LLM
    from src.server.services.oauth_providers import SUBSCRIPTIONS

    mc = LLM.get_model_config()
    builtin = set(mc.get_byok_eligible_providers())
    name_re = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")
    seen: set[str] = set()

    kept = _saved_pairs((stored or {}).get("custom_providers"), "parent_provider")

    for idx, cp in enumerate(custom_providers):
        if not isinstance(cp, dict):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: must be an object")

        name = cp.get("name")
        parent = cp.get("parent_provider")

        if not isinstance(name, str) or not name:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: name is required")
        if not isinstance(parent, str) or not parent:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: parent_provider is required")
        if not name_re.match(name):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: invalid name '{name}'")
        # A pair saved before its name became a built-in keeps it, and the
        # resolver keeps its own dialect (``route_custom_entry``).
        # A subscription's name never: its models would read as the sign-in's
        # (``on_subscription``), and none was saved before it became one.
        if name in builtin and ((name, parent) not in kept or name in SUBSCRIPTIONS):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: name '{name}' conflicts with built-in provider")
        if name in seen:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: duplicate name '{name}'")
        if parent not in builtin and (name, parent) not in kept:
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: parent_provider '{parent}' is not a BYOK-eligible provider")
        seen.add(name)

        ura = cp.get("use_response_api")
        if ura is not None and not isinstance(ura, bool):
            raise HTTPException(status_code=400, detail=f"custom_providers[{idx}]: use_response_api must be a boolean")


def _saved_pairs(entries: object, field: str) -> set[tuple[str, str]]:
    """``(name, entry[field])`` for each saved entry whose two values are strings.

    An entry saved before passes the provider check unchanged. A save carries
    the whole list, so one whose provider has since left the manifest, gone
    platform-only or lost BYOK eligibility would otherwise block every later
    change to the user's preferences.
    """
    if not isinstance(entries, list):
        return set()
    return {
        (entry["name"], entry[field])
        for entry in entries
        if isinstance(entry, dict)
        and isinstance(entry.get("name"), str)
        and isinstance(entry.get(field), str)
    }
