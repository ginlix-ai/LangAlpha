"""A normalized list matched against the manifest: which manifest models a
credential serves, which of them the list names, and what else it lists."""

from __future__ import annotations

from typing import NotRequired, TypedDict

from src.llms.gateway_names import gateway_slug, manifest_slug_index
from src.llms.model_spec import borrows_reasoning, canonical_reasoning_efforts, reasoning_block, wire_api

from .catalog import GatewayCatalog
from .wire import OPENROUTER, is_chat_model

# Fallback reasons the match itself gives.
NO_MATCH = "no_match"  # the list names no manifest model
EMPTY_LIST = "empty_list"


def served_models(provider: str, mc, *, holder: str | None = None, base_url_saved: bool = False) -> dict[str, list[str]]:
    """``model_id -> [manifest keys]`` a turn would serve with this credential.

    A subscription serves its own entries (and, once the manifest marks base
    entries with ``subscription``, those). A key stored under ``holder`` serves
    each visible model whose turn would find it there and be allowed to use it,
    the walk ``byok_candidates`` / ``key_fits`` decide for a turn.
    """
    from src.server.services.llm.byok import byok_candidates, key_fits

    holder = holder or provider
    oauth = mc.get_provider_info(provider).get("access_type") == "oauth"
    out: dict[str, list[str]] = {}
    for key, entry in mc.llm_config.items():
        if not entry or not entry.get("visible"):
            continue
        model_provider = entry.get("provider")
        if oauth:
            serves = model_provider == provider or entry.get("subscription") == provider
        else:
            info = mc.get_provider_info(model_provider)
            serves = (
                bool(info)
                and not info.get("platform")
                and info.get("access_type") != "oauth"
                and holder in byok_candidates(model_provider, mc)
                and key_fits(model_provider, holder, has_base_url=base_url_saved, mc=mc)
            )
        if serves:
            out.setdefault(entry.get("model_id") or key, []).append(key)
    if not oauth:
        _add_brand_rows(out, provider, mc)
    return out


def _add_brand_rows(out: dict[str, list[str]], provider: str, mc) -> None:
    """A variant with no models of its own (a coding plan, or a region of one)
    offers those of the provider ``models_from`` names. A brand's rows are each
    picked as an entry naming the variant, which runs on the variant's key and
    endpoint even where that key may not reach the rows' own
    (``own_api_key``); a plan's rows run under a region of it as they are,
    sent to that region's host. The same models the picker offers
    (``connectionBuiltIns`` in the web app)."""
    source = mc.models_from(provider)
    if source == provider:
        return
    for key, entry in mc.llm_config.items():
        if not entry or not entry.get("visible") or entry.get("provider") != source:
            continue
        keys = out.setdefault(entry.get("model_id") or key, [])
        if key not in keys:
            keys.append(key)


class ModelsResult(TypedDict):
    """What ``/providers/{p}/available-models`` answers, before ``cached``."""

    provider: str
    status: str  # "ok" or "fallback"
    reason: NotRequired[str | None]  # a fallback's only
    fetched_at: str | None
    ready: list[str]
    missing: list[str]
    context_windows: dict[str, int]
    others: list[dict]
    total: int
    cached: NotRequired[bool]


def build_result(
    provider: str,
    shape: str,
    models: list[dict] | None,
    mc,
    *,
    holder: str | None = None,
    base_url_saved: bool = False,
    reason: str | None = None,
    fetched_at: str | None = None,
    catalog: GatewayCatalog | None = None,
) -> ModelsResult:
    """Intersect a normalized list with the manifest, or fall back to it.

    ``models`` None is a list that could not be read, ``reason`` saying why.
    The fallback rule: no list, or a list naming none of the manifest models
    the credential serves (for OpenRouter, matched by name: an empty list),
    answers with every manifest model it serves, as before lists were read.
    A gateway's ``catalog`` describes each listed model, on every read like
    the OpenRouter annotation, and its own list stands in for a list that
    could not be read.
    """
    served = served_models(provider, mc, holder=holder, base_url_saved=base_url_saved)
    manifest_keys = [k for keys in served.values() for k in keys]
    offered = catalog.describe(models, mc) if catalog is not None else models
    listed = {m["id"]: m for m in offered or []}

    ready: list[str] = []
    context_windows: dict[str, int] = {}
    for model_id, keys in served.items():
        live = listed.get(model_id)
        if live is None:
            continue
        ready.extend(keys)
        if live.get("context_window"):
            context_windows.update({k: live["context_window"] for k in keys})

    if shape == OPENROUTER:
        index = manifest_slug_index(mc)
        others = []
        for m in models or []:
            if m["id"] in served:
                continue
            known = index.get(gateway_slug(m["id"]))
            if known and m.get("reasoning"):
                m = {**m, "reasoning": _tuned_ladder(m["reasoning"], mc.get_model_config(known))}
            others.append({**m, "known": known is not None, **({"manifest_key": known} if known else {})})
        if models is not None and not models:
            reason = EMPTY_LIST
    else:
        others = [m for m in offered or [] if m["id"] not in served]
        # Only a list that could have matched and did not is suspect; a provider
        # the manifest tunes nothing for has every model under ``others``.
        if models is not None and manifest_keys and not ready:
            reason = NO_MATCH

    if models is None or reason in (NO_MATCH, EMPTY_LIST):
        return ModelsResult(
            provider=provider,
            status="fallback",
            reason=reason,
            fetched_at=fetched_at,
            ready=manifest_keys,
            missing=[],
            context_windows={},
            others=others,
            total=len(manifest_keys) + len(others),
        )
    return ModelsResult(
        provider=provider,
        status="ok",
        fetched_at=fetched_at,
        ready=ready,
        missing=[k for k in manifest_keys if k not in ready],
        context_windows=context_windows,
        others=others,
        total=len(ready) + len(others),
    )


def _tuned_ladder(block: dict, manifest_entry: dict | None) -> dict:
    """A known model's OpenRouter ladder, held to the levels the manifest tunes
    it for and starting at the manifest's default.

    OpenRouter's list is what the route accepts; the manifest's is what the
    model is tuned to run at (a level it leaves out, it leaves out on purpose).
    A manifest offering none of OpenRouter's levels leaves OpenRouter's whole.
    """
    tuned = reasoning_block(manifest_entry)
    allowed = canonical_reasoning_efforts(tuned.get("efforts"))
    efforts = [e for e in block["efforts"] if e in allowed]
    if not efforts:
        return block
    out = {k: v for k, v in block.items() if k != "default"}
    out["efforts"] = efforts
    default = next((d for d in (tuned.get("default"), block.get("default")) if d in efforts), None)
    if default:
        out["default"] = default
    return out


def match_endpoint_models(models: list[dict], mc, *, route_api: str) -> dict:
    """Split a custom endpoint's list into the manifest models it serves and the rest.

    A gateway names a model its own way (``openai/gpt-6.1-sol``,
    ``GPT-6.1-Sol``), so ids meet the manifest by ``gateway_slug``. A matched
    model keeps the endpoint's id, which is what that endpoint is called with,
    and the manifest key as its name, so it inherits what the manifest declares
    for it. The rest are chat models the manifest does not know, offered for
    the user to describe.

    ``context`` is set only where the endpoint lists a smaller window than
    the manifest: a gateway can serve a model capped below what its lab
    offers, and compaction has to trigger before that cap, not the lab's.

    ``input_modalities`` is set only where the endpoint lists fewer inputs than
    the manifest declares: an entry naming them is what attachments read
    (``config.input_modalities``), so a vision model this endpoint serves as
    text only is not sent images.

    ``reasoning`` is an empty ladder where ``route_api``, the API the endpoint
    is called through, cannot take the model's effort control
    (``borrows_reasoning``). The server sends no level there, and the entry
    saying so is what keeps the effort menu from offering one.
    """
    index = manifest_slug_index(mc)
    recognized: list[dict] = []
    seen: set[str] = set()
    others: list[dict] = []
    for m in models:
        key = index.get(gateway_slug(m["id"]))
        if key and key not in seen:
            seen.add(key)
            entry = mc.get_model_config(key) or {}
            match = {"name": key, "model_id": m["id"], "display_name": entry.get("display_name") or key}
            listed, declared = m.get("context_window"), entry.get("context")
            if isinstance(listed, int) and isinstance(declared, int) and listed < declared:
                match["context"] = listed
            if fewer := _fewer_inputs(m.get("input_modalities"), entry.get("input_modalities")):
                match["input_modalities"] = fewer
            own_api = wire_api(mc.get_provider_info(entry.get("provider")))
            if reasoning_block(entry) and not borrows_reasoning(entry, route_api, own_api):
                match["reasoning"] = {"efforts": []}
            recognized.append(match)
        elif not key and is_chat_model(m["id"]):
            others.append(m)
    return {"recognized": recognized, "others": others}


#: A list's word for an input, where it is not the manifest's (OpenRouter's
#: ``file`` is a PDF).
_LISTED_INPUT = {"file": "pdf"}


def _fewer_inputs(listed: list[str] | None, declared: list[str] | None) -> list[str] | None:
    """The declared inputs the list also names, when that is fewer; else None.
    A list that names none says nothing either way."""
    if not listed or not declared:
        return None
    offered = {_LISTED_INPUT.get(x, x) for x in listed}
    kept = [x for x in declared if x == "text" or x in offered]
    return kept if kept != list(declared) else None


def filter_others(result: ModelsResult, q: str | None) -> ModelsResult:
    """Keep the ``others`` whose id or name holds every word of ``q``."""
    words = (q or "").lower().split()
    if not words:
        return result
    others = [
        m for m in result["others"]
        if all(w in f"{m['id']} {m.get('display_name', '')}".lower() for w in words)
    ]
    return {**result, "others": others}
