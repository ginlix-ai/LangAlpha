"""A gateway's models as its own catalog describes them, read when they are
listed instead of kept in the manifest.

A provider declaring ``model_catalog`` (OpenCode Go) serves many vendors'
models, each on the API the gateway serves it natively on, and adds and drops
them faster than a manifest release. Its list names what the key can call; the
catalog (models.dev, which OpenCode reads for its own models) says for each
which API to call it on (``provider.npm``), its levels, window, inputs and
prices. Each listed model leaves here as a whole custom entry: the route to save
it under and the settings that API needs. A model the catalog does not describe
still runs, on the fallback route, untuned. What the manifest tunes for the
same model (prompt guidance, compaction) the turn inherits from it by name, as
an OpenRouter pick does (``shadowed_model``).

Reading and describing only; the service caches the section.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.llms.gateway_names import gateway_slug, manifest_slug_index
from src.llms.model_spec import canonical_reasoning_efforts, default_reasoning_effort, wire_api

from .wire import FETCH_TIMEOUT_S, OPENAI, ListFetchError, ListRequest, fetch_list

logger = logging.getLogger(__name__)

# models.dev's whole catalog is one file, about 6 MB in 2026.
MAX_CATALOG_BYTES = 32 * 1024 * 1024

#: Per API a route speaks: where a level goes, and what a reasoning model needs
#: sent to reason on it. ``max_tokens`` is the catalog's output limit, which
#: Anthropic's API requires and Chat Completions otherwise leaves to the vendor.
_API_SHAPES: dict[str, dict[str, Any]] = {
    "anthropic": {
        "write": "parameters.output_config.effort",
        "reasoning_parameters": {"thinking": {"type": "adaptive"}},
        "max_tokens": True,
    },
    "openai-chat": {
        "write": "parameters.reasoning_effort",
        "reasoning_parameters": {},
        "max_tokens": True,
    },
    # Chat Completions through the client that keeps a model's
    # ``reasoning_content`` and sends it back, which the gateway's chat models
    # return and expect between tool calls.
    "glm": {
        "write": "parameters.reasoning_effort",
        "reasoning_parameters": {},
        "max_tokens": True,
    },
    # Not stored server side, so the reasoning comes back encrypted to send on
    # the next turn, as on our own Responses rows.
    "openai-responses": {
        "write": "parameters.reasoning.effort",
        "reasoning_parameters": {
            "reasoning": {"summary": "auto"},
            "include": ["reasoning.encrypted_content"],
            "store": False,
        },
        "max_tokens": False,
    },
    "gemini": {
        "write": "parameters.thinking_level",
        "reasoning_parameters": {"include_thoughts": True},
        "max_tokens": False,
    },
}


async def fetch_section(config: dict) -> dict | None:
    """The gateway's section of its catalog, or None when it cannot be read,
    which lists the gateway's models untuned."""
    url = config["url"]
    try:
        payload = await fetch_list(
            ListRequest(shape=OPENAI, url=url, headers={}),
            timeout=FETCH_TIMEOUT_S * 2,
            max_bytes=MAX_CATALOG_BYTES,
        )
    except ListFetchError as e:
        logger.warning("[model_catalog] could not read %s: %s", url, e)
        return None
    section = payload.get(config["provider"]) if isinstance(payload, dict) else None
    return section if isinstance(section, dict) and isinstance(section.get("models"), dict) else None


def _dict(value: Any) -> dict:
    """``value`` where it is an object, else an empty one: the catalog is
    edited by anyone, and one model's odd field must not fail the list."""
    return value if isinstance(value, dict) else {}


def _route(catalog: dict, section: dict | None, described: dict | None) -> str:
    npm = _dict(_dict(described).get("provider")).get("npm") or _dict(section).get("npm")
    return (catalog["routes"].get(npm) if isinstance(npm, str) else None) or catalog["fallback_route"]


def _pricing(cost: Any) -> dict | None:
    if not isinstance(cost, dict):
        return None
    out = {k: float(cost[k]) for k in ("input", "output", "cache_read", "cache_write")
           if isinstance(cost.get(k), (int, float)) and not isinstance(cost.get(k), bool)}
    return out or None


def described_model(listed: dict, catalog: dict, section: dict | None, mc, slugs: dict[str, str]) -> dict:
    """``listed`` as a whole entry: the route it runs on, and what the catalog
    and the manifest's own row of the same model say about it."""
    described = _dict(_dict(section).get("models")).get(listed["id"])
    route = _route(catalog, section, described)
    out = {**listed, "route": route}
    if not isinstance(described, dict):
        return out

    # Tuned by the catalog's own description, which the picker lists as ready.
    out["described"] = True
    if isinstance(described.get("name"), str) and described["name"]:
        out["display_name"] = described["name"]
    limit = _dict(described.get("limit"))
    if type(limit.get("context")) is int and limit["context"] > 0:
        out["context_window"] = limit["context"]
    inputs = _dict(described.get("modalities")).get("input")
    if isinstance(inputs, list) and inputs:
        out["input_modalities"] = [m for m in inputs if isinstance(m, str)]
    if pricing := _pricing(described.get("cost")):
        out["pricing"] = pricing

    # The manifest's row of the same model, matched as the turn matches it
    # (``shadowed_model``). Its usual level is lent here, where the catalog's
    # ladder has it: a route on another API borrows none of the row's ladder at
    # turn time (``borrows_reasoning``).
    manifest_key = slugs.get(gateway_slug(listed["id"]))
    tuned = mc.get_model_config(manifest_key) if manifest_key else None
    if tuned:
        out["known"] = True
        out["manifest_key"] = manifest_key

    shape = _API_SHAPES.get(wire_api(mc.get_provider_info(route)))
    if shape is None:
        return out
    parameters: dict[str, Any] = {}
    if shape["max_tokens"] and type(limit.get("output")) is int and limit["output"] > 0:
        parameters["max_tokens"] = limit["output"]
    if described.get("reasoning"):
        parameters.update(shape["reasoning_parameters"])
        options = described.get("reasoning_options")
        levels = next(
            (o.get("values") for o in (options if isinstance(options, list) else [])
             if isinstance(o, dict) and o.get("type") == "effort"),
            None,
        )
        efforts = canonical_reasoning_efforts(levels)
        if efforts:
            usual = _dict(_dict(tuned).get("reasoning")).get("default")
            out["reasoning"] = {
                "efforts": list(efforts),
                "default": default_reasoning_effort(efforts, usual),
                "write": shape["write"],
            }
    if parameters:
        out["parameters"] = parameters
    return out


@dataclass(frozen=True)
class GatewayCatalog:
    """A gateway's ``model_catalog`` declaration and its section, as read."""

    config: dict
    section: dict | None

    def describe(self, models: list[dict] | None, mc) -> list[dict] | None:
        """``models`` each as ``described_model`` makes it. Where the gateway's
        own list could not be read, the catalog's stands in for it; None where
        neither could be."""
        if models is None:
            if self.section is None:
                return None
            models = [{"id": model_id, "display_name": model_id} for model_id in self.section["models"]]
        slugs = manifest_slug_index(mc)
        return [described_model(m, self.config, self.section, mc, slugs) for m in models]
