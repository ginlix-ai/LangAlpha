import copy
import functools
import inspect
import logging
import os
import json
import sys
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Dict, Optional, Any
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.language_models import BaseChatModel
from langchain_deepseek import ChatDeepSeek
from langchain_qwq import ChatQwen

from .endpoints import anthropic_sdk_base_url, is_official_openai_endpoint
from .model_spec import (
    ModelSpec,
    canonical_reasoning_efforts,
    clamp_reasoning_effort,
    client_wire,
    default_reasoning_effort,
    reasoning_block,
)
from .preferences import compaction_profile_for
from .pricing_utils import get_price_tier
from .provider_compaction import native_compaction_kind
from .reasoning import apply_reasoning_effort
from .reasoning_lineage import ROUTE_ENDPOINT_SEP
from .task_costs import task_cost

load_dotenv()

logger = logging.getLogger(__name__)


class MissingProviderKeyError(ValueError):
    """The provider's key is in no override and not in the environment.

    A ValueError still, for callers that already catch one; its own class lets
    a resolver tell an unconfigured provider from a bad argument.
    """


class ModelConfig:
    """Manages model configuration from JSON files."""

    def __init__(self):
        # Load models.json for model parameters
        llm_config_path = Path(__file__).parent / "manifest" / "models.json"
        with open(llm_config_path, 'r') as f:
            self.llm_config = json.load(f)

        # Load providers.json for token tracking and provider info
        manifest_path = Path(__file__).parent / "manifest" / "providers.json"
        with open(manifest_path, 'r') as f:
            self.manifest = json.load(f)

        # Flatten grouped provider_config into a flat dict for downstream access.
        # Raw self.manifest stays pristine for grouped UI views.
        self._flat_providers = self._flatten_providers(
            self.manifest.get("provider_config", {})
        )

        # Precompute parent → [child variants] map once. ``get_child_variants``
        # is on the chat hot path (BYOK sibling walk); scanning _flat_providers
        # on every call is wasteful since the manifest is static.
        self._child_variants: dict[str, list[str]] = {}
        for name, cfg in self._flat_providers.items():
            parent = cfg.get("parent_provider")
            if not parent or parent == name:
                continue
            if cfg.get("platform", False):
                continue
            self._child_variants.setdefault(parent, []).append(name)

        self._providers_with_models = frozenset(
            info["provider"]
            for info in self.llm_config.values()
            if info and info.get("visible") and info.get("provider")
        )

        # A gateway's catalog routes each model to one of its APIs, each a
        # variant of the gateway: the same connection, called another way.
        self._connections: dict[str, str] = {}
        for name, info in self._flat_providers.items():
            catalog = info.get("model_catalog")
            if catalog and not info.get("parent_provider"):
                for route in [*catalog["routes"].values(), catalog["fallback_route"]]:
                    if route != name:
                        self._connections[route] = name

    @staticmethod
    def _flatten_providers(grouped: dict) -> dict:
        """Flatten grouped provider_config into a flat dict.

        Handles two patterns:
        - Pattern A: group key IS a complete provider, variants override fields
        - Pattern B: group key is a brand container, default variant shares group key
        """
        flat = {}
        for group_key, config in grouped.items():
            variants = config.get("variants")  # don't mutate manifest
            # ``group``: the key each provider sits under. A vendor's regions and
            # plans are separate providers (``z-ai-cn-coding`` names ``z-ai-cn``
            # as its parent), so a picker groups them by this instead.
            shared = {**{k: v for k, v in config.items() if k != "variants"}, "group": group_key}

            if not variants:
                flat[group_key] = shared
                continue

            has_self_variant = group_key in variants
            for vkey, overrides in variants.items():
                merged = {**shared, **overrides, "group": group_key}
                # A list URL belongs to the base URL declared beside it, so a
                # variant that moves its base (a coding endpoint) lists there.
                if "base_url" in overrides and "models_url" not in overrides:
                    merged.pop("models_url", None)
                if vkey != group_key:
                    # setdefault: a variant may declare an explicit parent_provider
                    merged.setdefault("parent_provider", group_key)
                flat[vkey] = merged

            if not has_self_variant:
                flat[group_key] = shared

        # Post-flatten validation: every entry must have an sdk field
        # (platform variants are exempt — they inherit sdk at runtime from parent)
        for key, entry in flat.items():
            models_url = entry.get("models_url")
            if models_url is not None and not (
                isinstance(models_url, str)
                and models_url.startswith(("https://", "http://"))
            ):
                raise ValueError(f"Provider '{key}' models_url must be an http(s) URL")
            if entry.get("platform"):
                continue
            if "sdk" not in entry:
                raise ValueError(
                    f"Provider '{key}' missing 'sdk' after flatten. "
                    f"Check providers.json — Pattern B providers must have "
                    f"a self-variant with the same key as the group."
                )

        return flat

    def get_model_config(self, model_id: str) -> Optional[Dict]:
        """Get model configuration from llm_config."""
        return self.llm_config.get(model_id)

    @property
    def flat_providers(self) -> Dict[str, Dict]:
        """Public accessor for the flattened provider dict."""
        return self._flat_providers

    def get_provider_info(self, provider: str) -> Dict:
        """Get provider configuration from the flattened provider dict."""
        return self._flat_providers.get(provider, {})

    def get_model_info(self, provider: str, model_id: str) -> Optional[Dict[str, Any]]:
        """Get full model information from manifest by provider and model_id.

        Args:
            provider: Provider name (e.g., 'openai', 'anthropic', 'xai')
            model_id: Model ID (e.g., 'gpt-5', 'claude-opus-4', 'grok-4.7')

        Returns:
            Model info dictionary with pricing, parameters, etc., or None if not found
        """
        models = self.manifest["models"].get(provider, [])
        for model in models:
            if model["id"] == model_id:
                return model
        return None

    def get_byok_eligible_providers(self) -> list[str]:
        """Return list of provider names that have byok_eligible=true.

        Includes all access types (api_key, oauth, coding_plan) since all
        represent user-provided model access for credit tracking purposes.
        Excludes platform variants (system-only proxy routes).
        """
        return [
            name
            for name, cfg in self._flat_providers.items()
            if cfg.get("byok_eligible", True) and not cfg.get("platform", False)
        ]

    def get_parent_provider(self, provider: str) -> str:
        """Return the parent provider name (self if no parent)."""
        info = self.get_provider_info(provider)
        return info.get("parent_provider", provider)

    def models_from(self, provider: str) -> str:
        """The provider whose manifest models ``provider`` offers.

        Its own when it has any, else its nearest ancestor's: a coding plan with
        none offers its brand's, and a region of a plan offers the plan's, whose
        model ids its brand's rows may not share (Kimi Code's ``k3.1-pro``).
        """
        seen: set[str] = set()
        current = provider
        while current not in seen and current not in self._providers_with_models:
            seen.add(current)
            parent = self.get_parent_provider(current)
            if parent == current:
                return provider
            current = parent
        return current if current in self._providers_with_models else provider

    def runs_manifest_models(self, provider: str) -> bool:
        """Whether manifest models name ``provider`` itself, and so run on its
        deployment key. A gateway's models are all the user's picks, and a
        plan that offers its brand's runs them as picks too, while the brand's
        own rows read the brand's key."""
        return provider in self._providers_with_models

    def plan_regions(self, provider: str) -> list[str]:
        """The other regions of the coding plan ``provider`` is a region of.
        A plan runs in one region at a time, so a key saved in one replaces
        theirs; a provider that is no plan's region has none."""
        info = self._flat_providers.get(provider, {})
        if info.get("access_type") != "coding_plan":
            return []
        return [
            name for name, other in self._flat_providers.items()
            if name != provider
            and other.get("group") == info.get("group")
            and other.get("access_type") == "coding_plan"
            and other.get("byok_eligible", True)
            and not other.get("platform")
            and self.connection_of(name) == name
        ]

    def connection_of(self, provider: str) -> str:
        """The provider whose connection ``provider`` is one API of: a route
        its gateway's ``model_catalog`` names, which takes the gateway's key
        and whose models are the gateway's picks. Any other provider,
        a region included, is its own connection."""
        return self._connections.get(provider, provider)

    def get_child_variants(self, provider: str) -> list[str]:
        """Return provider names whose parent_provider is ``provider``.

        Excludes the provider itself; platform-only variants are excluded
        since BYOK keys are never stored under them. O(1) lookup against
        the precomputed map in ``__init__``.
        """
        return list(self._child_variants.get(provider, ()))

    def shares_api_key(self, provider: str, other: str) -> bool:
        """Whether a key issued for ``provider`` is also valid for ``other``.

        A manifest family shares one key unless a variant declares
        ``"own_api_key": true``: its vendor issues that variant's keys apart
        from its parent's (a coding-plan key against the platform endpoint, or
        one region's key against another's), so the other side rejects them.
        Such a variant is the root of its own credential and its descendants
        stay with it, which is why this walks up to the nearest declaring
        ancestor rather than comparing the two flags.
        """
        return self._key_issuer(provider) == self._key_issuer(other)

    def _key_issuer(self, provider: str) -> str:
        seen: set[str] = set()
        while provider not in seen:
            seen.add(provider)
            info = self._flat_providers.get(provider, {})
            parent = info.get("parent_provider")
            if info.get("own_api_key") is True or not parent:
                return provider
            provider = parent
        return provider

    def get_display_name(self, provider: str) -> str:
        """Return display name, preferring own name then resolving through parent."""
        info = self.get_provider_info(provider)
        if info.get("display_name"):
            return info["display_name"]
        parent = info.get("parent_provider", provider)
        if parent != provider:
            parent_info = self.get_provider_info(parent)
            return parent_info.get("display_name", parent.title())
        return provider.title()

    def _task_cost_key(
        self, model_name: str, model_info: dict[str, Any], provider_info: dict[str, Any]
    ) -> str:
        """The manifest key whose task cost a model shows.

        A subscription runs the same model on the same tokens as its keyed twin,
        the parent provider's entry for the same model id, so it shows that
        figure as a guide to how hard a task draws on the plan. A regional or
        other keyed variant is priced on its own terms and keeps its own key.
        """
        if provider_info.get("access_type") != "oauth":
            return model_name
        parent = provider_info.get("parent_provider")
        model_id = model_info.get("model_id", model_name)
        for key, info in self.llm_config.items():
            if info and info.get("provider") == parent and info.get("model_id", key) == model_id:
                return key
        return model_name

    def get_model_metadata(self) -> dict[str, dict[str, Any]]:
        """Return {model_key: {sdk, provider, access_type, ...}} for all visible models."""
        result = {}
        for model_name, model_info in self.llm_config.items():
            if not model_info or not model_info.get("visible", False):
                continue
            provider = model_info.get("provider", "unknown")
            provider_info = self.get_provider_info(provider)
            sdk = provider_info.get("sdk", "unknown")
            access_type = provider_info.get("access_type", "api_key")
            entry: dict[str, Any] = {"sdk": sdk, "provider": provider, "access_type": access_type}
            # Only include tier when explicitly set — absence means "not platform-managed"
            if "tier" in model_info:
                entry["tier"] = model_info["tier"]
            # OAuth plan allowlist — the connected subscription's plan_type must
            # be one of these for the model to be usable (absence = no gate).
            if "oauth_plans" in model_info:
                entry["oauth_plans"] = model_info["oauth_plans"]
            # The name the provider knows it by, where that is not the key: an
            # entry the user saves for it under another connection sends this.
            if model_info.get("model_id") and model_info["model_id"] != model_name:
                entry["model_id"] = model_info["model_id"]
            # Optional editorial metadata for the model-detail flyout. Additive —
            # only surfaced for models that authored it in models.json; the
            # frontend renders only the rows that are present. `display_name` is
            # what the pickers print in place of the key.
            for key in (
                "display_name",
                "speed",
                "intelligence",
                "context",
                "input_modalities",
            ):
                if key in model_info:
                    entry[key] = model_info[key]
            # Reasoning levels this model actually honors. Absent = no selector
            # at all, which is the truthful answer for a model with no reasoning
            # parameters — the frontend must not synthesize a default ladder.
            efforts = self.get_reasoning_efforts(model_name)
            if efforts:
                entry["reasoning_efforts"] = efforts
                entry["reasoning_effort_default"] = self.get_reasoning_effort_default(
                    model_name
                )
            # What the model itself asks for when the user has picked nothing —
            # the frontend prints it so "Default" names a level rather than an
            # unknown. Absent means the fail-safe (detailed) applies.
            guidance = self.get_prompt_guidance(model_name)
            if guidance:
                entry["prompt_guidance"] = guidance
            # Same contract for the compaction preset: the frontend prints it
            # so a per-model row names its default instead of showing a bare
            # "Default". Derived here, never in the client, so the band table
            # has one home.
            profile = self.get_compaction_profile(model_name)
            if profile:
                entry["compaction_profile"] = profile
            # Whose compaction the model takes, so the settings offer the
            # choice only where it does something and name that provider's
            # trade-off. Absent means its provider cannot compact for it.
            native = native_compaction_kind(model_info, provider_info)
            if native:
                entry["native_compaction"] = native
            # Cost tier (1-5) derived live from canonical providers.json pricing —
            # not stored in models.json, so it auto-syncs when prices change.
            price_tier = get_price_tier(model_info.get("model_id", model_name), provider)
            if price_tier is not None:
                entry["price"] = price_tier
            # What a typical benchmark task costs at each level the model
            # offers, against a baseline model (see task_costs.py).
            costs = task_cost(self._task_cost_key(model_name, model_info, provider_info), efforts)
            if costs is not None:
                entry["task_cost"] = costs
            # Mark variants that require their own API key (different env_key from parent).
            # e.g. z-ai-cn needs ZAI_CN_API_KEY, not ZAI_API_KEY.
            parent_provider = provider_info.get("parent_provider")
            if parent_provider:
                parent_info = self.get_provider_info(parent_provider)
                if provider_info.get("env_key") != parent_info.get("env_key"):
                    entry["requires_own_key"] = "true"
            result[model_name] = entry
        return result

    def get_input_modalities(self, model_name: str) -> list[str]:
        """Get supported input modalities for a model. Defaults to ["text"].

        Reads directly from models.json entries, so variant providers
        (codex-oauth, claude-oauth, etc.) are resolved correctly without
        needing parent-provider fallback.
        """
        model_config = self.llm_config.get(model_name)
        if not model_config:
            return ["text"]
        return model_config.get("input_modalities", ["text"])

    def get_reasoning_efforts(self, model_name: str) -> list[str]:
        """Reasoning levels this model actually honors, in canonical order.

        Empty means the model has no reasoning control at all, which is the
        truthful answer for an entry with no reasoning parameters and the
        reason this returns a list rather than a default ladder.
        """
        entry = self.llm_config.get(model_name) or {}
        return list(canonical_reasoning_efforts(reasoning_block(entry).get("efforts")))

    def get_reasoning_effort_default(self, model_name: str) -> str | None:
        """Manifest-entry wrapper over :func:`default_reasoning_effort`."""
        entry = self.llm_config.get(model_name) or {}
        return default_reasoning_effort(
            self.get_reasoning_efforts(model_name),
            reasoning_block(entry).get("default"),
        )

    def resolve_reasoning_effort(self, model_name: str, requested: str) -> str | None:
        """Manifest-entry wrapper over :func:`clamp_reasoning_effort`.

        Unhonored input is normal here rather than a bug: the account-wide
        default is chosen with no model in hand, and stored preferences outlive
        manifest edits.
        """
        return clamp_reasoning_effort(
            self.get_reasoning_efforts(model_name),
            self.get_reasoning_effort_default(model_name),
            requested,
        )

    def get_prompt_guidance(self, model_name: str) -> str | None:
        """How much prompt scaffolding this model needs: ``"lean"``,
        ``"detailed"``, or None when the manifest does not say.

        Deliberately separate from the ``intelligence`` score, which is
        editorial copy for the model picker — a wording change there must not
        move agent behavior. Reads the raw entry (no ``visible`` gate) because
        Background, compaction and fetch models are not all listed in the picker.
        """
        model_config = self.llm_config.get(model_name)
        if not model_config:
            return None
        value = model_config.get("prompt_guidance")
        return value if value in ("lean", "detailed") else None

    def get_compaction_profile(self, model_name: str) -> str | None:
        """Manifest-entry wrapper over :func:`compaction_profile_for`.

        The rule itself lives in ``preferences`` so a user-defined model, which
        has no manifest row to name here, resolves by the same one.
        """
        return compaction_profile_for(self.llm_config.get(model_name))


_UNSET = object()  # Sentinel to distinguish "no override" from "override to None"

# How long an OpenAI-SDK stream may go without a chunk before the call fails.
# The SDK's own default is 120s, and a model can be silent that long while it
# writes one large tool call whose arguments its provider does not stream, so
# real calls failed at it, and failed again on every retry. 600s matches the
# read timeout, which keepalive frames reset and so cannot catch this alone.
# It bounds the wait for the first chunk too, so a timeout is retried once
# rather than the full count (``is_stalled_stream``).
STREAM_CHUNK_TIMEOUT_S = 600.0

# Header spellings the first-party codex clients send — codex-rs:
# session-id/thread-id, opencode/hermes: session_id.
_CODEX_SESSION_HEADERS = ("session-id", "thread-id", "session_id")


def _derive_codex_affinity(cache_key: str) -> str:
    """UUID-normalize a cache key for a session header (codex's, or one a
    provider names with ``session_header``): pass-through for UUID keys, uuid5
    otherwise so raw key material never egresses in a header."""
    try:
        return str(uuid.UUID(str(cache_key)))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-session:{cache_key}"))


def _client_service_tier(client) -> str | None:
    """The service tier the built client will actually send, if any.

    Read off the client rather than off the parameters we assembled: an SDK with
    no first-class field relocates the value into ``model_kwargs``, and one that
    discards it never sends it at all.
    """
    tier = getattr(client, "service_tier", None)
    if tier:
        return tier
    return (getattr(client, "model_kwargs", None) or {}).get("service_tier")


def _merged_default_headers(params: dict, *bases: dict | None) -> dict:
    """Merge header sources left-to-right (later wins), ending with any
    ``parameters``-level ``default_headers`` already expanded into ``params``
    so it augments the base headers instead of replacing the mapping."""
    merged: dict = {}
    for base in bases:
        merged.update(base or {})
    merged.update(params.get("default_headers") or {})
    return merged

# Constructor keys a model's ``parameters`` can use to move its endpoint: the
# OpenAI family's ``base_url``/``openai_api_base``/``api_base``, Anthropic's
# ``anthropic_api_url``, Gemini's ``client_options``, OpenRouter's
# ``openrouter_api_base``. A custom model's parameters are its user's, so each
# of these is a URL the user supplied.
ENDPOINT_PARAM_KEYS = (
    "base_url",
    "openai_api_base",
    "api_base",
    "openrouter_api_base",
    "anthropic_api_url",
    "client_options",
)

# Constructor keys that choose how a client connects: a proxy, an http client,
# a prebuilt SDK client. A client held to an egress transport drops them, since
# each would route around it, and ChatOpenAI refuses a proxy beside an http
# client outright.
CONNECTION_PARAM_KEYS = frozenset({
    "openai_proxy",
    "anthropic_proxy",
    "client_args",
    "http_client",
    "http_async_client",
    "client",
    "async_client",
    "root_client",
    "root_async_client",
})


def param_levels(params: Mapping | None) -> list[tuple[str, Mapping]]:
    """Each level of ``parameters`` a client constructor reads keys from, with
    the prefix that names it in a message.

    Every LangChain chat model the factory builds promotes a ``model_kwargs``
    key that names one of its fields to that field (``_build_model_kwargs``),
    so a key there routes the client as it would at the top. It promotes
    once, and a ``model_kwargs`` inside it is refused, so there is no third.
    """
    if not isinstance(params, Mapping):
        return []
    levels: list[tuple[str, Mapping]] = [("", params)]
    nested = params.get("model_kwargs")
    if isinstance(nested, Mapping):
        levels.append(("model_kwargs.", nested))
    return levels


def _drop_connection_params(params: dict, model: str) -> None:
    """Drop the routing a held client cannot keep, at either level. A row saved
    before these were refused at save time still carries them, and the call
    runs without."""
    dropped = sorted(key for key in CONNECTION_PARAM_KEYS if params.pop(key, None) is not None)
    nested = params.get("model_kwargs")
    if isinstance(nested, Mapping) and not CONNECTION_PARAM_KEYS.isdisjoint(nested):
        dropped += sorted(
            f"model_kwargs.{key}"
            for key, value in nested.items()
            if key in CONNECTION_PARAM_KEYS and value is not None
        )
        # A new mapping: the one in ``params`` is the model's own ``parameters``.
        params["model_kwargs"] = {k: v for k, v in nested.items() if k not in CONNECTION_PARAM_KEYS}
    if dropped:
        logger.warning(
            "Model %r is held to the egress transport; ignoring parameters %s",
            model,
            ", ".join(dropped),
        )


# Name regex for custom models: alphanumeric start, then alphanumeric/./_/-/:
# Colon allowed for Ollama-style name:tag format (e.g. gemma4:31b)
CUSTOM_MODEL_NAME_RE = r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$"


class LLM:
    """Factory class for creating LangChain LLM clients."""

    # Class-level model config instance
    _model_config = None

    # No hold unless a caller asks for one, including on an instance that was
    # put together without ``__init__``.
    egress_transport: Callable[[str], Any] | None = None

    @classmethod
    def get_model_config(cls) -> ModelConfig:
        """Get or create the model configuration singleton."""
        if cls._model_config is None:
            cls._model_config = ModelConfig()
        return cls._model_config

    def __init__(
        self,
        model: str | ModelSpec,
        api_key: str | None = None,
        base_url_override=_UNSET,
        reasoning_effort: str | None = None,
        *,
        egress_transport: Callable[[str], Any] | None = None,
        **override_params,
    ):
        """
        Initializes the LLM factory.

        Args:
            model: A models.json key, or a :class:`ModelSpec` for a model that
                has no manifest row (user-defined).
            api_key: Optional API key override (e.g. from BYOK).
            base_url_override: Override base URL. Use _UNSET (default) for no override,
                None to clear to SDK default, or a string for a custom URL.
            reasoning_effort: Optional reasoning effort level ("low", "medium", "high").
            egress_transport: Builds the transport every request of the client
                is sent through, given its SDK family ("openai", "anthropic",
                "gemini" or "openrouter"). None leaves each SDK its own; whether an endpoint
                needs one is the caller's call, not the factory's.
            **override_params: Additional parameters to override defaults.
        """
        self.egress_transport = egress_transport
        self.model_config = self.get_model_config()
        spec = (
            model
            if isinstance(model, ModelSpec)
            else ModelSpec.from_manifest(self.model_config, model)
        )

        self.custom_model_name = spec.name  # Store the custom name
        self.model = spec.model_id  # Use model_id for API calls
        self.provider = spec.provider

        # System serving: route through platform proxy when no BYOK key. An
        # empty key is still the user's (a keyless endpoint of theirs).
        if api_key is None and spec.system_provider:
            self.provider = spec.system_provider

        # Deep copy: reasoning-effort overrides mutate nested dicts (e.g.
        # extra_body.thinking); a shallow copy would contaminate the spec, and
        # through it the process-wide manifest, for every subsequent request.
        self.parameters = copy.deepcopy(spec.parameters)
        self.extra_body = copy.deepcopy(spec.extra_body)
        self.profile_overrides = spec.profile

        # Apply reasoning effort override (before provider resolution).
        # Validation lives here because this is the only point that sees both
        # the requested level and the model's declared enum; past it the mapper
        # is pure translation. A level the model does not honor steps down to
        # the nearest one it does, rather than reaching the provider as a hard
        # error; only a level outside the vocabulary falls back to the default.
        # The clamp reads the spec's own ladder rather than a manifest lookup,
        # which is what a user-defined model would miss.
        # The resolved level, not the requested one, is what the call reports:
        # a request above the model's ladder is stepped down here, so the two
        # differ. With no request at all the model's own default resolves here
        # and takes the same path, so it is reported and sent as one value.
        if reasoning_effort:
            effort = clamp_reasoning_effort(
                spec.reasoning_efforts, spec.reasoning_effort_default, reasoning_effort
            )
        else:
            # Not a request, so it is written *under* the caller's own
            # parameters. The manifest used to seed its default level into
            # `parameters`, where an override naming that key directly replaced
            # it; the mapper now writes what the seed held, and inherits its
            # place in the order.
            effort = spec.reasoning_effort_default
            if effort:
                apply_reasoning_effort(
                    effort, self.parameters, self.extra_body, spec.reasoning_surface
                )

        # Override with any provided parameters
        self.parameters.update(override_params)

        # A level the caller asked for outright is the request being answered,
        # so it goes over the overrides -- where it already went when the mapper
        # ran on this side of the update and the seed sat underneath.
        if reasoning_effort and effort:
            apply_reasoning_effort(
                effort, self.parameters, self.extra_body, spec.reasoning_surface
            )
        self.resolved_reasoning_effort = effort

        # Store optional API key override (BYOK)
        self.api_key_override = api_key

        # Get provider info from manifest (empty for a provider it does not
        # list, and for the user's own provider under a name it does list).
        self.provider_info = (
            {} if spec.own_provider else self.model_config.get_provider_info(self.provider)
        )

        # Extract provider configuration. The SDK and Responses opt-in are read
        # the way ``wire_api`` reads them, which decides a shadow's ladder.
        self.sdk, self.use_response_api = client_wire(
            self.provider_info, spec.use_response_api_override, spec.sdk_fallback
        )
        self.env_key = self.provider_info.get("env_key")
        self.base_url = self.provider_info.get("base_url")

        # Apply base_url override (sentinel distinguishes "not set" from "set to None")
        if base_url_override is not _UNSET:
            self.base_url = base_url_override

        # A provider the manifest no longer lists (a custom model saved on a
        # removed provider) has no endpoint of its own. Built anyway, the client
        # would dial the fallback SDK's default host, sending the key to a
        # vendor that never issued it.
        if not self.provider_info and not self.base_url:
            raise ValueError(
                f"Provider '{self.provider}' of model '{spec.name}' is no longer "
                "available. Pick another provider for it in Settings."
            )

        self.use_previous_response_id = self.provider_info.get("use_previous_response_id", False) if self.sdk == "openai" else False
        # prompt_cache_key: opt-in for sdk="openai"; codex applies it always-on.
        self.prompt_cache_key_enabled = (
            bool(self.provider_info.get("prompt_cache_key", False)) if self.sdk == "openai" else False
        )

        # Optional default headers from provider config, with model-level beta merging
        self.default_headers = self.provider_info.get("default_headers")
        self._merge_additional_betas(spec.additional_betas)

    def _merge_additional_betas(self, additional_betas: list[str] | None) -> None:
        """Merge model-level additional_betas into the anthropic-beta header."""
        if not additional_betas:
            return
        existing_headers = self.default_headers or {}
        existing = existing_headers.get("anthropic-beta", "")
        merged = ",".join(filter(None, [existing, *additional_betas]))
        self.default_headers = {**existing_headers, "anthropic-beta": merged}

    @classmethod
    def from_custom_config(
        cls,
        config: dict,
        api_key: str | None = None,
        base_url_override=_UNSET,
        cache_key: str | None = None,
        reasoning_effort: str | None = None,
        *,
        egress_transport: Callable[[str], Any] | None = None,
        **override_params,
    ):
        """
        Create an LLM instance from an inline config dict (user-defined custom model).

        Bypasses models.json lookup — the caller supplies model_id, provider,
        parameters, and extra_body directly.

        Args:
            config: Dict with keys: model_id, provider, and optional parameters/extra_body.
            api_key: Optional API key override (e.g. from BYOK).
            base_url_override: Override base URL. _UNSET = no override.
            cache_key: Session-stable key sent as ``prompt_cache_key`` on
                OpenAI/Codex requests (always-on for Codex, which also derives
                session-affinity headers from it; opt-in for OpenAI).
            reasoning_effort: Optional reasoning effort level, clamped against
                the ladder the config declares, or the one it inherits from the
                built-in whose name it took.
            egress_transport: See :class:`LLM`.
            **override_params: Additional parameters to override defaults.

        Returns:
            A LangChain chat model instance.
        """
        instance = cls(
            ModelSpec.from_custom(config, cls.get_model_config()),
            api_key=api_key,
            base_url_override=base_url_override,
            reasoning_effort=reasoning_effort,
            egress_transport=egress_transport,
            **override_params,
        )
        return instance.get_llm(cache_key=cache_key)

    def _resolve_billing_type(self) -> str:
        """Determine billing type based on how this LLM was constructed.

        Returns one of:
        - "byok"     — user-provided API key
        - "oauth"    — user-provided OAuth token
        - "platform" — system key (platform pays, credits deducted)
        """
        if self.api_key_override is not None:
            if self.provider_info.get("access_type") == "oauth":
                return "oauth"
            return "byok"
        return "platform"

    def get_llm(self, cache_key: str | None = None):
        """Build the LangChain LLM client for the configured provider.

        ``cache_key`` becomes ``prompt_cache_key`` on OpenAI/Codex requests via
        ``model_kwargs`` (not ``bind()``) so it survives ``bind_tools()`` and
        ``with_structured_output()``. Codex always applies the key — and also
        derives session-affinity headers from it — while regular OpenAI applies
        it only when the provider opts in via providers.json.
        """
        # A provider that routes and caches by conversation names the header it
        # reads the thread from (OpenCode Go's ``x-opencode-session``). It
        # refuses a request without one, so a call outside any thread (a title,
        # an insight) is a session of its own.
        session_header = self.provider_info.get("session_header")
        if session_header:
            self.default_headers = {
                **(self.default_headers or {}),
                session_header: _derive_codex_affinity(cache_key) if cache_key else str(uuid.uuid4()),
            }

        effective_cache_key: str | None = None
        if cache_key and (
            self.sdk == "codex"
            or (self.sdk == "openai" and self.prompt_cache_key_enabled)
        ):
            effective_cache_key = str(cache_key)

        if self.sdk == "openai":
            client = self._get_openai_llm(cache_key=effective_cache_key)
        elif self.sdk == "codex":
            client = self._get_codex_llm(cache_key=effective_cache_key)
        elif self.sdk == "dashscope":
            client = self._get_dashscope_llm(cache_key=effective_cache_key)
        elif self.sdk == "deepseek":
            client = self._get_deepseek_llm()
        elif self.sdk == "openrouter":
            client = self._get_openrouter_llm()
        elif self.sdk == "glm":
            client = self._get_glm_llm()
        elif self.sdk == "qwq":
            client = self._get_qwq_llm()
        elif self.sdk == "anthropic":
            client = self._get_anthropic_llm()
        elif self.sdk == "gemini":
            client = self._get_gemini_llm()
        else:
            raise ValueError(f"Unsupported SDK: {self.sdk} for provider {self.provider}")

        # The entry's declarations win over the SDK's table, which most models
        # are missing from, so the two can't drift; capability flags the entry
        # does not declare are kept.
        if self.profile_overrides:
            base = client.profile if isinstance(client.profile, dict) else {}
            client.profile = {**base, **self.profile_overrides}

        # Tag the client with billing metadata so PerCallTokenTracker can
        # attribute each LLM call to the correct billing source, and with the
        # resolved provider so ReasoningCompatibilityMiddleware can tell whose
        # reasoning blocks these are. ``self.provider`` is read after the
        # system_provider reassignment above, so the stamp names the route the
        # request actually takes — langchain's own ``model_provider`` field
        # cannot be trusted here (ChatAnthropic reports "anthropic" for every
        # Anthropic-compatible shim).
        # ``manifest_model`` is the models.json key, not ``self.model`` (the API
        # model id) — it is what get_input_modalities() looks up. Middleware that
        # runs inside ModelResilienceMiddleware sees a substituted client, so a
        # capability read has to come off the client in hand rather than off a
        # name captured when the stack was built.
        # ``pricing_model_id``/``pricing_provider`` are the same identity a
        # manifest lookup would yield, carried explicitly because a user-defined
        # model's key is its display name and resolves to nothing. Their config
        # names the id and provider outright, which is a better billing source
        # than the vendor echo. ``provider_route`` cannot stand in: it qualifies
        # off-manifest endpoints by base_url to keep reasoning lineage separate,
        # so it identifies a trust boundary, not a set of rates.
        billing_type = self._resolve_billing_type()
        service_tier = _client_service_tier(client)
        existing = client.metadata or {}
        client.metadata = {
            **existing,
            "billing_type": billing_type,
            "provider_route": self._provider_route(),
            "manifest_model": self.custom_model_name,
            "pricing_model_id": self.model,
            "pricing_provider": self.provider,
            # Tuning that shapes this call rather than the turn. On the graph
            # run these would be the parent's, inherited unchanged by every
            # subagent call that ran a different model at a different level.
            #
            # The two are independent readings, not one signal. The effort is
            # the level that went out, stepped down from the request where the
            # ladder is shorter. The tier is present only when this client
            # carries the parameter, which most routes never pass at all, so its
            # absence says nothing about how the call was served.
            **(
                {"reasoning_effort": self.resolved_reasoning_effort}
                if self.resolved_reasoning_effort
                else {}
            ),
            **({"service_tier": service_tier} if service_tier else {}),
        }

        return client

    def _provider_route(self) -> str:
        """Identity of the upstream this client actually reaches.

        The provider key alone is not that identity. BYOK can repoint a built-in
        provider at another endpoint, and a custom Anthropic-shaped provider is
        rewritten onto its manifest parent to inherit the right SDK (#221) —
        both keep ``provider`` while changing the upstream. Since a reasoning
        signature is only verifiable by the endpoint that minted it, an
        off-manifest ``base_url`` is qualified into the route so it gets its own
        lineage instead of inheriting the manifest route's trust.
        """
        if self.base_url != self.provider_info.get("base_url"):
            return f"{self.provider}{ROUTE_ENDPOINT_SEP}{self.base_url}"
        return self.provider

    def _resolve_api_key(self) -> str:
        """Resolve API key: BYOK override > env var > local fallback.

        An empty override is a user's keyless endpoint (LM Studio, Ollama, vLLM)
        and gets the placeholder, never the deployment's key: the user chose
        where the request goes, so the env key would be sent to their server.
        """
        if self.api_key_override is not None:
            return self.api_key_override or "EMPTY"
        if self.env_key:
            key = os.getenv(self.env_key)
            if key:
                return key
            if self.provider_info.get("access_type") != "local":
                raise MissingProviderKeyError(f"{self.env_key} environment variable is not set")
        return "EMPTY"

    def _hold_params_to_egress(self, params: dict, family: str) -> None:
        """Send every request of the client ``params`` builds through the egress
        transport. Applied last, so nothing in ``parameters`` can swap it out."""
        if self.egress_transport is None:
            return
        _drop_connection_params(params, self.model)
        transport = self.egress_transport(family)
        # Redirects are not followed: no model API needs one, and a hop is one
        # more connection for an endpoint to choose.
        if family == "openai":
            import openai

            # Explicitly, or the field falls back to OPENAI_PROXY, which
            # ChatOpenAI refuses beside a client of its own.
            params["openai_proxy"] = None
            params["http_client"] = openai.DefaultHttpxClient(
                transport=transport, follow_redirects=False
            )
            params["http_async_client"] = openai.DefaultAsyncHttpxClient(
                transport=transport, follow_redirects=False
            )
        elif family == "gemini":
            # One mapping serves both of google-genai's clients, and a transport
            # in it is also what turns off its aiohttp path, which no httpx
            # transport would see.
            params["client_args"] = {"transport": transport, "follow_redirects": False}
        elif family == "openrouter":
            params["http_transports"] = (transport, transport)
            params["follow_redirects"] = False
        else:
            raise ValueError(f"No egress hold for SDK family {family!r}")

    def _hold_anthropic_to_egress(self, llm) -> None:
        """Pre-seed ChatAnthropic's SDK clients with ones on the egress transport.

        ChatAnthropic takes no http client: it builds each SDK client lazily in a
        cached property from a default one. Seeding the cache is the one way in,
        so a release that stops caching under these names is refused here rather
        than left to dial on its own.
        """
        if self.egress_transport is None:
            return
        import anthropic

        for name in ("_client", "_async_client"):
            if not isinstance(inspect.getattr_static(type(llm), name, None), functools.cached_property):
                raise TypeError(
                    f"{type(llm).__name__}.{name} is not a cached property; "
                    "the egress transport cannot be attached"
                )
        transport = self.egress_transport("anthropic")
        client_params = llm._client_params
        http_kwargs = {"timeout": client_params["timeout"]} if "timeout" in client_params else {}
        # Not followed, as in ``_hold_params_to_egress``.
        http_kwargs["follow_redirects"] = False
        sync_client = anthropic.Client(
            **client_params,
            http_client=anthropic.DefaultHttpxClient(transport=transport, **http_kwargs),
        )
        async_client = anthropic.AsyncClient(
            **client_params,
            http_client=anthropic.DefaultAsyncHttpxClient(transport=transport, **http_kwargs),
        )
        if client_params.get("auth_token"):
            # As ChatAnthropicOAuth does for its own clients: the SDK fills a
            # None api_key from the environment, and a bearer client must not
            # send that key alongside its token.
            sync_client.api_key = None
            async_client.api_key = None
        llm.__dict__["_client"] = sync_client
        llm.__dict__["_async_client"] = async_client

    def _resolve_base_url(self, param_name: str = "base_url") -> dict:
        """Resolve base URL with HOST_IP substitution. Returns dict to merge into params."""
        if not self.base_url:
            return {}
        url = self.base_url
        if "{HOST_IP}" in url:
            from src.config.env import HOST_IP
            url = url.replace("{HOST_IP}", HOST_IP)
        return {param_name: url}

    def _build_openai_params(self, cache_key: str | None = None) -> dict:
        """Build the constructor kwargs every OpenAI-SDK chat client shares.

        Split out so a provider that needs its own ChatOpenAI subclass differs
        by the class alone, not by a second copy of this that drifts.
        """
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("base_url"))

        # Handle Response API if configured
        if self.use_response_api:
            params["output_version"] = "responses/v1"

        # Enable use_previous_response_id if configured in provider
        if self.use_previous_response_id:
            params["use_previous_response_id"] = True

        # Add all parameters from llm_config
        params.update(self.parameters)

        # Merged after the parameters expansion so a parameters["default_headers"]
        # entry augments the provider-level headers instead of clobbering them.
        merged_headers = _merged_default_headers(params, self.default_headers)
        if merged_headers:
            params["default_headers"] = merged_headers

        # Explicit prompt caching is api.openai.com-only: strip the opt-in when
        # routed anywhere else (platform proxy, OpenAI-compatible endpoints) so
        # ineligible backends never see the param or the breakpoint marker.
        if params.get("prompt_cache_options") is not None and not is_official_openai_endpoint(
            params.get("base_url") or params.get("openai_api_base")
        ):
            params.pop("prompt_cache_options")

        # Pass extra_body for provider-specific fields (e.g. caching, thinking)
        if self.extra_body:
            params["extra_body"] = self.extra_body

        # model_kwargs (not bind) so the key survives bind_tools / with_structured_output.
        if cache_key:
            existing_mk = params.get("model_kwargs") or {}
            params["model_kwargs"] = {**existing_mk, "prompt_cache_key": cache_key}

        self._hold_params_to_egress(params, "openai")
        return params

    def _get_openai_llm(self, cache_key: str | None = None):
        """Get OpenAI or OpenAI-compatible LLM."""
        return ChatOpenAI(**self._build_openai_params(cache_key))

    def _get_dashscope_llm(self, cache_key: str | None = None):
        """Get DashScope (Qwen) LLM via ``ChatDashScope`` (streamed reasoning bridge)."""
        from src.llms.extension import ChatDashScope

        return ChatDashScope(**self._build_openai_params(cache_key))

    def _get_codex_llm(self, cache_key: str | None = None):
        """Get Codex OAuth LLM (store=false, stateless)."""
        from src.llms.extension.codex import ChatCodexOpenAI, codex_identity_headers

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "streaming": True,
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("base_url"))

        if self.use_response_api:
            params["output_version"] = "responses/v1"

        params.update(self.parameters)

        # First-party identity (see ``codex_identity_headers``). Built after the
        # parameters merge so a parameters["default_headers"] entry augments the
        # mapping instead of clobbering it.
        params["default_headers"] = _merged_default_headers(
            params,
            codex_identity_headers(),
            self.default_headers,
        )

        # Cache affinity: the codex backend routes its prompt cache by session
        # headers, not by prompt_cache_key (wire-tested ~3× fewer warm-chain
        # misses). Pinned headers win over the derived value, checked
        # case-insensitively so a differently-cased pin can't end up
        # contradicting a derived duplicate on the wire.
        if cache_key:
            affinity = _derive_codex_affinity(cache_key)
            present = {k.lower() for k in params["default_headers"]}
            for header in _CODEX_SESSION_HEADERS:
                if header not in present:
                    params["default_headers"][header] = affinity

        # The codex backend 400s on explicit-caching params — never forward the opt-in.
        params.pop("prompt_cache_options", None)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        if cache_key:
            existing_mk = params.get("model_kwargs") or {}
            params["model_kwargs"] = {**existing_mk, "prompt_cache_key": cache_key}

        self._hold_params_to_egress(params, "openai")
        return ChatCodexOpenAI(**params)

    def _get_deepseek_llm(self):
        """Get DeepSeek or DeepSeek-compatible LLM."""
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("api_base"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        self._hold_params_to_egress(params, "openai")
        return ChatDeepSeek(**params)

    def _get_openrouter_llm(self):
        """Get an OpenRouter LLM via the official ``ChatOpenRouter``."""
        from src.llms.extension import ChatOpenRouterClient

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "streaming": True,
            "stream_usage": True,
            # The SDK retries 5xx and dropped connections for up to ~150s per
            # unit; the resilience middleware owns everything past that.
            "max_retries": 1,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("base_url"))
        params.update(self.parameters)
        params = ChatOpenRouterClient.factory_kwargs(params, self.extra_body, self.model)

        merged_headers = _merged_default_headers(params, self.default_headers)
        if merged_headers:
            params["default_headers"] = merged_headers

        self._hold_params_to_egress(params, "openrouter")
        return ChatOpenRouterClient(**params)

    def _get_glm_llm(self):
        """Get GLM/bigmodel LLM via vendored langchain-zai ``ChatZai`` (reasoning round-trip)."""
        from src.llms.vendor.langchain_zai import ChatZai

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("base_url"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        # A gateway's Chat Completions route runs here too, for the reasoning
        # it sends back (OpenCode Go), and reads the provider's headers.
        merged_headers = _merged_default_headers(params, self.default_headers)
        if merged_headers:
            params["default_headers"] = merged_headers

        if self.extra_body:
            params["extra_body"] = self.extra_body

        self._hold_params_to_egress(params, "openai")
        return ChatZai(**params)

    def _get_qwq_llm(self):
        """Get QwQ or QwQ-compatible LLM (for Qwen models with reasoning support)."""
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
            "stream_chunk_timeout": STREAM_CHUNK_TIMEOUT_S,
        }
        params.update(self._resolve_base_url("api_base"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        self._hold_params_to_egress(params, "openai")
        return ChatQwen(**params)

    def _get_anthropic_llm(self):
        """Get Anthropic LLM."""
        from langchain_anthropic import ChatAnthropic
        from src.llms.extension import ChatAnthropicOAuth

        is_oauth = self.provider_info.get("access_type") == "oauth"

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "streaming": True,
            "max_tokens": 32000,  # Default for Anthropic SDK models
            "max_retries": 5,
            "timeout": 600.0,  # 10 minutes - sufficient for long reasoning
        }

        # Set base URL from provider configuration if available
        if self.base_url:
            params["base_url"] = self.base_url

        # Add all parameters from llm_config, excluding enable_caching
        # (enable_caching is not a ChatAnthropic parameter, it's used by our caching logic)
        # This will override max_tokens if explicitly set in model config
        filtered_params = {k: v for k, v in self.parameters.items() if k != "enable_caching"}
        params.update(filtered_params)

        # After the parameters expansion, which can carry either alias. The
        # stored URL is left as it is: it keys the route identity.
        for url_key in ("base_url", "anthropic_api_url"):
            if params.get(url_key):
                params[url_key] = anthropic_sdk_base_url(params[url_key])

        # Merged after the parameters expansion so a parameters["default_headers"]
        # entry augments the provider-level headers instead of clobbering them.
        merged_headers = _merged_default_headers(params, self.default_headers)
        if merged_headers:
            params["default_headers"] = merged_headers

        # Pass extra_body via model_kwargs so ChatAnthropic's Pydantic validator
        # doesn't warn about an unknown field (extra_body isn't a declared field).
        if self.extra_body:
            params.setdefault("model_kwargs", {})["extra_body"] = self.extra_body

        if self.egress_transport is not None:
            _drop_connection_params(params, self.model)

        # OAuth tokens (sk-ant-oat*) need Authorization: Bearer, not X-Api-Key.
        # ChatAnthropicOAuth redirects api_key → auth_token on the underlying SDK client.
        llm = ChatAnthropicOAuth(**params) if is_oauth else ChatAnthropic(**params)
        self._hold_anthropic_to_egress(llm)
        return llm

    def _get_gemini_llm(self):
        """Get Gemini LLM."""
        from langchain_google_genai import ChatGoogleGenerativeAI

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "timeout": 600.0,  # 10 minutes - sufficient for long reasoning
        }

        # Set base URL from provider configuration if available
        if self.base_url:
            params["base_url"] = self.base_url

        # Add all parameters from llm_config
        params.update(self.parameters)

        # A gateway serving Gemini's API reads the provider's headers too
        # (OpenCode Go refuses a request without its session header). The
        # client names them ``additional_headers``.
        merged_headers = _merged_default_headers(
            params, self.default_headers, params.pop("additional_headers", None)
        )
        params.pop("default_headers", None)
        if merged_headers:
            params["additional_headers"] = merged_headers

        # Pass extra_body via model_kwargs to avoid Pydantic unknown-field warning.
        if self.extra_body:
            params.setdefault("model_kwargs", {})["extra_body"] = self.extra_body

        self._hold_params_to_egress(params, "gemini")
        return ChatGoogleGenerativeAI(**params)


def stamp_call_metadata(client: Any, **fields: Any) -> None:
    """Add turn-shaping facts to a built client's metadata, in place.

    The counterpart to the stamp in ``LLM.get_llm``, for facts the factory does
    not see (the resolved prompt guidance, the compaction preset). They belong
    on the client rather than the graph run because a subagent inherits graph
    metadata wholesale while running its own model, so a value put there is
    asserted for calls it was never true of. Clients are built per turn, so the
    added keys stay local to this one. ``None`` fields are dropped: an absent
    key reads as "not resolved", where a null reads as a resolved nothing.
    """
    populated = {k: v for k, v in fields.items() if v is not None}
    if populated:
        client.metadata = {**(client.metadata or {}), **populated}


# Backward compatibility functions
def create_llm(
    model: str,
    api_key: str | None = None,
    default_headers: dict | None = None,
    base_url=_UNSET,
    reasoning_effort: str | None = None,
    cache_key: str | None = None,
    **kwargs,
):
    """
    Convenience function for creating an LLM instance.

    Args:
        model: The model name
        api_key: Optional API key override (e.g. from BYOK)
        default_headers: Optional headers to merge onto the LLM instance
            (e.g. ChatGPT-Account-Id for Codex OAuth)
        base_url: Override base URL. None = SDK default, str = custom URL.
        reasoning_effort: Optional reasoning effort level ("low", "medium", "high").
        cache_key: Session-stable key sent as ``prompt_cache_key`` on
            OpenAI/Codex requests (always-on for Codex, which also derives
            session-affinity headers from it; opt-in for OpenAI).
        **kwargs: Additional parameters to override

    Returns:
        A LangChain chat model instance
    """
    instance = LLM(
        model,
        api_key=api_key,
        base_url_override=base_url,
        reasoning_effort=reasoning_effort,
        **kwargs,
    )
    if default_headers:
        existing = instance.default_headers or {}
        instance.default_headers = {**existing, **default_headers}
    return instance.get_llm(cache_key=cache_key)


def platform_key_configured(model_name: str) -> bool:
    """Whether the deployment holds a key for a catalog model, the one
    ``create_llm`` reads when no override is passed."""
    try:
        LLM(model_name)._resolve_api_key()
    except MissingProviderKeyError:
        return False
    return True


def create_llm_from_custom(
    config: dict,
    api_key: str | None = None,
    base_url=_UNSET,
    cache_key: str | None = None,
    reasoning_effort: str | None = None,
    **kwargs,
):
    """
    Convenience function for creating an LLM from a user-defined custom model config.

    Args:
        config: Dict with model_id, provider, and optional parameters/extra_body.
        api_key: Optional API key override (e.g. from BYOK).
        base_url: Override base URL. _UNSET = no override, None = SDK default.
        cache_key: Session-stable key sent as ``prompt_cache_key`` on
            OpenAI/Codex requests (always-on for Codex, which also derives
            session-affinity headers from it; opt-in for OpenAI).
        reasoning_effort: Optional reasoning effort level, clamped against the
            ladder the config itself declares.
        **kwargs: Additional parameters to override.

    Returns:
        A LangChain chat model instance.
    """
    return LLM.from_custom_config(
        config,
        api_key=api_key,
        base_url_override=base_url,
        cache_key=cache_key,
        reasoning_effort=reasoning_effort,
        **kwargs,
    )


def narrow_prompt_cache_key(model: Any, suffix: str) -> Any:
    """Return a copy of ``model`` with ``:suffix`` appended to its ``prompt_cache_key``.

    Used to namespace parallel sub-tasks (subagent fanout, compaction) onto
    separate OpenAI cache shards — OpenAI rate-limits at ~15 RPM per
    (prefix + ``prompt_cache_key``) bucket. Codex session-affinity headers are
    deliberately NOT narrowed: they pin a replica (not a cache scope), the
    copy shares the parent's already-built HTTP clients so a field-level
    header update never reaches the wire anyway, and riding the parent
    thread's lane is what codex-rs itself does. No-op when the model has no
    ``prompt_cache_key`` set or ``suffix`` is empty.
    """
    if not suffix:
        return model
    if not isinstance(model, BaseChatModel):
        return model
    mk = getattr(model, "model_kwargs", None) or {}
    parent_key = mk.get("prompt_cache_key")
    if not parent_key:
        return model
    new_mk = {**mk, "prompt_cache_key": f"{parent_key}:{suffix}"}
    return model.model_copy(update={"model_kwargs": new_mk})


def get_llm_by_type(llm_type: str) -> BaseChatModel:
    """
    Get LLM instance by type.
    Supports both legacy type names and direct model names.

    Args:
        llm_type: The LLM type or model name (e.g., 'basic', 'reasoning', 'gpt-4o')

    Returns:
        A LangChain chat model instance
    """
    try:
        llm = LLM(llm_type).get_llm()
        return llm
    except ValueError as e:
        raise ValueError(f"Unknown LLM type or model: {llm_type}. Error: {e}")


def get_configured_llm_models() -> dict[str, list[str]]:
    """
    Get visible LLM models grouped by parent provider.

    Only returns models with "visible": true in models.json.
    Models are grouped by their parent provider (e.g., platform variants → parent).

    Returns:
        Dictionary mapping parent provider to list of visible model names.
    """
    try:
        config = LLM.get_model_config()  # singleton — no disk I/O
        models: dict[str, list[str]] = {}

        for model_name, model_info in config.llm_config.items():
            if model_info and model_info.get("visible", False):
                provider = model_info.get("provider", "unknown")
                parent = config.get_parent_provider(provider)
                models.setdefault(parent, []).append(model_name)

        return models

    except Exception as e:
        # Log error and return empty dict to avoid breaking the application
        print(f"Warning: Failed to load LLM configuration: {e}")
        return {}

def get_input_modalities(
    model_name: str,
    custom_modalities: list[str] | None = None,
) -> list[str]:
    """Get supported input modalities for a model name.

    When *custom_modalities* is provided (from a custom model's stored
    preferences), it is returned directly.  Otherwise falls back to the
    ``models.json`` lookup via the singleton :class:`ModelConfig`.
    """
    if custom_modalities is not None:
        return custom_modalities
    return LLM.get_model_config().get_input_modalities(model_name)


# Published per-request PDF page ceilings. Anthropic's is the only one that
# moves with the context window (600 at 1M, 100 below it), and it is the reason
# a single global cap can't be right: the same document is fine on Sonnet 5 and
# rejected on Sonnet 4.6. OpenAI documents a size limit but no page limit, hence
# None — "not bounded by pages" rather than "unknown".
_ANTHROPIC_SDK_PROVIDERS = frozenset({"anthropic", "claude-oauth"})
_GEMINI_MAX_PDF_PAGES = 1000
_ANTHROPIC_MAX_PDF_PAGES_1M = 600
_ANTHROPIC_MAX_PDF_PAGES = 100


def get_max_pdf_pages(model_name: str) -> int | None:
    """Documented per-request PDF page ceiling for a model, or None if unbounded.

    Unknown models get the tightest published ceiling rather than None: this
    gates what we transmit, so guessing generously turns an unknown into a 400
    the caller has no way to recover.
    """
    model_info = LLM.get_model_config().get_model_config(model_name) or {}
    provider = model_info.get("provider", "")

    if provider in _ANTHROPIC_SDK_PROVIDERS:
        context = model_info.get("context") or 0
        return (
            _ANTHROPIC_MAX_PDF_PAGES_1M
            if context >= 1_000_000
            else _ANTHROPIC_MAX_PDF_PAGES
        )
    if provider == "gemini":
        return _GEMINI_MAX_PDF_PAGES
    if provider in ("openai", "codex-oauth"):
        return None
    return _ANTHROPIC_MAX_PDF_PAGES


# What a request is charged for one PDF page, per vendor, where that was
# measured to sit far from the 3,000 the context estimate uses otherwise
# (``PDF_PAGE_TOKENS`` in the compaction utils). Measured on a 10-page window
# of arXiv 2112.10752, an image-heavy paper: Gemini 520 a page, which reads a
# page as its image, 560 being the most its default resolution charges; OpenAI
# 3,230, close enough to the default that it keeps it.
_PDF_PAGE_TOKENS = {"gemini": 560}


def get_pdf_page_tokens(route: str) -> int | None:
    """What ``route`` (from :func:`image_route`) charges for a PDF page, or
    None where no measurement sets it apart from the default estimate."""
    return _PDF_PAGE_TOKENS.get(route)


# Published per-side pixel ceilings for an image in a request, per vendor: the
# largest side, then (image count, tighter side) steps that apply from that
# count on. Two tighten with the number of images in the request, which is why
# the count is an argument: Anthropic's above 20, DeepSeek's from 15. OpenAI,
# Gemini and DashScope document that they shrink an oversized image rather than
# refuse it, hence no rule. Anthropic's images are held to its tightest step
# from the first, for the reason ``get_max_image_side`` gives.
_IMAGE_SIDE_RULES: dict[str, tuple[int, tuple[tuple[int, int], ...]]] = {
    "anthropic": (8000, ((21, 2000),)),
    "deepseek": (8192, ((15, 4096),)),
    "z-ai": (6000, ()),
}
_UNBOUNDED_IMAGE_PROVIDERS = frozenset({"openai", "gemini", "dashscope"})

# The vendors whose image rules are known. Any other route (an aggregator, a
# local server) is judged by the vendor of the model it serves, if known.
_IMAGE_FAMILIES = frozenset({
    "anthropic", "openai", "gemini", "dashscope", "deepseek", "z-ai", "moonshot", "minimax", "xai",
})


def _provider_family(provider: str) -> str:
    """The root of a provider's variant chain, so a regional or plan route
    (``z-ai-cn-coding``, ``claude-oauth``) is judged as the vendor it serves."""
    config = LLM.get_model_config()
    seen = {provider}
    while (parent := config.get_parent_provider(provider)) not in seen:
        seen.add(parent)
        provider = parent
    return provider


def _endpoint_family(endpoint: str) -> str | None:
    """The vendor whose manifest endpoints share this URL's host, if exactly one."""
    from urllib.parse import urlparse

    host = urlparse(endpoint).hostname
    if not host:
        return None
    families = {
        _provider_family(name)
        for name, info in LLM.get_model_config().flat_providers.items()
        if isinstance(info.get("base_url"), str)
        and urlparse(info["base_url"]).hostname == host
    } & _IMAGE_FAMILIES
    return next(iter(families)) if len(families) == 1 else None


def image_route(client_metadata: dict[str, Any] | None) -> str:
    """The vendor whose image rules a call through this client has to meet.

    Read off the client's stamp rather than the manifest key: a user-defined
    model has no manifest row, a platform route replaces the manifest provider
    when the caller brings no key, and a BYOK base URL can point a provider's
    slot at another vendor. The endpoint decides when it is a known vendor's,
    then the provider the client was built for, then the manifest model's own.
    Empty when none is known, which gets the strictest rules.
    """
    metadata = client_metadata or {}
    route = metadata.get("provider_route")
    if isinstance(route, str) and ROUTE_ENDPOINT_SEP in route:
        family = _endpoint_family(route.split(ROUTE_ENDPOINT_SEP, 1)[1])
        if family:
            return family
    model = metadata.get("manifest_model")
    model_info = (
        LLM.get_model_config().get_model_config(model) if isinstance(model, str) else None
    ) or {}
    for provider in (metadata.get("pricing_provider"), model_info.get("provider")):
        if isinstance(provider, str) and provider:
            family = _provider_family(provider)
            if family in _IMAGE_FAMILIES:
                return family
    return ""


# The image types a route takes as is, from each vendor's docs. Most list the
# same four; Gemini and DashScope list more, and neither lists GIF.
COMMON_IMAGE_TYPES = frozenset({"image/png", "image/jpeg", "image/gif", "image/webp"})
_IMAGE_TYPES = {
    "gemini": frozenset({"image/png", "image/jpeg", "image/webp", "image/heic", "image/heif"}),
    "dashscope": frozenset({
        "image/png", "image/jpeg", "image/webp", "image/bmp", "image/tiff", "image/heic",
    }),
    # xAI refuses GIF (OpenCode #53787).
    "xai": frozenset({"image/png", "image/jpeg", "image/webp"}),
}


def get_image_types(route: str) -> frozenset[str]:
    """MIME types ``route`` (from :func:`image_route`) takes as is."""
    return _IMAGE_TYPES.get(route, COMMON_IMAGE_TYPES)


def get_max_image_side(route: str, image_count: int) -> int | None:
    """Largest side in pixels each image may have on ``route``, or None if unbounded.

    ``route`` comes from :func:`image_route`. ``image_count`` is the number of
    images in the request being sent, not in the one message carrying this
    image. A route with no rule of its own gets the tightest of the known rules
    at that count, for the reason ``get_max_pdf_pages`` gives: it may be any of
    those vendors behind a gateway, and no one vendor's rule is the tightest
    at every count.

    Anthropic gets its many-image side at every count: a thinking block it
    signed stays valid only while every message before it is byte-identical,
    and re-fitting the images already sent when a later one takes the request
    past 20 would invalidate the thinking after the first of them. An image
    larger than that side gives up the detail from its first call instead.
    """
    if route == "anthropic":
        return _side_rule(route, sys.maxsize)
    return get_max_image_side_by_count(route, image_count)


def get_max_image_side_by_count(route: str, image_count: int) -> int | None:
    """:func:`get_max_image_side` with Anthropic held to its rule at
    ``image_count`` too: the side a thread whose images were sent before the
    fixed one shipped keeps fitting them to, since a new fit would invalidate
    the thinking after them."""
    if route in _UNBOUNDED_IMAGE_PROVIDERS:
        return None
    if route in _IMAGE_SIDE_RULES:
        return _side_rule(route, image_count)
    return min(_side_rule(vendor, image_count) for vendor in _IMAGE_SIDE_RULES)


def _side_rule(vendor: str, image_count: int) -> int:
    side, steps = _IMAGE_SIDE_RULES[vendor]
    for from_count, tighter in steps:
        if image_count >= from_count:
            side = tighter
    return side


def should_enable_caching(model_name: str) -> bool:
    """
    Check if a model should enable Anthropic prompt caching.

    Args:
        model_name: The model name from llm_config.json

    Returns:
        True if the model has enable_caching=True in its parameters
    """
    try:
        config = LLM.get_model_config()
        model_info = config.get_model_config(model_name)
        if not model_info:
            return False

        # Check if model has enable_caching in parameters
        parameters = model_info.get("parameters", {})
        return parameters.get("enable_caching", False)
    except Exception:
        return False


def ensure_model_in_manifest(model_name: str) -> None:
    """Raise a neutral ValueError if ``model_name`` isn't in models.json.

    Shared guard: when a name reaches create_llm() but isn't in the manifest,
    the factory raises a generic "Model X not found" message that used to leak
    to users as an SSE error. Callers on the post-BYOK fallback path
    (AgentConfig.get_llm_client, background_client) call this helper instead,
    so a missing manifest entry always surfaces as a user-friendly message
    pointing at Settings. ``resolve_llm_config`` already preflights the main
    model path via HTTPException — this is the belt-and-suspenders guard for
    code paths downstream of that preflight.
    """
    if LLM.get_model_config().get_model_config(model_name) is None:
        raise ValueError(
            f"Model '{model_name}' is not defined in models.json. "
            "If this is a custom model, configure it in Settings with a valid API key. "
            "If it's a built-in, check for typos."
        )
