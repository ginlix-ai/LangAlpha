import copy
import os
import json
import uuid
from pathlib import Path
from typing import Dict, Optional, Any
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.language_models import BaseChatModel
from langchain_deepseek import ChatDeepSeek
from langchain_qwq import ChatQwen

from .endpoints import is_official_openai_endpoint
from .model_spec import (
    ModelSpec,
    canonical_reasoning_efforts,
    clamp_reasoning_effort,
    default_reasoning_effort,
    reasoning_block,
)
from .preferences import compaction_profile_for
from .pricing_utils import get_price_tier
from .reasoning import apply_reasoning_effort
from .reasoning_lineage import ROUTE_ENDPOINT_SEP

load_dotenv()


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
            shared = {k: v for k, v in config.items() if k != "variants"}

            if not variants:
                flat[group_key] = shared
                continue

            has_self_variant = group_key in variants
            for vkey, overrides in variants.items():
                merged = {**shared, **overrides}
                if vkey != group_key:
                    # setdefault: a variant may declare an explicit parent_provider
                    merged.setdefault("parent_provider", group_key)
                flat[vkey] = merged

            if not has_self_variant:
                flat[group_key] = shared

        # Post-flatten validation: every entry must have an sdk field
        # (platform variants are exempt — they inherit sdk at runtime from parent)
        for key, entry in flat.items():
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
            provider: Provider name (e.g., 'openai', 'anthropic', 'volcengine')
            model_id: Model ID (e.g., 'gpt-5', 'claude-opus-4', 'doubao-seed-1-6-250615')

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

    def get_child_variants(self, provider: str) -> list[str]:
        """Return provider names whose parent_provider is ``provider``.

        Excludes the provider itself; platform-only variants are excluded
        since BYOK keys are never stored under them. O(1) lookup against
        the precomputed map in ``__init__``.
        """
        return list(self._child_variants.get(provider, ()))

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
            # Optional editorial metadata for the model-detail flyout. Additive —
            # only surfaced for models that authored it in models.json; the
            # frontend renders only the rows that are present.
            for key in (
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
            # Cost tier (1-5) derived live from canonical providers.json pricing —
            # not stored in models.json, so it auto-syncs when prices change.
            price_tier = get_price_tier(model_info.get("model_id", model_name), provider)
            if price_tier is not None:
                entry["price"] = price_tier
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
        flash/compaction/fetch models are not all listed in the picker.
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

# Header spellings the first-party codex clients send — codex-rs:
# session-id/thread-id, opencode/hermes: session_id.
_CODEX_SESSION_HEADERS = ("session-id", "thread-id", "session_id")


def _derive_codex_affinity(cache_key: str) -> str:
    """UUID-normalize a cache key for codex session headers — pass-through for
    UUID keys, uuid5 otherwise so raw key material never egresses in a header."""
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

# Name regex for custom models: alphanumeric start, then alphanumeric/./_/-/:
# Colon allowed for Ollama-style name:tag format (e.g. gemma4:31b)
CUSTOM_MODEL_NAME_RE = r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$"


def _profile_overrides_from_config(model_info: dict) -> dict[str, Any]:
    """Map a models.json entry onto ``ModelProfile`` keys (manifest is the SoT)."""
    overrides: dict[str, Any] = {}
    context = model_info.get("context")
    if isinstance(context, int):
        overrides["max_input_tokens"] = context
    max_tokens = (model_info.get("parameters") or {}).get("max_tokens")
    if isinstance(max_tokens, int):
        overrides["max_output_tokens"] = max_tokens
    modalities = model_info.get("input_modalities")
    if isinstance(modalities, list):
        overrides["text_inputs"] = "text" in modalities
        overrides["image_inputs"] = "image" in modalities
        overrides["audio_inputs"] = "audio" in modalities
        overrides["video_inputs"] = "video" in modalities
    return overrides


class LLM:
    """Factory class for creating LangChain LLM clients."""

    # Class-level model config instance
    _model_config = None

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
            **override_params: Additional parameters to override defaults.
        """
        self.model_config = self.get_model_config()
        spec = (
            model
            if isinstance(model, ModelSpec)
            else ModelSpec.from_manifest(self.model_config, model)
        )

        self.custom_model_name = spec.name  # Store the custom name
        self.model = spec.model_id  # Use model_id for API calls
        self.provider = spec.provider

        # System serving: route through platform proxy when no BYOK key
        if not api_key and spec.system_provider:
            self.provider = spec.system_provider

        # Deep copy: reasoning-effort overrides mutate nested dicts (e.g.
        # extra_body.thinking); a shallow copy would contaminate the spec, and
        # through it the process-wide manifest, for every subsequent request.
        self.parameters = copy.deepcopy(spec.parameters)
        self.extra_body = copy.deepcopy(spec.extra_body)

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

        # Get provider info from manifest (empty for a provider it does not list)
        self.provider_info = self.model_config.get_provider_info(self.provider)

        # Extract provider configuration
        self.sdk = self.provider_info.get("sdk") or spec.sdk_fallback
        self.env_key = self.provider_info.get("env_key")
        self.base_url = self.provider_info.get("base_url")

        # Apply base_url override (sentinel distinguishes "not set" from "set to None")
        if base_url_override is not _UNSET:
            self.base_url = base_url_override

        # Store response API flags for OpenAI SDK
        response_api = (
            self.provider_info.get("use_response_api", False)
            if spec.use_response_api_override is None
            else spec.use_response_api_override
        )
        self.use_response_api = bool(response_api) and self.sdk in ("openai", "codex", "dashscope")
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
            **override_params: Additional parameters to override defaults.

        Returns:
            A LangChain chat model instance.
        """
        name = config.get("name")
        instance = cls(
            ModelSpec.from_custom(
                config, cls.get_model_config().get_model_config(name) if name else None
            ),
            api_key=api_key,
            base_url_override=base_url_override,
            reasoning_effort=reasoning_effort,
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
        if self.api_key_override is not None and self.api_key_override != "":
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

        Empty-string overrides are accepted for local providers (LM Studio,
        Ollama, vLLM, etc.) where no real key is required.
        """
        if self.api_key_override is not None and self.api_key_override != "":
            return self.api_key_override
        env_keys = []
        if self.env_key:
            env_keys.append(self.env_key)
        for env_key in self.provider_info.get("env_key_aliases", []):
            if env_key and env_key not in env_keys:
                env_keys.append(env_key)
        for env_key in env_keys:
            key = os.getenv(env_key)
            if key:
                return key
        if env_keys:
            if self.provider_info.get("access_type") != "local":
                joined = " or ".join(env_keys)
                raise ValueError(f"{joined} environment variable is not set")
        return "EMPTY"

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
        from src.llms.extension import ChatCodexOpenAI

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "streaming": True,
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
        }
        params.update(self._resolve_base_url("base_url"))

        if self.use_response_api:
            params["output_version"] = "responses/v1"

        params.update(self.parameters)

        # The codex backend gates newer models (e.g. GPT-5.6 Luna) to first-party
        # clients: without an originator naming a known client AND a User-Agent
        # carrying the matching "<originator>/" prefix, it 404s "Model not found".
        # Built after the parameters merge so a parameters["default_headers"]
        # entry augments the mapping instead of clobbering it.
        params["default_headers"] = _merged_default_headers(
            params,
            {"originator": "codex_cli_rs", "User-Agent": "codex_cli_rs/0.46.0"},
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

        return ChatCodexOpenAI(**params)

    def _get_deepseek_llm(self):
        """Get DeepSeek or DeepSeek-compatible LLM."""
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
        }
        params.update(self._resolve_base_url("api_base"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        return ChatDeepSeek(**params)

    def _get_glm_llm(self):
        """Get GLM/bigmodel LLM via vendored langchain-zai ``ChatZai`` (reasoning round-trip)."""
        from src.llms.vendor.langchain_zai import ChatZai

        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
        }
        params.update(self._resolve_base_url("base_url"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        client = ChatZai(**params)

        # Override the package profile with manifest values so the two can't drift
        # (compaction reads profile["max_input_tokens"]); capability flags preserved.
        model_info = self.model_config.get_model_config(self.custom_model_name) or {}
        overrides = _profile_overrides_from_config(model_info)
        if overrides:
            base = client.profile if isinstance(client.profile, dict) else {}
            client.profile = {**base, **overrides}

        return client

    def _get_qwq_llm(self):
        """Get QwQ or QwQ-compatible LLM (for Qwen models with reasoning support)."""
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
        }
        params.update(self._resolve_base_url("api_base"))

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

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

        # Merged after the parameters expansion so a parameters["default_headers"]
        # entry augments the provider-level headers instead of clobbering them.
        merged_headers = _merged_default_headers(params, self.default_headers)
        if merged_headers:
            params["default_headers"] = merged_headers

        # Pass extra_body via model_kwargs so ChatAnthropic's Pydantic validator
        # doesn't warn about an unknown field (extra_body isn't a declared field).
        if self.extra_body:
            params.setdefault("model_kwargs", {})["extra_body"] = self.extra_body

        # OAuth tokens (sk-ant-oat*) need Authorization: Bearer, not X-Api-Key.
        # ChatAnthropicOAuth redirects api_key → auth_token on the underlying SDK client.
        if is_oauth:
            return ChatAnthropicOAuth(**params)
        return ChatAnthropic(**params)

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

        # Pass extra_body via model_kwargs to avoid Pydantic unknown-field warning.
        if self.extra_body:
            params.setdefault("model_kwargs", {})["extra_body"] = self.extra_body

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
    (AgentConfig.get_llm_client, FlashAgent.__init__) call this helper instead,
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
