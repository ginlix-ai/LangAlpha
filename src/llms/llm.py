import os
import json
from pathlib import Path
from typing import Dict, Optional, Any
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.language_models import BaseChatModel
from langchain_deepseek import ChatDeepSeek
from langchain_qwq import ChatQwen

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

    def get_model_config(self, model_id: str) -> Optional[Dict]:
        """Get model configuration from llm_config."""
        return self.llm_config.get(model_id)

    def get_provider_info(self, provider: str) -> Dict:
        """Get provider configuration from manifest."""
        return self.manifest["provider_config"].get(provider, {})

    def get_model_pricing(self, custom_model_name: str) -> Optional[Dict[str, Any]]:
        """Get pricing information for a specific model from manifest."""
        # Get model info from llm_config first
        model_info = self.llm_config.get(custom_model_name)
        if not model_info:
            return None

        provider = model_info["provider"]
        model_id = model_info["model_id"]

        # Then look up pricing in manifest
        models = self.manifest["models"].get(provider, [])
        for model in models:
            if model["id"] == model_id:
                return model.get('pricing')
        return None

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
        """Return list of provider names that have byok_eligible=true in manifest."""
        return [
            name
            for name, cfg in self.manifest.get("provider_config", {}).items()
            if cfg.get("byok_eligible", False)
        ]

    def get_parent_provider(self, provider: str) -> str:
        """Return the parent provider name (self if no parent)."""
        info = self.get_provider_info(provider)
        return info.get("parent_provider", provider)

    def get_display_name(self, provider: str) -> str:
        """Return display name, resolving through parent if needed."""
        info = self.get_provider_info(provider)
        parent = info.get("parent_provider")
        if parent:
            info = self.get_provider_info(parent)
        else:
            parent = provider
        return info.get("display_name", parent.title())

    def get_model_metadata(self) -> dict[str, dict[str, str]]:
        """Return {model_key: {sdk, provider}} for all visible models."""
        result = {}
        for model_name, model_info in self.llm_config.items():
            if not model_info or not model_info.get("visible", False):
                continue
            provider = model_info.get("provider", "unknown")
            sdk = self.get_provider_info(provider).get("sdk", "unknown")
            result[model_name] = {"sdk": sdk, "provider": provider}
        return result


_UNSET = object()  # Sentinel to distinguish "no override" from "override to None"

# Name regex for custom models: alphanumeric start, then alphanumeric/./_ /-
CUSTOM_MODEL_NAME_RE = r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$"


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
        model: str,
        api_key: str | None = None,
        base_url_override=_UNSET,
        reasoning_effort: str | None = None,
        **override_params,
    ):
        """
        Initializes the LLM factory.

        Args:
            model: The customized model name (key in llm_config.json).
            api_key: Optional API key override (e.g. from BYOK).
            base_url_override: Override base URL. Use _UNSET (default) for no override,
                None to clear to SDK default, or a string for a custom URL.
            reasoning_effort: Optional reasoning effort level ("low", "medium", "high").
            **override_params: Additional parameters to override defaults.
        """
        self.model_config = self.get_model_config()

        # Get model configuration from models.json
        model_info = self.model_config.get_model_config(model)
        if not model_info:
            raise ValueError(f"Model {model} not found in models.json")

        self.custom_model_name = model  # Store the custom name
        self.model = model_info["model_id"]  # Use model_id for API calls
        self.provider = model_info["provider"]
        self.parameters = model_info.get("parameters", {}).copy()
        self.extra_body = model_info.get("extra_body", {}).copy()

        # Override with any provided parameters
        self.parameters.update(override_params)

        # Apply reasoning effort override (before provider resolution)
        if reasoning_effort:
            from src.llms.reasoning import apply_reasoning_effort

            apply_reasoning_effort(reasoning_effort, self.parameters, self.extra_body)

        # Store optional API key override (BYOK)
        self.api_key_override = api_key

        # Get provider info from manifest
        self.provider_info = self.model_config.get_provider_info(self.provider)

        # Extract provider configuration
        self.sdk = self.provider_info.get("sdk")
        self.env_key = self.provider_info.get("env_key")
        self.base_url = self.provider_info.get("base_url")

        # Apply base_url override (sentinel distinguishes "not set" from "set to None")
        if base_url_override is not _UNSET:
            self.base_url = base_url_override

        # Store response API flags for OpenAI SDK
        self.use_response_api = self.provider_info.get("use_response_api", False) if self.sdk in ("openai", "codex") else False
        self.use_previous_response_id = self.provider_info.get("use_previous_response_id", False) if self.sdk == "openai" else False

        # Optional default headers from provider config, with model-level beta merging
        self.default_headers = self.provider_info.get("default_headers")
        self._merge_additional_betas(model_info.get("additional_betas"))

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
            **override_params: Additional parameters to override defaults.

        Returns:
            A LangChain chat model instance.
        """
        instance = cls.__new__(cls)
        instance.model_config = cls.get_model_config()
        instance.custom_model_name = config.get("name", config["model_id"])
        instance.model = config["model_id"]
        instance.provider = config["provider"]
        instance.parameters = (config.get("parameters") or {}).copy()
        instance.extra_body = (config.get("extra_body") or {}).copy()
        instance.parameters.update(override_params)
        instance.api_key_override = api_key

        # Get provider info from manifest (empty dict for unknown providers)
        instance.provider_info = instance.model_config.get_provider_info(instance.provider)

        # Extract provider configuration; default to openai SDK for unknown providers
        # (most custom endpoints are OpenAI-compatible)
        instance.sdk = instance.provider_info.get("sdk") or "openai"
        instance.env_key = instance.provider_info.get("env_key")
        instance.base_url = instance.provider_info.get("base_url")

        if base_url_override is not _UNSET:
            instance.base_url = base_url_override

        # use_response_api: explicit config override > provider_info > False
        if config.get("_use_response_api") is not None:
            instance.use_response_api = bool(config["_use_response_api"]) and instance.sdk in ("openai", "codex")
        else:
            instance.use_response_api = (
                instance.provider_info.get("use_response_api", False)
                if instance.sdk in ("openai", "codex")
                else False
            )
        instance.use_previous_response_id = (
            instance.provider_info.get("use_previous_response_id", False)
            if instance.sdk == "openai"
            else False
        )
        instance.default_headers = instance.provider_info.get("default_headers")
        instance._merge_additional_betas(config.get("additional_betas"))
        return instance.get_llm()

    def get_llm(self):
        """
        Initializes and returns a LangChain LLM client for the configured provider.

        Returns:
            A LangChain chat model instance.

        Raises:
            ValueError: If required API keys are not set or provider is unsupported.
        """
        # Use the resolved SDK (already determined in __init__)
        if self.sdk == "openai":
            return self._get_openai_llm()
        elif self.sdk == "codex":
            return self._get_codex_llm()
        elif self.sdk == "deepseek":
            return self._get_deepseek_llm()
        elif self.sdk == "qwq":
            return self._get_qwq_llm()
        elif self.sdk == "anthropic":
            return self._get_anthropic_llm()
        elif self.sdk == "gemini":
            return self._get_gemini_llm()
        else:
            raise ValueError(f"Unsupported SDK: {self.sdk} for provider {self.provider}")

    def _resolve_api_key(self) -> str:
        """Resolve API key: BYOK override > env var > local fallback."""
        if self.api_key_override:
            return self.api_key_override
        if self.env_key:
            key = os.getenv(self.env_key)
            if not key:
                raise ValueError(f"{self.env_key} environment variable is not set")
            return key
        return "lm-studio" if self.provider == "lm-studio" else "EMPTY"

    def _resolve_base_url(self, param_name: str = "base_url") -> dict:
        """Resolve base URL with HOST_IP substitution. Returns dict to merge into params."""
        if not self.base_url:
            return {}
        url = self.base_url
        if "{HOST_IP}" in url:
            host_ip = os.getenv("HOST_IP")
            if not host_ip:
                raise ValueError(f"HOST_IP environment variable is not set for {self.provider}")
            url = url.replace("{HOST_IP}", host_ip)
        return {param_name: url}

    def _get_openai_llm(self):
        """Get OpenAI or OpenAI-compatible LLM."""
        params = {
            "model": self.model,
            "api_key": self._resolve_api_key(),
            "stream_usage": True,
            "max_retries": 5,
            "timeout": 600.0,
        }
        params.update(self._resolve_base_url("base_url"))

        if self.default_headers:
            params["default_headers"] = self.default_headers

        # Handle Response API if configured
        if self.use_response_api:
            params["output_version"] = "responses/v1"

        # Enable use_previous_response_id if configured in provider
        if self.use_previous_response_id:
            params["use_previous_response_id"] = True

        # Add all parameters from llm_config
        params.update(self.parameters)

        # Pass extra_body for provider-specific fields (e.g. caching, thinking)
        if self.extra_body:
            params["extra_body"] = self.extra_body

        return ChatOpenAI(**params)

    def _get_codex_llm(self):
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

        if self.default_headers:
            params["default_headers"] = self.default_headers

        if self.use_response_api:
            params["output_version"] = "responses/v1"

        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

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

        is_oauth = self.provider_info.get("auth_type") == "oauth"

        # Set API key: prefer BYOK override, then env var
        api_key = self.api_key_override or (os.getenv(self.env_key) if self.env_key else None)

        params = {
            "model": self.model,
            "api_key": api_key,
            "max_tokens": 64000,  # Default for Anthropic SDK models
            "max_retries": 5,
            "timeout": 600.0,  # 10 minutes - sufficient for long reasoning
        }

        if not params["api_key"]:
            raise ValueError(f"{self.env_key or 'ANTHROPIC_API_KEY'} environment variable is not set")

        # Set base URL from provider configuration if available
        if self.base_url:
            params["base_url"] = self.base_url

        if self.default_headers:
            params["default_headers"] = self.default_headers

        # Add all parameters from llm_config, excluding enable_caching
        # (enable_caching is not a ChatAnthropic parameter, it's used by our caching logic)
        # This will override max_tokens if explicitly set in model config
        filtered_params = {k: v for k, v in self.parameters.items() if k != "enable_caching"}
        params.update(filtered_params)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        # OAuth tokens (sk-ant-oat*) need Authorization: Bearer, not X-Api-Key.
        # ChatAnthropicOAuth redirects api_key → auth_token on the underlying SDK client.
        if is_oauth:
            return ChatAnthropicOAuth(**params)
        return ChatAnthropic(**params)

    def _get_gemini_llm(self):
        """Get Gemini LLM."""
        from langchain_google_genai import ChatGoogleGenerativeAI

        # Set API key: prefer BYOK override, then env var
        api_key = self.api_key_override or (os.getenv(self.env_key) if self.env_key else None)

        params = {
            "model": self.model,
            "api_key": api_key,
            "timeout": 600.0,  # 10 minutes - sufficient for long reasoning
        }

        if not params["api_key"]:
            raise ValueError(f"{self.env_key or 'GEMINI_API_KEY'} environment variable is not set")

        # Set base URL from provider configuration if available
        if self.base_url:
            params["base_url"] = self.base_url

        # Add all parameters from llm_config
        params.update(self.parameters)

        if self.extra_body:
            params["extra_body"] = self.extra_body

        return ChatGoogleGenerativeAI(**params)


# Backward compatibility functions
def create_llm(
    model: str,
    api_key: str | None = None,
    default_headers: dict | None = None,
    base_url=_UNSET,
    reasoning_effort: str | None = None,
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
    return instance.get_llm()


def create_llm_from_custom(
    config: dict,
    api_key: str | None = None,
    base_url=_UNSET,
    **kwargs,
):
    """
    Convenience function for creating an LLM from a user-defined custom model config.

    Args:
        config: Dict with model_id, provider, and optional parameters/extra_body.
        api_key: Optional API key override (e.g. from BYOK).
        base_url: Override base URL. _UNSET = no override, None = SDK default.
        **kwargs: Additional parameters to override.

    Returns:
        A LangChain chat model instance.
    """
    return LLM.from_custom_config(config, api_key=api_key, base_url_override=base_url, **kwargs)


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
    Models are grouped by their parent provider (e.g., anthropic-aws → anthropic).

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

def should_enable_caching(model_name: str) -> bool:
    """
    Check if a model should enable Anthropic prompt caching.

    Args:
        model_name: The model name from llm_config.json

    Returns:
        True if the model has enable_caching=True in its parameters
    """
    try:
        config = ModelConfig()
        model_info = config.get_model_config(model_name)
        if not model_info:
            return False

        # Check if model has enable_caching in parameters
        parameters = model_info.get("parameters", {})
        return parameters.get("enable_caching", False)
    except Exception:
        return False
