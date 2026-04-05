"""Monkey-patches for upstream library bugs."""

import logging

logger = logging.getLogger(__name__)


def _patch_langchain_anthropic_usage_metadata():
    """Fix TypeError when cache_creation fields are None.

    langchain-anthropic v1.4.0 crashes on dict.get(k, 0) returning None
    when third-party providers return null for cache token fields.
    """
    try:
        import langchain_anthropic.chat_models as chat_models
        from pydantic import BaseModel

        original_fn = chat_models._create_usage_metadata
        if getattr(original_fn, "_is_patched", False):
            return

        def _patched(anthropic_usage: BaseModel):
            cache_creation = getattr(anthropic_usage, "cache_creation", None)
            if cache_creation is not None:
                for field in ("ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"):
                    if isinstance(cache_creation, dict):
                        if cache_creation.get(field) is None:
                            cache_creation[field] = 0
                    elif getattr(cache_creation, field, None) is None:
                        object.__setattr__(cache_creation, field, 0)
            return original_fn(anthropic_usage)

        _patched._is_patched = True
        chat_models._create_usage_metadata = _patched
        logger.debug("Patched langchain_anthropic._create_usage_metadata for None cache values")
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic: {e}")
