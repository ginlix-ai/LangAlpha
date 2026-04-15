"""
Tests for the key test endpoint models and helpers (src/server/app/api_keys.py).

Covers TestApiKeyRequest Pydantic validation and the _sanitize_error helper.
No HTTP calls — tests the validation model and utility function directly.
"""

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# TestApiKeyRequest validation
# ---------------------------------------------------------------------------


def test_valid_provider_and_key():
    """Valid provider and key format passes validation."""
    from src.server.app.api_keys import TestApiKeyRequest

    req = TestApiKeyRequest(provider="anthropic", api_key="sk-" + "a" * 20)
    assert req.provider == "anthropic"
    assert req.api_key == "sk-" + "a" * 20


def test_empty_key_allowed():
    """Empty key is allowed for local providers (lm-studio, vllm, ollama)."""
    from src.server.app.api_keys import TestApiKeyRequest

    req = TestApiKeyRequest(provider="lm-studio", api_key="")
    assert req.api_key == ""


def test_key_too_long():
    """Key longer than 4096 chars raises validation error."""
    from src.server.app.api_keys import TestApiKeyRequest

    with pytest.raises(ValidationError) as exc_info:
        TestApiKeyRequest(provider="openai", api_key="k" * 4097)

    errors = exc_info.value.errors()
    assert any("4096" in str(e["msg"]) for e in errors)


def test_non_ascii_key():
    """Non-ASCII key raises validation error."""
    from src.server.app.api_keys import TestApiKeyRequest

    with pytest.raises(ValidationError) as exc_info:
        TestApiKeyRequest(provider="openai", api_key="sk-" + "\u00e9" * 20)

    errors = exc_info.value.errors()
    assert any("ASCII" in str(e["msg"]) for e in errors)


# ---------------------------------------------------------------------------
# _sanitize_error
# ---------------------------------------------------------------------------


def test_sanitize_error_redacts_key_fragments():
    """_sanitize_error strips API key fragments from error messages."""
    from src.server.app.api_keys import _sanitize_error

    msg = "Invalid API key: sk-ant-api03-abcdefghijklmn for account"
    sanitized = _sanitize_error(msg)
    assert "sk-ant-api03-abcdefghijklmn" not in sanitized
    assert "[REDACTED]" in sanitized
    # Non-key text should remain
    assert "Invalid API key:" in sanitized
    assert "for account" in sanitized


def test_sanitize_error_leaves_safe_messages():
    """_sanitize_error leaves messages without key fragments untouched."""
    from src.server.app.api_keys import _sanitize_error

    msg = "Connection refused: timeout after 5s"
    assert _sanitize_error(msg) == msg
