"""What storage refuses a vault value for, and what it deliberately leaves alone.

The rule used to be every control character, which cost the vault every
multi-line secret a user actually holds. It is now a null byte and a length
cap: the sinks that cannot carry a newline refuse it where they build their
value, and the header sink's half of that split is pinned beside
``resolve_header_refs``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.server.models.vault import (
    MAX_SECRET_VALUE_LENGTH,
    CreateSecretRequest,
    UpdateSecretRequest,
    validate_secret_value,
)

PEM = "-----BEGIN PRIVATE KEY-----\nMIIBVgIBADANBgkq\n-----END PRIVATE KEY-----\n"


def test_a_pem_is_stored_with_its_newlines_intact():
    """The secret this rule used to make unstorable, and the reason it moved.

    A trimmed or rejected PEM is not the key it came from, so nothing
    downstream can repair it; only the header sink needs a single-line value,
    and only the header sink refuses one.
    """
    assert validate_secret_value(PEM) == PEM


def test_a_tab_is_part_of_the_value():
    assert validate_secret_value("sk-live\tpadded") == "sk-live\tpadded"


def test_a_null_byte_is_refused():
    """The one byte no sink accepts: ``os.environ`` and argv both raise on it."""
    with pytest.raises(ValueError, match="null byte"):
        validate_secret_value("sk-live\x00rest")


def test_the_length_cap_still_holds():
    assert validate_secret_value("a" * MAX_SECRET_VALUE_LENGTH)
    with pytest.raises(ValueError, match=str(MAX_SECRET_VALUE_LENGTH)):
        validate_secret_value("a" * (MAX_SECRET_VALUE_LENGTH + 1))


def test_an_absent_value_is_not_a_value():
    assert validate_secret_value(None) is None


@pytest.mark.parametrize(
    "model, extra",
    [(CreateSecretRequest, {"name": "API_KEY"}), (UpdateSecretRequest, {})],
    ids=["create", "update"],
)
def test_both_request_models_carry_the_one_rule(model, extra):
    assert model(value=PEM, **extra).value == PEM
    with pytest.raises(ValidationError):
        model(value="sk-live\x00rest", **extra)
    with pytest.raises(ValidationError):
        model(value="a" * (MAX_SECRET_VALUE_LENGTH + 1), **extra)
