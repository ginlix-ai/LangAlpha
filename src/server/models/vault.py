"""Request models for the vault routers.

Both tiers accept the identical body, so the models live here rather than in
one router that the other imports sideways.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

_NUL_MESSAGE = "Value must not contain a null byte"

MAX_SECRET_VALUE_LENGTH = 4096
_LENGTH_MESSAGE = (
    f"Value must be at most {MAX_SECRET_VALUE_LENGTH} characters"
)


def validate_secret_value(value: str | None) -> str | None:
    """What storage refuses, wherever a value arrives: a null byte, and length.

    One value fans out to sinks with different control-character rules (relay
    and probe headers, a stdio server's env and argv, the sandbox vault file,
    ``vault.get()`` in agent code), so each sink enforces its own where it
    builds its value, and storage refuses only what none of them can hold. A
    null byte is that: ``os.environ`` and argv both raise on one. It lives here
    rather than in the two models alone because an import builds its secrets
    straight from a pasted blob.
    """
    if value is None:
        return value
    if len(value) > MAX_SECRET_VALUE_LENGTH:
        raise ValueError(_LENGTH_MESSAGE)
    if "\x00" in value:
        raise ValueError(_NUL_MESSAGE)
    return value


class CreateSecretRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    value: str = Field(..., min_length=1, max_length=MAX_SECRET_VALUE_LENGTH)
    description: str = Field("", max_length=256)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError(
                "Name must be 1-64 characters: letters, digits, underscores; "
                "must start with a letter or underscore"
            )
        return v

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str) -> str:
        return validate_secret_value(v)


class UpdateSecretRequest(BaseModel):
    value: str | None = Field(None, min_length=1, max_length=MAX_SECRET_VALUE_LENGTH)
    description: str | None = Field(None, max_length=256)

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str | None) -> str | None:
        return validate_secret_value(v)
