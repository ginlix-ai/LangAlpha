"""Request models for the vault routers.

Both tiers accept the identical body, so the models live here rather than in
one router that the other imports sideways.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class CreateSecretRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    value: str = Field(..., min_length=1, max_length=4096)
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


class UpdateSecretRequest(BaseModel):
    value: str | None = Field(None, min_length=1, max_length=4096)
    description: str | None = Field(None, max_length=256)
