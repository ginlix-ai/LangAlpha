"""Request bodies of the setup step's routes."""

from pydantic import BaseModel, Field


class SaveKeysRequest(BaseModel):
    # A null removes the key.
    keys: dict[str, str | None]


class KeyTestRequest(BaseModel):
    name: str
    value: str = Field(min_length=1, max_length=4096)


class SaveSandboxRequest(BaseModel):
    provider: str
    # Left out, the saved groups stay as they are.
    install_groups: list[str] | None = None
