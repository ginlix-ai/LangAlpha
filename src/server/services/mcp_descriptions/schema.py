"""What the model returns, and the version its rows are cached under.

The output model is the structured-output schema the model fills, so a field
here is a question the model is asked; the stored rows keep the same name.
"""

from __future__ import annotations

from pydantic import BaseModel

# A change to the instructions or the output shape bumps this, which makes every
# cached description stale: the worker writes new ones and prompt text stops
# reading the old. Old rows stay for audit.
PROFILE_PROMPT_VERSION = "1"


class ServerProfile(BaseModel):
    """The server as a whole: the line the agent reads beside its name."""

    description: str
