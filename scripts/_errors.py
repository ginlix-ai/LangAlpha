"""How the operator scripts under ``scripts/`` name a failure in their output."""

from __future__ import annotations

import traceback
from pathlib import Path


def where(exc: BaseException) -> str:
    """An exception's type and innermost frame. Its message can quote message
    content (a validation error echoes its input), so it is not printed."""
    frames = traceback.extract_tb(exc.__traceback__)
    at = f" at {Path(frames[-1].filename).name}:{frames[-1].lineno}" if frames else ""
    return f"{type(exc).__name__}{at}"
