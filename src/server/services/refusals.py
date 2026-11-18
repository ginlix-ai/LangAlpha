"""The refusals no retry or workaround can get past: a rule of the user's
policy, or an account whose access through its organization has ended."""

from __future__ import annotations

from fastapi import HTTPException


class Refusal(HTTPException):
    """Answered as any ``HTTPException`` is, and read as its message where it
    is not answered as a response: a turn's error frame or an agent's tool
    result."""

    def __str__(self) -> str:
        return str(self.detail["message"])
