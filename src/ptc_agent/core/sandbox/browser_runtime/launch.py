"""What the open browser was launched with, which every command must repeat.

The engine takes a user agent and the stream's settings, and Chrome its scale,
only when the browser starts, and restarts it, losing its pages, when a command
asks for anything else. So both are settled together when a browser starts and
kept in one record until it closes, and every command, the agent's or one run
for the person, runs with that record.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .protocol import STREAM_QUALITIES, StreamQuality, StreamQualityName, state_file, write_state

_FILE = "launch"


@dataclass(frozen=True)
class Launch:
    """The user agent, empty for the engine's own, and the live view's quality.

    ``closing`` marks the launch of a browser a command means to close, after
    working on it or not: the command runs on the launch as it is, since a
    changed one would restart the browser under it, and the user agent goes
    only once the next command finds the browser gone. One that finds it up,
    since the close never ran or never finished, keeps it.
    """

    user_agent: str
    quality: StreamQualityName
    closing: bool = False

    @property
    def stream(self) -> StreamQuality:
        return STREAM_QUALITIES[self.quality]


def current() -> Launch | None:
    """The record of the browser last launched, or None when there is none."""
    try:
        stored = json.loads(state_file(_FILE).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(stored, dict):
        return None
    user_agent, quality = stored.get("user_agent"), stored.get("quality")
    if not isinstance(user_agent, str) or quality not in STREAM_QUALITIES:
        return None
    return Launch(user_agent, quality, stored.get("closing") is True)


def write(launch: Launch) -> None:
    stored: dict[str, object] = {"user_agent": launch.user_agent, "quality": launch.quality}
    if launch.closing:
        stored["closing"] = True
    write_state(_FILE, json.dumps(stored))


def forget() -> None:
    state_file(_FILE).unlink(missing_ok=True)
