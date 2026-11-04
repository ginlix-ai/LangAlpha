"""The APNs credentials, or None when this server does not send pushes."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.config import env

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApnsConfig:
    key_id: str
    team_id: str
    private_key: str
    bundle_ids: tuple[str, ...]


def push_available() -> bool:
    """Whether this server sends pushes at all, without reading the key file."""
    return env.APNS_ENABLED


def apns_config() -> Optional[ApnsConfig]:
    """The credentials, reading a key file only now: the file may be mounted
    after the process starts, and a server that never sends never needs it.
    An unreadable file is logged and counts as off."""
    if not env.APNS_ENABLED:
        return None
    key = env.APNS_PRIVATE_KEY
    if not key:
        try:
            key = Path(env.APNS_PRIVATE_KEY_PATH).read_text().strip()
        except OSError as e:
            logger.error(
                "[PUSH] APNs key file %s is unreadable: %s",
                env.APNS_PRIVATE_KEY_PATH,
                type(e).__name__,
            )
            return None
    return ApnsConfig(
        key_id=env.APNS_KEY_ID,
        team_id=env.APNS_TEAM_ID,
        private_key=key,
        bundle_ids=env.APNS_BUNDLE_IDS,
    )
