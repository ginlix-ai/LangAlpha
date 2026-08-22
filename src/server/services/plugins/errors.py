"""Failure vocabulary for the plugin pipeline."""

from typing import Any

from src.server.models.plugin import Diagnostic


class PluginFatal(Exception):
    """A failure at a severity where nothing may be written.

    Covers the spec's fatal rungs (unreadable archive, invalid plugin.json
    beyond the two tolerated warns) and — by our own authority within the
    ``ai.langalpha`` namespace — any extension error. The router maps it to a
    422 carrying the collected diagnostics.
    """

    def __init__(
        self, message: str, *, diagnostics: list[Diagnostic] | None = None
    ):
        super().__init__(message)
        self.diagnostics = diagnostics or []


class PluginAmbiguous(Exception):
    """The archive holds more than one plugin and no subdir chose between
    them. Carries the discovered candidates so the router can return a
    structured 422 that the install wizard renders as a chooser."""

    def __init__(self, candidates: list[Any]):
        super().__init__(
            f"the archive contains {len(candidates)} plugins; pick one"
        )
        self.candidates = candidates


class PluginRejected(Exception):
    """A request the plugin's own declarations refuse (a 422 with a message).

    Distinct from ValueError so a router that maps ValueError to 409 (a
    conflict with installed state) can't swallow it as one.
    """
