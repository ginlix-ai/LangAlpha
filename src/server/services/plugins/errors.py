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
    structured 422 that the install wizard renders as a chooser.

    ``fallback_path`` marks the second way this is raised: a plugin *was*
    selectable, and only after validating it did it turn out to carry no
    components at all. Asking is right when a person is waiting, but an
    update reconciling a plugin it already installed is not asking anyone,
    and must go on meaning the same plugin. That caller installs
    ``fallback_path`` instead of raising the chooser.
    """

    def __init__(self, candidates: list[Any], *, fallback_path: str | None = None):
        super().__init__(
            f"the plugin this archive selects carries no MCP servers and no "
            f"skills; the archive offers {len(candidates)} others, pick one"
            if fallback_path is not None
            else f"the archive contains {len(candidates)} plugins; pick one"
        )
        self.candidates = candidates
        self.fallback_path = fallback_path


class PluginRejected(Exception):
    """A request the plugin's own declarations refuse (a 422 with a message).

    Distinct from ValueError so a router that maps ValueError to 409 (a
    conflict with installed state) can't swallow it as one.
    """
