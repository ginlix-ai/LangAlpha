"""One renderer for every prompt template this package puts on the wire.

The templates ship inside the package, so a template that will not render is a
broken build rather than a runtime state. The exception propagates to the
turn-boundary and per-call handlers, which log it and send the call without the
context, so a packaging fault costs the context and never the turn.
"""

from __future__ import annotations

from typing import Any


def render_template(template_name: str, /, **kwargs: Any) -> str:
    """The named template, rendered against *kwargs* and stripped."""
    from ptc_agent.agent.prompts import get_loader

    return get_loader().render(template_name, **kwargs).strip()
