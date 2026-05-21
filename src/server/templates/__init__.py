"""Template system for LangAlpha.

Templates are upper-layer "applications" (e.g. sirius-valuation) that group
workspaces under a shared dashboard. A template defines:

  1. A manifest (id / name / description / form fields) — exposed via API
  2. An ``initial_prompt_template`` — the natural-language instruction sent
     to the agent on instantiation. The template can use ``{display_name}``,
     ``{entry_key}`` and any ``params`` keys.
  3. A schema for ``summary`` / ``payload`` (validated by the frontend; the
     backend stores them as opaque JSONB).

Adding a new template = drop a new manifest into ``manifests/`` and register
it in ``registry.py``. No DB schema changes, no orchestrator changes.
"""

from src.server.templates.registry import (
    TEMPLATE_REGISTRY,
    TemplateDefinition,
    get_template,
    list_template_manifests,
)

__all__ = [
    "TEMPLATE_REGISTRY",
    "TemplateDefinition",
    "get_template",
    "list_template_manifests",
]
