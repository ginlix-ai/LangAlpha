"""Registry of repository-shipped JavaScript workflows."""

from __future__ import annotations

from functools import cache
from pathlib import Path

import structlog

from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowMeta,
    WorkflowScriptError,
    compile_check,
)

logger = structlog.get_logger(__name__)


class PrebuiltWorkflowRegistry:
    """Load validated prebuilt workflows from a repository root."""

    def __init__(self, root: Path | None = None) -> None:
        self._scripts: dict[str, str] = {}
        self._metadata: dict[str, WorkflowMeta] = {}
        workflows_dir = (root or Path.cwd()) / "workflows"
        if not workflows_dir.is_dir():
            # cwd-anchored: a process launched outside the repo root gets an
            # empty registry — say so instead of vanishing silently.
            logger.info(
                "No prebuilt workflows directory; registry is empty",
                path=str(workflows_dir),
            )
            return
        for script_path in sorted(workflows_dir.glob("*/workflow.js")):
            directory_name = script_path.parent.name
            try:
                source = script_path.read_text(encoding="utf-8")
                meta = compile_check(source)
                if meta.name != directory_name:
                    raise WorkflowScriptError(
                        f"meta.name '{meta.name}' must match directory "
                        f"'{directory_name}'"
                    )
            except Exception as error:
                logger.warning(
                    "Skipping invalid prebuilt workflow",
                    path=str(script_path),
                    error=str(error),
                )
                continue
            self._scripts[directory_name] = source
            self._metadata[directory_name] = meta

    def get(self, name: str) -> str | None:
        return self._scripts.get(name)

    def meta(self, name: str) -> WorkflowMeta | None:
        return self._metadata.get(name)

    def names(self) -> list[str]:
        return sorted(self._scripts)

    def files(self) -> dict[str, str]:
        """Mount keys — flat ``<name>.js``, matching the user tier's shape so
        a script edited in the overlay lands on the name it runs under."""
        return {f"{name}.js": self._scripts[name] for name in self.names()}


@cache
def get_prebuilt_workflows() -> PrebuiltWorkflowRegistry:
    return PrebuiltWorkflowRegistry()
