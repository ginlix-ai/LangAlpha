"""Jinja2 template loader for prompt templates."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from jinja2 import Environment, FileSystemLoader

from ptc_agent.agent.prompts.formatter import workspace_path_vars
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from src.utils.timezone_utils import get_timezone_label


class PromptLoader:
    """Load and render Jinja2 prompt templates.

    Uses Jinja2's built-in template object caching for efficient
    repeated template lookups.

    Time is captured once at initialization to ensure consistent
    date values across all prompts (preserves input cache).
    """

    def __init__(
        self,
        templates_dir: Path | None = None,
        session_start_time: datetime | None = None,
    ) -> None:
        """Initialize the prompt loader.

        Args:
            templates_dir: Path to templates directory. Defaults to
                          ./templates relative to this file.
            session_start_time: Time to use for all prompts. If None,
                               captures current time at initialization.
        """
        self.templates_dir = templates_dir or Path(__file__).parent / "templates"
        # Capture session start time once at initialization for cache consistency
        self._session_start_time = session_start_time or datetime.now(tz=UTC)
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        self._config = self._load_config()

    @property
    def session_date(self) -> str:
        """Get formatted session date (YYYY-MM-DD)."""
        return self._session_start_time.strftime("%Y-%m-%d")

    @property
    def session_datetime(self) -> str:
        """Get formatted session datetime (YYYY-MM-DD HH:MM:SS)."""
        return self._session_start_time.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def session_start_time(self) -> datetime:
        """Get the raw session start time."""
        return self._session_start_time

    def _load_config(self) -> dict:
        """Load configuration from prompts.yaml."""
        config_path = Path(__file__).parent / "config" / "prompts.yaml"
        if config_path.exists():
            return yaml.safe_load(config_path.read_text())
        return {}

    def render(self, template_name: str, **kwargs: Any) -> str:
        """Render a template with variables.

        Args:
            template_name: Path to template relative to templates_dir
            **kwargs: Variables to pass to the template

        Returns:
            Rendered template string
        """
        template = self.env.get_template(template_name)
        # Build context with session time, config defaults, then user overrides
        # Order: session time (lowest) -> config defaults -> kwargs (highest)
        context = {
            "date": self.session_date,
            "datetime": self.session_datetime,
            # The stock layout, so a caller with no workspace bound (a preview,
            # a component rendered on its own) still gets a coherent path table
            # instead of a template error.
            **workspace_path_vars(None, root=DEFAULT_SANDBOX_ROOT),
            **self._config.get("defaults", {}),
            **kwargs,  # User can override date if needed
        }
        return template.render(**context)

    def get_system_prompt(self, **kwargs: Any) -> str:
        """Get the main system prompt.

        Args:
            **kwargs: Variables to pass to the template

        Returns:
            Rendered system prompt
        """
        return self.render("system.md.j2", **kwargs)

    def get_subagent_prompt(self, subagent_type: str, **kwargs: Any) -> str:
        """Get prompt for a sub-agent type.

        Args:
            subagent_type: Sub-agent type name (researcher, general)
            **kwargs: Variables to pass to the template

        Returns:
            Rendered sub-agent prompt
        """
        return self.render(f"subagents/{subagent_type}.md.j2", **kwargs)

    def get_subagent_base_prompt(
        self,
        *,
        identity_line: str = "",
        role_prompt: str = "",
        role_prompt_template: str | None = None,
        preloaded_skills_content: str = "",
        sections: dict[str, bool] | None = None,
        **kwargs: Any,
    ) -> str:
        """Render a subagent prompt using the unified ``subagent_base.md.j2``.

        Args:
            identity_line: First line of the prompt (agent identity).
            role_prompt: Inline role-specific instructions.
            role_prompt_template: Path to role template (relative to
                ``templates/subagents/``).  Rendered and used as ``role_prompt``
                if ``role_prompt`` is empty.
            preloaded_skills_content: Pre-loaded SKILL.md content to inject.
            sections: Section toggle overrides (``section_<name>`` vars).
            **kwargs: Additional template variables.

        Returns:
            Rendered subagent prompt string.
        """
        # Render role template if specified and no inline role_prompt provided
        if role_prompt_template and not role_prompt:
            role_prompt = self.render(f"subagents/{role_prompt_template}", **kwargs)

        # Convert sections dict to section_* template variables
        section_vars: dict[str, bool] = {}
        for key, value in (sections or {}).items():
            section_vars[f"section_{key}"] = value

        return self.render(
            "subagent_base.md.j2",
            identity_line=identity_line,
            role_prompt=role_prompt,
            preloaded_skills_content=preloaded_skills_content,
            **section_vars,
            **kwargs,
        )

    def get_component(self, component_name: str, **kwargs: Any) -> str:
        """Get a single component template.

        Args:
            component_name: Component name (search_first, visualizations, etc.)
            **kwargs: Variables to pass to the template

        Returns:
            Rendered component string
        """
        return self.render(f"components/{component_name}.md.j2", **kwargs)


# Singleton instance
_loader: PromptLoader | None = None


def get_loader(session_start_time: datetime | None = None) -> PromptLoader:
    """Get the singleton PromptLoader instance.

    Args:
        session_start_time: Optional time to use if creating a new loader.
                           Ignored if loader already exists.

    Returns:
        The global PromptLoader instance
    """
    global _loader
    if _loader is None:
        _loader = PromptLoader(session_start_time=session_start_time)
    return _loader


def init_loader(session_start_time: datetime | None = None) -> PromptLoader:
    """Initialize a new loader with a specific start time.

    This resets any existing loader and creates a new one with the
    specified start time. Use this at the start of a new session.

    Args:
        session_start_time: Time to use for all prompts. If None,
                           captures current time.

    Returns:
        The new PromptLoader instance
    """
    global _loader
    _loader = PromptLoader(session_start_time=session_start_time)
    return _loader


def reset_loader() -> None:
    """Reset the singleton loader (useful for testing)."""
    global _loader
    _loader = None


def format_current_time(dt: datetime, timezone_str: str | None = None) -> str:
    """Format a datetime into a human-readable current time string.

    Pure function — never calls datetime.now() itself. Takes a pre-captured
    datetime and optional timezone string, returns formatted time.

    Args:
        dt: Pre-captured timezone-aware datetime (typically from datetime.now(tz=UTC)).
        timezone_str: Optional IANA timezone (e.g., "America/New_York") to convert to.

    Returns:
        Formatted string like "3:00 PM EST, Monday, February 23, 2026"
    """
    if timezone_str:
        try:
            dt = dt.astimezone(ZoneInfo(timezone_str))
        except (KeyError, TypeError):
            pass
    tz_label = get_timezone_label(dt)
    return dt.strftime("%-I:%M %p") + f" {tz_label}, " + dt.strftime("%A, %B %-d, %Y")
