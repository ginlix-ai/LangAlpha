"""
Skill context utilities for the chat endpoint.

Pure request-parsing helpers: extract ``SkillContext`` items from a request's
``additional_context`` and detect ``/command`` prefixes in the message text.
Body loading + inline injection live in ``ptc_agent`` (``SkillsMiddleware``);
the server only resolves *which* skills the turn requested.
"""

import logging
import re
from typing import Any, List, Optional

from src.server.models.additional_context import SkillContext
from ptc_agent.agent.middleware.skills import SkillMode, get_command_to_skill_map

logger = logging.getLogger(__name__)


def parse_skill_contexts(
    additional_context: Optional[List[Any]]
) -> List[SkillContext]:
    """Extract skill contexts from additional_context list.

    Filters the additional_context list to return only SkillContext items.

    Args:
        additional_context: List of context items from ChatRequest

    Returns:
        List of SkillContext objects

    Example:
        >>> contexts = parse_skill_contexts([
        ...     {"type": "skills", "name": "user-profile", "instruction": "Help onboard"},
        ... ])
        >>> len(contexts)
        1
        >>> contexts[0].name
        'user-profile'
    """
    if not additional_context:
        return []

    skill_contexts = []

    for ctx in additional_context:
        # Handle both dict and Pydantic model
        if isinstance(ctx, dict):
            ctx_type = ctx.get("type")
            if ctx_type == "skills":
                skill_contexts.append(SkillContext(
                    type="skills",
                    name=ctx.get("name", ""),
                    instruction=ctx.get("instruction"),
                ))
        elif isinstance(ctx, SkillContext):
            skill_contexts.append(ctx)
        elif hasattr(ctx, "type") and ctx.type == "skills":
            skill_contexts.append(SkillContext(
                type="skills",
                name=getattr(ctx, "name", ""),
                instruction=getattr(ctx, "instruction", None),
            ))

    if skill_contexts:
        logger.debug(
            f"Parsed {len(skill_contexts)} skill contexts: "
            f"{[s.name for s in skill_contexts]}"
        )

    return skill_contexts


def detect_slash_commands(
    message_text: str,
    mode: SkillMode | None = None,
) -> tuple[str, list[SkillContext]]:
    """Detect slash command prefixes in user message text.

    Scans the message for ``/<command>`` tokens that match registered skills.
    Returns the cleaned message (with the command prefix stripped) and a list
    of SkillContext objects for the matched commands.

    This provides a server-side fallback for skill activation — skills are
    activated even if the frontend fails to send ``additional_context``.

    Args:
        message_text: Raw user message text
        mode: Optional agent mode filter

    Returns:
        Tuple of (cleaned_message, detected_skill_contexts)
    """
    if not message_text or not message_text.startswith("/"):
        return message_text, []

    command_map = get_command_to_skill_map(mode)
    if not command_map:
        return message_text, []

    # Build regex: match /<command> at start of message, followed by whitespace or end
    # Sort by length descending to prefer longer matches (e.g. "/3-statement-model" over "/3")
    sorted_commands = sorted(command_map.keys(), key=len, reverse=True)
    escaped = [re.escape(cmd) for cmd in sorted_commands]
    pattern = re.compile(r"^/(" + "|".join(escaped) + r")(?:\s+|$)")

    match = pattern.match(message_text)
    if not match:
        return message_text, []

    command_name = match.group(1)
    skill_name = command_map[command_name]

    # Strip the /command prefix from the message
    cleaned = message_text[match.end():].strip()
    if not cleaned:
        # Message was just the command with no body — keep original text as-is
        # so the agent at least knows what the user asked for
        cleaned = message_text

    detected = [SkillContext(type="skills", name=skill_name)]
    logger.debug(
        f"Detected slash command '/{command_name}' -> skill '{skill_name}'"
    )
    return cleaned, detected
