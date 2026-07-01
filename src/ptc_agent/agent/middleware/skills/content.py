"""Skill content loading + inline-injection helpers.

Reads SKILL.md from the local filesystem and builds the ``<loaded-skill>`` blocks
appended to the user message when a skill is activated. Lives in ``ptc_agent``
(not the server) so ``SkillsMiddleware`` can own body injection without importing
``src.server`` — keeping the layering one-directional (middleware → skills only).
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from ptc_agent.agent.middleware._message_utils import message_id as _message_id
from ptc_agent.agent.middleware.skills.registry import SkillMode, get_skill

logger = logging.getLogger(__name__)


def loaded_skill_marker(name: str, message_id: str | None = None) -> str:
    """Opening tag marking a skill's injected SKILL.md body in the message history.

    Single source of truth shared by the writer (``build_skill_content``) and the
    scanner (``compute_already_loaded``) so the marker format can't drift between them.

    The optional ``mid`` attribute binds the marker to the framework-assigned id of
    the message the body is written into. The scanner only treats a marker as live
    when its ``mid`` equals the id of the message it is found in, so neither user-typed
    text nor a SKILL.md that documents this format can forge a "body still loaded"
    match — the id is assigned by the framework, not by content anyone can author.
    """
    if message_id:
        return f'<loaded-skill name="{name}" mid="{message_id}">'
    return f'<loaded-skill name="{name}">'


@dataclass(frozen=True)
class SkillRequest:
    """A skill the client asked to activate this turn.

    Lightweight stand-in for the server's ``SkillContext`` so this module stays
    free of any ``src.server`` dependency; only ``name``/``instruction`` are used.
    """

    name: str
    instruction: Optional[str] = None


@dataclass
class SkillPrefixResult:
    """Result of building skill content for inline injection."""

    content: str  # Formatted skill text wrapped in <loaded-skill> tags
    loaded_skill_names: list[str] = field(default_factory=list)  # Skills injected fresh this turn (excludes already-loaded)


def load_skill_content(
    skill_name: str,
    skill_dirs: Optional[list[str]] = None,
    mode: SkillMode | None = None,
) -> Optional[str]:
    """Load SKILL.md content for a skill from local file system.

    Searches through skill directories to find and load the SKILL.md file
    for the specified skill.

    Args:
        skill_name: Name of the skill (e.g. 'user-profile')
        skill_dirs: Optional list of local skill directories to search.
                   If not provided, uses project_root/skills.
        mode: Optional agent mode filter. If provided, only loads skills
              whose exposure matches the mode.

    Returns:
        Content of SKILL.md as string, or None if not found
    """
    # Verify skill exists in registry (and matches mode if specified)
    skill = get_skill(skill_name, mode=mode)
    if not skill:
        logger.warning(f"Skill '{skill_name}' not found in registry")
        return None

    # Default skill directory: project_root/skills
    if skill_dirs is None:
        project_root = Path.cwd()
        skill_dirs = [str(project_root / "skills")]

    # Search for SKILL.md in each directory (last wins)
    content = None

    for skill_dir in skill_dirs:
        skill_md_path = Path(skill_dir) / skill_name / "SKILL.md"

        if skill_md_path.exists():
            try:
                content = skill_md_path.read_text(encoding="utf-8")
                logger.debug(
                    f"Loaded SKILL.md for '{skill_name}' from {skill_md_path}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to read SKILL.md for '{skill_name}' "
                    f"from {skill_md_path}: {e}"
                )

    if content is None:
        logger.warning(
            f"SKILL.md not found for skill '{skill_name}' in any skill directory"
        )

    return content


def build_tool_descriptions(
    skill_name: str, mode: SkillMode | None = None
) -> Optional[str]:
    """Build formatted tool descriptions for a skill.

    Mirrors the format from ``SkillsMiddleware._build_skill_result``.

    Args:
        skill_name: Name of the skill
        mode: Optional agent mode filter

    Returns:
        Formatted tool description string, or None if skill has no tools
    """
    skill = get_skill(skill_name, mode=mode)
    if not skill or not skill.tools:
        return None

    return skill.format_tool_descriptions()


def build_skill_content(
    skills: list[SkillRequest],
    skill_dirs: Optional[list[str]] = None,
    mode: SkillMode | None = None,
    already_loaded: Optional[set[str]] = None,
    message_id: Optional[str] = None,
) -> Optional[SkillPrefixResult]:
    """Build skill content for inline injection into the user message.

    Creates formatted skill content wrapped in ``<loaded-skill>`` XML tags,
    suitable for appending inline to the last user message. Also returns the
    list of freshly loaded skill names so the caller can set ``loaded_skills``
    in graph state for immediate tool availability.

    Skills whose name is in ``already_loaded`` are active in the thread already —
    their tools persist via ``loaded_skills`` state — so the (large) SKILL.md body
    is skipped and only their per-turn instruction is re-emitted (e.g. MarketView
    re-sends chart-annotation every turn with a fresh symbol/timeframe).

    Args:
        skills: Skills to load (anything exposing ``.name``/``.instruction``).
        skill_dirs: Optional list of local skill directories to search.
        mode: Optional agent mode filter. Skills whose exposure doesn't match the
              mode are skipped.
        already_loaded: Skill names already active in the thread; their bodies are
              skipped (instruction still refreshed).
        message_id: Framework id of the message this content is appended to. Embedded
              in each block's marker so the dedup scanner can verify the marker really
              belongs to that message (see ``loaded_skill_marker``).

    Returns:
        SkillPrefixResult with content string and freshly loaded skill names, or
        None if there is nothing to inject.
    """
    if not skills:
        return None

    already_loaded = already_loaded or set()
    newly_loaded: list[str] = []
    skill_blocks: list[str] = []
    instructions: list[tuple[str, str]] = []
    seen_names: set[str] = set()

    for skill_ctx in skills:
        # Same skill named twice in one request: emit it once. ``already_loaded``
        # only guards prior turns; this guards intra-turn duplication (e.g.
        # duplicate additional_context entries) so a body/instruction can't be
        # pasted twice in the same message.
        if skill_ctx.name in seen_names:
            continue
        seen_names.add(skill_ctx.name)

        # Already active this thread AND still exposed in this mode: tools persist
        # in state, so skip the body and only refresh the instruction (cheap, may
        # carry per-turn context). The mode re-check keeps this branch consistent
        # with the fresh path below — a stale loaded_skills entry for a skill not
        # exposed in the current mode falls through and is skipped, not injected.
        if skill_ctx.name in already_loaded and get_skill(skill_ctx.name, mode=mode):
            if skill_ctx.instruction:
                instructions.append((skill_ctx.name, skill_ctx.instruction))
            continue

        content = load_skill_content(skill_ctx.name, skill_dirs, mode=mode)
        if not content:
            logger.warning(f"Skipping skill '{skill_ctx.name}': SKILL.md not found")
            continue

        newly_loaded.append(skill_ctx.name)

        # Build per-skill block with tool descriptions
        block_parts = [content]
        tool_desc = build_tool_descriptions(skill_ctx.name, mode=mode)
        if tool_desc:
            block_parts.append(f"\n**Available tools:**\n{tool_desc}")
            block_parts.append(
                "You can call these tools directly without needing to call LoadSkill."
            )

        block_content = "\n".join(block_parts)
        skill_blocks.append(
            f"{loaded_skill_marker(skill_ctx.name, message_id)}\n"
            f"{block_content}\n</loaded-skill>"
        )

        if skill_ctx.instruction:
            instructions.append((skill_ctx.name, skill_ctx.instruction))

    # Nothing to inject: no fresh bodies and no instructions to refresh.
    if not skill_blocks and not instructions:
        return None

    # Combine all skill blocks
    parts = list(skill_blocks)

    # Add instructions. Use the bare single-instruction form only when exactly
    # one skill is represented in the whole message; otherwise name each one so
    # the owning skill is unambiguous (a fresh body block plus a separate
    # already-loaded skill's instruction would otherwise read as orphaned).
    if instructions:
        represented = set(newly_loaded) | {name for name, _ in instructions}
        if len(instructions) == 1 and len(represented) == 1:
            parts.append(f"\n[Instruction: {instructions[0][1]}]")
        else:
            parts.append("\n[Instructions]")
            parts.extend(f"- {name}: {text}" for name, text in instructions)

    combined_content = "\n\n".join(parts)

    logger.debug(
        f"Built skill content: fresh={newly_loaded}, instructions={len(instructions)}"
    )

    return SkillPrefixResult(
        content=combined_content,
        loaded_skill_names=newly_loaded,
    )


def _message_text(message: Any) -> str:
    """Best-effort plain text of a checkpoint message (str or multimodal list)."""
    content = getattr(message, "content", None)
    if content is None and isinstance(message, dict):
        content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict):
                parts.append(part.get("text", "") or "")
            elif isinstance(part, str):
                parts.append(part)
        return " ".join(parts)
    return ""


def compute_already_loaded(
    loaded: Any, messages: Any, summarization_event: Any
) -> set[str]:
    """Skills whose SKILL.md body is still live in the effective message window.

    A skill's tools persist via ``loaded_skills`` state, so later turns can skip
    re-pasting the full SKILL.md body. Two caveats this handles:

    - **No compaction**: the full history is in the model's view, so every
      injected body still survives — trust ``loaded`` as-is.
    - **Compaction**: the body lives only in the message history, which compaction
      summarizes. Reuse ``get_effective_messages`` so this view can't drift from
      what the model actually sees, then drop the lossy summary at index 0 — a body
      that survives only inside the summary is gone verbatim and must be re-injected
      (its tools stay available via state regardless).

    The marker scan is identity-bound: a skill counts as live only when its marker,
    carrying ``mid`` equal to the scanned message's own framework id, is present in
    that message. Content alone can't forge a match (a user-typed marker, or a
    SKILL.md documenting the format, carries a ``mid`` that won't equal the id of the
    message it sits in), so a spurious "still loaded" can't suppress a real body.

    Non-string skill names are filtered out; any unexpected shape degrades to an
    empty set so the caller re-injects in full.
    """
    loaded_set = {name for name in (loaded or []) if isinstance(name, str)}
    if not loaded_set:
        return set()

    event = summarization_event
    if not isinstance(event, dict) or not event.get("cutoff_index"):
        return loaded_set

    from ptc_agent.agent.middleware.compaction import get_effective_messages

    effective = get_effective_messages(messages or [], event)[1:]
    live: set[str] = set()
    for m in effective:
        mid = _message_id(m)
        if not mid:
            continue
        text = _message_text(m)
        for name in loaded_set:
            if name not in live and loaded_skill_marker(name, mid) in text:
                live.add(name)
    return live
