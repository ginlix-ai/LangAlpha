"""A tree of skills with no plugin.json anywhere, wrapped into a package.

Most skill collections are published without a plugin manifest: one SKILL.md
at the repository root, a ``skills/<name>/`` folder per skill, or skills
grouped by topic (``finance/<name>/SKILL.md``). Discovery finds no plugin in
any of them, and refusing the install over a manifest the format does not
need was refusing the commonest thing a user pastes. So when nothing in the
tree is a plugin, each folder holding a SKILL.md is read as a skill, laid out
under ``skills/<name>/`` beside a synthesized plugin.json, and the validator,
the fan-out and update run on that exactly as they run on a package written
that way. An ``agents/`` folder at the top comes along as it is: Claude Code
reads one there without a manifest too.
"""

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ptc_agent.agent.middleware.skills.discovery import validate_skill_name
from src.server.models.plugin import Diagnostic
from src.server.services.os_litter import is_os_litter
from src.server.services.plugins.agents import AGENTS_DIR, is_agent_file
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.schemas import PLUGIN_SCHEMA
from src.server.services.user_skills.validate import peek_frontmatter

# Same horizon as plugin roots: real collections nest two or three levels.
MAX_SKILL_DEPTH = 6
# A runaway guard sized well above the largest real collection (~75 skills);
# past it the user almost certainly linked a whole monorepo, and one folder of
# it is what they meant.
MAX_WRAPPED_SKILLS = 100

# Not part of any skill: dependency trees and bytecode caches. Hidden entries
# (``.git``, ``.github``, editor config) are repository plumbing and are
# skipped the same way, which also keeps them out of a root-level skill's
# 64-file budget.
_JUNK_DIRS = frozenset({"node_modules", "__pycache__"})
# Folders whose SKILL.md files are not the collection's own skills: fixtures
# for testing skills, and demos that vendor copies of them (a demo project
# under ``examples/`` repeating every skill it uses is a real layout). A link
# that points into one of these explicitly still reads it, because the walk
# starts below the linked folder.
_NOT_SKILL_HOMES = frozenset({"test", "tests", "__tests__", "fixtures", "examples"})

_PLUGIN_NAME_JUNK_RE = re.compile(r"[^a-z0-9.-]+")
_PLUGIN_NAME_MAX = 64
_FALLBACK_NAME = "skills"


def _junk(segments: Sequence[str]) -> bool:
    return is_os_litter(segments) or any(
        s.startswith(".") or s in _JUNK_DIRS for s in segments
    )


def plugin_name_from_hint(hint: str | None) -> str:
    """The closest legal plugin name to ``hint``, or ``"skills"``.

    plugin.json's name rule: lowercase letters, digits, dots and hyphens,
    alphanumeric at both ends, no ``--`` or ``..``.
    """
    name = _PLUGIN_NAME_JUNK_RE.sub("-", (hint or "").lower())
    name = re.sub(r"\.{2,}", ".", re.sub(r"-{2,}", "-", name))
    name = name[:_PLUGIN_NAME_MAX].strip(".-")
    return name or _FALLBACK_NAME


@dataclass(frozen=True, slots=True)
class _Skill:
    directory: tuple[str, ...]
    name: str
    description: str
    # The name is what SKILL.md declares, not a stand-in for one it got wrong.
    declared: bool


def _skill_dirs(files: dict[str, bytes]) -> list[tuple[str, ...]]:
    """Every folder holding a SKILL.md, shallowest first, none inside another.

    A SKILL.md below a skill folder belongs to that skill (a bundled example,
    a nested reference) rather than starting a second one. Shallow first also
    decides duplicates: when two folders claim one name, the one nearer the
    top is the collection's and the deeper one is the copy.
    """
    found: set[tuple[str, ...]] = set()
    for path in files:
        segments = tuple(path.split("/"))
        if segments[-1] != "SKILL.md":
            continue
        directory = segments[:-1]
        if len(directory) > MAX_SKILL_DEPTH:
            continue
        if _junk(directory) or any(s in _NOT_SKILL_HOMES for s in directory):
            continue
        found.add(directory)
    kept: list[tuple[str, ...]] = []
    kept_set: set[tuple[str, ...]] = set()
    for directory in sorted(found, key=lambda d: (len(d), d)):
        if any(directory[:i] in kept_set for i in range(len(directory))):
            continue
        if len(kept) >= MAX_WRAPPED_SKILLS:
            message = (
                f"this package has no plugin.json and more than "
                f"{MAX_WRAPPED_SKILLS} skill folders; link to the folder "
                "holding the skills you want instead (a tree URL, or a subdir)"
            )
            raise PluginFatal(
                message,
                diagnostics=[
                    Diagnostic(
                        level="error",
                        scope="plugin",
                        code="too_many_skills",
                        message=message,
                    )
                ],
            )
        kept.append(directory)
        kept_set.add(directory)
    return kept


def _top_agents(
    files: dict[str, bytes], directories: Sequence[tuple[str, ...]]
) -> dict[str, bytes]:
    """The files under a top-level ``agents/`` that no skill folder holds.

    Loose ones come along too, so the agents reader reports them rather than
    their going unmentioned.
    """
    out: dict[str, bytes] = {}
    for path, content in files.items():
        segments = tuple(path.split("/"))
        if not path.startswith(AGENTS_DIR) or _junk(segments):
            continue
        # A SKILL.md at the top makes the whole tree one skill, its agents/ too.
        if any(segments[:depth] in directories for depth in range(len(segments))):
            continue
        out[path] = content
    return out


def _describe(
    files: dict[str, bytes], directory: tuple[str, ...], fallback: str
) -> _Skill:
    """Name a skill the way the fan-out will: by its declared name.

    The fan-out requires the folder to be named what SKILL.md declares, so a
    valid declared name is the folder it is laid out under. Anything else
    keeps the folder's own name and fails there with the specific reason,
    which is per-skill isolation rather than a judgment made here.
    """
    front: dict[str, Any] = peek_frontmatter(
        files["/".join((*directory, "SKILL.md"))]
    )
    declared = str(front.get("name") or "").strip()
    description = str(front.get("description") or "").strip()
    if declared and validate_skill_name(declared, declared)[0]:
        return _Skill(directory, declared, description, True)
    return _Skill(
        directory, directory[-1] if directory else fallback, description, False
    )


def wrap_skill_tree(
    files: dict[str, bytes], *, name_hint: str | None
) -> tuple[dict[str, bytes], list[Diagnostic]] | None:
    """Lay a manifest-less tree out as a canonical package, or None when it
    holds no skill or subagent either.

    The plugin is named after its one skill when that is all it holds and the
    skill has a valid name, which is what the user installed; otherwise after
    ``name_hint`` (the folder or repository the package came from).
    """
    directories = _skill_dirs(files)
    agents = _top_agents(files, directories)
    agent_files = sum(map(is_agent_file, agents))
    if not agent_files:
        agents = {}
    if not directories and not agents:
        return None
    hinted = plugin_name_from_hint(name_hint)
    diagnostics: list[Diagnostic] = []
    if () in directories and any(map(is_agent_file, files)):
        # The files still install, inside the skill, so the subagents they
        # define would otherwise go missing without a word.
        diagnostics.append(
            Diagnostic(
                scope="plugin",
                code="agents_in_skill",
                message=(
                    "a SKILL.md at the top makes this package one skill, so its "
                    "agents/ folder stays inside that skill and none of it installs "
                    "as a subagent"
                ),
            )
        )
    skills: dict[str, _Skill] = {}
    owner: dict[tuple[str, ...], str] = {}
    for directory in directories:
        skill = _describe(files, directory, hinted)
        first = skills.get(skill.name)
        if first is not None:
            diagnostics.append(
                Diagnostic(
                    scope="skill",
                    target="/".join(directory),
                    code="duplicate_skill",
                    message=(
                        f"skill {skill.name!r} is also at "
                        f"{'/'.join(first.directory)}; only that one was "
                        "installed"
                    ),
                )
            )
            continue
        skills[skill.name] = skill
        owner[directory] = skill.name

    out: dict[str, bytes] = {}
    for path, content in files.items():
        segments = path.split("/")
        # At most one ancestor is a kept skill folder: nested ones were
        # dropped by the walk.
        for depth in range(min(len(segments) - 1, MAX_SKILL_DEPTH) + 1):
            name = owner.get(tuple(segments[:depth]))
            if name is None:
                continue
            rest = segments[depth:]
            if not _junk(rest):
                out[f"skills/{name}/{'/'.join(rest)}"] = content
            break

    out.update(agents)

    source = (name_hint or "").strip() or hinted
    only = next(iter(skills.values())) if len(skills) == 1 and not agents else None
    if only is not None and only.declared:
        name, description = only.name, only.description
    else:
        name, description = hinted, ""
    held = " and ".join(
        kind for kind, present in (("skills", skills), ("subagents", agents)) if present
    )
    manifest = {
        "$schema": PLUGIN_SCHEMA["$id"],
        "name": name,
        "version": "0.0.0",
        "description": description or f"{held.capitalize()} from {source}",
    }
    out["plugin.json"] = json.dumps(manifest, indent=2).encode("utf-8")
    counts = [
        f"{n} {kind}{'' if n == 1 else 's'}"
        for n, kind in ((len(skills), "skill"), (agent_files, "subagent"))
        if n
    ]
    count = " and ".join(counts)
    # How the package was read, not something wrong with it: a client says it
    # beside what landed instead of listing it among the warnings.
    diagnostics.insert(
        0,
        Diagnostic(
            level="info",
            scope="plugin",
            code="synthesized_manifest",
            message=(
                f"this package has no plugin.json, so it was read as a folder "
                f"of {held}: {count} wrapped into a plugin named {name!r}"
            ),
        ),
    )
    return out, diagnostics
