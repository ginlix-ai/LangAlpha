"""Skill component collection: ``skills/<dir>/`` → per-skill upload zips.

Each immediate child directory of ``skills/`` that contains a SKILL.md is
repackaged as a standalone zip and pushed through the same
``validate_skill_archive`` pipeline as a direct upload — host and plugin
installs can never disagree on what a valid skill is. A bad skill drops that
skill only (spec §11); the directory name must match the skill's declared
name so update diffs and export stay exact.
"""

import io
import zipfile
from dataclasses import dataclass, field
from typing import ClassVar

from src.server.models.plugin import Diagnostic
from src.server.services.plugins.manifest import SPEC_URL
from src.server.services.user_skills.limits import MAX_SKILL_ARCHIVE_BYTES


@dataclass
class SkillPlan:
    """One skills/ child directory, ready for validation or already skipped."""

    # How this plan reports itself (ComponentResult.of). A skill's name only
    # exists once SKILL.md validates, and nothing coerces it, so a result
    # carries the validated name explicitly and never a rename.
    kind: ClassVar[str] = "skill"
    name: ClassVar[str] = ""
    renamed: ClassVar[bool] = False

    dir: str
    zip_bytes: bytes | None = None
    skip_code: str | None = None
    skip_reason: str | None = None
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.dir


def _skill_diag(target: str, code: str, message: str) -> Diagnostic:
    return Diagnostic(
        scope="skill", target=target, code=code, message=message,
        spec_ref=SPEC_URL,
    )


def _skip(plan: SkillPlan, code: str, reason: str) -> SkillPlan:
    plan.skip_code = code
    plan.skip_reason = reason
    plan.diagnostics.append(_skill_diag(plan.dir, code, reason))
    return plan


def collect_skills(
    files: dict[str, bytes],
) -> tuple[list[SkillPlan], list[Diagnostic]]:
    """Group the ``skills/`` subtree into per-directory plans.

    Loose files directly under ``skills/`` are reported and dropped; a
    directory without SKILL.md is skipped as a skill but reported.
    """
    diagnostics: list[Diagnostic] = []
    by_dir: dict[str, dict[str, bytes]] = {}
    for path, content in files.items():
        if not path.startswith("skills/"):
            continue
        rest = path[len("skills/"):]
        if "/" not in rest:
            diagnostics.append(
                _skill_diag(
                    path, "loose_file",
                    "files directly under skills/ are not part of any skill",
                )
            )
            continue
        child, relative = rest.split("/", 1)
        by_dir.setdefault(child, {})[relative] = content

    plans: list[SkillPlan] = []
    for child in sorted(by_dir):
        members = by_dir[child]
        plan = SkillPlan(dir=child)
        if "SKILL.md" not in members:
            plans.append(
                _skip(plan, "missing_skill_md", "no SKILL.md at the skill root")
            )
            continue
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for relative in sorted(members):
                zf.writestr(relative, members[relative])
        raw = buf.getvalue()
        if len(raw) > MAX_SKILL_ARCHIVE_BYTES:
            plans.append(
                _skip(
                    plan, "too_large",
                    f"skill exceeds the archive size limit "
                    f"({MAX_SKILL_ARCHIVE_BYTES // (1024 * 1024)} MiB)",
                )
            )
            continue
        plan.zip_bytes = raw
        plans.append(plan)
    return plans, diagnostics
