"""Fan a package's ``skills/`` directories into user_skills rows.

Each plan goes through the same ``validate_skill_archive`` pipeline as a
direct upload, so a plugin can never install a skill the upload path would
reject. Per-skill isolation throughout: a name collision, a contested
trigger, or a failed archive store drops that skill only and is reported.

Two phases, because the work divides cleanly by whether order matters.
Deciding a plan's fate and storing its archive are independent per skill and
are the expensive half (a thread to unpack, a storage round trip to write), so
they run concurrently. Creating the rows is not independent: the writes
serialize on the same per-user advisory lock anyway, and each skill's slash
command is seeded against the ones already created, so that half stays a loop
in plan order, which is also the order the report reads back in.
"""

import asyncio
import logging
from dataclasses import dataclass

from src.server.database.user_skills import (
    SkillNameTaken,
    list_user_skills,
    upsert_user_skill,
)
from src.server.models.plugin import ComponentResult, Diagnostic, InstallReport
from src.server.services import skill_archive_storage
from src.server.services.features import get_skill_command_overrides
from src.server.services.plugins.skills import SkillPlan
from src.server.services.user_skills.commands import (
    ensure_free_of_platform,
    free_seed,
)
from src.server.services.user_skills.limits import (
    MAX_CONCURRENT_ARCHIVE_OPS,
    MAX_SKILL_INLINE_BLOB_BYTES,
)
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.user_skills.validate import (
    SkillValidationError,
    ValidatedSkill,
    reserved_skill_names,
    validate_skill_archive,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _Prepared:
    """A plan that passed every check, with its archive already stored."""

    validated: ValidatedSkill
    archive_key: str | None
    archive_blob: bytes | None


@dataclass(frozen=True, slots=True)
class _Rejected:
    """A plan that will not become a row, carrying the report it owes.

    ``code`` empty means the component result is the whole report — a plan the
    package itself already marked skipped has said why once and does not owe a
    second finding.
    """

    status: str
    reason: str
    name: str = ""
    code: str = ""
    level: str = "warning"


async def store_skill_archive(
    user_id: str, canonical_zip: bytes, content_hash: str
) -> tuple[str | None, bytes | None, str | None]:
    """(archive_key, archive_blob, error) — mirrors the upload path's split."""
    if skill_archive_storage.is_configured():
        try:
            key = await skill_archive_storage.store_archive(
                user_id=user_id, content=canonical_zip, content_hash=content_hash
            )
        except skill_archive_storage.SkillArchiveStorageError:
            logger.warning(
                "[plugins] skill archive store failed for %s", user_id,
                exc_info=True,
            )
            return None, None, "could not store the skill archive"
        return key, None, None
    if len(canonical_zip) > MAX_SKILL_INLINE_BLOB_BYTES:
        return None, None, (
            "object storage is not configured on this deployment; skill "
            f"archives are limited to {MAX_SKILL_INLINE_BLOB_BYTES} bytes"
        )
    return None, canonical_zip, None


_EXISTS_REASON = "a skill with this name already exists; left untouched"


async def _prepare(
    user_id: str,
    plan: SkillPlan,
    *,
    account_names: set[str],
    reserved: set[str],
    overrides: dict[str, str],
    gate: asyncio.Semaphore,
) -> _Prepared | _Rejected:
    """Decide one plan and store its archive. No shared state is written.

    ``account_names`` is the set as it stood before the fan-out, and stays
    accurate for the whole phase: a plan's name must equal its directory, and
    a zip cannot hold the same directory twice, so no plan here can take a
    name another one in this package wants. A name taken by something *else*
    mid-run is caught by the ``overwrite=False`` write instead, which reports
    the same ``exists`` this check does.
    """
    if plan.skip_code is not None:
        return _Rejected("skipped", plan.skip_reason or "")
    try:
        async with gate:
            validated = await asyncio.to_thread(
                validate_skill_archive, plan.zip_bytes
            )
    except SkillValidationError as e:
        return _Rejected("invalid", str(e), code="invalid_skill", level="error")
    if validated.name != plan.dir:
        return _Rejected(
            "skipped",
            f"SKILL.md declares name {validated.name!r} but the directory "
            f"is {plan.dir!r}; they must match",
            name=validated.name,
            code="name_mismatch",
        )
    if validated.name in reserved:
        return _Rejected(
            "skipped",
            "collides with a built-in skill or command",
            name=validated.name,
            code="reserved_name",
        )
    if validated.name in account_names:
        return _Rejected("exists", _EXISTS_REASON, name=validated.name)
    # The name is itself a live trigger; per-skill skip on conflict — a
    # plugin install must never 409 whole over one contested trigger.
    try:
        await ensure_free_of_platform(user_id, validated.name, overrides=overrides)
    except ValueError as e:
        return _Rejected(
            "skipped", str(e), name=validated.name, code="trigger_conflict"
        )

    async with gate:
        archive_key, archive_blob, error = await store_skill_archive(
            user_id, validated.canonical_zip, validated.content_hash
        )
    if error is not None:
        return _Rejected("error", error, name=validated.name)
    return _Prepared(validated, archive_key, archive_blob)


async def fan_out_skills(
    user_id: str,
    plugin_id: str,
    plans: list[SkillPlan],
    report: InstallReport,
) -> None:
    """Create a skill row per valid plan, reporting every plan."""
    if not plans:
        return
    account_rows = await list_user_skills(user_id)
    account_names = {r["name"] for r in account_rows}
    overrides = await get_skill_command_overrides(user_id)
    reserved = reserved_skill_names()

    def _skip(plan: SkillPlan, code: str, reason: str, *, name: str = "") -> None:
        report.components.append(
            ComponentResult.of(plan, "skipped", name=name, reason=reason)
        )
        report.diagnostics.append(
            Diagnostic(scope="skill", target=plan.dir, code=code, message=reason)
        )

    gate = asyncio.Semaphore(MAX_CONCURRENT_ARCHIVE_OPS)
    outcomes = await asyncio.gather(
        *(
            _prepare(
                user_id, plan,
                account_names=account_names, reserved=reserved,
                overrides=overrides, gate=gate,
            )
            for plan in plans
        ),
        return_exceptions=True,
    )
    # An unexpected failure anywhere in the phase strands every archive the
    # other plans already stored, since no row will ever reference them.
    # Collecting the exceptions rather than letting the first one propagate is
    # what makes that cleanup reachable.
    failure = next((o for o in outcomes if isinstance(o, BaseException)), None)
    if failure is not None:
        for outcome in outcomes:
            if isinstance(outcome, _Prepared):
                await drop_archive_if_unused(user_id, outcome.archive_key)
        raise failure

    for plan, outcome in zip(plans, outcomes, strict=True):
        report.diagnostics.extend(plan.diagnostics)
        if isinstance(outcome, _Rejected):
            report.components.append(
                ComponentResult.of(
                    plan, outcome.status, name=outcome.name,
                    reason=outcome.reason,
                )
            )
            if outcome.code:
                report.diagnostics.append(
                    Diagnostic(
                        level=outcome.level, scope="skill", target=plan.dir,
                        code=outcome.code, message=outcome.reason,
                    )
                )
            continue

        validated = outcome.validated
        command_seed = free_seed(validated, account_rows, overrides)
        try:
            row, superseded = await upsert_user_skill(
                user_id,
                validated.name,
                description=validated.description,
                license=validated.license,
                frontmatter=validated.frontmatter,
                allowed_tools=validated.allowed_tools,
                confirmed=True,
                content_hash=validated.content_hash,
                archive_key=outcome.archive_key,
                archive_blob=outcome.archive_blob,
                archive_bytes=len(validated.canonical_zip),
                file_count=validated.file_count,
                workspace_id=None,
                plugin_id=plugin_id,
                plugin_skill_dir=plan.dir,
                command=command_seed,
                overwrite=False,
            )
        except SkillNameTaken:
            # The name was taken between the check above and this write. Same
            # outcome the check produces, so it reads the same in the report.
            await drop_archive_if_unused(user_id, outcome.archive_key)
            report.components.append(
                ComponentResult.of(
                    plan, "exists", name=validated.name, reason=_EXISTS_REASON
                )
            )
            continue
        except ValueError as e:
            # Cap or a raced trigger conflict — either way this skill only.
            await drop_archive_if_unused(user_id, outcome.archive_key)
            _skip(plan, "trigger_conflict", str(e), name=validated.name)
            continue
        except BaseException:
            await drop_archive_if_unused(user_id, outcome.archive_key)
            raise
        await drop_archive_if_unused(user_id, superseded)
        account_names.add(validated.name)
        account_rows.append(row)
        report.skills_created += 1
        report.components.append(
            ComponentResult.of(plan, "created", name=validated.name)
        )
