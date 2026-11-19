"""Storing a skill archive as one of the user's skills: the pipeline an
upload and an install from the organization library share, so an install
meets every rule an upload does (validation, caps, triggers)."""

from __future__ import annotations

import asyncio

from fastapi import HTTPException

from src.server.database.user_skills import (
    MAX_SKILL_TOTAL_BYTES_PER_USER,
    MAX_SKILLS_PER_USER,
    SkillCapReached,
    SkillTriggerTaken,
    upsert_user_skill,
)
from src.server.services import skill_archive_storage
from src.server.services.user_skills.commands import ensure_free_of_platform, upload_seed
from src.server.services.user_skills.limits import MAX_SKILL_INLINE_BLOB_BYTES
from src.server.services.user_skills.materialize import drop_archive_if_unused
from src.server.services.user_skills.validate import (
    SkillNamesUnavailable,
    SkillValidationError,
    validate_skill_archive,
)


def refused(status: int, code: str, message: str, **params: object) -> HTTPException:
    """A refusal a client explains in its own words: ``code`` says which one,
    ``params`` fill its sentence, and ``message`` serves any client that does
    not know the code."""
    return HTTPException(
        status_code=status, detail={"code": code, "message": message, **params}
    )


def conflict(e: SkillTriggerTaken | SkillCapReached) -> HTTPException:
    if isinstance(e, SkillTriggerTaken):
        return refused(409, e.code, str(e), **e.params)
    if e.limit == "count":
        return refused(409, "skill_count_cap", str(e), max=MAX_SKILLS_PER_USER)
    return refused(
        409, "skill_storage_cap", str(e), max=MAX_SKILL_TOTAL_BYTES_PER_USER
    )



async def store_skill_archive(
    user_id: str,
    raw: bytes,
    *,
    workspace_id: str | None = None,
    org_library_item_id: str | None = None,
) -> dict:
    """Validate an archive's bytes and store them as the user's skill row,
    for an upload and for an install from the organization library, which
    comes in with the item it copies.

    Re-uploading an existing name replaces it in place (within its scope).
    The slow object PUT happens before the DB write; on DB failure the
    object is deleted; a replaced row's superseded object is deleted after
    commit (unless another same-content row still references it)."""
    try:
        # Unzip + re-zip + hash over as much as 8 MB: off the event loop.
        validated = await asyncio.to_thread(validate_skill_archive, raw, upload=True)
    except SkillValidationError as e:
        raise refused(400, e.code, str(e), **e.params) from e

    # The name is itself a live trigger (effective_trigger falls back to it),
    # so it has to clear the platform tier the same way an alias does — a
    # builtin the user renamed to this name would otherwise be shadowed by
    # the upload. Renaming onto an existing row is already blocked in
    # set_platform_alias; this is the same rule in the other order.
    try:
        await ensure_free_of_platform(user_id, validated.name)
    except SkillTriggerTaken as e:
        raise conflict(e) from e
    except SkillNamesUnavailable as e:
        # Not the upload's fault and not a name conflict: a directory skills
        # ship from would not list, so the reservation is short rather than
        # clear. Refusing here is also what keeps the row out of the database,
        # since everything that writes runs below this line.
        raise HTTPException(status_code=503, detail=str(e)) from e
    command_seed = await upload_seed(user_id, validated, workspace_id)

    archive_key: str | None = None
    archive_blob: bytes | None = None
    if skill_archive_storage.is_configured():
        try:
            archive_key = await skill_archive_storage.store_archive(
                user_id=user_id,
                content=validated.canonical_zip,
                content_hash=validated.content_hash,
            )
        # Base class, not just the upload subclass: a key the adapter refuses
        # to build is still a storage failure the caller should see as one,
        # and the reconciler's copy of this call already catches the base.
        except skill_archive_storage.SkillArchiveStorageError as exc:
            raise HTTPException(
                status_code=502,
                detail="Could not store the skill archive — please retry.",
            ) from exc
    else:
        if len(validated.canonical_zip) > MAX_SKILL_INLINE_BLOB_BYTES:
            raise refused(
                400,
                "upload_too_large",
                "Object storage is not configured on this deployment; "
                f"skill archives are limited to {MAX_SKILL_INLINE_BLOB_BYTES} "
                "bytes",
                size=len(validated.canonical_zip),
                max=MAX_SKILL_INLINE_BLOB_BYTES,
            )
        archive_blob = validated.canonical_zip

    try:
        row, superseded_key = await upsert_user_skill(
            user_id,
            validated.name,
            description=validated.description,
            license=validated.license,
            frontmatter=validated.frontmatter,
            allowed_tools=validated.allowed_tools,
            confirmed=True,
            content_hash=validated.content_hash,
            archive_key=archive_key,
            archive_blob=archive_blob,
            archive_bytes=len(validated.canonical_zip),
            file_count=validated.file_count,
            workspace_id=workspace_id,
            command=command_seed,
            org_library_item_id=org_library_item_id,
        )
    except (SkillTriggerTaken, SkillCapReached) as e:
        await drop_archive_if_unused(user_id, archive_key)
        raise conflict(e) from e
    except BaseException:
        await drop_archive_if_unused(user_id, archive_key)
        raise

    await drop_archive_if_unused(user_id, superseded_key)
    return row
