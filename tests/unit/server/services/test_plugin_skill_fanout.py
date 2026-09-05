"""The skill fan-out's report contract, now that it runs in two phases.

Deciding a plan and storing its archive happen concurrently; only the row
writes stay ordered. That split is invisible in the happy path and would be
invisible in a green E2E, because the things it can break are ordering and
cleanup: a report that no longer reads back in plan order, a rejection that
loses the diagnostic it owes, or an archive stored for a plan whose phase
partner blew up and which therefore never becomes a row.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import ExitStack, contextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.user_skills import SkillCapReached
from src.server.models.plugin import InstallReport
from src.server.services.plugins.skill_fanout import fan_out_skills
from src.server.services.plugins.skills import SkillPlan
from src.server.services.user_skills.validate import (
    SkillValidationError,
    ValidatedSkill,
)

USER = "test-user-123"
PLUGIN_ID = "22222222-2222-2222-2222-222222222222"

M = "src.server.services.plugins.skill_fanout."


def _validated(name: str) -> ValidatedSkill:
    return ValidatedSkill(
        name=name,
        description=f"{name} description",
        license=None,
        frontmatter={"name": name},
        allowed_tools=[],
        skill_md=f"---\nname: {name}\n---\n\nBody.\n",
        canonical_zip=b"PK-canonical",
        content_hash=f"hash-{name}",
        file_count=1,
    )


def _plan(directory: str) -> SkillPlan:
    # The bytes carry the directory so the validate stub can dispatch on its
    # input. A call-order counter cannot: phase one runs concurrently, which
    # is the whole thing under test, so the calls do not arrive in plan order.
    return SkillPlan(dir=directory, zip_bytes=directory.encode())


@contextmanager
def _env(
    *,
    validate,
    store=None,
    upsert=None,
    reserved: set[str] | None = None,
    account: list[dict] | None = None,
    held: int | None = None,
):
    """Patch every collaborator the fan-out reaches. Callers override the two
    that carry the behaviour under test and ignore the rest.

    ``held`` is the cap's own count, which spans scopes and so is not the
    length of ``account``; it defaults to agreeing with it."""
    store = store or AsyncMock(return_value=("key", None, None))
    upsert = upsert or AsyncMock(
        side_effect=lambda user_id, name, **kw: ({"name": name}, None)
    )
    patches = (
        patch(M + "list_user_skills", new=AsyncMock(return_value=account or [])),
        patch(
            M + "count_user_skills",
            new=AsyncMock(return_value=held if held is not None else len(account or [])),
        ),
        patch(M + "get_skill_command_overrides", new=AsyncMock(return_value={})),
        patch(M + "reserved_skill_names", return_value=reserved or set()),
        patch(M + "validate_skill_archive", side_effect=validate),
        patch(M + "ensure_free_of_platform", new=AsyncMock(return_value=None)),
        patch(M + "store_skill_archive", new=store),
        patch(M + "upsert_user_skill", new=upsert),
        patch(M + "drop_archive_if_unused", new=AsyncMock()),
    )
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


@pytest.mark.asyncio
async def test_the_report_reads_back_in_plan_order():
    """Phase one finishes out of order on purpose here. The report must not."""
    plans = [_plan(f"skill-{i}") for i in range(5)]
    delays = {"skill-0": 0.04, "skill-1": 0.0, "skill-2": 0.03,
              "skill-3": 0.0, "skill-4": 0.02}
    order_finished: list[str] = []

    def _validate(raw):
        name = raw.decode()
        # to_thread runs this off-loop; sleeping here really does let a later
        # plan reach the write queue first.
        time.sleep(delays[name])
        order_finished.append(name)
        return _validated(name)

    report = InstallReport()
    with _env(validate=_validate):
        await fan_out_skills(USER, PLUGIN_ID, plans, report)

    assert [r.name for r in report.components] == [p.dir for p in plans]
    assert all(r.status == "created" for r in report.components)
    assert report.skills_created == 5
    # The premise: without it this test would pass on a serial implementation.
    assert order_finished != [p.dir for p in plans]


@pytest.mark.asyncio
async def test_each_rejection_keeps_its_component_and_diagnostic():
    """One plan per rejection shape, mixed with a good one, all in one run."""
    plans = [
        _plan("good"),
        _plan("bad-zip"),
        _plan("mismatched"),
        _plan("reserved"),
        _plan("taken"),
        SkillPlan(dir="pre-skipped", skip_code="policy",
                  skip_reason="dropped by the package"),
    ]

    def _validate(raw):
        name = raw.decode()
        if name == "bad-zip":
            raise SkillValidationError("SKILL.md is missing")
        if name == "mismatched":
            return _validated("declares-something-else")
        return _validated(name)

    report = InstallReport()
    with _env(
        validate=_validate,
        reserved={"reserved"},
        account=[{"name": "taken"}],
    ):
        await fan_out_skills(USER, PLUGIN_ID, plans, report)

    by_dir = dict(zip([p.dir for p in plans], report.components, strict=True))
    assert by_dir["good"].status == "created"
    assert by_dir["bad-zip"].status == "invalid"
    assert by_dir["mismatched"].status == "skipped"
    assert by_dir["reserved"].status == "skipped"
    assert by_dir["taken"].status == "exists"
    assert by_dir["pre-skipped"].status == "skipped"

    codes = {d.code for d in report.diagnostics}
    assert {"invalid_skill", "name_mismatch", "reserved_name"} <= codes
    # A plan the package already explained does not owe a second finding, and
    # `exists` is a status rather than a defect.
    assert not any(d.target == "pre-skipped" for d in report.diagnostics)
    assert next(d for d in report.diagnostics if d.code == "invalid_skill").level == "error"


@pytest.mark.asyncio
async def test_a_cap_the_write_raises_is_reported_as_a_cap():
    """The storage cap has no pre-check, so it arrives here on the ordinary
    path — a plugin whose skills are simply too large. Sharing an ``except
    ValueError`` with the trigger check told those users they had a naming
    conflict, and the remedy for a naming conflict is nothing like the one for
    a full account."""
    plans = [_plan("fits"), _plan("too-big"), _plan("contested")]

    def _upsert(user_id, name, **kw):
        if name == "too-big":
            raise SkillCapReached(
                "Skill storage limit reached (…). Delete one.", limit="bytes"
            )
        if name == "contested":
            raise ValueError("command 'contested' is taken")
        return {"name": name}, None

    report = InstallReport()
    with _env(
        validate=lambda raw: _validated(raw.decode()),
        upsert=AsyncMock(side_effect=_upsert),
    ):
        await fan_out_skills(USER, PLUGIN_ID, plans, report)

    by_dir = dict(zip([p.dir for p in plans], report.components, strict=True))
    assert by_dir["fits"].status == "created"
    assert by_dir["too-big"].status == "error"
    assert by_dir["contested"].status == "skipped"

    by_target = {d.target: d for d in report.diagnostics}
    assert by_target["too-big"].code == "cap_reached"
    # The cap's own sentence, not the count cap's: they name different limits.
    assert "storage" in by_target["too-big"].message
    assert by_target["contested"].code == "trigger_conflict"


@pytest.mark.asyncio
async def test_the_budget_counts_what_the_cap_counts():
    """Workspace-scoped skills count against the cap but are not in this
    scope's listing. Budgeting against that listing meant an account of six
    user-tier and forty-four workspace skills read as six, so the install
    validated and stored every archive before the write refused each one by a
    limit it had been told it was nowhere near."""
    from src.server.database.user_skills import MAX_SKILLS_PER_USER

    plans = [_plan("a"), _plan("b")]
    report = InstallReport()
    with _env(
        validate=lambda raw: _validated(raw.decode()),
        account=[{"name": "only-one-here"}],
        held=MAX_SKILLS_PER_USER,
    ):
        await fan_out_skills(USER, PLUGIN_ID, plans, report)

    assert report.skills_created == 0
    assert {d.code for d in report.diagnostics} == {"cap_reached"}
    # Refused before the prepare phase, so nothing was stored to be cleaned up.
    assert all(c.status == "error" for c in report.components)


@pytest.mark.asyncio
async def test_an_unexpected_phase_failure_strands_no_archive():
    """The stores run concurrently, so by the time one plan raises, others have
    already written objects that no row will ever reference."""
    plans = [_plan("stores-fine"), _plan("explodes")]

    def _validate(raw):
        return _validated(raw.decode())

    async def _store(user_id, zip_bytes, content_hash):
        if content_hash == "hash-explodes":
            raise RuntimeError("object storage fell over")
        return "key-stores-fine", None, None

    dropped = AsyncMock()
    report = InstallReport()
    with _env(validate=_validate, store=AsyncMock(side_effect=_store)):
        # Last patch wins, so this one shadows the env's own stub.
        with patch(M + "drop_archive_if_unused", new=dropped):
            with pytest.raises(RuntimeError, match="object storage fell over"):
                await fan_out_skills(USER, PLUGIN_ID, plans, report)

    assert "key-stores-fine" in [call.args[1] for call in dropped.await_args_list]


@pytest.mark.asyncio
async def test_the_decide_and_store_phase_actually_overlaps():
    """The point of the split. Serialized, this is 5 x the unit delay."""
    plans = [_plan(f"skill-{i}") for i in range(5)]
    inflight, peak = 0, 0

    def _validate(raw):
        return _validated(raw.decode())

    async def _store(user_id, zip_bytes, content_hash):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.02)
        inflight -= 1
        return "key", None, None

    report = InstallReport()
    with _env(validate=_validate, store=AsyncMock(side_effect=_store)):
        await fan_out_skills(USER, PLUGIN_ID, plans, report)

    assert peak > 1, "archive stores ran one at a time"
    assert report.skills_created == 5
