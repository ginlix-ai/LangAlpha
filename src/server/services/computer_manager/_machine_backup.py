"""Seam: mirroring the projects on one machine, all of them or only the changed.

One runtime serves every project on a computer, so a turn can write into any
of them and a teardown ends all of them. Both paths go through one loop,
fenced by the machine's durable sandbox ref.

The routine path after a turn syncs only the projects that changed.
Mirroring the turn's own project alone misses a sibling's edit or deletion
until that sibling's next sync, and a sandbox lost in between restores what
the user deleted. Syncing every project after every turn costs more the more
projects a machine holds (a no-change pass over four projects of 100k entries
measured 5.7 s, most of it shipping manifests both ways). A single exec that
compares change times against each project's scan mark finds the changed
projects instead (0.63 s for the same 100k entries), and the rest are
skipped. A pass before a teardown still mirrors every project: a shortcut is
not worth taking on the last chance to save a file.

One file of the ComputerManager split; see the package __init__."""

from __future__ import annotations

import logging
import time
from typing import Optional

from ptc_agent.core.paths import WorkspaceLayout
from ptc_agent.core.session import Session

from src.server.database.workspace import get_live_workspace_ids_for_computer
from src.server.database.workspace_file import get_scan_marks_for_computer
from src.server.models.computer import ComputerStatus
from src.server.services.persistence.sync_result import BackupIncomplete, UnsavedFile
from src.server.services.persistence.transfer import (
    ScanMark,
    ScanRules,
    SweepResult,
    SweepTarget,
    scan_cap_bytes,
    sweep_projects,
)
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable
from src.utils.storage import is_storage_enabled

from src.server.services.computer_manager._types import ComputerBinding

logger = logging.getLogger(__name__)


class MachineBackupMixin:
    async def _backup_machine_files_to_db(
        self,
        computer_id: str,
        *,
        workspace_id: Optional[str] = None,
        expected_sandbox_id: Optional[str] = None,
        strict: bool = False,
        session: Optional[Session] = None,
        routine: bool = False,
    ) -> int:
        """Mirror every project on the machine, ``workspace_id`` first.

        Returns how many projects were actually mirrored, not how many were
        asked."""
        ordered: list[str] = [workspace_id] if workspace_id else []
        failures: list[str] = []
        try:
            siblings = await get_live_workspace_ids_for_computer(computer_id)
        except Exception as e:
            # Losing the sibling list means the unmirrored set is unknown, so a
            # strict caller must not read "no failures" as "everything is saved".
            logger.warning(
                f"Could not list the projects on computer {computer_id}: {e}; "
                f"backing up {workspace_id} alone"
            )
            siblings = []
            failures.append(f"sibling list unavailable: {type(e).__name__}: {e}")
        ordered += [ws for ws in siblings if ws != workspace_id]
        return await self._mirror_projects(
            computer_id,
            ordered,
            expected_sandbox_id=expected_sandbox_id,
            strict=strict,
            session=session,
            failures=failures,
            routine=routine,
        )

    async def _mirror_projects(
        self,
        computer_id: str,
        ordered: list[str],
        *,
        expected_sandbox_id: Optional[str],
        strict: bool = False,
        session: Optional[Session] = None,
        failures: Optional[list[str]] = None,
        layouts: Optional[dict[str, WorkspaceLayout]] = None,
        routine: bool = False,
    ) -> int:
        """Mirror ``ordered`` in turn; returns how many were actually mirrored.

        Each project is fenced by the machine's durable ref rather than by its own
        shadow column: the session belongs to the computer, so a project row that
        lags behind it must not be able to skip its own mirror. One project's
        failure never skips its siblings, and the caller's contract is unchanged
        per project - best effort logs, strict refuses the teardown.
        ``failures`` carries what already went wrong before the loop."""
        failures = list(failures or ())
        layouts = layouts or {}
        mirrored = 0
        unsaved: list[UnsavedFile] = []
        unnamed_failure = bool(failures)
        for ws_id in ordered:
            try:
                if await self.backup_project_files(
                    ws_id,
                    computer_id=computer_id,
                    strict=strict,
                    expected_sandbox_id=expected_sandbox_id,
                    session=session,
                    layout=layouts.get(ws_id),
                    routine=routine,
                ):
                    mirrored += 1
            except Exception as e:
                logger.error(
                    f"File backup failed for {ws_id} on computer {computer_id}: "
                    f"{type(e).__name__}: {e}"
                )
                failures.append(f"{ws_id}: {type(e).__name__}: {e}")
                named = e.unsaved if isinstance(e, BackupIncomplete) else []
                unsaved.extend(named)
                # A project that failed outright has no file list, and naming
                # only the others' files would read as the whole problem.
                unnamed_failure = unnamed_failure or not named

        if strict and failures:
            raise BackupIncomplete(
                f"File backup left {len(failures)} of {len(ordered)} project(s) "
                f"on computer {computer_id} unmirrored: " + "; ".join(failures),
                [] if unnamed_failure else unsaved,
            )
        if mirrored < len(ordered):
            logger.warning(
                f"File backup mirrored {mirrored} of {len(ordered)} project(s) "
                f"on computer {computer_id}; the rest keep their last mirror"
            )
        return mirrored

    async def backup_changed_projects(
        self, workspace_id: str, *, session: Optional[Session] = None
    ) -> None:
        """After a turn in ``workspace_id``, mirror every project on its machine that changed.

        The turn's project is swept first because it is the likeliest to have
        changed, and the walk of a changed project stops at its first newer
        entry. Without a sandbox to sweep, or if the sweep fails, only the
        turn's project is mirrored, which is what a turn did before.
        """
        await self._backup_changes(await self.resolve_binding(workspace_id), session)

    async def backup_computer_changes(self, computer_id: str) -> bool:
        """After calls from another computer's turn ran on ``computer_id``, or
        the commands they left there ended, mirror every project there that
        changed. No turn of that computer's own sweeps what they wrote, and its
        stop backup may be far off. Only through a live session this worker
        holds: attaching runs the machine's start path, which may rebuild it at
        another tier, or build a fresh one, from the backup this would have
        brought up to date. False when this worker holds none, so the mirror
        is owed to the worker that does (``commands.mirror``). Raises
        ``BackupIncomplete`` when a project there was left unmirrored, since
        no turn there will sweep it again."""
        from src.server.database.computer import get_computer

        computer = await get_computer(computer_id)
        if computer is None or computer.get("status") != ComputerStatus.RUNNING:
            # Gone, or stopped and so mirrored at its stop.
            return True
        binding = self._binding_from_computer("", computer)
        if not binding.files_backup:
            return True
        session = self.cached_session_for_computer(computer)
        if session is None:
            return False
        if not await self._backup_changes(binding, session):
            raise BackupIncomplete(
                f"computer {computer_id}: a changed project was left unmirrored"
            )
        return True

    async def _backup_changes(
        self, binding: ComputerBinding, session: Optional[Session]
    ) -> bool:
        """Whether every project it set out to mirror was mirrored."""
        started = time.monotonic()
        if not binding.files_backup:
            # Its owner chose so; the last backup stays as it was.
            return True
        session = session or self._cached_session(binding.computer_id)
        ordered = [binding.workspace_id] if binding.workspace_id else []
        layouts, sweep = {}, None
        if getattr(session, "sandbox", None):
            try:
                ordered, layouts, sweep = await self._changed_projects(binding, session)
            except Exception as e:
                logger.warning(
                    f"[backup-sweep] computer {binding.computer_id}: sweep failed "
                    f"({type(e).__name__}: {e}); mirroring "
                    + ("the turn's project only" if ordered else "every project")
                )
                if not ordered:
                    # A computer another's turn reached holds no project of
                    # that turn's: every project there, as its stop mirrors.
                    ordered = await get_live_workspace_ids_for_computer(binding.computer_id)
        swept_at = time.monotonic()
        mirrored = await self._mirror_projects(
            binding.computer_id,
            ordered,
            expected_sandbox_id=binding.provider_ref,
            session=session,
            layouts=layouts,
            routine=True,
        )
        if sweep is not None:
            logger.info(
                f"[backup-sweep] computer {binding.computer_id}: "
                f"changed={len(sweep.changed)} unchanged={len(sweep.unchanged)} "
                f"missing={len(sweep.missing)} "
                f"unswept={len(ordered) - len(sweep.changed)}; "
                f"visited {sweep.visited} entries, walk {sweep.walk_ms} ms, "
                f"plan+sweep {int((swept_at - started) * 1000)} ms, "
                f"mirrored {mirrored}/{len(ordered)} in "
                f"{int((time.monotonic() - swept_at) * 1000)} ms"
            )
        return mirrored == len(ordered)

    async def _changed_projects(
        self, binding: ComputerBinding, session: Session
    ) -> tuple[list[str], dict[str, WorkspaceLayout], SweepResult]:
        """The projects to mirror, in order, with the folders already resolved.

        A project whose folder cannot be resolved is mirrored without a sweep:
        the sync decides what a missing folder means."""
        rows = await get_scan_marks_for_computer(binding.computer_id)
        rows.sort(key=lambda r: r["workspace_id"] != binding.workspace_id)
        sandbox_id = self._session_sandbox_id(session)
        # The cap a sync of any of these projects would use now: it depends on
        # the deployment and the sandbox, not the project, so one answer
        # serves every mark on the machine.
        rules = ScanRules.of(
            scan_cap_bytes(session.sandbox, blobs_on=is_storage_enabled())
        )
        root = binding.root_dir or self.config.filesystem.working_directory

        unswept: list[str] = []
        layouts: dict[str, WorkspaceLayout] = {}
        targets: list[SweepTarget] = []
        for row in rows:
            ws_id = row["workspace_id"]
            try:
                layouts[ws_id] = await self._project_layout(
                    ws_id, binding.computer_id, dir_name=row["dir_name"], root=root
                )
            except WorkspaceLayoutUnavailable:
                unswept.append(ws_id)
                continue
            targets.append(
                SweepTarget(
                    ws_id,
                    layouts[ws_id].workspace,
                    ScanMark.trusted(row["files_scan_mark"], sandbox_id, rules),
                )
            )
        sweep = await sweep_projects(session.sandbox, targets)
        moved: list[str] = []
        if sweep.missing:
            # The run is over, so a settle on any worker may move a folder
            # between the read above and the sweep. It records the new name
            # before it moves anything: a folder that left its path shows a
            # changed row, and the mirror reads it again under the hold.
            read = {r["workspace_id"]: r["dir_name"] for r in rows}
            moved = [
                r["workspace_id"]
                for r in await get_scan_marks_for_computer(binding.computer_id)
                if r["workspace_id"] in sweep.missing
                and r["dir_name"] != read.get(r["workspace_id"])
            ]
        return unswept + sweep.changed + moved, layouts, sweep
