"""DbJsonFolderRoute: a mounted folder of JSON files, one per row in Postgres.

Where ``DbJsonRoute`` serves a fixed set of files every user has, a folder
holds one file per row, under a name the writer picks: writing a new name
makes one, and the file mount deletes and renames them. A file can therefore
go between a Read and the Write after it, which the save answers as such
rather than as a change. ``AutomationsBackend`` is the one folder.
"""

from __future__ import annotations

from typing import ClassVar

from ptc_agent.agent.backends.db_json_route import (
    README_FILE,
    DbJsonRoute,
    Stored,
    UserDataValidationError,
)
from ptc_agent.agent.backends.langgraph_store import lock_for_namespace
from ptc_agent.agent.backends.results import WriteTextResult


class DbJsonFolderRoute(DbJsonRoute):
    """A folder of DB-backed JSON files, one per row, plus a README. A
    subclass names its files from its rows, overriding ``file_named``,
    ``names`` and ``rendered``."""

    # The names a file here may take, as a refusal of another says them.
    name_rule: ClassVar[str] = ""
    # What each file here stands for, as a refusal names it.
    entry: ClassVar[str] = "an entry"

    @property
    def fixed_names(self) -> None:
        """None: the rows name the files here, so only a read can tell."""
        return None

    def _readme_instead(self) -> str:
        return "Write the files beside it instead."

    def _stale(self, filename: str, *, gone: bool) -> UserDataValidationError:
        if not gone:
            return super()._stale(filename, gone=gone)
        path = self._absolute(filename)
        return self._refusal(
            "deleted",
            filename,
            f"{path} was deleted since your last Read, so nothing was saved. Writing it again "
            "creates it anew: do that only if the user wants it back.",
        )

    async def _save(
        self,
        filename: str,
        content: str,
        version: str | None,
        *,
        served: str | None,
        may_delete: bool,
        settle: bool = False,
    ) -> Stored:
        if served is not None and content == served:
            # The base answers a write-back of what was read as no change
            # without reading the rows, which here would hide a delete since.
            with self._refusals(filename):
                if await self._live(filename) is None:
                    raise self._stale(filename, gone=True)
        return await super()._save(
            filename, content, version, served=served, may_delete=may_delete, settle=settle
        )

    # --- write ---

    def _unwritable(self, file_path: str, filename: str | None) -> UserDataValidationError:
        if filename == README_FILE:
            return self._readme_refusal(file_path)
        name = file_path.rsplit("/", 1)[-1]
        return self._refusal("schema_error", name, f"{file_path} can't be created: {self.name_rule}.")

    async def awrite_text(self, file_path: str, content: str) -> bool | WriteTextResult:
        """As the base's, except that a file the agent hasn't read in this
        run is written only where none is yet, which makes a new one."""
        filename = self._filename(file_path)
        if filename is None or filename == README_FILE or filename in self._read_cache:
            return await super().awrite_text(file_path, content)
        return self._written(await self._save(filename, content, None, served=None, may_delete=True))

    # --- file mount ---

    def is_writable(self, file_path: str) -> bool:
        if file_path.rstrip("/") == self._root_prefix.rstrip("/"):
            return True
        return super().is_writable(file_path)

    async def adelete_versioned(self, file_path: str, version: str | None = None) -> Stored | None:
        """Delete the file through the mount, only at ``version`` when given:
        the report, or None where no file is. Raises ``ReadOnlyStoreError``
        for the README."""
        filename = self._filename(file_path)
        if filename is None:
            return None
        if filename == README_FILE:
            raise self._documentation(file_path)
        file = self.file_named(filename)
        user_id = self._user_id
        async with lock_for_namespace(self._namespace()):
            with self._refusals(filename):
                async with self._transaction() as conn:
                    await file.lock(user_id, conn)
                    rows = await file.fetch(user_id, conn)
                    rendered = file.render(rows)
                    if rendered is None:
                        return None
                    if version is not None and rendered[1] != version:
                        raise self._stale(filename, gone=False)
                    plan = file.plan_delete(rows)
                    report = await file.commit(user_id, plan.changes, conn)
            await file.committed(user_id, plan.changes)
            self._invalidate(filename)
        return Stored(report)

    def _occupied(self, source: str, target: str) -> UserDataValidationError:
        """A move onto a file, which would delete it. The usual source is a
        helper's temporary copy, which a save here made into a file of its own."""
        return self._refusal(
            "exists",
            target,
            f"{self._absolute(target)} already exists, and {source} is {self.entry} of its own. "
            f"To replace {target}, write {source}'s content into {target} and rm {source}; to keep "
            f"both, move {source} to another name.",
        )

    async def arename_versioned(self, file_path: str, to: str) -> str | None:
        """Move the file to ``to`` in this folder, keeping the rows behind
        it: the report, or None where no file is. Refuses a ``to`` where a
        file is (``exists``) or no file may be, and raises
        ``ReadOnlyStoreError`` for the README."""
        filename = self._filename(file_path)
        if filename is None:
            return None
        if filename == README_FILE:
            raise self._documentation(file_path)
        target = self._filename(to)
        if target is None:
            raise self._unwritable(to, None)
        if target == README_FILE:
            raise self._refusal(
                "exists", target, f"{self._absolute(target)} already exists; move the file to another name."
            )
        file, user_id = self.file_named(filename), self._user_id
        async with lock_for_namespace(self._namespace()):
            with self._refusals(filename):
                async with self._transaction() as conn:
                    await file.lock(user_id, conn)
                    there = self.file_named(target)
                    if there.render(await there.fetch(user_id, conn)) is not None:
                        raise self._occupied(filename, target)
                    report = await file.rename(user_id, target, conn)
            self._invalidate(filename)
            self._invalidate(target)
        return report
