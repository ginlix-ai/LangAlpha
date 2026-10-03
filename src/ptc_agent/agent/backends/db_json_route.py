"""DbJsonRoute: a mounted directory of JSON files that are rows in Postgres.

The shared plumbing behind ``UserDataBackend`` (`.agents/user/profile/`) and
``AutomationsBackend`` (`.agents/user/automations/`). Reads render live rows.
Every save, from the Write and Edit tools or through the file mount, runs the
same flow: check the content for everything the rows don't decide, then under
the user's lock check the version the writer last saw, plan the write from the
rows that check read, refuse a plan that deletes what the writer never saw,
and commit, all in one transaction. A route whose rows sit behind a remote
call holds no transaction; its store checks the version again as it commits.
A subclass names its directory, its README and the ``DbJsonFile`` behind each
name, which supplies those steps for its rows. Here that is a fixed set every
user has; ``DbJsonFolderRoute`` serves one file per row instead, under a name
the writer picks.

The version (a hash of the agent-visible content) never reaches the agent. A
Read caches it beside the content it served, so a Write is refused unless the
agent read the file in this request and nobody changed it since. An Edit is
checked against the content it edited: the last Read's, or the live file's
when there was none, since ``old_string`` has to match it either way.
"""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import functools
import hashlib
import json
import re
from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, NamedTuple

import psycopg
import structlog

from ptc_agent.agent.backends.langgraph_store import ReadOnlyStoreError, grep_texts, lock_for_namespace
from ptc_agent.agent.backends.read_window import shows_whole_file
from ptc_agent.agent.backends.results import EditTextResult, WriteTextResult
from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.core.paths import logged_path
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.observability.private_errors import failure
from src.server.database.pool import get_db_connection

logger = structlog.get_logger(__name__)

README_FILE = "README.md"

# Past this many, a refusal counts the rest rather than naming them.
_LISTED_DELETES = 10

# Deeper than any file these routes hold. How deep ``json.loads`` reads
# depends on the interpreter's stack, so without a bound of its own a file
# refused on one Python would save on another.
_MAX_JSON_DEPTH = 64


ErrorType = Literal[
    "parse_error",
    "schema_error",
    "version_conflict",
    "read_required",
    "incomplete_read",
    "constraint_error",
    "server_error",
    # A create where a file already is.
    "exists",
    # A save over a version of a file deleted since.
    "deleted",
]


@dataclass
class UserDataValidationError(Exception):
    """Raised when a write payload fails parse / schema / version / constraint checks.

    The backend's `awrite_text` surfaces the `message` verbatim to the agent's
    Write/Edit tool, so it must be self-explanatory and tell the agent how to recover.
    """

    error_type: ErrorType
    file: str  # e.g. "portfolio.json"
    field_path: str  # e.g. "holdings[2].quantity"
    hint: str
    # The README beside the file, which the route that refused the write
    # names, since only it knows where the file is mounted.
    readme: str | None = None
    # Every problem the check found, as (field path, message), so one retry
    # can fix all; ``field_path`` and ``hint`` repeat the first.
    problems: list[tuple[str, str]] = field(default_factory=list)

    def _text(self) -> str:
        if self.problems:
            count = len(self.problems)
            plural = "s" if count > 1 else ""
            head = f"{self.error_type}:{self.file}: {count} problem{plural}"
            lines = [f"{head}, nothing was saved."]
            lines += [f"- {p}: {msg}" if p else f"- {msg}" for p, msg in self.problems]
            return "\n".join(lines)
        where = f"{self.file}:{self.field_path}" if self.field_path else self.file
        return f"{self.error_type}:{where}: {self.hint}"

    @property
    def message(self) -> str:
        text = self._text()
        # Parse + schema failures usually mean the agent guessed at the shape,
        # so the next retry reads the documented one. Not for README.md
        # itself, where the pointer would lead back to the refused file.
        if self.readme and self.error_type in {"parse_error", "schema_error"} and self.file != README_FILE:
            separator = "\n" if "\n" in text else " "
            text += f"{separator}See {self.readme} for the fields and examples."
        return text

    def __str__(self) -> str:
        return self.message


class UnreadableJsonError(Exception):
    """JSON a writer sent that fails past the decoder's own errors."""


class StaleVersion(Exception):
    """Raised by a commit whose store checks the version itself and holds
    another, which the save answers as any version conflict."""


class ReadUnavailable(Exception):
    """A read the store can't answer right now. Its text reaches the reader,
    where any other failure reads as no file at all."""


def load_json(content: str, **kwargs: Any) -> Any:
    """``json.loads`` for a file a writer sent, refusing what not every Python
    reads alike. ``JSONDecodeError`` and a hook's own non-``ValueError`` pass
    through; the rest is ``UnreadableJsonError``, whose text is a hint."""
    too_deep = UnreadableJsonError(f"nested more than {_MAX_JSON_DEPTH} levels deep")
    # A ``\u0000`` escape decodes to a NUL, which the database cannot store.
    has_nul = UnreadableJsonError("a string holds a NUL character (\\u0000), which cannot be saved")
    try:
        value = json.loads(content, **kwargs)
    except json.JSONDecodeError:
        raise
    except RecursionError:
        raise too_deep from None
    except ValueError:
        # Python reads an integer of at most 4300 digits, and says so in
        # terms of its own settings.
        raise UnreadableJsonError("a number has too many digits") from None
    # Walked without recursion, since the value may nest deeper than Python
    # can recurse.
    stack = [(value, 0)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, str):
            if "\x00" in node:
                raise has_nul
        elif isinstance(node, (dict, list)):
            if depth == _MAX_JSON_DEPTH:
                raise too_deep
            if isinstance(node, dict):
                if any("\x00" in key for key in node):
                    raise has_nul
                stack.extend((child, depth + 1) for child in node.values())
            else:
                stack.extend((child, depth + 1) for child in node)
    return value


@dataclass(frozen=True)
class Served:
    """A file as a write is checked against it."""

    content: str
    version: str
    # The agent was shown every line of ``content``. Only a Read can say so.
    whole: bool = False


@dataclass
class Plan[C]:
    """What one save changes. ``deletes`` names each row it removes, which a
    Write after a Read that showed only part of the file may not do."""

    changes: C
    deletes: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.changes)


class Stored(NamedTuple):
    """What a save left: the writer's report, and for a writer that holds the
    file next, the file as the save left it and its version."""

    report: str | None
    content: str | None = None
    version: str | None = None


# A failure unrelated to the content. A model told only to retry kept retrying,
# then created throwaway entries in the user's data to find the cause.
_SERVER_FAILURE = (
    "the server failed while saving, for a reason unrelated to your content; nothing was saved. "
    "Retry once. If it fails again, stop and tell the user; don't add test entries to diagnose it."
)


@functools.cache
def _text_version(text: str) -> str:
    """A README's version, hashed once per class rather than per listing."""
    return hashlib.sha256(text.encode()).hexdigest()[:32]


class DbJsonFile[R, P, C]:
    """One file of rows: ``R`` the rows, ``P`` a written file as parsed, ``C``
    what a save of it changes. The route runs each save through these in one
    order, under its locks; a file only says what its rows are."""

    # The report of a write of the very content the writer read.
    unchanged: str | None = None

    async def fetch(self, user_id: str, conn: Any = None) -> R:
        """The rows; during a save, on ``conn``'s transaction."""
        raise NotImplementedError

    def render(self, rows: R) -> tuple[str, str] | None:
        """The file as the agent reads it, and its version; None when the
        rows hold no file."""
        raise NotImplementedError

    async def lock(self, user_id: str, conn: Any) -> None:
        """Serialize this user's writers across processes, for the rest of
        ``conn``'s transaction."""
        raise NotImplementedError

    async def parse(self, user_id: str, call: CallContext, content: str, served: str | None) -> P:
        """``content`` checked for everything the rows don't decide, with
        whatever the plan reads beyond the rows, before the save locks
        anything: content refused here costs no query, and a read here holds
        no second pool connection while the save's locks keep other saves
        waiting. ``served`` is the content the writer saw, or None when it
        saw the live file. Raises ``UserDataValidationError`` on content it
        refuses."""
        raise NotImplementedError

    def plan(self, call: CallContext, parsed: P, rows: R) -> Plan[C]:
        """What writing ``parsed`` over ``rows`` changes. Raises
        ``UserDataValidationError`` on content it refuses."""
        raise NotImplementedError

    async def hold(self, user_id: str, changes: C, rows: R, conn: Any) -> str | None:
        """Lock what ``changes`` writes over, for the rest of ``conn``'s
        transaction, and the version of ``rows`` as they stand locked (empty
        when they are gone), which the save holds to the one it checked. None
        when the save's own lock already keeps every other writer out."""
        return None

    async def commit(self, user_id: str, changes: C, conn: Any) -> str | None:
        """Write ``changes``; the report the writer reads, or None for a
        plain acknowledgement. Raising rolls the whole save back;
        ``StaleVersion`` says the store found the rows changed."""
        raise NotImplementedError

    async def committed(self, user_id: str, changes: C) -> None:
        """After the commit: drop what caches the rows outside the route."""

    def plan_delete(self, rows: R) -> Plan[C]:
        """What deleting the file changes, in a ``DbJsonFolderRoute``."""
        raise NotImplementedError

    async def rename(self, user_id: str, to: str, conn: Any) -> str | None:
        """File the rows under the name ``to``, in a ``DbJsonFolderRoute``,
        and report it; None when they are gone by the time it holds them.
        Raises ``UserDataValidationError`` (``exists``) when another writer
        took ``to`` first."""
        raise NotImplementedError


class DbJsonRoute:
    """Composite route over a directory of DB-backed JSON files plus a README:
    a fixed set of files, which a save changes but nothing makes, deletes or
    moves."""

    # The directory under the sandbox root, one of ``USER_DATA_DIRS``.
    directory: ClassVar[str] = ""
    # The files every user has, by name.
    files: ClassVar[Mapping[str, DbJsonFile[Any, Any, Any]]] = {}
    data_files: ClassVar[frozenset[str]] = frozenset()
    # Files the server keeps that no save changes, served as the README is.
    read_only_files: ClassVar[frozenset[str]] = frozenset()
    readme_content: ClassVar[str] = ""
    # Whether the file mount serves this route. False keeps it to the file
    # tools, which also refuse Bash and code a command naming it.
    mountable: ClassVar[bool] = True

    # How the file panel serves these files: read-only, since a write there
    # would skip the checks this route makes.
    source: ClassVar[str] = ""
    read_failure: ClassVar[str] = ""
    read_only: ClassVar[str] = ""
    undeletable: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.data_files = frozenset(cls.files)

    def __init__(
        self,
        *,
        user_id: str,
        call: CallContext,
        sandbox_backend: SandboxBackend,
        root_prefix: str,
    ) -> None:
        if not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        self._user_id = user_id
        # Who a save runs for, which a file whose meaning depends on the
        # conversation reads its defaults from.
        self._call = call
        self._sandbox = sandbox_backend
        self._root_prefix = root_prefix
        # Agent → filename → what that agent's last Read served. Only the Read
        # tool's path fills it. Request-scoped via agent.py, never reused across
        # users. The request's subagents share this route and run beside the
        # main agent, so each agent's Reads are its own: another's would let a
        # Write land over content its writer never saw.
        self._reads: dict[str | None, dict[str, Served]] = {}

    # --- the files there are ---

    @classmethod
    def file_named(cls, name: str) -> DbJsonFile[Any, Any, Any] | None:
        """The file ``name`` would be here, whether or not the rows hold it;
        None for a name no file here may take."""
        return cls.files.get(name)

    @classmethod
    async def names(cls, user_id: str) -> list[str]:
        """The name of every file there is, the README aside."""
        return sorted(cls.files)

    @classmethod
    async def rendered(cls, user_id: str) -> dict[str, tuple[str, str]]:
        """Every file there is, by name: its content and version."""
        names = sorted(cls.files)
        files = [cls.files[name] for name in names]
        fetched = await asyncio.gather(*(file.fetch(user_id) for file in files))
        return {
            name: rendered
            for name, file, rows in zip(names, files, fetched)
            if (rendered := file.render(rows)) is not None
        }

    @classmethod
    async def load(cls, path: str, user_id: str) -> str | None:
        """The file at ``path`` (relative to the sandbox root), as the agent
        reads it; None when there is none. For the file panel."""
        file = cls.file_named(path.rsplit("/", 1)[-1])
        rendered = file.render(await file.fetch(user_id)) if file else None
        return rendered[0] if rendered else None

    # --- composite-compatible surface ---

    @property
    def root_prefix(self) -> str:
        return self._root_prefix

    def normalize_path(self, path: str) -> str:
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    # --- helpers ---

    def _namespace(self) -> tuple[str, ...]:
        """Key of the in-process lock serializing this user's writes."""
        return (self._user_id, self.directory)

    def _filename(self, normalized_path: str) -> str | None:
        """The basename when ``path`` names the README or a file this route
        may hold, whether or not the rows hold it; else None."""
        if not normalized_path.startswith(self._root_prefix):
            return None
        suffix = normalized_path[len(self._root_prefix):]
        if "/" in suffix:
            return None
        if suffix == README_FILE or self.file_named(suffix) is not None:
            return suffix
        return None

    def _absolute(self, filename: str) -> str:
        return f"{self._root_prefix}{filename}"

    def _refusal(self, error_type: ErrorType, filename: str, hint: str) -> UserDataValidationError:
        return UserDataValidationError(
            error_type=error_type,
            file=filename,
            field_path="",
            hint=hint,
            readme=self._absolute(README_FILE),
        )

    def _writable_files(self) -> str:
        return " / ".join(sorted(self.data_files - self.read_only_files))

    def _readme_instead(self) -> str:
        return f"Update {self._writable_files()} instead."

    def _readme_refusal(self, file_path: str) -> UserDataValidationError:
        return self._refusal(
            "schema_error",
            README_FILE,
            f"{file_path} is documentation, not data; it can't be edited. {self._readme_instead()}",
        )

    def is_fixed_path(self, file_path: str) -> bool:
        """Whether a file is always at ``file_path``, so a create there is
        refused without reading the rows: the README, or a fixed file."""
        filename = self._filename(file_path)
        return filename == README_FILE or filename in self.files

    async def _live(self, filename: str) -> Served | None:
        file = self.file_named(filename)
        rendered = file.render(await file.fetch(self._user_id))
        return Served(*rendered) if rendered else None

    @property
    def _read_cache(self) -> dict[str, Served]:
        """The calling agent's Reads: a background subagent's when the tool
        call is its own, else the main agent's."""
        # Imported here: the middleware package imports this module.
        from ptc_agent.agent.middleware.background_subagent.context import current_background_agent_id

        return self._reads.setdefault(current_background_agent_id.get(), {})

    def _invalidate(self, filename: str) -> None:
        """Drop the calling agent's Read. Another agent's stays: after a save
        here, its Write is told the file changed since, which a dropped Read
        would not say."""
        self._read_cache.pop(filename, None)

    @staticmethod
    @contextlib.asynccontextmanager
    async def _transaction() -> AsyncIterator[Any]:
        async with get_db_connection() as conn, conn.transaction():
            yield conn

    def _require_read(self, filename: str) -> Served:
        """What the agent last read, or a refusal telling it to read."""
        served = self._read_cache.get(filename)
        if served is None:
            path = self._absolute(filename)
            # A reply to a question starts a new run with an empty cache, which
            # the model sees as the same turn, so say what reset it.
            raise self._refusal(
                "read_required",
                filename,
                f"no Read of {path} since this run started (answering a question starts a new "
                f"run). Read({path}), then write again.",
            )
        return served

    def _unseen_deletes(self, filename: str, deletes: list[str]) -> UserDataValidationError:
        listed = ", ".join(deletes[:_LISTED_DELETES])
        if len(deletes) > _LISTED_DELETES:
            listed += f" and {len(deletes) - _LISTED_DELETES} more"
        return self._refusal(
            "incomplete_read",
            filename,
            f"this write leaves out {listed}, which would delete "
            f"{'it' if len(deletes) == 1 else 'them'}, but your last Read didn't show the whole "
            "file. Read it without offset or limit before a Write that removes anything. If it "
            "is too long to show whole, remove each one with Edit.",
        )

    @contextlib.contextmanager
    def _refusals(self, filename: str) -> Iterator[None]:
        """Answer every failure of a save that saved nothing as a refusal."""
        try:
            yield
        except UserDataValidationError as exc:
            # A refusal over a stale Read drops it, so the next Read pulls
            # fresh rows and the next Write of a deleted file makes it anew;
            # kept, a retry would be refused again.
            if exc.error_type in ("version_conflict", "deleted"):
                self._invalidate(filename)
            exc.readme = exc.readme or self._absolute(README_FILE)
            raise
        except ReadUnavailable as exc:
            hint = f"{exc} Nothing was saved."
            raise self._refusal("server_error", filename, hint) from exc
        except psycopg.DataError as exc:
            # A value the column cannot hold: the content's fault, not an
            # outage to retry.
            raise self._refusal(
                "schema_error",
                filename,
                f"a value is too long or out of range for its column ({type(exc).__name__}).",
            ) from exc
        except Exception as exc:
            fields, trace = failure(exc)
            logger.error(
                "db json route write failed",
                path=logged_path(self._absolute(filename)),
                exc_info=trace,
                **fields,
            )
            raise self._refusal("server_error", filename, _SERVER_FAILURE) from exc

    def _exists(self, filename: str) -> UserDataValidationError:
        path = self._absolute(filename)
        # A reply to a question starts a new run with no Reads, which the
        # model sees as the same turn, so say what reset it.
        return self._refusal(
            "exists",
            filename,
            f"{path} already exists. To change it, Read({path}) first (answering a question starts "
            "a new run, which needs a new Read); to add another, write it under another name.",
        )

    def _stale(self, filename: str, *, gone: bool) -> UserDataValidationError:
        """A save over a version the rows no longer have; ``gone`` when they
        hold no file, which a fixed one never is."""
        path = self._absolute(filename)
        return self._refusal(
            "version_conflict",
            filename,
            f"{path} changed since your last Read, so this write could undo that change. "
            f"Read({path}) again and reapply your change.",
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
        """Plan and commit a save over ``version``, or only where no file is
        when it is None, under the in-process and database locks.
        ``may_delete`` says the writer saw everything the save could drop: a
        Write after a Read of the whole file, an Edit, which removes only text
        it quotes, or a program, which writes the whole file. ``settle``
        answers with the file as the save left it, read in the save's own
        transaction, which spares the writer reading it back.
        """
        file = self.file_named(filename)
        if served is not None and content == served:
            # Written back as it was read: nothing to change, whatever the
            # rows did since, so nothing to read or lock.
            self._invalidate(filename)
            return Stored(file.unchanged)
        user_id = self._user_id
        with self._refusals(filename):
            parsed = await file.parse(user_id, self._call, content, served)
        async with lock_for_namespace(self._namespace()):
            with self._refusals(filename):
                # One transaction, which anything raised inside rolls back:
                # the plan and the writes go over the rows the version check
                # read, under the lock taken before reading them.
                async with self._transaction() as conn:
                    await file.lock(user_id, conn)
                    rows = await file.fetch(user_id, conn)
                    rendered = file.render(rows)
                    if version is None and rendered is not None:
                        raise self._exists(filename)
                    if (rendered[1] if rendered else None) != version:
                        raise self._stale(filename, gone=rendered is None)
                    plan = file.plan(self._call, parsed, rows)
                    if plan.deletes and not may_delete:
                        raise self._unseen_deletes(filename, plan.deletes)
                    held = await file.hold(user_id, plan.changes, rows, conn)
                    if held is not None and held != version:
                        raise self._stale(filename, gone=not held)
                    try:
                        report = await file.commit(user_id, plan.changes, conn)
                    except StaleVersion:
                        raise self._stale(filename, gone=False) from None
                    if settle:
                        # A save that changed nothing leaves the rows it read,
                        # and one that deleted the file leaves none.
                        after = file.render(await file.fetch(user_id, conn) if plan else rows)
            await file.committed(user_id, plan.changes)
            self._invalidate(filename)
        if settle and after is not None:
            return Stored(report, *after)
        return Stored(report)

    # --- read ---

    async def aread_text(self, file_path: str) -> str | None:
        """The live file, for readers other than the Read tool.

        It leaves the read cache alone: the agent never saw this content, so it
        can't vouch for a later Write.
        """
        filename = self._filename(file_path)
        if filename is None:
            # Anything under the prefix that isn't a known file is not ours:
            # return None so the composite reports file-not-found.
            return None
        if filename == README_FILE:
            return self.readme_content
        try:
            live = await self._live(filename)
        except Exception:
            logger.exception("db json route read failed", path=logged_path(file_path))
            return None
        return live.content if live else None

    async def aread_range(self, file_path: str, offset: int = 0, limit: int = 2000) -> str | None:
        """The Read tool's path. Every Read shows live rows, even ones changed
        earlier in the turn, and records what it showed for the next Write."""
        filename = self._filename(file_path)
        if filename is None:
            return None
        if filename == README_FILE:
            content = self.readme_content
        else:
            try:
                live = await self._live(filename)
            except ReadUnavailable:
                raise
            except Exception:
                logger.exception("db json route read failed", path=logged_path(file_path))
                return None
            if live is None:
                # Gone since an earlier Read, which a Write must not update.
                self._invalidate(filename)
                return None
            content = live.content
            self._read_cache[filename] = Served(
                content, live.version, whole=shows_whole_file(content, offset, limit)
            )
        lines = content.splitlines(keepends=True)
        start = max(0, offset)
        end = start + max(0, limit)
        return "".join(lines[start:end])

    # --- write / edit ---

    def _read_only(self, filename: str | None) -> bool:
        """Whether no save writes ``filename``: none here, the README, or a
        file the server keeps."""
        return filename in (None, README_FILE) or filename in self.read_only_files

    def _read_only_refusal(self, file_path: str) -> UserDataValidationError:
        return self._refusal(
            "schema_error",
            file_path.rsplit("/", 1)[-1],
            f"{file_path} is kept by the server; it can't be edited. {self._readme_instead()}",
        )

    def _unwritable(self, file_path: str, filename: str | None) -> UserDataValidationError:
        if filename == README_FILE:
            return self._readme_refusal(file_path)
        if filename in self.read_only_files:
            return self._read_only_refusal(file_path)
        # The composite routed this path here, so the folder is ours and the
        # file can't exist; say which ones can rather than just fail.
        names = self._writable_files()
        return self._refusal(
            "schema_error",
            file_path.rsplit("/", 1)[-1],
            f"{file_path} can't be created: this folder holds only {names} and {README_FILE}, "
            f"which the server keeps. Update {names} instead.",
        )

    @staticmethod
    def _written(stored: Stored) -> bool | WriteTextResult:
        return {"success": True, "message": stored.report} if stored.report else True

    async def awrite_text(self, file_path: str, content: str) -> bool | WriteTextResult:
        """Validate + apply a JSON write over the agent's last Read in this
        run. Raises UserDataValidationError on bad input."""
        filename = self._filename(file_path)
        if self._read_only(filename):
            raise self._unwritable(file_path, filename)
        base = self._require_read(filename)
        return self._written(
            await self._save(filename, content, base.version, served=base.content, may_delete=base.whole)
        )

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> EditTextResult:
        filename = self._filename(file_path)
        if filename is None:
            return {"success": False, "error": f"File not found: {file_path}"}
        if filename == README_FILE:
            return {"success": False, "error": self._readme_refusal(file_path).hint}
        if filename in self.read_only_files:
            return {"success": False, "error": self._read_only_refusal(file_path).hint}
        if old_string == new_string:
            return {"success": False, "error": "old_string and new_string are identical"}

        base = self._read_cache.get(filename)
        if base is None:
            try:
                base = await self._live(filename)
            except Exception as exc:
                return {"success": False, "error": f"Read failed: {exc!s}"}
            if base is None:
                return {"success": False, "error": f"File not found: {file_path}"}
        content = base.content

        occurrences = content.count(old_string)
        if occurrences == 0:
            preview = old_string if len(old_string) <= 120 else f"{old_string[:120]}…"
            return {"success": False, "error": f"String not found: {preview!r}"}
        if occurrences > 1 and not replace_all:
            return {
                "success": False,
                "error": (
                    f"String appears {occurrences} times. Provide more context or "
                    "set replace_all=True."
                ),
            }
        new_content = content.replace(old_string, new_string, occurrences if replace_all else 1)

        try:
            report = (
                await self._save(filename, new_content, base.version, served=base.content, may_delete=True)
            ).report
        except UserDataValidationError as exc:
            return {"success": False, "error": str(exc)}

        if report:
            message = report
        elif replace_all:
            message = f"Edited {file_path} ({occurrences} occurrences replaced)"
        else:
            message = f"Edited {file_path}"
        return {
            "success": True,
            "occurrences": occurrences if replace_all else 1,
            "size": len(new_content),
            "message": message,
        }

    # --- file mount ---
    #
    # The mount checks a save against the version it served, so it serves this
    # route's version rather than a hash of the content: a run moving `state`
    # changes the content but must not make a save conflict. No Read tool call
    # stands behind a save there, so the version is the whole check.

    def is_writable(self, file_path: str) -> bool:
        """Whether a file at ``file_path`` takes saves, or for the folder
        itself, whether new files may be made in it."""
        if file_path.rstrip("/") == self._root_prefix.rstrip("/"):
            return False
        return not self._read_only(self._filename(file_path))

    async def aread_versioned(self, file_path: str) -> tuple[str, str] | None:
        """(content, version), or None for a path this route has no file at.
        Raises when the rows can't be read, which the mount answers as a retry."""
        filename = self._filename(file_path)
        if filename is None:
            return None
        if filename == README_FILE:
            return self.readme_content, _text_version(self.readme_content)
        live = await self._live(filename)
        return (live.content, live.version) if live else None

    async def alist(self, path: str) -> list[dict[str, Any]] | None:
        """Every file, rendered for its size, so each carries the content it
        was rendered from and a read after the listing costs nothing."""
        if path.rstrip("/") != self._root_prefix.rstrip("/"):
            return None
        files = {
            README_FILE: (self.readme_content, _text_version(self.readme_content)),
            **await self.rendered(self._user_id),
        }
        return [
            {
                "name": name,
                "type": "file",
                "size": len(content.encode()),
                "version": version,
                "writable": not self._read_only(name),
                "content": content,
            }
            for name, (content, version) in sorted(files.items())
        ]

    async def awrite_versioned(self, file_path: str, content: str, version: str | None) -> Stored:
        """Apply a save through the mount over ``version``, or only where no
        file is when it is None: its report, and the file as it left it with
        its version, which the mount holds next (none when the save deleted
        it).

        A program writes the whole file, so the save may delete what it leaves
        out, and its report lists every deletion. Raises
        ``UserDataValidationError`` as ``awrite_text`` does.
        """
        filename = self._filename(file_path)
        if self._read_only(filename):
            raise self._unwritable(file_path, filename)
        return await self._save(filename, content, version, served=None, may_delete=True, settle=True)

    def _documentation(self, file_path: str) -> ReadOnlyStoreError:
        return ReadOnlyStoreError(f"{file_path} is documentation, kept by the server.")

    async def adelete_versioned(self, file_path: str) -> Stored | None:
        """None where no file is; anything else here raises
        ``ReadOnlyStoreError``, since every user has each of these files."""
        filename = self._filename(file_path)
        if filename is None:
            return None
        if filename == README_FILE:
            raise self._documentation(file_path)
        raise ReadOnlyStoreError(
            f"{file_path} is a view of saved data and cannot be deleted. "
            "Write it back with the entries removed to clear it."
        )

    # --- glob / grep ---

    def _covers(self, normalized_path: str) -> bool:
        return normalized_path.startswith(self._root_prefix) or (
            normalized_path.rstrip("/") == self._root_prefix.rstrip("/")
        )

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        if not self._covers(self.normalize_path(path)):
            return []
        try:
            names = await self.names(self._user_id)
        except Exception:
            logger.exception("db json route listing failed", path=self._root_prefix)
            names = []
        out: list[str] = []
        for filename in sorted([README_FILE, *names]):
            absolute = self._absolute(filename)
            if fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(absolute, pattern):
                out.append(absolute)
        return out

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 - mirror sandbox.agrep_rich
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        # Context-window args (lines_after / lines_before / lines_context / multiline)
        # are accepted for signature parity with the sandbox grep but are not
        # honored: these files are small JSON documents the agent should
        # just Read whole instead of grep-context-paging.
        if not self._covers(self.normalize_path(path)):
            return []

        flags = re.IGNORECASE if case_insensitive else 0
        try:
            compiled = re.compile(pattern, flags=flags)
        except re.error:
            return []

        try:
            live = await self.rendered(self._user_id)
        except Exception:
            logger.exception("db json route listing failed", path=self._root_prefix)
            live = {}
        contents = {README_FILE: self.readme_content}
        for filename, (content, _version) in live.items():
            # What the agent last read, else the live file. Never cached: a
            # Grep is no Read a Write can stand on.
            served = self._read_cache.get(filename)
            contents[filename] = served.content if served else content
        texts: list[tuple[str, str]] = []
        for filename, content in sorted(contents.items()):
            absolute = self._absolute(filename)
            if glob and not fnmatch.fnmatch(filename, glob) and not fnmatch.fnmatch(absolute, glob):
                continue
            texts.append((absolute, content))
        return grep_texts(
            texts, compiled, output_mode, show_line_numbers=show_line_numbers, head_limit=head_limit, offset=offset
        )
