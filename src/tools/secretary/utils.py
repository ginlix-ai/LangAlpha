"""Utility functions for secretary tools."""

import functools
import json
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone as dt_timezone
from typing import Any, Literal, NamedTuple
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo

from ptc_agent.core.paths import computer_roots

logger = logging.getLogger(__name__)

MAX_OUTPUT_CHARS = 8000

# Ceiling on how many turns a single read pulls from the DB (output is
# truncated to MAX_OUTPUT_CHARS regardless).
_MAX_HISTORY_TURNS = 50

# When the default single-turn read lands on a text-less newest turn (tool-only
# / chart-only), look back this many turns for the most-recent turn with text.
_EMPTY_LATEST_FALLBACK_TURNS = 5

# Inserted between turns when more than one turn is returned.
_TURN_SEPARATOR = "\n\n---\n\n"

# How a run's label words its stored status. A run that stopped on a question
# or an approval card is waiting on the user, not finished.
_RUN_STATUS = {
    "in_progress": "still running",
    "completed": "completed",
    "error": "failed",
    "cancelled": "stopped",
    "interrupted": "waiting on the user",
}

# Destinations worth rewriting into a workspace reference. Deliberately
# narrower than the web client, which treats any relative link as a file
# (`filePaths.isFilePath`) and leaves the panel to resolve the name: the relay
# is rewriting text it will persist, so it only claims destinations that look
# like files. Every extension the client's own tables name as one belongs here
# -- `filePaths.KIND_BY_EXT` (what a deliverable card can show) and
# `filePanel/fileMeta` (what the panel lists and offers to download) -- because
# a deliverable that relays unqualified resolves against Home, which holds
# none of these files.
_FILE_EXTS = (
    r"md|markdown|txt|pdf|doc|docx|odt|rtf|"
    r"py|js|jsx|ts|tsx|html|htm|css|sh|bash|sql|r|rb|go|rs|java|ipynb|pptx|ppt|key|"
    r"csv|tsv|json|jsonl|yaml|yml|xml|toml|ini|cfg|log|env|xlsx|xlsm|xls|"
    r"png|jpg|jpeg|gif|svg|webp|bmp|"
    r"numbers|pages|"
    r"parquet|feather|pkl|pickle|npy|npz|h5|hdf5|db|sqlite|"
    r"mp3|wav|mp4|mov|webm|"
    r"zip|tar|tgz|gz|bz2|xz|7z|rar"
)

# Workspace-qualified path prefix: __wsref__/{workspace_id}/relative/path
# Uses a path-based encoding instead of ws:// protocol to survive HTML sanitizers.
_WSREF_PREFIX = "__wsref__"

# A markdown link or image and its destination, bare or in angle brackets.
# A bare destination carries one level of balanced parens, as CommonMark says and
# the client's own `normalizeFileRefs.LINK_DEST_RE` already reads -- `report(1).pdf`
# is a real deliverable name. A title or a URL fails the file test below.
# A bare destination holds no angle bracket either, so an unclosed `<a.jsonl`
# fails to match rather than matching bare and losing its last character to the
# closing-bracket strip -- `.jsonl` shortened to `.json` is another live
# extension, so that link would have resolved to a different real file.
# The label is bounded because it is unanchored: on a run of `[` with no `]`,
# an unbounded `[^\]\n]*` rescans to the end of the line from every one of
# them, and this runs synchronously on a whole turn of agent text inside an
# async handler. 512 is past any real link label.
# A destination may carry a CommonMark title, which is not part of the path. The
# title cannot be found by cutting at the first space, because a bare
# `results/Q3 deck.pptx` is a name the agent really writes and the bracketed form
# above exists to keep it: the quote or paren is the only signal. So the
# destination stops as early as it can and the title rides in the closing group,
# which `_rewrite` already emits untouched. Left unqualified, a titled link is
# not safely ignored the way a URL is -- the parser hands the client the bare
# path, which then resolves against Home and opens nothing.
_DEST = r"<[^<>\n]+>|(?:[^()<>\n]|\([^()<>\n]*\))+?"
_TITLE = r"""(?:[ \t]+(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\)))?"""
_MD_LINK_RE = re.compile(r"(!?\[[^\]\n]{0,512}\]\()(" + _DEST + r")(" + _TITLE + r"\))")

# Fenced blocks and inline code spans: shown, not read. A link inside one is
# syntax the reply is displaying, so splicing a workspace id into it corrupts
# what the reader copies out, and this text is persisted, so the corruption
# outlives the turn. The web client splits the same way before its own rewrites
# (`markdownSegments.mapOutsideCode`).
# A closing fence only has to be *at least* as long as the opener, so the
# closer is the opener plus any further run of its own character. Repeating
# the whole opener instead would accept only multiples of its length, and a
# three-backtick block closed by four would swallow the rest of the turn.
# An inline span is delimited by a whole run of backticks and closes on the
# next run of the same length, which is why both delimiters are fenced off by
# lookarounds: a run is only a delimiter if nothing longer contains it. Reading
# one backtick at a time instead paired the inner run of `` `[r](a.md)` `` with
# itself and handed the link in between to the rewrite, so an agent showing the
# citation syntax got a workspace id spliced into the example it was showing.
_CODE_RE = re.compile(
    r"^[ \t]*(?P<fence>(?P<fchar>[`~])(?P=fchar){2,})[^\n]*\n"
    r".*?(?:^[ \t]*(?P=fence)(?P=fchar)*[ \t]*$|\Z)"
    r"|(?<!`)(?P<tick>`+)(?!`)[^\n]*?(?<!`)(?P=tick)(?!`)",
    re.MULTILINE | re.DOTALL,
)


def _map_outside_code(text: str, rewrite) -> str:
    """Apply `rewrite` to the prose of `text`, leaving code spans untouched."""
    out: list[str] = []
    last = 0
    for m in _CODE_RE.finditer(text):
        out.append(rewrite(text[last : m.start()]))
        out.append(m.group(0))
        last = m.end()
    out.append(rewrite(text[last:]))
    return "".join(out)

# Path separators, encoded or not: `..%2f..%2f` climbs exactly as `../../` does.
_SEP_RE = re.compile(r"/|%2[fF]")

# The place inside a file a reference points at, in its `:line` forms:
# :line, :start-end, :line:col. The `#fragment` and `?query` forms are read by
# `_split_location`.
_LINE_LOCATION_RE = re.compile(r":\d+(?:-\d+|:\d+)?$")


@functools.lru_cache(maxsize=64)
def _roots_re(roots: tuple[str, ...]) -> re.Pattern[str]:
    """Any of ``roots`` in every spelling, with or without a `file://` scheme.

    A link is tested before it is decoded, so a root with characters that
    travel percent-encoded matches in that spelling too, the one the client's
    ``ownRootPatterns`` reads (``encodeURIComponent`` per segment).
    """
    spellings = dict.fromkeys(
        spelling
        for r in roots
        for spelling in (r, "/".join(quote(s, safe="!*'()") for s in r.split("/")))
    )
    alternation = "|".join(re.escape(s) for s in spellings)
    return re.compile(f"^(?:file://)?(?:{alternation})/", re.IGNORECASE)


class _Placement(NamedTuple):
    """Where a workspace's files are: its computer's root, None when it is on
    none, and its folder there; and where they were: the roots of the
    computers it moved off and the names its folder had."""

    root: str | None = None
    previous_roots: Sequence[str] = ()
    folder: str | None = None
    previous_folders: Sequence[str] = ()


def _without_root(path: str, where: _Placement) -> str:
    """``path`` without the computer root it is spelled under, current or legacy.

    The current root is the workspace's own computer's: a computer whose user
    cannot create the stock root keeps its workspaces under another one, and
    its analyst spells that out in an absolute link like any other. A turn
    from before a move spells its links under a root the workspace left,
    where only a path through its folder, by any name it had, is its own; the
    folder goes with that root, and the rest of the root held its siblings.
    A path through the current folder reads as the current one first, and
    without it, as the server's file routes and the client read it.
    """
    here = _roots_re(computer_roots(where.root)).match(path)
    rest = path[here.end() :] if here else None
    if rest is not None and where.folder:
        first, _, inside = rest.partition("/")
        if unquote(first) == where.folder:
            return inside
    names = {n.casefold() for n in (where.folder, *where.previous_folders) if n}
    left = {r.rstrip("/") for r in where.previous_roots} - {""}
    for root in sorted(left, key=len, reverse=True):
        there = _roots_re((root,)).match(path)
        if there:
            first, _, inside = path[there.end() :].partition("/")
            if unquote(first).casefold() in names:
                return inside
    return path if rest is None else rest


_BARE_DOMAIN_RE = re.compile(r"^www\.", re.IGNORECASE)
_FILE_NAME_RE = re.compile(r"\.(?:" + _FILE_EXTS + r")$", re.IGNORECASE)
# A colon introduces a scheme only before the first slash, which is the rule
# every URL reader on the client uses too (react-markdown's `defaultUrlTransform`
# and `hast-util-sanitize`). Rejecting every colon also rejected
# `results/Q3:final.pdf`, a name the sandbox allows and the resolver accepts, so
# the link relayed unqualified and opened nothing in Home.
_SCHEME_RE = re.compile(r"^[^/]*:")


def _split_location(path: str) -> tuple[str, str]:
    """A reference split into the file and everything trailing it.

    A destination is a URL, so `#` opens the fragment and `?` the query, each
    at its first occurrence. That is the reading `agentPaths.normalizeAgentHref`
    does on the other side of the wire, and a name carrying either character
    travels percent-encoded, so `results/issue%231.md` arrives here whole and
    the client decodes it after the split. Reading a literal `#` as part of a
    name instead would make this the only reader in the chain that disagrees
    with the markdown its own client renders.

    Whitespace marks nothing, which is the half worth spelling out: the panel's
    `findHeadingIndex` matches a heading as written and not only as a slug, so
    `report.md#Valuation Assumptions` is a reference it opens. Cutting at the
    space left a head ending in `Assumptions`, which is no file, and the link
    relayed unqualified into a workspace that does not hold it.
    """
    tail = ""
    # `#` first: everything after it is fragment, a `?` inside it included, so
    # the query pass then reads only what really precedes the fragment.
    for mark in ("#", "?"):
        cut = path.find(mark)
        if cut != -1:
            path, tail = path[:cut], path[cut:] + tail
    line = _LINE_LOCATION_RE.search(path)
    return (path[: line.start()], line.group(0) + tail) if line else (path, tail)


def _qualify_file_paths(
    text: str,
    workspace_id: str,
    root: str | None = None,
    previous_roots: Sequence[str] = (),
    folder: str | None = None,
    previous_folders: Sequence[str] = (),
) -> str:
    """Rewrite workspace file links to __wsref__/{workspace_id}/path.

    Transforms:
        [report.md](t/report.md) → [report.md](__wsref__/{wid}/t/report.md)
        [model](./model.py:42)        → [model](__wsref__/{wid}/model.py:42)
        [deck](<t/Q3 deck.pptx>) → [deck](<__wsref__/{wid}/t/Q3 deck.pptx>)

    The relayed text renders in Home, which holds none of these files, so
    every relative file link has to carry where it lives, bare names included.
    A path-based prefix survives HTML sanitizers that strip unknown protocols.
    URLs, in-page anchors, paths outside the workspace and already-qualified
    links are left as written.
    """
    if not workspace_id or not text:
        return text
    where = _Placement(root, previous_roots, folder, previous_folders)

    def _rewrite(m: re.Match) -> str:
        prefix, dest, suffix = m.group(1), m.group(2), m.group(3)
        bracketed = dest.startswith("<")
        # `file_refs.clean_path` folds separators before it judges a path, so
        # the guards below read the same string the resolver will, or the relay
        # mints a reference that resolver then refuses.
        path = (dest[1:-1] if bracketed else dest).strip().replace("\\", "/")
        path, tail = _split_location(path)
        path = _without_root(path, where)
        while path.startswith("./"):
            path = path[2:]
        if (
            not path
            or path.startswith(("/", "#", f"{_WSREF_PREFIX}/"))
            or _SCHEME_RE.match(path)
            # A destination that climbs out of the working directory names no
            # workspace file, and `file_refs.clean_path` refuses one, so
            # qualifying it would mint a reference the resolver rejects.
            or ".." in _SEP_RE.split(path)
            # A scheme-less bare domain is a link out, not a file. The client
            # says so itself (`filePaths.isFilePath`), but reads the `__wsref__`
            # prefix first, so a qualified one arrives already claimed.
            or _BARE_DOMAIN_RE.match(path)
            or not _FILE_NAME_RE.search(path)
        ):
            return m.group(0)
        qualified = f"{_WSREF_PREFIX}/{workspace_id}/{path}{tail}"
        if bracketed or any(c.isspace() for c in qualified):
            qualified = f"<{qualified}>"
        return f"{prefix}{qualified}{suffix}"

    return _map_outside_code(text, lambda prose: _MD_LINK_RE.sub(_rewrite, prose))


def _parse_sse_string(raw: str) -> tuple[str, dict] | None:
    """Parse a raw SSE string into (event_type, data_dict).

    Raw SSE format: "id: 42\\nevent: message_chunk\\ndata: {...}\\n\\n"

    Args:
        raw: Raw SSE string from Redis

    Returns:
        Tuple of (event_type, data_dict) or None if parsing fails
    """
    try:
        event_type = ""
        data_str = ""

        for line in raw.split("\n"):
            line = line.strip()
            if line.startswith("event:"):
                event_type = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_str = line[len("data:"):].strip()

        if not event_type or not data_str:
            return None

        data = json.loads(data_str)
        return (event_type, data)
    except (json.JSONDecodeError, ValueError, AttributeError):
        return None


def _truncate_single(text: str) -> str:
    """Head-truncate one turn's text to ``MAX_OUTPUT_CHARS`` (keeps the start)."""
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    return text[:MAX_OUTPUT_CHARS] + (
        "\n\n[truncated — full output available in workspace]"
    )


def _join_recent_turns(turn_texts: list[str]) -> str:
    """Join turn texts oldest -> newest, capped at ``MAX_OUTPUT_CHARS``.

    Turn boundaries are known from the list (not rediscovered by scanning the
    joined text), so a turn whose own markdown contains ``---`` is never
    mistaken for a separator. When the cap is exceeded, whole older turns are
    dropped from the front so the most-recent turns survive; the newest turn is
    head-truncated if it alone overflows. A banner notes any dropped turns.
    """
    turn_texts = [t for t in turn_texts if t]
    if not turn_texts:
        return ""

    joined = _TURN_SEPARATOR.join(turn_texts)
    if len(joined) <= MAX_OUTPUT_CHARS:
        return joined

    # Keep the newest turns that fit, always retaining at least the newest one.
    kept: list[str] = []
    total = 0
    for text in reversed(turn_texts):
        extra = len(text) + (len(_TURN_SEPARATOR) if kept else 0)
        if kept and total + extra > MAX_OUTPUT_CHARS:
            break
        kept.append(text)
        total += extra
    kept.reverse()

    body = _truncate_single(_TURN_SEPARATOR.join(kept))
    if len(kept) < len(turn_texts):
        body = (
            "[earlier turns truncated — full output available in workspace]\n\n"
            + body
        )
    return body


async def _placement(workspace_id: str) -> _Placement:
    if not workspace_id:
        return _Placement()
    from src.server.database.computer import get_computer_for_workspace

    computer = await get_computer_for_workspace(workspace_id) or {}
    return _Placement(
        computer.get("root_dir"),
        tuple(computer.get("previous_root_dirs") or ()),
        computer.get("dir_name"),
        tuple(computer.get("previous_dir_names") or ()),
    )


async def extract_text_from_thread(
    thread_id: str, turns: int = 1, timezone: str = "UTC"
) -> dict[str, Any]:
    """Extract the text each run of a thread committed, one labeled block per run.

    Reads from Redis if the thread is actively running, otherwise reads
    from the database: each settled run's text comes from its turn slice.

    Args:
        thread_id: The conversation thread ID
        turns: How many of the most-recent turns to include. 1 (default) =
            only the latest turn; N > 1 = the last N turns; <= 0 = the full
            thread history. The window applies to the persisted record; while
            a turn is actively streaming, only that live turn is returned.
        timezone: The user's IANA zone, which the run labels state times in.

    Returns:
        Dict with keys: text, status, thread_id, workspace_id
    """
    from src.server.database.conversation.threads_read import (
        get_thread_by_id,
    )

    # Look up thread
    thread = await get_thread_by_id(thread_id)
    if not thread:
        return {
            "text": "",
            "status": "not_found",
            "thread_id": thread_id,
            "workspace_id": "",
        }

    workspace_id = str(thread.get("workspace_id", ""))
    zone = user_zone(timezone)
    where = await _placement(workspace_id)

    # The ledger routes live-vs-settled (v4 2.4): an in_progress row means
    # the turn is still streaming into Redis on SOME worker; anything else
    # reads the finalized turns from the DB.
    from src.server.database.runs import lifecycle as tl_db

    active_run = await tl_db.get_active_run(thread_id)
    if active_run is not None:
        status = "running"
    else:
        status = thread.get("current_status", "unknown")

    # Qualify relative file paths with workspace context so the Chief of Staff
    # (and its frontend) can resolve them across workspaces, then cap length.
    if active_run is not None:
        # The active stream is always a single live turn — read the ledger
        # row's run stream directly. Resolving through local BTM state can
        # pick a retained terminal LocalRunExecution from a prior run on this worker
        # while the live run executes elsewhere (v4 2.4c review F6).
        text = await _extract_from_redis(
            thread_id, str(active_run["conversation_response_id"])
        )
        text = _truncate_single(
            _run_block(
                active_run,
                _qualify_file_paths(text, workspace_id, *where),
                zone,
            )
        )
    else:
        runs = await _runs_to_read(thread_id, turns)
        text = _join_recent_turns([
            _run_block(
                run,
                _qualify_file_paths(run["main_text"], workspace_id, *where),
                zone,
            )
            for run in runs
        ])

    return {
        "text": text,
        "status": status,
        "thread_id": thread_id,
        "workspace_id": workspace_id,
    }


def user_zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception:  # noqa: BLE001 - an unknown zone reads as UTC
        return ZoneInfo("UTC")


def spoken_computer_name(name: str | None) -> str:
    """A computer's name as an agent reads it, and so as it passes the name
    back as ``computer``: on one line, and never cut short."""
    return " ".join((name or "").split()) or "Untitled"


def computer_label(name: str | None, computers: int) -> str | None:
    """What an agent is told a computer is called, out of ``computers`` live ones.

    Which computer a workspace is on tells an agent something only once the
    user has another it could be on, so a user with one sees no computer named.
    """
    if name is None or computers < 2:
        return None
    return spoken_computer_name(name)


def computer_state(
    kind: str | None, status: str | None
) -> Literal["", "stopped", "failed"]:
    """What a call on a computer meets before it runs, as the agent is told.

    "stopped" for a hosted computer that is down or going down, which the call
    starts first. A machine of the user's own reads as nothing there: its
    status is only whether our processes run on it, and reaching it connects.
    "failed" for one in error, which no call starts.
    """
    if status == "error":
        return "failed"
    if kind != "ssh" and status in ("stopped", "stopping"):
        return "stopped"
    return ""


def computer_machine(
    kind: str | None, provider_config: Mapping[str, Any] | None
) -> str:
    """Whose machine a computer is, as a token the agent's context words.

    "" for a computer this deployment hosts. Otherwise the machine is the
    user's ("own"), and the one they work on ("personal") when the desktop
    app's helper paired it; "_mac" follows when it runs macOS. A computer
    added before its system was kept reads as not a Mac.
    """
    if kind != "ssh":
        return ""
    config = provider_config or {}
    owner = "personal" if config.get("helper") else "own"
    return f"{owner}_mac" if config.get("os") == "Darwin" else owner


@dataclass(frozen=True, slots=True)
class PlacedComputer:
    """One of the user's computers, as the Chief of Staff is told of it."""

    name: str
    #: ``computer_machine``'s token: "" for a hosted computer.
    machine: str
    #: Where its workspace folders sit, and where a call on it starts.
    root: str
    is_home: bool
    #: ``computer_state``'s word for it: "" when a call there just runs.
    state: Literal["", "stopped", "failed"] = ""


@dataclass(frozen=True, slots=True)
class ComputerPlacement:
    """Which computer each of the user's workspaces is on, as the Chief of Staff is told.

    Every workspace names its folder, which sits at the root of its computer:
    the Chief of Staff's tools reach any of the user's computers. With more
    than one, each workspace names its computer too, and ``computers`` holds
    them by id in the roster's order, primary first; with one it is empty.
    """

    computers: Mapping[str, PlacedComputer] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "ComputerPlacement":
        return cls(
            {
                str(row["computer_id"]): PlacedComputer(
                    name=name,
                    machine=computer_machine(row.get("kind"), row.get("provider_config")),
                    root=row["root_dir"],
                    is_home=bool(row["is_home"]),
                    # Home's computer is being started for the turn that reads this.
                    state=""
                    if row["is_home"]
                    else computer_state(row.get("kind"), row.get("status")),
                )
                for row in rows
                if (name := computer_label(row["name"], len(rows))) is not None
            }
        )

    def _placed(self, workspace: Mapping[str, Any]) -> PlacedComputer | None:
        return self.computers.get(str(workspace.get("computer_id")))

    def path(self, workspace: Mapping[str, Any]) -> str | None:
        """The folder's whole path when the workspace is on another computer
        than Home's: a call there takes that machine's own paths, and its
        root is not one the agent can guess."""
        placed = self._placed(workspace)
        folder = workspace.get("dir_name")
        if placed is None or placed.is_home or not folder:
            return None
        return f"{placed.root.rstrip('/')}/{folder}"

    def computer(self, workspace: Mapping[str, Any]) -> str | None:
        placed = self._placed(workspace)
        return placed.name if placed else None

    def machine(self, workspace: Mapping[str, Any]) -> str | None:
        """What sets the workspace's computer apart when it is the user's own.

        Said only beside the computer's name: with one computer, Home's own
        block says whose it is.
        """
        placed = self._placed(workspace)
        if placed is None or not placed.machine:
            return None
        if placed.machine.endswith("_mac"):
            return "the user's own Mac"
        return "the user's own machine"


async def read_computer_placement(user_id: str) -> ComputerPlacement:
    # Local import: this module loads at agent import time.
    from src.server.database.computer import get_computer_roster

    return ComputerPlacement.from_rows(await get_computer_roster(user_id))


def _run_block(run: dict[str, Any], text: str, zone: ZoneInfo) -> str:
    """One run's text under a line saying which run it is and how it ended.

    Without the line, the text of an earlier run reads as the newest run's, a
    run that wrote nothing vanishes, and a report-back on an analyst's run
    reads as another attempt at the work.
    """
    started = run.get("created_at")
    if isinstance(started, str):
        started = datetime.fromisoformat(started)
    if isinstance(started, datetime):
        if started.tzinfo is None:
            started = started.replace(tzinfo=dt_timezone.utc)
        parts = [f"Run started {started.astimezone(zone):%Y-%m-%d %H:%M %Z}"]
    else:
        parts = ["Run"]
    status = str(run.get("status") or "")
    parts.append(_RUN_STATUS.get(status, status or "status unknown"))
    if (attempt := run.get("attempt_no") or 1) > 1:
        parts.append(f"attempt {attempt} of the same request")
    metadata = run.get("metadata")
    if isinstance(metadata, dict) and (
        analyst := metadata.get("report_back_ptc_thread_id")
    ):
        parts.append(f"a report-back on analyst thread {analyst}")
    if not text:
        parts.append("no text yet" if status == "in_progress" else "no text")
    label = "[" + ", ".join(parts) + "]"
    return f"{label}\n{text}" if text else label


async def _extract_from_redis(thread_id: str, run_id: str) -> str:
    """Extract text content from Redis SSE event buffer.

    Reads the tail of the per-run Redis Stream
    (``workflow:stream:{tid}:{run_id}``) for the caller-resolved *run_id*
    (the ledger-active row — authoritative on every worker). XREVRANGE
    with COUNT yields the most-recent 500 entries cheaply, then we
    reverse to chronological order to mirror the old RPUSH semantics.
    """
    # Local import to avoid load-order coupling with the server package
    # at agent import time.
    from src.server.services.runs.stream_writer import stream_key
    from src.utils.cache.redis_cache import get_cache_client

    key = stream_key(thread_id, run_id)

    try:
        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or cache.client is None:
            return ""
        entries = await cache.client.xrevrange(key, count=500)
    except Exception as e:
        logger.error(f"Failed to read Redis events for thread {thread_id}: {e}")
        return ""

    chunks: list[str] = []
    # XREVRANGE returns newest first; reverse so chunks concatenate in order.
    for _entry_id, fields in reversed(entries or []):
        raw = fields.get(b"event")
        if raw is None:
            continue
        try:
            raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        except UnicodeDecodeError:
            continue
        parsed = _parse_sse_string(raw_str)
        if parsed is None:
            continue
        event_type, data = parsed
        if (
            event_type == "message_chunk"
            and isinstance(data, dict)
            and data.get("content_type") == "text"
        ):
            content = data.get("content", "")
            if content:
                chunks.append(content)

    return "".join(chunks)


async def _with_main_text(
    thread_id: str, runs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Copies of ``runs`` each carrying ``main_text``, what the turn committed
    (``committed_texts``). A run with no committed text to read has none."""
    if not runs:
        return []
    # Local import: this module loads at agent import time.
    from src.server.services.history.committed import committed_texts

    texts = await committed_texts(
        thread_id,
        {str(run["conversation_response_id"]): run.get("turn_index") for run in runs},
    )
    return [
        {**run, "main_text": texts.get(str(run["conversation_response_id"]), "")}
        for run in runs
    ]


async def _runs_to_read(thread_id: str, turns: int = 1) -> list[dict[str, Any]]:
    """The settled runs to show, oldest -> newest, for the most-recent ``turns`` turns.

    ``turns == 1`` is the newest run, plus the newest earlier run that has
    text when it has none (a tool-only or chart-only turn), so an empty read
    is not mistaken for "the agent produced nothing". ``turns > 1`` reads the
    last ``turns`` turns and ``<= 0`` recent history, clamped to
    ``_MAX_HISTORY_TURNS``; a run without text stays in, labeled as such. Read
    failures propagate so the caller surfaces an error instead of an empty,
    success-looking result.
    """
    from src.server.database.conversation.responses import get_recent_responses_for_thread

    if turns != 1:
        limit = _MAX_HISTORY_TURNS if turns <= 0 else min(turns, _MAX_HISTORY_TURNS)
        return await _with_main_text(
            thread_id, await get_recent_responses_for_thread(thread_id, limit=limit)
        )

    # Hot path is limit=1; a text-less newest run pays one wider read.
    runs = await _with_main_text(
        thread_id, await get_recent_responses_for_thread(thread_id, limit=1)
    )
    if not runs or runs[0]["main_text"]:
        return runs
    window = await _with_main_text(
        thread_id,
        await get_recent_responses_for_thread(
            thread_id, limit=_EMPTY_LATEST_FALLBACK_TURNS
        ),
    )
    if not window:
        return runs
    newest = window[-1]
    if newest["main_text"]:  # a newer run settled between the reads
        return [newest]
    for run in reversed(window[:-1]):
        if run["main_text"]:
            return [run, newest]
    return [newest]
