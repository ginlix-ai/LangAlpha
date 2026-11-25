"""A member's personal memory, served read-only for their turns.

Every path the agent names here is persisted where every member reads it:
the tool call's arguments, the listing a Glob returns, the stub kept of a
read. A file's own name can be as personal as its content, so the route
never shows one. Each file and folder goes by an alias, a keyed digest of
the member, the workspace and the entry's path, a common text extension kept: the same
entry has the same alias on every call and turn, two members' files of one
name have different ones, and without the deployment's key a name cannot be
guessed from its alias. The index, ``memory.md``, keeps its name, which
says nothing of anyone.
"""

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import hmac
import html
import json
import logging
import posixpath
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from html.entities import html5
from typing import Any

from langchain_core.messages import ToolMessage

from ptc_agent.agent.backends import (
    CompositeFilesystemBackend,
    StoreBackend,
    user_memory_namespace,
)
from ptc_agent.agent.backends.text_grep import UnsupportedPattern
from ptc_agent.agent.middleware.member_context.context import MemberContext
from ptc_agent.agent.tools.context_file_policy import AGENT_MD, CappedFile
from ptc_agent.agent.backends.read_window import READ_CLIPPED_NOTE, READ_FULL_WINDOW_NOTE
from ptc_agent.agent.tools.file_ops import DEFAULT_READ_LINES, create_filesystem_tools, read_clipped_marker
from ptc_agent.agent.tools.glob import create_glob_tool, glob_cut_note
from ptc_agent.core.paths import MEMORY_INDEX_FILENAME, SandboxLayout, WorkspaceLayout

logger = logging.getLogger(__name__)

#: Hex digits of an alias: 64 bits, past any collision in one member's memory.
_ALIAS_HEX = 16
#: The extensions kept on an alias, lower-cased: only common text formats,
#: since any other suffix (``notes.AnaLopez``) could carry a name. An image's
#: or a PDF's is not kept: every entry is text in the store, and Read would
#: take such a name for a file to fetch from the computer.
_KEPT_EXTENSIONS = frozenset(
    ".md .markdown .txt .json .jsonl .csv .tsv .yaml .yml .toml .xml .html .htm"
    " .py .js .ts .sql .log .ini .cfg".split()
)

#: The most of one memory file a Read shows, and of a Glob's listing of the
#: memory: a call fills the member's reads back in past what the compaction
#: trigger measures, so the newest alone must fit the budget a call adds.
MEMORY_READ_CHARS = 40_000

#: How long the turn's one read of the member's memory index may take,
#: aliasing included.
INDEX_READ_TIMEOUT_S = 5.0

#: The ``additional_kwargs`` key a stub records its read under (``MemberRead``).
MEMBER_READ_KEY = "member_read"

#: The tools whose result from a member's memory is stored as a stub, with
#: the argument naming the path each reads.
READ_ARG = {"Read": "file_path", "Glob": "path"}


def _digest(key: bytes, member: MemberContext, entry: str) -> str:
    message = "\x00".join((member.user_id, member.workspace_id, entry)).encode()
    return hmac.new(key, message, hashlib.sha256).hexdigest()[:_ALIAS_HEX]


def alias_of(key: bytes, member: MemberContext, path: str) -> str:
    """``path`` (a store key, ``/`` between folders) with every folder and the
    file under their aliases. A folder's is taken over its path with a
    trailing ``/``, so it never equals a file's of the same name."""
    if path == MEMORY_INDEX_FILENAME:
        return path
    parts = path.split("/")
    out = [_digest(key, member, "/".join(parts[: i + 1]) + "/") for i in range(len(parts) - 1)]
    extension = posixpath.splitext(parts[-1])[1]
    kept = extension if extension.lower() in _KEPT_EXTENSIONS else ""
    out.append(_digest(key, member, path) + kept)
    return "/".join(out)


def read_only_error(member: MemberContext) -> str:
    return (
        f"{member.shown_name}'s memory is read-only in a shared workspace. Ask them to save "
        "the change from their own workspace."
    )


@dataclass(frozen=True)
class MemberRead:
    """What a stub records of a read of a member's memory, enough to make it
    again: whose memory, and the tool and arguments that read it."""

    user_id: str
    tool: str
    args: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def of(cls, message: Any) -> MemberRead | None:
        """A stub's record, or None for any other message."""
        if not isinstance(message, ToolMessage):
            return None
        record = (message.additional_kwargs or {}).get(MEMBER_READ_KEY)
        if not isinstance(record, dict):
            return None
        return cls(
            user_id=str(record.get("user_id")),
            tool=str(record.get("tool")),
            args=dict(record.get("args") or {}),
        )

    def as_dict(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "tool": self.tool, "args": dict(self.args)}

    @property
    def key(self) -> str:
        """The read told apart from any other, whatever its call's id."""
        return json.dumps(self.as_dict(), sort_keys=True, default=str)


class MemberMemory(StoreBackend):
    """The member's user-tier memory under ``WorkspaceLayout.MEMBERS_DIR``,
    every entry under its alias, refusing every write.

    Mounted on the main agent's own Read, Write, Edit and Glob alone
    (``MemberMount``): the file mount never serves it, so it is on no disk
    Bash or code could read; no subagent tool is built over it; and no
    search from above reaches it, so a Glob or Grep of the workspace never
    lists it beside the workspace's own files. A name that is no alias, a
    file's real name among them, is not found.
    """

    mountable = False
    refuses_writes = True
    reached_only_inside = True
    #: What one Read shows of a file here, so what a later call fills back in
    #: (``FILL_BUDGET_CHARS``), the newest read included, stays in its budget.
    read_chars = MEMORY_READ_CHARS

    def __init__(self, *, member: MemberContext, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._member = member
        # Alias to store key, for finding the entry a path names: listed once
        # a turn and again on a miss, since the memory is written only from
        # the member's own workspace. Aliasing text lists afresh.
        self._keys: dict[str, str] | None = None
        # Every entry any listing this turn held, kept once it is removed:
        # aliasing takes these, since a read running beside another may find
        # its listing replaced by one that no longer holds what it read.
        self._seen: dict[str, str] = {}

    @property
    def index_path(self) -> str:
        return f"{self.root_prefix}{MEMORY_INDEX_FILENAME}"

    def alias(self, path: str) -> str:
        return alias_of(self._member.alias_key, self._member, path)

    async def keys(self, *, fresh: bool = False) -> Mapping[str, str]:
        """Every entry's alias with its store key."""
        if self._keys is None or fresh:
            # Strict: a listing cut short would leave the entries it missed
            # under their real names; failing, it leaves the memory out.
            items = await self._all_items(strict=True)
            self._keys = {self.alias(str(item.key)): str(item.key) for item in items}
            self._seen.update(self._keys)
        return self._keys

    async def _resolved(self, file_path: str) -> str | None:
        """The store key ``file_path`` names by its alias, or None."""
        normalized = self.normalize_path(file_path)
        if not normalized.startswith(self.root_prefix):
            return None
        wanted = normalized[len(self.root_prefix) :]
        if wanted == MEMORY_INDEX_FILENAME:
            return wanted
        found = (await self.keys()).get(wanted)
        if found is None:
            found = (await self.keys(fresh=True)).get(wanted)
        return found

    async def aread_text(self, file_path: str) -> str | None:
        # Listed just before the read: aliasing what it returns takes this
        # listing with every other (``aliased_text``), so an entry removed
        # after it still goes by its alias.
        await self.keys(fresh=True)
        key = await self._resolved(file_path)
        if key is None:
            return None
        return await super().aread_text(f"{self.root_prefix}{key}")

    async def adownload_file_bytes_capped(self, filepath: str, cap: Any) -> bytes | None:
        # Memory is text in the store; without this the composite would read
        # the folder's path from the disk, where nothing is a member's.
        return None

    async def aglob_paths(self, pattern: str, path: str = ".", *, strict: bool = False) -> list[str]:
        normalized = self.normalize_path(path)
        if not (
            normalized.startswith(self.root_prefix)
            or normalized.rstrip("/") == self.root_prefix.rstrip("/")
        ):
            return []
        subtree = normalized[len(self.root_prefix) :].rstrip("/")
        # Matched on the aliases alone, so a pattern spelling a real name
        # finds nothing.
        return sorted(
            f"{self.root_prefix}{alias}"
            for alias in await self.keys(fresh=True)
            if (not subtree or alias == subtree or alias.startswith(subtree + "/"))
            and (fnmatch.fnmatch(alias, pattern) or fnmatch.fnmatch(posixpath.basename(alias), pattern))
        )

    async def aread_tree(self, path: str) -> dict[str, str]:
        return {}

    async def agrep_rich(self, pattern: str, path: str = ".", **kwargs: Any) -> Any:
        raise UnsupportedPattern("a member's memory is read with Read, not searched")

    async def aliased_text(self, text: str, *, source: str | None = None) -> str:
        """``text`` with every entry's path in it under its alias. A link's
        destination is read whole, as Markdown spells it (escapes, balanced
        parentheses, angle brackets), and resolved against ``source``, the
        entry the text was read from, then as any entry's path or a path's
        tail; one naming no entry is hidden, since its name may be a file the
        member removed. Elsewhere a file's path is taken as a whole token, a
        folder's with a trailing ``/``. One tail two entries share is hidden,
        since no alias would be right. A path through the member's own memory
        folder (``SandboxLayout.MEMORY_USER_DIR``, however rooted) is pointed
        at where it is mounted here. A name is matched as written or
        percent-encoded.

        The entries are listed afresh, after ``text`` was read, and taken with
        every listing before it this turn (``aread_text``'s among them): the
        member may add or remove one from their own workspace mid-turn, and an
        entry missing from the map would leave its real name in the text."""
        folder = self._member.folder
        text = _OWN_FOLDER.sub(folder.rstrip("/"), text)
        await self.keys(fresh=True)
        names = _Names({key: alias for alias, key in self._seen.items() if key != MEMORY_INDEX_FILENAME})
        source_dir = posixpath.dirname(source) if source is not None else None
        text = _links_aliased(text, folder, names, source_dir)
        # A URL leads out: a name in its query or fragment is no entry's.
        urls: list[str] = []

        def held(m: re.Match[str]) -> str:
            urls.append(m.group())
            return f"\x00{len(urls) - 1}\x00"

        text = names.prose(_URL.sub(held, text), folder)
        return re.sub(r"\x00(\d+)\x00", lambda m: urls[int(m.group(1))], text)

    async def source_of(self, file_path: Any) -> str | None:
        """The entry a Read of ``file_path`` returned, for resolving its links:
        found among every listing this turn, so one removed since the read,
        and gone from a listing another read took after it, still is."""
        if not isinstance(file_path, str):
            return None
        normalized = self.normalize_path(file_path)
        if normalized.startswith(self.root_prefix):
            wanted = normalized[len(self.root_prefix) :]
            if wanted == MEMORY_INDEX_FILENAME or wanted in self._seen:
                return self._seen.get(wanted, wanted)
        return await self._resolved(file_path)


class _Names:
    """The entries' paths, every tail of each and every folder, with the
    alias each goes by."""

    def __init__(self, keys: Mapping[str, str]) -> None:
        self.keys = dict(keys)
        split = [(k.split("/"), a.split("/")) for k, a in self.keys.items()]
        self.files = _tails(split, "")
        # Named alone, it is the index, whatever folder holds another.
        self.files.pop(MEMORY_INDEX_FILENAME, None)
        self.folders = _tails(
            ((parts[:depth], shown[:depth]) for parts, shown in split for depth in range(1, len(parts))),
            "/",
        )
        self.whole_folders = {
            "/".join(parts[:depth]) + "/": "/".join(shown[:depth]) + "/"
            for parts, shown in split
            for depth in range(1, len(parts))
        }
        self._patterns: dict[str, re.Pattern[str]] = {}

    def lookup(self, names: Mapping[str, str], written: str) -> str | None:
        """The alias of the name ``written`` spells, as written or
        percent-encoded; hidden when it spells more than one."""
        found = names.get(written)
        if found is not None:
            return found
        spelled = [name for name in names if self._pattern(name).fullmatch(written)]
        if not spelled:
            return None
        return names[spelled[0]] if len(spelled) == 1 else HIDDEN_NAME + ("/" if written.endswith("/") else "")

    def _pattern(self, name: str) -> re.Pattern[str]:
        if (pattern := self._patterns.get(name)) is None:
            pattern = self._patterns[name] = re.compile(_spelled(name))
        return pattern

    def resolve(self, rest: str, *, ups: int | None, source_dir: str | None) -> str | None:
        """What a link's path ``rest`` shows, past its leading ``/``, ``./``
        and ``../`` (``ups`` of those, None for a path from the root): its
        entry's alias, or None when it names none."""
        if not rest:
            return rest
        folder = rest.endswith("/")
        path = rest.rstrip("/")
        if {".", ".."} & set(path.split("/")):
            # Interior ``.`` and ``..`` (``reports/../notes.md``) taken out,
            # each ``..`` left over climbing past the source's folder.
            path, climbed = posixpath.normpath(path), 0
            while path == ".." or path.startswith("../"):
                climbed, path = climbed + 1, path[3:]
            if (ups is None and climbed) or path in ("", "."):
                return None  # above the memory's root, or the folder itself
            shown = self.resolve(
                path + ("/" if folder else ""), ups=None if ups is None else ups + climbed, source_dir=source_dir
            )
            return None if shown is None else "../" * climbed + shown
        segments = path.split("/")
        if source_dir is not None and ups is not None and not {".", ".."} & set(segments):
            base = source_dir.split("/") if source_dir else []
            if ups <= len(base):
                joined = "/".join([*base[: len(base) - ups], path])
                hit = self.lookup(self.whole_folders, joined + "/") if folder else self.lookup(self.keys, joined)
                if hit is None and not folder:
                    hit = self.lookup(self.whole_folders, joined + "/")
                    hit = hit[:-1] if hit is not None else None
                if hit is not None and not hit.startswith(HIDDEN_NAME):
                    shown = "/".join(hit.rstrip("/").split("/")[-len(segments) :])
                    return shown + ("/" if folder else "")
        # The index keeps its name, once no folder beside the source holds one.
        if path == MEMORY_INDEX_FILENAME:
            return rest
        hit = None if folder else self.lookup(self.files, path)
        if hit is None and (bare := self.lookup(self.folders, path + "/")) is not None:
            hit = bare if folder else bare[:-1]
        return hit

    def prose(self, text: str, folder: str) -> str:
        """``text`` with every entry's path written as a whole token, and each
        folder's with a trailing ``/``, under its alias: a file's path first;
        failing that, its longest folder, so a name under a folder that is no
        entry still loses the folder's. The mounted folder, a ``/``, ``./``
        or ``../`` before it is kept."""
        if not self.keys:
            return text

        def either(names: Mapping[str, str]) -> str:
            return "|".join(_spelled(n) for n in sorted(names, key=len, reverse=True))

        token = re.compile(
            r"(?<![\w/.-])(" + re.escape(folder) + r"|/|(?:\.\.?/)*)(?:(" + either(self.files)
            + r")(?![\w/-])" + (r"|(" + either(self.folders) + r")" if self.folders else "") + ")"
        )

        def shown(m: re.Match[str]) -> str:
            names, written = (self.files, m.group(2)) if m.group(2) is not None else (self.folders, m.group(3))
            return m.group(1) + (self.lookup(names, written) or HIDDEN_NAME)

        return token.sub(shown, text)


#: Where a link's destination starts: after an inline link's ``](``, or a
#: reference definition's label (a footnote's is prose), on its line or the
#: next, as Markdown allows, a line of Read's numbered output and a block
#: quote's ``>`` included.
_LINK_START = re.compile(
    r"\]\([ \t]*|^(?:[ \t]*\d+\t)?[ \t]{0,3}(?:>[ \t]?)*\[(?!\^)(?:\\.|[^\]\\\n])+\]:[ \t]*"
    r"(?:\n(?:[ \t]*\d+\t)?[ \t]*(?:>[ \t]*)*)?",
    re.MULTILINE,
)
_SCHEME = re.compile(r"[A-Za-z][\w+.-]*:")
#: A URL in the text, up to what ends it in prose or Markdown.
_URL = re.compile(r"\b[A-Za-z][\w+.-]*://[^\s<>()\[\]\"'`]+")
_ESCAPED = re.compile(r"\\([!-/:-@\[-`{-~])")
_PUNCTUATION = frozenset("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")


#: A link's title after its destination, which is no part of the path.
_TITLE = re.compile(r"(.*?)(?:[ \t]+(?:\"[^\"\n]*\"|'[^'\n]*'|\([^()\n]*\)))?[ \t]*", re.DOTALL)


def _destination(text: str, at: int, *, inline: bool) -> tuple[int, str | None, bool]:
    """The link destination starting at ``at``: where it ends, its text, and
    whether it is in angle brackets. Its text is None where none starts.

    Unbracketed, it runs to the link's closing parenthesis (balanced ones and
    escaped ones inside it), or a reference definition's to the line's end,
    less a title: a name in the memory may hold a space, which Markdown would
    end the destination at, and the rest of the name would stay in sight."""
    if text.startswith("<", at):
        end = at + 1
        while end < len(text) and text[end] not in "<>\n":
            end += 2 if text[end] == "\\" else 1
        if end < len(text) and text[end] == ">":
            return end + 1, text[at + 1 : end], True
        return at, None, False
    end, depth = at, 0
    while end < len(text) and text[end] != "\n":
        ch = text[end]
        if ch == "\\" and end + 1 < len(text) and text[end + 1] in _PUNCTUATION:
            end += 2
            continue
        if inline and ch == ")" and depth == 0:
            break
        depth += {"(": 1, ")": -1}.get(ch, 0) if inline else 0
        end += 1
    destination = _TITLE.fullmatch(text, at, end).group(1)
    return (at + len(destination), destination, False) if destination else (at, None, False)


def _destination_shown(raw: str, base: str, names: _Names, source_dir: str | None) -> str | None:
    """What a link's destination ``raw`` is shown as: its entry's alias, or
    hidden where it names none; None for one leading out, kept as written."""
    # Decoded first: a reference such as ``&#47;`` holds a ``#``.
    path, _, fragment = html.unescape(_ESCAPED.sub(r"\1", raw)).partition("#")
    # A URL, a network path (``//host/...``) or an anchor leads out.
    if not path or _SCHEME.match(path) or path.startswith("//"):
        return None
    if (at := path.find(base)) >= 0:
        lead, rest, ups = path[: at + len(base)], path[at + len(base) :], None
        if rest.startswith("/"):
            lead, rest = lead + "/", rest[1:]
    else:
        lead, rest = re.match(r"(/|(?:\.{1,2}/)*)(.*)", path, re.DOTALL).groups()
        ups = None if lead == "/" else lead.count("../")
    shown = names.resolve(rest, ups=ups, source_dir=source_dir)
    return HIDDEN_NAME if shown is None else lead + shown + (f"#{fragment}" if fragment else "")


#: A raw HTML link's or image's target, quoted.
_HTML_TARGET = re.compile(r"""(?i)(\b(?:href|src)[ \t]*=[ \t]*)(["'])(.*?)\2""")


def _links_aliased(text: str, folder: str, names: _Names, source_dir: str | None) -> str:
    """``text`` with every link's destination under its alias, or hidden
    where it names no entry: the agent would pass that name to Read, in a
    call every member reads. A link out (a URL or an anchor) is kept. A raw
    HTML link's or image's target is read the same way."""
    base = folder.rstrip("/")

    def html_target(m: re.Match[str]) -> str:
        shown = _destination_shown(m.group(3), base, names, source_dir)
        return m.group() if shown is None else f"{m.group(1)}{m.group(2)}{shown}{m.group(2)}"

    text = _HTML_TARGET.sub(html_target, text)
    out: list[str] = []
    done = 0
    for start in _LINK_START.finditer(text):
        if start.end() < done:
            continue
        end, raw, angle = _destination(text, start.end(), inline=start.group().startswith("]"))
        if raw is None:
            continue
        written = _destination_shown(raw, base, names, source_dir)
        if written is None:
            continue
        out += [text[done : start.end()], f"<{written}>" if angle else written]
        done = end
    return "".join(out) + text[done:]


#: The member's memory folder as their own workspace names it, with any root
#: before it: the folder itself, or a path through it.
_OWN_FOLDER = re.compile(
    r"(?<![\w.-])(?:[\w.~-]*/)*" + re.escape(SandboxLayout.MEMORY_USER_DIR) + r"(?![\w-]|\.\w)"
)

def _spelled(name: str) -> str:
    """A pattern for ``name`` in any spelling Markdown reads as it: each
    character plain, percent-encoded (hex in either case), as an HTML
    character reference (``&#47;``, ``&#x2F;``, ``&sol;``), or a punctuation
    mark escaped with a backslash, as in ``taxes\\(2026\\).md``, in prose as
    in a link. The ``/`` between a path's parts is never percent-encoded,
    since an encoded one is part of a name."""

    def referenced(ch: str) -> str:
        point = ord(ch)
        hexed = "".join(f"[{d.lower()}{d}]" if d.isalpha() else d for d in f"{point:X}")
        named = [re.escape(f"&{n}") for n in _ENTITY_NAMES.get(ch, ())]
        return "|".join([f"&#0*{point};", f"&#[xX]0*{hexed};", *named])

    def encoded(ch: str) -> str:
        return "".join(
            "%" + "".join(f"[{d.lower()}{d}]" if d.isalpha() else d for d in f"{b:02X}")
            for b in ch.encode()
        )

    def one(ch: str) -> str:
        if ch == "/":
            return f"(?:/|{referenced(ch)})"
        plain = f"\\\\?{re.escape(ch)}" if ch in _PUNCTUATION else re.escape(ch)
        return f"(?:{plain}|{encoded(ch)}|{referenced(ch)})"

    out, at = [], 0
    while at < len(name):
        # A reference standing for two characters (``&fjlig;``); no second
        # character of one starts another, so pairing left to right misses none.
        if at + 1 < len(name) and (pair := _ENTITY_NAMES.get(name[at : at + 2])):
            named = "|".join(re.escape(f"&{n}") for n in pair)
            out.append(f"(?:{one(name[at])}{one(name[at + 1])}|{named})")
            at += 2
        else:
            out.append(one(name[at]))
            at += 1
    return "".join(out)


#: The named HTML references of each character, and of each pair of them
#: one stands for, as Markdown decodes them (only the forms ending in ``;``).
_ENTITY_NAMES: dict[str, list[str]] = {}
for _name, _chars in html5.items():
    if _name.endswith(";"):
        _ENTITY_NAMES.setdefault(_chars, []).append(_name)


#: What a path's tail that two entries share is shown as.
HIDDEN_NAME = "[a name in their memory]"


def _tails(paths: Iterable[tuple[list[str], list[str]]], end: str) -> dict[str, str]:
    """Each path's every tail (``a/b/c``, ``b/c``, ``c``) with ``end`` after
    it, mapped to the alias's same tail. A whole path keeps its alias over
    another's tail; a tail of two paths, and no whole path, is hidden."""
    whole: dict[str, str] = {}
    tails: dict[str, str] = {}
    for parts, shown in paths:
        whole["/".join(parts) + end] = "/".join(shown) + end
        for i in range(1, len(parts)):
            name, alias = "/".join(parts[i:]) + end, "/".join(shown[i:]) + end
            if tails.get(name, alias) != alias:
                alias = HIDDEN_NAME + end
            tails[name] = alias
    return {**tails, **whole}


class MembersClosed(StoreBackend):
    """``WorkspaceLayout.MEMBERS_DIR`` in a shared workspace's build, where no
    member's memory is on the computer: nothing under it is read from or
    written to the disk, so no file a command or another member left there
    passes for a member's memory. The member whose own context the turn
    carries has their folder routed over it (``MemberMount``), for the main
    agent's own Read and Glob alone."""

    mountable = False
    refuses_writes = True
    reached_only_inside = True

    async def aread_text(self, file_path: str) -> str | None:
        return None

    async def adownload_file_bytes_capped(self, filepath: str, cap: Any) -> bytes | None:
        return None

    async def aglob_paths(self, pattern: str, path: str = ".", *, strict: bool = False) -> list[str]:
        return []

    async def aread_tree(self, path: str) -> dict[str, str]:
        return {}

    async def agrep_rich(self, pattern: str, path: str = ".", **kwargs: Any) -> Any:
        raise UnsupportedPattern("a member's memory is read with Read, not searched")


def close_members(files: Any, *, store: Any, sandbox: Any, layout: WorkspaceLayout) -> Any:
    """``files`` with ``MEMBERS_DIR`` closed (``MembersClosed``), for every
    agent of a shared workspace's turn."""
    closed = MembersClosed(
        store=store,
        namespace_factory=lambda: ("shared_workspace", "no_member_memory"),
        # Ending in ``/``, so a sibling such as ``members-old`` stays the disk's.
        root_prefix=f"{layout.workspace}/{WorkspaceLayout.MEMBERS_DIR}/",
        sandbox_backend=sandbox,
        read_only=True,
        read_only_error=(
            f"Nothing can be written under {WorkspaceLayout.MEMBERS_DIR}/: it holds members' "
            "memory, which is read-only in a shared workspace."
        ),
    )
    if isinstance(files, CompositeFilesystemBackend):
        return files.with_routes([closed])
    return CompositeFilesystemBackend(sandbox=sandbox, routes=[closed])


@dataclass(frozen=True)
class MemberMount:
    """A member's memory as the main agent's file tools reach it: the route,
    the main agent's backend with it mounted, and the tools over that
    backend. Grep is not among them: a match would carry the memory's text
    into a result no stub replaces."""

    member: MemberContext
    memory: MemberMemory
    files: CompositeFilesystemBackend
    read: Any
    write: Any
    edit: Any
    glob: Any

    @property
    def index_path(self) -> str:
        return self.memory.index_path

    @property
    def folder(self) -> str:
        return self.member.folder

    def over(self, tools: Sequence[Any]) -> list[Any]:
        """``tools`` with the main agent's own Read, Write, Edit and Glob in
        place of the ones built without the memory."""
        own = {tool.name: tool for tool in (self.read, self.write, self.edit, self.glob)}
        return [own.get(tool.name, tool) for tool in tools]

    def inside(self, path: Any) -> bool:
        """Whether ``path``, as a tool was given it, names the memory's folder
        or a file in it."""
        if not isinstance(path, str):
            return False
        try:
            return self.files.route_for(path) is self.memory
        except ValueError:  # a path the backend refuses names nothing
            return False

    async def index(self) -> str | None:
        """The index with every file it names under its alias, None when it
        could not be read."""
        async def aliased() -> str | None:
            text = await self.memory.aread_text(self.index_path)
            return await self.memory.aliased_text(text, source=MEMORY_INDEX_FILENAME) if text else text

        try:
            # One deadline for the read and the listing aliasing it takes.
            return await asyncio.wait_for(aliased(), timeout=INDEX_READ_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - a failed read leaves the memory out
            logger.warning("[member_context] memory index read failed", exc_info=True)
            return None

    async def read_again(self, read: MemberRead) -> Any:
        """What ``read`` returns made again through the same tool, every path
        in it under its alias; None where it no longer reads the memory (a
        path the workspace's rename moved off it) or the read fails, which
        leaves the stub standing."""
        tool = {"Read": self.read, "Glob": self.glob}.get(read.tool)
        if tool is None or not self.inside(read.args.get(READ_ARG[read.tool])):
            return None
        content = await tool.coroutine(**dict(read.args))
        if isinstance(content, str) and content.startswith("ERROR"):
            return None
        return await self.aliased(read.tool, read.args, content)

    async def aliased(self, tool: str, args: Mapping[str, Any], content: Any) -> Any:
        """A Read's or Glob's result from the memory, every path in it under
        its alias, a Read's links resolved against the file it read."""
        if not isinstance(content, str):
            return content
        source = await self.memory.source_of(args.get(READ_ARG["Read"])) if tool == "Read" else None
        text = await self.memory.aliased_text(content, source=source)
        if tool == "Glob":
            return _listing_capped(text)
        return _read_capped(text, args)


def _listing_capped(listing: str) -> str:
    """A Glob's aliased listing cut to ``MEMORY_READ_CHARS``: the tool caps
    its paths by count, and each alias can be longer than the name."""
    if len(listing) <= MEMORY_READ_CHARS:
        return listing
    header, _, rest = listing.partition("\n")
    total = int(m.group(1)) if (m := re.match(r"Found (\d+) ", header)) else 0
    # The tool's own note, if it cut the listing too, is written again.
    paths = [line for line in rest.split("\n") if line and not line.startswith("[showing first ")]
    room = MEMORY_READ_CHARS - len(header) - len(glob_cut_note(len(paths), max(total, len(paths))))
    kept: list[str] = []
    for path in paths:
        room -= len(path) + 1
        if room < 0:
            break
        kept.append(path)
    return "\n".join([header, *kept]) + glob_cut_note(len(kept), max(total, len(paths)))


def _read_capped(text: str, args: Mapping[str, Any]) -> str:
    """A Read's aliased result cut again to ``MEMORY_READ_CHARS`` at whole
    lines, under Read's own note: Read capped the file as stored, and each
    alias can be much longer than the name it stands for."""
    if len(text) <= MEMORY_READ_CHARS:
        return text
    # The note Read itself added, if any, counted lines before aliasing.
    for note in (READ_CLIPPED_NOTE, READ_FULL_WINDOW_NOTE):
        text = text.split(note, 1)[0]
    lines = text.split("\n")
    numbered = [m for line in lines if (m := _NUMBERED.match(line))]
    if not numbered:
        return text[:MEMORY_READ_CHARS]
    file_path = str(args.get(READ_ARG["Read"]) or "")
    max_lines = int(args.get("limit") or DEFAULT_READ_LINES)
    first = int(numbered[0].group(1))
    budget = MEMORY_READ_CHARS - len(
        read_clipped_marker(MEMORY_READ_CHARS, first, first + len(lines), file_path, max_lines)
    )
    kept: list[str] = []
    room = budget
    for line in lines:
        room -= len(line) + 1
        if room < 0:
            break
        kept.append(line)
    # One line past the cap alone is cut inside it.
    kept = kept or [lines[0][:budget]]
    last = next((int(m.group(1)) for line in reversed(kept) if (m := _NUMBERED.match(line))), first)
    return "\n".join(kept) + read_clipped_marker(MEMORY_READ_CHARS, first, last, file_path, max_lines)


#: A line of Read's numbered output.
_NUMBERED = re.compile(r"\s*(\d+)\t")


def member_memory(
    member: MemberContext,
    *,
    store: Any,
    files: Any,
    sandbox: Any,
    layout: WorkspaceLayout,
    operation_callback: Any = None,
    agent_md: CappedFile = AGENT_MD,
) -> MemberMount:
    """``member``'s memory, rooted at their folder in the turn's workspace and
    mounted over ``files``, the backend every agent of the turn shares."""
    memory = MemberMemory(
        member=member,
        store=store,
        namespace_factory=lambda: user_memory_namespace(member.user_id),
        root_prefix=f"{layout.workspace}/{member.folder}",
        sandbox_backend=sandbox,
        read_only=True,
        read_only_error=read_only_error(member),
    )
    main = (
        files.with_routes([memory])
        if isinstance(files, CompositeFilesystemBackend)
        else CompositeFilesystemBackend(sandbox=sandbox, routes=[memory])
    )
    read, write, edit = create_filesystem_tools(
        main, operation_callback, record_creators=True, agent_md=agent_md
    )
    return MemberMount(
        member=member,
        memory=memory,
        files=main,
        read=read,
        write=write,
        edit=edit,
        glob=create_glob_tool(main),
    )
