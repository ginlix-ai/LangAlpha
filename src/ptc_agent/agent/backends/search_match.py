"""Which files the sandbox's Glob and Grep reach, decided for files no disk holds.

A store route's files have to be found exactly as files on disk are: Glob's
pattern as ``glob_runtime.find`` walks a folder, and Grep's ``glob`` and
``type`` as the sandbox's ripgrep (14.1) walks one. Each pattern is read in
its own dialect into tokens, and the tokens are compiled into an automaton
that is run a state set at a time, so a match costs at most the pattern's
length times the path's. A backtracking ``re`` over a translated glob takes
exponential time on patterns as short as ``**/a/**/a/...``, and re-spelling
one dialect as the other loses where its tokens end.
"""

from __future__ import annotations

import fnmatch
import posixpath
import re
import threading
from bisect import bisect_right
from collections.abc import Callable, Iterable
from functools import lru_cache
from pathlib import Path

from ptc_agent.core.paths import AGENT_HISTORY_DIRS, ALWAYS_HIDDEN_DIR_NAMES
from ptc_agent.core.sandbox.glob_runtime import agent_glob_pattern

# What ``aglob_files`` hands the disk glob for the agent's own Glob, which
# always hides history.
GLOB_EXCLUDED_DIRS = sorted(ALWAYS_HIDDEN_DIR_NAMES | {"__pycache__"})
GLOB_HISTORY_CHILDREN: dict[str, list[str]] = {}
for _history_dir in AGENT_HISTORY_DIRS:
    _parent, _name = posixpath.split(_history_dir)
    GLOB_HISTORY_CHILDREN.setdefault(posixpath.basename(_parent), []).append(_name)

_TYPES_FILE = Path(__file__).with_name("rg_types.txt")


class RgRefused(ValueError):
    """A ``--glob`` or ``--type`` rg rejects, so that it searches nothing."""


def _bytes(text: str) -> bytes:
    return text.encode("utf-8", "surrogatepass")


def _lexical(path: str) -> str:
    """The absolute ``path`` with ``.``, ``..`` and doubled ``/`` resolved
    by name, as the kernel resolves them where no link is on the way."""
    names: list[str] = []
    for name in path.split("/"):
        if name == "..":
            if names:
                names.pop()
        elif name not in ("", "."):
            names.append(name)
    return "/" + "/".join(names)


def _under(path: str, directory: str) -> bool:
    if directory == "/":
        return path.startswith("/") and path != "/"
    return path.startswith(directory + "/")


# --- ripgrep: globset's tokens, compiled as globset compiles them ------------

_LIT, _ANY, _STAR, _RPREFIX, _RSUFFIX, _RZOM, _CLASS, _ALT = range(8)

_ALL = frozenset(range(256))
_SLASH = frozenset(b"/")
_NOT_SLASH = _ALL - _SLASH


class _RgParser:
    """globset 14.1's ``Parser``, step for step, so that what a ``*`` or a
    ``**`` becomes rests on the same raw neighbours rg looks at."""

    def __init__(self, glob: str) -> None:
        self._glob = glob
        self._at = 0
        self._prev: str | None = None
        self._cur: str | None = None
        self._stack: list[list[tuple]] = [[]]

    def _bump(self) -> str | None:
        self._prev = self._cur
        if self._at < len(self._glob):
            self._cur = self._glob[self._at]
            self._at += 1
        else:
            self._cur = None
        return self._cur

    def _peek(self) -> str | None:
        return self._glob[self._at] if self._at < len(self._glob) else None

    def _push(self, token: tuple) -> None:
        self._stack[-1].append(token)

    def parse(self) -> list[tuple]:
        while (char := self._bump()) is not None:
            if char == "?":
                self._push((_ANY,))
            elif char == "*":
                self._star()
            elif char == "[":
                self._class()
            elif char == "{":
                if len(self._stack) > 1:
                    raise RgRefused("nested alternate groups are not allowed")
                self._stack.append([])
            elif char == "}":
                alternatives = []
                while len(self._stack) >= 2:
                    alternatives.append(self._stack.pop())
                self._push((_ALT, alternatives))
            elif char == "," and len(self._stack) > 1:
                self._stack.append([])
            elif char == "\\":
                escaped = self._bump()
                if escaped is None:
                    raise RgRefused("dangling '\\'")
                self._push((_LIT, escaped))
            else:
                self._push((_LIT, char))
        if len(self._stack) > 1:
            raise RgRefused("unclosed alternate group")
        return self._stack[0]

    def _star(self) -> None:
        prev = self._prev
        if self._peek() != "*":
            self._push((_STAR,))
            return
        self._bump()
        if not self._stack[-1]:
            if self._peek() not in (None, "/"):
                self._push((_STAR,))
                self._push((_STAR,))
            else:
                self._push((_RPREFIX,))
                self._bump()
            return
        if prev != "/" and (len(self._stack) <= 1 or prev not in (",", "{")):
            self._push((_STAR,))
            self._push((_STAR,))
            return
        following = self._peek()
        if following is None:
            self._bump()
            is_suffix = True
        elif following in (",", "}") and len(self._stack) >= 2:
            is_suffix = True
        elif following == "/":
            self._bump()
            is_suffix = False
        else:
            self._push((_STAR,))
            self._push((_STAR,))
            return
        popped = self._stack[-1].pop()
        if popped[0] in (_RPREFIX, _RSUFFIX):
            self._push(popped)
        else:
            self._push((_RSUFFIX,) if is_suffix else (_RZOM,))

    def _class(self) -> None:
        ranges: list[list[str]] = []
        negated = self._peek() in ("!", "^")
        if negated:
            self._bump()
        first, in_range = True, False
        while True:
            char = self._bump()
            if char is None:
                raise RgRefused("unclosed character class")
            if char == "]" and not first:
                break
            if char == "]" or (char == "-" and first):
                ranges.append([char, char])
            elif char == "-" and not in_range:
                in_range = True
            elif in_range:
                # A range a later `-` extends, as `[a-c-e]` reads a-e.
                ranges[-1][1] = char
                if ranges[-1][1] < ranges[-1][0]:
                    raise RgRefused(f"invalid range {ranges[-1][0]}-{char}")
                in_range = False
            else:
                ranges.append([char, char])
            first = False
        if in_range:
            ranges.append(["-", "-"])
        self._push((_CLASS, _class_bytes(negated, ranges)))


def _class_bytes(negated: bool, ranges: list[list[str]]) -> frozenset[int]:
    """The bytes a class matches. rg matches bytes, and spells a character
    outside ASCII by its UTF-8 bytes inside the class too, so ``[é]`` is
    either of two bytes and a range runs from the last byte of its low end
    to the first of its high end."""
    members: set[int] = set()
    for low, high in ranges:
        low_bytes, high_bytes = _bytes(low), _bytes(high)
        if low == high:
            members.update(low_bytes)
        else:
            members.update(low_bytes[:-1])
            members.update(range(low_bytes[-1], high_bytes[0] + 1))
            members.update(high_bytes[1:])
    return _ALL - members if negated else frozenset(members)


# A step is remembered until the sets an automaton holds pass this many
# bits, or its steps this many, when it starts over: a glob of thousands of
# alternatives then keeps a few MB however many paths it meets.
_MAX_HELD_BITS = 1 << 23
_MAX_CACHED_STEPS = 4096

_SLASH_BYTE = ord("/")


def _mask(positions: Iterable[int]) -> int:
    """The set of ``positions``, built in one pass rather than a bit at a
    time, which would copy the growing set for each."""
    positions = list(positions)
    if not positions:
        return 0
    bits = bytearray((max(positions) >> 3) + 1)
    for position in positions:
        bits[position >> 3] |= 1 << (position & 7)
    return int.from_bytes(bits, "little")


class _Nfa:
    """A Thompson automaton over bytes, run a state set at a time.

    A set is an int with a bit per state, and the states are laid out so
    that every edge but a loop leads forward, all that consume a byte to the
    next state: a step is a few shifts and masks over the whole set, which
    costs about what one state does however many alternatives a glob holds.
    Each set met is numbered and each step remembered by number, so the
    walk, which matches every directory on the way down to a file, reruns a
    shared prefix by lookups alone. A run holds a lock, since the automaton
    of a type is shared and may be run off the event loop.
    """

    def __init__(self) -> None:
        self._count = 1
        self.accept = 0
        self._moves: list[tuple[int, frozenset[int]]] = []
        self._loops_any: list[int] = []
        self._loops_not_slash: list[int] = []
        self._skips: list[int] = []
        self._hops: list[int] = []
        # Each group of alternatives: where it is entered, the first state
        # of each alternative, the last, the state they all lead to, and
        # whether one alternative takes no byte.
        self._groups: list[tuple[int, list[int], list[int], int, bool]] = []
        self._lock = threading.Lock()

    def _state(self) -> int:
        self._count += 1
        return self._count - 1

    def emit(self, tokens: list[tuple], at: int) -> int:
        """The tokens' states after ``at``, which is the last state laid out
        so far, as the state each returns is."""
        for token in tokens:
            kind = token[0]
            if kind in (_LIT, _ANY, _CLASS):
                if kind == _LIT:
                    steps = [frozenset((byte,)) for byte in _bytes(token[1])]
                else:
                    steps = [_NOT_SLASH if kind == _ANY else token[1]]
                for over in steps:
                    self._moves.append((at, over))
                    at = self._state()
            elif kind == _STAR:
                self._skips.append(at)
                at = self._state()
                self._loops_not_slash.append(at)
            elif kind == _RPREFIX:
                # (?:/?|.*/), its `/` alone being `.*/` with nothing before.
                self._skips.append(at)
                self._hops.append(at)
                anything = self._state()
                self._loops_any.append(anything)
                self._moves.append((anything, _SLASH))
                at = self._state()
            elif kind == _RSUFFIX:
                # /.*
                self._moves.append((at, _SLASH))
                slash = self._state()
                self._skips.append(slash)
                at = self._state()
                self._loops_any.append(at)
            elif kind == _RZOM:
                # (?:/|/.*/)
                self._moves.append((at, _SLASH))
                slash = self._state()
                self._skips.append(slash)
                self._hops.append(slash)
                anything = self._state()
                self._loops_any.append(anything)
                self._moves.append((anything, _SLASH))
                at = self._state()
            else:
                # globset drops an empty alternative, and a group left with
                # none matches the empty string.
                alternatives = [alt for alt in token[1] if alt]
                if alternatives:
                    starts, ends = [], []
                    for alternative in alternatives:
                        starts.append(self._state())
                        ends.append(self.emit(alternative, starts[-1]))
                    at_end = self._state()
                    self._groups.append(
                        (at, starts, ends, at_end, any(map(_nullable, alternatives)))
                    )
                    at = at_end
        return at

    def compile(self) -> None:
        """The masks a step reads, by the byte it takes."""
        by_class: dict[frozenset[int], list[int]] = {}
        for at, over in self._moves:
            by_class.setdefault(over, []).append(at)
        # A class of most bytes is stored by the few it leaves out.
        narrow, holes, wide = [0] * 256, [0] * 256, 0
        for over, ats in by_class.items():
            mask = _mask(ats)
            if len(over) <= 128:
                for byte in over:
                    narrow[byte] |= mask
            else:
                wide |= mask
                for byte in _ALL - over:
                    holes[byte] |= mask
        shared: dict[int, int] = {}
        self._move = [
            shared.setdefault(mask, mask)
            for mask in (narrow[byte] | (wide & ~holes[byte]) for byte in range(256))
        ]
        self._loop_slash = _mask(self._loops_any)
        self._loop = self._loop_slash | _mask(self._loops_not_slash)
        self._skip = _mask(self._skips)
        self._hop = _mask(self._hops)
        # A group's alternatives sit between where it is entered and where
        # they lead, so its own states are held shifted down to its first.
        # One that may take no byte leads past itself at once, so a run of
        # them is crossed in one pass rather than one pass each.
        self._entries = {
            at: (
                starts[0],
                _mask(s - starts[0] for s in starts)
                | (1 << (joined - starts[0]) if nullable else 0),
            )
            for at, starts, _, joined, nullable in self._groups
        }
        self._entered = _mask(self._entries)
        self._firsts = [group[1][0] for group in self._groups]
        self._lasts = [
            _mask(e - starts[0] for e in ends) for _, starts, ends, _, _ in self._groups
        ]
        self._joins = [group[3] for group in self._groups]
        self._left = _mask(e for group in self._groups for e in group[2])
        del self._moves, self._loops_any, self._loops_not_slash, self._skips, self._hops
        start = self._closed(1)
        self._sets = [0, start]
        self._ids = {0: 0, start: 1}
        self._held = start.bit_length()
        self._steps: dict[int, int] = {}

    def _closed(self, states: int) -> int:
        """``states`` with every state an empty string reaches from them.
        Each group is entered and left once a call, its entry and its ends
        struck from what is still to cross."""
        skip, hop = self._skip, self._hop
        entries, ends = self._entered, self._left
        while True:
            before = states
            # A run of states each passing to the next: adding the run to
            # the states in it carries a bit from the first one set to the
            # state past the run, clearing what it passes, which the xor
            # sets again.
            run = states & skip
            if run:
                states |= (run + skip) ^ skip
            hopped = states & hop
            if hopped:
                states |= hopped << 2
            entered = states & entries
            while entered:
                low = entered & -entered
                first, starts = self._entries[low.bit_length() - 1]
                states |= starts << first
                entries ^= low
                entered = states & entries
            left = states & ends
            while left:
                group = bisect_right(self._firsts, (left & -left).bit_length() - 1) - 1
                states |= 1 << self._joins[group]
                ends &= ~(self._lasts[group] << self._firsts[group])
                left = states & ends
            if states == before:
                return states

    def _number(self, states: int) -> int:
        number = self._ids.get(states)
        if number is None:
            number = len(self._sets)
            self._sets.append(states)
            self._ids[states] = number
            self._held += states.bit_length()
        return number

    def run(self, data: bytes) -> int:
        """The states ``data`` leaves the automaton in, as a set; 0 once it
        fails."""
        with self._lock:
            number = 1
            for byte in data:
                key = number << 8 | byte
                stepped = self._steps.get(key)
                if stepped is None:
                    states = self._sets[number]
                    if (
                        len(self._steps) >= _MAX_CACHED_STEPS
                        or self._held >= _MAX_HELD_BITS
                    ):
                        # Start over, keeping the empty set and the start,
                        # whose numbers a step taken before may hold.
                        self._steps.clear()
                        self._ids = {0: 0, self._sets[1]: 1}
                        del self._sets[2:]
                        self._held = self._sets[1].bit_length()
                        number = self._number(states)
                        key = number << 8 | byte
                    loop = self._loop_slash if byte == _SLASH_BYTE else self._loop
                    moved = (states & loop) | ((states & self._move[byte]) << 1)
                    stepped = self._number(self._closed(moved) if moved else 0)
                    self._steps[key] = stepped
                if not stepped:
                    return 0
                number = stepped
            return self._sets[number]

    def matches(self, data: bytes) -> bool:
        return bool(self.run(data) >> self.accept & 1)


def _nullable(tokens: list[tuple]) -> bool:
    """Whether ``tokens`` match the empty string."""
    for token in tokens:
        if token[0] == _ALT:
            alternatives = [alt for alt in token[1] if alt]
            if alternatives and not any(map(_nullable, alternatives)):
                return False
        elif token[0] not in (_STAR, _RPREFIX):
            return False
    return True


def _rg_nfa(tokens: list[tuple]) -> _Nfa:
    nfa = _Nfa()
    if tokens == [(_RPREFIX,)]:
        # globset matches a glob of `**` alone as `.*`.
        nfa._loops_any.append(0)
    else:
        nfa.accept = nfa.emit(tokens, 0)
    nfa.compile()
    return nfa


def _literal_text(tokens: list[tuple]) -> str | None:
    if not tokens or any(token[0] != _LIT for token in tokens):
        return None
    return "".join(token[1] for token in tokens)


def _strategy(tokens: list[tuple]) -> tuple[str, bytes | tuple[bytes, bool]] | None:
    """How globset matches a glob compiled with ``literal_separator``, when
    not by its regex: the strategies it picks, in its order, which differ
    from the regex at the edges (``foo.`` as a whole name matches nothing)."""
    kinds = [token[0] for token in tokens]
    # The basename-literal strategy: `**/` and a literal with no `/`.
    if kinds and kinds[0] == _RPREFIX and len(tokens) > 1:
        rest = tokens[1:]
        if all(token[0] == _LIT and token[1] != "/" for token in rest):
            return "basename", _bytes("".join(token[1] for token in rest))
    literal = _literal_text(tokens)
    if literal is not None:
        return "literal", _bytes(literal)
    # The extension strategy: `**/*.ext`.
    if kinds[:3] == [_RPREFIX, _STAR, _LIT] and tokens[2][1] == ".":
        ext = _literal_text(tokens[3:]) if len(tokens) > 3 else ""
        if ext is not None and "." not in ext and "/" not in ext:
            return "ext", _bytes("." + ext)
    # The prefix strategy: a literal and, at most, `/**` after it.
    if kinds and kinds[-1] != _STAR:
        body = tokens[:-1] if kinds[-1] == _RSUFFIX else tokens
        prefix = _literal_text(body)
        if prefix is not None or (not body and kinds[-1] == _RSUFFIX):
            prefix = (prefix or "") + ("/" if kinds[-1] == _RSUFFIX else "")
            if prefix:
                return "prefix", _bytes(prefix)
    # The suffix strategy: `**/` and a literal, holding a `/`.
    if kinds and kinds[0] == _RPREFIX and len(tokens) > 1 and kinds[1] != _STAR:
        component = kinds[1] == _LIT
        suffix = _literal_text(tokens[1:])
        if suffix is not None:
            suffix = ("/" if component else "") + suffix
            if suffix and suffix != "/":
                return "suffix", (_bytes(suffix), component)
    # The required-extension strategy: whatever ends in a literal `.ext`.
    ext_chars: list[str] = []
    for token in reversed(tokens):
        if token[0] != _LIT or token[1] == "/":
            return None
        ext_chars.append(token[1])
        if token[1] == ".":
            return "required", _bytes("".join(reversed(ext_chars)))
    return None


def _candidate(path: bytes) -> tuple[bytes, bytes]:
    """globset's ``Candidate``: the base name, empty when ``path`` ends in a
    dot, and its extension from the last dot."""
    if not path or path.endswith(b"."):
        return b"", b""
    base = path[path.rfind(b"/") + 1 :]
    dot = base.rfind(b".")
    return base, base[dot:] if dot >= 0 else b""


class _GlobSet:
    """globset's ``GlobSet``: whether any of its globs matches a path."""

    def __init__(
        self,
        globs: Iterable[list[tuple]],
        automaton: Callable[[list[tuple]], _Nfa] = _rg_nfa,
    ) -> None:
        self._literals: set[bytes] = set()
        self._basenames: set[bytes] = set()
        self._exts: set[bytes] = set()
        self._prefixes: list[bytes] = []
        self._suffixes: list[bytes] = []
        self._required: dict[bytes, list[_Nfa]] = {}
        self._regexes: list[_Nfa] = []
        self.empty = True
        for tokens in globs:
            self.empty = False
            strategy = _strategy(tokens)
            if strategy is None:
                self._regexes.append(automaton(tokens))
                continue
            kind, value = strategy
            if kind == "basename":
                self._basenames.add(value)
            elif kind == "literal":
                self._literals.add(value)
            elif kind == "ext":
                self._exts.add(value)
            elif kind == "prefix":
                self._prefixes.append(value)
            elif kind == "suffix":
                suffix, component = value
                if component:
                    self._literals.add(suffix[1:])
                self._suffixes.append(suffix)
            else:
                self._required.setdefault(value, []).append(automaton(tokens))

    def is_match(self, path: bytes) -> bool:
        base, ext = _candidate(path)
        return bool(
            (ext and ext in self._exts)
            or (base and base in self._basenames)
            or path in self._literals
            or any(path.endswith(suffix) for suffix in self._suffixes)
            or any(path.startswith(prefix) for prefix in self._prefixes)
            or (ext and any(nfa.matches(path) for nfa in self._required.get(ext, ())))
            or any(nfa.matches(path) for nfa in self._regexes)
        )


def _rg_tokens(glob: str) -> list[tuple]:
    return _RgParser(glob).parse()


# The whitespace Rust's `trim_end` drops, which a `--glob` loses at its end:
# Unicode's White_Space, which unlike `str.isspace` leaves out \x1c-\x1f.
_RUST_SPACE = (
    "\t\n\x0b\x0c\r \x85\xa0\u1680"
    + "".join(map(chr, range(0x2000, 0x200B)))
    + "\u2028\u2029\u202f\u205f\u3000"
)

_WHITELIST, _IGNORE = "whitelist", "ignore"


class _RgOverride:
    """One ``--glob``, read as ripgrep's override layer reads it: a line of
    a gitignore, matched against a path relative to rg's directory, with
    every verdict inverted."""

    def __init__(self, line: str) -> None:
        self.set: _GlobSet | None = None
        self.ignores = self.only_dir = False
        if line.startswith("#"):
            return
        if not line.endswith("\\ "):
            line = line.rstrip(_RUST_SPACE)
        if not line:
            return
        absolute = False
        if line.startswith(("\\!", "\\#")):
            line = line[1:]
        else:
            if line.startswith("!"):
                self.ignores, line = True, line[1:]
            if line.startswith("/"):
                absolute, line = True, line[1:]
        if line.endswith("/"):
            self.only_dir, line = True, line[:-1]
            if line.endswith("\\"):
                line = line[:-1]
        if (
            not absolute
            and "/" not in line
            and not (line.startswith("**/") or line == "**")
        ):
            line = f"**/{line}"
        if line.endswith("/**"):
            line += "/*"
        tokens = _rg_tokens(line)
        # One automaton serves both the match and ``may_match_below``, so the
        # steps one takes the other finds taken.
        self._nfa = _rg_nfa(tokens)
        self.set = _GlobSet([tokens], lambda _: self._nfa)

    def matched(self, path: bytes, *, is_dir: bool) -> str | None:
        if self.set is None:
            return None
        if (not self.only_dir or is_dir) and self.set.is_match(path):
            return _IGNORE if self.ignores else _WHITELIST
        # A file a whitelist glob does not match is ignored.
        if not self.ignores and not is_dir:
            return _IGNORE
        return None

    def may_match_below(self, directory: bytes) -> bool:
        """Whether a path below ``directory`` could match, by the regex,
        which every strategy of the glob's matches a subset of."""
        return bool(self._nfa.run(directory + b"/" if directory else b""))


@lru_cache(maxsize=1)
def _rg_type_globs() -> dict[str, tuple[str, ...]]:
    types: dict[str, tuple[str, ...]] = {}
    for line in _TYPES_FILE.read_text().splitlines():
        if line and not line.startswith("#"):
            name, _, globs = line.partition(": ")
            types[name] = tuple(globs.split(", "))
    return types


@lru_cache(maxsize=64)
def _rg_type(name: str) -> _GlobSet:
    """The file names ``--type name`` selects, ``all`` selecting every type."""
    types = _rg_type_globs()
    if name == "all":
        globs = [glob for selected in types.values() for glob in selected]
    elif name in types:
        globs = list(types[name])
    else:
        raise RgRefused(f"unrecognized file type: {name}")
    return _GlobSet(_rg_tokens(glob) for glob in globs)


def _rg_file_name(path: bytes) -> bytes | None:
    """ignore's ``file_name``: none for a path that ends in a dot, so such a
    name is never hidden and no type selects it."""
    if not path or path.endswith(b"."):
        return None
    return path[path.rfind(b"/") + 1 :]


class RgWalk:
    """A Grep's ``glob`` and ``type``, read as the sandbox's ripgrep reads
    them for a walk run in ``cwd``.

    Below the path it is given, rg asks of each entry, in this order: the
    filter, which decides when it matches and ignores any file it does not
    when it is a plain glob; then the type, which selects files by name;
    then whether the name starts with a dot, which skips it. A directory it
    skips takes everything inside with it. Raises ``RgRefused`` where rg
    rejects the filter or the type.
    """

    def __init__(self, glob: str | None, file_type: str | None, cwd: str) -> None:
        for given in (glob, file_type):
            try:
                (given or "").encode()
            except UnicodeEncodeError as exc:
                # No command line carries it, so rg never runs.
                raise RgRefused(str(exc)) from exc
        self._override = _RgOverride(glob) if glob else None
        self._types = _rg_type(file_type) if file_type else None
        if self._types is not None and self._types.empty:
            self._types = None
        self._cwd = _bytes(cwd)

    def _relative(self, path: bytes) -> bytes:
        """``path`` as rg matches a filter against it: relative to its
        directory, cut as bytes, or whole where it is outside."""
        if self._cwd != b"." and b"/" in path and path.startswith(self._cwd):
            path = path[len(self._cwd) :]
            if path.startswith(b"/"):
                path = path[1:]
        return path

    def _skips(self, path: str, *, is_dir: bool) -> bool:
        raw = _bytes(path)
        verdict = None
        if self._override is not None:
            verdict = self._override.matched(self._relative(raw), is_dir=is_dir)
        if verdict is None and self._types is not None and not is_dir:
            name = _rg_file_name(raw)
            verdict = _WHITELIST if name and self._types.is_match(name) else _IGNORE
        if verdict is None:
            name = _rg_file_name(raw)
            return name is not None and name.startswith(b".")
        return verdict == _IGNORE

    def _entries(self, start: str, spelled: str) -> list[str]:
        at = start.rstrip("/")
        entries = []
        for name in spelled.split("/"):
            if name:
                at = f"{at}/{name}"
                entries.append(at)
        return entries

    def finds(self, start: str, spelled: str) -> bool:
        """Whether a walk from ``start`` searches the file spelled
        ``spelled`` from there. The file ``start`` names itself, spelled
        ``""``, is searched whatever its name, the filter or the type."""
        entries = self._entries(start, spelled)
        return not any(
            self._skips(entry, is_dir=depth < len(entries) - 1)
            for depth, entry in enumerate(entries)
        )

    def reaches(self, start: str, spelled: str) -> bool:
        """Whether a walk from ``start`` could search a file below the
        directory spelled ``spelled`` from there."""
        entries = self._entries(start, spelled)
        if any(self._skips(entry, is_dir=True) for entry in entries):
            return False
        override = self._override
        if override is None or override.set is None or override.ignores:
            return True
        if override.only_dir:
            return False
        directory = _bytes(entries[-1] if entries else start.rstrip("/"))
        if self._cwd.startswith(directory.rstrip(b"/") + b"/"):
            # rg's directory is below it, and paths there are matched from
            # it rather than whole.
            return True
        return override.may_match_below(self._relative(directory))


# --- the disk glob: fnmatch per name, one name at a time ---------------------


def _magic(segment: str) -> bool:
    return any(char in segment for char in "*?[")


# A class or a pattern longer than this is matched without a cache, so
# patterns that are long and all different cannot fill one.
_CACHED_CLASS_CHARS = 64


@lru_cache(maxsize=256)
def _fn_class_cached(text: str) -> Callable[[str], object]:
    return re.compile(fnmatch.translate(text)).match


def _fn_class(text: str) -> Callable[[str], object]:
    """The one-character match of a class, read by fnmatch itself, whose
    rules for ranges, ``!`` and a ``]`` first the disk glob goes by."""
    if len(text) > _CACHED_CLASS_CHARS:
        return re.compile(fnmatch.translate(text)).match
    return _fn_class_cached(text)


def _fn_tokens(segment: str) -> tuple[tuple, ...]:
    """``segment`` cut where fnmatch cuts it: ``*``, ``?``, a class, or a
    character, a ``[`` left open being a character. Where each ``]`` is
    comes from one pass, since a ``[`` left open would otherwise look for
    one to the end of the segment, which a run of them makes quadratic."""
    end = len(segment)
    closes = [end] * (end + 1)
    for at in range(end - 1, -1, -1):
        closes[at] = at if segment[at] == "]" else closes[at + 1]
    tokens: list[tuple] = []
    at = 0
    while at < end:
        char = segment[at]
        at += 1
        if char == "*":
            if not tokens or tokens[-1][0] != _STAR:
                tokens.append((_STAR,))
        elif char == "?":
            tokens.append((_ANY,))
        elif char == "[":
            close = at
            if close < end and segment[close] == "!":
                close += 1
            if close < end and segment[close] == "]":
                close += 1
            close = closes[close]
            if close >= end:
                tokens.append((_LIT, "["))
            else:
                tokens.append((_CLASS, _fn_class(segment[at - 1 : close + 1])))
                at = close + 1
        else:
            tokens.append((_LIT, char))
    return tuple(tokens)


def _fn_match(tokens: tuple[tuple, ...], name: str) -> bool:
    """``fnmatch.fnmatchcase(name, segment)`` for ``segment``'s tokens, one
    character at a time."""
    end = len(tokens)

    def settle(states: Iterable[int]) -> set[int]:
        out = set()
        for state in states:
            out.add(state)
            if state < end and tokens[state][0] == _STAR:
                out.add(state + 1)
        return out

    states = settle((0,))
    for char in name:
        nxt = set()
        for state in states:
            if state == end:
                continue
            kind = tokens[state][0]
            if kind == _STAR:
                nxt.add(state)
            elif (
                kind == _ANY
                or (kind == _LIT and tokens[state][1] == char)
                or (kind == _CLASS and tokens[state][1](char))
            ):
                nxt.add(state + 1)
        if not nxt:
            return False
        states = settle(nxt)
    return end in states


def _settle(segments: tuple[str, ...], states: Iterable[int]) -> set[int]:
    """``states`` with every segment that names no further entry passed: a
    ``.``, and a ``**`` that matches no directory."""
    last = len(segments) - 1
    out: set[int] = set()
    for state in states:
        # A state met already has had the segments after it passed.
        while state not in out:
            out.add(state)
            if state <= last and (
                segments[state] == "." or (segments[state] == "**" and state < last)
            ):
                state += 1
            else:
                break
    return out


def _step(
    segments: tuple[str, ...],
    tokens: tuple[tuple[tuple, ...] | None, ...],
    states: set[int],
    name: str,
    *,
    is_file: bool,
) -> tuple[set[int], bool]:
    """The states after the walk takes ``name``, and whether, a file, it is
    one the walk yields. A trailing ``**`` yields whatever it reaches; a
    ``**`` before more segments, or a segment before more, takes only a
    directory; ``..`` is never followed. ``tokens`` holds each wildcard
    segment cut, and None for one matched by name."""
    last = len(segments) - 1
    nxt: set[int] = set()
    found = False
    for state in states:
        if state > last:
            continue
        segment = segments[state]
        if segment == "**":
            if state == last:
                found = found or is_file
                if not is_file:
                    nxt.add(state)
            elif not is_file:
                nxt.add(state)
        elif segment not in ("", ".", "..") and (
            name == segment if tokens[state] is None else _fn_match(tokens[state], name)
        ):
            if state == last:
                found = found or is_file
            elif not is_file:
                nxt.add(state + 1)
    return _settle(segments, nxt), found


class DiskGlob:
    """Glob's ``pattern`` from ``search_path``, as ``glob_runtime.find``
    reads it, for files at paths no disk holds.

    A name is matched one at a time: ``*``, ``?`` and a class never match a
    ``/``, a whole ``**`` segment spans any number of folders, and dot-names
    match. A file is then kept as ``find`` keeps one, outside the excluded
    folders and the history folders a wildcard reaches.
    """

    def __init__(
        self,
        pattern: str,
        search_path: str,
        excluded_dirs: Iterable[str] = GLOB_EXCLUDED_DIRS,
        history_children: dict[str, list[str]] = GLOB_HISTORY_CHILDREN,
    ) -> None:
        pattern = agent_glob_pattern(pattern)
        self.absolute = pattern.startswith("/")
        # A `..` steps back out of whatever the name before it is on disk: a
        # folder, a link into the mount, a file or nothing. Only the sandbox's
        # glob can tell, so a pattern with one finds nothing here.
        self.climbs = ".." in pattern.split("/")
        self._search_path = search_path
        self._excluded = frozenset(excluded_dirs)
        self._history = {
            parent: frozenset(names) for parent, names in history_children.items()
        }
        literal = [] if self.absolute else [search_path.rstrip("/")]
        for segment in pattern.split("/")[:-1]:
            if _magic(segment):
                break
            literal.append(segment)
        self._history_root = "/".join(literal) or "/"

        full = posixpath.join(search_path, pattern)
        segments = full.split("/")
        rooted = 0 if self.absolute else len(search_path.rstrip("/").split("/"))
        first = next(
            (i for i in range(rooted, len(segments)) if _magic(segments[i])), None
        )
        self._exact: str | None = None
        if self.climbs:
            self._exact = ""
            return
        if first is None:
            # The file is named outright, and a trailing `/` or `.` names
            # a directory.
            self._exact = _lexical(full) if segments[-1] not in ("", ".", "..") else ""
            return
        head = "/".join(segments[:first]) + "/" if first else ""
        self._root = _lexical(head.rstrip("/") or head)
        self._segments = tuple(
            [seg for seg in segments[first:-1] if seg] + segments[-1:]
        )
        # Cut once here rather than at each name, since a long segment
        # takes as long to cut as the pattern is.
        self._tokens = tuple(
            _fn_tokens(seg) if seg != "**" and _magic(seg) else None
            for seg in self._segments
        )

    def _step(
        self, states: set[int], name: str, *, is_file: bool
    ) -> tuple[set[int], bool]:
        return _step(self._segments, self._tokens, states, name, is_file=is_file)

    def _below(self, path: str) -> list[str] | None:
        if not _under(path, self._root):
            return None
        return path[len(self._root.rstrip("/")) + 1 :].split("/")

    def _kept(self, path: str) -> bool:
        inner = posixpath.relpath(path, self._search_path).split("/")[:-1]
        if self._excluded.intersection(inner):
            return False
        if self._history:
            rel_dir = posixpath.relpath(posixpath.dirname(path), self._history_root)
            below = [] if rel_dir == "." else rel_dir.split("/")
            parents = [posixpath.basename(self._history_root), *below[:-1]]
            if any(
                name in self._history.get(parent, ())
                for parent, name in zip(parents, below)
            ):
                return False
        return True

    def finds(self, path: str) -> bool:
        """Whether the glob lists a file at ``path``, every name above it
        being a folder."""
        if self._exact is not None:
            walked = path == self._exact
        else:
            names = self._below(path)
            walked = False
            if names:
                states = _settle(self._segments, {0})
                for name in names[:-1]:
                    states, _ = self._step(states, name, is_file=False)
                walked = self._step(states, names[-1], is_file=True)[1]
        return walked and self._kept(path)

    def enters(self, link: str) -> bool:
        """Whether the walk goes into the link into the file mount at
        ``link``: ``find`` follows one only where the pattern's literal
        prefix, or the search path, runs through it, and passes it by where a
        wildcard reaches it."""
        link = link.rstrip("/") or "/"
        if self._exact is not None:
            return _under(self._exact, link)
        return self._root == link or _under(self._root, link)

    def reaches(self, directory: str) -> bool:
        """Whether a file below ``directory`` could be listed."""
        directory = directory.rstrip("/") or "/"
        if self._exact is not None:
            return _under(self._exact, directory)
        if self._root == directory or _under(self._root, directory):
            return True
        names = self._below(directory)
        if names is None:
            return False
        states = _settle(self._segments, {0})
        for name in names:
            states, _ = self._step(states, name, is_file=False)
        last = len(self._segments) - 1
        return any(
            state <= last and self._segments[state] not in ("", "..")
            for state in states
        )
