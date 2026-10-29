"""The subagent file's frontmatter, read the same way wherever a file arrives.

The agent's folder and a plugin's ``agents/`` folder carry the same file, so
both read it here: a file one of them accepts the other reads the same way.
"""

from __future__ import annotations

import re
from typing import Any

import yaml


class StrictLoader(yaml.SafeLoader):
    """Safe YAML that refuses a key given twice, where plain YAML keeps the last."""


def _mapping(loader: StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=True)
        try:
            duplicate = key in seen
        except TypeError:
            continue  # construct_mapping refuses an unhashable key
        if duplicate:
            raise yaml.constructor.ConstructorError(
                None, None, f"{key} is given twice", key_node.start_mark
            )
        seen.add(key)
    return loader.construct_mapping(node, deep=deep)


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


# What Postgres stores in neither text nor JSONB: a NUL, and a lone UTF-16
# surrogate, which no UTF-8 encoding holds. A YAML escape (``"\0"``,
# ``"\ud800"``) or a JSON one makes either from printable text.
UNSAVABLE = re.compile("[\x00\ud800-\udfff]")


def unsavable_char(value: Any) -> str | None:
    """A character saved text can't hold in a string anywhere in ``value``,
    key or item, or None. A lone surrogate wins over a NUL wherever each
    sits, since a refusal can echo a NUL but not a surrogate.

    Walked without recursion, since YAML may nest deeper than Python
    recurses, and each container once: a YAML alias is the same object
    wherever it is used, so a few hundred bytes of nested aliases walked as
    a tree would take hours.
    """
    stack = [value]
    seen: set[int] = set()
    nul = None
    while stack:
        node = stack.pop()
        if isinstance(node, str):
            for found in UNSAVABLE.finditer(node):
                if found.group() != "\x00":
                    return found.group()
                nul = "\x00"
        elif isinstance(node, (dict, list)) and id(node) not in seen:
            seen.add(id(node))
            stack.extend(node)
            if isinstance(node, dict):
                stack.extend(node.values())
    return nul


class FrontmatterFault(Exception):
    """Why a subagent file's frontmatter didn't load, worded without a final
    period so each reader can end it its own way.

    ``kind`` is ``fences`` (no frontmatter between two ``---`` lines),
    ``nul`` (a raw NUL, which YAML refuses by character position), ``yaml``
    (not valid YAML) or ``mapping`` (valid YAML, but not key: value lines).
    ``quotable`` marks a ``yaml`` fault that quoting the value would fix.
    """

    def __init__(self, kind: str, message: str, *, quotable: bool = False) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.quotable = quotable


def load_frontmatter(content: str) -> tuple[dict[Any, Any], str]:
    """(the frontmatter as a mapping, the body after it), or a FrontmatterFault."""
    parts = split_frontmatter(content)
    if parts is None:
        raise FrontmatterFault(
            "fences",
            "a subagent file starts with its frontmatter between two --- lines, then its "
            "instructions",
        )
    front, body, first_line = parts
    if "\x00" in front:
        raise FrontmatterFault(
            "nul", "the frontmatter holds a NUL character (\\x00), which saved text can't hold"
        )
    try:
        loaded = yaml.load(front, Loader=StrictLoader)  # noqa: S506 - a SafeLoader subclass
    except (yaml.YAMLError, RecursionError, ValueError) as exc:
        # ValueError: a value YAML types and can't build (``2026-13-45``),
        # which quoting keeps a string; RecursionError: nesting too deep.
        mark = getattr(exc, "problem_mark", None)
        where = f" (line {mark.line + first_line})" if mark is not None else ""
        problem = getattr(exc, "problem", None) or str(exc)
        raise FrontmatterFault(
            "yaml",
            f"the frontmatter is not valid YAML{where}: {problem}",
            quotable=not isinstance(exc, (yaml.constructor.ConstructorError, RecursionError)),
        ) from None
    if loaded is None:
        return {}, body
    if not isinstance(loaded, dict):
        raise FrontmatterFault("mapping", "the frontmatter must be key: value lines")
    return loaded, body


def split_frontmatter(content: str) -> tuple[str, str, int] | None:
    """(frontmatter, body, line the frontmatter starts on), or None without fences."""
    lines = content.removeprefix("﻿").replace("\r\n", "\n").split("\n")
    start = 0
    while start < len(lines) and not lines[start].strip():
        start += 1
    if start == len(lines) or lines[start].rstrip() != "---":
        return None
    for end in range(start + 1, len(lines)):
        if lines[end].rstrip() == "---":
            return "\n".join(lines[start + 1:end]), "\n".join(lines[end + 1:]), start + 2
    return None
