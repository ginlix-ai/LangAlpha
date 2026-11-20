"""Input schemas for tools built every turn, inferred once per function.

A tool that closes over a turn's values is built for every turn, and LangChain
infers its input schema from the function's signature each time, a new
pydantic model per tool per turn: about half the time it takes to build a
turn's tools, on the event loop, and garbage only a full collection frees.
Every turn's closure of one function mostly has the same signature, so the
schema is inferred once and reused while the annotations, defaults and
docstring stay equal. One that does change, such as an argument described by
what the turn can reach, is inferred again.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Hashable
from types import CodeType
from typing import Any, Literal

from langchain_core.tools import BaseTool, create_schema_from_function, tool
from langchain_core.tools.structured import _filter_schema_args
from pydantic import BaseModel

# One slot per function and name, so a signature that changes every turn
# replaces its entry rather than adding one.
_SCHEMAS: dict[tuple[CodeType, str], tuple[Hashable, type[BaseModel]]] = {}


def input_schema(name: str, func: Callable[..., Any]) -> type[BaseModel]:
    """The schema ``StructuredTool.from_function`` would infer for ``func``,
    reused while what that inference reads of ``func`` equals the last one's:
    its signature as ``inspect`` sees it, through any wrapper, and as text, so
    defaults equal but of another type (``0`` and ``False``) or a ``Literal``
    in another order count as a change, its docstring and its qualified
    name."""
    slot = (inspect.unwrap(func).__code__, name)
    try:
        signature = inspect.signature(func)
        read = (signature, str(signature), func.__doc__, func.__qualname__)
        hash(read)
    except TypeError:  # an annotation or default that cannot be compared
        read = None
    cached = _SCHEMAS.get(slot)
    if read is not None and cached is not None and cached[0] == read:
        return cached[1]
    schema = create_schema_from_function(name, func, filter_args=_filter_schema_args(func))
    if read is not None:
        _SCHEMAS[slot] = (read, schema)
    return schema


def turn_tool(
    name: str | None = None,
    *,
    description: str | None = None,
    response_format: Literal["content", "content_and_artifact"] = "content",
) -> Callable[[Callable[..., Any]], BaseTool]:
    """``@tool`` for a function defined per turn, with its schema from
    ``input_schema``. Only the options that schema honours: one that changes
    inference, such as ``parse_docstring``, would be dropped."""

    def make(func: Callable[..., Any]) -> BaseTool:
        tool_name = name or func.__name__
        return tool(
            tool_name,
            description=description,
            args_schema=input_schema(tool_name, func),
            response_format=response_format,
        )(func)

    return make
