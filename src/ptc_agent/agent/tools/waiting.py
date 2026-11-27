"""Whether a command is waiting rather than working.

A plain steer moves only such a command to the background, since the agent
is not busy with it; Send now moves any. A wrong guess only moves a command
early, never stops one, so the read is broad: a command that starts by
sleeping (``sleep 60 && python check.py``), a loop that sleeps (a poll), or
one that only sleeps and prints.
"""

from __future__ import annotations

import ast
import re

# `sleep N` as the first thing a command runs, before any && ; || or newline.
_LEADING_SLEEP = re.compile(r"\A\s*sleep\s+\d+(\.\d+)?[smhd]?\s*(\Z|&&|;|\|\||\n)")
_LOOP = re.compile(r"\b(while|until|for)\b")
_SLEEP = re.compile(r"\bsleep\s+\d")
_SLEEP_MODULES = frozenset({"time", "asyncio"})


def is_waiting(tool: str, command: str) -> bool:
    """Whether ``command`` waits rather than works: it starts with a sleep,
    or a loop in it sleeps."""
    if tool == "ExecuteCode":
        return _code_waits(command)
    command = command.replace("\\\n", " ")
    if _LEADING_SLEEP.search(command):
        return True
    return bool(_LOOP.search(command) and _SLEEP.search(command))


def _code_waits(code: str) -> bool:
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False
    body = [n for n in tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
    if body and _sleeps(body[0]) and not isinstance(body[0], (ast.FunctionDef, ast.AsyncFunctionDef)):
        return True
    return any(isinstance(n, (ast.While, ast.For, ast.AsyncFor)) and _sleeps(n) for n in ast.walk(tree))


def _sleeps(node: ast.AST) -> bool:
    """Whether ``node`` calls a sleep anywhere in it."""
    return any(isinstance(n, ast.Call) and _is_sleep(n.func) for n in ast.walk(node))


def _is_sleep(func: ast.expr) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "sleep"
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "sleep"
        and isinstance(func.value, ast.Name)
        and func.value.id in _SLEEP_MODULES
    )
