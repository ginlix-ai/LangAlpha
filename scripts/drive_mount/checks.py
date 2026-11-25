"""The harness's one check registry.

A suite module registers its cases with ``@case("suite")`` at import; a case
pins one or more checks, each named by the behaviour it holds the mount to.
``run`` runs a suite's cases in the order they were written (later cases in
a kernel suite may build on files earlier ones left), and a case that raises
is pinned as failed under its own name rather than ending the run, unless
the error is marked ``fatal`` (the mount itself gone).
"""

from __future__ import annotations

import traceback
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class Case:
    suite: str
    name: str
    fn: Callable


CASES: list[Case] = []


def case(suite: str) -> Callable[[Callable], Callable]:
    def register(fn: Callable) -> Callable:
        CASES.append(Case(suite, fn.__name__, fn))
        return fn
    return register


@dataclass
class Results:
    checks: list[dict] = field(default_factory=list)

    def pin(self, name: str, ok: bool, detail: str = "", **extra) -> bool:
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail, **extra})
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""), flush=True)
        return bool(ok)

    def failed(self) -> list[dict]:
        return [c for c in self.checks if not c["ok"]]


def run(suite: str, results: Results, *args, only: list[str] | None = None) -> None:
    """Every case of ``suite`` (those whose name holds one of ``only``)."""
    for c in CASES:
        if c.suite != suite or (only and not any(o in c.name for o in only)):
            continue
        try:
            c.fn(results, *args)
        except Exception as exc:  # noqa: BLE001 - a crash is that case failing
            if getattr(exc, "fatal", False):
                raise
            results.pin(c.name, False, f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=-3)}")
