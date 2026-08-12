"""Server-side QuickJS execution for agent-authored workflows."""

from __future__ import annotations

import asyncio
import hashlib
import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from itertools import islice
from typing import Any, Protocol

import quickjs_rs
import structlog
from quickjs_rs import Runtime, SourceTransform, ThreadWorker

from ptc_agent.agent.backends.workflows import WORKFLOW_NAME_RE

logger = structlog.get_logger(__name__)

_EXPORT_META_RE = re.compile(
    r"^\s*export\s+(?=const\s+meta\b)",
    flags=re.MULTILINE,
)
# Anchored at the end of whatever precedes the live declaration, so it can only
# ever remove that declaration's own keyword.
_EXPORT_KEYWORD_RE = re.compile(r"\bexport\s+$")
# Every candidate keyword, live or quoted. Matches the word alone so blanking
# it preserves each surrounding whitespace character, and with it every offset.
_EXPORT_BEFORE_META_RE = re.compile(r"\bexport\b(?=\s+const\s+meta\b)")
_META_DECL_RE = re.compile(r"\bconst\s+meta\b\s*=")
# A parser probe costs milliseconds, so both meta scans are bounded — a script
# is agent-authored and may be hostile, and the per-eval compile timeout cannot
# bound a scan made of many small evals. Both ceilings sit far above any real
# metadata literal (one closing brace per phase entry, plus nesting).
_MAX_DECL_CANDIDATES = 16
_MAX_BRACE_PROBES = 256
_COMPILE_MEMORY_LIMIT = 16 * 1024 * 1024
_COMPILE_TIMEOUT = 2.0
_HOST_TEXT_LIMIT = 500
# `meta.name` is bounded by its pattern; the description had no bound but the
# script's own size cap, and it is retained for the life of a compile-cache
# entry and copied into the registry, lifecycle frames and checkpoint records.
# Well past any real one-line summary, far short of a payload. Public because
# the launch description is the same value reaching the same sinks, and a
# caller-supplied one overrides this: the bound belongs to the field, not to
# the door it came through.
MAX_DESCRIPTION_CHARS = 512
# Compiling goes through `new Function`, the one primitive QuickJS offers that
# parses without executing. The candidate crosses as a global string rather
# than being spliced into the evaluated source, so a script cannot close the
# wrapper early and reach top level — see _compile.
_COMPILE_SRC_GLOBAL = "__workflow_compile_src"
_COMPILE_EVAL = f"void new Function({_COMPILE_SRC_GLOBAL});"


class WorkflowScriptError(Exception):
    """Compile or validation failure with agent-actionable detail."""


class WorkflowHostError(Exception):
    """Dispatch-time host failure exposed as a JavaScript throw."""


class WorkflowUsageError(WorkflowHostError):
    """A dispatch the script got wrong — deterministic misuse, not a child or
    infrastructure failure, so the prelude escalates it as a script bug."""


@dataclass(frozen=True)
class WorkflowMeta:
    """Validated workflow metadata."""

    name: str
    description: str


@dataclass(frozen=True)
class WorkflowLimits:
    """QuickJS resource limits for one workflow run."""

    memory_limit_mb: int = 128
    cpu_budget_s: float = 30.0


@dataclass(frozen=True)
class WorkflowOutcome:
    """Terminal workflow script outcome."""

    status: str
    result: Any = None
    error: str | None = None
    error_stack: str | None = None


class WorkflowHost(Protocol):
    """Host operations available to a workflow script."""

    async def agent(self, prompt: str, opts: dict[str, Any]) -> Any: ...

    def phase(self, title: str) -> None: ...

    def log(self, message: str) -> None: ...


def _transform_source(
    script: str, parses: Callable[[str], bool] | None, *, invoke: bool
) -> tuple[str, str]:
    """Strip the meta export and wrap the script in an async IIFE."""
    transformed = _strip_meta_export(script, parses)
    suffix = ")()" if invoke else ")"
    return transformed, f"(async () => {{\n{transformed}\n}}{suffix}"


def _strip_meta_export(script: str, parses: Callable[[str], bool] | None) -> str:
    """Remove the ``export`` keyword introducing the *live* meta declaration.

    A line-initial ``export const meta`` inside a block comment or template
    literal matches the same pattern the real declaration does; stripping that
    one leaves the real export inside the wrapper, where nothing parses. The
    probe says which declaration is code. Falling back to the first textual
    match when there is none keeps a script that does not parse at all
    reporting its own syntax error rather than a missing-metadata one.
    """
    declaration = _find_meta_declaration(_blank_meta_exports(script), parses) if parses else None
    if declaration is None:
        return _EXPORT_META_RE.sub("", script, count=1)
    start = declaration.start()
    return _EXPORT_KEYWORD_RE.sub("", script[:start]) + script[start:]


def _blank_meta_exports(script: str) -> str:
    """Blank every ``export`` that introduces a ``const meta``, in place.

    The scan asks whether a prefix parses as a function body, and ``export``
    never does — so with the keywords left in, the live declaration is
    rejected for the same reason a quoted one is. Blanking is length
    preserving, so an offset in the answer is an offset in the script.
    """
    return _EXPORT_BEFORE_META_RE.sub(lambda match: " " * len(match.group()), script)


@contextmanager
def _probe_context() -> Iterator[Callable[[str], bool]]:
    """A throwaway parse context for locating the live meta declaration.

    Its own runtime rather than the caller's: the scan must not spend a run's
    CPU budget, and it never touches a script's globals.
    """
    runtime = Runtime(memory_limit=_COMPILE_MEMORY_LIMIT)
    ctx = runtime.new_context(timeout=_COMPILE_TIMEOUT)
    try:
        yield _compile_probe(ctx)
    finally:
        ctx.close()
        runtime.close()


def _js_detail(error: quickjs_rs.JSError) -> str:
    detail = error.message
    stack = error.stack or ""
    location = next((line.strip() for line in stack.splitlines() if line.strip()), "")
    return f"{detail} ({location})" if location else detail


def _find_meta_literal(script: str, parses: Callable[[str], bool]) -> str:
    """Extract meta's object literal using QuickJS itself as the lexer.

    Every boundary question here — is this ``const meta`` live code or text
    inside a comment, does this ``}`` close the literal — is answered by
    handing a candidate to the engine's own parser, so regex literals,
    template substitutions and escapes cost nothing to support.
    """
    declaration = _find_meta_declaration(script, parses)
    if declaration is None:
        raise WorkflowScriptError(
            "export const meta = { name, description } literal is required"
        )

    start = script.find("{", declaration.end())
    if start < 0 or script[declaration.end() : start].strip():
        raise WorkflowScriptError("meta must be a pure object literal with name and description")
    end = start
    for _ in range(_MAX_BRACE_PROBES):
        end = script.find("}", end + 1)
        if end < 0:
            break
        # A prefix stopping short of the closing brace cannot balance, so the
        # first one the parser accepts ends the literal.
        if parses(f"(async function () {{ return ({script[start : end + 1]}); }})"):
            return script[start : end + 1]
    raise WorkflowScriptError(
        "meta object literal is unterminated or too complex to extract"
    )


def _find_meta_declaration(
    script: str, parses: Callable[[str], bool]
) -> re.Match[str] | None:
    for match in islice(_META_DECL_RE.finditer(script), _MAX_DECL_CANDIDATES):
        # Only live code both parses on its own and rejects a stray token
        # appended to it — a prefix ending inside a line comment swallows the
        # token, and one ending inside a string, block comment or regex
        # literal does not parse at all.
        body = script[: match.start()]
        if _parses_body(parses, body) and not _parses_body(parses, f"{body})"):
            return match
    return None


def _parses_body(parses: Callable[[str], bool], body: str) -> bool:
    return parses(f"(async function () {{\n{body}\n}})")


def _compile(ctx: Any, source: str) -> None:
    """Parse ``source`` without executing any of it, raising ``JSError`` if it
    does not parse.

    The source crosses as a global string and is compiled by ``new Function``,
    so a script that closes the wrapper early ("});") produces nothing but a
    discarded function object — there is no arrangement of braces that reaches
    a live evaluation from here.
    """
    ctx.globals[_COMPILE_SRC_GLOBAL] = source
    ctx.eval(_COMPILE_EVAL)


@contextmanager
def _compile_stage(what: str) -> Iterator[None]:
    """Name the compile stage that ran out of budget.

    Every stage shares the runtime's CPU and memory limits and differs only in
    what to call itself; each site keeps its own arm for the failures that are
    about the script rather than the budget.
    """
    try:
        yield
    except (quickjs_rs.TimeoutError, quickjs_rs.InterruptError) as error:
        raise WorkflowScriptError(f"{what} exceeded the compile CPU budget") from error
    except quickjs_rs.MemoryLimitError as error:
        raise WorkflowScriptError(
            f"{what} exceeded the compile memory limit"
        ) from error


def _compile_probe(ctx: Any) -> Callable[[str], bool]:
    """Bind a "does QuickJS accept this source" test to one compile context."""

    def parses(source: str) -> bool:
        try:
            with _compile_stage("meta extraction"):
                _compile(ctx, source)
        except quickjs_rs.JSError:
            return False
        return True

    return parses


def compile_check(script: str) -> WorkflowMeta:
    """Syntax-check a workflow and return its validated metadata."""
    runtime = Runtime(memory_limit=_COMPILE_MEMORY_LIMIT)
    ctx = runtime.new_context(timeout=_COMPILE_TIMEOUT)
    try:
        parses = _compile_probe(ctx)
        transformed, syntax_source = _transform_source(script, parses, invoke=False)
        try:
            with _compile_stage("script syntax check"):
                _compile(ctx, syntax_source)
        except quickjs_rs.JSError as error:
            raise WorkflowScriptError(
                f"JavaScript syntax error: {_js_detail(error)}"
            ) from error

        literal = _find_meta_literal(transformed, parses)
        try:
            with _compile_stage("meta evaluation"):
                raw = ctx.eval(f"({literal})")
        except (quickjs_rs.JSError, quickjs_rs.MarshalError) as error:
            raise WorkflowScriptError(
                "meta must be a pure object literal with literal name and "
                "description values"
            ) from error
    finally:
        ctx.close()
        runtime.close()

    if not isinstance(raw, dict):
        raise WorkflowScriptError("meta must be a pure object literal")
    name = raw.get("name")
    if not isinstance(name, str) or not WORKFLOW_NAME_RE.fullmatch(name):
        raise WorkflowScriptError(f"meta.name must match {WORKFLOW_NAME_RE.pattern}")
    description = raw.get("description")
    if not isinstance(description, str) or not description.strip():
        raise WorkflowScriptError("meta.description must be a non-empty string")
    if len(description) > MAX_DESCRIPTION_CHARS:
        raise WorkflowScriptError(
            f"meta.description is {len(description)} chars; the cap is "
            f"{MAX_DESCRIPTION_CHARS}"
        )
    # Only the two fields the server reads survive the literal, and both are
    # length-bounded above. The rest is script-declared and unbounded —
    # anything kept here is pinned for the life of the compile memo, so a
    # padded meta would retain megabytes per entry.
    return WorkflowMeta(name=name, description=description)


_COMPILE_CACHE: dict[str, WorkflowMeta | str] = {}
_COMPILE_CACHE_MAX = 256


async def acompile_check(script: str) -> WorkflowMeta:
    """Run compile_check off the event loop, memoized by content hash.

    The meta-literal eval executes attacker-controllable initializer
    expressions (bounded by _COMPILE_TIMEOUT), so request paths must never
    run it on the event loop or re-run it for unchanged content.
    """
    key = hashlib.sha256(script.encode()).hexdigest()
    cached = _COMPILE_CACHE.get(key)
    if cached is None:
        try:
            cached = await asyncio.to_thread(compile_check, script)
        except WorkflowScriptError as error:
            cached = str(error)
        if len(_COMPILE_CACHE) >= _COMPILE_CACHE_MAX:
            _COMPILE_CACHE.pop(next(iter(_COMPILE_CACHE)))
        _COMPILE_CACHE[key] = cached
    if isinstance(cached, str):
        raise WorkflowScriptError(cached)
    return cached


def compile_is_memoized(script: str) -> bool:
    """Whether ``acompile_check`` can answer for ``script`` without compiling.

    Lets a caller that describes many scripts at once spend its compiles on
    the ones that actually need one, rather than paying the full price for a
    namespace larger than the memo.
    """
    return hashlib.sha256(script.encode()).hexdigest() in _COMPILE_CACHE


def _script_error(error: quickjs_rs.JSError) -> WorkflowOutcome:
    return WorkflowOutcome(
        status="script_error",
        error=f"{error.name}: {error.message}",
        error_stack=error.stack,
    )


@dataclass
class _Preemption:
    """Whether this run's interrupt handler stopped JavaScript on the deadline.

    QuickJS abandons the job it is running the moment the handler returns
    True, which leaves the top-level promise pending with no host call in
    flight — the exact shape quickjs-rs reports as a deadlock, and it checks
    for that before it checks its own deadline. Without a record of who
    tripped the handler, a CPU-budget overrun after the first host await is
    indistinguishable from a script awaiting a promise nothing resolves.
    """

    deadline: bool = False


def _cpu_timeout(limits: WorkflowLimits) -> WorkflowOutcome:
    return WorkflowOutcome(
        status="cpu_timeout",
        error=f"Workflow exceeded the {limits.cpu_budget_s:g}s CPU budget",
    )


def _cancelled_if_unwinding(error: BaseException) -> None:
    """Convert a QuickJS failure into cancellation when this task is unwinding.

    A cancel reaches the worker as an interrupt, so the engine sees whatever
    the abandoned job raised rather than ``CancelledError``.
    """
    current_task = asyncio.current_task()
    if current_task is not None and current_task.cancelling():
        raise asyncio.CancelledError from error


def _unexpected_failure(error: BaseException) -> WorkflowOutcome:
    logger.exception("Unexpected workflow QuickJS failure")
    return WorkflowOutcome(
        status="script_error",
        error=f"Workflow execution failed: {type(error).__name__}: {error}",
    )


def _install_stop_aware_interrupt(
    runtime: Runtime, stop: threading.Event, preemption: _Preemption
) -> None:
    """Let a stop interrupt the JavaScript that runs *after* cancellation.

    quickjs-rs clears the runtime's deadline slot before draining pending jobs
    on cancel, and that drain resumes the script — a ``catch``/``finally``, or
    the continuation a ``parallel()`` builds out of its own ``allSettled`` — with
    no interrupt source left, so it can hold this worker thread for the life of
    the process. Or-ing a stop flag into the handler is what bounds that drain:
    writing a deadline instead cannot, because the same path clears it first.
    """
    # The only reach into quickjs-rs internals in this module — the deadline
    # slot the upstream handler reads, and the hook that installs the poll
    # callback. Both are checked before use, so a dependency bump that renames
    # either costs stop preemption instead of failing every workflow run.
    if not hasattr(runtime, "_deadline"):
        logger.warning(
            "quickjs-rs deadline slot is missing; a stopped workflow cannot "
            "preempt JavaScript"
        )
        return

    def _interrupt() -> bool:
        if stop.is_set():
            return True
        deadline = runtime._deadline
        if deadline is None or time.monotonic() < deadline:
            return False
        preemption.deadline = True
        return True

    try:
        runtime._engine_rt.set_interrupt_handler(_interrupt)
    except Exception:
        logger.warning(
            "quickjs-rs interrupt hook is unavailable; a stopped workflow "
            "cannot preempt JavaScript",
            exc_info=True,
        )


async def run_workflow_script(
    script: str,
    args: Any,
    host: WorkflowHost,
    limits: WorkflowLimits,
) -> WorkflowOutcome:
    """Execute one workflow in an isolated QuickJS worker thread."""
    server_loop = asyncio.get_running_loop()
    prelude = resources.files(__package__).joinpath("prelude.js").read_text(encoding="utf-8")
    worker = ThreadWorker(name="workflow-quickjs")
    stop = threading.Event()
    preemption = _Preemption()

    async def _inner() -> WorkflowOutcome:
        runtime = None
        ctx = None

        async def _host_agent(prompt: str, opts: dict[str, Any] | None = None) -> Any:
            future = asyncio.run_coroutine_threadsafe(
                host.agent(prompt, opts or {}), server_loop
            )
            try:
                result = await asyncio.wrap_future(future)
                return {"ok": True, "value": result}
            except asyncio.CancelledError:
                future.cancel()
                raise
            except WorkflowHostError as error:
                return {
                    "ok": False,
                    "error": str(error),
                    "usage": isinstance(error, WorkflowUsageError),
                }

        def _host_phase(title: Any) -> None:
            host.phase(str(title)[:_HOST_TEXT_LIMIT])

        def _host_log(message: Any) -> None:
            host.log(str(message)[:_HOST_TEXT_LIMIT])

        try:
            # On the worker thread and in its own context: the scan must agree
            # with the one compile_check ran, without spending this run's CPU
            # budget or blocking the server loop to do it.
            with _probe_context() as parses:
                _transformed, wrapped = _transform_source(script, parses, invoke=True)
            runtime = Runtime(
                memory_limit=limits.memory_limit_mb * 1024 * 1024,
                transform_flags=SourceTransform.TOP_LEVEL_CONST_TO_VAR,
            )
            _install_stop_aware_interrupt(runtime, stop, preemption)
            ctx = runtime.new_context(timeout=limits.cpu_budget_s)
            ctx.register("__host_agent", _host_agent, is_async=True)
            ctx.register("__host_phase", _host_phase, is_async=False)
            ctx.register("__host_log", _host_log, is_async=False)
            ctx.eval(f"{prelude}\nundefined;")
            ctx.globals["args"] = args
            handle = await ctx.eval_handle_async(wrapped, timeout=limits.cpu_budget_s)
            with handle:
                resolved = await handle.await_promise(timeout=None)
                with resolved:
                    try:
                        result = resolved.to_python()
                    except quickjs_rs.MarshalError as error:
                        return WorkflowOutcome(
                            status="script_error",
                            error=(
                                "Workflow return value must be JSON-serializable: "
                                f"{error}"
                            ),
                        )
            if result is quickjs_rs.UNDEFINED:
                result = None
            return WorkflowOutcome(status="completed", result=result)
        except asyncio.CancelledError:
            raise
        except quickjs_rs.MemoryLimitError as error:
            return WorkflowOutcome(status="out_of_memory", error=str(error))
        except (quickjs_rs.TimeoutError, quickjs_rs.InterruptError):
            return _cpu_timeout(limits)
        except quickjs_rs.DeadlockError as error:
            # The budget IS enforced across host awaits — quickjs-rs suspends
            # its deadline while parked in a host call and re-arms it extended
            # by the parked time — but a script that overruns *after* its
            # first await never reaches the deadline check: the interrupt
            # abandons the job first, and the pending-promise check runs
            # before it. Only a handler that never tripped means a genuinely
            # unresolved promise, which is the script's bug and keeps the
            # library's own wording.
            _cancelled_if_unwinding(error)
            if preemption.deadline:
                return _cpu_timeout(limits)
            return _unexpected_failure(error)
        except quickjs_rs.JSError as error:
            return _script_error(error)
        except quickjs_rs.MarshalError as error:
            return WorkflowOutcome(
                status="script_error",
                error=f"Workflow value must be JSON-serializable: {error}",
            )
        except quickjs_rs.HostCancellationError:
            raise asyncio.CancelledError from None
        except Exception as error:
            _cancelled_if_unwinding(error)
            return _unexpected_failure(error)
        finally:
            if ctx is not None:
                try:
                    ctx.close()
                except Exception:
                    logger.debug("Failed to close workflow QuickJS context", exc_info=True)
            if runtime is not None:
                try:
                    runtime.close()
                except Exception:
                    logger.debug("Failed to close workflow QuickJS runtime", exc_info=True)

    inner_future = None
    try:
        inner_future = worker.run_async(_inner())
        return await asyncio.wrap_future(inner_future)
    except asyncio.CancelledError:
        if inner_future is not None:
            inner_future.cancel()
        raise
    finally:
        # The interrupt handler polls, so flagging every exit path here — this
        # run is over, nothing it left running may keep the thread — bounds the
        # JavaScript the engine resumes while unwinding.
        stop.set()
        # Joining here could block the server loop until a CPU-bound eval stops.
        threading.Thread(target=worker.close, daemon=True).start()
