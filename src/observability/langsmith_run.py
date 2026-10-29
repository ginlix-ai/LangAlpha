"""LangSmith run stamping.

Which provider served a call is decided at runtime — a fetch may fall through
a chain, a search engine comes from user prefs. LangSmith filters on run
metadata, not on the payload a tool returns, so that fact has to be written
onto the enclosing run rather than handed back to the caller.
"""

from __future__ import annotations

import functools
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

APP_VERSION = "app_version"

# How much of an error's text a run keeps: enough to name it, not a traceback.
ERROR_TEXT_MAX = 300


@functools.cache
def build_sha() -> Optional[str]:
    """The commit the running image was built from, if it was stamped.

    Read once, since the image is built for one commit and cannot change
    under a running process, but on first use rather than at import: this
    module loads early, and a host-side run gets the value from ``.env``,
    which ``load_dotenv`` may not have read yet.
    """
    return os.environ.get("LANGALPHA_BUILD_SHA", "").strip() or None


def merge_run_metadata(run: Any, fields: Mapping[str, Any]) -> None:
    """Merge the populated fields into a run's metadata as a copy.

    Until the tracer posts a run, its metadata dict can still be the callback
    manager's own, shared with the runs started after it, so an update in
    place would stamp them too.
    """
    populated = {k: v for k, v in fields.items() if v is not None}
    if populated:
        run.extra["metadata"] = {**run.metadata, **populated}


def stamp_run(tags: Optional[Sequence[str]] = None, **fields: Any) -> None:
    """Attach metadata (and optional tags) to the enclosing LangSmith run.

    No-ops outside a traced run — which is every call when tracing is off — so
    callers stamp unconditionally instead of guarding.
    """
    try:
        from langsmith.run_helpers import get_current_run_tree

        run = get_current_run_tree()
        if run is None:
            return
        merge_run_metadata(run, fields)
        if tags:
            merged = list(run.tags or [])
            seen = set(merged)
            for t in tags:
                if t not in seen:
                    seen.add(t)
                    merged.append(t)
            run.tags = merged
    except Exception as e:
        # A tool call must never fail because its telemetry did.
        logger.debug("stamp_run failed: %s", e)


# Tool-run outcome keys. The tracer reads them when the run ends and the
# monitoring app filters on them, so they are one contract in one place.
TOOL_OUTCOME = "tool_outcome"
TOOL_PROVIDER = "tool_provider"
# The path a sandbox or file call took, which tool_provider alone does not
# say: a file the mount serves or a store route answers never reaches the
# machine, and an SSH machine may be dialled direct or through the tunnel.
TOOL_CHANNEL = "tool_channel"
# The computer's size, which a sandbox call's latency and failures read against.
RESOURCE_TIER = "resource_tier"
ERROR_CLASS = "error_class"


def _stamp_tool_run(fields: dict[str, Any]) -> None:
    try:
        from langsmith.run_helpers import get_current_run_tree

        run = get_current_run_tree()
        # A helper the sandbox or a data client shares can run under a chain
        # or with no run at all; only a tool run has an outcome to record.
        if run is None or run.run_type != "tool":
            return
        merge_run_metadata(run, fields)
    except Exception as e:
        logger.debug("tool run stamp failed: %s", e)


def stamp_tool_provider(provider: Optional[str]) -> None:
    """Name the backend that actually served the enclosing tool call."""
    _stamp_tool_run({TOOL_PROVIDER: provider})


def stamp_tool_channel(channel: Optional[str]) -> None:
    """Name the path the enclosing tool call took (``TOOL_CHANNEL``)."""
    _stamp_tool_run({TOOL_CHANNEL: channel})


def stamp_resource_tier(tier: Optional[str]) -> None:
    """Name the size of the machine the enclosing tool call ran on (``RESOURCE_TIER``)."""
    _stamp_tool_run({RESOURCE_TIER: tier})


def _class_of(error: BaseException | str) -> str:
    return error if isinstance(error, str) else type(error).__name__


def mark_tool_failed(
    error: BaseException | str, provider: Optional[str] = None
) -> None:
    """Record that the enclosing tool call failed though it returned normally.

    Most tools hand a failure back as text for the model to read rather than
    raising, so LangSmith would count the run a success. The tracer turns
    this stamp into an errored run when the call ends. Call it only for a
    failure of the tool itself or its provider, never for a mistake in what
    the agent asked or for a target that declined (``mark_tool_unavailable``).
    ``error`` is the exception, whose class becomes the run's error class, or
    a class name for a failure that has none.
    """
    _stamp_tool_run(
        {TOOL_OUTCOME: "error", ERROR_CLASS: _class_of(error), TOOL_PROVIDER: provider}
    )


def mark_tool_unavailable(
    error: BaseException | str, provider: Optional[str] = None
) -> None:
    """Record that the tool and its provider worked but the target declined.

    A 404, a paywall, an anti-bot page or an empty one is frequent and nobody
    can act on it, so the run stays un-errored and out of every failure alarm,
    yet apart from a success, so a rate of it can still be read.
    """
    _stamp_tool_run(
        {
            TOOL_OUTCOME: "unavailable",
            ERROR_CLASS: _class_of(error),
            TOOL_PROVIDER: provider,
        }
    )


def record_child_run(
    name: str,
    *,
    start_time: Optional[datetime] = None,
    inputs: Optional[dict[str, Any]] = None,
    outputs: Optional[dict[str, Any]] = None,
    error: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    """Post a finished run under the enclosing LangSmith run.

    Without a parent nothing is posted, so a caller outside a trace (a REST
    route, tracing off) never starts a stray root trace. The run is created
    already ended: one queued POST, and nothing waits on the network.
    """
    try:
        from langsmith.run_helpers import get_current_run_tree, get_tracing_context

        parent = get_current_run_tree()
        if parent is None or parent.extra.get("__disabled"):
            return
        if get_tracing_context().get("enabled") is False:
            return
        fields = {k: v for k, v in (metadata or {}).items() if v is not None}
        sha = build_sha()
        if sha:
            fields.setdefault(APP_VERSION, sha)
        child = parent.create_child(
            name,
            "chain",
            inputs=inputs,
            outputs=outputs,
            error=error,
            start_time=start_time,
            end_time=datetime.now(timezone.utc),
            extra={"metadata": fields},
        )
        # The child inherits the parent's metadata, but the parent's outcome
        # is its own: an inherited error_class would read as this run's.
        merged = child.extra["metadata"]
        for key in (TOOL_OUTCOME, ERROR_CLASS, TOOL_PROVIDER, TOOL_CHANNEL):
            if key not in fields:
                merged.pop(key, None)
        child.post()
        # Posting never reads child_runs, and a parent that outlives its
        # children (a turn's chain) would otherwise hold every one.
        with suppress(ValueError):
            parent.child_runs.remove(child)
    except Exception as e:
        logger.debug("child run post failed: %s", e)
