"""The LangSmith tracer a turn's runs are posted through.

When tracing is on, langchain_core builds a ``LangChainTracer`` for every run
that names none, and that tracer appends each run to its parent, so a root
holds its whole tree until it ends: every node's inputs and every model call's
serialized messages. A turn has no step cap, so that tree grows for as long as
the turn runs. Naming this tracer in a run's callbacks stops langchain_core
from building its own, and the runs still land where they did.

Since every run passes through it, it also writes the stamps LangSmith is
filtered on (the build, a model call's billed cost, a tool run's outcome;
keys in ``langsmith_run``) and stops a model or tool call resending its
inputs when it closes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Mapping, cast
from uuid import UUID

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool
from langchain_core.tracers.langchain import LangChainTracer
from langchain_core.tracers.schemas import Run
from langgraph.errors import GraphBubbleUp
from pydantic import ValidationError

from src.observability.langsmith_run import (
    APP_VERSION,
    ERROR_CLASS,
    ERROR_TEXT_MAX,
    TOOL_OUTCOME,
    build_sha,
    merge_run_metadata,
)
from src.utils.tracking.core import PricedCall, PricingResolver, price_call
from src.utils.tracking.per_call_token_tracker import (
    billing_attribution,
    build_call_record,
)

logger = logging.getLogger(__name__)

# Pricing breakdown entries reported to LangSmith as input cost.
_INPUT_COST_KEYS = (
    "input",
    "cached_input",
    "cache_storage",
    "cache_5m_creation",
    "cache_1h_creation",
)

# Where langchain_core validates a tool's arguments.
_PARSE_INPUT = BaseTool._parse_input.__code__


class NonRetainingTracer(LangChainTracer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # One per run tree and shared with its copies, so a model with no
        # pricing card logs its miss once per turn, as billing does.
        self._pricer = PricingResolver()

    def copy_with_metadata_defaults(
        self,
        *,
        metadata: Mapping[str, str] | None = None,
        tags: list[str] | None = None,
    ) -> NonRetainingTracer:
        # The base builds the copy with ``self.__class__``.
        copy = cast(
            "NonRetainingTracer",
            super().copy_with_metadata_defaults(metadata=metadata, tags=tags),
        )
        copy._pricer = self._pricer
        return copy

    def _start_trace(self, run: Run) -> None:
        super()._start_trace(run)
        # On each run rather than in tracing_metadata, so the tracer stays
        # configured exactly as the one langchain_core would have built.
        sha = build_sha()
        if sha and APP_VERSION not in run.metadata:
            merge_run_metadata(run, {APP_VERSION: sha})
        if run.run_type in ("llm", "tool"):
            # A model's prompt and a tool's arguments are fixed when the run
            # starts and are posted then, but langchain_core resends them with
            # every update unless this flag is set, and nothing sets it; for a
            # model call that doubles the largest upload a turn makes.
            # LangSmith clears it if the inputs change after the post. Not for
            # chains: one can receive its inputs at its end.
            run.extra["inputs_is_truthy"] = True

    @staticmethod
    def _add_child_run(parent_run: Run, child_run: Run) -> None:
        # Posting never reads child_runs (each run is sent on its own, with
        # its parent id and dotted order), and a turn has no step cap, so
        # keeping them would grow the root without bound.
        return None

    def _on_llm_end(self, run: Run) -> None:
        try:
            self._stamp_cost(run)
        except Exception as e:
            logger.debug("LangSmith cost stamp failed: %s", e)
        super()._on_llm_end(run)

    def _stamp_cost(self, run: Run) -> None:
        """Write our cost for the call onto the usage of its output message.

        LangSmith prices a run off its own model table, which does not know our
        manifest's cards, and reads a chat run's usage off its output message
        ahead of ``metadata.usage_metadata``, which the base hook fills from
        that same message. The record is built and priced by the same functions
        billing uses, so the trace and the bill cannot disagree about a call.
        A BYOK or OAuth call keeps the list price of the provider its user pays,
        so cost per task holds whoever paid; ``billing_type`` on the run is
        what separates our spend from theirs.
        """
        outputs = run.outputs or {}
        generations = outputs.get("generations") or []
        # Billing prices one generation, and the base hook sums usage across
        # several with int-only arithmetic, which a float cost would break.
        if len(generations) != 1 or len(generations[0]) != 1:
            return
        message = (generations[0][0] or {}).get("message") or {}
        fields = message.get("kwargs") or {}
        usage = fields.get("usage_metadata")
        if not usage:
            return
        attribution = billing_attribution(run.metadata, run.id)
        attribution["started_at"] = run.start_time.isoformat()
        record = build_call_record(
            SimpleNamespace(
                usage_metadata=usage,
                response_metadata=fields.get("response_metadata") or {},
            ),
            outputs.get("llm_output"),
            attribution,
            run_id=run.id,
            parent_run_id=run.parent_run_id,
            timestamp=(run.end_time or datetime.now(timezone.utc)).isoformat(),
        )
        if record is None:
            return
        priced = price_call(record, self._pricer)
        if not priced.listed:
            # No card: say nothing rather than a zero that reads as free.
            return
        # Copy on write: the dicts may be shared with the caller's message.
        message["kwargs"] = {
            **fields,
            "usage_metadata": {**usage, **_cost_fields(priced)},
        }

    def _on_tool_end(self, run: Run) -> None:
        _settle_tool_run(run, None)
        super()._on_tool_end(run)

    def _errored_tool_run(self, error: BaseException, run_id: UUID) -> Run:
        run = super()._errored_tool_run(error, run_id)
        _settle_tool_run(run, error)
        return run


def _cost_fields(priced: PricedCall) -> dict[str, Any]:
    breakdown = priced.breakdown

    def cost(key: str) -> float:
        entry = breakdown.get(key)
        return float(entry.get("cost", 0.0)) if isinstance(entry, dict) else 0.0

    input_cost = 0.0
    for key in _INPUT_COST_KEYS:
        if key in breakdown:
            input_cost += cost(key)
    fields: dict[str, Any] = {
        "input_cost": input_cost,
        "output_cost": cost("output"),
        "total_cost": priced.cost,
    }
    details = {
        "cache_read": cost("cached_input"),
        "cache_creation": cost("cache_5m_creation") + cost("cache_1h_creation"),
    }
    details = {k: v for k, v in details.items() if v > 0}
    if details:
        fields["input_cost_details"] = details
    return fields


def _settle_tool_run(run: Run, error: BaseException | None) -> None:
    """Fold a finished tool run's outcome into its metadata.

    A tool that fails by returning text for the model (``mark_tool_failed``)
    gets ``run.error`` too, since LangSmith's own error rate reads only that;
    one whose target declined (``mark_tool_unavailable``) keeps its outcome
    and no error, so that rate and the alarms built on it leave it out.
    A graph interrupt raised inside a tool (AskUserQuestion, a confirmation)
    is a pause the tool resumes from, not a failure, so it gets no outcome
    and loses the error the base tracer gave it. Arguments the tool's schema
    rejected are the agent's mistake, settled as a refusal is.
    Never raises: the run is closed after this, and a failed stamp must not
    stop that.
    """
    try:
        metadata = run.metadata
        stamped: dict[str, Any] = {}
        if isinstance(error, GraphBubbleUp):
            run.error = None
            return
        if error is not None and _rejects_arguments(error):
            run.error = None
            error = None
        if error is not None:
            stamped[TOOL_OUTCOME] = "error"
            if not metadata.get(ERROR_CLASS):
                stamped[ERROR_CLASS] = type(error).__name__
        elif metadata.get(TOOL_OUTCOME) == "error":
            if not run.error:
                run.error = _short_error(metadata.get(ERROR_CLASS), run.outputs)
        elif metadata.get(TOOL_OUTCOME) != "unavailable":
            # A tool that refuses with an error status and no mark (an unknown
            # subagent, a bad workflow name) is answering the agent's mistake.
            stamped[TOOL_OUTCOME] = "ok"
        merge_run_metadata(run, stamped)
    except Exception as e:
        logger.debug("LangSmith tool outcome stamp failed: %s", e)


def _rejects_arguments(error: BaseException) -> bool:
    """Whether the tool refused the agent's arguments before it ran.

    langchain_core validates them inside the same ``try`` as the tool body, so
    a ValidationError alone may be the tool's own; only one raised while the
    input was parsed has that parse in its traceback.
    """
    if not isinstance(error, ValidationError):
        return False
    tb = error.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code is _PARSE_INPUT:
            return True
        tb = tb.tb_next
    return False


def _short_error(error_class: Any, outputs: Mapping[str, Any] | None) -> str:
    output = (outputs or {}).get("output")
    if isinstance(output, BaseMessage):
        output = output.text
    # Only the head: a failed tool's output can run to megabytes.
    text = (output if isinstance(output, str) else "").lstrip()[:ERROR_TEXT_MAX]
    line = text.split("\n", 1)[0].strip()
    label = str(error_class or "error")
    return (f"{label}: {line}" if line else label)[:ERROR_TEXT_MAX]


def with_run_tracer(config: dict[str, Any]) -> dict[str, Any]:
    """``config`` with a ``NonRetainingTracer`` added to its callbacks.

    Unchanged when tracing is off, or when the callbacks already carry a
    tracer or are a callback manager, which brings its parent's tracer. The
    tracer is built from the context langchain_core's ``_configure`` reads for
    its own (enabled, project or agent address, client, tags and metadata), so
    it has to be called where the run starts, and once per run, since its
    ``order_map`` lives as long as it does.
    """
    callbacks = config.get("callbacks")
    if callbacks is not None and not isinstance(callbacks, list):
        return config
    if any(isinstance(cb, LangChainTracer) for cb in callbacks or ()):
        return config
    tracer = _ambient_equivalent()
    if tracer is None:
        return config
    return {**config, "callbacks": [*(callbacks or []), tracer]}


def _ambient_equivalent() -> NonRetainingTracer | None:
    try:
        from langchain_core.tracers.context import (
            _get_tracer_kwargs,
            _tracing_v2_is_enabled,
            tracing_v2_callback_var,
        )

        # A tracer set by ``tracing_v2_enabled`` is what _configure would add.
        if not _tracing_v2_is_enabled() or tracing_v2_callback_var.get() is not None:
            return None
        # The same kwargs _configure builds its own tracer from, so the
        # destination (a parent's project and address, else the ambient one)
        # resolves exactly as it would without us.
        return NonRetainingTracer(**_get_tracer_kwargs())
    except Exception as e:
        # langchain_core then builds its own tracer, which still traces the
        # run; it only holds the tree again.
        logger.warning("Falling back to the ambient LangSmith tracer: %r", e)
        return None
