"""Runtime context: what the model is told about the world it is running in.

Three surfaces, split by how long the content lives.
``BaselineContextMiddleware`` freezes the per-thread baseline (agent.md, the
memory indices, the memo pointer, identity, and the harness-authored blocks
in ``harness_blocks.py``) into ``runtime_baseline`` at the turn
boundary and re-renders it byte-identically as a system block on every call. Everything that is true for one turn is a durable row written once into
``messages`` (``durable.py``) so it keeps its place in time: the turn's own
anchor from ``TurnContextMiddleware``, whatever moved underneath the
baseline, and market_watch's live-price stamps. ``RowCarrierMiddleware``
carries those rows in the shape the model reads. ``carrier.py`` decides the
shape: an operator role where one is proven, else a ``<system-reminder>`` block
inside the trailing user message.
"""

from .baseline import (
    AGENT_MD_PATH,
    BLOCK_KINDS,
    DEFAULT_REBUILD_AFTER_UPDATES,
    MAX_AGENT_MD_SIZE,
    MAX_MEMORY_BLOCK_SIZE,
    BaselineContextMiddleware,
    BaselineSources,
    BlockReader,
    MemoSource,
    MemoryTierSource,
)
from .carrier import (
    Carrier,
    CarrierShape,
    ComposedRequest,
    PinSite,
    apply_breakpoint,
    compose_request,
    may_hold_breakpoint,
    resolve_carrier,
)
from .changes import (
    DIFF_MAX_LINES,
    DIFF_REWRITE_LINES,
    UPDATE_SCHEMA_VERSION,
    SourceRead,
    render_diff,
    sha256_text,
)
from .clock import resolve_preferred_market
from .durable import (
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    DurableUpdate,
    build_update_message,
    is_runtime_update_message,
    last_stated,
    render_update_row,
    rows_in_view,
    runtime_update_from_message,
)
from .envelope import (
    ENVELOPE_CLOSE,
    ENVELOPE_HARD_CAP_TOKENS,
    ENVELOPE_OPEN,
    ENVELOPE_STEADY_TOKENS,
    count_tokens,
    frame_reminder,
    render_stamp,
)
from .epoch import BaselineEpoch, Observations, advance_epoch
from .frozen_prompt import FrozenPromptMiddleware
from .harness_blocks import HARNESS_BLOCKS, HarnessBlock
from .state import STATE_BASELINE
from .surface import (
    KNOWN_SURFACES,
    Surface,
    is_known_surface,
    parse_surface,
    split_surface,
)
from .tail import RowCarrierMiddleware
from .templates import render_template
from .turn import (
    ELAPSED_MIN_GAP,
    NON_CHANGE_ROW_KINDS,
    NOTICE_ROW_KINDS,
    SUBAGENTS_ROW_KIND,
    TURN_ROW_KIND,
    TURN_SCHEMA_VERSION,
    WAKEUP_CANCELLED_ROW_KIND,
    TimeUpToolStopMiddleware,
    TurnContextMiddleware,
)
from .turn_context import TurnContext

__all__ = [
    "AGENT_MD_PATH",
    "BLOCK_KINDS",
    "DEFAULT_REBUILD_AFTER_UPDATES",
    "DIFF_MAX_LINES",
    "DIFF_REWRITE_LINES",
    "ELAPSED_MIN_GAP",
    "ENVELOPE_CLOSE",
    "ENVELOPE_HARD_CAP_TOKENS",
    "ENVELOPE_OPEN",
    "ENVELOPE_STEADY_TOKENS",
    "HARNESS_BLOCKS",
    "KNOWN_SURFACES",
    "MAX_AGENT_MD_SIZE",
    "MAX_MEMORY_BLOCK_SIZE",
    "NON_CHANGE_ROW_KINDS",
    "NOTICE_ROW_KINDS",
    "RUNTIME_UPDATE_KEY",
    "RUNTIME_UPDATE_SOURCE",
    "STATE_BASELINE",
    "SUBAGENTS_ROW_KIND",
    "TURN_ROW_KIND",
    "TURN_SCHEMA_VERSION",
    "UPDATE_SCHEMA_VERSION",
    "WAKEUP_CANCELLED_ROW_KIND",
    "BaselineContextMiddleware",
    "BaselineEpoch",
    "BaselineSources",
    "BlockReader",
    "Carrier",
    "CarrierShape",
    "ComposedRequest",
    "DurableUpdate",
    "FrozenPromptMiddleware",
    "HarnessBlock",
    "MemoSource",
    "MemoryTierSource",
    "Observations",
    "PinSite",
    "RowCarrierMiddleware",
    "SourceRead",
    "Surface",
    "TimeUpToolStopMiddleware",
    "TurnContext",
    "TurnContextMiddleware",
    "advance_epoch",
    "apply_breakpoint",
    "build_update_message",
    "compose_request",
    "count_tokens",
    "frame_reminder",
    "is_known_surface",
    "is_runtime_update_message",
    "last_stated",
    "may_hold_breakpoint",
    "parse_surface",
    "render_diff",
    "render_template",
    "render_stamp",
    "render_update_row",
    "resolve_carrier",
    "resolve_preferred_market",
    "rows_in_view",
    "runtime_update_from_message",
    "sha256_text",
    "split_surface",
]
