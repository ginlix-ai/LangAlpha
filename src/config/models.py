"""
Pydantic models for infrastructure configuration.

These models define the schema for config.yaml (infrastructure settings).
"""

from datetime import date
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, field_validator, model_validator

#: Provider names a release renamed, each to its current name. Refused, never read,
#: so a config or ruleset naming one fails the boot.
RENAMED_PROVIDERS: dict[str, str] = {"ginlix-data": "langalpha-data"}


def current_provider_name(name: str | None) -> str | None:
    """*name*, refused when a release renamed it."""
    if name in RENAMED_PROVIDERS:
        raise ValueError(f"provider {name!r} was renamed to {RENAMED_PROVIDERS[name]!r}: use the new name")
    return name


class BackgroundExecutionConfig(BaseModel):
    """Configuration for background workflow execution."""

    max_concurrent_workflows: int = Field(
        default=100, description="Maximum number of concurrent background workflows"
    )
    workflow_result_ttl: int = Field(
        default=86400, description="Workflow result retention time in seconds (24 hours)"
    )
    abandoned_workflow_timeout: int = Field(
        default=3600,
        description="Auto-cleanup timeout for workflows with no active connections (1 hour)",
    )
    cleanup_interval: int = Field(
        default=300, description="Background cleanup task interval in seconds (5 minutes)"
    )
    enable_intermediate_storage: bool = Field(
        default=True, description="Store intermediate results during execution"
    )
    max_stored_messages_per_agent: int = Field(
        default=150000, description="Maximum events to buffer per workflow"
    )
    max_stream_bytes_per_run: int = Field(
        default=96 * 1024 * 1024,
        description=(
            "Maximum bytes one run's event stream may hold in Redis, counting "
            "every copy a frame is kept as (a subagent's task stream keeps up "
            "to three); a run past it finalizes error(transport_lost), as at "
            "the event cap"
        ),
    )
    event_storage_backend: Literal["redis", "memory"] = Field(
        default="redis", description='Backend for event buffering: "redis" or "memory"'
    )
    subagent_collector_timeout: float = Field(
        default=120, description="Initial subagent collector timeout in seconds"
    )
    subagent_orphan_collector_timeout: float = Field(
        default=600, description="Orphan subagent collector idle timeout in seconds"
    )

    # Streaming & queue settings
    subagent_task_max_wait: int = Field(
        default=30, description="Max seconds to wait for subagent task to appear in registry"
    )

    # Subagent event capture
    in_memory_event_tail_max_events: int = Field(
        default=1000,
        description=(
            "Max captured-event records held in the in-memory hot tail per subagent task. "
            "Older events spill to Redis."
        ),
    )
    spill_subagent_events_to_redis: bool = Field(
        default=True,
        description=(
            "Kill-switch: when True, subagent events are also spilled to Redis so "
            "the in-memory tail stays bounded without losing events for reconnect/persistence."
        ),
    )

    # Timeout settings
    sse_drain_timeout: float = Field(
        default=30.0, description="Seconds to wait for per-task SSE drain before clearing events"
    )
    shutdown_timeout: float = Field(
        default=50.0, description="Max seconds for graceful shutdown of running workflows"
    )
    checkpoint_flush_timeout: float = Field(
        default=10.0, description="Timeout (seconds) for checkpoint state reads/writes"
    )
    admission_compaction_wait_timeout: float = Field(
        default=260.0,
        gt=0,
        description=(
            "Max seconds a new turn blocks at admission for an in-progress "
            "compaction (auto Tier-2 summarize or manual /compact|/offload) to "
            "finish before returning 'compacting' -> 409 retry. Summarize is a "
            "full LLM call over a large transcript and can run minutes, so this "
            "is generous to avoid spurious 409s; keep it under the governing "
            "in-request limits — the upstream nginx proxy_read_timeout and the "
            "client read timeout (the connection is held this long while the "
            "turn waits). NOTE: timeout_keep_alive (300s) is NOT the bound; it "
            "only caps idle time BETWEEN requests, not an in-flight request. "
            "Admission floors this at compaction_timeout + a small margin so a "
            "healthy in-progress compaction is never 409'd before its own call "
            "budget self-terminates and releases the guard."
        ),
    )
    compaction_timeout: float = Field(
        default=240.0,
        gt=0,
        description=(
            "Wall-clock budget (seconds) for a single compaction LLM call "
            "(auto Tier-2 summarize and manual /compact). The call is wrapped "
            "in asyncio.wait_for so a hung summarize fails naturally — auto "
            "emits a 'summarize error' (closing the window), manual raises -> "
            "HTTP 500 — instead of blocking the thread forever. Bounded below "
            "the httpx client timeout (600s) and below admission_compaction_"
            "wait_timeout so the call self-terminates before admission 409s."
        ),
    )
    wait_for_persistence_timeout: float = Field(
        default=30.0, description="Max seconds callers block waiting for persistence completion"
    )
    stop_drain_timeout: float = Field(
        default=1.5,
        description="Max seconds to drain killed-subagent events before the stop teardown sentinel",
    )
    max_workflow_retries: int = Field(
        default=3, description="Max transient-error retry count for workflow execution"
    )


class RedisTTLConfig(BaseModel):
    """Redis TTL settings for various cache types."""

    results_list: int = Field(default=300, description="Results list cache TTL (5 minutes)")
    result_detail: int = Field(default=900, description="Result detail cache TTL (15 minutes)")
    metadata: int = Field(default=900, description="Metadata tags/tickers cache TTL (15 minutes)")
    metadata_summary: int = Field(
        default=600, description="Metadata summary cache TTL (10 minutes)"
    )
    workflow_events: int = Field(
        default=86400, description="Workflow event buffer TTL (24 hours)"
    )
    ohlcv: Dict[str, int] = Field(
        default_factory=dict, description="Per-interval OHLCV cache TTLs"
    )
    workflow_status: int = Field(
        default=3600, description="TTL for completed/cancelled workflow status keys (1 hour)"
    )
    cancel_flag: int = Field(
        default=300, description="TTL for workflow cancel flag (5 minutes)"
    )
    market_watch: int = Field(default=21600, description="Market watch list TTL in seconds")
    memo_metadata_inflight: int = Field(
        default=300,
        description=(
            "TTL for the cross-worker visibility key marking a memo metadata "
            "task as in flight (5 minutes)"
        ),
    )
    memo_metadata_cancel: int = Field(
        default=60,
        description=(
            "TTL for the cooperative cross-worker memo metadata cancel flag (1 minute)"
        ),
    )


class RedisSWRConfig(BaseModel):
    """Stale-While-Revalidate configuration for Redis cache."""

    soft_ttl_ratio: float = Field(
        default=0.6,
        description="Refresh when remaining TTL < this ratio of original",
    )


class RedisConfig(BaseModel):
    """Redis cache configuration."""

    cache_enabled: bool = Field(default=True, description="Enable/disable caching globally")
    max_connections: int = Field(default=10, description="Connection pool size")
    # Pools are split by connection LIFETIME, not by feature: short cache ops
    # must never queue behind a stream reader parked in XREAD BLOCK, and a
    # subscriber holding a connection for 30 minutes must not sit in either.
    stream_max_connections: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Pool size for blocking stream readers (XREAD)",
    )
    pubsub_max_connections: int = Field(
        default=150,
        ge=1,
        le=10000,
        description="Pool size for long-lived pub/sub subscriptions",
    )
    pool_timeout: float = Field(
        default=2.0,
        gt=0,
        le=60,
        description="Seconds a caller queues for a free cache connection before failing",
    )
    socket_timeout: int = Field(
        default=5, description="Redis socket read/write timeout in seconds"
    )
    socket_connect_timeout: int = Field(
        default=5, description="Redis socket connect timeout in seconds"
    )
    ttl: RedisTTLConfig = Field(default_factory=RedisTTLConfig)
    swr: RedisSWRConfig = Field(default_factory=RedisSWRConfig)


class MarketDataProviderConfig(BaseModel):
    """Configuration for a single market data provider.

    Per-capability market lists override ``markets`` for that capability
    only (None = no override). Market tokens: region codes plus ``all``
    and ``non-us``.
    """

    name: str
    markets: List[str] = Field(default_factory=lambda: ["all"])
    intraday_markets: Optional[List[str]] = None
    daily_markets: Optional[List[str]] = None
    snapshot_markets: Optional[List[str]] = None

    _current_name = field_validator("name")(current_provider_name)


class MarketDataConfig(BaseModel):
    """Market data provider chain configuration."""

    providers: List[MarketDataProviderConfig] = Field(default_factory=list)


class NewsDataConfig(BaseModel):
    """News data provider chain configuration."""

    providers: List[MarketDataProviderConfig] = Field(default_factory=list)


class NewsPollFeedConfig(BaseModel):
    """One global feed kept warm by the news refresh poller.

    ``provider`` None targets the provider chain (Market general feed); a name
    (e.g. ``tickertick``) targets that source directly. Always polled with no
    tickers, so it maps to a global cache key — ``news:tickertick:general:50``
    with a provider, ``news:general:50`` without one.
    """

    provider: str | None = Field(default=None)
    limit: int = Field(default=50, ge=1, le=100)

    _current_provider = field_validator("provider")(current_provider_name)


class NewsPollConfig(BaseModel):
    """News refresh poller — delta-merges the latest page into a rolling buffer."""

    enabled: bool = Field(default=True)
    interval_seconds: int = Field(default=60, ge=10)
    max_items: int = Field(default=100, ge=1, le=500)
    feeds: List[NewsPollFeedConfig] = Field(default_factory=list)


class OrderReconcileConfig(BaseModel):
    """Order reconciliation sweep -- asks each brokerage what became of an order.

    The two grace windows are the values with teeth. ``submitting_grace_seconds``
    decides when a call that left the relay and never came back is old enough
    to be settled on the vendor's word rather than waited on, so it must
    comfortably exceed the slowest order call a brokerage answers; its floor
    sits above the relay's 55 s wall clock, so no setting can judge a call the
    relay is still carrying. ``approved_grace_seconds`` decides when an approval
    no call spent is refused, so it must exceed the gap between an answer and
    its tool call. A proposal whose run ended without asking waits the same.

    The other two bound the vendor half. ``max_concurrent_groups`` counts
    accounts rather than calls, because two accounts are two sets of brokerage
    credentials and share no rate limit while two reads of one account do, and
    it bounds one process rather than the fleet, so a deployment running W
    workers can have W times this many accounts in flight. Its ceiling is set
    against the app's connection pool, since an account in flight holds an
    advisory-lock session on top of whatever its writes take.
    ``group_timeout_seconds`` is the only thing that bounds a pass at all: a
    listing walks up to twenty calls and the relay gives each one its own wall
    clock, so the floor is that wall clock, and the value should still clear a
    full paginated history or a user with a long one is cut off every pass.
    """

    enabled: bool = Field(default=True)
    interval_seconds: int = Field(default=60, ge=5)
    submitting_grace_seconds: int = Field(default=120, ge=90)
    approved_grace_seconds: int = Field(default=600, ge=60)
    open_after_seconds: int = Field(default=60, ge=5)
    batch_limit: int = Field(default=50, ge=1, le=500)
    match_window_seconds: int = Field(default=900, ge=60)
    max_concurrent_groups: int = Field(default=8, ge=1, le=16)
    group_timeout_seconds: int = Field(default=120, ge=60)


class McpDescriptionsConfig(BaseModel):
    """The background service that has a model describe MCP servers.

    ``user_servers`` covers the servers users add themselves, which run on
    each user's own model; off, only shipped brokerages are described. With
    ``enabled`` off nothing is queued and every row keeps its own text.
    """

    enabled: bool = Field(default=True)
    user_servers: bool = Field(default=True)
    interval_seconds: int = Field(default=20, ge=5)


class ThreadSearchConfig(BaseModel):
    """The sweep that keeps conversation search's index whole.

    A turn is indexed as it settles, in the process that ran it; each pass
    finds the recently settled turns that trigger missed (a worker lost, a
    deploy) and retries the answers whose read failed, and the deployment
    walks every thread, the backfill for turns that settled before search
    existed, then again each day. One worker sweeps at a time. Each batch
    takes ``batch_threads`` threads; the walk goes in slices of a few
    seconds' work, each resting at least as long as it took and never less
    than ``pause_seconds``, so a backfill never holds the database or the
    checkpoint reader for long. Off, turns are still indexed as they settle.
    """

    enabled: bool = Field(default=True)
    interval_seconds: int = Field(default=120, ge=10)
    batch_threads: int = Field(default=10, ge=1, le=200)
    pause_seconds: float = Field(default=0.5, ge=0)


class FeatureFlagOverride(BaseModel):
    """Deployment override for a code-declared feature (src/config/features.py).

    Unset fields inherit the catalog defaults, so a config.yaml entry only
    needs the fields it wants to change.
    """

    enabled: Optional[bool] = Field(
        default=None, description="Kill switch: false turns the feature off for everyone"
    )
    gate: Optional[Literal["none", "opt_in", "opt_out", "plan", "grant"]] = Field(
        default=None,
        description=(
            "Access model while enabled: everyone / user opt-in / user opt-out "
            "/ platform plan tier / granted to the user"
        ),
    )
    min_tier: Optional[int] = Field(
        default=None, description="Plan gate only: minimum platform access tier"
    )


class ModelsPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    allowed: Optional[List[StrictStr]] = Field(
        default=None, description="The models a turn may run; null allows any"
    )


class OwnModelAccessPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    api_keys: StrictBool = Field(
        default=True,
        description="The user's own provider API keys, custom models and custom providers",
    )
    subscriptions: StrictBool = Field(
        default=True,
        description="The user's own model sign-ins and a connected Claude Code subscription",
    )


class OrgLibraryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    publish: StrictBool = Field(
        default=True,
        description="Publishing the user's threads, files and skills to their organization's library",
    )


class SharedWorkspacesPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    create: StrictBool = Field(
        default=True,
        description="Creating a workspace shared with others in the user's organization",
    )


class PolicyLayer(BaseModel):
    """Restrictions on what users may do, one layer of the effective policy.

    The deployment's (``policy:`` in config.yaml) and an organization's share
    this schema. Closed and strictly typed at every level: a key or a value
    this version cannot enforce must fail loudly, never read as no restriction.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    models: ModelsPolicy = Field(default_factory=ModelsPolicy)
    own_model_access: OwnModelAccessPolicy = Field(default_factory=OwnModelAccessPolicy)
    public_links: StrictBool = Field(
        default=True,
        description="Links to the user's threads and files that open without signing in as them",
    )
    trading: StrictBool = Field(
        default=True,
        description="The agent placing, changing or cancelling a live order for the user",
    )
    own_machines: StrictBool = Field(
        default=True,
        description="The user's work running on a machine the user brings (an SSH computer)",
    )
    user_connectors: StrictBool = Field(
        default=True,
        description=(
            "MCP servers the user adds themselves, from a plugin or an import "
            "included; a brokerage is governed by trading instead"
        ),
    )
    org_library: OrgLibraryPolicy = Field(default_factory=OrgLibraryPolicy)
    shared_workspaces: SharedWorkspacesPolicy = Field(
        default_factory=SharedWorkspacesPolicy
    )


class MarketWatchConfig(BaseModel):
    """Market watch tuning (the on/off flag lives in the features registry)."""

    min_interval_seconds: int = Field(
        default=25, ge=5, description="Throttle between injections per thread"
    )
    max_symbols: int = Field(default=10, ge=1, le=50, description="Watch list cap")
    target_warning_minutes: int = Field(
        default=60,
        ge=1,
        le=720,
        description="Minutes before a target lapses that the thread is told",
    )


class ClaudeCodeConfig(BaseModel):
    """Claude Code on cloud computers (the runtime's own switch is the
    ``claude_code_runtime`` feature). With ``sandbox_placement`` the CLI of a
    turn on a cloud computer runs inside that computer's own sandbox, and only
    on Medium computers or larger (``hosted.SANDBOX_MIN_TIER``)."""

    stored_custody: bool = Field(
        default=False,
        description="Run Claude Code turns on cloud computers on a setup token the user lets the server store",
    )
    sandbox_placement: bool = Field(
        default=False,
        description="Run a cloud computer's Claude Code CLI inside the computer's own sandbox, not in the worker",
    )
    hosted_turns_per_user: int = Field(
        default=3, ge=1, description="Claude Code turns one user runs at once on cloud computers"
    )
    cli_memory_mb: int = Field(
        default=800,
        ge=256,
        description="Memory each Claude Code CLI on a cloud computer is counted at: its size's memory over this "
        "is how many it runs at once",
    )
    claude_oauth_ends_on: Optional[date] = Field(
        default=None,
        description="The day the Claude sign-in (claude-oauth) stops working on this deployment; set, the app "
        "marks it legacy and asks the users signed in with it to move to Claude Code",
    )


class WorkflowOrchestrationConfig(BaseModel):
    """Caps and timeouts for RunWorkflow programmatic subagent runs."""

    enabled: bool = Field(
        default=True,
        description="Expose the RunWorkflow tool to the PTC main agent",
    )
    memory_limit_mb: int = Field(
        default=128,
        ge=16,
        le=1024,
        description="QuickJS heap limit per workflow run (MB)",
    )
    cpu_budget_s: float = Field(
        default=30.0,
        gt=0,
        le=600,
        description="Continuous-JS interrupt budget in seconds (host awaits excluded)",
    )
    schema_max_bytes: int = Field(
        default=4096,
        ge=256,
        le=65536,
        description="Max serialized size of a dispatch schema",
    )
    schema_max_depth: int = Field(
        default=5,
        ge=1,
        le=16,
        description="Max nesting depth of a dispatch schema",
    )
    schema_max_properties: int = Field(
        default=32,
        ge=1,
        le=256,
        description="Max keys at any schema object level",
    )
    max_dispatches_per_run: int = Field(
        default=64, ge=1, le=256, description="Max subagent dispatches per run"
    )
    max_concurrent_children: int = Field(
        default=8, ge=1, le=32, description="Max concurrently running children"
    )
    child_timeout: int = Field(
        default=1800, ge=1, le=7200, description="Per-child subagent timeout in seconds"
    )
    run_timeout: int = Field(
        default=16200,
        ge=1,
        le=86400,
        description=(
            "Whole-run wall-clock timeout in seconds; must exceed "
            "child_timeout or the run dies before any child of it can. The "
            "default clears several sequential waves of children"
        ),
    )
    max_runs_per_thread: int = Field(
        default=2, ge=1, le=8, description="Max concurrently active runs per thread"
    )
    max_result_bytes: int = Field(
        default=1024 * 1024,
        ge=1024,
        le=16 * 1024 * 1024,
        description=(
            "Per-child result dump cap; bounds what the script manipulates in "
            "JS, not what any reader is handed"
        ),
    )
    max_summary_bytes: int = Field(
        default=96 * 1024,
        ge=1024,
        le=1024 * 1024,
        description=(
            "Cap on the run summary — the text handed to the model and "
            "archived for TaskOutput; the full value stays in result.json"
        ),
    )
    max_prompt_chars: int = Field(
        default=400_000,
        ge=1,
        le=1_000_000,
        description="Per-dispatch prompt length cap",
    )
    max_script_bytes: int = Field(
        default=200 * 1024,
        ge=1024,
        le=4 * 1024 * 1024,
        description="Workflow script size cap in bytes",
    )

    @model_validator(mode="after")
    def _run_timeout_outlasts_one_child(self) -> "WorkflowOrchestrationConfig":
        """Reject a run timeout no child can time out inside.

        At or below one ``child_timeout`` the run-level kill always beats the
        per-child one, so every timeout is reported as the blunt whole-run
        failure and no child is ever named. The bound stops at one child on
        purpose: covering a worst-case wave would assume every child burns its
        full timeout with none overlapping, which is a property of the script
        rather than of the configuration — and asserting it here left in-range
        settings (``max_concurrent_children: 1``) unsatisfiable at every
        ``run_timeout``. The shipped default still carries several waves of
        margin, as a recommendation rather than a rule.
        """
        if self.run_timeout <= self.child_timeout:
            raise ValueError(
                f"workflow.run_timeout={self.run_timeout} must exceed "
                f"child_timeout={self.child_timeout}, or the run dies before "
                "any child of it can"
            )
        return self


class SharedWorkspacesConfig(BaseModel):
    """Limits on shared workspaces (the on/off flag lives in the features
    registry)."""

    max_per_organization: int = Field(
        default=20,
        ge=0,
        description=(
            "Live shared workspaces one organization may hold; each has a "
            "computer of its own that counts against no member's limit"
        ),
    )


class DriveHistoryConfig(BaseModel):
    """What an ordinary Drive file keeps of its history: the copy from before
    its last save, for a while, as a one-step undo. A memo keeps every
    version, whatever its size."""

    max_bytes: int | None = Field(
        default=100 * 1024 * 1024,
        ge=1,
        description=(
            "A replaced copy this size or larger is not kept as an undo copy; "
            "null keeps every size"
        ),
    )
    undo_days: int = Field(
        default=7,
        ge=1,
        description=(
            "Days the copy from before a file's last save is kept. The agent's "
            "instructions, its Drive doc page and the CLI state the defaults, "
            "7 days and 100 MB"
        ),
    )
    replaced_grace_seconds: int = Field(
        default=3600,
        ge=0,
        description=(
            "Seconds the bytes of a copy that is not kept stay readable by "
            "their digest, for a reader already holding them, before they are freed"
        ),
    )


class DriveConfig(BaseModel):
    """Limits on the Drive (the on/off flag lives in the features registry)."""

    history: DriveHistoryConfig = Field(default_factory=DriveHistoryConfig)

    max_file_bytes: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Largest file the Drive keeps; unset, a file may be as large as the "
            "space's allowance when object storage takes uploads in parts"
        ),
    )
    max_request_bytes: int = Field(
        default=50 * 1024 * 1024,
        ge=1,
        description=(
            "Largest file one request carries whole in a worker's memory: a "
            "plain save, the file mount's save and the agent's own save"
        ),
    )
    max_file_bytes_in_database: int = Field(
        default=10 * 1024 * 1024,
        ge=1,
        description=(
            "Largest binary file when no object storage is configured, since "
            "the database then holds its bytes"
        ),
    )
    max_space_bytes: int = Field(
        default=5 * 1024 * 1024 * 1024,
        ge=0,
        description=(
            "Bytes one space's live files may keep with object storage, their "
            "old versions included; the trash is not counted. A personal "
            "space whose owner the deployment's account service gives an "
            "allowance takes that instead"
        ),
    )
    max_space_bytes_in_database: int = Field(
        default=512 * 1024 * 1024,
        ge=0,
        description=(
            "Bytes one space's live files may keep when no object storage is "
            "configured, whatever allowance the account service gives, since "
            "the database then holds them"
        ),
    )
    max_entries: int = Field(
        default=100_000,
        ge=1,
        description="Files and folders one space may hold, the trash included",
    )
    trash_days: int = Field(
        default=14, ge=1, description="Days a trashed entry is kept before it is purged"
    )
    trash_max_ratio: float = Field(
        default=1.0,
        ge=0,
        description=(
            "Bytes the trash alone may keep, as a multiple of the space's "
            "allowance; past it the oldest trashed items are purged early"
        ),
    )


class PromptConfig(BaseModel):
    """Deployment-wide prompt settings."""

    guidance: Literal["auto", "lean", "detailed"] = Field(
        default="auto",
        description=(
            "Prompt scaffolding level for this deployment. 'auto' defers to the "
            "model's prompt_guidance in models.json; 'lean'/'detailed' pin it. A "
            "per-user model_preference.prompt_guidance still wins over this — a "
            "profile for the running model first, then the account-wide value."
        ),
    )


class CliApprovalsConfig(BaseModel):
    """How long what a computer's ``la`` asks the user to approve waits.

    A request holds open for ``hold_seconds`` for the answer, under the
    edge's limit on one request, and the CLI asks again until the request
    expires."""

    hold_seconds: float = Field(
        default=50.0, gt=0, le=90, description="How long one request waits for the user's answer"
    )
    pending_minutes: int = Field(
        default=30, ge=1, description="How long a request waits for an answer before it expires"
    )
    approved_minutes: int = Field(
        default=5, ge=1, description="How long an approval waits for its command to use it"
    )
    max_pending: int = Field(
        default=5, ge=1, description="Requests one user may have waiting at once"
    )

    @model_validator(mode="after")
    def _a_hold_ends_before_its_request(self) -> "CliApprovalsConfig":
        # A request that expired mid-hold would be asked again under a new id.
        if self.hold_seconds >= self.pending_minutes * 60:
            raise ValueError("hold_seconds must be shorter than pending_minutes")
        return self


class InfrastructureConfig(BaseModel):
    """Root model for infrastructure configuration (config.yaml)."""

    model_config = ConfigDict(extra="allow")

    prompt: PromptConfig = Field(default_factory=PromptConfig)

    # Application Settings
    debug: bool = Field(default=False, description="Debug mode flag")
    workflow_timeout: int = Field(default=21600, description="Workflow timeout in seconds")
    sse_keepalive_interval: float = Field(
        default=15.0, description="SSE keepalive interval in seconds"
    )

    # Feature Flags
    result_log_db_enabled: bool = Field(
        default=True, description="Enable result logging to database"
    )
    redis_warm_on_startup: bool = Field(
        default=True, description="Enable Redis cache warming on startup"
    )
    brokerage_marks_warm_on_startup: bool = Field(
        default=False, description="Fetch the built-in brokerages' logos on startup"
    )
    langsmith_tracing: bool = Field(default=False, description="Enable LangSmith tracing")
    langsmith_full_payloads: bool = Field(
        default=False,
        description="Trace middleware inputs and graph nodes' messages to LangSmith",
    )
    market_watch: MarketWatchConfig = Field(default_factory=MarketWatchConfig)
    claude_code: ClaudeCodeConfig = Field(default_factory=ClaudeCodeConfig)
    cli_approvals: CliApprovalsConfig = Field(default_factory=CliApprovalsConfig)

    # User-facing product features (deployment overrides of the code catalog
    # in src/config/features.py). Distinct from the infra toggles above, which
    # are operator-only and never exposed to users.
    features: Dict[str, FeatureFlagOverride] = Field(default_factory=dict)

    # What every user of this deployment is restricted to; an organization's
    # policy can only narrow it (src/server/services/policy.py).
    policy: Optional[PolicyLayer] = None

    shared_workspaces: SharedWorkspacesConfig = Field(
        default_factory=SharedWorkspacesConfig
    )
    drive: DriveConfig = Field(default_factory=DriveConfig)

    # SSE Event Logging
    sse_event_log_enabled: bool = Field(default=True, description="Enable SSE event logging")
    sse_event_log_level: str = Field(default="info", description="SSE event log level")

    # General Application Logging
    log_level: str = Field(default="error", description="Root logger level")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    module_log_levels: Dict[str, str] = Field(
        default_factory=dict, description="Module-specific log levels"
    )

    # CORS Settings
    allowed_origins: List[str] = Field(
        default_factory=lambda: ["*"], description="Allowed CORS origins"
    )

    # Background Execution
    background_execution: BackgroundExecutionConfig = Field(
        default_factory=BackgroundExecutionConfig
    )

    # Redis Cache
    redis: RedisConfig = Field(default_factory=RedisConfig)

    # Market Data
    market_data: MarketDataConfig = Field(default_factory=MarketDataConfig)
    news_data: NewsDataConfig = Field(default_factory=NewsDataConfig)
    news_poll: NewsPollConfig = Field(default_factory=NewsPollConfig)

    # RunWorkflow orchestration caps
    workflow: WorkflowOrchestrationConfig = Field(
        default_factory=WorkflowOrchestrationConfig
    )

    mcp_descriptions: McpDescriptionsConfig = Field(default_factory=McpDescriptionsConfig)
    # Order reconciliation sweep
    order_reconcile: OrderReconcileConfig = Field(
        default_factory=OrderReconcileConfig
    )
    # Conversation search index sweep
    thread_search: ThreadSearchConfig = Field(default_factory=ThreadSearchConfig)
