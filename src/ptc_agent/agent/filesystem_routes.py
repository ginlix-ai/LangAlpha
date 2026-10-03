"""The store-backed routes mounted over a build's sandbox filesystem.

Memory, memo, workflows and the user's own data all reach the agent as files
under the sandbox root, each served by a store route rather than the sandbox.
Which of them exist is a single question about identity, so the gates that
answer it live here with the mounting they gate.

The user tier hangs off the computer root; workspace memory hangs off the
turn's own folder, so two workspaces sharing a computer do not share a
memory file. Nothing here touches disk: moving a mount moves a prefix, and
the namespace behind it -- which is what holds the bytes -- never changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ptc_agent.agent.backends import (
    CompositeFilesystemBackend,
    NamespaceFactory,
    RequestScopedStoreCache,
    StoreBackend,
    WorkflowsBackend,
    prebuilt_workflow_backend,
    workflow_namespace,
)
from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.channels import ChannelsBackend
from ptc_agent.agent.backends.db_json_route import DbJsonRoute
from ptc_agent.agent.backends.user_data import UserDataBackend
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    get_prebuilt_workflows,
)
from ptc_agent.agent.middleware.runtime_context import (
    BaselineSources,
    MemoSource,
    MemoryTierSource,
)
from ptc_agent.core.paths import (
    MEMO_INDEX_FILENAME,
    MEMO_USER_DIR,
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    WORKFLOW_DIR,
    SandboxLayout,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox.livefs_mount import CallContext

# The routes that serve the user's database rows as files, one per directory
# of ``USER_DATA_DIRS``.
USER_DATA_ROUTES = (UserDataBackend, AutomationsBackend)


def route_for(path: str) -> type[DbJsonRoute] | None:
    """The route serving ``path`` (relative to the sandbox root), if it may
    be one of the user's data files, whether or not one is there."""
    directory, _, name = path.rpartition("/")
    return next(
        (r for r in USER_DATA_ROUTES if r.directory == directory and r.file_named(name) is not None),
        None,
    )


@dataclass(frozen=True)
class IdentityGates:
    """Which identity-derived surfaces a build gets.

    Memory, memo and the workflow store are opt-in on identity: without a user
    id they are disabled entirely rather than falling back to a shared
    namespace that would cross-pollinate unauthenticated sessions.
    """

    user_memory: bool
    workspace_memory: bool
    memo: bool
    user_data: bool
    workflow: bool
    workflow_fs: bool
    workflow_tool: bool
    # The user's chat-app settings, for the file tools only: the mount never
    # asks for them, so they are no mount tier.
    channels: bool = False

    @property
    def memory(self) -> bool:
        return self.user_memory or self.workspace_memory


def resolve_identity_gates(
    *,
    store: Any | None,
    user_id: str | None,
    workspace_id: str | None,
    disable_subagents: bool,
    channels: bool = False,
) -> IdentityGates:
    from src.config.settings import get_workflow_orchestration_config

    workflow = get_workflow_orchestration_config().enabled
    identified = store is not None and bool(user_id)
    return IdentityGates(
        user_memory=identified,
        workspace_memory=identified and bool(workspace_id),
        memo=identified,
        # Independent of `store`: the user's data files (profile and
        # automations) talk to the application DB tables, not the LangGraph
        # store.
        user_data=bool(user_id),
        workflow=workflow,
        workflow_fs=workflow and identified,
        # RunWorkflow dispatches subagents, so it drops with the recursion
        # gate. The skill that advertises it is gated on the same flag:
        # advertising a skill whose tool this build never registers strands
        # the agent.
        workflow_tool=workflow and not disable_subagents,
        # Asked for by a build whose deployment has the channel gateway.
        channels=channels and bool(user_id),
    )


def build_filesystem_backend(
    *,
    backend: Any,
    gates: IdentityGates,
    store: Any | None,
    user_id: str | None,
    workspace_id: str | None,
    layout: WorkspaceLayout | None = None,
    call: CallContext | None = None,
) -> tuple[Any, BaselineSources | None]:
    """Mount the store-backed routes over the sandbox filesystem.

    Returns the backend the filesystem tools should see, and the store-tier
    sources the runtime-context baseline reads at the turn boundary (the
    memory namespaces and the memo catalog), one value because they are all
    derived from the same gate resolution and namespace closures.

    ``layout`` is the turn's workspace folder, which the workspace memory
    mount hangs off. Omitted, the workspace owns the computer root.
    ``call`` is who the files are written for, which a new automation
    defaults to: this workspace, this conversation for ``thread: "current"``,
    and this clock, else the user's own.
    """
    if not (
        gates.memory
        or gates.memo
        or gates.user_data
        or gates.workflow
        or gates.channels
    ):
        return backend, None

    # One cache per agent (≈ per request). Shared by every memory/memo
    # backend route and the baseline's turn-boundary read, so a turn pays
    # one set of store reads even when the agent also touches the files.
    # Agent-side writes invalidate the affected key so reads in later
    # rounds within the same turn see the fresh value.
    store_cache: RequestScopedStoreCache | None = (
        RequestScopedStoreCache()
        if (gates.memory or gates.memo or gates.workflow_fs)
        else None
    )
    sandbox_root = backend.computer_root.rstrip("/")
    layout = layout or SandboxLayout(sandbox_root).for_workspace(None)
    workspace_memory_dir = WorkspaceLayout.MEMORY_DIR

    # INVARIANT: these closures capture identity at agent-creation time
    # (``user_id`` is bound once per call). Safe only because one PTCAgent
    # is built per request. If an orchestrator ever reuses agent instances
    # across requests, memory will cross-pollinate between users. Resolve
    # identity at call time (e.g. via `langgraph.runtime.get_runtime()`)
    # before introducing reuse.
    routes: list[Any] = []
    user_namespace_factory: NamespaceFactory | None = None
    workspace_namespace_factory: NamespaceFactory | None = None
    memo_namespace_factory: NamespaceFactory | None = None

    if gates.user_memory:

        def _user_namespace() -> tuple[str, ...]:
            return (user_id, "memory")

        user_namespace_factory = _user_namespace
        routes.append(
            StoreBackend(
                store=store,
                namespace_factory=_user_namespace,
                root_prefix=f"{sandbox_root}/{MEMORY_USER_DIR}/",
                sandbox_backend=backend,
                cache=store_cache,
            )
        )

    if gates.workspace_memory:

        def _workspace_namespace() -> tuple[str, ...]:
            return (user_id, "workspaces", workspace_id, "memory")

        workspace_namespace_factory = _workspace_namespace
        routes.append(
            StoreBackend(
                store=store,
                namespace_factory=_workspace_namespace,
                root_prefix=f"{layout.memory}/",
                sandbox_backend=backend,
                cache=store_cache,
            )
        )

    if gates.memo:

        def _memo_namespace() -> tuple[str, ...]:
            # Plural: avoid string-prefix collision with the
            # ``(user_id, "memory")`` tier in AsyncPostgresStore,
            # whose asearch is ``LIKE 'user_id.memo%'``.
            return (user_id, "memos")

        memo_namespace_factory = _memo_namespace
        routes.append(
            StoreBackend(
                store=store,
                namespace_factory=_memo_namespace,
                root_prefix=f"{sandbox_root}/{MEMO_USER_DIR}/",
                sandbox_backend=backend,
                read_only=True,
                read_only_error=(
                    "Memo is user-managed. Ask the user to edit or "
                    "upload via the memo panel."
                ),
                cache=store_cache,
            )
        )

    if gates.workflow:
        workflow_root = f"{sandbox_root}/{WORKFLOW_DIR}/"
        prebuilt_route = prebuilt_workflow_backend(
            files=get_prebuilt_workflows().files(),
            root_prefix=workflow_root,
            sandbox_backend=backend,
        )
        if gates.workflow_fs:

            def _workflow_namespace() -> tuple[str, ...]:
                return workflow_namespace(user_id)

            routes.append(
                WorkflowsBackend(
                    store_backend=StoreBackend(
                        store=store,
                        namespace_factory=_workflow_namespace,
                        root_prefix=workflow_root,
                        sandbox_backend=backend,
                        cache=store_cache,
                    ),
                    prebuilt_backend=prebuilt_route,
                )
            )
        else:
            routes.append(prebuilt_route)

    if gates.user_data:
        routes.extend(
            route(
                user_id=user_id,
                call=call or CallContext(),
                sandbox_backend=backend,
                root_prefix=f"{sandbox_root}/{route.directory}/",
            )
            for route in USER_DATA_ROUTES
        )

    if gates.channels:
        routes.append(
            ChannelsBackend(
                user_id=user_id,
                call=call or CallContext(),
                sandbox_backend=backend,
                root_prefix=f"{sandbox_root}/{ChannelsBackend.directory}/",
            )
        )

    if not routes:
        return backend, None

    # A tier the gates left without a namespace has nowhere to read from, so
    # it is left out rather than carried as a source that always fails.
    tier_factories = (
        {
            "user": (
                user_namespace_factory,
                f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}",
            ),
            "workspace": (
                workspace_namespace_factory,
                f"{workspace_memory_dir}/{MEMORY_INDEX_FILENAME}",
            ),
        }
        if gates.memory
        else {}
    )
    memory_tiers: dict[str, MemoryTierSource] = {
        tier: MemoryTierSource(namespace_factory=factory, display_path=display)
        for tier, (factory, display) in tier_factories.items()
        if factory is not None
    }
    memo_source = (
        MemoSource(
            namespace_factory=memo_namespace_factory,
            display_path=f"{MEMO_USER_DIR}/",
            index_key=MEMO_INDEX_FILENAME,
        )
        if gates.memo and memo_namespace_factory is not None
        else None
    )

    return (
        CompositeFilesystemBackend(sandbox=backend, routes=routes),
        BaselineSources(
            store=store,
            store_cache=store_cache,
            memory=memory_tiers,
            index_key=MEMORY_INDEX_FILENAME,
            memo=memo_source,
        ),
    )
