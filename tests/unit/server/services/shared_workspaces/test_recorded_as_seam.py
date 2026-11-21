"""The account a run is recorded as is chosen in one place.

``choose_recorded_as`` answers the sender for every turn today. Each test here
makes it answer another account for a shared workspace's turn, and checks
that admission, the live gate and every usage row follow it, while what is
about the person (credentials and model settings, the name on the run, the
thread's creator, tools, the computer's session) stays the sender's. A site
that reads the sender where it means the recorded account fails here.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import ExitStack, asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.shared_workspaces import HolderNotAPerson, is_holder
from src.server.services.shared_workspaces.access import Seat
from src.server.services.turn_runtime import TurnRoute
from src.server.handlers.chat.request_prep import PriorThread
from tests.unit.server.handlers.conftest import spend  # noqa: F401

SENDER = "user-a"
HOLDER = "shared-11111111-2222-3333-4444-555555555555"
ACCOUNT = "account-of-the-workspace"
PTC = "src.server.handlers.chat.ptc_run"

pytestmark = pytest.mark.asyncio


def _flipped(sender, owner, org_id):
    return ACCOUNT if is_holder(owner) else sender


class _Seen:
    """Everything the run sent or wrote, by kind."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def add(self, kind: str, **data) -> None:
        self.calls.append((kind, data))

    def of(self, kind: str) -> list[dict]:
        return [data for k, data in self.calls if k == kind]


class _Resp:
    status_code = 200

    def __init__(self, url):
        self._body = (
            {"granted": True, "ceiling_credits": 100.0, "ttl_seconds": 60}
            if url.endswith("/acquire")
            else {"quota": {"allowed": True}}
        )
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


def _platform(stack: ExitStack, seen: _Seen) -> None:
    """Platform gating on, the auth service a recorder, the decision flipped."""

    class _HTTP:
        async def post(self, url, json=None, headers=None, timeout=None, **_):
            seen.add("auth", url=url.rsplit("/", 1)[-1], body=json or {}, headers=dict(headers or {}))
            return _Resp(url)

    stack.enter_context(patch.dict(os.environ, {"INTERNAL_SERVICE_TOKEN": "svc"}))
    stack.enter_context(patch("src.server.services.platform_client.HOST_MODE", "platform"))
    stack.enter_context(patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://auth.test"))
    stack.enter_context(patch("src.server.services.platform_client.get_http_client", new=AsyncMock(return_value=_HTTP())))
    stack.enter_context(patch("src.server.services.credit_gate_port.AUTH_SERVICE_URL", "http://auth.test"))
    stack.enter_context(patch("src.config.settings.HOST_MODE", "platform"))
    stack.enter_context(patch("src.server.services.shared_workspaces.turn.choose_recorded_as", new=_flipped))
    stack.enter_context(patch(
        "src.server.database.shared_workspaces.shared_of_holder",
        new=AsyncMock(return_value=SimpleNamespace(org_id="org-1")),
    ))


def _usage_rows(stack: ExitStack, seen: _Seen) -> None:
    conn = MagicMock()

    @asynccontextmanager
    async def _transaction():
        yield

    @asynccontextmanager
    async def _connection():
        yield conn

    conn.transaction = _transaction

    async def _create(data, conn=None):
        seen.add("usage", user_id=data["user_id"], msg_type=data["msg_type"])
        return True

    stack.enter_context(patch("src.server.database.pool.get_db_connection", new=_connection))
    stack.enter_context(patch("src.server.database.conversation.create_usage_record", new=AsyncMock(side_effect=_create)))
    stack.enter_context(patch("src.server.database.runs.credit_ledger.settle_task_run", new=AsyncMock(return_value=True)))


def _records():
    return [{
        "model_name": "claude-test", "served_model": "claude-test",
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
        "billing_type": "platform", "started_at": "2026-01-01T00:00:00+00:00",
        "timestamp": "2026-01-01T00:00:01+00:00", "run_id": "call-1", "parent_run_id": None,
    }]


def _workspace(owner):
    return {"workspace_id": "ws-1", "user_id": owner, "status": "running"}


@pytest.mark.parametrize("owner", [HOLDER, SENDER], ids=["shared", "personal"])
async def test_admission_at_both_doors_asks_for_the_recorded_account(owner):
    from src.server.app.threads import create, messaging
    from src.server.dependencies.usage_limits import ChatAuthResult
    from src.server.models.chat import ChatMessage, ChatRequest
    from src.server.models.conversation import ThreadCreateRequest

    expected = ACCOUNT if owner == HOLDER else SENDER
    seen = _Seen()
    config = MagicMock(credential_source=None)
    handed: dict = {}

    async def _empty():
        if False:
            yield ""

    def _turn(**kw):
        handed.update(kw)
        return _empty()

    class _Stop(Exception):
        pass

    with ExitStack() as stack:
        _platform(stack, seen)
        stack.enter_context(patch("src.server.app.setup.agent_config", MagicMock()))
        stack.enter_context(patch(f"{messaging.__name__}.get_thread_auth_meta", new=AsyncMock(return_value=None)))
        stack.enter_context(patch("src.server.services.turn_runtime.requested_workspace", new=AsyncMock(return_value=_workspace(owner))))
        resolve = stack.enter_context(patch("src.server.services.llm.thread_model.resolve_turn_config", new=AsyncMock(return_value=config)))
        stack.enter_context(patch(f"{messaging.__name__}._assert_stream_transport_ready", new=AsyncMock()))
        stack.enter_context(patch("src.server.handlers.chat.image_intake.inline_image_urls", new=AsyncMock(side_effect=lambda r: r)))
        stack.enter_context(patch("src.server.handlers.chat.astream_ptc_workflow", side_effect=_turn))
        await messaging._handle_send_message(
            ChatRequest(workspace_id="ws-1", messages=[ChatMessage(role="user", content="hi")]),
            ChatAuthResult(user_id=SENDER, access_tier=0),
            "thread-new",
        )
        stack.enter_context(patch(f"{create.__name__}.HOST_MODE", "platform"))
        stack.enter_context(patch("src.server.services.account.own_access", new=AsyncMock(return_value=SimpleNamespace(any=False))))
        stack.enter_context(patch(f"{create.__name__}.requested_workspace", new=AsyncMock(return_value=_workspace(owner))))
        stack.enter_context(patch("src.server.services.turn_runtime.TurnRoute.bind", new=AsyncMock(side_effect=_Stop())))
        with pytest.raises(_Stop):
            await create.create_thread_endpoint(
                ThreadCreateRequest(workspace_id="ws-1", first_query="hi"), MagicMock(), SimpleNamespace(user_id=SENDER, computer_id=None)
            )

    asked = [c["headers"]["X-User-Id"] for c in seen.of("auth") if c["url"] == "validate"]
    assert asked == [expected, expected]
    # The model and its credentials are the sender's, and so is the turn.
    assert resolve.await_args.args[0] == SENDER
    assert handed["user_id"] == SENDER
    assert (handed["identity"].sender, handed["identity"].recorded_as) == (SENDER, expected)


def _turn_patches(stack: ExitStack, seen: _Seen) -> dict:
    from src.server.services import credit_gate_port
    from src.server.services.report_back.handoff.carry import PriorAttempt
    from src.server.services.runs.executor import LocalRunExecutor
    from tests.unit.server.handlers.test_ptc_run_workspace_status import _make_workspace_manager

    _platform(stack, seen)
    _usage_rows(stack, seen)
    wm = _make_workspace_manager(has_ready=True, observed_state=None)
    acquire = wm.get_session_for_workspace.side_effect

    async def _session(workspace_id, **kw):
        seen.add("session", user_id=kw.get("user_id"))
        return await acquire(workspace_id, **kw)

    wm.get_session_for_workspace.side_effect = _session
    stack.enter_context(patch(f"{PTC}.setup")).agent_config = MagicMock()
    stack.enter_context(patch(f"{PTC}.WorkspaceManager")).get_instance.return_value = wm
    stack.enter_context(patch(f"{PTC}.ensure_thread", new=AsyncMock(return_value=PriorThread())))
    for name in ("keep_named_model", "turn_start_checkpoint_id", "open_with_steers_ahead"):
        stack.enter_context(patch(f"{PTC}.{name}", new=AsyncMock(return_value=None)))
    for name in ("apply_fetch_override", "_fire_and_forget", "update_workspace_activity", "RunSSEProducer"):
        stack.enter_context(patch(f"{PTC}.{name}"))
    stack.enter_context(patch(f"{PTC}._resolve_fork", return_value=("Q", None)))
    stack.enter_context(patch(f"{PTC}.carry.prior_attempt", new=AsyncMock(return_value=PriorAttempt(latest=None, retried=None))))
    stack.enter_context(patch(f"{PTC}.wait_or_steer", new=AsyncMock(return_value=(True, None))))
    stack.enter_context(patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())))
    stack.enter_context(patch(f"{PTC}.is_claude_code_model", return_value=False))
    stack.enter_context(patch(f"{PTC}.normalize_request_messages", return_value=[]))
    stack.enter_context(patch(f"{PTC}.prepare_skill_contexts", return_value=None))
    stack.enter_context(patch(f"{PTC}.attach_request_files", new=AsyncMock(return_value=[])))
    stack.enter_context(patch(f"{PTC}._artifact_hook", return_value=None))
    stack.enter_context(patch(f"{PTC}.build_ptc_graph_with_session", new=AsyncMock(return_value=MagicMock())))
    stack.enter_context(patch(f"{PTC}.BackgroundRegistryStore")).get_instance.return_value.get_or_create_registry = AsyncMock()
    stack.enter_context(patch("src.server.services.shared_workspaces.turn.get_user_profile_for_prompt", new=AsyncMock(return_value=None)))
    stack.enter_context(patch("src.server.services.writer_guard.guard_enabled", return_value=False))
    stack.enter_context(patch("src.server.services.thread_control_stream.announce_run_started", new=AsyncMock()))

    async def _creator(thread_id, who):
        seen.add("creator", user_id=who)

    async def _row(**kw):
        seen.add("run_row", user_id=kw["user_id"], named=kw["metadata"].get("user_id"))
        return {"turn_index": 1, "attempt_no": 1, "created_at": datetime.now(timezone.utc)}

    async def _feed(**kw):
        seen.add("feed", user_id=kw["user_id"])

    stack.enter_context(patch("src.server.services.shared_workspaces.turn.stamp_thread_creator", new=_creator))
    # START's own reach check reads the sender's seat before its transaction.
    stack.enter_context(patch(
        "src.server.services.shared_workspaces.access.caller_seat",
        new=AsyncMock(return_value=Seat("org")),
    ))
    stack.enter_context(patch("src.server.database.runs.lifecycle.start_run", new=_row))
    stack.enter_context(patch("src.server.services.thread_lifecycle_feed.publish_run_started", new=_feed))
    gates: list = []
    real_gate = credit_gate_port.build_run_credit_gate
    stack.enter_context(patch(f"{PTC}.build_run_credit_gate", side_effect=lambda *a, **k: gates.append(real_gate(*a, **k)) or gates[-1]))
    inputs = MagicMock(project=None)
    inputs.direct_mcp.drive = lambda g: g
    inputs.build_kwargs.return_value = {}
    stack.enter_context(patch("src.server.services.thread_agent.AgentInputs.read", new=AsyncMock(return_value=inputs)))
    stack.enter_context(patch(
        "src.server.services.egress.direct_tools.order_ledger_for_turn",
        side_effect=lambda **kw: seen.add("orders", user_id=kw["user_id"]),
    ))

    def _graph_config(**kw):
        seen.add("tools", user_id=kw["user_id"])
        return {"configurable": {}}

    async def _log(*_a, **_k):
        yield "event: done\n\n"

    stack.enter_context(patch(f"{PTC}.build_graph_config", side_effect=_graph_config))
    stack.enter_context(patch(f"{PTC}.stream_from_log", new=_log))
    started: dict = {"gates": gates}

    async def _start_run(self, **kw):
        started.update(kw)
        await kw["workflow_generator"].aclose()

    stack.enter_context(patch.object(LocalRunExecutor, "start_run", new=_start_run))
    return started


async def _finalize(seen: _Seen, metadata: dict, *, stopped: bool) -> None:
    """The executor's finalize of the run, through the coordinator's usage
    writer and the subagent collection's entry points."""
    from src.server.services.runs import finalization, subagent_collection
    from src.server.services.runs.coordinator import RunCoordinator
    from src.server.services.runs.executor import LocalRunExecution, LocalRunExecutor, LocalRunStatus

    status = "cancelled" if stopped else "completed"
    tracker = MagicMock()
    tracker.get_per_call_records.return_value = _records()
    handler = MagicMock(agent_config=None, quiet=None)
    handler.get_tool_usage.return_value = {}
    executor = LocalRunExecutor()
    executor.executions[("thread-1", "run-1")] = LocalRunExecution(
        thread_id="thread-1", run_id="run-1", status=LocalRunStatus.RUNNING, created_at=datetime.now(),
        metadata={**metadata, "token_callback": tracker, "handler": handler}, user_stop=stopped,
    )

    async def _cas(key, handle, outcome, tail_drain=None):
        await RunCoordinator.get_instance()._build_usage_writer(handle, outcome)(MagicMock(), status)
        return finalization.FinalizeVerdict(applied=True, status=status, bills_subagents=stopped)

    async def _subagent_usage(response_id, tasks, thread_id, workspace_id, account, is_byok=False):
        seen.add("subagent_usage", user_id=account)

    async def _collector(thread_id, response_id, tasks, workspace_id, account, **_):
        seen.add("collector", user_id=account)

    task = SimpleNamespace(task_id="task-1", live_writers=[])
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=MagicMock(claim_run_subagents=AsyncMock(return_value=[task])))
    store.cancel_run_tasks = AsyncMock(return_value=SimpleNamespace(claimed=[task]))
    with ExitStack() as stack:
        stack.enter_context(patch.object(finalization, "classify_outcome", new=AsyncMock(return_value=(status, status, None, None, None))))
        stack.enter_context(patch.object(finalization, "drive_finalize_cas", new=_cas))
        stack.enter_context(patch.object(finalization, "_delivered_steering_inputs", new=AsyncMock(return_value=None)))
        stack.enter_context(patch.object(subagent_collection, "persist_subagent_usage", new=_subagent_usage))
        stack.enter_context(patch.object(subagent_collection, "collect_subagent_results_for_turn", new=_collector))
        stack.enter_context(patch.object(subagent_collection, "_settle_finished"))
        stack.enter_context(patch.object(subagent_collection, "_claim_settled_parents", new=AsyncMock()))
        stack.enter_context(patch.object(subagent_collection, "_billable_at_run_end", side_effect=lambda t, _r: t))
        stack.enter_context(patch.object(subagent_collection, "_archive_after_unwind"))
        stack.enter_context(patch("src.server.services.background_registry_store.BackgroundRegistryStore.get_instance", return_value=store))
        stack.enter_context(patch.object(executor, "append_run_end_event", new=AsyncMock()))
        await executor._finalize_run("thread-1", "run-1", kind="cancelled" if stopped else "stream_end")
        await asyncio.sleep(0.05)


@pytest.mark.parametrize("stopped", [False, True], ids=["completed", "stopped"])
async def test_a_shared_turns_gate_and_usage_follow_while_the_person_stays_the_sender(stopped):
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow
    from src.server.services import membership
    from src.server.services.shared_workspaces.turn import TurnIdentity
    from tests.unit.server.handlers.test_ptc_run_workspace_status import _make_config, _make_request

    seen = _Seen()

    async def _standing(user_id, **_):
        seen.add("standing", user_id=user_id)
        return SimpleNamespace(refusal=lambda: None)

    with ExitStack() as stack:
        started = _turn_patches(stack, seen)
        identity = await TurnIdentity.of(SENDER, HOLDER)
        request = _make_request()
        request.burst_slot_id = None
        async for _ in astream_ptc_workflow(
            request=request, thread_id="thread-1", run_id="run-1", user_input="hi", user_id=SENDER,
            route=TurnRoute("ws-1", "analyst", HOLDER), config=_make_config(), identity=identity,
        ):
            pass
        lease = started["gates"][0].lease
        stack.enter_context(patch.object(membership, "teams_door_open", return_value=True))
        stack.enter_context(patch("src.server.services.account.read_standing", new=_standing))
        await lease._acquire(0.5)
        await lease.account_refusal()
        await lease.join(started["gates"][0])
        await lease.leave(started["gates"][0])
        await _finalize(seen, started["metadata"], stopped=stopped)

    gate = {c["url"]: c["body"]["user_id"] for c in seen.of("auth")}
    assert gate == {"acquire": ACCOUNT, "release": ACCOUNT}
    assert [c["user_id"] for c in seen.of("usage")] == [ACCOUNT]
    assert [c["user_id"] for c in seen.of("subagent_usage")] == [ACCOUNT]
    assert [c["user_id"] for c in seen.of("collector")] == ([] if stopped else [ACCOUNT])
    # The person acting, wherever the run names or acts for someone.
    assert seen.of("run_row") == [{"user_id": SENDER, "named": SENDER}]
    for kind in ("standing", "creator", "feed", "session", "orders", "tools"):
        assert [c["user_id"] for c in seen.of(kind)] == [SENDER], kind
    assert started["metadata"]["user_id"] == SENDER


async def test_a_call_between_turns_is_recorded_as_the_turns_account(spend):  # noqa: F811
    """A compact's model calls land on a row of their own, under the account
    its thread's turns are recorded as; the model is resolved for the sender."""
    from src.server.handlers import thread_maintenance as tm
    from src.server.services.thread_mutation import MutationSession
    from tests.unit.server.handlers.test_trigger_compaction import (
        ASK, COMPACT_MESSAGES, THREAD_MODEL, _compaction, _make_agent_config,
    )

    seen = _Seen()
    config = _make_agent_config()
    state = MagicMock(values={"messages": [MagicMock(id="m1")], "_summarization_event": None})
    graph = MagicMock(aget_state=AsyncMock(return_value=state))
    inputs = MagicMock(project=None)
    inputs.build_kwargs.return_value = {}

    async def _ask(*_a, **_k):
        await spend("platform")
        return _compaction()

    @asynccontextmanager
    async def _exclusive(thread_id, verb):
        yield MutationSession(conn=None, saver=None)

    with ExitStack() as stack:
        _platform(stack, seen)
        _usage_rows(stack, seen)
        stack.enter_context(patch("src.server.app.setup.agent_config", config))
        stack.enter_context(patch("src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
                                  return_value=MagicMock(exclusive=_exclusive)))
        stack.enter_context(patch("src.server.database.conversation.get_thread_auth_meta",
                                  new=AsyncMock(return_value={"workspace_id": "ws-1", "user_id": HOLDER})))
        stack.enter_context(patch("src.server.database.workspace.get_workspace", new=AsyncMock(return_value=_workspace(HOLDER))))
        stack.enter_context(patch("src.server.database.conversation.get_thread_with_summary",
                                  new=AsyncMock(return_value={"workspace_id": "ws-1"})))
        stack.enter_context(patch("src.server.services.workspace_manager.WorkspaceManager.get_instance",
                                  return_value=MagicMock(get_session_for_workspace=AsyncMock(return_value=MagicMock(sandbox=None)))))
        stack.enter_context(patch("src.server.services.thread_agent.AgentInputs.read", new=AsyncMock(return_value=inputs)))
        stack.enter_context(patch("ptc_agent.agent.graph.build_ptc_graph_with_session", new=AsyncMock(return_value=graph)))
        stack.enter_context(patch(f"{tm.__name__}.get_checkpointer", return_value=MagicMock()))
        stack.enter_context(patch(f"{tm.__name__}.turn_base_checkpoint_id", new=AsyncMock(return_value="cp-1")))
        stack.enter_context(patch(f"{tm.__name__}._previous_run", new=AsyncMock(return_value=None)))
        stack.enter_context(patch(f"{tm.__name__}._update_graph_state", new=AsyncMock(return_value=True)))
        stack.enter_context(patch(ASK, new=AsyncMock(side_effect=_ask)))
        stack.enter_context(patch(COMPACT_MESSAGES, new=AsyncMock(side_effect=AssertionError("summarizer"))))
        stack.enter_context(patch("src.server.database.api_keys.is_byok_active", new=AsyncMock(return_value=False)))
        resolve = stack.enter_context(patch(f"{THREAD_MODEL}.resolve_turn_config", new=AsyncMock(return_value=config)))
        stack.enter_context(patch("src.server.database.runs.lifecycle.get_latest_attempt", new=AsyncMock(return_value=None)))
        await tm.trigger_compaction("thread-1", user_id=SENDER)

    assert seen.of("usage") == [{"user_id": ACCOUNT, "msg_type": "ptc"}]
    assert resolve.await_args.args[0] == SENDER


async def test_usage_settled_later_keeps_the_account_its_run_row_carries():
    """A resumed subagent's earlier round is recorded as the run that spent
    it, read off that run's row (``carried_recorded_as``), not chosen again:
    a later choice would not be the account that run was admitted for."""
    from src.server.services.runs import subagent_usage

    seen = _Seen()
    take = SimpleNamespace(
        response_id="earlier-run", records=_records(), tool_usage=None, settle_run_id=None,
        task=SimpleNamespace(task_id="task-1", agent_id="agent-1", subagent_type="research"),
    )
    ending = {"status": "completed", "metadata": {}, "workspace_id": "ws-1", "user_id": SENDER}
    with ExitStack() as stack:
        _platform(stack, seen)
        _usage_rows(stack, seen)
        stack.enter_context(patch("src.server.database.runs.lifecycle.get_run_ending", new=AsyncMock(return_value=ending)))
        await subagent_usage.bill_resumed_round("thread-1", take)

    assert seen.of("usage") == [{"user_id": SENDER, "msg_type": "task"}]


async def test_a_holder_is_never_recorded():
    from src.server.database.conversation.usage import create_usage_record

    conn = MagicMock()
    with pytest.raises(HolderNotAPerson):
        await create_usage_record({"user_id": HOLDER}, conn=conn)
    conn.cursor.assert_not_called()
