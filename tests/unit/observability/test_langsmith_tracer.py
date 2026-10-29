"""NonRetainingTracer posts every run as the stock tracer does, without the tree.

A turn has no step cap, so a root that keeps its children grows for as long as
the turn runs. These pin both halves: each run still reaches LangSmith on its
own with the parent and ordering it had, and nothing holds the finished runs.
The stamps it adds (build, per-call cost, tool outcome) are pinned below.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from langchain_core.callbacks.manager import CallbackManager
from langchain_core.language_models.fake_chat_models import (
    FakeListChatModel,
    GenericFakeChatModel,
)
from langchain_core.load import dumpd
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import StructuredTool
from langchain_core.tracers.langchain import LangChainTracer
from langsmith.run_helpers import tracing_context
from langsmith.run_trees import RunTree

from src.observability import (
    RESOURCE_TIER,
    mark_tool_failed,
    mark_tool_unavailable,
    stamp_resource_tier,
)
from src.observability.langsmith_run import build_sha
from src.observability.langsmith_tracer import NonRetainingTracer, with_run_tracer
from src.utils.tracking.core import calculate_cost_from_per_call_records
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker


class _RecordingClient:
    """Stands in for the LangSmith client: records what would be sent."""

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []

    def create_run(self, **kwargs: Any) -> None:
        self.created.append(kwargs)

    def update_run(self, **kwargs: Any) -> None:
        self.updated.append(kwargs)

    def flush(self) -> None:
        return None


def _traced_chain(seen: dict[str, Any]):
    """outer -> (inner, chat model); inner records what the tracer holds."""
    model = FakeListChatModel(responses=["ok"])

    def inner(x: str, config) -> str:
        tracer = next(
            h for h in config["callbacks"].handlers if isinstance(h, LangChainTracer)
        )
        roots = [r for r in tracer.run_map.values() if r.parent_run_id is None]
        seen["root_children_mid_run"] = len(roots[0].child_runs)
        return x

    def outer(x: str, config) -> str:
        RunnableLambda(inner).invoke(x, config)
        return model.invoke(x, config).content

    return RunnableLambda(outer, name="outer")


def _segments(dotted_order: str) -> list[str]:
    return dotted_order.split(".")


@pytest.mark.parametrize("tracer_cls", [LangChainTracer, NonRetainingTracer])
def test_each_run_is_posted_with_its_parent_and_order(tracer_cls):
    client = _RecordingClient()
    tracer = tracer_cls(project_name="proj", client=client)
    seen: dict[str, Any] = {}

    assert _traced_chain(seen).invoke("hi", {"callbacks": [tracer]}) == "ok"

    created = {str(c["id"]): c for c in client.created}
    names = sorted(c["name"] for c in created.values())
    assert names == ["FakeListChatModel", "inner", "outer"]
    assert sorted(str(u["run_id"]) for u in client.updated) == sorted(created)

    root = next(c for c in created.values() if c["name"] == "outer")
    assert root.get("parent_run_id") is None
    assert str(root["trace_id"]) == str(root["id"])
    assert _segments(root["dotted_order"])[-1].endswith(str(root["id"]))
    for child in (c for c in created.values() if c is not root):
        assert str(child["parent_run_id"]) == str(root["id"])
        assert str(child["trace_id"]) == str(root["id"])
        segs = _segments(child["dotted_order"])
        assert ".".join(segs[:-1]) == root["dotted_order"]
        assert segs[-1].endswith(str(child["id"]))
        assert child["inputs"]
    assert all(c["session_name"] == "proj" for c in client.created)

    # The control proves the probe can see a held child; the subclass holds none.
    expected = 1 if tracer_cls is LangChainTracer else 0
    assert seen["root_children_mid_run"] == expected
    assert tracer.run_map == {}


def test_nests_under_an_outer_run_without_holding_on_to_it():
    """A subagent starts under its launcher's tool run, which another tracer owns."""
    client = _RecordingClient()
    launcher = RunTree(name="Task", run_type="tool", client=client, session_name="proj")
    with tracing_context(parent=launcher, enabled=True):
        config = with_run_tracer({})
        RunnableLambda(lambda x: x, name="subagent").invoke("hi", config)

    run = next(c for c in client.created if c["name"] == "subagent")
    assert str(run["parent_run_id"]) == str(launcher.id)
    assert str(run["trace_id"]) == str(launcher.trace_id)
    assert run["dotted_order"].startswith(launcher.dotted_order + ".")
    assert run["session_name"] == "proj"
    # langchain_core's own tracer appends the subagent's root here, so the
    # launcher's tree would hold the subagent's too.
    assert launcher.child_runs == []


def _ambient_tracer() -> LangChainTracer:
    manager = CallbackManager.configure(inheritable_callbacks=None)
    return next(h for h in manager.handlers if isinstance(h, LangChainTracer))


def test_built_as_langchain_core_builds_its_own():
    client = _RecordingClient()
    with tracing_context(
        enabled=True,
        client=client,
        project_name="proj",
        tags=["t1"],
        metadata={"k": "v"},
    ):
        ours = with_run_tracer({})["callbacks"][-1]
        ambient = _ambient_tracer()

    assert type(ours) is NonRetainingTracer
    assert type(ambient) is LangChainTracer
    assert ours.project_name == ambient.project_name == "proj"
    assert ours.client is ambient.client is client
    assert ours.tags == ambient.tags
    assert ours.tracing_metadata == ambient.tracing_metadata


def test_under_a_parent_run_uses_its_project_and_client():
    client = _RecordingClient()
    parent = RunTree(name="Task", run_type="tool", client=client, session_name="theirs")
    with tracing_context(parent=parent, enabled=True):
        ours = with_run_tracer({})["callbacks"][-1]
        ambient = _ambient_tracer()

    assert ours.project_name == ambient.project_name == "theirs"
    assert ours.client is ambient.client is client


def test_named_tracer_stops_langchain_core_adding_its_own():
    client = _RecordingClient()
    with tracing_context(enabled=True, client=client):
        config = with_run_tracer({"callbacks": [object()]})
        manager = CallbackManager.configure(inheritable_callbacks=config["callbacks"])

    tracers = [h for h in manager.handlers if isinstance(h, LangChainTracer)]
    assert len(tracers) == 1
    assert type(tracers[0]) is NonRetainingTracer


def test_unchanged_when_tracing_is_off():
    config = {"callbacks": [object()]}
    with tracing_context(enabled=False):
        assert with_run_tracer(config) is config
        assert with_run_tracer({}) == {}


def test_unchanged_when_the_callbacks_already_trace():
    client = _RecordingClient()
    with tracing_context(enabled=True, client=client):
        traced = {"callbacks": [LangChainTracer(client=client)]}
        assert with_run_tracer(traced) is traced
        # A callback manager carries its parent's tracer to the run.
        managed = {"callbacks": CallbackManager(handlers=[], parent_run_id=uuid4())}
        assert with_run_tracer(managed) is managed


# ---------------------------------------------------------------------------
# Run stamps: build version, per-call cost, tool outcome
# ---------------------------------------------------------------------------

# Synthetic rates, so a reprice of any real model cannot move these tests.
_CARD = {"input": 3.0, "output": 15.0, "cached_input": 0.3}
_PRICED_ID = "synthetic-model"


@pytest.fixture
def build_env(monkeypatch):
    """Set or clear LANGALPHA_BUILD_SHA around build_sha's process-wide cache."""

    def _set(sha: str | None) -> None:
        if sha is None:
            monkeypatch.delenv("LANGALPHA_BUILD_SHA", raising=False)
        else:
            monkeypatch.setenv("LANGALPHA_BUILD_SHA", sha)
        build_sha.cache_clear()

    yield _set
    build_sha.cache_clear()


@pytest.fixture
def price_cards(monkeypatch):
    cards = {_PRICED_ID: dict(_CARD)}
    monkeypatch.setattr(
        "src.llms.pricing_utils.find_model_pricing",
        lambda pricing_id, provider=None: cards.get(pricing_id),
    )
    monkeypatch.setattr(
        "src.llms.pricing_utils.resolve_pricing_identity",
        lambda name, billing_type="platform": (None, name),
    )
    return cards


def _usage() -> dict[str, Any]:
    return {
        "input_tokens": 1_000,
        "output_tokens": 500,
        "total_tokens": 1_500,
        "input_token_details": {"cache_read": 200},
    }


def _llm_call(
    client: _RecordingClient,
    *,
    billing_type: str = "platform",
    pricing_id: str = _PRICED_ID,
    extra_callbacks: list[Any] | None = None,
) -> tuple[AIMessage, dict[str, Any]]:
    """One model call through the tracer; returns the message and its LLM update."""
    message = AIMessage(
        content="ok",
        response_metadata={"model_name": "vendor-echo"},
        usage_metadata=_usage(),
    )
    model = GenericFakeChatModel(messages=iter([message]))
    tracer = NonRetainingTracer(project_name="proj", client=client)
    model.invoke(
        "hi",
        {
            "callbacks": [*(extra_callbacks or []), tracer],
            "metadata": {
                "billing_type": billing_type,
                "manifest_model": pricing_id,
                "pricing_model_id": pricing_id,
                "pricing_provider": "acme",
            },
        },
    )
    (update,) = [u for u in client.updated if u["run_type"] == "llm"]
    return message, update


def _usage_of(update: dict[str, Any]) -> dict[str, Any]:
    return update["extra"]["metadata"]["usage_metadata"]


def _output_usage(update: dict[str, Any]) -> dict[str, Any]:
    return update["outputs"]["generations"][0][0]["message"]["kwargs"]["usage_metadata"]


@pytest.mark.parametrize("sha", ["build-abc123", None])
def test_every_run_carries_the_build_it_came_from(build_env, sha):
    build_env(sha)
    client = _RecordingClient()

    _traced_chain({}).invoke("hi", {"callbacks": [NonRetainingTracer(client=client)]})

    assert len(client.created) == 3
    for run in client.created:
        metadata = run["extra"].get("metadata", {})
        if sha:
            assert metadata["app_version"] == sha
        else:
            assert "app_version" not in metadata


def test_llm_run_cost_is_what_billing_charges_for_the_call(price_cards):
    client = _RecordingClient()
    tracker = PerCallTokenTracker()

    _, update = _llm_call(client, extra_callbacks=[tracker])

    usage = _usage_of(update)
    billed = calculate_cost_from_per_call_records(tracker.get_per_call_records())
    assert billed["total_cost"] > 0
    assert usage["total_cost"] == pytest.approx(billed["total_cost"])
    assert usage["total_cost"] == pytest.approx(usage["input_cost"] + usage["output_cost"])
    assert usage["input_cost_details"]["cache_read"] > 0
    # LangSmith reads a chat run's usage off its output message first.
    assert _output_usage(update)["total_cost"] == usage["total_cost"]


def test_byok_and_oauth_runs_carry_the_platform_list_price(price_cards):
    totals = {}
    for billing_type in ("platform", "byok", "oauth"):
        _, update = _llm_call(_RecordingClient(), billing_type=billing_type)
        totals[billing_type] = _usage_of(update)["total_cost"]

    assert totals["platform"] > 0
    assert totals["byok"] == totals["platform"]
    assert totals["oauth"] == totals["platform"]


def test_a_model_with_no_price_card_gets_no_cost(price_cards):
    """A zero would read as free and override LangSmith's own estimate."""
    _, update = _llm_call(_RecordingClient(), pricing_id="unpriced-model")

    usage = _usage_of(update)
    assert usage["input_tokens"] == 1_000
    for key in ("input_cost", "output_cost", "total_cost"):
        assert key not in usage
        assert key not in _output_usage(update)


def test_cost_never_lands_on_the_callers_message(price_cards, monkeypatch):
    """The serializer may hand the tracer the message's own usage dict, as this
    one does, so the stamp has to go on a copy."""

    def sharing_dumpd(obj: Any) -> Any:
        if isinstance(obj, AIMessage):
            return {
                "lc": 1,
                "type": "constructor",
                "id": ["langchain", "schema", "messages", "AIMessage"],
                "kwargs": {
                    "content": obj.content,
                    "usage_metadata": obj.usage_metadata,
                    "response_metadata": obj.response_metadata,
                },
            }
        return dumpd(obj)

    monkeypatch.setattr("langchain_core.tracers.core.dumpd", sharing_dumpd)

    message, update = _llm_call(_RecordingClient())

    assert _output_usage(update)["total_cost"] > 0
    assert message.usage_metadata == _usage()


def test_llm_and_tool_runs_close_without_resending_their_inputs():
    client = _RecordingClient()
    model = FakeListChatModel(responses=["ok"])

    def echo(text: str) -> str:
        """probe"""
        return text

    probe = StructuredTool.from_function(func=echo, name="probe", description="probe")
    chain = RunnableLambda(
        lambda x, config: probe.invoke({"text": model.invoke(x, config).content}, config),
        name="outer",
    )

    chain.invoke("hi", {"callbacks": [NonRetainingTracer(client=client)]})

    created = {c["run_type"]: c for c in client.created}
    updated = {u["run_type"]: u for u in client.updated}
    for run_type in ("llm", "tool"):
        assert created[run_type]["inputs"]
        assert updated[run_type]["inputs"] is None
    # A chain can receive inputs only at its end, so it keeps the stock patch.
    assert updated["chain"]["inputs"] == {"input": "hi"}


def _tool_update(func: Any, *, as_tool_call: bool = False) -> dict[str, Any]:
    """Invoke ``func`` as a traced tool and return the tool run's update."""
    client = _RecordingClient()
    probe = StructuredTool.from_function(func=func, name="probe", description="probe")
    config = {"callbacks": [NonRetainingTracer(project_name="proj", client=client)]}
    payload: Any = (
        {"type": "tool_call", "name": "probe", "args": {}, "id": "tc-1"}
        if as_tool_call
        else {}
    )
    try:
        probe.invoke(payload, config)
    except Exception:
        pass
    (update,) = [u for u in client.updated if u["run_type"] == "tool"]
    return update


def test_a_tool_marked_failed_is_an_errored_run():
    def failing() -> str:
        """probe"""
        mark_tool_failed("provider_error", "acme")
        return "  upstream said no\nsecond line"

    update = _tool_update(failing)

    metadata = update["extra"]["metadata"]
    assert metadata["tool_outcome"] == "error"
    assert metadata["tool_provider"] == "acme"
    assert update["error"] == "provider_error: upstream said no"


def test_a_tool_whose_target_declined_is_not_an_errored_run():
    def declined() -> str:
        """probe"""
        mark_tool_unavailable("paywall", "acme")
        return "paywalled"

    update = _tool_update(declined)

    metadata = update["extra"]["metadata"]
    assert metadata["tool_outcome"] == "unavailable"
    assert metadata["error_class"] == "paywall"
    assert update["error"] is None


def test_an_unmarked_error_status_reply_is_ok():
    """A tool refusing the agent's own mistake answers it; nothing failed."""

    def refusing() -> ToolMessage:
        """probe"""
        return ToolMessage(
            content="Error: unknown subagent type", tool_call_id="tc-1", status="error"
        )

    update = _tool_update(refusing, as_tool_call=True)

    assert update["extra"]["metadata"]["tool_outcome"] == "ok"
    assert update["error"] is None


def test_a_raising_tool_is_an_error_classed_by_its_exception():
    def raising() -> str:
        """probe"""
        raise ValueError("bad")

    update = _tool_update(raising)

    metadata = update["extra"]["metadata"]
    assert metadata["tool_outcome"] == "error"
    assert metadata["error_class"] == "ValueError"
    assert update["error"]


def test_a_graph_interrupt_is_a_pause_not_an_outcome():
    from langgraph.errors import GraphInterrupt

    def pausing() -> str:
        """probe"""
        raise GraphInterrupt(())

    update = _tool_update(pausing)

    assert "tool_outcome" not in update["extra"]["metadata"]
    assert update["error"] is None


def test_arguments_the_schema_rejects_are_the_agents_mistake():
    def needs_text(text: str) -> str:
        """probe"""
        return text

    update = _tool_update(needs_text)

    assert update["extra"]["metadata"]["tool_outcome"] == "ok"
    assert update["error"] is None


def test_a_validation_error_inside_the_tool_is_its_own_failure():
    from pydantic import BaseModel

    class Reply(BaseModel):
        n: int

    def parsing() -> str:
        """probe"""
        return str(Reply.model_validate({"n": "not a number"}))

    update = _tool_update(parsing)

    metadata = update["extra"]["metadata"]
    assert metadata["tool_outcome"] == "error"
    assert metadata["error_class"] == "ValidationError"
    assert update["error"]


def test_a_huge_failed_output_yields_a_bounded_error():
    def flooding() -> str:
        """probe"""
        mark_tool_failed("provider_error")
        return "x" * 2_000_000

    update = _tool_update(flooding)

    assert update["error"].startswith("provider_error: xxx")
    assert len(update["error"]) <= 300


def test_a_tool_stamps_its_resource_tier():
    def sized() -> str:
        """probe"""
        stamp_resource_tier("standard")
        return "ok"

    update = _tool_update(sized)

    assert update["extra"]["metadata"][RESOURCE_TIER] == "standard"


def test_stamping_a_tier_outside_a_tool_run_is_a_no_op():
    stamp_resource_tier("standard")
