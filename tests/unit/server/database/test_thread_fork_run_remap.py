"""A fork's copied values name the copies' task run ids: TaskOutput reads a
workflow's archived result (a lane's ``ui`` record) and replay finds the run a
Task call started (its launch artifact on ``messages``) by the ledger's run id,
which in the fork is the new one."""

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import _DeltaSnapshot

from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
    read_task_result,
)
from src.server.database.thread_fork import _remapped_blob, remap_launches, remap_ui_records

OLD = "c1fa7905-e86e-4c7f-87ae-d54f76939dcb"
NEW = "0b6c1f0e-3a55-4b1e-9d1c-5e2f7a8b9c10"
RUNS = {OLD: NEW}


def _result(run_id: str) -> dict:
    return {
        "type": "ui",
        "id": f"task-result-{run_id}",
        "name": "task_result",
        "props": {"task_run_id": run_id, "text": "MANGO", "truncated": False, "result_ref": None},
        "metadata": {},
    }


def _workflow_run(run_id: str) -> dict:
    return {
        "type": "ui",
        "id": f"workflow-run-{run_id}",
        "name": "workflow_run",
        "props": {"task_id": "NGwxdQ", "frames": [{"run_id": "NGwxdQ", "phase": "run_completed"}]},
        "metadata": {},
    }


def test_task_result_found_under_the_new_run_id():
    out = remap_ui_records([_result(OLD)], RUNS)
    assert out == [_result(NEW)]
    assert read_task_result(out, NEW) == "MANGO"
    assert read_task_result(out, OLD) is None


def test_workflow_snapshot_id_follows_the_run():
    (out,) = remap_ui_records([_workflow_run(OLD)], RUNS)
    assert out["id"] == f"workflow-run-{NEW}"
    assert out["props"] == _workflow_run(OLD)["props"]


def test_unmapped_records_come_back_as_the_same_object():
    other = "11111111-2222-4333-8444-555555555555"
    value = [_result(other), {"type": "ui", "id": "handoff-3-x", "name": "model_handoff", "props": {}}]
    assert remap_ui_records(value, RUNS) is value
    # A workflow run with no ledger row keys its snapshot by task id.
    single = _workflow_run("NGwxdQ")
    assert remap_ui_records(single, RUNS) is single


def test_blob_rewritten_only_when_it_changes():
    serde = JsonPlusSerializer()
    type_, blob = serde.dumps_typed([_result(OLD), _workflow_run(OLD)])
    new_type, new_blob = _remapped_blob(serde, type_, blob, RUNS, remap_ui_records)
    assert new_type == type_
    assert serde.loads_typed((new_type, new_blob)) == [_result(NEW), _workflow_run(NEW)]

    untouched = serde.dumps_typed([_result("other")])
    assert _remapped_blob(serde, *untouched, RUNS, remap_ui_records) is None
    assert _remapped_blob(serde, "empty", None, RUNS, remap_ui_records) is None


def _launch(run_id: str) -> ToolMessage:
    return ToolMessage(
        content="Task NGwxdQ started",
        tool_call_id="call_1",
        id="tool-1",
        additional_kwargs={
            "task_artifact": {"task_id": "NGwxdQ", "action": "init", "prompt": "Say MANGO", "task_run_id": run_id}
        },
    )


def test_launch_artifact_names_the_new_run():
    ai = AIMessage(content="", id="ai-1")
    out = remap_launches([ai, _launch(OLD)], RUNS)
    assert out[0] is ai
    assert out[1].additional_kwargs["task_artifact"]["task_run_id"] == NEW
    assert out[1].id == "tool-1"


def test_launches_in_a_delta_snapshot_are_remapped():
    snap = _DeltaSnapshot([_launch(OLD)])
    out = remap_launches(snap, RUNS)
    assert isinstance(out, _DeltaSnapshot)
    assert out.value[0].additional_kwargs["task_artifact"]["task_run_id"] == NEW
    unmapped = _DeltaSnapshot([_launch("other")])
    assert remap_launches(unmapped, RUNS) is unmapped


def test_messages_naming_no_mapped_run_come_back_as_the_same_object():
    value = [AIMessage(content="hi", id="a"), _launch("11111111-2222-4333-8444-555555555555")]
    assert remap_launches(value, RUNS) is value


def test_messages_blob_round_trips_and_holds_the_id_as_bytes():
    serde = JsonPlusSerializer()
    type_, blob = serde.dumps_typed([_launch(OLD)])
    # The copy's byte search finds a value by its run id before decoding it.
    assert OLD.encode() in blob
    new_type, new_blob = _remapped_blob(serde, type_, blob, RUNS, remap_launches)
    (msg,) = serde.loads_typed((new_type, new_blob))
    assert msg.additional_kwargs["task_artifact"]["task_run_id"] == NEW
