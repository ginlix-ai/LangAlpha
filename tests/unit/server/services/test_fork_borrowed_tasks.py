"""Where a fork reads each carried task's lane (``forks.tasks.task_lanes``).

The fork copies no lane, so each carried task names the thread holding its
lane and the run ids the lane knows its runs by. A fork of a fork reads a task
its source still borrows from where the source reads it, and one the source
resumed from the source itself.
"""

from __future__ import annotations

from src.server.database.thread_fork import LedgerPlan
from src.server.services.forks.model import BorrowedTask, Borrowing
from src.server.services.forks.tasks import task_lanes

SOURCE = "s0000000-0000-0000-0000-000000000000"
ORIGIN = "o0000000-0000-0000-0000-000000000000"


def _run(run_id: str, final: str | None) -> dict:
    return {"task_run_id": run_id, "final_checkpoint_id": final}


def _plan(tasks: dict[str, list[dict]]) -> LedgerPlan:
    plan = LedgerPlan(source_thread_id=SOURCE)
    for task_id, runs in tasks.items():
        plan.tasks.append(({"task_id": task_id}, runs))
        for r in runs:
            plan.run_map[r["task_run_id"]] = f"new-{r['task_run_id']}"
    return plan


def _inherited(*tasks: BorrowedTask) -> Borrowing:
    return Borrowing(turn_base=1, checkpoint_id="cp", task_lanes=tasks)


def test_a_task_of_the_source_is_read_there_at_its_newest_end():
    plan = _plan({"a": [_run("r1", "c1"), _run("r2", "c2")]})

    (task,) = task_lanes(SOURCE, plan, None)

    assert task == BorrowedTask(
        task_id="a", thread_id=SOURCE, checkpoint_id="c2", runs={"new-r1": "r1", "new-r2": "r2"}
    )


def test_a_task_the_source_borrows_is_read_where_the_source_reads_it():
    held = BorrowedTask(task_id="a", thread_id=ORIGIN, checkpoint_id="c1", runs={"r1": "o1"})
    plan = _plan({"a": [_run("r1", "c1")]})

    (task,) = task_lanes(SOURCE, plan, _inherited(held))

    assert task.thread_id == ORIGIN and task.checkpoint_id == "c1"
    assert task.runs == {"new-r1": "o1"}


def test_a_task_the_source_resumed_is_read_from_the_source():
    # The resume wrote the lane into the source, its records naming the
    # source's own ids for the carried run as well as the new one.
    held = BorrowedTask(task_id="a", thread_id=ORIGIN, checkpoint_id="c1", runs={"r1": "o1"})
    plan = _plan({"a": [_run("r1", "c1"), _run("r2", "c2")]})

    (task,) = task_lanes(SOURCE, plan, _inherited(held))

    assert task.thread_id == SOURCE and task.checkpoint_id == "c2"
    assert task.runs == {"new-r1": "r1", "new-r2": "r2"}


def test_a_task_that_never_checkpointed_has_no_lane_to_read():
    plan = _plan({"a": [_run("r1", None)], "b": [_run("r2", "c2")]})

    assert [t.task_id for t in task_lanes(SOURCE, plan, None)] == ["b"]


def test_a_task_whose_lane_the_source_holds_is_read_there_with_no_later_run():
    # The source carried the lane in for a resume that never made a run of
    # its own, or one after the cut: its lane is at the same checkpoint,
    # stamped with the source's own run ids.
    held = BorrowedTask(task_id="a", thread_id=ORIGIN, checkpoint_id="c1", runs={"r1": "o1"})
    plan = _plan({"a": [_run("r1", "c1")]})

    (task,) = task_lanes(SOURCE, plan, _inherited(held), frozenset({"a"}))

    assert task.thread_id == SOURCE and task.checkpoint_id == "c1"
    assert task.runs == {"new-r1": "r1"}
