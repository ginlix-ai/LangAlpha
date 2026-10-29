"""Billing rows for subagent usage.

A subagent's calls are billed on their own ``msg_type='task'`` rows under the
response that owns the task, whichever path owns it: the turn's collector, an
orphan collector, a stopped run's kill, or the resume that takes an earlier
run's round off the task. Every path writes its rows here. Kept apart from
collection, which drives the first three, so the resume's biller, handed to
the agent's registry, carries none of the archive machinery.
"""

import logging

logger = logging.getLogger(__name__)

async def persist_subagent_usage(
    response_id: str,
    tasks: list,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    is_byok: bool = False,
) -> None:
    """Persist each subagent's token usage as a separate row with msg_type='task'."""
    from ptc_agent.agent.middleware.background_subagent.registry import take_task_usage
    from src.server.services.background_registry_store import BackgroundRegistryStore

    # Snapshot-and-clear usage under the registry lock, gated on still
    # owning the task (collector_response_id == response_id). A resume
    # clears that field and bills an earlier run's usage itself, so each
    # round is taken exactly once.
    bg_registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)

    if bg_registry is not None:
        claimed = await bg_registry.take_owned_usage(tasks, response_id)
    else:
        # Registry gone (thread teardown): the tasks still carry their claim,
        # and the take has no awaits, so it is atomic without the lock.
        claimed = take_task_usage(tasks, response_id)

    await _persist_takes(
        claimed, response_id, thread_id, workspace_id, user_id, is_byok
    )


async def bill_resumed_round(thread_id: str, take) -> None:
    """Bill a resumed subagent's earlier round under the run that spent it.

    The resume has taken this usage off the task, so it is billed here or
    not at all, and only when that run's ending bills its subagents: the
    same rule its own collector or kill applies, read off its settled row.
    """
    from src.server.contracts.status import bills_subagents
    from src.server.database.runs.lifecycle import get_run_ending

    ending = await get_run_ending(take.response_id)
    if not bills_subagents(ending):
        logger.info(
            f"[SubagentUsage] Earlier round of task {take.task.task_id} in "
            f"thread_id={thread_id} not billed: run {take.response_id} ended "
            f"{ending.get('status') if ending else 'unknown'}"
        )
        return
    metadata = ending.get("metadata") or {}
    await _persist_takes(
        [take],
        take.response_id,
        thread_id,
        str(ending["workspace_id"]),
        str(ending["user_id"]),
        bool(metadata.get("is_byok", False)),
    )


async def _persist_takes(
    claimed: list,
    response_id: str,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    is_byok: bool,
) -> None:
    from src.server.services.persistence.usage import UsagePersistenceService

    if not claimed:
        return

    persisted_count = 0
    persisted_records = 0

    for take in claimed:
        task, records = take.task, take.records
        try:
            usage_service = UsagePersistenceService(
                thread_id=thread_id,
                workspace_id=workspace_id,
                user_id=user_id,
            )
            await usage_service.track_llm_usage(records)

            if take.tool_usage:
                usage_service.record_tool_usage_batch(take.tool_usage)

            # track_llm_usage([]) initializes _token_usage to a zeroed
            # dict, so tool-only tasks still get stamped; None only on its
            # internal cost-calculation error path, where skipping is the
            # documented is_byok fallback contract.
            if usage_service._token_usage is not None:
                usage_service._token_usage["task_id"] = task.task_id
                usage_service._token_usage["agent_id"] = task.agent_id
                usage_service._token_usage["subagent_type"] = task.subagent_type

            # settle_task_run_id makes the usage insert and the ledger's
            # billing-settle stamp one transaction; on failure (False) the
            # row stays countable for the recovery sweep to degraded-settle.
            persisted = await usage_service.persist_usage(
                response_id=response_id,
                msg_type="task",
                status="completed",
                is_byok=is_byok,
                settle_task_run_id=take.settle_run_id,
            )
            if not persisted:
                # The records left task memory before this call, so a swallowed
                # False loses them outright: the ledger row stays countable for
                # the sweep to degraded-settle, but no billing row will ever be
                # written for this task. Counting it as persisted would report
                # the loss as a success.
                logger.error(
                    "[SubagentUsage] usage persist reported failure for task "
                    "%s in thread_id=%s; %d record(s) are unrecoverable",
                    task.task_id,
                    thread_id,
                    len(records),
                )
                continue
            persisted_count += 1
            persisted_records += len(records)

        except Exception as e:
            logger.error(
                f"[SubagentUsage] Failed to persist usage for task {task.task_id} "
                f"in thread_id={thread_id}: {e}",
                exc_info=True,
            )

    if persisted_count:
        logger.info(
            f"[SubagentUsage] Persisted {persisted_count} subagent usage row(s) "
            f"({persisted_records} LLM calls) for response_id={response_id} "
            f"thread_id={thread_id}"
        )
