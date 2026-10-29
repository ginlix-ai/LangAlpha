"""The worker: claims queued jobs and writes descriptions, in every worker.

Nothing is kept between passes and nothing is consulted from process memory.
A job is a Postgres row claimed under its scope's advisory lock with a lease
(``mcp_descriptions.claim_job``), so a second worker never runs the same scope
at once, and a worker that dies mid-job leaves the row for another to claim
when the lease runs out. A run stops itself inside the lease, and every write
it makes is fenced by its claim.

A new description changes prompt text only, so landing one bumps the owners'
workspaces and nothing else, in the same transaction that writes it: every
user whose row reads that brokerage scope, or the one user whose own server
it is.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from typing import Any, Optional

from src.config.settings import get_mcp_descriptions_config
from src.server.database import mcp_descriptions as db
from src.server.models.mcp_server import DESCRIPTION_MAX
from src.server.services.brokerage_capabilities import denied_tools
from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions.jobs import enqueue_if_new, owners_of
from src.server.services.mcp_descriptions.keys import (
    Scope,
    described_tools,
    dial_identity,
    platform_model,
    server_digest,
)
from src.server.services.mcp_descriptions.prompts import (
    PROFILE_SYSTEM_PROMPT,
    profile_user_prompt,
)
from src.server.services.mcp_descriptions.prompt_text import users_own_words
from src.server.services.mcp_descriptions.schema import PROFILE_PROMPT_VERSION, ServerProfile
from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

_LOG = "[McpDescriptions]"
# Short, since stop waits on whatever model call is in flight and the singletons
# stop in turn: a cancelled job keeps its claim and reruns once its lease ends.
STOP_GRACE = 5.0
# Backoff after a failed run, by attempt: a model outage clears in minutes, a
# tool list a model cannot describe will not clear at all.
_RETRY_AFTER_S = (60, 300, 1800, 7200)
# A run stops itself well inside its lease, so no other worker reclaims a job
# whose first run is still calling the model.
JOB_RUN_S = db.JOB_LEASE_S - 120
# The seed is the reconciler for whatever queueing missed: a pass a worker died
# in, a requeue that failed. Each worker runs one at start and then this often.
SEED_EVERY_S = 6 * 3600


def _checked(text: str, scope: Scope) -> str:
    """The model's answer as one line no longer than a user's own description
    may be, or an error the job retries on. The prompt asks for this, but the
    line lands verbatim in other agents' prompts, so it is enforced here too,
    along with never naming a tool the scope's grant refuses."""
    line = " ".join(text.split())
    if not line or len(line) > DESCRIPTION_MAX:
        raise ValueError(f"description of {len(line)} characters")
    named = [
        tool
        for tool in denied_tools(scope.brokerage, scope.groups) or ()
        if "_" in tool and re.search(rf"\b{re.escape(tool)}\b", line)
    ]
    if named:
        raise ValueError(f"description names refused tools {named}")
    return line


async def _still_wanted(job: dict[str, Any], scope: Scope, digest: str) -> bool:
    """Whether the job would still change what someone reads: the scope's
    description was written from other tools, and a user's own server still
    exists at the same address with no description of its own, with users'
    servers still described at all."""
    if digest == await db.current_digest(scope.key, prompt_version=PROFILE_PROMPT_VERSION):
        return False
    if scope.brokerage is not None:
        return True
    if not get_mcp_descriptions_config().user_servers:
        return False
    row = await db.row_for(job["user_id"], job["server_name"])
    return (
        row is not None
        and not users_own_words(row)
        and Scope.of(
            job["user_id"],
            job["server_name"],
            row["url"],
            row["connection"],
            dial_identity(job["server_name"], row),
        )
        == scope
    )


async def run_job(job: dict[str, Any], llm_service: Any) -> bool:
    """Describe one job's server unless nobody would read the result.
    Returns whether a description landed.

    A user who connects into a brokerage scope while the description lands may
    have resolved their workspace before it committed, and missed the bump
    inside it, so the owners are read again once it has landed."""
    scope = Scope.parse(job["scope_key"])
    described = described_tools(scope, list(job["tools"] or []))
    digest = server_digest(described)
    if not described or not await _still_wanted(job, scope, digest):
        await db.finish_job(job["job_id"], job["attempts"])
        return False
    user_id = None if scope.brokerage else job["user_id"]
    label = brokerage_by_name(scope.brokerage).label if scope.brokerage else job["server_name"]
    profile = await llm_service.complete(
        user_id=user_id,
        system_prompt=PROFILE_SYSTEM_PROMPT,
        user_prompt=profile_user_prompt(label, described),
        response_schema=ServerProfile,
        slot="background",
    )
    description = _checked(profile.description, scope)
    owners = await owners_of(scope, user_id)
    landed = await db.complete_job(
        job["job_id"],
        job["attempts"],
        scope_key=scope.key,
        prompt_version=PROFILE_PROMPT_VERSION,
        server_digest=digest,
        model=platform_model() if scope.brokerage else None,
        description=description,
        owners=owners,
    )
    if landed and scope.brokerage and (late := await owners_of(scope) - owners):
        await db.bump_workspaces(late)
    return landed


async def _queue_settled(found: list[dict[str, Any]], conn=None) -> int:
    """Queue, per server, the settled list its sessions are served: the
    host-side one when it was taken under the current config, else the latest
    workspace one that was. A server with a connection is served its
    host-side list alone, since the sandbox never holds its credential. A list
    taken under a since-edited config is skipped: the edit rediscovers.
    Returns how many were queued."""
    by_server: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for snapshot in found:
        by_server.setdefault((snapshot["user_id"], snapshot["server_name"]), []).append(snapshot)
    queued = 0
    for (user_id, server_name), snapshots in by_server.items():
        row = await db.row_for(user_id, server_name, conn=conn)
        if row is None:
            continue
        try:
            fingerprint = dial_identity(server_name, row)
        except Exception:
            # A row that does not parse is served nothing, and must not end
            # the pass for every server after it.
            continue
        current = [
            s
            for s in snapshots
            if s["config_hash"] == fingerprint and (s["from_vendor"] or row["connection"] is None)
        ]
        if not current:
            continue
        served = max(current, key=lambda s: (s["from_vendor"], s["discovered_at"]))
        queued += await enqueue_if_new(
            user_id=user_id,
            server_name=server_name,
            tools=await db.snapshot_tools(served, conn=conn),
            from_vendor=served["from_vendor"],
            settled=served,
            conn=conn,
        )
    return queued


async def seed_from_snapshots() -> int | None:
    """Queue every discovered server whose scope has no current description,
    which is all of them after this ships or after a prompt-version bump:
    discovery queues new lists itself but never revisits a settled one. One
    worker's pass at a time, reading one server's tools at a time. Returns
    how many were queued, or None while another worker's pass runs."""
    async with db.seed_pass() as conn:
        if conn is None:
            return None
        return await _queue_settled(await db.settled_snapshots(conn=conn), conn)


async def requeue_server(user_id: str, server_name: str) -> None:
    """Queue one server from its settled list, for an edit that makes a
    generated description readable again without moving its tools: the user
    clearing their own. Never raises: the periodic seed queues what a failed
    requeue missed."""
    try:
        await _queue_settled(await db.settled_snapshots(user_id, server_name))
    except Exception:
        logger.warning(f"{_LOG} could not requeue {server_name} for {user_id}", exc_info=True)


class McpDescriptionWorker:
    _instance: Optional["McpDescriptionWorker"] = None

    def __init__(self) -> None:
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "McpDescriptionWorker":
        if cls._instance is None:
            cls._instance = McpDescriptionWorker()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        if not get_mcp_descriptions_config().enabled:
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="mcp-description-worker")
        logger.info(f"{_LOG} started")

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning(f"{_LOG} loop ended with an error at shutdown", exc_info=True)
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        interval = get_mcp_descriptions_config().interval_seconds
        seed_at = 0.0
        while not self._stop_event.is_set():
            try:
                # A pass that raised is retried on the next; one another worker
                # was running is left to it until the next period, and a pass
                # its worker died in is picked up by whichever worker is next.
                if time.monotonic() >= seed_at:
                    logger.info(f"{_LOG} seeded {await seed_from_snapshots()} jobs")
                    seed_at = time.monotonic() + SEED_EVERY_S
                while not self._stop_event.is_set() and await self.run_once():
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(f"{_LOG} pass failed", exc_info=True)
            jitter = interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                continue

    async def run_once(self) -> bool:
        """Claim and run one job. Returns whether there was one."""
        from src.server.app import setup

        if setup.llm_service is None:
            return False
        job = await db.claim_job(PROFILE_PROMPT_VERSION)
        if job is None:
            return False
        try:
            async with asyncio.timeout(JOB_RUN_S):
                landed = await run_job(job, setup.llm_service)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt = int(job["attempts"])
            retry = _RETRY_AFTER_S[min(attempt, len(_RETRY_AFTER_S)) - 1]
            logger.warning(
                f"{_LOG} job {job['job_id']} ({job['scope_key']}) attempt {attempt} "
                f"failed, retrying in {retry}s: {e!r}",
                exc_info=True,
            )
            await db.fail_job(
                job["job_id"], attempt, job["scope_key"], repr(e), retry_after_s=retry
            )
            return True
        logger.info(f"{_LOG} job {job['job_id']} ({job['scope_key']}) landed={landed}")
        return True
