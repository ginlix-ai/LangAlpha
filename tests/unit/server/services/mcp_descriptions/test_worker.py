"""The worker writes a description only when someone would read it, from the
tools the scope's grant leaves the agent, checks the answer, and lands it with
the owners' refresh in one step."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config.models import McpDescriptionsConfig
from src.server.services.brokerage_capabilities import group_keys_for
from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions import jobs, worker
from src.server.services.mcp_descriptions.keys import Scope, dial_identity, server_digest
from src.server.services.mcp_descriptions.schema import ServerProfile

QUOTES = {"name": "get_quotes", "description": "quotes", "input_schema": {}}
PENDING = {"name": "get_pending_instruction", "description": "pending", "input_schema": {}}
REFUSED = {"name": "place_event_instruction", "description": "order", "input_schema": {}}
NAMES = [t["name"] for t in (QUOTES, PENDING, REFUSED)]
WEBULL = brokerage_by_name("webull").url
MINE = "https://mcp.example.com/mcp"
EVERY_GROUP = Scope.at_brokerage("webull", group_keys_for("webull"))


class FakeDB:
    JOB_LEASE_S = 600

    def __init__(self):
        self.current: dict[str, dict] = {}
        self.finished: list[int] = []
        self.bumped: list[str] = []
        self.rows: dict[tuple[str, str], dict] = {}

    async def current_digest(self, scope_key, *, prompt_version, conn=None):
        return (self.current.get(scope_key) or {}).get("server_digest")

    async def complete_job(self, job_id, attempts, *, scope_key, owners, **row):
        self.current[scope_key] = row
        self.bumped += sorted(owners)
        return True

    async def finish_job(self, job_id, attempts):
        self.finished.append(job_id)

    async def row_for(self, user_id, server_name, *, conn=None):
        return self.rows.get((user_id, server_name))

    async def bump_workspaces(self, user_ids, *, conn=None):
        self.bumped += sorted(user_ids)

    async def rows_at_host(self, host, *, conn=None):
        def row(user_id, granted):
            connection = {"server_url": WEBULL, "granted_capabilities": granted}
            return {"user_id": user_id, "name": "webull", "url": WEBULL, "connection": connection}

        return [row("u1", list(group_keys_for("webull"))), row("u2", ["market_data"])]


class FakeLLM:
    def __init__(self):
        self.calls: list[dict] = []
        self.answer = "d"
        self.error: Exception | None = None

    async def complete(self, **call):
        self.calls.append(call)
        if self.error:
            raise self.error
        return ServerProfile(description=self.answer)

    def described(self) -> list[list[str]]:
        return [[n for n in NAMES if f'"{n}"' in c["user_prompt"]] for c in self.calls]


@pytest.fixture
def env(monkeypatch):
    db, llm = FakeDB(), FakeLLM()
    monkeypatch.setattr(worker, "db", db)
    monkeypatch.setattr(jobs, "db", db)
    monkeypatch.setattr(worker, "platform_model", lambda: "platform-model")
    return SimpleNamespace(db=db, llm=llm)


def _job(tools, scope=EVERY_GROUP, user_id=None, server="webull"):
    return {"job_id": 1, "scope_key": scope.key, "tools": tools, "user_id": user_id,
            "server_name": server, "attempts": 1}


def _mine(env, description=""):
    env.db.rows[("u9", "mine")] = {"url": MINE, "description": description, "connection": None}
    return Scope.of("u9", "mine", MINE, None, dial_identity("mine", env.db.rows[("u9", "mine")]))


@pytest.mark.asyncio
async def test_a_brokerage_is_described_on_the_platform_model_for_the_users_who_read_it(env):
    assert await worker.run_job(_job([QUOTES, PENDING]), env.llm)

    row = env.db.current[EVERY_GROUP.key]
    assert (row["model"], row["description"], env.llm.calls[0]["user_id"]) == (
        "platform-model", "d", None
    )
    assert env.db.bumped == ["u1"]


@pytest.mark.asyncio
async def test_the_description_never_covers_a_tool_no_grant_can_call(env):
    await worker.run_job(_job([QUOTES, PENDING, REFUSED]), env.llm)

    assert env.llm.described() == [["get_quotes", "get_pending_instruction"]]
    assert env.db.current[EVERY_GROUP.key]["server_digest"] == server_digest([QUOTES, PENDING])


@pytest.mark.asyncio
async def test_the_description_covers_only_what_the_grant_leaves_the_agent(env):
    scope = Scope.at_brokerage("webull", ["market_data"])

    await worker.run_job(_job([QUOTES, PENDING, REFUSED], scope=scope), env.llm)

    assert env.llm.described() == [["get_quotes"]]
    assert (list(env.db.current), env.db.bumped) == ([scope.key], ["u2"])


@pytest.mark.asyncio
async def test_a_rerun_over_the_same_tools_asks_nothing(env):
    await worker.run_job(_job([QUOTES]), env.llm)

    assert not await worker.run_job(_job([QUOTES]), env.llm)
    assert (len(env.llm.calls), env.db.finished) == (1, [1])


@pytest.mark.asyncio
async def test_tools_that_change_back_are_described_again_rather_than_left_stale(env):
    changed = {**QUOTES, "description": "quotes and bars"}
    for tools in ([QUOTES], [changed], [QUOTES]):
        await worker.run_job(_job(tools), env.llm)

    assert len(env.llm.calls) == 3
    assert env.db.current[EVERY_GROUP.key]["server_digest"] == server_digest([QUOTES])


@pytest.mark.asyncio
async def test_a_users_server_is_described_on_their_model_for_them_alone(env):
    tool = {"name": "lookup", "description": "", "input_schema": {}}
    scope = _mine(env)

    await worker.run_job(_job([tool], scope=scope, user_id="u9", server="mine"), env.llm)

    assert env.llm.calls[0]["user_id"] == "u9"
    assert (list(env.db.current), env.db.current[scope.key]["model"]) == (
        [scope.key], None
    )
    assert env.db.bumped == ["u9"]


@pytest.mark.asyncio
async def test_no_description_is_written_that_nobody_would_read(env):
    tool = {"name": "lookup", "description": "", "input_schema": {}}
    scope = _mine(env, description="My notes server.")
    job = _job([tool], scope=scope, user_id="u9", server="mine")

    assert not await worker.run_job(job, env.llm)
    env.db.rows[("u9", "mine")] = {"url": "https://other.example.com/mcp", "description": "",
                                    "connection": None}
    assert not await worker.run_job(job, env.llm)
    del env.db.rows[("u9", "mine")]
    assert not await worker.run_job(job, env.llm)
    assert (env.llm.calls, env.db.finished) == ([], [1, 1, 1])


@pytest.mark.asyncio
async def test_a_grant_that_leaves_no_tools_is_not_described(env):
    assert not await worker.run_job(_job([REFUSED], scope=Scope.at_brokerage("webull", ())), env.llm)
    assert (env.llm.calls, env.db.finished) == ([], [1])


@pytest.mark.asyncio
async def test_users_servers_are_not_described_once_switched_off(env, monkeypatch):
    tool = {"name": "lookup", "description": "", "input_schema": {}}
    job = _job([tool], scope=_mine(env), user_id="u9", server="mine")
    monkeypatch.setattr(
        worker, "get_mcp_descriptions_config", lambda: McpDescriptionsConfig(user_servers=False)
    )

    assert not await worker.run_job(job, env.llm)
    assert (env.llm.calls, env.db.finished) == ([], [1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    ["", "word " * 200, "Places orders; call place_event_instruction to trade."],
)
async def test_an_answer_the_agent_should_not_read_is_retried_not_stored(env, answer):
    env.llm.answer = answer

    with pytest.raises(ValueError):
        await worker.run_job(_job([QUOTES, PENDING], scope=Scope.at_brokerage("webull", ())), env.llm)
    assert env.db.current == {}


def test_an_answer_is_stored_as_one_line():
    assert worker._checked(" Reads quotes.\n\nUse it for prices. ", EVERY_GROUP) == (
        "Reads quotes. Use it for prices."
    )


@pytest.mark.asyncio
async def test_a_failed_call_writes_nothing_and_bumps_no_one(env):
    env.llm.error = RuntimeError("model down")

    with pytest.raises(RuntimeError, match="model down"):
        await worker.run_job(_job([QUOTES]), env.llm)
    assert (env.db.current, env.db.bumped) == ({}, [])
