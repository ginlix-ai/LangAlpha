"""Fixtures for integration tests.

Provides:
- Database fixtures for integration tests against real PostgreSQL
- Singleton reset fixtures for MCP clients
- Sandbox provider selection, and Daytona outages reported as skips
"""

from __future__ import annotations

import functools
import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from daytona import (
    DaytonaA11yUnavailableError,
    DaytonaBadGatewayError,
    DaytonaConnectionError,
    DaytonaGitTransportFailedError,
    DaytonaRateLimitError,
    DaytonaRecordingFfmpegNotFoundError,
    DaytonaServiceUnavailableError,
    DaytonaTimeoutError,
)
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError


# ---------------------------------------------------------------------------
# Sandbox provider selection
# ---------------------------------------------------------------------------


def requested_sandbox_providers() -> list[str]:
    """SANDBOX_TEST_PROVIDERS (comma-separated), else SANDBOX_TEST_PROVIDER, else memory."""
    raw = os.getenv("SANDBOX_TEST_PROVIDERS") or os.getenv(
        "SANDBOX_TEST_PROVIDER", "memory"
    )
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def daytona_in_use() -> bool:
    return "daytona" in requested_sandbox_providers() and bool(
        os.environ.get("DAYTONA_API_KEY")
    )


# ---------------------------------------------------------------------------
# Daytona outages
#
# A test that runs on Daytona (marked ``provider_daytona``) and fails because
# Daytona refused, dropped or could not reach a request is re-run after a
# backoff; if the outage outlasts the re-runs, it is reported as skipped with a
# ``::warning::`` annotation instead of failed, so an outage does not read as a
# broken change. Anything else, an assertion included, still fails.
# ---------------------------------------------------------------------------

# Provider-side failures. A subclass naming one feature or one remote (no
# recorder, a git remote refusing) is about what the test asked for, so it
# stays a failure. Timeouts are left out, the SDK's connection timeouts
# included (it maps a client read timeout to DaytonaConnectionTimeoutError, a
# DaytonaConnectionError): a call that hangs is as likely the code under test
# as the provider.
_OUTAGE_ERRORS: tuple[type[BaseException], ...] = (
    DaytonaServiceUnavailableError,  # 503, e.g. "while the sandbox's host recovers"
    DaytonaBadGatewayError,  # 502
    DaytonaConnectionError,  # the request never completed, incl. connect timeouts
    DaytonaRateLimitError,  # 429 on the org every concurrent run shares
    SandboxHostUnavailableError,  # the runtime's own wrapping of that 503
)
_NOT_OUTAGES: tuple[type[BaseException], ...] = (
    DaytonaTimeoutError,
    DaytonaA11yUnavailableError,
    DaytonaRecordingFfmpegNotFoundError,
    DaytonaGitTransportFailedError,
)

# Two re-runs, 30 s then 60 s apart: "retry in a few moments" is the provider's
# own advice. The suite-wide cap bounds a full outage to a few minutes of
# waiting, and after a few Daytona tests in a row end in an outage the rest
# are skipped without trying, since each attempt can take minutes to fail.
_RERUNS = 2
_RERUN_DELAY_S = 30
_SUITE_RERUNS = 8
_OUTAGES_BEFORE_GIVING_UP = 3
_SKIP_PREFIX = "Daytona unavailable: "

_on_daytona = pytest.StashKey[bool]()
_skipped: list[tuple[str, str]] = []  # (nodeid, reason) of every outage skip
_last_outage = ""
# Counted per process. The Daytona job runs serially; under xdist each worker
# would keep its own counts, and the no-pass rule would need them merged.
_outages_in_a_row = 0
# Daytona calls that succeeded. A pass of a test marked provider_daytona is not
# proof Daytona answered: some only check local state before any call.
_daytona_answered = 0
_ANNOTATIONS = 10  # GitHub shows ten warnings per step


def daytona_outage(exc: BaseException | None) -> BaseException | None:
    """The provider-side failure behind *exc*, or None when the test itself failed.

    Follows ``raise ... from`` (code that wraps a provider error keeps it as the
    cause) but not implicit context: an assertion raised while handling a 503
    is still the assertion's failure. So is a 503 raised by the cleanup after a
    failed assertion, which Python raises with the assertion as its context;
    reruns that keep meeting it would otherwise skip the failure away. An
    exception group is an outage only when every member is.
    """
    if exc is None or isinstance(exc, AssertionError):
        return None
    if isinstance(exc, BaseExceptionGroup):
        found = [daytona_outage(e) for e in exc.exceptions]
        return found[0] if found and all(found) else None
    if _after_a_failure(exc):
        return None
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, _OUTAGE_ERRORS) and not isinstance(exc, _NOT_OUTAGES):
            return exc
        exc = exc.__cause__
    return None


def _after_a_failure(exc: BaseException) -> bool:
    """Whether *exc* was raised while a failed check was being handled.

    Only a check counts: the SDK raises its own errors while handling an
    HTTP client's, and those are still the provider's.
    """
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc.__cause__, exc.__context__]
    while pending:
        e = pending.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        if isinstance(e, (AssertionError, pytest.fail.Exception)):
            return True
        pending += [e.__cause__, e.__context__]
    return False


def _describe(exc: BaseException) -> str:
    # A 502 carries the gateway's whole HTML page; its first line is enough.
    first_line = (str(exc).strip().splitlines() or [""])[0]
    return f"{type(exc).__name__}: {first_line}"[:300]


def _count_daytona_answers() -> None:
    """Count every Daytona call that came back: a sandbox created or found,
    a command run. The provider and runtime are the only ways tests reach
    Daytona, so a run with none of these heard nothing from it."""
    from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
    from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

    for owner, name in ((DaytonaProvider, "create"), (DaytonaProvider, "get"), (DaytonaRuntime, "exec")):
        original = getattr(owner, name)
        if getattr(original, "_counts_daytona", False):
            continue

        @functools.wraps(original)
        async def counted(*args, _original=original, **kwargs):
            global _daytona_answered
            result = await _original(*args, **kwargs)
            _daytona_answered += 1
            return result

        counted._counts_daytona = True
        setattr(owner, name, counted)


def pytest_configure(config):
    # In CI a Daytona run without a key would skip every Daytona test and pass
    # on the rest, proving nothing; locally the skip is the point.
    if (
        os.environ.get("CI")
        and "daytona" in requested_sandbox_providers()
        and not os.environ.get("DAYTONA_API_KEY")
    ):
        raise pytest.UsageError("Daytona was asked for, but DAYTONA_API_KEY is empty")
    # The outage counters are per process; split across xdist workers, the
    # rule that a run with no Daytona pass fails would never see a whole run.
    if daytona_in_use() and getattr(config.option, "numprocesses", None):
        raise pytest.UsageError("Run the Daytona tier serially: its outage counters are per process")
    if daytona_in_use():
        _count_daytona_answers()
    if daytona_in_use() and config.pluginmanager.hasplugin("rerunfailures"):
        if getattr(config.option, "max_suite_reruns", None) is None:
            config.option.max_suite_reruns = _SUITE_RERUNS


def pytest_collection_modifyitems(config, items):
    if not daytona_in_use():
        return
    reruns = config.pluginmanager.hasplugin("rerunfailures")
    for item in items:
        if item.get_closest_marker("provider_daytona") is None:
            continue
        item.stash[_on_daytona] = True
        if reruns and item.get_closest_marker("flaky") is None:
            item.add_marker(
                pytest.mark.flaky(
                    reruns=_RERUNS,
                    reruns_delay=_RERUN_DELAY_S,
                    reruns_delay_backoff_factor=2,
                    condition=lambda error: daytona_outage(error) is not None,
                )
            )


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if item.stash.get(_on_daytona, False) and (
        _outages_in_a_row >= _OUTAGES_BEFORE_GIVING_UP
    ):
        pytest.skip(
            f"{_SKIP_PREFIX}{_last_outage} (not tried after "
            f"{_outages_in_a_row} Daytona tests in a row hit an outage)"
        )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if report.failed and item.stash.get(_on_daytona, False) and call.excinfo:
        outage = daytona_outage(call.excinfo.value)
        if outage is not None:
            # A string, so it survives xdist's report serialization.
            report.daytona_outage = _describe(outage)
    return report


@pytest.hookimpl(wrapper=True)
def pytest_runtest_logreport(report):
    """Turn the final report of an outage into a skip.

    Here rather than in makereport because only here is a report known to be
    final: pytest-rerunfailures marks the attempts it re-runs ``rerun`` before
    logging them. A wrapper, so the skip is in place before any reader runs,
    the session's failure count included.
    """
    global _last_outage, _outages_in_a_row
    reason = getattr(report, "daytona_outage", None)
    if reason and report.outcome == "failed":
        path, lineno = report.location[:2]
        report.outcome = "skipped"
        report.longrepr = (path, None if lineno is None else lineno + 1, _SKIP_PREFIX + reason)
        _last_outage = reason
        _outages_in_a_row += 1
    elif report.when == "call" and report.passed and "provider_daytona" in report.keywords:
        _outages_in_a_row = 0
    # Matched by text as well, so an xdist controller, which only sees the
    # converted report, still lists it.
    if report.skipped and isinstance(report.longrepr, tuple):
        text = str(report.longrepr[2])
        if _SKIP_PREFIX in text:
            _skipped.append((report.nodeid, text[text.index(_SKIP_PREFIX) + len(_SKIP_PREFIX):]))
    return (yield)


def _proved_nothing() -> bool:
    # Outages, and not one Daytona call came back: Daytona down, or a change
    # that points the provider somewhere it cannot connect. A run that proved
    # nothing must not pass, so it fails and says which to look for.
    return bool(_skipped) and _daytona_answered == 0


def _outages_fail() -> bool:
    # The nightly run is the backstop for what no PR triggers, so there a skip
    # for an outage is a failure: the run did not check what it exists to.
    return bool(_skipped) and os.environ.get("DAYTONA_OUTAGES_FAIL") == "1"


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    if (_proved_nothing() or _outages_fail()) and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if not _skipped:
        return
    terminalreporter.section("Daytona outages, reported as skipped")
    if _outages_fail() and not _proved_nothing():
        terminalreporter.write_line(
            "This is the nightly run, where an outage that outlasts the re-runs "
            "fails the run: it did not check what it exists to."
        )
    if _proved_nothing():
        terminalreporter.write_line(
            "Daytona answered no call, so this run fails: Daytona is down, or this "
            "change stops the provider reaching it. Re-run once Daytona is back."
        )
    for nodeid, reason in _skipped:
        terminalreporter.write_line(f"{nodeid}: {reason}")
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    lines = [f"{nodeid}: {reason}" for nodeid, reason in _skipped[:_ANNOTATIONS]]
    if len(_skipped) > _ANNOTATIONS:
        lines.append(f"{len(_skipped) - _ANNOTATIONS} more Daytona tests skipped; see the log")
    for line in lines:
        # Workflow-command escaping: a raw %, CR or LF would cut the message.
        line = line.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        terminalreporter.write_line(f"::warning title=Daytona unavailable::{line}")


# ---------------------------------------------------------------------------
# MCP singleton teardown (pre-existing)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def _reset_fmp_singleton():
    """Close and reset the FMP client singleton after each test."""
    yield
    from data_client.fmp import close_fmp_client
    await close_fmp_client()


@pytest_asyncio.fixture(autouse=True)
async def _reset_ginlix_singleton():
    """Close and reset the ginlix-data httpx client after each test."""
    yield
    try:
        import plugins.langalpha_market_data.price_data_mcp_server as mod
        if hasattr(mod, "_ginlix_http") and mod._ginlix_http is not None:
            await mod._ginlix_http.aclose()
            mod._ginlix_http = None
    except (ImportError, AttributeError):
        pass


# ---------------------------------------------------------------------------
# Database integration fixtures
# ---------------------------------------------------------------------------

# Tables in dependency (FK) order -- children first so TRUNCATE CASCADE is safe
_ALL_TABLES = [
    "platform_secret_rollouts",
    "automation_executions",
    "automations",
    "conversation_feedback",
    "conversation_usages",
    "conversation_responses",
    "conversation_queries",
    "conversation_threads",
    "workspace_mcp_tool_schemas",
    "workspace_mcp_servers",
    "user_mcp_servers",
    "user_mcp_builtin_disables",
    "workspace_skill_disables",
    "user_skills",
    "user_plugins",
    "workspace_file_uploads",
    "workspace_files",
    "workspace_file_blobs",
    "watchlist_items",
    "watchlists",
    "user_portfolios",
    "user_api_keys",
    "user_preferences",
    "workspaces",
    "users",
]


def _build_db_uri() -> str:
    """Build PostgreSQL connection string from env vars (CI-compatible defaults).

    Uses TEST_DB_* env vars so the app's .env (which sets DB_HOST to
    host.docker.internal for Docker networking) does not bleed into tests
    that run on the host machine.
    """
    host = os.getenv("TEST_DB_HOST", "localhost")
    port = os.getenv("TEST_DB_PORT", "5432")
    name = os.getenv("TEST_DB_NAME", "langalpha_test")
    user = os.getenv("TEST_DB_USER", "postgres")
    password = os.getenv("TEST_DB_PASSWORD", "postgres")
    # Deliberately not DB_SSLMODE — reading it would let a production-shaped .env
    # dictate test TLS policy, which is what TEST_DB_* exists to prevent.
    sslmode = os.getenv("TEST_DB_SSLMODE", "prefer")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"


async def _run_alembic_upgrade(db_uri: str) -> None:
    """Run alembic migrations against the given database URI.

    Uses the project's alembic.ini + migrations/ directory as the single
    source of truth, so the test schema always matches production.

    Runs in a thread because the migration uses asyncio.run() internally
    (for LangGraph checkpoint setup), which would conflict with the
    already-running event loop in pytest-asyncio fixtures.
    """
    import asyncio

    from alembic import command
    from alembic.config import Config

    project_root = Path(__file__).resolve().parent.parent.parent
    alembic_cfg = Config(str(project_root / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(project_root / "migrations"))

    # Convert psycopg URI to SQLAlchemy+psycopg URI for alembic
    sa_url = db_uri.replace("postgresql://", "postgresql+psycopg://", 1)
    alembic_cfg.set_main_option("sqlalchemy.url", sa_url)

    await asyncio.to_thread(command.upgrade, alembic_cfg, "head")


@pytest.fixture(scope="session")
def test_db_uri() -> str:
    """Build and return the test database URI."""
    return _build_db_uri()


@pytest_asyncio.fixture(scope="session")
async def test_db_pool(test_db_uri):
    """Session-scoped async connection pool for integration tests.

    Runs alembic migrations (single source of truth) to create the schema,
    yields the pool, then tears down by truncating all tables.
    """
    import psycopg

    # Nuke the whole schema for a clean slate before migrations. A named
    # drop-list goes stale every time a migration adds a table (it silently
    # missed subagent_runs/subagent_tasks/hook_outbox/provenance_*), and a
    # survivor breaks the re-run: alembic starts from scratch against
    # objects that still exist. CI never sees this — its postgres is fresh
    # per run; local reruns are the case this must survive.
    async with await psycopg.AsyncConnection.connect(
        test_db_uri, autocommit=False
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("DROP SCHEMA public CASCADE")
            await cur.execute("CREATE SCHEMA public")
        await conn.commit()

    # Run alembic migrations -- the single source of truth for schema
    await _run_alembic_upgrade(test_db_uri)

    # Now create the pool for actual test operations
    pool = AsyncConnectionPool(
        conninfo=test_db_uri,
        min_size=1,
        max_size=5,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        },
        open=False,
    )
    await pool.open()
    await pool.wait()

    yield pool

    # Teardown: truncate (not drop) so the schema remains for debugging
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for table in _ALL_TABLES:
                await cur.execute(f"TRUNCATE TABLE {table} CASCADE")

    await pool.close()


@pytest_asyncio.fixture
async def cleanup_tables(test_db_pool):
    """Truncate all tables after each test for isolation.

    NOT autouse — only activated when a test explicitly requests it
    (or transitively via seed_user/seed_workspace).
    """
    yield
    async with test_db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for table in _ALL_TABLES:
                await cur.execute(f"TRUNCATE TABLE {table} CASCADE")


@pytest_asyncio.fixture
async def db_conn(test_db_pool):
    """Yield a single async connection from the test pool.

    The connection is used in autocommit mode (pool default).
    """
    async with test_db_pool.connection() as conn:
        yield conn


@pytest_asyncio.fixture
async def patched_get_db_connection(test_db_pool, test_db_uri):
    """Patch get_db_connection to use the test pool instead of production.

    This allows database module functions (workspace.py, user.py, etc.)
    to transparently use the test database. The shared lock session opens on
    it too, fresh per test so no lock outlives the test that took it.
    """
    from contextlib import asynccontextmanager

    import psycopg

    from src.server.database.session_lock import close_shared_lock_session

    @asynccontextmanager
    async def _test_get_db_connection(conn=None):
        if conn is not None:
            yield conn
            return
        async with test_db_pool.connection() as owned:
            yield owned

    # Every module that does a module-level `from .pool import
    # get_db_connection` holds its own local reference, so each one needs its
    # own patch. Which modules are already loaded depends on what the session
    # ran before this test, so a hand-kept list is wrong in both directions
    # (it once missed mcp_tool_schemas, and later every service a plugin
    # write goes through). Sweep every loaded module still holding the pool's
    # function instead. A module first imported during the test takes the
    # patched source on pool.get_db_connection at import.
    import importlib
    import pkgutil
    import sys

    import src.server.database as _database_pkg
    from src.server.database.pool import get_db_connection as _pool_gdc

    for _info in pkgutil.walk_packages(
        _database_pkg.__path__, prefix="src.server.database."
    ):
        importlib.import_module(_info.name)
    holders = [
        module
        for name, module in list(sys.modules.items())
        if name.startswith(("src.", "ptc_agent."))
        and getattr(module, "get_db_connection", None) is _pool_gdc
    ]
    from contextlib import ExitStack

    async def _test_session_connection():
        return await psycopg.AsyncConnection.connect(
            test_db_uri, autocommit=True, prepare_threshold=0
        )

    with ExitStack() as stack:
        for module in holders:
            stack.enter_context(
                patch.object(module, "get_db_connection", _test_get_db_connection)
            )
        stack.enter_context(
            patch("src.server.database.pool.open_session_connection", _test_session_connection)
        )
        await close_shared_lock_session()
        try:
            yield _test_get_db_connection
        finally:
            await close_shared_lock_session()


@pytest_asyncio.fixture
async def seed_response(patched_get_db_connection):
    """Async callable that seeds a response row the trigger-compliant way.

    The terminal-immutability trigger rejects INSERTs with a terminal
    status, so seeding inserts ``in_progress`` and finalizes with an
    UPDATE, the same shape production takes through the lifecycle API.
    """

    async def _seed(
        conversation_response_id: str,
        conversation_thread_id: str,
        turn_index: int,
        status: str = "completed",
        execution_time=None,
    ):
        async with patched_get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO conversation_responses
                        (conversation_response_id, conversation_thread_id,
                         turn_index, status, created_at)
                    VALUES (%s, %s, %s, 'in_progress', NOW())
                    """,
                    (
                        conversation_response_id,
                        conversation_thread_id,
                        turn_index,
                    ),
                )
                await cur.execute(
                    """
                    UPDATE conversation_responses
                    SET status = %s,
                        execution_time = COALESCE(%s, execution_time)
                    WHERE conversation_response_id = %s
                    """,
                    (status, execution_time, conversation_response_id),
                )

    return _seed


@pytest.fixture
def test_user_id() -> str:
    """Deterministic test user ID."""
    return "test-user-integration-001"


@pytest_asyncio.fixture
async def seed_user(patched_get_db_connection, cleanup_tables, test_user_id):
    """Insert a test user and return the user dict.

    Most database modules require a user row to exist (FK constraints).
    """
    from src.server.database.user import create_user

    user = await create_user(
        user_id=test_user_id,
        email="test@example.com",
        name="Test User",
    )
    return user


@pytest_asyncio.fixture
async def seed_workspace(seed_user, patched_get_db_connection):
    """Insert a test workspace and return its dict.

    Depends on seed_user to satisfy FK constraints.
    """
    from src.server.database.workspace import create_workspace

    ws = await create_workspace(
        user_id=seed_user["user_id"],
        name="Test Workspace",
        description="Integration test workspace",
        status="running",
    )
    return ws
