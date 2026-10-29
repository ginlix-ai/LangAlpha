"""PostgreSQL invariants for trusted platform-secret rollout state."""

from __future__ import annotations

import hashlib

import pytest

from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretDefinition,
    ReconciledPlatformSecret,
)
from ptc_agent.core.sandbox.runtime import ExecResult


# The shared psycopg pool owns background connection workers on the session
# event loop. Run this module on that same loop so a test holding one pool
# connection can acquire a second without starving a dormant pool worker.
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(autouse=True)
def _reset_rollout_cache():
    """The per-process rollout cache must not leak identity between tests."""

    from src.server.services.platform_secret_rollout import (
        reset_platform_secret_rollout_cache,
    )

    reset_platform_secret_rollout_cache()
    yield
    reset_platform_secret_rollout_cache()


def _identity(
    *, secret_id: str, placeholder: str, egress_rule: str = ""
) -> ReconciledPlatformSecret:
    definition = PlatformSecretDefinition(
        source_env_var="FMP_API_KEY",
        sandbox_env_var="FMP_API_KEY",
        name_suffix="platform-fmp-api-key",
        description="Platform FMP API key",
        hosts=("financialmodelingprep.com",),
    )
    return ReconciledPlatformSecret(
        definition=definition,
        name=f"prod-{definition.name_suffix}",
        provider_secret_id=secret_id,
        placeholder=placeholder,
        egress_rule=egress_rule,
    )


def _capable_config():
    """CoreConfig stand-in for certify: a capable provider with a non-empty
    catalog, so ``platform_secrets_active`` is True (certify never iterates it)."""

    definition = PlatformSecretDefinition(
        source_env_var="FMP_API_KEY",
        sandbox_env_var="FMP_API_KEY",
        name_suffix="platform-fmp-api-key",
        description="Platform FMP API key",
        hosts=("financialmodelingprep.com",),
    )
    sandbox = type(
        "Sandbox", (), {"provider": "daytona", "platform_secrets": (definition,)}
    )()
    return type("Config", (), {"sandbox": sandbox})()


def _extra_identity() -> ReconciledPlatformSecret:
    definition = PlatformSecretDefinition(
        source_env_var="EXTRA_API_KEY",
        sandbox_env_var="EXTRA_API_KEY",
        name_suffix="platform-extra-api-key",
        description="Test-only extra platform key",
        hosts=("example.com",),
    )
    return ReconciledPlatformSecret(
        definition=definition,
        name=f"prod-{definition.name_suffix}",
        provider_secret_id="extra-id",
        placeholder="dtn_secret_extra",
    )


async def _seed_computer(seed_workspace) -> str:
    """A machine for the seeded project, unprovisioned: the lifecycle lives there."""
    from src.server.database.computer import create_computer
    from src.server.database.workspace import bind_workspace_to_computer

    computer = await create_computer(seed_workspace["user_id"], name="Cloud computer")
    computer_id = str(computer["computer_id"])
    assert await bind_workspace_to_computer(
        str(seed_workspace["workspace_id"]),
        computer_id,
        expected_computer_id=None,
        dir_name="test-ws",
    )
    return computer_id


async def _register(*identities: ReconciledPlatformSecret, provider: str = "daytona"):
    from src.server.services.platform_secret_rollout import (
        register_platform_secret_rollouts,
    )

    return await register_platform_secret_rollouts(
        list(identities),
        credential_values={
            identity.definition.sandbox_env_var: "credential-value"
            for identity in identities
        },
        provider=provider,
    )


class _VerifiedRuntime:
    """Emulates the in-sandbox hashing probe for the registered placeholder."""

    updates_env = True

    def __init__(self, *placeholders: str):
        self._lines = "\n".join(
            hashlib.sha256(p.encode()).hexdigest() for p in placeholders
        )

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        return ExecResult(f"{self._lines}\n", "", 0)


async def test_user_config_cannot_modify_trusted_rollout_columns(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.workspace import get_workspace, update_workspace

    workspace_id = str(seed_workspace["workspace_id"])
    await update_workspace(
        workspace_id,
        config={"platform_secret_version": 999},
    )
    workspace = await get_workspace(workspace_id)

    assert workspace["config"]["platform_secret_version"] == 999
    assert workspace["platform_secret_version"] == 0


async def test_identity_change_bumps_generation_and_keeps_certified_version(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.computer import get_computer
    from src.server.database.workspace import get_workspace
    from src.server.services.platform_secret_rollout import (
        stamp_platform_secret_version,
    )

    first = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    computer_id = await _seed_computer(seed_workspace)
    await stamp_platform_secret_version(
        computer_id=computer_id,
        expected_sandbox_id=None,
        rollout_set=first,
    )

    second = await _register(
        _identity(secret_id="secret-2", placeholder="dtn_secret_two")
    )
    computer = await get_computer(computer_id)
    workspace = await get_workspace(str(seed_workspace["workspace_id"]))

    assert second.generation == first.generation + 1
    # The row keeps the generation it was certified at: behind the new
    # generation (so it re-pends), but never zeroed. Version 0 stays reserved
    # for "never certified", preserving the plaintext/placeholder
    # discriminator that routes hot-swap vs scrub-restart. The stamp is one
    # statement, so the project shadow carries the same generation.
    assert computer["platform_secret_version"] == first.generation
    assert workspace["platform_secret_version"] == first.generation


async def test_set_membership_change_bumps_generation_monotonically(
    seed_workspace, patched_get_db_connection
):
    fmp = _identity(secret_id="secret-1", placeholder="dtn_secret_one")

    first = await _register(fmp)
    grown = await _register(fmp, _extra_identity())
    shrunk = await _register(fmp)

    # Addition and removal are both identity changes; the shared sequence
    # never regresses even though the removed row carried the highest value.
    assert grown.generation == first.generation + 1
    assert sorted(grown.bindings) == ["EXTRA_API_KEY", "FMP_API_KEY"]
    assert shrunk.generation == grown.generation + 1
    assert sorted(shrunk.bindings) == ["FMP_API_KEY"]


async def test_credential_rotation_without_identity_change_keeps_generation(
    seed_workspace, patched_get_db_connection
):
    from src.server.services.platform_secret_rollout import (
        register_platform_secret_rollouts,
    )

    identity = _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    first = await _register(identity)
    second = await register_platform_secret_rollouts(
        [identity],
        credential_values={"FMP_API_KEY": "rotated-credential-value"},
        provider="daytona",
    )

    assert second.generation == first.generation
    assert (
        second.rollouts[0].current_credential_sha256
        != first.rollouts[0].current_credential_sha256
    )


async def _computer_at(conn_factory, user_id, *, index, kind, version) -> None:
    import uuid

    async with conn_factory() as conn:
        await conn.execute(
            """INSERT INTO computers (computer_id, user_id, kind, provider_ref,
                       name, status, platform_secret_version)
               VALUES (%s, %s, %s, %s, %s, 'running', %s)""",
            (uuid.UUID(int=index), user_id, kind, f"{kind}-{index}", f"{kind}-{index}",
             version),
        )


async def test_a_second_kind_registers_beside_the_first(
    seed_user, patched_get_db_connection
):
    """A provider switch keeps the old kind's rows and generation, and the new
    kind's generation is above every generation and version stamped so far."""
    from src.server.services.platform_secret_rollout import (
        get_platform_secret_rollouts,
    )

    daytona = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    await _computer_at(
        patched_get_db_connection, seed_user["user_id"],
        index=1, kind="daytona", version=daytona.generation,
    )
    # A version stamped by an earlier build, above every rollout generation.
    await _computer_at(
        patched_get_db_connection, seed_user["user_id"],
        index=2, kind="e2b", version=daytona.generation + 40,
    )
    async with patched_get_db_connection() as conn:
        await conn.execute(
            "SELECT setval('platform_secret_generation_seq', %s)",
            (daytona.generation + 40,),
        )

    e2b = await _register(
        _identity(secret_id="e2b-1", placeholder="e2b_secret_fmp_api_key"),
        provider="e2b",
    )

    assert e2b.provider == "e2b"
    assert e2b.generation > daytona.generation + 40
    assert await get_platform_secret_rollouts("daytona", fresh=True) == daytona
    assert await get_platform_secret_rollouts("e2b", fresh=True) == e2b


async def test_an_identity_change_on_one_kind_bumps_only_that_kind(
    seed_workspace, patched_get_db_connection
):
    from src.server.services.platform_secret_rollout import (
        get_platform_secret_rollouts,
        register_platform_secret_rollouts,
    )

    fmp = _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    daytona = await _register(fmp)
    e2b = await _register(
        _identity(secret_id="e2b-1", placeholder="e2b_secret_fmp_api_key"),
        provider="e2b",
    )

    rerouted = await _register(
        _identity(
            secret_id="e2b-1",
            placeholder="e2b_secret_fmp_api_key",
            egress_rule='{"header": "apikey"}',
        ),
        provider="e2b",
    )
    assert rerouted.generation > e2b.generation
    assert await get_platform_secret_rollouts("daytona", fresh=True) == daytona

    rotated = await register_platform_secret_rollouts(
        [fmp], credential_values={"FMP_API_KEY": "rotated"}, provider="daytona"
    )
    assert rotated.generation == daytona.generation
    assert await get_platform_secret_rollouts("e2b", fresh=True) == rerouted


async def test_each_kind_lists_its_own_computers_behind(
    seed_user, patched_get_db_connection
):
    """A computer is behind only when its version differs from its own kind's
    generation, and a kind with no rows is never listed."""
    import uuid

    from src.server.services.platform_secret_rollout import (
        get_all_platform_secret_rollouts,
        list_workspaces_behind_platform_secret,
    )

    daytona = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    e2b = await _register(
        _identity(secret_id="e2b-1", placeholder="e2b_secret_fmp_api_key"),
        provider="e2b",
    )
    for index, kind, version in (
        (1, "daytona", daytona.generation),  # current
        (2, "daytona", e2b.generation),  # another kind's generation: behind
        (3, "e2b", e2b.generation),  # current
        (4, "e2b", daytona.generation),  # behind
        (5, "e2b", 0),  # never certified: behind
    ):
        await _computer_at(
            patched_get_db_connection, seed_user["user_id"],
            index=index, kind=kind, version=version,
        )

    rollout_sets = await get_all_platform_secret_rollouts()
    assert rollout_sets == {"daytona": daytona, "e2b": e2b}
    rows = await list_workspaces_behind_platform_secret(rollout_sets, limit=10)
    assert [(r["computer_id"], r["kind"]) for r in rows] == [
        (uuid.UUID(int=2), "daytona"),
        (uuid.UUID(int=4), "e2b"),
        (uuid.UUID(int=5), "e2b"),
    ]
    only_daytona = await list_workspaces_behind_platform_secret(
        {"daytona": daytona}, limit=10
    )
    assert [r["computer_id"] for r in only_daytona] == [uuid.UUID(int=2)]


async def test_egress_rule_change_is_an_identity_change(
    seed_workspace, patched_get_db_connection
):
    """On E2B the rule rides each sandbox, so a host or header edit re-pends them.

    An empty rule is what a provider that keeps the routing registers, and it
    has to match the column's default or the upgrade would re-pend that fleet.
    """
    routed = '{"header": "apikey", "hosts": ["financialmodelingprep.com"]}'
    first = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    unchanged = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one", egress_rule="")
    )
    assert unchanged.generation == first.generation
    assert unchanged.rollouts[0].egress_rule_sha256 == ""

    with_rule = await _register(
        _identity(
            secret_id="secret-1", placeholder="dtn_secret_one", egress_rule=routed
        )
    )
    assert with_rule.generation == first.generation + 1
    assert with_rule.rollouts[0].egress_rule_sha256 == hashlib.sha256(
        routed.encode()
    ).hexdigest()

    rerouted = await _register(
        _identity(
            secret_id="secret-1",
            placeholder="dtn_secret_one",
            egress_rule=routed.replace("apikey", "x-api-key"),
        )
    )
    assert rerouted.generation == with_rule.generation + 1


async def test_certification_attaches_a_verified_replacement(
    seed_workspace, patched_get_db_connection
):
    """Verify, then bind: the order that keeps an uncertified sandbox unbound."""
    from src.server.database.computer import try_bind_computer_provider_ref
    from src.server.database.workspace import get_workspace_identity
    from src.server.services.platform_secret_rollout import certify_platform_secrets

    rollout_set = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    computer_id = await _seed_computer(seed_workspace)

    config = _capable_config()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
        version = await certify_platform_secrets(
            config, runtime=_VerifiedRuntime("dtn_secret_one"), kind="daytona"
        )

    assert version == rollout_set.generation
    computer = await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="replacement-sandbox",
        expected_previous_provider_ref=None,
        platform_secret_version=version,
    )

    assert computer["platform_secret_version"] == rollout_set.generation
    assert computer["provider_ref"] == "replacement-sandbox"
    # The bind is one statement over machine and shadow.
    identity = await get_workspace_identity(str(seed_workspace["workspace_id"]))
    assert identity["sandbox_id"] == "replacement-sandbox"
    assert identity["provider_ref"] == "replacement-sandbox"


async def test_certification_without_a_catalog_stamps_the_zero_sentinel(
    seed_workspace, patched_get_db_connection
):
    """0 means "never certified — may hold plaintext env" (migration 021).

    A no-catalog deployment must stamp it rather than carry the previous
    sandbox's generation forward, which would leave a plaintext sandbox looking
    certified and invisible to the sweeper.
    """
    from src.server.services.platform_secret_rollout import certify_platform_secrets

    await _register(_identity(secret_id="secret-1", placeholder="dtn_secret_one"))
    sandbox = type("Sandbox", (), {"provider": "daytona", "platform_secrets": ()})()
    config = type("Config", (), {"sandbox": sandbox})()

    assert await certify_platform_secrets(config, runtime=None, kind="daytona") == 0


async def test_stamp_cas_is_fenced_on_the_sandbox_id(
    seed_workspace, patched_get_db_connection
):
    """The stamp names the sandbox it just scrubbed; a machine that has moved
    on to another sandbox must refuse it, and the same call lands when the
    identity matches."""
    from src.server.database.computer import (
        get_computer,
        try_bind_computer_provider_ref,
    )
    from src.server.services.platform_secret_rollout import (
        stamp_platform_secret_version,
    )

    rollout_set = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    computer_id = await _seed_computer(seed_workspace)
    await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="attached-sandbox",
        expected_previous_provider_ref=None,
        platform_secret_version=0,
    )

    with pytest.raises(RuntimeError, match="before its platform secret generation"):
        await stamp_platform_secret_version(
            computer_id=computer_id,
            expected_sandbox_id="not-the-attached-sandbox",
            rollout_set=rollout_set,
        )
    assert (await get_computer(computer_id))["platform_secret_version"] == 0

    await stamp_platform_secret_version(
        computer_id=computer_id,
        expected_sandbox_id="attached-sandbox",
        rollout_set=rollout_set,
    )
    computer = await get_computer(computer_id)
    assert computer["platform_secret_version"] == rollout_set.generation


async def test_bind_cas_rejects_a_concurrent_attachment(
    seed_workspace, patched_get_db_connection
):
    """A losing provisioner gets None, not the row, never a silent overwrite.

    None is the signal to delete the sandbox it just built and re-attach to the
    winner's; a last-writer-wins UPDATE here is how two workers both believe
    they own the workspace and one sandbox is billed with nothing pointing at
    it.
    """
    from src.server.database.computer import (
        get_computer,
        try_bind_computer_provider_ref,
    )
    from src.server.database.workspace import get_workspace_identity

    computer_id = await _seed_computer(seed_workspace)
    await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="winner-sandbox",
        expected_previous_provider_ref=None,
        platform_secret_version=7,
    )

    lost = await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="replacement-sandbox",
        expected_previous_provider_ref="stale-previous-sandbox",
        platform_secret_version=9,
    )

    assert lost is None
    computer = await get_computer(computer_id)
    assert computer["provider_ref"] == "winner-sandbox"
    assert computer["platform_secret_version"] == 7
    identity = await get_workspace_identity(str(seed_workspace["workspace_id"]))
    assert identity["sandbox_id"] == "winner-sandbox"


@pytest.mark.parametrize("stopped_status", ["stopping", "stopped"])
async def test_bind_cas_refuses_to_resurrect_a_stopped_computer(
    seed_workspace, patched_get_db_connection, stopped_status
):
    """Identity matching alone would let a recovery undo a concurrent stop.

    The dangerous race shares the sandbox id rather than changing it, so the
    id-only predicate matched: worker B stops the machine while worker A
    recovers the SAME sandbox, and since this statement writes ``running``
    unconditionally the bind resurrected the row. The outcome is a ``stopped``
    row whose sandbox is still up and billing, or a machine that restarts
    itself after a user stopped it.
    """
    from src.server.database.computer import (
        get_computer,
        try_bind_computer_provider_ref,
        update_computer_status,
    )
    from src.server.database.workspace import get_workspace_identity

    computer_id = await _seed_computer(seed_workspace)
    await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="sb-old",
        expected_previous_provider_ref=None,
        platform_secret_version=1,
    )
    await update_computer_status(computer_id, stopped_status)

    # Same expected id: this is a recovery of the very sandbox being stopped.
    lost = await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="sb-new",
        expected_previous_provider_ref="sb-old",
        platform_secret_version=2,
    )

    assert lost is None
    computer = await get_computer(computer_id)
    assert computer["status"] == stopped_status
    assert computer["provider_ref"] == "sb-old"
    identity = await get_workspace_identity(str(seed_workspace["workspace_id"]))
    assert identity["status"] == stopped_status
    assert identity["sandbox_id"] == "sb-old"


async def test_a_kind_without_rows_fails_readiness_until_registered(
    patched_get_db_connection, cleanup_tables
):
    """Absence is never memoized: a kind another worker registers later is
    read on the next call."""
    from src.server.services.platform_secret_rollout import (
        PlatformSecretReadinessError,
        get_platform_secret_rollouts,
        reset_platform_secret_rollout_cache,
    )

    await _register(_identity(secret_id="secret-1", placeholder="dtn_secret_one"))
    with pytest.raises(PlatformSecretReadinessError, match="not initialized for e2b"):
        await get_platform_secret_rollouts("e2b")

    e2b = await _register(
        _identity(secret_id="e2b-1", placeholder="e2b_secret_fmp_api_key"),
        provider="e2b",
    )
    reset_platform_secret_rollout_cache()  # as on a worker that did not register
    assert await get_platform_secret_rollouts("e2b") == e2b


async def test_sweep_filters_unsupported_providers_before_batch_limit(
    seed_user, patched_get_db_connection,
):
    import uuid

    from src.server.services.platform_secret_rollout import (
        list_workspaces_behind_platform_secret,
    )

    rollout = await _register(_identity(secret_id="sweep-secret", placeholder="opaque"))
    async with patched_get_db_connection() as conn:
        for index in range(1, 27):
            await conn.execute(
                """INSERT INTO computers (computer_id, user_id, kind, provider_ref,
                           name, status)
                   VALUES (%s, %s, 'docker', %s, %s, 'running')""",
                (uuid.UUID(int=index), seed_user["user_id"], f"docker-{index}",
                 f"docker-{index}"),
            )
        await conn.execute(
            """INSERT INTO computers (computer_id, user_id, kind, provider_ref,
                       status, provider_config, root_dir)
               VALUES (%s, %s, 'daytona', 'daytona-ref', 'running',
                       '{"auto_stop_interval":600}', '/custom')""",
            (uuid.UUID(int=27), seed_user["user_id"]),
        )
    rows = await list_workspaces_behind_platform_secret({"daytona": rollout}, limit=1)
    assert len(rows) == 1
    assert rows[0]["computer_id"] == uuid.UUID(int=27)
    assert rows[0]["kind"] == "daytona"
    assert rows[0]["provider_ref"] == "daytona-ref"
    assert rows[0]["provider_config"] == {"auto_stop_interval": 600}
    assert rows[0]["root_dir"] == "/custom"


async def test_sweep_lists_a_never_certified_fixed_env_computer(
    seed_user, patched_get_db_connection,
):
    """It cannot be scrubbed, so the sweep certifies it or rebuilds it instead."""
    import uuid

    from src.server.services.platform_secret_rollout import (
        list_workspaces_behind_platform_secret,
    )

    rollout = await _register(
        _identity(secret_id="sweep-secret", placeholder="opaque"), provider="e2b"
    )
    async with patched_get_db_connection() as conn:
        await conn.execute(
            """INSERT INTO computers (computer_id, user_id, kind, provider_ref,
                       status, platform_secret_version)
               VALUES (%s, %s, 'e2b', 'e2b-1', 'running', 0)""",
            (uuid.UUID(int=1), seed_user["user_id"]),
        )
    rows = await list_workspaces_behind_platform_secret({"e2b": rollout}, limit=10)
    assert [(row["computer_id"], row["kind"]) for row in rows] == [
        (uuid.UUID(int=1), "e2b")
    ]


async def test_sweep_pages_past_machines_that_never_converge(
    seed_user, patched_get_db_connection,
):
    """A full batch of machines that fail every pass must not hide the rest."""
    import uuid

    from src.server.services.platform_secret_rollout import (
        list_workspaces_behind_platform_secret,
    )

    certified = await _register(_identity(secret_id="page-secret", placeholder="p1"))
    rollout = await _register(_identity(secret_id="page-secret", placeholder="p2"))
    async with patched_get_db_connection() as conn:
        for index in range(1, 4):
            await conn.execute(
                """INSERT INTO computers (computer_id, user_id, kind, provider_ref,
                           name, status, platform_secret_version)
                   VALUES (%s, %s, 'daytona', %s, %s, 'running', %s)""",
                (uuid.UUID(int=index), seed_user["user_id"], f"d-{index}", f"d-{index}",
                 certified.generation),
            )
    first = await list_workspaces_behind_platform_secret({"daytona": rollout}, limit=2)
    rest = await list_workspaces_behind_platform_secret(
        {"daytona": rollout}, limit=2, after=first[-1]["computer_id"]
    )
    assert [r["computer_id"] for r in first] == [uuid.UUID(int=1), uuid.UUID(int=2)]
    assert [r["computer_id"] for r in rest] == [uuid.UUID(int=3)]
