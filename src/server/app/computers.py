"""Computer lifecycle routes shared with workspace aliases.

Aliases resolve workspace to computer and call these actions so transitions
cannot drift while both API paths remain live.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from ptc_agent.core.sandbox.providers import provider_class
from ptc_agent.core.sandbox.providers.ssh_errors import (
    SSHConnectError,
    SSHFolderMarked,
    connect_failure,
)
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.services.persistence.sync_result import BackupIncomplete
from src.server.services.background_starts import schedule_start
from src.server.app.status_stream import (
    SSE_HEADERS,
    sse_status_event,
    status_event_stream,
)
from src.server.database.computer import (
    computer_capacity_lock,
    get_computer,
    get_computers_for_user,
    observed_layout_version,
    rename_computer as db_rename_computer,
    set_computer_files_backup,
    update_computer_status,
)
from src.server.database.computer_names import ComputerNameTaken, name_taken
from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.workspace import (
    count_live_workspaces_by_computer,
    get_live_workspace_folders_for_computer,
)
from src.server.database.workspace_move import MoveRefused
from src.server.dependencies.usage_limits import (
    assert_provider_allowed,
    assert_spec_allowed,
    enforce_computer_count,
    enforce_computer_limit,
    get_provider_allowances,
    platform_gating_active,
)
from src.server.models.computer import (
    CLAIMABLE_FOR_START,
    ComputerActionResponse,
    ComputerAlwaysOnRequest,
    ComputerCreate,
    ComputerFilesBackupRequest,
    ComputerListResponse,
    ComputerProviderOption,
    ComputerProvidersResponse,
    ComputerReconnectRequest,
    ComputerReinstallRequest,
    ComputerTierSpec,
    ComputerRemoveRequest,
    ComputerRemoveResponse,
    ComputerRenameRequest,
    ComputerResponse,
    ComputerSessionResponse,
    ComputerSpecRequest,
    ComputerStorageResponse,
)
from src.server.models.workspace_move import MakePrimaryResponse, WorkspaceMoveOut
from src.server.services.computer_disk import (
    STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
    disk_from_row,
    storage_breakdown,
)
from src.server.services.computer_start_failure import (
    record_start_failure,
    start_failure_from_row,
)
from src.server.services.computer_errors import (
    ComputerBusyError,
    ComputerRemovalRefused,
    ComputerRepairRefused,
    MachineBusyError,
    ProviderNotOffered,
    UnknownResourceTier,
)
from src.server.services.spec_change import (
    SPEC_CHANGE_IN_PROGRESS,
    spec_change_from_row,
    spec_refusal,
)
from src.server.services.ssh_computers import (
    SSH_KIND,
    SSHComputerLimit,
    SSHFolderTaken,
    SSHKeyMissing,
    SSHCreatePlan,
    SSHUnavailable,
    plan_create,
    ssh_info,
    tunnel_of,
    tunnels_for,
)
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded
from src.server.services.ssh_removal import leave_machine
from src.server.services.ssh_tunnel_sweep import release_tunnel
from src.server.services.ssh_tunnels import tunnels_enabled
from src.server.services.workspace_move import (
    claim_workspace_move,
    move_error_detail,
    move_from_row,
    set_primary_computer,
)
from src.server.services.workspace_status_pubsub import (
    publish_computer_status_change,
    subscribe_to_computer_status,
)
from src.server.utils.api import CurrentUserId
from src.server.utils.error_sanitization import sandbox_failure_http

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/computers", tags=["Computers"])

# The server lays a hosted machine's folders again from the backup whenever it
# rebuilds one, so only a machine its owner brought may go without.
_FILES_BACKUP_REQUIRED = (
    "File backup can only be turned off on a computer you brought yourself."
)


@contextlib.asynccontextmanager
async def _computer_quota_decision(user_id: str, *, always: bool = False):
    """Serialize platform quota reads with the mutation that consumes the slot.

    ``always`` takes the lock without platform gating too, for a create whose
    own checks (a name, a folder) must hold until its insert.
    """
    if not (always or platform_gating_active()):
        yield
        return
    async with computer_capacity_lock(user_id):
        yield


class ComputerRuntimeUnavailable(RuntimeError):
    """Workspace aliases fall back when the runtime is absent; computer routes return 503."""


def _computer_manager() -> Any:
    """Defer provider-stack imports so read routes remain loadable without the runtime."""
    try:
        from src.server.services.computer_manager import ComputerManager
    except ImportError as e:
        raise ComputerRuntimeUnavailable(str(e)) from None
    return ComputerManager.get_instance()


def _sandbox_config() -> Any:
    """The deployment's sandbox settings, the same the manager builds computers from."""
    return _computer_manager().config.sandbox


@contextlib.asynccontextmanager
async def _computer_action_errors(action: str, computer_id: str, *, spec: bool = False):
    """Let sandbox failures reach the app-level handler that owns their wording.

    A refusal the client can act on is a 409 in the spec change's error shape,
    the same one an accepted change's outcome carries. ``spec`` also names the
    unsaved files of an incomplete backup; elsewhere that stays a 400 sentence.
    """
    try:
        yield
    except HTTPException:
        raise
    except ComputerRuntimeUnavailable as e:
        logger.warning("Computer runtime unavailable for %s: %s", action, e)
        raise HTTPException(
            status_code=503, detail="Computer runtime is not available"
        ) from None
    except (SandboxGoneError, SandboxTransientError) as e:
        # An owner's machine that refused is a refusal however the failure
        # was wrapped on its way here, or one start path answers 503 and
        # another 422 for the same machine.
        if connect_failure(e) is not None:
            raise sandbox_failure_http(e) from None
        raise
    except MachineBusyError as e:
        raise HTTPException(
            status_code=409, detail=spec_refusal("turn_active", str(e))
        ) from None
    except ComputerBusyError as e:
        raise HTTPException(status_code=409, detail=spec_refusal("busy", str(e))) from None
    except ComputerNameTaken as e:
        # Names the holder, so a client words it in its own language.
        raise HTTPException(
            status_code=409,
            detail={
                "code": "name_taken",
                "message": str(e),
                "name": e.name,
                "computer_id": e.computer_id,
            },
        ) from None
    except ComputerRemovalRefused as e:
        logger.warning("Refused to %s computer %s: %s", action, computer_id, e.code)
        detail = {"code": e.code, "message": str(e)}
        if e.deleted:
            detail["workspaces_deleted"] = e.deleted
        raise HTTPException(status_code=409, detail=detail) from None
    except (ProviderNotOffered, UnknownResourceTier) as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except BackupIncomplete as e:
        # The ids and causes are the operator's; the person asking gets files.
        logger.warning(f"Refused to {action}: {e}")
        if spec:
            raise HTTPException(
                status_code=409,
                detail=spec_refusal("backup_incomplete", e.user_message, e.unsaved),
            ) from None
        raise HTTPException(status_code=400, detail=e.user_message) from None
    except RuntimeError as e:
        if connect_failure(e) is not None:
            raise sandbox_failure_http(e) from None
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error {action} computer {computer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to {action} computer")


def require_computer_owner(computer: Optional[dict], *, user_id: str) -> None:
    if not computer:
        raise HTTPException(status_code=404, detail="Computer not found")
    if computer.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")


@contextlib.contextmanager
def move_refusals():
    """A move that cannot start answers in the shape its failure would be read in."""
    try:
        yield
    except MoveRefused as e:
        status = 404 if e.code == "not_found" else 409
        raise HTTPException(status_code=status, detail=move_error_detail(e.code)) from None


async def accept_workspace_move(
    user_id: str,
    workspace_id: str,
    computer_id: str,
    *,
    make_primary: bool = False,
    from_backup: bool = False,
) -> WorkspaceMoveOut:
    """Claim a move and run it after answering; its outcome is read back from the row.

    Keyed by the move: the claim already refuses a second one for the user,
    and a move whose runner dies is finished by the sweep, not rerun here.
    """
    try:
        manager = _computer_manager()
    except ComputerRuntimeUnavailable as e:
        logger.warning("Computer runtime unavailable for a move: %s", e)
        raise HTTPException(
            status_code=503, detail="Computer runtime is not available"
        ) from None
    with move_refusals():
        move = await claim_workspace_move(
            user_id=user_id,
            workspace_id=workspace_id,
            target_computer_id=computer_id,
            make_primary=make_primary,
            from_backup=from_backup,
        )
    schedule_start(
        f"workspace-move:{move['move_id']}", lambda: manager.run_accepted_move(move)
    )
    logger.info(
        "Accepted move %s of workspace %s to computer %s for user %s",
        move["move_id"],
        workspace_id,
        computer_id,
        user_id,
    )
    return move_from_row(move)  # type: ignore[return-value]


def _spec_in_progress() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail=spec_refusal("spec_in_progress", SPEC_CHANGE_IN_PROGRESS),
    )


def computer_to_response(
    computer: Dict[str, Any],
    *,
    owner: bool = True,
    workspace_count: int = 0,
    tunnels: Optional[Dict[str, Dict[str, Any]]] = None,
) -> ComputerResponse:
    """Withhold provider_ref from non-owners because it addresses a real billed machine."""
    return ComputerResponse(
        computer_id=str(computer["computer_id"]),
        user_id=computer["user_id"],
        kind=computer["kind"],
        name=computer.get("name") or "",
        status=computer["status"],
        resource_tier=computer.get("resource_tier") or "standard",
        is_always_on=bool(computer.get("is_always_on")),
        files_backup=computer.get("files_backup", True) is not False,
        is_primary=bool(computer.get("is_primary")),
        root_dir=computer.get("root_dir") or "",
        workspace_count=workspace_count,
        layout_version=observed_layout_version(computer),
        provider_ref=computer.get("provider_ref") if owner else None,
        created_at=computer["created_at"],
        updated_at=computer["updated_at"],
        last_activity_at=computer.get("last_activity_at"),
        stopped_at=computer.get("stopped_at"),
        config=computer.get("config"),
        disk=disk_from_row(computer),
        spec_change=spec_change_from_row(computer),
        ssh=ssh_info(computer, tunnels),
        start_failure=start_failure_from_row(computer),
    )


async def _owned_computer(computer_id: str, user_id: str) -> Dict[str, Any]:
    computer = await get_computer(computer_id)
    require_computer_owner(computer, user_id=user_id)
    return computer  # type: ignore[return-value]


async def _report_start_failure(computer_id: str, exc: Exception) -> None:
    """A background start behind 202 must publish failure or subscribers wait until timeout.

    Settle any surviving claim before publishing so clients re-reading on error
    see the settled row; the start's own revert normally owns this cleanup. The
    failure is on the row before the frame goes out, which carries it too: the
    start records its own as it reverts, and a claim settled here is recorded
    here.
    """
    recorded = None
    try:
        row = await get_computer(computer_id)
        if row is not None and row.get("status") == "starting":
            settled = await update_computer_status(
                computer_id, "stopped", expected="starting"
            )
            if settled is not None:
                await record_start_failure(computer_id, exc, kind=settled["kind"])
            row = await get_computer(computer_id)
        failure = start_failure_from_row(row) if row is not None else None
        if failure is not None:
            recorded = failure.model_dump(mode="json")
    except Exception:
        logger.exception(
            "Could not settle computer %s after a failed start", computer_id
        )
    extra: Dict[str, Any] = {"error": sandbox_failure_http(exc).detail}
    if recorded is not None:
        extra["start_failure"] = recorded
    await publish_computer_status_change(computer_id, "error", extra=extra)


async def _run_computer_start(
    manager: Any, computer_id: str, on_start_claimed: Callable[[], None] | None = None
) -> None:
    """Do not report shutdown cancellation as failure; the start already reverts its claim."""
    try:
        if on_start_claimed is None:
            await manager.start_computer(computer_id)
        else:
            await manager.start_computer(computer_id, on_start_claimed=on_start_claimed)
    except Exception as exc:
        await _report_start_failure(computer_id, exc)
        raise


def schedule_computer_start(
    computer_id: str, on_start_claimed: Callable[[], None] | None = None
) -> asyncio.Task:
    manager = _computer_manager()
    return schedule_start(
        f"computer:{computer_id}",
        lambda: _run_computer_start(manager, computer_id, on_start_claimed),
    )


async def _schedule_claimed_start(computer_id: str) -> str:
    """Acknowledge only after the background owner has persisted its start claim."""
    claimed = asyncio.Event()
    task = schedule_computer_start(computer_id, claimed.set)
    waiter = asyncio.create_task(claimed.wait())
    try:
        while True:
            # A deduplicated task owns another request's callback. The durable
            # row lets every waiter observe its claim, including other workers.
            done, _ = await asyncio.wait(
                (task, waiter), timeout=0.1, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                try:
                    await task
                except HTTPException:
                    raise
                except Exception as exc:
                    raise sandbox_failure_http(exc) from None
            computer = await get_computer(computer_id)
            if computer is None:
                raise ValueError(f"Computer {computer_id} not found")
            if done or computer["status"] not in CLAIMABLE_FOR_START:
                return computer["status"]
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


@router.post("", response_model=ComputerResponse, status_code=201)
async def create_computer(
    request: ComputerCreate,
    x_user_id: CurrentUserId,
):
    """Create a computer and prepare its sandbox in the background.

    Building a sandbox takes long enough that whoever first used the computer
    would wait through it, so it starts now and the client watches its events.
    Preparing holds a running slot from the insert, so the limit is asked here.
    A ``name`` another of the user's live computers has, compared trimmed and
    case-folded, is a 409 ``name_taken``; an unnamed one is numbered past them.
    """
    async with _computer_action_errors("create", "new"):
        sandbox = _sandbox_config()
        kind = request.kind or sandbox.provider
        if kind not in sandbox.creatable_providers():
            # The manager refuses it too, but an ssh create dials the machine
            # it names first, which only a kind on offer may cause.
            raise ProviderNotOffered("Provider not offered")
        if request.resource_tier not in sandbox.settings_for(kind).resource_tiers:
            # Ahead of the plan gate, which would offer an upgrade for a size
            # this provider never builds (xlarge on Daytona).
            raise UnknownResourceTier(
                f"This provider does not offer the {request.resource_tier!r} spec"
            )
        await assert_provider_allowed(x_user_id, kind, default_kind=sandbox.provider)
        plan: Optional[SSHCreatePlan] = None
        if request.ssh is not None:
            if request.name and (taken := await name_taken(x_user_id, request.name)):
                # Ahead of the probe, which dials the machine, and asked again
                # under the lock below, ahead of a take-over.
                raise taken
            with _ssh_create_refusals():
                plan = await plan_create(
                    x_user_id, request.ssh, settings=sandbox.settings_for(SSH_KIND)
                )
        if not request.files_backup and provider_class(kind).recreatable:
            raise HTTPException(
                status_code=422,
                detail=_FILES_BACKUP_REQUIRED,
            )
        # Every create and rename holds this lock, so a name still free under
        # it is free at the insert, after a take-over that cannot be undone.
        async with _computer_quota_decision(x_user_id, always=True):
            if plan is not None:
                if request.name and (taken := await name_taken(x_user_id, request.name)):
                    raise taken
                with _ssh_create_refusals():
                    await plan.recheck(x_user_id)
            if provider_class(kind).metered:
                # The cap first: a user at it who asked for a size their plan
                # refuses would step down only to be refused again.
                await enforce_computer_count(x_user_id)
                await assert_spec_allowed(x_user_id, request.resource_tier)
                await enforce_computer_limit(x_user_id)
            computer = await _computer_manager().create_computer_for_user(
                x_user_id,
                kind=kind,
                name=request.name,
                resource_tier=request.resource_tier,
                # The insert squashes this to FALSE when a live primary already
                # exists, so it reads as "primary if they have none". Without it a
                # user's first, named machine is not primary and their first project
                # mints a second one. The manager declines it for a machine it
                # cannot rebuild, such as an owner's SSH machine.
                is_primary=True,
                prepare=True,
                files_backup=request.files_backup,
                **(plan.create_kwargs() if plan is not None else {}),
            )
        computer_id = str(computer["computer_id"])
        status = await _schedule_claimed_start(computer_id)
        logger.info(
            "Created computer %s for user %s, preparing it (%s)",
            computer_id, x_user_id, status,
        )
        return computer_to_response(
            {**computer, "status": status},
            tunnels=await tunnels_for(x_user_id, [computer]),
        )


@contextlib.contextmanager
def _ssh_create_refusals():
    """An ssh create refused before its row exists, in a sentence the client shows as is.

    A machine that did not answer is a 422 too, unlike a start's 503: there is
    no computer yet to try again, only the dialog that named the machine.
    """
    try:
        yield
    except SSHComputerLimit as e:
        raise HTTPException(status_code=409, detail=e.detail()) from None
    except (SSHKeyMissing, SSHFolderTaken) as e:
        raise HTTPException(status_code=409, detail=str(e)) from None
    except SSHDialBudgetExceeded as e:
        raise HTTPException(
            status_code=429,
            detail=str(e),
            headers={"Retry-After": str(e.retry_after)},
        ) from None
    except SSHFolderMarked as e:
        raise HTTPException(status_code=409, detail=e.detail()) from None
    except SSHConnectError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    except SSHUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


@router.get("", response_model=ComputerListResponse)
async def list_computers(x_user_id: CurrentUserId):
    rows = await get_computers_for_user(x_user_id)
    # One query for the whole list: stopping a machine takes every project on it
    # down, so the count is what the card needs to say so before asking.
    counts = await count_live_workspaces_by_computer(
        [str(row["computer_id"]) for row in rows]
    )
    tunnels = await tunnels_for(x_user_id, rows)
    return ComputerListResponse(
        computers=[
            computer_to_response(
                row,
                workspace_count=counts.get(str(row["computer_id"]), 0),
                tunnels=tunnels,
            )
            for row in rows
        ],
        total=len(rows),
    )


# Registered ahead of /{computer_id}, which would otherwise read "providers" as an id.
@router.get("/providers", response_model=ComputerProvidersResponse)
async def list_computer_providers(x_user_id: CurrentUserId):
    """The providers a new computer may be created on, and which the caller may pick."""
    try:
        sandbox = _sandbox_config()
    except ComputerRuntimeUnavailable as e:
        logger.warning("Computer runtime unavailable for providers: %s", e)
        raise HTTPException(
            status_code=503, detail="Computer runtime is not available"
        ) from None
    kinds = sandbox.creatable_providers()
    allowed = await get_provider_allowances(
        x_user_id, kinds, default_kind=sandbox.provider
    )
    return ComputerProvidersResponse(
        providers=[
            ComputerProviderOption(
                kind=kind,
                is_default=kind == sandbox.provider,
                allowed=allowed[kind],
                tiers=_tier_specs(sandbox, kind),
                default_name=provider_class(kind).default_computer_name,
            )
            for kind in kinds
        ],
        tunnels=tunnels_enabled(),
    )


def _tier_specs(sandbox: Any, kind: str) -> list[ComputerTierSpec]:
    """The sizes ``kind`` offers, from the settings its sandboxes are built from.

    Empty for a machine its owner sizes, whose one tier is a placeholder.
    """
    if not provider_class(kind).recreatable:
        return []
    tiers = sandbox.settings_for(kind).resource_tiers
    return [
        ComputerTierSpec(
            tier=name, cpu=preset.cpu, memory_gib=preset.memory, disk_gib=preset.disk
        )
        for name, preset in sorted(
            tiers.items(), key=lambda item: (item[1].cpu, item[1].memory, item[1].disk)
        )
    ]


async def _counted_response(computer: Dict[str, Any]) -> ComputerResponse:
    """A single machine's response with its live project count.

    Every route that answers with a whole machine carries the count, including
    the ones that just changed it: a response is what the client replaces its
    row with, so one answering zero erases the number the card warns with.
    Whether its tunnel is up rides along for the same reason.
    """
    computer_id = str(computer["computer_id"])
    counts = await count_live_workspaces_by_computer([computer_id])
    return computer_to_response(
        computer,
        workspace_count=counts.get(computer_id, 0),
        tunnels=await tunnels_for(computer["user_id"], [computer]),
    )


@router.get("/{computer_id}", response_model=ComputerResponse)
async def get_computer_details(computer_id: str, x_user_id: CurrentUserId):
    return await _counted_response(await _owned_computer(computer_id, x_user_id))


@router.patch("/{computer_id}", response_model=ComputerResponse)
async def rename_computer(
    computer_id: str,
    request: ComputerRenameRequest,
    x_user_id: CurrentUserId,
):
    """A name another of the user's live computers has is a 409 ``name_taken``;
    the computer's own name in another case is a rename like any other."""
    async with _computer_action_errors("rename", computer_id):
        await _owned_computer(computer_id, x_user_id)
        # Under the lock a create holds from its last name check to its insert.
        async with computer_capacity_lock(x_user_id):
            updated = await db_rename_computer(
                computer_id, request.name, user_id=x_user_id
            )
        if updated is None:
            raise HTTPException(status_code=404, detail="Computer not found")
    return await _counted_response(updated)


@router.delete("/{computer_id}", response_model=ComputerRemoveResponse)
async def remove_computer(
    computer_id: str,
    x_user_id: CurrentUserId,
    remove_key: bool = Query(
        True,
        description=(
            "On an ssh computer, take the key's line off its machine too, "
            "unless another of the user's computers logs in through the same file"
        ),
    ),
    delete_workspaces: list[str] = Query(
        default=[],
        description=(
            "As the body's delete_workspaces, which this joins; kept for clients "
            "that name them here"
        ),
    ),
    delete_files: bool = Query(
        False,
        description=(
            "On an ssh computer, also delete LangAlpha's files from its machine: "
            "the folders of the workspaces deleted with it, its folders in the root, "
            "and the runtime in ~/.langalpha when no other computer uses it"
        ),
    ),
    body: Optional[ComputerRemoveRequest] = None,
):
    """Remove a computer and its sandbox for good.

    Any but the primary, which is where new workspaces go. One with workspaces
    is refused unless ``delete_workspaces`` names every one of them, the ones
    the owner was shown, so a removal never takes a workspace they did not
    see (one created meanwhile, in another tab). The body carries them, as
    the query would overflow a request line limit on a full computer; the
    query an older client names them in still counts. ``workspaces_deleted``
    answers the ones that went, for the client to forget. Their folders, and
    on an ssh computer anything else LangAlpha put on the machine, stay unless
    ``delete_files`` says to delete them too. A refusal the
    user can act on is a 409 whose ``code`` the client words. On an ssh computer,
    ``key`` says what became of the key's line on its machine. The tunnel of
    a machine left with no computer goes on the connection the delete ran
    on, or without one in the background, where a machine that does not
    answer holds up nobody.
    """
    named = list(dict.fromkeys([*delete_workspaces, *(body.delete_workspaces if body else ())]))
    async with _computer_action_errors("remove", computer_id):
        computer = await _owned_computer(computer_id, x_user_id)
        deleted = await _computer_manager().remove_computer_with_workspaces(
            computer_id, named, delete_files=delete_files
        )
        logger.info("Removed computer %s for user %s", computer_id, x_user_id)
        if computer.get("kind") != SSH_KIND:
            return ComputerRemoveResponse(workspaces_deleted=deleted)
        settings = _sandbox_config().settings_for(SSH_KIND)
        left = await leave_machine(
            x_user_id,
            computer,
            settings=settings,
            remove_key=remove_key,
            delete_files=delete_files,
        )
        if tunnel_of(computer) and not left.tunnel_released:
            schedule_start(
                f"tunnel-release:{computer_id}",
                lambda: release_tunnel(x_user_id, computer, settings=settings),
            )
    return ComputerRemoveResponse(
        key=left.key, files=left.files, workspaces_deleted=deleted
    )


@router.get("/{computer_id}/storage", response_model=ComputerStorageResponse)
async def get_computer_storage(computer_id: str, x_user_id: CurrentUserId):
    """Measure now when the machine runs; otherwise answer the last reading.

    A stopped machine is never started for this: the stored reading is what
    the warning already shows, and a folder breakdown is not worth a boot. A
    breakdown taken in the last minute is answered again: the panel asks on
    every open, and each measurement is a du over the whole root.
    """
    computer = await _owned_computer(computer_id, x_user_id)
    try:
        manager = _computer_manager()
    except ComputerRuntimeUnavailable:
        return ComputerStorageResponse(disk=disk_from_row(computer), live=False)
    if not manager.measures_disk(computer):
        return ComputerStorageResponse(disk=None, live=False)

    computer, sizes = await manager.refresh_computer_disk(
        computer,
        breakdown=True,
        breakdown_max_age_s=STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
    )
    if sizes is None:
        return ComputerStorageResponse(disk=disk_from_row(computer), live=False)
    folders = await get_live_workspace_folders_for_computer(computer_id)
    workspaces, other = storage_breakdown(computer, folders, sizes)
    return ComputerStorageResponse(
        disk=disk_from_row(computer),
        workspaces=workspaces,
        other_bytes=other,
        live=True,
    )


@router.get("/{computer_id}/session", response_model=ComputerSessionResponse)
async def get_computer_session(computer_id: str, x_user_id: CurrentUserId):
    """Resolve sessions only when Postgres says running so this read cannot provision."""
    computer = await _owned_computer(computer_id, x_user_id)
    response = ComputerSessionResponse(
        computer_id=str(computer["computer_id"]),
        status=computer["status"],
        provider_ref=computer.get("provider_ref"),
    )
    if computer["status"] != "running":
        return response

    try:
        session = _computer_manager().cached_session_for_computer(computer)
    except ComputerRuntimeUnavailable:
        return response

    sandbox = getattr(session, "sandbox", None) if session is not None else None
    if sandbox is None:
        return response
    ref = getattr(sandbox, "sandbox_id", None)
    return response.model_copy(
        update={
            "ready": getattr(sandbox, "runtime", None) is not None,
            "session_provider_ref": ref if isinstance(ref, str) and ref else None,
        }
    )


@router.get("/{computer_id}/events")
async def computer_status_events(computer_id: str, x_user_id: CurrentUserId):
    computer = await _owned_computer(computer_id, x_user_id)

    def _frame(
        status: str,
        sandbox_state: str | None = None,
        error: str | None = None,
        stage: str | None = None,
        start_failure: dict | None = None,
    ) -> str:
        return sse_status_event(
            {"computer_id": computer_id},
            status,
            sandbox_state,
            error,
            stage,
            start_failure,
        )

    async def _read_status() -> str | None:
        row = await get_computer(computer_id)
        return row["status"] if row else None

    return StreamingResponse(
        status_event_stream(
            initial_status=computer["status"],
            read_status=_read_status,
            subscribe=lambda: subscribe_to_computer_status(computer_id),
            frame=_frame,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.post("/{computer_id}/start", response_model=ComputerActionResponse)
async def start_computer(
    computer_id: str,
    x_user_id: CurrentUserId,
    lazy: bool = Query(
        False,
        description=(
            "If true, schedule the start in the background and return 202 "
            "immediately with status='starting'. If false (default), block "
            "until the computer is up and return status='running'."
        ),
    ),
):
    async with _computer_action_errors("start", computer_id):
        manager = _computer_manager()
        computer = await _owned_computer(computer_id, x_user_id)
        if computer["status"] == "stopping":
            await manager.reconcile_stopping_computer(computer_id)
            computer = await _owned_computer(computer_id, x_user_id)
        status = computer["status"]

        if status == "running":
            return ComputerActionResponse(
                computer_id=computer_id,
                status=status,
                message=f"Computer is already {status}",
            )
        if status == "starting" and lazy:
            payload = ComputerActionResponse(
                computer_id=computer_id,
                status=status,
                message="Computer start already in progress",
            )
            return Response(
                status_code=202,
                content=payload.model_dump_json(),
                media_type="application/json",
            )
        if status not in CLAIMABLE_FOR_START and status != "starting":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot start computer in '{status}' state",
            )

        if lazy:
            status = await _schedule_claimed_start(computer_id)
            payload = ComputerActionResponse(
                computer_id=computer_id,
                status=status,
                message="Computer start initiated",
            )
            return Response(
                status_code=202,
                content=payload.model_dump_json(),
                media_type="application/json",
            )

        updated = await manager.start_computer(computer_id)
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "running",
            message="Computer started successfully",
        )


@router.post("/{computer_id}/stop", response_model=ComputerActionResponse)
async def stop_computer(computer_id: str, x_user_id: CurrentUserId):
    async with _computer_action_errors("stop", computer_id):
        await _owned_computer(computer_id, x_user_id)
        updated = await _computer_manager().stop_computer(computer_id)
        logger.info(f"Stopped computer {computer_id}")
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "stopped",
            message="Computer stopped successfully",
        )


@router.post("/{computer_id}/archive", response_model=ComputerActionResponse)
async def archive_computer(computer_id: str, x_user_id: CurrentUserId):
    async with _computer_action_errors("archive", computer_id):
        await _owned_computer(computer_id, x_user_id)
        updated = await _computer_manager().archive_computer(computer_id)
        logger.info(f"Archived computer {computer_id}")
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "stopped",
            message="Computer archived successfully",
        )


@router.post(
    "/{computer_id}/spec", response_model=ComputerResponse, status_code=202
)
async def set_computer_spec(
    computer_id: str,
    request: ComputerSpecRequest,
    response: Response,
    x_user_id: CurrentUserId,
):
    """Accept a spec change and run it after answering; the outcome lands on ``spec_change``.

    A recreate takes minutes, longer than an edge proxy holds a request open,
    so the refusals a client can act on are answered here (403 off-plan, 429
    over quota, 409 ``turn_active``, ``spec_in_progress`` or ``busy``) and
    everything after is read back from the row. The current tier skips the
    count check so retries at quota remain valid, and answers 200 unchanged.
    """
    async with _computer_action_errors("set spec for", computer_id, spec=True):
        computer = await _owned_computer(computer_id, x_user_id)
        running = spec_change_from_row(computer)
        if running is not None and running.state == "in_progress":
            # Ahead of the same-tier shortcut: a running change persists its
            # target tier early, so a repeat would otherwise read as done.
            raise _spec_in_progress()
        manager = _computer_manager()
        if await manager.precheck_computer_spec(computer, request.tier):
            response.status_code = 200
            return await _counted_response(computer)
        if platform_gating_active():
            await assert_spec_allowed(
                x_user_id,
                request.tier,
                current_tier=computer.get("resource_tier") or "standard",
            )
        claimed = await manager.claim_computer_spec(computer_id, request.tier)
        if claimed is None:
            if await get_computer(computer_id) is None:
                raise HTTPException(status_code=404, detail="Computer not found")
            raise _spec_in_progress()
        claim_id = claimed["spec_change"]["claim_id"]
        # Keyed by the claim, not the machine: the row already refuses a second
        # fresh change, and a stale takeover must get a runner of its own
        # rather than be deduplicated onto the one it replaced.
        schedule_start(
            f"computer-spec:{computer_id}:{claim_id}",
            lambda: manager.run_accepted_spec_change(
                computer_id, request.tier, user_id=x_user_id, claim_id=claim_id
            ),
        )
        logger.info(
            "Accepted spec change of computer %s to %r for user %s",
            computer_id,
            request.tier,
            x_user_id,
        )
        return await _counted_response(claimed)


@router.post("/{computer_id}/make-primary", response_model=MakePrimaryResponse)
async def make_primary_computer(
    computer_id: str,
    response: Response,
    x_user_id: CurrentUserId,
):
    """Make this the computer new workspaces and Home land on.

    Home lives on the primary. With Home on no computer yet, or already on
    this one, the flip lands now (200). Otherwise Home's files move first,
    which outlasts a request: the move is accepted (202), the flip lands
    when it commits, and both are read back from Home's move.
    """
    async with _computer_action_errors("make primary", computer_id):
        await _owned_computer(computer_id, x_user_id)
        with move_refusals():
            flipped = await set_primary_computer(x_user_id, computer_id)
        if flipped:
            logger.info("Made computer %s primary for user %s", computer_id, x_user_id)
            return MakePrimaryResponse(computer_id=computer_id, is_primary=True)
        move = await accept_workspace_move(
            x_user_id, get_home_workspace_id(x_user_id), computer_id, make_primary=True
        )
        response.status_code = 202
        return MakePrimaryResponse(computer_id=computer_id, is_primary=False, move=move)


@router.post("/{computer_id}/files-backup", response_model=ComputerResponse)
async def set_computer_files_backup_route(
    computer_id: str,
    request: ComputerFilesBackupRequest,
    x_user_id: CurrentUserId,
):
    """Turn routine backups of a machine owner's workspaces on or off.

    Off keeps the last backup as it is: a project's backup already under way
    finishes, and none starts once this answers, since each reads the switch
    under its project's sync lock. On, the next turn's backup brings it up to
    date. Nothing on the machine changes, so it answers at once.
    """
    async with _computer_action_errors("set file backup for", computer_id):
        computer = await _owned_computer(computer_id, x_user_id)
        if not request.enabled and provider_class(computer["kind"]).recreatable:
            raise HTTPException(status_code=409, detail=_FILES_BACKUP_REQUIRED)
        updated = await set_computer_files_backup(computer_id, request.enabled)
        if updated is None:
            raise HTTPException(status_code=404, detail="Computer not found")
        logger.info(
            f"Set computer {computer_id} file backup to {request.enabled} "
            f"for user {x_user_id}"
        )
        return await _counted_response(updated)


@router.post("/{computer_id}/always-on", response_model=ComputerResponse)
async def set_computer_always_on(
    computer_id: str,
    request: ComputerAlwaysOnRequest,
    x_user_id: CurrentUserId,
):
    """Disabling and idempotent enabling consume no slot and must not fail at quota.

    Start stopped machines in the background because always-on takes effect immediately.
    """
    async with _computer_action_errors("set always-on for", computer_id):
        await _owned_computer(computer_id, x_user_id)
        updated = await _computer_manager().set_computer_always_on(
            computer_id, request.enabled, user_id=x_user_id
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Computer not found")

        if request.enabled and updated.get("status") in CLAIMABLE_FOR_START:
            status = await _schedule_claimed_start(computer_id)
            logger.info(
                f"Always-on enabled: scheduled start for computer {computer_id}"
            )
            updated = {**updated, "status": status}

        logger.info(
            f"Set computer {computer_id} always-on to {request.enabled} "
            f"for user {x_user_id}"
        )
        return await _counted_response(updated)


@contextlib.contextmanager
def _repair_refusals():
    """A reinstall or reconnect refused before it changed anything, as a detail
    with the ``code`` the client words its copy from.

    A machine that answered and refused is a 422, one that did not a 503, as
    a start answers them; a folder another computer holds is a 409.
    """
    try:
        yield
    except ComputerRepairRefused as e:
        raise HTTPException(status_code=e.status, detail=e.detail()) from None
    except SSHDialBudgetExceeded as e:
        raise HTTPException(
            status_code=429,
            detail={"code": e.code, "message": str(e), "retry_after": e.retry_after},
            headers={"Retry-After": str(e.retry_after)},
        ) from None
    except SSHKeyMissing as e:
        raise HTTPException(
            status_code=409, detail={"code": "key_missing", "message": str(e)}
        ) from None
    except SSHUnavailable as e:
        raise HTTPException(
            status_code=503, detail={"code": "ssh_unavailable", "message": str(e)}
        ) from None
    except SSHFolderMarked as e:
        raise HTTPException(status_code=409, detail=e.detail()) from None
    except SSHConnectError as e:
        if e.answered:
            raise HTTPException(
                status_code=422, detail={"code": e.code, "message": str(e)}
            ) from None
        raise HTTPException(
            status_code=503, detail={"code": e.code, "message": e.for_owner}
        ) from None


async def _accepted_repair_start(computer_id: str, message: str) -> ComputerActionResponse:
    """Start the repaired computer as a lazy start does, answering once its
    claim is on the row."""
    status = await _schedule_claimed_start(computer_id)
    return ComputerActionResponse(computer_id=computer_id, status=status, message=message)


@router.post(
    "/{computer_id}/reinstall", response_model=ComputerActionResponse, status_code=202
)
async def reinstall_computer(
    computer_id: str,
    request: ComputerReinstallRequest,
    x_user_id: CurrentUserId,
):
    """Give a stopped ssh computer a folder it may have, then start it.

    For a start refused because the computer's folder is another computer's
    (``start_failure.remedy`` ``reinstall``). ``take_back`` clears that other
    computer, which ``take_over`` names as ``start_failure.owner`` did, from
    the machine, as its removal would have, under this computer's own login; a
    mark of any other computer refuses with 409 ``folder_marked`` and its
    ``owner``, one of the caller's own live computers is never cleared (409
    ``folder_owned``), and a folder no other computer marks just starts.
    ``new`` moves this computer to ``root_dir``, which must be missing or empty
    (422 ``root_dir``) and no other computer's (409 ``folder_marked`` with its
    ``owner``, unless ``take_over`` names it, or ``folder_owned``); each
    workspace's files are then restored there from its backup. Refused with 400
    ``not_ssh``, 409 ``not_stopped``, ``spec_in_progress`` or ``busy``, and 503
    when the machine does not answer. Accepted, the start is scheduled as a
    lazy start is and its outcome lands on the row.
    """
    async with _computer_action_errors("reinstall", computer_id):
        await _owned_computer(computer_id, x_user_id)
        settings = _sandbox_config().settings_for(SSH_KIND)
        with _repair_refusals():
            await _computer_manager().reinstall_computer(
                computer_id,
                folder=request.folder,
                root_dir=request.root_dir,
                settings=settings,
                take_over=request.take_over,
            )
        logger.info(
            "Reinstalled computer %s (%s) for user %s",
            computer_id,
            request.folder,
            x_user_id,
        )
        return await _accepted_repair_start(computer_id, "Computer reinstalled; starting")


@router.post(
    "/{computer_id}/reconnect", response_model=ComputerActionResponse, status_code=202
)
async def reconnect_computer(
    computer_id: str,
    request: ComputerReconnectRequest,
    x_user_id: CurrentUserId,
):
    """Point a stopped ssh computer at its machine as a pairing reached it again, then start it.

    For a start refused because the machine no longer answers as it was paired
    (``start_failure.remedy`` ``reconnect``). ``ssh`` is the ready pairing's
    target and its probe's host key; a pairing bound to this computer
    (``POST /ssh/pairings`` with its ``computer_id``) suggests its folder. The
    folder is checked as this computer's: its own mark passes, another computer's
    refuses with 409 ``folder_marked`` and its ``owner``, which
    ``ssh.take_over`` naming takes the folder from once the new target is
    saved. A machine that is not the computer's (another host key, and neither
    the computer's own folder in the login's home nor its mark) is refused with
    409 ``different_machine`` unless ``ssh.move`` says the owner chose to move
    it there, where each backed-up workspace is restored. A tunnel the computer
    no longer names is taken off its machine afterwards. Refusals as for a
    reinstall, 422 for a machine that refused, and 503 for one that did not
    answer or a tunnel that is not the caller's.
    """
    async with _computer_action_errors("reconnect", computer_id):
        await _owned_computer(computer_id, x_user_id)
        settings = _sandbox_config().settings_for(SSH_KIND)
        with _repair_refusals():
            updated, before = await _computer_manager().reconnect_computer(
                computer_id, request.ssh, settings=settings
            )
        left = tunnel_of(before)
        if left and left != tunnel_of(updated):
            schedule_start(
                f"tunnel-release:{computer_id}:{left}",
                lambda: release_tunnel(x_user_id, before, settings=settings),
            )
        logger.info("Reconnected computer %s for user %s", computer_id, x_user_id)
        return await _accepted_repair_start(computer_id, "Computer reconnected; starting")
