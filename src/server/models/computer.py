"""Request and response models for the Computer management API.

A computer is one isolated execution environment addressed on its own, so the
lifecycle fields a workspace used to carry (status, resource tier, always-on)
are read and written here. ``provider_ref`` is the vendor's id for the machine
and is only ever handed to the owner.
"""

import ipaddress
import posixpath
import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ptc_agent.core.sandbox.providers.ssh_errors import MACHINE_ID_PATTERN

from src.server.services.persistence.sync_result import UnsavedReason


class ComputerStatus(StrEnum):
    """Computer lifecycle states: the workspace set minus ``unbound``.

    StrEnum, not ``(str, Enum)``: a member of the latter renders as
    ``ComputerStatus.RUNNING`` in an f-string while comparing equal to
    ``"running"``, so every log line and message built from one had to route
    around it. Here a member is its value everywhere a string is wanted.
    """

    CREATING = "creating"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    DELETED = "deleted"


# A machine is claimable for start only from a settled down state. ``creating``
# is one: 046 copies a workspace's status onto the machine it backfills, so a
# create that bound a sandbox and then crashed leaves a startable machine
# sitting at ``creating``.
CLAIMABLE_FOR_START = (ComputerStatus.STOPPED, ComputerStatus.CREATING)


ComputerKind = Literal["daytona", "docker", "e2b", "ssh"]

# Spec presets a request may name. Shown as Small, Medium, Large and Extra
# Large; the ids predate those names and are persisted on computer rows. Each
# provider offers its own subset (xlarge is E2B's alone).
ResourceTierId = Literal["standard", "performance", "max", "xlarge"]


_HOSTNAME = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?$"
)
# A login name as useradd accepts it, or as SSSD gives a directory account,
# ``name@domain``, which is what ``id -un`` reports on such a machine. Either
# way it cannot be read as an ssh option or a second argument.
_USERNAME = re.compile(
    r"^(?=.{1,128}$)[A-Za-z_][A-Za-z0-9_.-]*(?:@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)?$"
)
# A tunnel's id, as ``ssh_tunnels`` mints it.
_TUNNEL_ID = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
# "<type> <base64 blob> [comment]", the form ssh-keyscan and known_hosts use.
_HOST_KEY = re.compile(
    r"^((?:ssh|ecdsa|sk)-[A-Za-z0-9@.-]+) ([A-Za-z0-9+/]+={0,2})(?: .*)?$"
)


def is_login_name(value: str) -> bool:
    """Whether *value* is an account name SSH may be told to log in as."""
    return _USERNAME.fullmatch(value) is not None


class ComputerSSHAddress(BaseModel):
    """Where a machine answers SSH."""

    model_config = ConfigDict(str_strip_whitespace=True)

    host: str = Field(
        min_length=1,
        max_length=255,
        description="Hostname or IP address, without a scheme, user or port",
    )
    port: int = Field(22, ge=1, le=65535, description="SSH port")

    @field_validator("host")
    @classmethod
    def _check_host(cls, value: str) -> str:
        bare = value[1:-1] if value.startswith("[") and value.endswith("]") else value
        try:
            return str(ipaddress.ip_address(bare))
        except ValueError:
            pass
        if not _HOSTNAME.match(bare):
            raise ValueError(
                "Enter a hostname or IP address only, without a scheme, user or port"
            )
        return bare.rstrip(".").lower()


class ComputerSSHTarget(ComputerSSHAddress):
    """A machine to reach over SSH: an address and the account to log in as."""

    username: str = Field(min_length=1, max_length=128, description="Login name")

    @field_validator("username")
    @classmethod
    def _check_username(cls, value: str) -> str:
        if not is_login_name(value):
            raise ValueError("Enter the login name of an account on the machine")
        return value


def normal_root_dir(value: str) -> str:
    """*value* as the absolute folder a computer works in, or ValueError.

    normpath keeps a leading "//", which would make one folder two to the
    check that keeps computers out of each other's folders.
    """
    if not value.startswith("/") or any(ord(c) < 32 for c in value):
        raise ValueError("root_dir must be an absolute path")
    normal = posixpath.normpath("/" + value.lstrip("/"))
    if normal == "/" or ".." in normal.split("/"):
        raise ValueError("root_dir must be a folder below /")
    return normal


# computers.root_dir is VARCHAR(255).
ROOT_DIR_MAX = 255


class ComputerSSHPinnedTarget(ComputerSSHTarget):
    """A machine to log in to with the host key its owner confirmed."""

    host_key: str = Field(
        min_length=1,
        max_length=16384,
        description=(
            "The machine's public host key as an OpenSSH line, the one the "
            "probe returned and the user confirmed. Any other key is refused."
        ),
    )
    tunnel_id: Optional[str] = Field(
        None,
        pattern=_TUNNEL_ID,
        description=(
            "The caller's tunnel the pairing reached the machine through. The "
            "machine is then reached through the gateway: host is only its "
            "name, and port its SSH port on the machine itself"
        ),
    )

    @field_validator("host_key")
    @classmethod
    def _check_host_key(cls, value: str) -> str:
        match = _HOST_KEY.match(value)
        if match is None:
            raise ValueError("host_key is not an OpenSSH public key line")
        # The comment carries nothing the pin needs.
        return f"{match.group(1)} {match.group(2)}"


class ComputerSSHSpec(ComputerSSHPinnedTarget):
    """The SSH machine a new computer runs on, as the probe confirmed it."""

    root_dir: Optional[str] = Field(
        None,
        max_length=1024,
        description=(
            "Absolute folder the computer works in on the machine; when "
            "absent, ~/langalpha in the login's home, or the next free "
            "~/langalpha-N when another of the caller's computers uses it"
        ),
    )
    take_over: Optional[str] = Field(
        None,
        pattern=MACHINE_ID_PATTERN,
        description=(
            "The owner a 409 folder_marked named: that computer, which the "
            "caller does not have, is cleared from the machine as its removal "
            "would have, and root_dir taken from it. Its files stay"
        ),
    )

    @field_validator("root_dir")
    @classmethod
    def _check_root_dir(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        return normal_root_dir(value)


class ComputerReinstallRequest(BaseModel):
    """How a reinstall gives the computer a folder it may have."""

    model_config = ConfigDict(str_strip_whitespace=True)

    folder: Literal["take_back", "new"] = Field(
        description=(
            "take_back clears the computer whose mark is on this computer's "
            "folder from the machine, as its removal would have, and keeps "
            "the folder; new moves this computer to root_dir"
        )
    )
    root_dir: Optional[str] = Field(
        None,
        max_length=ROOT_DIR_MAX,
        description=(
            "With new: the folder to move to, absolute or under ~ (the login's "
            "home). It must be missing or empty"
        ),
    )
    take_over: Optional[str] = Field(
        None,
        pattern=MACHINE_ID_PATTERN,
        description=(
            "The computer the caller chose to take the folder from: with "
            "take_back, the start_failure's owner; with new, the owner a 409 "
            "folder_marked named for root_dir. That computer, which the caller "
            "does not have, is cleared from the machine as its removal would "
            "have. A folder marked by any other refuses with folder_marked"
        ),
    )

    @model_validator(mode="after")
    def _root_dir_with_new(self) -> "ComputerReinstallRequest":
        if self.folder == "take_back":
            self.root_dir = None
            return self
        value = self.root_dir or ""
        if value == "~" or value.startswith("~/"):
            if any(ord(c) < 32 for c in value):
                raise ValueError("root_dir must be an absolute path")
            return self
        if not value:
            raise ValueError("folder new needs root_dir")
        self.root_dir = normal_root_dir(value)
        return self


class ComputerSSHReconnectTarget(ComputerSSHPinnedTarget):
    """The machine as a pairing reached it again."""

    take_over: Optional[str] = Field(
        None,
        pattern=MACHINE_ID_PATTERN,
        description=(
            "The owner a 409 folder_marked named for this computer's folder. "
            "The reconnect is saved and that computer, which the caller does "
            "not have, is then cleared from the machine through the new target"
        ),
    )
    move: bool = Field(
        False,
        description=(
            "After a 409 different_machine: the caller chose to move the "
            "computer to this machine, which holds nothing of it. Each "
            "backed-up workspace's files are restored there"
        ),
    )


class ComputerReconnectRequest(BaseModel):
    """The machine as a pairing reached it again."""

    ssh: ComputerSSHReconnectTarget = Field(
        description=(
            "The ready pairing's target (host, port, username, tunnel_id) "
            "with its probe's host_key"
        )
    )


class ComputerSSHProbeRequest(ComputerSSHTarget):
    """A machine to probe, and the computer it is probed again for."""

    computer_id: Optional[str] = Field(
        None,
        max_length=64,
        description=(
            "An ssh computer of the caller's whose machine is being reached "
            "again by hand; the probe then skips the computer limit"
        ),
    )


class ComputerSSHPairingCreate(BaseModel):
    """What a pairing is for: a new machine, or one of the caller's computers."""

    computer_id: Optional[str] = Field(
        None,
        max_length=64,
        description=(
            "An ssh computer of the caller's whose machine is being paired "
            "again; the pairing then skips the computer limit and suggests "
            "that computer's folder"
        ),
    )


class ComputerCreate(BaseModel):
    """Request model for creating a computer."""

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=255,
        description=(
            "User-facing computer name, unique among the user's computers; "
            "a default is used when absent or blank"
        ),
    )
    resource_tier: ResourceTierId = Field(
        "standard",
        description="Spec preset the computer is created at",
    )
    kind: Optional[ComputerKind] = Field(
        None,
        description=(
            "Sandbox provider the computer runs on, for good; the deployment "
            "default when absent. Must be one GET /computers/providers lists."
        ),
    )
    ssh: Optional[ComputerSSHSpec] = Field(
        None,
        description="The machine to use; required with kind ssh, refused otherwise",
    )
    files_backup: bool = Field(
        True,
        description=(
            "Whether routine backups of its workspaces' files run. Only a "
            "machine the server never rebuilds (kind ssh) may be created "
            "without them"
        ),
    )

    @field_validator("name", mode="before")
    @classmethod
    def _trimmed_name(cls, value: Any) -> Any:
        # Compared trimmed against the user's other names, so stored that way.
        if isinstance(value, str):
            return value.strip() or None
        return value

    @model_validator(mode="after")
    def _ssh_only_with_its_kind(self) -> "ComputerCreate":
        if self.kind == "ssh" and self.ssh is None:
            raise ValueError("kind ssh needs ssh: host, port, username and host_key")
        if self.kind != "ssh" and self.ssh is not None:
            raise ValueError("ssh is only accepted with kind ssh")
        if self.ssh is not None:
            # The machine's size is its own, so the one tier is the only one.
            self.resource_tier = "standard"
        return self


class ComputerSSHKeyResponse(BaseModel):
    """The caller's SSH client key, which every machine they add must trust."""

    public_key: str = Field(description="OpenSSH public key line")
    fingerprint: str = Field(description="SHA256 fingerprint of the public key")
    authorized_keys_command: str = Field(
        description=(
            "A shell command that adds the key to the login account's "
            "authorized_keys, run once on the machine"
        )
    )


class ComputerSSHProbeResponse(BaseModel):
    """What a probe found on the machine, for the user to confirm before adding it."""

    host_key: str = Field(
        description="The machine's host key as an OpenSSH line, pinned on create"
    )
    fingerprint: str = Field(description="SHA256 fingerprint of the host key")
    os: str
    arch: str
    distro: str
    home: str
    shell: str
    is_root: bool
    has_sudo: bool = Field(
        description="Whether the account may sudo without a password"
    )
    disk_free_bytes: Optional[int] = None
    model: str = Field("", description="A Mac's model name; empty elsewhere")
    cpu: str = Field("", description="The processor as the system names it; empty when not read")
    cores: Optional[int] = None
    memory_bytes: Optional[int] = None
    supported: bool = Field(description="Whether a computer can run on the machine")
    problems: List[str] = Field(
        default_factory=list,
        description="Why it cannot, or what will not work, one sentence each",
    )
    hostname: str = Field("", description="The machine's own name for itself")
    helper: bool = Field(
        False,
        description="LangAlpha's helper answers for the machine rather than an sshd",
    )
    suggested_root_dir: str = Field(
        description=(
            "The folder a computer created without root_dir would work in: "
            "~/langalpha in the login's home, or the next free ~/langalpha-N "
            "when another of the caller's computers already uses it"
        ),
    )


class ComputerSSHPairingResponse(BaseModel):
    """A pairing: the command that adds a machine by being run on it."""

    token: str = Field(
        description="Names the pairing in the routes below; also in the command"
    )
    command: str = Field(
        description="One line to run on the machine as the account the agent will use"
    )
    script_url: str = Field(
        description="The script the command runs, for reading first"
    )
    expires_at: datetime
    local_only: bool = Field(
        False,
        description=(
            "The command reaches this server only from the machine running it, "
            "so it can add that machine and no other"
        ),
    )


class ComputerSSHPairingMachine(BaseModel):
    """What the machine said about itself when the command ran."""

    username: str
    hostname: str
    os: str = Field("", description="uname -s: Linux or Darwin")
    addresses: List[str] = Field(
        description="Where it may be reached, the one it reported from first"
    )


class ComputerSSHPairingTarget(BaseModel):
    """The address and account that logged in."""

    host: str
    port: int
    username: str
    tunnel_id: Optional[str] = Field(
        None,
        description=(
            "Set when the machine was reached through its tunnel; a create "
            "passes it on, and host is then only the machine's name"
        ),
    )


class ComputerSSHPairingError(BaseModel):
    code: str = Field(
        description=(
            "unreachable, timeout, auth_failed, host_key_mismatch (something "
            "else answered there), private_address, not_connected (the "
            "machine's tunnel did not come up), gateway_unreachable (the "
            "tunnel gateway did not answer), no_host_key (the machine named "
            "no SSH host keys, so it was not dialed) or script_failed (the "
            "command stopped on the machine; the message is what it printed)"
        )
    )
    message: str = Field(description="A sentence the client may show as is")


class ComputerSSHPairingStatus(BaseModel):
    """Where a pairing stands, for the dialog that polls it."""

    status: Literal["waiting", "checking", "tunneling", "ready", "failed"] = Field(
        description=(
            "waiting for the command to run; checking while the server dials "
            "the machine; tunneling while the machine, which could not be "
            "dialed, sets up a tunnel and is reached through it; ready with "
            "target and probe; failed with error"
        )
    )
    expires_at: datetime
    machine: Optional[ComputerSSHPairingMachine] = None
    target: Optional[ComputerSSHPairingTarget] = None
    probe: Optional[ComputerSSHProbeResponse] = Field(
        None, description="As POST /ssh/probe answers, for the machine that logged in"
    )
    error: Optional[ComputerSSHPairingError] = None


# A form field the pairing script posts is cut to this, so what a pairing
# keeps of one stays small. The route caps the whole body before parsing it.
_MAX_FORM_FIELD = 16384


class _ScriptForm(BaseModel):
    """A form the pairing script posts, every field text and none refused.

    The script prints the answer's body, so a field of the wrong shape or
    length is cut to text here and judged where it is read: a JSON 422 would
    reach the machine's terminal as noise.
    """

    @field_validator("*", mode="before")
    @classmethod
    def _as_text(cls, value: Any) -> str:
        return str(value)[:_MAX_FORM_FIELD]


class ComputerSSHPairingReport(_ScriptForm):
    """What the script reports of the machine it runs on, all of it unchecked."""

    user: str = ""
    os: str = ""
    hostname: str = ""
    host_keys: str = ""
    ports: str = ""
    addresses: str = ""
    can_tunnel: str = ""
    # "1" from a machine with no SSH server to dial, such as one LangAlpha's
    # helper answers for: it is reached through a tunnel or not at all.
    tunnel_only: str = ""
    tunnel_id: str = ""
    # The run of the command that sent this, a report or a stop.
    run: str = ""
    # Sent with only the run beside it, and set even when empty: the script
    # stopping, and why.
    failed: Optional[str] = None


class ComputerSSHTunnelRequest(_ScriptForm):
    """The script's ask for a tunnel: the public half of the key it made."""

    public_key: str = ""
    hostname: str = ""


class ComputerSSHInfo(BaseModel):
    """Where an ssh computer's machine is, as its owner entered it."""

    host: str
    port: int
    username: str
    fingerprint: Optional[str] = Field(
        None, description="SHA256 fingerprint of the pinned host key"
    )
    tunnel: bool = Field(
        False,
        description=(
            "The machine connects out through a tunnel rather than being "
            "dialed; host is then only its name"
        ),
    )
    tunnel_connected: Optional[bool] = Field(
        None,
        description="Whether the tunnel is up now; null when there is none or it is not known",
    )
    helper: bool = Field(
        False,
        description=(
            "LangAlpha's helper answers for the machine rather than an sshd, "
            "as on a Mac under remote control; our key is then in the helper's "
            "own folder, never in ~/.ssh/authorized_keys"
        ),
    )
    os: Optional[str] = Field(
        None,
        description="The machine's system as uname -s names it; null on a computer added before it was kept",
    )
    model: Optional[str] = Field(
        None,
        description="A Mac's model name, as System Information gives it (\"MacBook Pro\"); null elsewhere",
    )
    distro: Optional[str] = Field(
        None, description='The system and its version ("macOS 15.4", "Ubuntu 24.04 LTS")'
    )
    arch: Optional[str] = Field(None, description="The processor architecture, as uname -m names it")
    cpu: Optional[str] = Field(None, description="The processor as the system names it")
    cores: Optional[int] = None
    memory_bytes: Optional[int] = None


class ComputerTierSpec(BaseModel):
    """One spec preset a provider offers, as its config sizes it."""

    tier: str = Field(description="Tier id, as a create or spec change names it")
    cpu: int = Field(description="vCPU cores")
    memory_gib: int = Field(description="Memory in GiB")
    disk_gib: int = Field(description="Disk in GiB")


class ComputerProviderOption(BaseModel):
    """One sandbox provider a new computer may be created on."""

    kind: ComputerKind = Field(description="Sandbox provider")
    is_default: bool = Field(
        description="Whether a computer created without a kind runs here"
    )
    allowed: Optional[bool] = Field(
        description=(
            "Whether the caller may create a computer here; null when it is "
            "not gated or the answer is unknown, and the create is then allowed"
        )
    )
    tiers: List[ComputerTierSpec] = Field(
        default_factory=list,
        description=(
            "The spec presets offered here, smallest first; empty where the "
            "machine is its owner's and they size it"
        ),
    )
    default_name: str = Field(
        description=(
            "What a computer created here without a name is called, before it "
            "is numbered past the user's other computers' names"
        )
    )


class ComputerProvidersResponse(BaseModel):
    """The sandbox providers a new computer may be created on."""

    providers: List[ComputerProviderOption] = Field(
        default_factory=list, description="The default provider first"
    )
    tunnels: bool = Field(
        False,
        description=(
            "Whether an ssh machine may connect out to this server through a "
            "tunnel, which one with no SSH server of its own needs"
        ),
    )


class ComputerSpecRequest(BaseModel):
    """Request model for changing a computer's spec tier."""

    tier: ResourceTierId = Field(
        description="Target spec preset",
    )


class ComputerAlwaysOnRequest(BaseModel):
    """Request model for toggling a computer's always-on flag."""

    enabled: bool = Field(description="Whether to keep the computer always-on")


class ComputerFilesBackupRequest(BaseModel):
    """Request model for a machine owner's file backup switch."""

    enabled: bool = Field(
        description=(
            "Whether routine backups of its workspaces' files run; off keeps "
            "the last backup as it is"
        )
    )


class ComputerRenameRequest(BaseModel):
    """Request model for renaming a computer."""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(
        min_length=1,
        max_length=255,
        description="New user-facing computer name, unique among the user's computers",
    )


DiskLevel = Literal["healthy", "notice", "warning", "critical"]


class ComputerDisk(BaseModel):
    """The last disk reading taken on a computer, shared by every workspace on it."""

    used_bytes: int = Field(description="Bytes in use on the computer's disk")
    total_bytes: int = Field(description="Size of the computer's disk in bytes")
    free_bytes: int = Field(description="Bytes still free on the computer's disk")
    measured_at: datetime = Field(description="When the reading was taken")
    level: DiskLevel = Field(
        description="How close the disk is to full, from free space alone"
    )


SpecChangeState = Literal["in_progress", "succeeded", "failed"]

# Why an accepted change failed, read back from the row.
SpecChangeErrorCode = Literal[
    "turn_active",
    "backup_incomplete",
    "busy",
    "interrupted",
    "disk_too_small",
    "not_allowed",
    "unknown",
]

# Why POST /computers/{id}/spec refused to accept a change (409).
SpecRefusalCode = Literal[
    "turn_active", "spec_in_progress", "busy", "backup_incomplete"
]


class UnsavedFileOut(BaseModel):
    """A file a strict backup could not save, so its only copy is on the machine."""

    path: str
    reason: UnsavedReason
    size: Optional[int] = None


class SpecChangeError(BaseModel):
    """Why a spec change failed, in a shape a client can map to copy."""

    code: SpecChangeErrorCode
    message: str = Field(description="A sentence the user can be shown as is")
    files: List[UnsavedFileOut] = Field(
        default_factory=list,
        description="For backup_incomplete: the files the backup could not save",
    )


class SpecChangeRefusal(BaseModel):
    """The 409 detail of a spec change refused before it was accepted.

    Same shape as :class:`SpecChangeError`, so a client renders a refusal and
    a failed outcome with one piece of code.
    """

    code: SpecRefusalCode
    message: str
    files: List[UnsavedFileOut] = Field(default_factory=list)


class ComputerSpecChange(BaseModel):
    """The last spec change requested on a computer and how it went.

    The change runs after the request that asked for it has returned, so this
    is how a client learns the outcome. Stored as this shape in
    ``computers.spec_change``.
    """

    target_tier: str = Field(description="Tier the change moves to")
    from_tier: str = Field(description="Tier the computer was at when it began")
    state: SpecChangeState
    error: Optional[SpecChangeError] = None
    started_at: datetime
    heartbeat_at: Optional[datetime] = Field(
        None,
        description=(
            "When the worker running this change last reported in. Null on "
            "changes recorded before it existed."
        ),
    )
    finished_at: Optional[datetime] = None
    claim_id: Optional[str] = Field(
        None,
        description=(
            "Identifies this change; a new request gets a new one. Null on "
            "changes recorded before it existed."
        ),
    )
    took_over: bool = Field(
        False,
        description=(
            "This change took over one whose worker stopped reporting, so the "
            "machine's real size is unknown and it is rebuilt even at the "
            "tier the row reads."
        ),
    )


StartFailureRemedy = Literal["reinstall", "reconnect", "retry"]


class StartFailure(BaseModel):
    """Why the computer's last start failed, kept until one succeeds."""

    code: str = Field(
        description=(
            "folder_marked (another computer's mark is on the root), an SSH "
            "connect code (root_dir, not_connected, auth_failed, "
            "host_key_mismatch, unreachable, timeout, gateway_unreachable, "
            "unsupported, private_address, host_key_invalid), or start_failed "
            "for anything else"
        )
    )
    message: str = Field(description="A sentence the client may show as is")
    owner: Optional[str] = Field(
        None,
        description="With folder_marked: the computer the folder's mark names",
    )
    remedy: StartFailureRemedy = Field(
        description=(
            "What fixes it: reinstall (take the folder back or pick another), "
            "reconnect (pair the machine again), or retry (start again)"
        )
    )
    at: datetime = Field(description="When the start failed")


class ComputerResponse(BaseModel):
    """Response model for computer details."""

    computer_id: str = Field(description="Unique computer identifier")
    user_id: str = Field(description="Owner user ID")
    kind: str = Field(description="Execution backend: daytona, docker, e2b, ssh")
    name: str = Field(description="User-facing computer name")
    status: str = Field(
        description=(
            "Computer status: creating, starting, running, stopping, stopped, "
            "error, deleted"
        )
    )
    resource_tier: str = Field(
        "standard",
        description=(
            "Spec preset: standard (Small), performance (Medium), max (Large), "
            "xlarge (Extra Large)"
        ),
    )
    is_always_on: bool = Field(
        False,
        description="Whether auto-stop is disabled (always-on computer)",
    )
    files_backup: bool = Field(
        True,
        description=(
            "Whether routine backups of its workspaces' files run; off only on "
            "a machine its owner brought"
        ),
    )
    is_primary: bool = Field(
        False,
        description="Whether this is the user's default computer",
    )
    root_dir: str = Field(description="Filesystem root of the computer's home")
    workspace_count: int = Field(
        0,
        description="Live projects on this computer, which its stop takes down together",
    )
    layout_version: Optional[int] = Field(
        None,
        description=(
            "Filesystem layout the machine last reported. Null until one has "
            "been observed, which is not the same as version zero."
        ),
    )
    provider_ref: Optional[str] = Field(
        None,
        description=(
            "Vendor identifier for the running machine. Present only for the "
            "owner; null when the computer has never been provisioned."
        ),
    )
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    last_activity_at: Optional[datetime] = Field(
        None,
        description="Last agent activity timestamp",
    )
    stopped_at: Optional[datetime] = Field(
        None,
        description="When the computer was stopped (if status=stopped)",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="Configuration settings",
    )
    disk: Optional[ComputerDisk] = Field(
        None,
        description=(
            "Last disk reading. Null when never measured, or when the disk has "
            "no size of its own (a local computer without a storage quota)."
        ),
    )
    spec_change: Optional[ComputerSpecChange] = Field(
        None,
        description="The last spec change and its outcome; null when none was requested",
    )
    ssh: Optional[ComputerSSHInfo] = Field(
        None, description="The machine an ssh computer runs on; null for other kinds"
    )
    start_failure: Optional[StartFailure] = Field(
        None,
        description=(
            "Why the last start failed; null once a start succeeds or a "
            "reinstall or reconnect is accepted"
        ),
    )

    model_config = ConfigDict(from_attributes=True)


class WorkspaceStorage(BaseModel):
    """One workspace folder's share of its computer's disk."""

    workspace_id: str
    name: str
    dir_name: Optional[str] = None
    bytes: int


class ComputerStorageResponse(BaseModel):
    """A computer's disk with each workspace folder's size."""

    disk: Optional[ComputerDisk] = None
    workspaces: List[WorkspaceStorage] = Field(
        default_factory=list,
        description="Workspace folders by size, largest first",
    )
    other_bytes: int = Field(
        0,
        description="Used bytes outside any workspace folder (packages, caches)",
    )
    live: bool = Field(
        False,
        description=(
            "Whether this was measured on the running machine (now, or within "
            "the last minute) rather than read from the stored reading"
        ),
    )


class ComputerListResponse(BaseModel):
    """Response model for a user's computer list."""

    computers: List[ComputerResponse] = Field(
        default_factory=list,
        description="The user's computers, primary first",
    )
    total: int = Field(0, description="Total number of computers")


class ComputerActionResponse(BaseModel):
    """Response model for computer actions (start, stop, archive)."""

    computer_id: str = Field(description="Computer identifier")
    status: str = Field(description="New computer status")
    message: str = Field(description="Action result message")


KeyRemovalOutcome = Literal["removed", "kept", "skipped", "unreached"]


class ComputerKeyRemoval(BaseModel):
    """What became of the client key's line on a removed ssh computer's machine."""

    outcome: KeyRemovalOutcome = Field(
        description=(
            "removed: taken out of ~/.ssh/authorized_keys. kept: another of the "
            "user's computers logs in with it through the same file, as the same "
            "user on the machine or from a host that shares the home. "
            "skipped: the file was not one to change safely (a link, another "
            "owner's, missing, a line for the key its owner changed, or the key "
            "authorized from another file). unreached: the machine did not answer"
        )
    )
    used_by: Optional[str] = Field(
        None, description="On kept, the name of the computer that still logs in with it"
    )
    fingerprint: Optional[str] = Field(
        None, description="SHA256 fingerprint of the key, for removing it by hand"
    )
    comment: Optional[str] = Field(
        None, description="The comment that ends the key's line"
    )


FilesRemovalOutcome = Literal["deleted", "unreached", "failed"]


class ComputerFilesRemoval(BaseModel):
    """What became of LangAlpha's files on a removed ssh computer's machine,
    its owner having asked for them to go."""

    outcome: FilesRemovalOutcome = Field(
        description=(
            "deleted: its folders in the root went, and the runtime in ~/.langalpha "
            "unless kept below. unreached: the deletion never reached the machine, "
            "so nothing went. failed: it reached the machine but did not finish, "
            "so part may have gone"
        )
    )
    root_kept: bool = Field(
        False,
        description=(
            "Only with deleted: the root folder stays, holding files the owner "
            "put there or a set-aside folder of theirs that could not go back"
        ),
    )
    runtime_kept: bool = Field(
        False,
        description=(
            "Only with deleted: ~/.langalpha stays, since another computer on "
            "the login uses it or an install holds it"
        ),
    )


class ComputerRemoveRequest(BaseModel):
    """Request body for removing a computer.

    The workspaces ride here rather than in the query, which a computer with
    a hundred or so would push past the request line limits of common proxies.
    """

    delete_workspaces: List[str] = Field(
        default_factory=list,
        description=(
            "Workspaces on it to delete first, each as its own delete would. "
            "One on it this does not name keeps the computer, and nothing is deleted"
        ),
    )


class ComputerRemoveResponse(BaseModel):
    """What a removal left behind that its owner may want to know about."""

    key: Optional[ComputerKeyRemoval] = Field(
        None,
        description="Null for a computer on any kind but ssh, and when the key was kept on request",
    )
    files: Optional[ComputerFilesRemoval] = Field(
        None, description="Null unless the removal was asked to delete the machine's files"
    )
    workspaces_deleted: List[str] = Field(
        default_factory=list,
        description=(
            "The named workspaces it deleted. One named but moved off the computer, "
            "or deleted by another request, meanwhile is not among them"
        ),
    )


class ComputerSessionResponse(BaseModel):
    """What a computer is, plus what the answering worker holds for it.

    ``status`` and ``provider_ref`` are the cross-worker truth from Postgres.
    ``ready`` and ``session_provider_ref`` describe only the worker that served
    this request, because a runtime session is process-local execution context;
    a client must never read them as liveness.
    """

    computer_id: str = Field(description="Computer identifier")
    status: str = Field(description="Computer status, from Postgres")
    provider_ref: Optional[str] = Field(
        None, description="Vendor identifier for the machine, from Postgres"
    )
    ready: bool = Field(
        False,
        description="This worker holds a session with a runtime attached",
    )
    session_provider_ref: Optional[str] = Field(
        None,
        description="What this worker's session is bound to, if it holds one",
    )
