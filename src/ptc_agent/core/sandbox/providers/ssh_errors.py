"""How an SSH machine failed to connect, without the SSH client library.

The server classifies these failures for every provider's errors, so this
module imports nothing that loads asyncssh.
"""

from __future__ import annotations

from typing import Literal

ConnectErrorCode = Literal[
    "auth_failed",
    "unreachable",
    "timeout",
    "not_connected",
    "gateway_unreachable",
    "host_key_mismatch",
    "host_key_invalid",
    "unsupported",
    "private_address",
    "root_dir",
]
# The codes of a machine that did not answer, as against one that answered and
# refused. ``not_connected`` is a tunneled machine with no tunnel up, and
# ``gateway_unreachable`` the tunnel gateway itself not answering.
UNANSWERED_CODES: frozenset[ConnectErrorCode] = frozenset(
    {"unreachable", "timeout", "not_connected", "gateway_unreachable"}
)
# Of those, the ones whose sentence already says what to do: a tunnel that is
# down on the machine's side, or on the service's, which is not the owner's
# machine failing to answer.
_SELF_EXPLAINED_CODES: frozenset[ConnectErrorCode] = frozenset(
    {"not_connected", "gateway_unreachable"}
)
CHECK_MACHINE = (
    "Check that the machine is on and accepts SSH connections, then try again."
)


class SSHConnectError(RuntimeError):
    """The machine could not be reached, refused us, or cannot host a computer.

    ``code`` is what a client words its copy from; ``str(exc)`` is a sentence
    the machine's owner can act on, so unlike other provider text it may be
    shown to them.
    """

    def __init__(self, code: ConnectErrorCode, message: str) -> None:
        self.code: ConnectErrorCode = code
        super().__init__(message)

    @property
    def answered(self) -> bool:
        """The machine answered and refused, which is its owner's to fix; one
        that did not answer may on retry."""
        return self.code not in UNANSWERED_CODES

    @property
    def for_owner(self) -> str:
        """The sentence for the owner, with what to check when it is their
        machine that did not answer."""
        if self.answered or self.code in _SELF_EXPLAINED_CODES:
            return str(self)
        return f"{self} {CHECK_MACHINE}"


# A computer's id on its machine, as its folder's mark holds it. Only an id
# of this shape is ever named back to the machine as a path.
MACHINE_ID_PATTERN = r"^[0-9a-f]{32}$"


class SSHFolderMarked(SSHConnectError):
    """The folder is the root of a computer whose own files are still on the machine.

    ``owner`` is that computer's id, from the folder's mark. A create that
    names it as ``take_over`` clears that computer from the machine, as its
    removal would have, and takes the folder.
    """

    code_name = "folder_marked"

    def __init__(self, message: str, owner: str) -> None:
        super().__init__("root_dir", message)
        self.owner = owner

    def detail(self) -> dict[str, str]:
        """The refusal as an API detail, whose ``owner`` a retry takes over."""
        return {"code": self.code_name, "message": str(self), "owner": self.owner}


def connect_failure(exc: BaseException) -> SSHConnectError | None:
    """The :class:`SSHConnectError` behind *exc*, however it was wrapped.

    Follows the chain Python prints: an explicit cause, else the implicit
    context unless ``raise ... from None`` suppressed it.
    """
    seen: BaseException | None = exc
    for _ in range(32):
        if seen is None:
            return None
        if isinstance(seen, SSHConnectError):
            return seen
        seen = seen.__cause__ or (
            None if seen.__suppress_context__ else seen.__context__
        )
    return None
