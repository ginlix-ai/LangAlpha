"""The file mount's notice: a row when the mount is found not serving, and one
when it serves again.

A thread's prompt says once whether the mount serves and holds that word for
the epoch (``FrozenPromptMiddleware``), and a turn takes a start still on its
way as one that serves rather than wait for it. So a prompt that says it
serves can be wrong: the start failed, or the mount went down since. The tools
already refuse what they cannot reach; the notice tells the model, as a
durable row in history, whenever the mount now differs from what it was last
told. A prompt that says the mount does not serve already says what the notice
would, so no row is written under it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from langchain.agents.middleware import AgentMiddleware

from ptc_agent.agent.middleware.runtime_context import (
    FILE_MOUNT_ROW_KIND,
    DurableUpdate,
    build_update_message,
    last_stated,
    prompt_says_mounted,
)

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.livefs_mount import MountHandle

FILE_MOUNT_SCHEMA_VERSION = 1


class _Computer(Protocol):
    livefs: MountHandle | None

    @property
    def mounts_livefs(self) -> bool: ...


class FileMountNoticeMiddleware(AgentMiddleware):
    """Announces in history that the file mount stopped serving, or serves again.

    Args:
        sandbox: The turn's computer, read on every call: a start no turn
            waited for answers in the middle of one.
        files_mounted: What the built prompt says, for a thread whose epoch
            has not stored its own word yet.
        waits_for_start: Wait out a start still to answer. For a Claude Code
            turn, which runs this once as it opens and cannot be told later,
            and waits for the computer's catch-ups before its CLI connects
            either way.
    """

    def __init__(
        self, sandbox: _Computer, *, files_mounted: bool, waits_for_start: bool = False
    ) -> None:
        super().__init__()
        self._sandbox = sandbox
        self._built = files_mounted
        self._waits = waits_for_start

    async def abefore_model(
        self, state: Any, runtime: Any = None
    ) -> dict[str, Any] | None:
        """A ``file_mount`` row when the mount differs from the last word on it.

        The last word is the last such row the model can still read, and the
        prompt's where there is none: a row a summary took is said again
        while the mount is still down. One it took that the mount has since
        outlived is not answered, since what the model then goes by is the
        prompt, which says the mount serves.
        """
        if not self._sandbox.mounts_livefs:
            # A thread moved to a machine that never mounts: its file tools
            # reach the transcripts there, which a row would say are gone.
            return None
        mount = self._sandbox.livefs
        if mount is not None and self._waits and mount.serving is None:
            await mount.started()
            # Taken back where the start ended without a mount.
            mount = self._sandbox.livefs
        serving = False if mount is None else mount.serving
        if serving is None or serving == last_stated(
            state, FILE_MOUNT_ROW_KIND, "serving", True
        ):
            return None
        if not prompt_says_mounted(state, self._built):
            return None
        row = DurableUpdate(
            kind=FILE_MOUNT_ROW_KIND,
            schema_version=FILE_MOUNT_SCHEMA_VERSION,
            # Every word is the template's, keyed on the provenance below.
            text="",
            provenance={"source": "harness", "serving": serving},
        )
        # No ids: the Pregel path mints them for a row a hook returns.
        return {"messages": [build_update_message(row)]}
