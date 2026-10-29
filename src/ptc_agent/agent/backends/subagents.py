"""SubagentsBackend: each of the user's subagents as a file in `.agents/user/agents/`.

One markdown file per subagent the user has (every offered base, switched off
or not, and every custom one), plus a read-only ``main.md`` for the agent
itself. ``services.subagents.file`` renders them and turns a save into the
same write the Subagents pane makes.
"""

from __future__ import annotations

from ptc_agent.agent.backends.db_json_folder import DbJsonFolderRoute
from ptc_agent.agent.backends.db_json_route import README_FILE, UserDataValidationError
from ptc_agent.core.paths import SandboxLayout
from src.server.services.subagents import file

__all__ = ["README_FILE", "SubagentsBackend"]


class SubagentsBackend(DbJsonFolderRoute):
    """Filesystem surface backed by the `user_subagents` table and the bases a turn merges."""

    directory = SandboxLayout.USER_AGENTS_DIR
    read_only_files = frozenset({file.MAIN_FILE})
    name_rule = file.NAME_RULE
    entry = "a subagent"

    source = "subagents_backend"
    read_failure = "Failed to read subagents"
    read_only = (
        "Subagent files are read-only through the file panel. "
        "Change them in the Subagents pane or ask the agent."
    )
    undeletable = (
        "Subagent files cannot be deleted through the file panel. "
        "Manage subagents in the Subagents pane."
    )

    _readme_text: str | None = None

    @classmethod
    def file_named(cls, name: str) -> file.SubagentFile | file.MainFile | None:
        return file.file_named(name)

    @classmethod
    async def names(cls, user_id: str) -> list[str]:
        return await file.file_names(user_id)

    @classmethod
    async def rendered(cls, user_id: str) -> dict[str, tuple[str, str]]:
        return await file.rendered_files(user_id)

    async def _readme(self) -> str:
        # Once per route, which lives for one request: it names the user's
        # connectors, and a listing reads it each time.
        if self._readme_text is None:
            self._readme_text = await file.readme(self._user_id)
        return self._readme_text

    def _read_only_refusal(self, file_path: str) -> UserDataValidationError:
        if file_path.rsplit("/", 1)[-1] != file.MAIN_FILE:
            return super()._read_only_refusal(file_path)
        return self._refusal(
            "schema_error",
            file.MAIN_FILE,
            f"{file_path} is kept by the server; it can't be edited. It shows what you run on, "
            "which the user sets in Settings, or for one thread in its composer.",
        )
