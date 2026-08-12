"""Single workflow mount: repo-shipped scripts overlaid by the user's own.

The resolver already prefers a user-saved workflow over a same-named
pre-built one; this backend makes the filesystem say the same thing. One
directory holds both, and any write — including an edit of a pre-built
script — forks that script into the user's tier, where deleting it exposes
the shipped version again.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import structlog
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends.langgraph_store import (
    MAX_CONTENT_BYTES,
    InvalidStoreKeyError,
    StoreBackend,
    StoreContentTooLargeError,
)
from ptc_agent.agent.backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)

PREBUILT_READ_ONLY_ERROR = (
    "Pre-built workflows are read-only here. Sign in so writes can fork them "
    "into your own workflow tier."
)

_WORKFLOW_TIER = "workflows"

_PREBUILT_NAMESPACE = ("prebuilt", _WORKFLOW_TIER)

WORKFLOW_SUFFIX = ".js"

# Every saved workflow is addressed as `/api/v1/workflows/{name}`, and that
# path converter never matches a `/`. A key outside this shape is a row no
# client can read back or delete, so it must never be created. The engine
# holds `meta.name` to the same rule, so a runnable name is always a
# saveable one.
WORKFLOW_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


class MalformedWorkflowError(ValueError):
    """Raised when a saved workflow row carries no readable script."""


def workflow_namespace(user_id: str) -> tuple[str, ...]:
    """Store namespace holding one user's saved workflow scripts."""
    return (user_id, _WORKFLOW_TIER)


def workflow_key(name: str) -> str:
    """Store key a saved workflow is addressed by."""
    return f"{name}{WORKFLOW_SUFFIX}"


def workflow_name_from_key(key: str) -> str | None:
    """The addressable workflow name for a store key, or ``None`` if it has none."""
    if not key.endswith(WORKFLOW_SUFFIX):
        return None
    name = key[: -len(WORKFLOW_SUFFIX)]
    return name if WORKFLOW_NAME_RE.fullmatch(name) else None


def build_workflow_value(content: str, existing: Any) -> dict[str, Any]:
    """The stored envelope for a script, preserving an existing ``created_at``.

    Matches what ``StoreBackend`` writes through the agent mount so the two
    doors into this tier — the mount and the REST surface — leave identical
    rows behind.
    """
    now = datetime.now(UTC).isoformat()
    created_at = now
    if isinstance(existing, dict) and isinstance(existing.get("created_at"), str):
        created_at = existing["created_at"]
    return {
        "content": content,
        "encoding": "utf-8",
        "created_at": created_at,
        "modified_at": now,
    }


def workflow_script_from_value(value: Any) -> str:
    """Script text out of a stored envelope; raises when the row is unreadable.

    Absent and malformed have to stay distinguishable: a reader that folds
    them together resolves a corrupt saved workflow to the shipped one of the
    same name and silently runs a different script.
    """
    if isinstance(value, dict):
        content = value.get("content")
        if isinstance(content, str):
            return content
    raise MalformedWorkflowError("stored workflow value carries no 'content' string")


def workflow_script_byte_cap() -> int:
    """The size limit a workflow write actually has to clear.

    ``max_script_bytes`` is operator-configurable above the store's own value
    cap, so the tighter of the two is authoritative — otherwise a script could
    clear the configured cap and still be refused deeper down.
    """
    from src.config.settings import get_workflow_orchestration_config

    return min(
        get_workflow_orchestration_config().max_script_bytes, MAX_CONTENT_BYTES
    )


def prebuilt_workflow_backend(
    *,
    files: Mapping[str, str],
    root_prefix: str,
    sandbox_backend: SandboxBackend,
) -> StoreBackend:
    """Mount the repo-shipped scripts as a read-only tier.

    Seeding an ``InMemoryStore`` keeps the shipped tier on the same read /
    glob / grep code path as the user tier, so the two answer identically.
    """
    store = InMemoryStore()
    for key, source in files.items():
        store.put(_PREBUILT_NAMESPACE, key, {"content": source})
    return StoreBackend(
        store=store,
        namespace_factory=lambda: _PREBUILT_NAMESPACE,
        root_prefix=root_prefix,
        sandbox_backend=sandbox_backend,
        read_only=True,
        read_only_error=PREBUILT_READ_ONLY_ERROR,
    )


class WorkflowsBackend:
    """Overlay the user's writable tier on the read-only shipped tier."""

    def __init__(
        self,
        *,
        store_backend: StoreBackend,
        prebuilt_backend: StoreBackend,
    ) -> None:
        if store_backend.root_prefix != prebuilt_backend.root_prefix:
            raise ValueError(
                "workflow overlay requires both tiers at one root_prefix: "
                f"{store_backend.root_prefix!r} != "
                f"{prebuilt_backend.root_prefix!r}"
            )
        self._store = store_backend
        self._prebuilt = prebuilt_backend

    @property
    def root_prefix(self) -> str:
        return self._store.root_prefix

    async def _saved_paths(self) -> set[str]:
        """Absolute paths the user tier owns — these shadow pre-builts.

        Anchored at the mount root, not the caller's path: shadowing is a
        property of the tier, not of the subtree being searched. Strict
        because the caller reads absence out of a miss, and a store timeout
        would otherwise end the walk early and answer "no rows".
        """
        return set(
            await self._store.aglob_paths("*", self.root_prefix, strict=True)
        )

    async def _shadows_a_prebuilt(self, file_path: str) -> bool:
        """Whether a user row owns this path even though the read came back empty.

        ``StoreBackend`` answers unreadable and absent with the same ``None``
        — a malformed envelope and a store timeout both look like "no such
        row" — so falling straight through would resolve a saved workflow to
        the *shipped* script of the same name. Worse than running the wrong
        script: read falls through to the built-in while write always lands
        in the user tier, so an ordinary read-modify-write replaces the
        user's workflow with a derivative of ours.

        So an unreadable listing is reported as a shadow, not as absence: the
        caller answers "no such file" and the user retries, where guessing
        the other way loses their script. Only a listing that completed and
        did not name this path is treated as proof there is no user row.
        """
        try:
            return file_path in await self._saved_paths()
        except Exception:  # noqa: BLE001 - unreadable is not absent
            logger.warning(
                "workflow shadow check failed; refusing prebuilt fallback",
                path=file_path,
                exc_info=True,
            )
            return True

    async def aread_text(self, file_path: str) -> str | None:
        saved = await self._store.aread_text(file_path)
        if saved is not None:
            return saved
        if await self._shadows_a_prebuilt(file_path):
            return None
        return await self._prebuilt.aread_text(file_path)

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        saved = await self._store.aread_range(file_path, offset, limit)
        if saved is not None:
            return saved
        if await self._shadows_a_prebuilt(file_path):
            return None
        return await self._prebuilt.aread_range(file_path, offset, limit)

    async def awrite_text(self, file_path: str, content: str) -> bool:
        # Refuse anything the REST surface could not address again — a row
        # saved under such a key is unreadable and undeletable for good.
        # Checked before the store is touched, so nothing is stranded.
        if file_path.startswith(self.root_prefix):
            key = file_path[len(self.root_prefix):]
            if workflow_name_from_key(key) is None:
                raise InvalidStoreKeyError(
                    f"'{key}' is not a usable workflow file name. Write "
                    f"<name>{WORKFLOW_SUFFIX} directly under "
                    f"{self.root_prefix}, where <name> matches "
                    f"{WORKFLOW_NAME_RE.pattern}."
                )
        cap = workflow_script_byte_cap()
        size = len(content.encode("utf-8"))
        if size > cap:
            raise StoreContentTooLargeError(
                f"Workflow script is {size} bytes; max is {cap}. Shorten the "
                "script or push detail into the agents it dispatches."
            )
        return await self._store.awrite_text(file_path, content)

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        # Copy-on-write: editing a shipped script forks it into the user tier
        # rather than failing, mirroring the shadowing rule. Fork and edit land
        # as one write under the store's lock, and the workflow cap rides along
        # — the store's own value cap is the looser of the two, so an edit left
        # to it could grow a script past the size every later run refuses.
        return await self._store.aedit_text(
            file_path,
            old_string,
            new_string,
            replace_all=replace_all,
            base_content=await self._prebuilt.aread_text(file_path),
            max_bytes=workflow_script_byte_cap(),
        )

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        saved = await self._store.aglob_paths(pattern, path)
        builtin = await self._prebuilt.aglob_paths(pattern, path)
        return sorted(set(saved) | set(builtin))

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        # Each tier is searched unsliced, then the merged result is sliced
        # once — slicing per tier would drop matches before the merge.
        kwargs: dict[str, Any] = dict(
            output_mode=output_mode,
            glob=glob,
            type=type,
            case_insensitive=case_insensitive,
            show_line_numbers=show_line_numbers,
            lines_after=lines_after,
            lines_before=lines_before,
            lines_context=lines_context,
            multiline=multiline,
            head_limit=None,
            offset=0,
        )
        saved = await self._store.agrep_rich(pattern, path, **kwargs)
        builtin = await self._prebuilt.agrep_rich(pattern, path, **kwargs)
        shadowed = await self._saved_paths()

        def _path_of(entry: Any) -> str:
            if output_mode == "content":
                return str(entry).split(":", 1)[0]
            if output_mode == "count":
                return str(entry[0])
            return str(entry)

        merged = list(saved) + [
            entry for entry in builtin if _path_of(entry) not in shadowed
        ]
        start = max(0, offset)
        return (
            merged[start : start + head_limit]
            if head_limit is not None
            else merged[start:]
        )
