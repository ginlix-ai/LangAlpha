"""Composite filesystem backend — prefix-routed fan-out over the rich-method surface."""

from __future__ import annotations

import asyncio
from typing import Any, Sequence

import structlog

from ptc_agent.agent.backends.memory import StoreMemoryBackend
from ptc_agent.agent.backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)


class CompositeFilesystemBackend:
    """Rich-method filesystem backend that routes by path prefix."""

    def __init__(
        self,
        *,
        sandbox: SandboxBackend,
        routes: Sequence[StoreMemoryBackend],
    ) -> None:
        self._sandbox = sandbox
        # Longest prefix wins.
        self._routes: list[StoreMemoryBackend] = sorted(
            routes, key=lambda b: len(b.root_prefix), reverse=True
        )

    @staticmethod
    def _looks_memory_targeted(raw_path: str) -> bool:
        return any(
            f".agents/{tier}/memory/" in raw_path
            for tier in ("user", "workspace")
        )

    def normalize_path(self, path: str) -> str:
        # `..` on a memory path could normalize into the sandbox FS and silently
        # bypass the store. Reject at the perimeter.
        if ".." in path.split("/") and self._looks_memory_targeted(path):
            raise ValueError(
                f"Path traversal not allowed on memory paths: {path!r}"
            )
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    @property
    def sandbox(self) -> SandboxBackend:
        return self._sandbox

    def __getattr__(self, name: str) -> Any:
        # Only fires when normal attribute lookup fails, so never shadows
        # methods defined on the composite. Delegates non-filesystem ops
        # (aexecute, skills_manifest, etc.) to the sandbox.
        sandbox = self.__dict__.get("_sandbox")
        if sandbox is None:
            raise AttributeError(name)
        return getattr(sandbox, name)

    def _route_for(self, normalized_path: str) -> StoreMemoryBackend | None:
        for route in self._routes:
            prefix = route.root_prefix
            if normalized_path.startswith(prefix):
                return route
            if normalized_path.rstrip("/") == prefix.rstrip("/"):
                return route
        return None

    async def aread_text(self, file_path: str) -> str | None:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aread_text(normalized)
        return await self._sandbox.aread_text(normalized)

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aread_range(normalized, offset, limit)
        return await self._sandbox.aread_range(normalized, offset, limit)

    async def awrite_text(self, file_path: str, content: str) -> bool:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.awrite_text(normalized, content)
        return await self._sandbox.awrite_text(normalized, content)

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aedit_text(
                normalized, old_string, new_string, replace_all=replace_all
            )
        return await self._sandbox.aedit_text(
            normalized, old_string, new_string, replace_all=replace_all
        )

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        normalized = self.normalize_path(path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aglob_paths(pattern, path)

        # Sandbox root (or outside every route) — fan out in parallel.
        matching_routes = [
            r for r in self._routes
            if r.root_prefix.startswith(normalized.rstrip("/") + "/")
            or normalized in ("/", "")
        ]
        batches = await asyncio.gather(
            self._sandbox.aglob_paths(pattern, normalized),
            *(r.aglob_paths(pattern, r.root_prefix) for r in matching_routes),
        )
        seen: set[str] = set()
        ordered: list[str] = []
        for batch in batches:
            for p in batch:
                if p in seen:
                    continue
                seen.add(p)
                ordered.append(p)
        return ordered

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
        normalized = self.normalize_path(path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.agrep_rich(
                pattern,
                path=normalized,
                output_mode=output_mode,
                glob=glob,
                type=type,
                case_insensitive=case_insensitive,
                show_line_numbers=show_line_numbers,
                lines_after=lines_after,
                lines_before=lines_before,
                lines_context=lines_context,
                multiline=multiline,
                head_limit=head_limit,
                offset=offset,
            )

        # Gather all hits first, then apply one global offset/head_limit slice —
        # per-backend pagination would produce the wrong slice at the root.
        matching_routes = [
            r for r in self._routes
            if r.root_prefix.startswith(normalized.rstrip("/") + "/")
            or normalized in ("/", "")
        ]
        sandbox_coro = self._sandbox.agrep_rich(
            pattern,
            path=normalized,
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
        route_coros = [
            r.agrep_rich(
                pattern,
                path=r.root_prefix,
                output_mode=output_mode,
                glob=glob,
                type=type,
                case_insensitive=case_insensitive,
                show_line_numbers=show_line_numbers,
                head_limit=None,
                offset=0,
            )
            for r in matching_routes
        ]
        sandbox_result, *route_results = await asyncio.gather(sandbox_coro, *route_coros)

        if not isinstance(sandbox_result, list):
            return sandbox_result

        combined: list[Any] = list(sandbox_result)
        for memory_result in route_results:
            if isinstance(memory_result, list):
                combined.extend(memory_result)

        start = max(0, offset)
        if head_limit is not None:
            return combined[start : start + head_limit]
        return combined[start:]
