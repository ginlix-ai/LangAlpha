"""The check every user MCP server passes before it is saved.

A server is written only once it has shown it works: a remote one answers a
tool listing with the credential it will be saved with, a command one starts
on the user's computer and lists its tools, and one that asks for a sign-in is
saved by the sign-in's callback, never before it. A row that never validated
is clutter the agent and every surface carry around, and a check that runs
after the save can only report what is already there.

Every door that writes a server calls ``check_servers`` first and turns its
outcome into the response; nothing here writes.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from fastapi import HTTPException

from ptc_agent.core.mcp_sanitize import looks_like_placeholder
from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_probe import probe_configured, probe_verdict

logger = logging.getLogger(__name__)

#: How a failed check reads. The probe's verdicts, plus the two a probe cannot
#: produce: a command that would not start, and a transport nothing can run.
FailureVerdict = Literal[
    "needs_credential",
    "credential_rejected",
    "missing_secrets",
    "unreachable",
    "command_failed",
    "unsupported",
]

#: What a server reports when its check raised rather than answered.
NOT_CHECKED = "the server could not be checked; try again"

# Plugins carry a handful of servers; the probe gate bounds the process as a
# whole, this bounds one request's share of it.
_MAX_PARALLEL = 4


@dataclass(frozen=True)
class Passed:
    server: str


@dataclass(frozen=True)
class CheckFailure:
    server: str
    verdict: FailureVerdict
    reason: str
    missing_secrets: tuple[str, ...] = ()

    def as_detail(self) -> dict[str, Any]:
        return {
            "server": self.server,
            "verdict": self.verdict,
            "reason": self.reason,
            "missing_secrets": list(self.missing_secrets),
        }


@dataclass(frozen=True)
class SignInNeeded:
    server: str
    url: str

    def as_detail(self) -> dict[str, str]:
        return {"server": self.server, "url": self.url}


#: One server's answer.
CheckResult = Passed | CheckFailure | SignInNeeded


@dataclass(frozen=True)
class CheckOutcome:
    """Several servers' answers, so a plugin reports all of its failures at once."""

    passed: frozenset[str] = frozenset()
    failed: list[CheckFailure] = field(default_factory=list)
    sign_in: list[SignInNeeded] = field(default_factory=list)

    @classmethod
    def of(cls, results: Iterable[CheckResult]) -> CheckOutcome:
        results = list(results)
        return cls(
            passed=frozenset(r.server for r in results if isinstance(r, Passed)),
            failed=sorted(
                (r for r in results if isinstance(r, CheckFailure)),
                key=lambda f: f.server,
            ),
            sign_in=sorted(
                (r for r in results if isinstance(r, SignInNeeded)),
                key=lambda s: s.server,
            ),
        )


class McpCheckFailed(Exception):
    """One or more servers failed their check; nothing was written."""

    def __init__(self, failures: Iterable[CheckFailure]) -> None:
        self.failures = list(failures)
        super().__init__("; ".join(f"{f.server}: {f.reason}" for f in self.failures))

    def detail(self) -> dict[str, Any]:
        first = self.failures[0] if self.failures else None
        return {
            "code": "mcp_check_failed",
            "message": (
                f"{first.server} did not pass its check: {first.reason}"
                if first is not None
                else "the server did not pass its check"
            ),
            "failures": [f.as_detail() for f in self.failures],
        }


class McpSignInRequired(Exception):
    """A server is saved only by its sign-in; the caller starts that instead."""

    def __init__(self, servers: Iterable[SignInNeeded]) -> None:
        self.servers = list(servers)
        super().__init__(", ".join(s.server for s in self.servers))

    def detail(self) -> dict[str, Any]:
        return {
            "code": "mcp_sign_in_required",
            "message": "Sign in to the server to add it.",
            "servers": [s.as_detail() for s in self.servers],
        }


def check_http_error(
    answer: CheckFailure | SignInNeeded | McpCheckFailed | McpSignInRequired,
) -> HTTPException:
    """The response a write path answers with when its check did not pass.

    422 for a failure, which only a different input can change; 409 for a
    sign-in, which the same input passes once the user has signed in.
    """
    match answer:
        case CheckFailure():
            answer = McpCheckFailed([answer])
        case SignInNeeded():
            answer = McpSignInRequired([answer])
    status = 422 if isinstance(answer, McpCheckFailed) else 409
    return HTTPException(status_code=status, detail=answer.detail())


def check_secrets(
    literals: Mapping[str, str], supplied: Mapping[str, str] | None = None
) -> dict[str, str]:
    """The values a check resolves ahead of the vault.

    ``literals`` are the inline credentials a write is about to vault, and
    ``supplied`` the values the request names outright. A stand-in such as
    ``<your-api-key>`` stands in for nothing, so it is withheld: its server
    then fails as a key with no value, rather than sending a vendor the
    docs' text.
    """
    extra = {k: v for k, v in literals.items() if not looks_like_placeholder(v)}
    extra.update(supplied or {})
    return extra


async def _check_remote(
    user_id: str,
    server: McpServerInput,
    extra_secrets: Mapping[str, str],
    bearer: str | None,
) -> CheckResult:
    outcome = await probe_configured(
        user_id,
        server.url or "",
        server.headers,
        extra_secrets=extra_secrets,
        bearer=bearer,
    )
    match probe_verdict(outcome):
        case "ok" | "ok_authed":
            return Passed(server.name)
        case "oauth":
            return SignInNeeded(server=server.name, url=server.url or "")
        case "missing_secrets":
            names = ", ".join(outcome.missing_secrets)
            return CheckFailure(
                server.name,
                "missing_secrets",
                f"no value is set for {names}",
                missing_secrets=outcome.missing_secrets,
            )
        case "needs_credential":
            return CheckFailure(
                server.name,
                "needs_credential",
                "the server asks for a key and none was sent",
            )
        case "credential_rejected" if outcome.auth == "oauth":
            # The server answered what went out with its sign-in challenge.
            return CheckFailure(
                server.name,
                "credential_rejected",
                "the server did not accept the sign-in; connect it again"
                if bearer is not None
                else "the server rejected the key; it signs in with OAuth, so "
                "remove the key to sign in instead",
            )
        case "credential_rejected":
            return CheckFailure(
                server.name,
                "credential_rejected",
                outcome.error or "the server rejected the key",
            )
        case _:
            return CheckFailure(
                server.name, "unreachable", outcome.error or "the server did not answer"
            )


async def check_server(
    user_id: str,
    server: McpServerInput,
    *,
    extra_secrets: Mapping[str, str] | None = None,
    workspace_id: str | None = None,
    bearer: str | None = None,
) -> CheckResult:
    """Check one server as it would be saved.

    ``extra_secrets`` are vault values the same request is about to write,
    which the check sees ahead of the user's stored ones and which are never
    stored here. ``bearer`` is an existing sign-in's token, for an edit to a
    server that is already connected. ``workspace_id`` picks the computer a
    command server starts on; without it the user's own is used.
    """
    extra = dict(extra_secrets or {})
    if server.transport == "sse":
        return CheckFailure(
            server.name,
            "unsupported",
            "legacy SSE servers cannot run here; use the server's streamable "
            "HTTP address",
        )
    if server.transport == "stdio":
        # Imported here: the command check reaches the computer manager, which
        # the remote-only callers of this module never need to load.
        from src.server.services.mcp_command_check import check_command_server

        return await check_command_server(
            user_id, server, extra_secrets=extra, workspace_id=workspace_id
        )
    return await _check_remote(user_id, server, extra, bearer)


async def check_servers(
    user_id: str,
    items: Sequence[tuple[McpServerInput, Mapping[str, str]]],
    *,
    workspace_id: str | None = None,
    bearers: Mapping[str, str] | None = None,
) -> list[CheckResult]:
    """Check several servers together; each one's answer at its own position.

    Each server comes with the values its own check sees ahead of the vault
    (``check_server``'s ``extra_secrets``). A check that raises is that
    server's failure alone, so one broken check never takes the others'
    answers with it.
    """
    gate = asyncio.Semaphore(_MAX_PARALLEL)
    bearers = bearers or {}

    async def one(server: McpServerInput, extra: Mapping[str, str]) -> CheckResult:
        async with gate:
            try:
                return await check_server(
                    user_id,
                    server,
                    extra_secrets=extra,
                    workspace_id=workspace_id,
                    bearer=bearers.get(server.name),
                )
            except Exception:
                logger.warning(
                    "[mcp_validation] check of %s could not run",
                    server.name,
                    exc_info=True,
                )
                return CheckFailure(server.name, "unreachable", NOT_CHECKED)

    return list(await asyncio.gather(*(one(s, extra) for s, extra in items)))
