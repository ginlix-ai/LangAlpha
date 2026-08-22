"""The ``ai.langalpha`` extension namespace (secrets + bindings).

``extensions["ai.langalpha"].secrets[]`` declares vault credentials the
plugin's servers need: the standard blueprint fields plus ``bind`` targets
that materialize ``${vault:NAME}`` references into the declared servers'
env/header maps at install time. The portable mcp.json is never rewritten —
binds land only on the internal rows we create.

A declaration is a request, not a grant. ``resolve_binds`` decides whether
the package's own binds are coherent; ``materialize_binds`` writes only the
ones the user has actually granted, which ``plugins.grants`` decides.

Within an implemented namespace the client has authority over semantics
(spec §8), so every extension error is a whole-install failure before any
write — a half-honored secrets contract would install servers that can never
authenticate.
"""

from collections.abc import Container
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, ValidationError, model_validator

from ptc_agent.config.core import VaultBlueprint
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.mcp import McpEntryPlan

NAMESPACE = "ai.langalpha"


class SecretBind(BaseModel):
    """One place a declared secret is injected: a server's env or header."""

    server: str  # the mcp.json entry key, not the coerced row name
    env: str | None = None
    header: str | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "SecretBind":
        if bool(self.env) == bool(self.header):
            raise ValueError("bind must set exactly one of env or header")
        return self


class PluginSecret(VaultBlueprint):
    """A blueprint plus where it binds. Bindless declarations are legal —
    they surface in the vault UI without touching any server."""

    bind: list[SecretBind] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


class LangalphaExtension(BaseModel):
    secrets: list[PluginSecret] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


def _fail(message: str) -> PluginFatal:
    return PluginFatal(f"extensions.{NAMESPACE}: {message}")


def parse_extension(payload: Any) -> LangalphaExtension:
    """Parse the namespace payload (None = absent). Raises PluginFatal."""
    if payload is None:
        return LangalphaExtension()
    try:
        return LangalphaExtension(**payload)
    except (ValidationError, TypeError) as e:
        raise _fail(str(e)) from e


@dataclass(frozen=True, slots=True)
class ResolvedBind:
    """One validated bind: which secret lands in which entry's which field."""

    secret_name: str
    plan: McpEntryPlan
    section: str  # "env" or "headers"
    field: str


def resolve_binds(
    extension: LangalphaExtension, plans: list[McpEntryPlan]
) -> list[ResolvedBind]:
    """Validate every bind and say where it would land. Writes nothing.

    A bind must name an entry in this plugin, match its transport (env for
    stdio, header for remotes), and not overwrite a value the package ships.
    sse entries are legal targets — their binds materialize when the upgrade
    probe installs them, from the same stored manifests.

    Separate from ``materialize_binds`` because the two answer to different
    authorities and can't run at the same moment: these errors are the
    package's own and must refuse the install before the first write, while
    whether a given secret may be referenced at all depends on the user's
    vault, which the pure validation phase cannot read.
    """
    by_key = {p.key: p for p in plans}
    resolved: list[ResolvedBind] = []
    for secret in extension.secrets:
        for bind in secret.bind:
            plan = by_key.get(bind.server)
            if plan is None:
                raise _fail(
                    f"secret {secret.name!r} binds to unknown server "
                    f"{bind.server!r}"
                )
            if plan.skip_code is not None:
                # The entry was already dropped (schema/policy); its bind has
                # nothing to land on, and failing the whole install over a
                # skipped entry would defeat per-entry isolation.
                continue
            if bind.env is not None:
                if plan.transport != "stdio":
                    raise _fail(
                        f"secret {secret.name!r} env-binds to remote server "
                        f"{bind.server!r}; use header"
                    )
                if bind.env in (plan.config.get("env") or {}):
                    raise _fail(
                        f"secret {secret.name!r} bind collides with shipped "
                        f"env {bind.env!r} on {bind.server!r}"
                    )
                resolved.append(
                    ResolvedBind(secret.name, plan, "env", bind.env)
                )
            else:
                if plan.transport not in ("http", "sse"):
                    raise _fail(
                        f"secret {secret.name!r} header-binds to stdio server "
                        f"{bind.server!r}; use env"
                    )
                headers = plan.config.get("headers") or {}
                if any(k.lower() == bind.header.lower() for k in headers):
                    raise _fail(
                        f"secret {secret.name!r} bind collides with shipped "
                        f"header {bind.header!r} on {bind.server!r}"
                    )
                resolved.append(
                    ResolvedBind(secret.name, plan, "headers", bind.header)
                )
    return resolved


def materialize_binds(
    extension: LangalphaExtension,
    plans: list[McpEntryPlan],
    granted: Container[str],
) -> None:
    """Write ``${vault:NAME}`` into each granted bind's target field.

    A bind the user has not granted is simply not written: the entry installs
    without the credential and derives ``needs_secret`` like any other unset
    reference, which is the same inert state an unfilled blueprint produces.
    """
    for bind in resolve_binds(extension, plans):
        if bind.secret_name not in granted:
            continue
        target = bind.plan.config.setdefault(bind.section, {})
        target[bind.field] = f"${{vault:{bind.secret_name}}}"
