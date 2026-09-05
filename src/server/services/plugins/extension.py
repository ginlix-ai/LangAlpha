"""The ``ai.langalpha`` extension namespace.

``extensions`` is the only extension point Agent Plugins defines — mcp.json is
``additionalProperties: false`` at every level — so this is where a package
says the things the portable document has nowhere to put.

``secrets[]`` declares vault credentials the plugin's servers need: the
standard blueprint fields plus ``bind`` targets that materialize
``${vault:NAME}`` references into the declared servers' env/header maps at
install time. The portable mcp.json is never rewritten — binds land only on
the internal rows we create. ``resolve_binds`` decides where each bind would
land; ``materialize_binds`` writes it.

``servers{}`` describes each entry: how it reads in the UI and the prompt, and
how much of its tool surface the agent sees. ``apply_server_metadata`` copies
it onto the plans. The same key is read from the bundles that ship with the
app (``ptc_agent.config.plugins``), which is why the model lives there.

Nothing in this namespace is fatal. The block is our own invention, optional
by construction, and every defect in it is an authoring slip in a field the
spec does not even define — while the cost of refusing was the entire package,
every other server and every skill in it, over one typo. So a bind that cannot
land is dropped with a diagnostic and the install continues, which is the same
isolation §7.1 already gives a defective entry.
"""

from collections.abc import Container
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, ValidationError, model_validator

from ptc_agent.config.core import VaultBlueprint
from ptc_agent.config.plugins import NAMESPACE, ServerMeta
from src.server.models.mcp_server import DESCRIPTION_MAX, INSTRUCTION_MAX
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.mcp import McpEntryPlan

__all__ = [
    "NAMESPACE",
    "LangalphaExtension",
    "PluginSecret",
    "SecretBind",
    "ServerMeta",
    "apply_server_metadata",
    "materialize_binds",
    "parse_extension",
    "resolve_binds",
]


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
    """One model for the whole namespace, whichever kind of package carries it.

    Every key is optional and some are read on only one path — ``icon`` is a
    bundle's, ``secrets`` an upload's. They are still declared here, because
    the alternative is that a package borrowing our own bundles' manifest
    shape fails ``extra=forbid`` and loses the rest of its namespace with it.
    """

    secrets: list[PluginSecret] = Field(default_factory=list)
    servers: dict[str, ServerMeta] = Field(default_factory=dict)
    # Accepted and ignored: a package's skills are the directories it carries,
    # never a claim its manifest makes. Kept so a manifest still naming them
    # is not rejected whole.
    skills: list[str] = Field(default_factory=list)
    #: The site that owns a wrapper bundle's mark. Bundles only.
    icon: str | None = None

    model_config = {"extra": "forbid"}


def _diag(code: str, target: str, message: str) -> Diagnostic:
    return Diagnostic(
        level="warning",
        scope="entry" if target else "plugin",
        target=target,
        code=code,
        message=f"extensions.{NAMESPACE}: {message}",
    )


def parse_extension(
    payload: Any, diagnostics: list[Diagnostic] | None = None
) -> LangalphaExtension:
    """Parse the namespace payload (None = absent).

    An unparseable block yields an empty extension rather than raising: the
    declarations are lost, so nothing binds and any secret the package needs
    surfaces as an unfilled reference, which is a state the wizard already
    knows how to show.
    """
    if payload is None:
        return LangalphaExtension()
    try:
        return LangalphaExtension(**payload)
    except (ValidationError, TypeError) as e:
        if diagnostics is not None:
            diagnostics.append(_diag("extension_invalid", "", str(e)))
        return LangalphaExtension()


@dataclass(frozen=True, slots=True)
class ResolvedBind:
    """One validated bind: which secret lands in which entry's which field."""

    secret_name: str
    plan: McpEntryPlan
    section: str  # "env" or "headers"
    field: str


def resolve_binds(
    extension: LangalphaExtension,
    plans: list[McpEntryPlan],
    *,
    document_dropped: bool = False,
    diagnostics: list[Diagnostic] | None = None,
) -> list[ResolvedBind]:
    """Say where each usable bind would land. Writes nothing.

    A bind must name an entry in this plugin, match its transport (env for
    stdio, header for remotes), and not overwrite a value the package ships.
    One that fails any of those is dropped, and the entry installs without it
    like any other unfilled reference. sse entries are legal targets — their
    binds materialize when the upgrade probe installs them, from the same
    stored manifests.

    ``document_dropped`` says mcp.json failed at document level, so there are
    no plans to name and every bind is unresolvable for a reason that is not
    the bind's — reporting each one would bury the diagnostic that explains
    the drop.

    ``diagnostics`` is written only by the validation pass. ``materialize_binds``
    re-runs this against the same manifests after the row exists, and a second
    copy of each finding in the install report would say nothing new.
    """
    by_key = {p.key: p for p in plans}
    resolved: list[ResolvedBind] = []

    def _drop(secret_name: str, server: str, message: str) -> None:
        if diagnostics is not None:
            diagnostics.append(
                _diag(
                    "bind_unusable",
                    server,
                    f"secret {secret_name!r} {message}; it was not bound",
                )
            )

    for secret in extension.secrets:
        for bind in secret.bind:
            plan = by_key.get(bind.server)
            if plan is None:
                if not document_dropped:
                    _drop(
                        secret.name, bind.server,
                        f"binds to unknown server {bind.server!r}",
                    )
                continue
            if plan.skip_code is not None:
                # The entry was already dropped (schema/policy) and said so;
                # its bind has nothing to land on.
                continue
            if bind.env is not None:
                if plan.transport != "stdio":
                    _drop(
                        secret.name, bind.server,
                        f"env-binds to remote server {bind.server!r} "
                        f"(use header)",
                    )
                    continue
                if bind.env in (plan.config.get("env") or {}):
                    _drop(
                        secret.name, bind.server,
                        f"binds over env {bind.env!r} the package ships",
                    )
                    continue
                resolved.append(
                    ResolvedBind(secret.name, plan, "env", bind.env)
                )
            else:
                if plan.transport not in ("http", "sse"):
                    _drop(
                        secret.name, bind.server,
                        f"header-binds to stdio server {bind.server!r} "
                        f"(use env)",
                    )
                    continue
                headers = plan.config.get("headers") or {}
                if any(k.lower() == bind.header.lower() for k in headers):
                    _drop(
                        secret.name, bind.server,
                        f"binds over header {bind.header!r} the package ships",
                    )
                    continue
                resolved.append(
                    ResolvedBind(secret.name, plan, "headers", bind.header)
                )
    return resolved


def materialize_binds(
    extension: LangalphaExtension,
    plans: list[McpEntryPlan],
    granted: Container[str],
    *,
    document_dropped: bool = False,
) -> None:
    """Write ``${vault:NAME}`` into each granted bind's target field.

    A name outside ``granted`` is simply not written: the entry installs
    without the credential and derives ``needs_secret`` like any other unset
    reference, which is the same inert state an unfilled blueprint produces.

    ``document_dropped`` is passed through rather than assumed from an empty
    ``plans``, because an empty plan list is also what a plugin with no MCP
    component at all looks like.
    """
    for bind in resolve_binds(
        extension, plans, document_dropped=document_dropped
    ):
        if bind.secret_name not in granted:
            continue
        target = bind.plan.config.setdefault(bind.section, {})
        target[bind.field] = f"${{vault:{bind.secret_name}}}"


def apply_server_metadata(
    extension: LangalphaExtension,
    plans: list[McpEntryPlan],
    *,
    document_dropped: bool = False,
    diagnostics: list[Diagnostic] | None = None,
) -> None:
    """Copy each entry's declared description, instruction and exposure mode
    onto its plan, so the installed row reads the way the package intended
    rather than falling to our defaults.

    Text past the row's limit is clipped rather than refused: these fields
    decide how a server introduces itself, and losing the server over the
    length of its own description would be the wrong trade. Blueprints are not
    copied — the request model rejects them on a user server, and ``secrets[]``
    is how a plugin declares a credential it wants the user to hold.
    """
    by_key = {p.key: p for p in plans}
    for key, meta in extension.servers.items():
        plan = by_key.get(key)
        if plan is None:
            if diagnostics is not None and not document_dropped:
                diagnostics.append(
                    _diag(
                        "server_meta_unknown", key,
                        f"describes unknown server {key!r}",
                    )
                )
            continue
        if not plan.installable:
            continue
        for field, cap in (
            ("description", DESCRIPTION_MAX),
            ("instruction", INSTRUCTION_MAX),
        ):
            value = getattr(meta, field)
            if not value:
                continue
            if len(value) > cap:
                value = value[:cap]
                if diagnostics is not None:
                    diagnostics.append(
                        _diag(
                            "server_meta_clipped", key,
                            f"{field} is longer than {cap} characters and was "
                            "clipped",
                        )
                    )
            plan.config[field] = value
        if meta.tool_exposure_mode is not None:
            plan.config["tool_exposure_mode"] = meta.tool_exposure_mode
