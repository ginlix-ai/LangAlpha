"""The ``ai.langalpha`` extension namespace.

``extensions`` is the only extension point Agent Plugins defines — mcp.json is
``additionalProperties: false`` at every level — so this is where a package
says the things the portable document has nowhere to put.

``secrets[]`` declares vault credentials the plugin's servers need: the
standard blueprint fields plus ``bind`` targets that materialize
``${vault:NAME}`` references (behind an optional literal ``prefix``, the
``Bearer `` most vendors want) into the declared servers' env/header maps at
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
isolation §7.1 already gives a defective entry. The block's own shape degrades
the same way: an unknown key, or an entry that does not validate, is dropped
alone (``parse_extension``).
"""

from collections.abc import Callable, Container
from dataclasses import dataclass
from typing import Any, get_args, get_origin

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

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
    "prefix_problem",
    "resolve_binds",
]

PREFIX_MAX = 64


def prefix_problem(prefix: str) -> str | None:
    """Why ``prefix`` cannot sit in front of a secret, or None when it can.

    The prefix is written verbatim ahead of a resolved credential, usually in
    an HTTP header, so a control character in it is header injection rather
    than a typo, and printable ASCII is all a header value may carry. ``$`` is
    refused outright: the value must hold one reference and nothing shaped
    like another, and a ``$VAR`` in front of it fails the row's own validation,
    losing the whole entry over its bind.
    """
    if not 1 <= len(prefix) <= PREFIX_MAX:
        return f"prefix must be 1-{PREFIX_MAX} characters"
    if any(not " " <= c <= "~" for c in prefix):
        return "prefix must be printable ASCII"
    if "$" in prefix:
        return "prefix must not contain '$'"
    return None


class SecretBind(BaseModel):
    """One place a declared secret is injected: a server's env or header."""

    server: str  # the mcp.json entry key, not the coerced row name
    env: str | None = None
    header: str | None = None
    # Literal text ahead of the secret (``"Bearer "``). Without it a package
    # had to inline ``Bearer ${vault:X}`` in its portable mcp.json, which no
    # other host resolves and which declares nothing for the wizard to ask for.
    prefix: str | None = None

    model_config = {"extra": "forbid"}

    @field_validator("prefix")
    @classmethod
    def _safe_prefix(cls, value: str | None) -> str | None:
        if value is not None and (problem := prefix_problem(value)):
            raise ValueError(problem)
        return value

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

    Every key is optional and some are read on only one path — ``secrets`` is
    an upload's. They are still declared here, so a package borrowing our own
    bundles' manifest shape is not warned about keys it was right to carry.
    """

    secrets: list[PluginSecret] = Field(default_factory=list)
    servers: dict[str, ServerMeta] = Field(default_factory=dict)
    #: Accepted and ignored, in the older list form or the dict form that
    #: carried per-skill icons: a package's skills are the directories it
    #: carries, never a claim its manifest makes.
    skills: list[str] | dict[str, Any] = Field(default_factory=dict)
    #: The plugin's mark when it wears someone else's brand: a site, or the
    #: https address of the image itself.
    icon: str | None = None

    model_config = {"extra": "forbid"}


# (code, path inside the namespace, message)
_Report = Callable[[str, str, str], None]


def _diag(code: str, target: str, message: str, *, at: str = "") -> Diagnostic:
    return Diagnostic(
        level="warning",
        scope="entry" if target else "plugin",
        target=target,
        code=code,
        message=f"extensions.{NAMESPACE}{'.' + at if at else ''}: {message}",
    )


def parse_extension(
    payload: Any, diagnostics: list[Diagnostic] | None = None
) -> LangalphaExtension:
    """Parse the namespace payload (None = absent), keeping every part that parses.

    A key this build does not read is dropped, and so is a list or map entry
    that does not validate (one secret, one bind, one server block), each with
    a warning naming it, while the rest of the block parses. A newer package's
    next contract field then costs an older build that field, not every
    secret, bind and server instruction beside it. Only a payload that is not
    an object is lost whole.
    """
    if payload is None:
        return LangalphaExtension()

    def report(code: str, at: str, message: str) -> None:
        if diagnostics is not None:
            diagnostics.append(_diag(code, "", message, at=at))

    if not isinstance(payload, dict):
        report("extension_invalid", "", "is not an object; ignored")
        return LangalphaExtension()
    fields: dict[str, Any] = {}
    for key, value in _known_keys(LangalphaExtension, payload, "", report).items():
        # Every top-level key has a default and none depends on another, so a
        # bad one is checked, and dropped, on its own.
        try:
            LangalphaExtension.model_validate({key: value})
        except ValidationError as e:
            report("extension_invalid", key, f"{_reasons(e, under=1)}; ignored")
            continue
        fields[key] = value
    return LangalphaExtension.model_validate(fields)


def _known_keys(
    model: type[BaseModel], data: dict[str, Any], at: str, report: _Report
) -> dict[str, Any]:
    """``data`` without the keys ``model`` does not declare, and with each
    nested list or map holding only the entries that validate."""
    kept: dict[str, Any] = {}
    for key, value in data.items():
        where = f"{at}.{key}" if at else key
        field = model.model_fields.get(key)
        if field is None:
            report("extension_key_unknown", where, "unknown key; ignored")
            continue
        nested = _entry_model(field.annotation)
        if nested is not None and isinstance(value, list):
            value = [
                parsed
                for i, item in enumerate(value)
                if (parsed := _entry(nested, item, f"{where}[{i}]", report))
                is not None
            ]
        elif nested is not None and isinstance(value, dict):
            value = {
                name: parsed
                for name, item in value.items()
                if (parsed := _entry(nested, item, f"{where}.{name}", report))
                is not None
            }
        kept[key] = value
    return kept


def _entry_model(annotation: Any) -> type[BaseModel] | None:
    """``M`` for a ``list[M]`` or ``dict[str, M]`` field of models, else None."""
    if get_origin(annotation) not in (list, dict):
        return None
    entry = get_args(annotation)[-1]
    return entry if isinstance(entry, type) and issubclass(entry, BaseModel) else None


def _entry(
    model: type[BaseModel], item: Any, at: str, report: _Report
) -> BaseModel | None:
    """One list or map entry, or None (reported) when it does not validate."""
    if not isinstance(item, dict):
        report("extension_invalid", at, "is not an object; ignored")
        return None
    try:
        return model.model_validate(_known_keys(model, item, at, report))
    except ValidationError as e:
        report("extension_invalid", at, f"{_reasons(e)}; ignored")
        return None


def _reasons(error: ValidationError, *, under: int = 0) -> str:
    """Pydantic's findings on one line, each after the field it is about,
    less the ``under`` leading parts the caller's path already names."""
    parts = []
    for found in error.errors(include_url=False):
        where = ".".join(str(part) for part in found["loc"][under:])
        message = found["msg"].removeprefix("Value error, ")
        parts.append(f"{where}: {message}" if where else message)
    return "; ".join(parts)


@dataclass(frozen=True, slots=True)
class ResolvedBind:
    """One validated bind: which secret lands in which entry's which field."""

    secret_name: str
    plan: McpEntryPlan
    section: str  # "env" or "headers"
    field: str
    prefix: str = ""


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
    One that fails any of those is dropped, and the entry is checked without
    it, so a server that needed the key fails its check rather than landing
    without it. sse entries are legal targets — their
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
                    ResolvedBind(
                        secret.name, plan, "env", bind.env, bind.prefix or ""
                    )
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
                    ResolvedBind(
                        secret.name, plan, "headers", bind.header,
                        bind.prefix or "",
                    )
                )
    return resolved


def materialize_binds(
    extension: LangalphaExtension,
    plans: list[McpEntryPlan],
    granted: Container[str],
    *,
    document_dropped: bool = False,
) -> None:
    """Write ``${vault:NAME}``, behind its prefix, into each granted bind's
    target field.

    A name outside ``granted`` is simply not written, so the entry is checked
    and saved without that credential.

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
        target[bind.field] = f"{bind.prefix}${{vault:{bind.secret_name}}}"


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
