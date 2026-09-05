"""Wire models for the Agent Plugins API.

The install report mirrors the spec's failure ladder: fatal manifest errors
never reach these models (they raise before any write); everything survivable
lands here as a per-component result plus diagnostics, so a partial install is
readable rather than silent.
"""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from ptc_agent.config.core import VaultBlueprint


class Diagnostic(BaseModel):
    """One validation finding, scoped to where the failure isolates."""

    level: Literal["warning", "error"] = "warning"
    # plugin = the whole bundle; mcp = the MCP component; entry = one mcp.json
    # server; skill = one skills/ directory; file = an archive member.
    scope: Literal["plugin", "mcp", "entry", "skill", "file"] = "plugin"
    # The entry key / skill dir / file path the finding is about; "" = whole
    # plugin.
    target: str = ""
    code: str
    message: str
    # Anchor into the Agent Plugins spec (agent-plugins.org) for the rule that
    # produced this finding, when one exists.
    spec_ref: Optional[str] = None


ComponentStatus = Literal[
    "created",
    "exists",
    "skipped",
    "invalid",
    "error",
    "upgradable",
    "updated",
    "deleted",
    "unchanged",
    "detached",
]


class ComponentResult(BaseModel):
    """The outcome for one declared component (an mcp.json entry or a skill)."""

    kind: Literal["mcp", "skill"]
    # The component's identity in the package: the mcp.json key or the
    # skills/ directory name. Stable across renames, so update diffs use it.
    key: str
    # The installed row name (post name-coercion); "" when nothing landed.
    name: str = ""
    renamed: bool = False
    status: ComponentStatus
    reason: str = ""
    warnings: list[str] = Field(default_factory=list)

    @classmethod
    def of(
        cls,
        plan: Any,
        status: ComponentStatus,
        *,
        name: str | None = None,
        reason: str = "",
        warnings: list[str] | None = None,
    ) -> "ComponentResult":
        """Report on ``plan``, taking its identity from the plan itself.

        Hand-assembling kind/key/name/renamed at each site is how ``renamed``
        came to say one thing at install and another after an update for the
        same component; a plan (McpEntryPlan or SkillPlan) is the one source.
        ``name`` overrides only where the landed row name differs from the
        plan's (an update keeps the installed name).
        """
        return cls(
            kind=plan.kind,
            key=plan.key,
            name=(plan.name if name is None else name) or "",
            renamed=plan.renamed,
            status=status,
            reason=reason,
            warnings=list(warnings or []),
        )


class InstallReport(BaseModel):
    components: list[ComponentResult] = Field(default_factory=list)
    diagnostics: list[Diagnostic] = Field(default_factory=list)
    # Vault secrets auto-extracted from embedded literals (spec violation the
    # import path vaults rather than ships).
    secrets_created: list[str] = Field(default_factory=list)
    # Blueprints this package declared that the user still has to fill in the
    # vault, carrying the package's own label and description: the surface
    # that renders them is asking consent for this package's request, and a
    # name several packages declare has a different answer for each of them.
    secrets_required: list[VaultBlueprint] = Field(default_factory=list)
    # Top-level package entries we don't model (README, LICENSE, extension
    # dirs) — reported, not round-tripped by export.
    dropped_files: list[str] = Field(default_factory=list)
    servers_created: int = 0
    skills_created: int = 0

    @property
    def landed_whole(self) -> bool:
        """True when nothing errored, so the package hash may be stamped.

        The hash is the claim "this exact tree is installed"; an ``error``
        component (a failed archive store, a cap refusal) means part of the
        tree is not, and update must be left free to reconcile it.

        ``exists`` counts too, and is the subtler case: it means a row outside
        this plugin holds the name. That is a settled outcome only while the
        other row is, so stamping the hash on it makes a contingent skip
        permanent — delete the conflicting row and no update will ever install
        the component, because the package never looks changed. Every
        remaining status is intrinsic to the package (a schema fault, a
        disallowed command, a reserved builtin name) and re-running reproduces
        it, so those settle honestly.
        """
        return not any(
            c.status in ("error", "exists") for c in self.components
        )


class PluginComponentRef(BaseModel):
    """A component row still owned by the plugin — the detail view's chips."""

    kind: Literal["mcp", "skill"]
    name: str
    key: str


class PluginInfo(BaseModel):
    """One installed plugin (the user_plugins row, without the manifests)."""

    name: str
    version: Optional[str] = None
    description: str = ""
    author: Optional[str] = None
    homepage: Optional[str] = None
    source_type: str
    source_ref: Optional[str] = None
    enabled: bool
    # Set only when the manifest names a vendor whose mark we do not ship, so a
    # null here is the answer "this one's art is a file the frontend already
    # holds", not "this one has none".
    icon_url: Optional[str] = None
    repository: Optional[str] = None
    license: Optional[str] = None
    keywords: list[str] = Field(default_factory=list)
    installed_at: Optional[str] = None
    updated_at: Optional[str] = None
    components: list[PluginComponentRef] = Field(default_factory=list)


class PluginListResponse(BaseModel):
    plugins: list[PluginInfo]
    max_plugins: int
    remaining_slots: int


class InstallResponse(BaseModel):
    """What every endpoint that installs components answers: the plugin as it
    now stands, and what the call just did to it."""

    plugin: PluginInfo
    report: InstallReport


class BindingsResponse(BaseModel):
    """Which declared secrets the bindings step wrote."""

    set: list[str]


class PluginEnabledResponse(BaseModel):
    """The plugin-level switch after a toggle: a state echo, not a report."""

    name: str
    enabled: bool


class UninstalledComponents(BaseModel):
    """Row names an uninstall removed, by kind. Rows the user detached are
    not here: they survive the uninstall by design."""

    servers: list[str]
    skills: list[str]


class UninstallResponse(BaseModel):
    """The receipt for an uninstall: what went with the plugin."""

    ok: bool = True
    deleted: UninstalledComponents


def plugin_row_to_info(
    row: dict[str, Any], *, components: list[PluginComponentRef] | None = None
) -> PluginInfo:
    """Project a user_plugins row (+ manifest metadata) onto the wire model.

    Every field the model carries, because the detail view draws the same
    panel for an installed package and a shipped one: a field this projection
    skips reads to the user as metadata the package did not declare.
    """
    manifest = row.get("manifest") or {}
    author = manifest.get("author") or {}
    return PluginInfo(
        name=row["name"],
        version=row.get("version"),
        description=str(manifest.get("description") or ""),
        author=author.get("name") if isinstance(author, dict) else None,
        homepage=manifest.get("homepage"),
        repository=manifest.get("repository"),
        license=manifest.get("license"),
        keywords=list(manifest.get("keywords") or []),
        source_type=row["source_type"],
        source_ref=row.get("source_ref"),
        enabled=bool(row["enabled"]),
        installed_at=row.get("installed_at"),
        updated_at=row.get("updated_at"),
        components=components or [],
    )
