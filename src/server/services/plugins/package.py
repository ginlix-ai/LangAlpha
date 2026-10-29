"""The pure phase of an install: extract, discover, validate, write nothing.

Everything a fatal problem could refuse is decided here, before the first
write — a bad manifest or an ``ai.langalpha`` extension error never reaches
the database. What survives is a ``ValidatedPackage``: the entry, skill and
subagent plans the fan-outs consume, plus the tree hash the update path diffs
against.
"""

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from functools import wraps
from typing import Any

from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.agents import (
    AgentPlan,
    collect_agents,
    is_agent_file,
)
from src.server.services.plugins.archive import extract_plugin_archive
from src.server.services.plugins.bare_skills import wrap_skill_tree
from src.server.services.plugins.discover import (
    adapt_dialect,
    discover_plugin_roots,
    select_candidate,
    select_subtree,
)
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.brand_icons import MAX_INLINE_ICON_BYTES
from src.server.services.plugins.extension import (
    NAMESPACE,
    LangalphaExtension,
    apply_server_metadata,
    parse_extension,
    resolve_binds,
)
from src.server.services.plugins.icons import vendored_icon
from src.server.services.plugins.manifest import (
    SPEC_URL,
    manifest_extension,
    validate_manifest,
)
from src.server.services.plugins.mcp import McpEntryPlan, validate_mcp_document
from src.server.services.plugins.skills import SkillPlan, collect_skills

from ptc_agent.config.core import VaultBlueprint
from ptc_agent.core.mcp_sanitize import (
    VAULT_REF_RE,
    iter_arg_flag_pairs,
    looks_like_secret,
)

logger = logging.getLogger(__name__)

_MODELED_ROOTS = ("plugin.json", "mcp.json", "skills", "agents")

# Every copy of a mark rides in the stored manifest, which every listing reads,
# so the copies are capped together as well as one by one: a manifest naming
# one small image under thousands of server keys would otherwise store it
# thousands of times. Room for a dozen of the largest marks.
MAX_INLINED_ICON_CHARS = 1024 * 1024


def _unreadable_is_fatal[**P, R](read: Callable[P, R]) -> Callable[P, R]:
    """Refuse a package that breaks a parser, as any unreadable one is.

    The readers catch the failures they expect, but the parsers under them
    give up in more ways than a reader names: nesting past the recursion
    limit, a YAML date that is no date, a number too long to convert, a name
    no encoder takes. Each is the package's fault, so it answers 422 like any
    other refusal, not 500, nor 409 on a route that reads a ValueError as a
    conflict.
    """

    @wraps(read)
    def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return read(*args, **kwargs)
        except (RecursionError, ValueError) as e:
            logger.warning("[plugins] a package broke a parser", exc_info=True)
            message = (
                "the package nests a document deeper than it can be read"
                if isinstance(e, RecursionError)
                else f"the package holds a value that cannot be read: {e}"
            )
            raise PluginFatal(
                message,
                diagnostics=[
                    Diagnostic(
                        level="error",
                        scope="plugin",
                        code="unreadable",
                        message=message,
                    )
                ],
            ) from e

    return guarded


def _is_secret_literal(key: str, value: Any) -> bool:
    """The same verdict ``plan_vault_extraction`` reaches on one field.

    Deliberately identical, including the ``search`` (a ``Bearer ${vault:X}``
    template is a reference, not a literal): the two must agree, or the scrub
    blanks a benign value the extractor left alone and export loses it.
    """
    return (
        isinstance(value, str)
        and bool(value.strip())
        and not VAULT_REF_RE.search(value)
        and looks_like_secret(key, value)
    )


def _scrub_entry_secrets(entry: dict[str, Any]) -> dict[str, Any]:
    """One mcpServers entry with its credential literals emptied in place."""
    clean = dict(entry)
    for field in ("env", "headers"):
        mapping = clean.get(field)
        if isinstance(mapping, dict):
            clean[field] = {
                k: ("" if _is_secret_literal(str(k), v) else v)
                for k, v in mapping.items()
            }
    args = clean.get("args")
    if isinstance(args, list):
        scrubbed_args = []
        for arg in args:
            flag, sep, value = (
                arg.partition("=") if isinstance(arg, str) else ("", "", "")
            )
            scrubbed_args.append(
                f"{flag}=" if sep and _is_secret_literal(flag, value) else arg
            )
        # The ``--token SECRET`` pair, on the shared scanner the extractor uses
        # so the two stay in step: the value element is emptied rather than
        # dropped, since removing it would shift every later arg by one.
        for i, _flag, _value in iter_arg_flag_pairs(args):
            scrubbed_args[i] = ""
        clean["args"] = scrubbed_args
    return clean


def _scrub_document_secrets(document: dict[str, Any]) -> dict[str, Any]:
    """Blank credential-looking literals out of the document we persist.

    The spec forbids embedded secrets, and the import path already lifts any
    it finds into the vault — but it rewrites the entry plan's own copy of the
    config, not this document. Left alone, the literal would survive as a
    second plaintext copy in ``user_plugins.mcp_document`` and come straight
    back out of ``/export``, defeating the extraction for the one case it
    exists to cover. The key is kept so a re-import still asks for the value.
    """
    servers = document.get("mcpServers")
    if not isinstance(servers, dict):
        return document
    return {
        **document,
        "mcpServers": {
            key: _scrub_entry_secrets(entry) if isinstance(entry, dict) else entry
            for key, entry in servers.items()
        },
    }


@dataclass(frozen=True, slots=True)
class ValidatedPackage:
    """Everything install needs, computed before any write."""

    manifest: dict[str, Any]
    mcp_document: dict[str, Any] | None
    # mcp.json was in the tree but did not survive document-level validation.
    # Distinct from a None document with no file: the plugin still means to
    # ship servers, so update must not read "no entries" as "all removed".
    mcp_document_invalid: bool
    entry_plans: list[McpEntryPlan]
    skill_plans: list[SkillPlan]
    extension: LangalphaExtension
    diagnostics: list[Diagnostic]
    dropped_files: list[str]
    content_hash: str
    # The tree had no plugin.json and the manifest is ours (``bare_skills``),
    # so its name is a guess from where the package came from rather than a
    # claim the package makes.
    synthesized: bool = False
    agent_plans: list[AgentPlan] = dataclass_field(default_factory=list)

    @property
    def name(self) -> str:
        return self.manifest["name"]

    def renamed(self, name: str) -> "ValidatedPackage":
        """The same package under ``name``. Synthesized manifests only.

        Update needs this: the guess depends on how many skills the tree holds
        and on the link it came from, so a collection that grew from one skill
        to two would otherwise re-validate under another name and be refused
        as a different plugin.
        """
        if not self.synthesized:
            raise ValueError("only a synthesized manifest can be renamed")
        return replace(self, manifest={**self.manifest, "name": name})

    @property
    def version(self) -> str | None:
        version = self.manifest.get("version")
        return str(version) if version is not None else None


_AGENTS_READ_MARK = b"\x00read:agents/1\x00"


def _tree_hash(files: dict[str, bytes]) -> str:
    """Deterministic identity of the selected, adapted subtree.

    Update's no-op detection compares this — hashing the wire bytes instead
    would re-reconcile a marketplace plugin on every unrelated sibling
    commit (and on every gzip-timestamp wobble of the same ref).
    """
    digest = hashlib.sha256()
    # An install by a build that predates plugin subagents stored the hash of
    # a reading that dropped agents/, over these same bytes. The mark makes
    # that install read as out of date, so Update lands its subagents. It
    # opens with a NUL, which no member path holds, so no file can pose as it.
    if any(is_agent_file(path) for path in files):
        digest.update(_AGENTS_READ_MARK)
    for path in sorted(files):
        content = files[path]
        digest.update(path.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _inline_icons(
    manifest: dict[str, Any], files: dict[str, bytes], diagnostics: list[Diagnostic]
) -> dict[str, Any]:
    """``manifest`` with each ``./`` icon replaced by the image it names.

    The installed plugin keeps its manifest and not its files, so a mark the
    package carries is read now or never. One that is missing, not a usable
    image, or past what the manifest has room for is dropped with a warning,
    and the next source in line draws.
    """
    namespace = manifest_extension(manifest, NAMESPACE)
    if namespace is None:
        return manifest
    marks: dict[str, str | None] = {}
    room = MAX_INLINED_ICON_CHARS
    crowded: list[str] = []

    def without_icon(meta: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in meta.items() if k != "icon"}

    def with_icon(meta: dict[str, Any], where: str) -> dict[str, Any]:
        nonlocal room
        value = meta.get("icon")
        if not isinstance(value, str) or not value.startswith("./"):
            return meta
        if value not in marks:
            marks[value] = mark = vendored_icon(value, files)
            if mark is None:
                diagnostics.append(
                    Diagnostic(
                        level="warning",
                        scope="plugin",
                        code="icon_unusable",
                        message=(
                            f"extensions.{NAMESPACE}.{where}icon: {value!r} is not "
                            "a PNG, ICO, GIF, JPEG, WebP or SVG in the package, at "
                            f"most {MAX_INLINE_ICON_BYTES // 1024} KB and at least "
                            "32px across"
                        ),
                    )
                )
        mark = marks[value]
        if mark is None:
            return without_icon(meta)
        if len(mark) > room:
            crowded.append(where)
            return without_icon(meta)
        room -= len(mark)
        return {**meta, "icon": mark}

    out = with_icon(namespace, "")
    servers = out.get("servers")
    if isinstance(servers, dict):
        out = {
            **out,
            "servers": {
                key: with_icon(meta, f"servers.{key}.") if isinstance(meta, dict) else meta
                for key, meta in servers.items()
            },
        }
    if crowded:
        diagnostics.append(
            Diagnostic(
                level="warning",
                scope="plugin",
                code="icon_unusable",
                message=(
                    f"extensions.{NAMESPACE}.{crowded[0]}icon"
                    + (f" and {len(crowded) - 1} more" if len(crowded) > 1 else "")
                    + ": the package's icons come to more than "
                    f"{MAX_INLINED_ICON_CHARS // 1024} KB together, so "
                    + ("these are" if len(crowded) > 1 else "this one is")
                    + " left out"
                ),
            )
        )
    return {**manifest, "extensions": {**manifest["extensions"], NAMESPACE: out}}


def _links_under(diagnostics: list[Diagnostic], root: str) -> list[Diagnostic]:
    """Archive diagnostics for one plugin's subtree, re-anchored to its root."""
    if not root:
        return list(diagnostics)
    prefix = root.rstrip("/") + "/"
    return [
        d.model_copy(update={"target": d.target[len(prefix):]})
        for d in diagnostics
        if d.target.startswith(prefix)
    ]


def _report_empty(
    tree: dict[str, bytes],
    root: str,
    subdir: str | None,
    dropped: list[str],
    diagnostics: list[Diagnostic],
) -> None:
    """Account for a package that installs nothing. Raises for a better one.

    A valid manifest over an empty tree is not an error at any rung of the
    spec's ladder, so it used to arrive as a plain success: a card in the
    list, a plugin slot spent, and nothing whatsoever to run. Two real shapes
    reach here. The first is a marketplace whose root manifest is a cover
    page, where the plugins the user actually wants are listed right beside
    it and the chooser already knows how to offer them — so it gets offered,
    but only when a person is waiting on the answer (an explicit ``subdir``
    is a decision already made, and re-asking would loop). The second is a
    package built for something we are not, an editor extension or a hosted
    app, where there is no better candidate and the only useful thing left is
    to say so.
    """
    alternatives = [
        c
        for c in discover_plugin_roots(tree, ignore_root=True)
        if c.path != root or c.source_url is not None
    ]
    if subdir is None and alternatives:
        raise PluginAmbiguous(alternatives, fallback_path=root)
    held = (
        f" What it does contain ({', '.join(dropped)}) is not a component "
        "type this client installs."
        if dropped
        else ""
    )
    diagnostics.append(
        Diagnostic(
            level="warning",
            scope="plugin",
            code="no_components",
            message=(
                "this package declares no MCP servers and carries no skills "
                f"or subagents, so installing it adds nothing you can run.{held}"
            ),
            spec_ref=SPEC_URL,
        )
    )


@dataclass(frozen=True, slots=True)
class SelectedPlugin:
    """One plugin chosen out of an archive tree and read in canonical shape,
    before anything is planned for it."""

    files: dict[str, bytes]
    root: str
    synthesized: bool
    manifest: dict[str, Any]
    mcp_document: dict[str, Any] | None
    mcp_document_invalid: bool
    entry_plans: list[McpEntryPlan]
    extension: LangalphaExtension
    diagnostics: list[Diagnostic]

    @property
    def content_hash(self) -> str:
        """The hash install records once this tree has landed, which the
        catalog listing compares against to tell an installed plugin is out
        of date. A synthesized plugin.json is derived from the rest, and
        leaving it out keeps ``renamed`` from reading as a content change."""
        return _tree_hash(
            {k: v for k, v in self.files.items() if k != "plugin.json"}
            if self.synthesized
            else self.files
        )


@_unreadable_is_fatal
def select_plugin_files(
    tree: dict[str, bytes],
    *,
    subdir: str | None = None,
    name_hint: str | None = None,
) -> SelectedPlugin:
    """Choose the plugin ``subdir`` names in ``tree`` and read it as install
    does. Raises PluginFatal or PluginAmbiguous, as validate_package does.

    The folder ``subdir`` names is read as an archive of its own: the plugin
    at it, else the one plugin below it, else its skills and a top-level
    ``agents/`` folder wrapped in a synthesized manifest named after ``name_hint`` (the folder or repository
    it came from). A folder with a manifest anywhere in it is never read as
    skills. The Discover listing reads an in-repo entry by passing its folder
    as the tree, so what it shows is what install takes; it catches around
    the call, where install lets the refusal through.
    """
    root = (subdir or "").strip("/")
    scope = select_subtree(tree, root)
    # A named folder that holds several plugins, or none (the plugin moved),
    # leaves the chooser to offer the archive's plugins, with root-wins
    # suspended: naming a path already passed over the root.
    candidates = discover_plugin_roots(scope)
    if candidates:
        try:
            candidate = select_candidate(candidates, None)
        except PluginAmbiguous:
            if root:
                raise PluginAmbiguous(
                    discover_plugin_roots(tree, ignore_root=True)
                ) from None
            raise
        files, diagnostics = adapt_dialect(
            select_subtree(scope, candidate.path), candidate.dialect
        )
        root = "/".join(p for p in (root, candidate.path) if p)
    else:
        wrapped = wrap_skill_tree(scope, name_hint=name_hint)
        if wrapped is None:
            if root and (offered := discover_plugin_roots(tree, ignore_root=True)):
                raise PluginAmbiguous(offered)
            where = f"under {root!r}" if root else "in the archive"
            raise PluginFatal(
                f"no plugin found {where} (looked for plugin.json, the "
                ".claude-plugin/.codex-plugin/.cursor-plugin manifest layouts "
                "and folders holding a SKILL.md, at every level, and an agents/ "
                "folder at the top)",
                diagnostics=[
                    Diagnostic(
                        level="error",
                        scope="plugin",
                        code="missing_manifest",
                        message=f"no plugin manifest, SKILL.md or agents/ folder {where}",
                    )
                ],
            )
        files, diagnostics = wrapped

    manifest, manifest_diags = validate_manifest(files["plugin.json"])
    diagnostics = diagnostics + manifest_diags
    manifest = _inline_icons(manifest, files, diagnostics)

    mcp_document: dict[str, Any] | None = None
    entry_plans: list[McpEntryPlan] = []
    if "mcp.json" in files:
        mcp_document, entry_plans, mcp_diags = validate_mcp_document(
            files["mcp.json"], plugin_schema=manifest.get("$schema")
        )
        diagnostics.extend(mcp_diags)
    extension = parse_extension(
        manifest_extension(manifest, NAMESPACE), diagnostics
    )
    return SelectedPlugin(
        files=files,
        root=root,
        synthesized=not candidates,
        manifest=manifest,
        mcp_document=mcp_document,
        mcp_document_invalid="mcp.json" in files and mcp_document is None,
        entry_plans=entry_plans,
        extension=extension,
        diagnostics=diagnostics,
    )


@_unreadable_is_fatal
def validate_package(
    raw: bytes, *, subdir: str | None = None, name_hint: str | None = None
) -> ValidatedPackage:
    """The pure phase: extract + discover + validate everything, write nothing.

    Raises PluginFatal on any failure past the tolerated warns, and
    PluginAmbiguous when the archive holds several plugins and ``subdir``
    doesn't settle which one.
    """
    tree, archive_diags = extract_plugin_archive(raw, subdir=subdir)
    plugin = select_plugin_files(tree, subdir=subdir, name_hint=name_hint)
    files, entry_plans, extension = plugin.files, plugin.entry_plans, plugin.extension
    # A marketplace repo's other plugins are not this install's business, so
    # only the skipped links inside the chosen subtree are reported.
    diagnostics = _links_under(archive_diags, plugin.root) + plugin.diagnostics
    apply_server_metadata(
        extension,
        entry_plans,
        document_dropped=plugin.mcp_document_invalid,
        diagnostics=diagnostics,
    )
    # Report only. The refs themselves are written by the async install and
    # update paths, which can read the vault and so can tell a secret this
    # package introduces from one the user already holds for something else.
    resolve_binds(
        extension,
        entry_plans,
        document_dropped=plugin.mcp_document_invalid,
        diagnostics=diagnostics,
    )

    skill_plans, skill_diags = collect_skills(files)
    diagnostics.extend(skill_diags)
    agent_plans, agent_diags = collect_agents(files)
    diagnostics.extend(agent_diags)

    dropped = sorted(
        {
            path.split("/", 1)[0]
            for path in files
            if path.split("/", 1)[0] not in _MODELED_ROOTS
        }
    )
    if not entry_plans and not skill_plans and not agent_plans:
        _report_empty(tree, plugin.root, subdir, dropped, diagnostics)
    return ValidatedPackage(
        manifest=plugin.manifest,
        mcp_document=(
            None
            if plugin.mcp_document is None
            else _scrub_document_secrets(plugin.mcp_document)
        ),
        mcp_document_invalid=plugin.mcp_document_invalid,
        entry_plans=entry_plans,
        skill_plans=skill_plans,
        extension=extension,
        diagnostics=diagnostics,
        dropped_files=dropped,
        content_hash=plugin.content_hash,
        synthesized=plugin.synthesized,
        agent_plans=agent_plans,
    )


async def pending_secret_declarations(
    user_id: str, package: ValidatedPackage
) -> list[VaultBlueprint]:
    """Declared blueprints the vault doesn't hold yet.

    The declarations as *this* package wrote them, never the merged vault
    catalog. One credential name can be declared by several packages and by
    the config tier, each with its own label and description; the request is
    about one of them, so it has to be put in the words of whoever made it. Projected to the blueprint fields because
    ``bind`` is server wiring, not something a consent screen asks about.

    Install refuses until every declared key has a value, so after one this
    is empty. An update asks only for the keys its changed servers use, so a
    key a new version declares for nothing it touches is reported here.
    """
    existing = set(await get_user_secret_names(user_id))
    return [
        VaultBlueprint(**s.model_dump(exclude={"bind"}))
        for s in package.extension.secrets
        if s.name not in existing
    ]
