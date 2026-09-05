"""The pure phase of an install: extract, discover, validate, write nothing.

Everything a fatal problem could refuse is decided here, before the first
write — a bad manifest or an ``ai.langalpha`` extension error never reaches
the database. What survives is a ``ValidatedPackage``: the entry and skill
plans the fan-outs consume, plus the tree hash the update path diffs against.
"""

import hashlib
from dataclasses import dataclass
from typing import Any

from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.archive import extract_plugin_archive
from src.server.services.plugins.discover import (
    PluginCandidate,
    adapt_dialect,
    discover_plugin_roots,
    select_candidate,
    select_subtree,
)
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.extension import (
    NAMESPACE,
    LangalphaExtension,
    apply_server_metadata,
    parse_extension,
    resolve_binds,
)
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

_MODELED_ROOTS = ("plugin.json", "mcp.json", "skills")


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

    @property
    def name(self) -> str:
        return self.manifest["name"]

    @property
    def version(self) -> str | None:
        version = self.manifest.get("version")
        return str(version) if version is not None else None


def _tree_hash(files: dict[str, bytes]) -> str:
    """Deterministic identity of the selected, adapted subtree.

    Update's no-op detection compares this — hashing the wire bytes instead
    would re-reconcile a marketplace plugin on every unrelated sibling
    commit (and on every gzip-timestamp wobble of the same ref).
    """
    digest = hashlib.sha256()
    for path in sorted(files):
        content = files[path]
        digest.update(path.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


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
    candidate: PluginCandidate,
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
        if c.path != candidate.path or c.source_url is not None
    ]
    if subdir is None and alternatives:
        raise PluginAmbiguous(alternatives, fallback_path=candidate.path)
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
                "this package declares no MCP servers and carries no skills, "
                f"so installing it adds nothing you can run.{held}"
            ),
            spec_ref=SPEC_URL,
        )
    )


def validate_package(raw: bytes, *, subdir: str | None = None) -> ValidatedPackage:
    """The pure phase: extract + discover + validate everything, write nothing.

    Raises PluginFatal on any failure past the tolerated warns, and
    PluginAmbiguous when the archive holds several plugins and ``subdir``
    doesn't settle which one.
    """
    tree, archive_diags = extract_plugin_archive(raw)
    # Root-wins decides which plugin an archive means when the caller did not
    # say. A caller naming a path has already decided, and leaving the rule on
    # made every nested plugin of a root-plugin repo unreachable: the chooser
    # could list the ninety-one plugins under a marketplace's cover page and
    # then refuse every path it had just offered.
    candidates = discover_plugin_roots(
        tree, ignore_root=bool(subdir and subdir.strip("/"))
    )
    if not candidates:
        raise PluginFatal(
            "no plugin found in the archive (looked for plugin.json and the "
            ".claude-plugin/.codex-plugin/.cursor-plugin manifest layouts at "
            "every level)",
            diagnostics=[
                Diagnostic(
                    level="error",
                    scope="plugin",
                    code="missing_manifest",
                    message="no plugin manifest anywhere in the archive",
                )
            ],
        )
    candidate = select_candidate(candidates, subdir)
    files, diagnostics = adapt_dialect(
        select_subtree(tree, candidate.path), candidate.dialect
    )
    # A marketplace repo's other plugins are not this install's business, so
    # only the skipped links inside the chosen subtree are reported.
    diagnostics = _links_under(archive_diags, candidate.path) + diagnostics

    manifest, manifest_diags = validate_manifest(files["plugin.json"])
    diagnostics = diagnostics + manifest_diags

    mcp_document: dict[str, Any] | None = None
    entry_plans: list[McpEntryPlan] = []
    mcp_document_invalid = False
    if "mcp.json" in files:
        mcp_document, entry_plans, mcp_diags = validate_mcp_document(
            files["mcp.json"], plugin_schema=manifest.get("$schema")
        )
        diagnostics.extend(mcp_diags)
        mcp_document_invalid = mcp_document is None
        if mcp_document is not None:
            mcp_document = _scrub_document_secrets(mcp_document)

    extension = parse_extension(
        manifest_extension(manifest, NAMESPACE), diagnostics
    )
    apply_server_metadata(
        extension,
        entry_plans,
        document_dropped=mcp_document_invalid,
        diagnostics=diagnostics,
    )
    # Report only. The refs themselves are written by the async install and
    # update paths, which can read the vault and so can tell a secret this
    # package introduces from one the user already holds for something else.
    resolve_binds(
        extension,
        entry_plans,
        document_dropped=mcp_document_invalid,
        diagnostics=diagnostics,
    )

    skill_plans, skill_diags = collect_skills(files)
    diagnostics.extend(skill_diags)

    dropped = sorted(
        {
            path.split("/", 1)[0]
            for path in files
            if path.split("/", 1)[0] not in _MODELED_ROOTS
        }
    )
    if not entry_plans and not skill_plans:
        _report_empty(tree, candidate, subdir, dropped, diagnostics)
    return ValidatedPackage(
        manifest=manifest,
        mcp_document=mcp_document,
        mcp_document_invalid=mcp_document_invalid,
        entry_plans=entry_plans,
        skill_plans=skill_plans,
        extension=extension,
        diagnostics=diagnostics,
        dropped_files=dropped,
        content_hash=_tree_hash(files),
    )


async def pending_secret_declarations(
    user_id: str, package: ValidatedPackage
) -> list[VaultBlueprint]:
    """Declared blueprints the vault doesn't hold yet — the bindings step.

    The declarations as *this* package wrote them, never the merged vault
    catalog. One credential name can be declared by several packages and by
    the config tier, each with its own label and description; the bindings
    step is asking about one of them, so it has to put the request in the
    words of whoever made it. Projected to the blueprint fields because
    ``bind`` is server wiring, not something a consent screen asks about.

    Install and update return the same report shape, so both answer this the
    same way: a package whose credentials are still unset is exactly as
    unfinished after an update as it was after the first install.
    """
    existing = set(await get_user_secret_names(user_id))
    return [
        VaultBlueprint(**s.model_dump(exclude={"bind"}))
        for s in package.extension.secrets
        if s.name not in existing
    ]
