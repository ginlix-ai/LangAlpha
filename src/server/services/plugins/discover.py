"""Plugin discovery (tree traversal) + vendor-dialect adaptation.

A package is not required to BE a plugin at its root — marketplace repos
(github.com/openai/plugins, cursor/plugins, anthropics/claude-plugins-official)
hold many plugins in subdirectories, each marked by a manifest that is almost
but not quite ours: ``.codex-plugin/plugin.json``, ``.cursor-plugin/
plugin.json``, or ``.claude-plugin/plugin.json`` + a root ``.mcp.json``, none
carrying the ``$schema`` the canonical validator requires. Discovery walks the
tree for those markers; adaptation rewrites one selected plugin's subtree into
canonical shape in memory (hoist + stamp the manifest, rename ``.mcp.json``,
normalize transports, map ``${VAR}`` credential references into declared
``ai.langalpha`` secrets) so the validator and its failure ladder run
unchanged. The stored and exported form is always the canonical one.
"""

import json
import re
from dataclasses import dataclass
from typing import Any

from src.server.models.plugin import Diagnostic
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.extension import NAMESPACE
from src.server.services.plugins.fetch import compose_tree_url
from src.server.services.plugins.paths import split_member
from src.server.services.plugins.schemas import MCP_SCHEMA, PLUGIN_SCHEMA, SCHEMA_URL_RE

# Ordered by priority when one directory carries several manifest dirs.
_VENDOR_DIALECTS = ("agent", "claude", "codex", "cursor")
_VENDOR_DIRS = {f".{d}-plugin": d for d in _VENDOR_DIALECTS}

# A plugin root deeper than this is noise (the example marketplaces nest at
# most two levels); the candidate cap is a runaway guard, not a real limit —
# the archive layer already caps members at 2000.
MAX_ROOT_DEPTH = 6
MAX_CANDIDATES = 200

# A value that is exactly one environment-variable reference, in the vendor
# dialects' ``${VAR}`` / ``${VAR:-default}`` forms. The name must also be a
# legal vault-secret name (VaultBlueprint's rule) or the reference stays
# literal — synthesizing an undeclarable secret would fail the whole install.
_ENV_REF_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]{0,63})(?::-[^}]*)?\}$")


# Where the example marketplaces keep their index, in probe order (cursor and
# anthropic under their manifest dir, openai under .agents/plugins/).
_MARKETPLACE_PATHS = tuple(
    f".{d}-plugin/marketplace.json" for d in _VENDOR_DIALECTS
) + (".agents/plugins/marketplace.json", "marketplace.json")


@dataclass
class PluginCandidate:
    """One discovered plugin root, with best-effort picker metadata."""

    path: str  # subtree root, "" = the archive root itself
    dialect: str  # canonical / agent / claude / codex / cursor / external
    name: str | None = None
    description: str | None = None
    version: str | None = None
    # Set only for marketplace entries whose plugin lives in ANOTHER repo:
    # installing this candidate means fetching that URL, not selecting a
    # subtree of the current archive.
    source_url: str | None = None


def _manifest_preview(raw: bytes) -> tuple[str | None, str | None, str | None]:
    """(name, description, version), best-effort — a broken manifest still
    lists, and fails properly only once actually selected."""
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, None, None
    if not isinstance(doc, dict):
        return None, None, None

    def _str(key: str) -> str | None:
        value = doc.get(key)
        return value if isinstance(value, str) and value else None

    return _str("name"), _str("description"), _str("version")


def _marketplace_entries(files: dict[str, bytes]) -> list[dict[str, Any]]:
    """The plugin index a marketplace repo publishes, or []. Best-effort:
    a malformed index degrades to bare traversal, never to a failure."""
    for path in _MARKETPLACE_PATHS:
        raw = files.get(path)
        if raw is None:
            continue
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(doc, dict) and isinstance(doc.get("plugins"), list):
            return [e for e in doc["plugins"] if isinstance(e, dict)]
    return []


def _relative_source(value: Any) -> str | None:
    """A marketplace source that points inside this repo, normalized."""
    if not isinstance(value, str) or value.startswith("https://"):
        return None
    segments = split_member(value)
    if not segments:
        return None
    return "/".join(segments)


def _external_source(source: Any) -> tuple[str, str | None, str | None] | None:
    """(repo url, path, ref) for a source living in another repo, or None."""
    if isinstance(source, str) and source.startswith("https://"):
        return source, None, None
    if not isinstance(source, dict):
        return None
    kind = source.get("kind") or source.get("source")
    url = source.get("url")
    if kind == "github" and isinstance(source.get("repo"), str):
        url = f"https://github.com/{source['repo']}"
    if kind not in ("git", "git-subdir", "github") or not isinstance(url, str):
        return None
    path = source.get("path") if isinstance(source.get("path"), str) else None
    ref = source.get("ref") if isinstance(source.get("ref"), str) else None
    if ref is None and isinstance(source.get("sha"), str):
        ref = source["sha"]
    return url, path, ref


def _entry_str(entry: dict[str, Any], key: str) -> str | None:
    value = entry.get(key)
    return value if isinstance(value, str) and value else None


def _apply_marketplace(
    files: dict[str, bytes], candidates: list[PluginCandidate]
) -> list[PluginCandidate]:
    """Fold the marketplace index into the discovered candidates.

    In-repo entries enrich the matching candidate's picker metadata (the
    plugin's own manifest wins; the index only fills gaps). Entries whose
    plugin lives in another repo become external candidates carrying the
    composed source URL — unless a same-named in-repo candidate exists (a
    marketplace that vendors a snapshot AND points upstream offers one row,
    the vendored one, which installs without a second fetch).
    """
    entries = _marketplace_entries(files)
    if not entries:
        return candidates
    by_path = {c.path: c for c in candidates}
    local_names = {c.name for c in candidates if c.name}
    external: list[PluginCandidate] = []
    seen_urls: set[str] = set()
    for entry in entries:
        source = entry.get("source")
        rel = _relative_source(source)
        if rel is None and isinstance(source, dict):
            kind = source.get("kind") or source.get("source")
            if kind == "local":
                rel = _relative_source(source.get("path"))
        if rel is not None:
            candidate = by_path.get(rel)
            if candidate is not None:
                candidate.name = candidate.name or _entry_str(entry, "name")
                candidate.description = candidate.description or _entry_str(
                    entry, "description"
                )
            continue
        remote = _external_source(source)
        if remote is None:
            continue
        name = _entry_str(entry, "name")
        if name in local_names:
            continue
        url = compose_tree_url(*remote)
        if url is None or url in seen_urls:
            continue
        seen_urls.add(url)
        external.append(
            PluginCandidate(
                path=remote[1] or "",
                dialect="external",
                name=name,
                description=_entry_str(entry, "description"),
                source_url=url,
            )
        )
    return candidates + external


def discover_plugin_roots(
    files: dict[str, bytes], *, ignore_root: bool = False
) -> list[PluginCandidate]:
    """Every plugin the archive offers, traversal first, then the
    marketplace index's external entries; in-repo candidates path-sorted.

    A root-level plugin wins outright (a plugin repo that happens to vendor
    another plugin in its tree is one plugin, not a marketplace). Nested
    candidates inside another candidate's subtree are dropped for the same
    reason.

    ``ignore_root`` asks the same question with that rule suspended: what
    else would this archive have offered? Some marketplaces put a manifest at
    the root that is a cover page rather than a plugin, carrying a name and a
    description and no components at all, and the rule above then hides the
    ninety-odd real plugins listed beside it. Nothing in the tree separates
    that from a genuine root plugin — only a caller that has already tried
    the root and found it empty can. So the traversal answers both questions
    and decides neither.
    """
    by_root: dict[tuple[str, ...], tuple[int, str, str]] = {}
    for path in files:
        segments = tuple(path.split("/"))
        if segments[-1] != "plugin.json":
            continue
        parent = segments[-2] if len(segments) >= 2 else None
        if parent in _VENDOR_DIRS:
            root, dialect = segments[:-2], _VENDOR_DIRS[parent]
            priority = 1 + _VENDOR_DIALECTS.index(dialect)
        else:
            root, dialect = segments[:-1], "canonical"
            priority = 0
        if len(root) > MAX_ROOT_DEPTH:
            continue
        current = by_root.get(root)
        if current is None or priority < current[0]:
            by_root[root] = (priority, dialect, path)

    root_is_plugin = () in by_root and not ignore_root
    if root_is_plugin:
        by_root = {(): by_root[()]}
    elif ignore_root:
        by_root.pop((), None)

    kept: list[tuple[str, ...]] = []
    kept_set: set[tuple[str, ...]] = set()
    for root in sorted(by_root):
        # sorted() puts an ancestor ahead of its descendants, so containment
        # only has to ask about this root's own prefix chain — at most
        # MAX_ROOT_DEPTH entries — rather than rescanning everything kept so
        # far. The archive that makes the difference is the one where no root
        # contains another, which is also the cheapest one to build.
        if any(root[:i] in kept_set for i in range(len(root))):
            continue
        if len(kept) >= MAX_CANDIDATES:
            # Inside the walk rather than after it: the refusal is the same
            # either way, and an archive that trips it should not first pay for
            # the rest of the scan.
            raise PluginFatal(
                f"the archive contains more than {MAX_CANDIDATES} plugin "
                "candidates"
            )
        kept.append(root)
        kept_set.add(root)

    candidates = []
    for root in kept:
        _priority, dialect, manifest_path = by_root[root]
        name, description, version = _manifest_preview(files[manifest_path])
        candidates.append(
            PluginCandidate(
                path="/".join(root),
                dialect=dialect,
                name=name,
                description=description,
                version=version,
            )
        )
    if root_is_plugin:
        return candidates
    merged = _apply_marketplace(files, candidates)
    # External entries are picker sugar, never worth failing over: clip to
    # the room the cap leaves.
    return merged[:MAX_CANDIDATES]


def select_candidate(
    candidates: list[PluginCandidate], subdir: str | None
) -> PluginCandidate:
    """Resolve discovery to exactly one candidate, or raise.

    No subdir and one candidate: that one. No subdir and several: ambiguous
    (the wizard's chooser). A subdir that matches no candidate is ambiguous
    too — the client needs the fresh candidate list either way, e.g. after a
    marketplace repo moved the plugin. External candidates are never
    selectable here: their content is in another repo, and the chooser
    installs them through their source URL instead.
    """
    local = [c for c in candidates if c.source_url is None]
    if subdir is None:
        if len(candidates) == 1 and local:
            return candidates[0]
        raise PluginAmbiguous(candidates)
    normalized = subdir.strip("/")
    for candidate in local:
        if candidate.path == normalized:
            return candidate
    raise PluginAmbiguous(candidates)


def select_subtree(files: dict[str, bytes], path: str) -> dict[str, bytes]:
    if not path:
        return dict(files)
    prefix = path + "/"
    return {
        p[len(prefix):]: content
        for p, content in files.items()
        if p.startswith(prefix)
    }


def _canonical_schema(doc: dict[str, Any], schema: dict[str, Any]) -> bool:
    """Stamp the canonical $schema unless a canonical one is already
    declared (an unsupported-version claim must keep failing as itself)."""
    declared = doc.get("$schema")
    if isinstance(declared, str) and SCHEMA_URL_RE.match(declared):
        return False
    doc["$schema"] = schema["$id"]
    return True


def _adapt_entry(entry: dict[str, Any]) -> None:
    """Normalize one server entry's transport vocabulary in place."""
    transport = entry.get("type")
    if transport == "http":
        entry["type"] = "streamable-http"
    elif transport is None:
        entry["type"] = "stdio" if "command" in entry else "streamable-http"


def _strip_env_refs(
    entry_key: str, entry: dict[str, Any], refs: dict[str, list[dict[str, str]]]
) -> None:
    """Move pure ``${VAR}`` env/header values out of the portable doc.

    The stripped reference becomes a bind target recorded in ``refs``; mixed
    content ("Bearer ${VAR}") stays literal. env maps bind only on stdio
    entries and headers only on remotes — same rule the bind machinery
    enforces.
    """
    field = "env" if entry.get("type") == "stdio" else "headers"
    mapping = entry.get(field)
    if not isinstance(mapping, dict):
        return
    for target, value in list(mapping.items()):
        match = _ENV_REF_RE.match(value) if isinstance(value, str) else None
        if match is None:
            continue
        del mapping[target]
        refs.setdefault(match.group(1), []).append(
            {"server": entry_key, ("env" if field == "env" else "header"): target}
        )
    if not mapping:
        entry.pop(field, None)


def _synthesize_secrets(
    manifest: dict[str, Any], refs: dict[str, list[dict[str, str]]]
) -> list[str]:
    """Declare the stripped references as ``ai.langalpha`` secrets.

    Returns the declared names. Skips synthesis entirely when the manifest
    already carries the namespace (never second-guess an explicit contract)
    or when ``extensions`` exists but is not an object.
    """
    if not refs:
        return []
    extensions = manifest.setdefault("extensions", {})
    if not isinstance(extensions, dict) or NAMESPACE in extensions:
        return []
    extensions[NAMESPACE] = {
        "secrets": [
            {
                "name": var,
                "label": var,
                "description": "",
                "bind": binds,
            }
            for var, binds in refs.items()
        ]
    }
    return list(refs)


def _take_mcp_document(
    out: dict[str, bytes], manifest: dict[str, Any], vendor_dir: str | None
) -> dict[str, Any] | bytes | None:
    """Find the package's MCP document and take it out of the tree.

    Vendors declare servers four ways: a root ``.mcp.json`` with no manifest
    key at all, a manifest key naming the file, a manifest key carrying the
    servers inline, and the same file inside the vendor directory. Only the
    first was read, so a plugin using any of the others installed with none
    of its servers — and the only hint was a warning that ``mcpServers`` was
    an unexpected key. Returns the parsed document, or the raw bytes when
    they don't parse (the validator owns that report), or None.
    """
    declared = manifest.pop("mcpServers", None)
    if isinstance(declared, dict):
        # The inline form carries the server map itself, not a whole document.
        return {"mcpServers": declared}
    paths: list[str] = []
    if isinstance(declared, str) and declared.strip():
        named = declared.strip()
        paths.append(named[2:] if named.startswith("./") else named)
    paths.append(".mcp.json")
    if vendor_dir:
        paths += [f"{vendor_dir}/.mcp.json", f"{vendor_dir}/mcp.json"]
    # Last, so a vendor-specific document wins over the shared one.
    paths.append("mcp.json")
    for path in paths:
        raw = out.pop(path, None)
        if raw is None:
            continue
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw
        return parsed if isinstance(parsed, dict) else raw
    return None


def adapt_dialect(
    files: dict[str, bytes], dialect: str
) -> tuple[dict[str, bytes], list[Diagnostic]]:
    """Rewrite a package into canonical shape, in memory.

    Vendor layouts are hoisted out of their ``.<vendor>-plugin`` directory;
    a canonical layout is left where it is but still gets the leniencies a
    vendor manifest gets, because the alternative is refusing a package over
    a ``$schema`` line that four layouts out of five never have to write.

    Unparseable manifests are passed through unadapted so the canonical
    validator reports them on its own ladder rung.
    """
    canonical = dialect == "canonical"
    vendor_dir = None if canonical else f".{dialect}-plugin"
    manifest_path = "plugin.json" if canonical else f"{vendor_dir}/plugin.json"
    out = dict(files)
    diagnostics: list[Diagnostic] = []
    if not canonical:
        diagnostics.append(
            Diagnostic(
                scope="plugin",
                code="dialect_adapted",
                message=(
                    f"adapted from the {vendor_dir}/plugin.json layout into "
                    "the Agent Plugins canonical form"
                ),
            )
        )

    manifest_raw = out.pop(manifest_path)
    manifest: dict[str, Any] | None = None
    try:
        parsed = json.loads(manifest_raw.decode("utf-8"))
        if isinstance(parsed, dict):
            manifest = parsed
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    if manifest is None:
        out["plugin.json"] = manifest_raw
        return out, diagnostics

    if _canonical_schema(manifest, PLUGIN_SCHEMA) and canonical:
        diagnostics.append(
            Diagnostic(
                scope="plugin",
                target="plugin.json",
                code="schema_assumed",
                message=(
                    "plugin.json declares no $schema; read as Agent Plugins "
                    "1.0.0"
                ),
            )
        )
    if isinstance(manifest.get("author"), str):
        manifest["author"] = {"name": manifest["author"]}

    refs: dict[str, list[dict[str, str]]] = {}
    mcp_doc = _take_mcp_document(out, manifest, vendor_dir)
    if isinstance(mcp_doc, bytes):
        out["mcp.json"] = mcp_doc
    elif mcp_doc is not None:
        stamped = _canonical_schema(mcp_doc, MCP_SCHEMA)
        if stamped and canonical:
            diagnostics.append(
                Diagnostic(
                    scope="mcp",
                    target="mcp.json",
                    code="schema_assumed",
                    message=(
                        "mcp.json declares no $schema; read as Agent Plugins "
                        "1.0.0"
                    ),
                )
            )
        # A document that claims canonical conformance is taken at its word,
        # so a genuinely wrong one still reports as wrong. One that claims
        # nothing gets the vendor vocabulary normalized instead.
        if stamped or not canonical:
            servers = mcp_doc.get("mcpServers")
            if isinstance(servers, dict):
                for key, entry in servers.items():
                    if isinstance(entry, dict):
                        _adapt_entry(entry)
                        _strip_env_refs(key, entry, refs)
        out["mcp.json"] = json.dumps(mcp_doc, indent=2).encode("utf-8")

    for var in _synthesize_secrets(manifest, refs):
        targets = ", ".join(
            f"{b['server']}.{b.get('header') or b.get('env')}"
            for b in refs[var]
        )
        diagnostics.append(
            Diagnostic(
                scope="plugin",
                target=var,
                code="env_ref_mapped",
                message=(
                    f"${{{var}}} reference on {targets} mapped to a declared "
                    "vault secret; set it in the bindings step"
                ),
            )
        )

    out["plugin.json"] = json.dumps(manifest, indent=2).encode("utf-8")
    return out, diagnostics
