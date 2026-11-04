"""What may leave a workspace as a served file, and which bytes are text.

The file routes, the share routes and the artifact versions all judge a path
and redact a body by these rules. They sit below ``app/`` so the agent's own
tools and the artifact services read the same rules without importing a
router.
"""

from __future__ import annotations

from dataclasses import dataclass

from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    ALWAYS_HIDDEN_BASENAMES as _SHARED_BASENAMES,
    ALWAYS_HIDDEN_DIR_NAMES,
    ALWAYS_HIDDEN_PATH_SEGMENTS,
    ALWAYS_HIDDEN_SUFFIXES,
    HIDDEN_DIR_NAMES,
    TRANSIENT_ROOT_PREFIX,
    SandboxLayout,
    is_agent_notes_path,
)

# Derived from shared constants (source of truth: ptc_agent.core.paths)
SYSTEM_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(AGENT_SYSTEM_DIRS))
_HIDDEN_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(HIDDEN_DIR_NAMES))
_ALWAYS_HIDDEN_SEGMENTS = ALWAYS_HIDDEN_PATH_SEGMENTS
_ALWAYS_HIDDEN_BASENAMES = _SHARED_BASENAMES + (".file_sync_marker",)
_ALWAYS_HIDDEN_SUFFIXES = ALWAYS_HIDDEN_SUFFIXES

_ALWAYS_HIDDEN_DIR_SEGMENTS = tuple(f"/{d}/" for d in ALWAYS_HIDDEN_DIR_NAMES)

# Hidden only to keep a listing tidy, and where a page keeps its libraries and
# build output. The rest of the always-hidden folders hold a repository's
# history, an environment and tool caches, which no page needs and which can
# carry credentials.
PAGE_LIBRARY_DIR_NAMES: frozenset[str] = frozenset(
    {"node_modules", "vendor", ".next", ".nuxt"}
)
_UNLOADABLE_DIR_SEGMENTS = tuple(
    f"/{d}/" for d in sorted(ALWAYS_HIDDEN_DIR_NAMES - PAGE_LIBRARY_DIR_NAMES)
)


def is_hidden_path(client_path: str) -> bool:
    if client_path == SandboxLayout.INTERNAL_DIR:
        return True
    return any(client_path.startswith(prefix) for prefix in _HIDDEN_DIR_PREFIXES)


def _is_transient_path(normalized: str) -> bool:
    # A transfer's partial or staged file, whose bytes are not yet the file
    # they will become; one a user edits or removes fails the transfer.
    return normalized.rfind("/") == 0 and normalized[1:].startswith(
        TRANSIENT_ROOT_PREFIX
    )


def is_always_hidden_path(client_path: str) -> bool:
    normalized = f"/{client_path.lstrip('/')}"

    if _is_transient_path(normalized):
        return True

    if normalized.endswith(_ALWAYS_HIDDEN_BASENAMES):
        return True

    if normalized.endswith(_ALWAYS_HIDDEN_SUFFIXES):
        return True

    if any(seg in normalized for seg in _ALWAYS_HIDDEN_SEGMENTS):
        return True

    if any(seg in normalized for seg in _ALWAYS_HIDDEN_DIR_SEGMENTS):
        return True

    return False


def is_agent_machinery_path(client_path: str) -> bool:
    """True for what no page has a use for and an owner's page must not read.

    The agent's own directories hold its tool wrappers and the user's profile,
    memory and holdings, which the file mount links into every workspace
    folder, and a page that runs a stranger's script has the reach of whatever
    this lets through.
    """
    return (
        _is_transient_path(f"/{client_path.lstrip('/')}")
        or is_hidden_path(client_path)
        or client_path.startswith(SYSTEM_DIR_PREFIXES)
    )


def page_may_load(client_path: str) -> bool:
    """Whether the owner's view of a page, and what the page publishes, may
    hold this file: anything but the agent's machinery and the always-hidden
    files, except the library folders (``PAGE_LIBRARY_DIR_NAMES``) a page
    loads its scripts and styles from."""
    if is_agent_machinery_path(client_path):
        return False
    normalized = f"/{client_path.lstrip('/')}"
    return not (
        normalized.endswith(_ALWAYS_HIDDEN_BASENAMES)
        or normalized.endswith(_ALWAYS_HIDDEN_SUFFIXES)
        or any(seg in normalized for seg in _ALWAYS_HIDDEN_SEGMENTS)
        or any(seg in normalized for seg in _UNLOADABLE_DIR_SEGMENTS)
    )


def is_text_content_type(content_type: str) -> bool:
    """True for content types whose bytes should be redacted / theme-injected."""
    ct = content_type.split(";", 1)[0].strip().lower()
    return (
        ct.startswith("text/")
        or ct in ("application/json", "application/xml", "image/svg+xml")
        or ct.endswith("+json")
        or ct.endswith("+xml")
    )


def is_utf8(data: bytes) -> bool:
    """True when bytes decode cleanly as UTF-8 (i.e. text, whatever the MIME)."""
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


@dataclass(frozen=True)
class ShareScope:
    """What a share token resolves to: a workspace and the subtree it opens.

    The pair is the unit, not the workspace alone. A token issued for one report
    serves that path and its subtree; nothing else in the workspace is reachable
    through it, however the URL is spelled.
    """

    workspace_id: str
    root_path: str

    def contains(self, relative_path: str) -> bool:
        if not self.root_path:
            return True
        return relative_path == self.root_path or relative_path.startswith(
            f"{self.root_path}/"
        )


def shared_path_visible(scope: ShareScope, client_path: str) -> bool:
    """Whether a canonical workspace path may leave through this share token.

    Run on the path a sandbox read resolved to, not on the one that was asked
    for: a symlink is how an in-scope request turns into an out-of-scope file.

    Stricter than the panel: it skips ``_is_system_path``'s carve-out for the
    user's profile, automation and subagent files. Their owner reads them through
    the file routes, and the file mount links them into every workspace folder,
    so only containment would stand between a share link and the user's holdings.

    The workspace's notes file stays out too, whatever the scope: it is the
    agent's own runtime context, and a shared page cannot tell it from a
    deliverable because the share carries no folder name to recognise it by.
    """
    return (
        scope.contains(client_path)
        and not is_always_hidden_path(client_path)
        and not is_agent_machinery_path(client_path)
        and not is_agent_notes_path(client_path)
    )


# What a comment's and a version file's path columns hold, in characters.
_MAX_ARTIFACT_PATH = 1024


def may_be_artifact(path: str) -> bool:
    """Whether a workspace-relative path may have an artifact record and go out
    with a publish, in a length its comments and versions can store. Wider than
    a share token's gate because the owner reviews each file a publish lists."""
    return (
        len(path) <= _MAX_ARTIFACT_PATH
        and page_may_load(path)
        and not is_agent_notes_path(path)
    )
