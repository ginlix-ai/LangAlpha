"""Canonical computer and workspace paths, shared without I/O or sandbox imports.

SandboxLayout owns machine-wide paths; WorkspaceLayout adds a project folder.
Emit constants for frontend and uploaded sandbox sources that cannot import
these classes, preventing duplicate path definitions.
"""

from __future__ import annotations

import posixpath
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

DEFAULT_SANDBOX_ROOT: str = "/home/workspace"

# Accept legacy roots from reused sandboxes and previously generated links.
LEGACY_SANDBOX_ROOTS: tuple[str, ...] = ("/home/daytona",)

SANDBOX_ROOTS: tuple[str, ...] = (DEFAULT_SANDBOX_ROOT, *LEGACY_SANDBOX_ROOTS)

# Reused sandboxes retain root-level tools until migration; name-based filters
# must recognize it alongside _internal.
LEGACY_ROOT_TOOLS_DIR: str = "tools"

# Where generated code sat before v2 moved it under .system.
LEGACY_ROOT_CODE_DIR: str = "code"

#: Root-level directories an earlier layout owned, which survive in a sandbox
#: reused across its migration. A name-based filter has to recognise them
#: alongside the current tiers, so the browser reads them from here rather than
#: keeping its own literals.
LEGACY_ROOT_DIRS: tuple[str, ...] = (LEGACY_ROOT_CODE_DIR, LEGACY_ROOT_TOOLS_DIR)


def workspace_root(root_dir: str, dir_name: str | None) -> str:
    """An empty folder name denotes an unsplit computer whose workspace owns the root."""
    return f"{root_dir}/{dir_name}" if dir_name else root_dir


def lexical_path(path: str) -> str:
    """Collapse ``.`` and ``..`` segments so a containment test reads the directory a path names.

    Lexical on purpose: the file lives on a remote machine, so there is nothing
    here to resolve against, and an uncollapsed ``..`` reads as an ordinary
    segment -- which lets a path spell its way into a folder the same test
    refuses by its real name. POSIX semantics because the sandbox is Linux
    whatever the host is.
    """
    return posixpath.normpath(path)


def _class_constants(cls: type, names: Sequence[str]) -> dict[str, str]:
    return {name: getattr(cls, name) for name in names}


@dataclass(frozen=True, slots=True)
class SandboxLayout:
    """Relative names are ClassVar because only the root varies across computers."""

    root: str = DEFAULT_SANDBOX_ROOT

    TOOLS_DIR: ClassVar[str] = "_internal/tools"
    # Docs are reference material the agent and the file panel both read, so
    # they sit in the readable tier. Only the wrappers stay under _internal,
    # which is what keeps a server a workspace disabled unimportable.
    TOOLS_DOCS_DIR: ClassVar[str] = ".agents/tools/docs"
    TOOLS_INIT_FILE: ClassVar[str] = "_internal/tools/__init__.py"
    MCP_CLIENT_FILE: ClassVar[str] = "_internal/tools/mcp_client.py"
    # Which workspaces claim which wrapper, and the union's server entries.
    # The uploaded client reads its own server map out of this file, so the
    # merge that writes it publishes the union in one atomic replace.
    UNION_LEDGER_FILE: ClassVar[str] = "_internal/tools/.union.json"
    UNION_LOCK_FILE: ClassVar[str] = "_internal/tools/.union.lock"

    MCP_SERVERS_DIR: ClassVar[str] = "mcp_servers"
    MCP_MANIFEST_FILE: ClassVar[str] = "mcp_servers/.mcp_manifest.json"

    AGENTS_DIR: ClassVar[str] = ".agents"
    SKILLS_DIR: ClassVar[str] = ".agents/skills"
    SKILLS_MANIFEST_FILE: ClassVar[str] = ".agents/skills/.skills_manifest.json"
    USER_DIR: ClassVar[str] = ".agents/user"
    MEMORY_USER_DIR: ClassVar[str] = ".agents/user/memory"
    MEMO_USER_DIR: ClassVar[str] = ".agents/user/memo"
    USER_PROFILE_DIR: ClassVar[str] = ".agents/user/profile"
    WORKFLOWS_DIR: ClassVar[str] = ".agents/workflows"
    TMP_DIR: ClassVar[str] = ".agents/tmp"

    SYSTEM_DIR: ClassVar[str] = ".system"
    SYSTEM_CODE_DIR: ClassVar[str] = ".system/code"
    SYSTEM_TRACE_DIR: ClassVar[str] = ".system/trace"

    # Runtime paths must remain hidden from agents and denied by path validation.
    INTERNAL_DIR: ClassVar[str] = "_internal"
    INTERNAL_SRC_DIR: ClassVar[str] = "_internal/src"
    PACKS_DIR: ClassVar[str] = "_internal/packs"
    WSFILES_DIR: ClassVar[str] = "_internal/.wsfiles"
    MANIFEST_FILE: ClassVar[str] = "_internal/.sandbox_manifest.json"
    MCP_TOKENS_FILE: ClassVar[str] = "_internal/.mcp_tokens.json"
    EGRESS_RELAY_FILE: ClassVar[str] = "_internal/.egress_relay.json"
    VAULT_SECRETS_FILE: ClassVar[str] = "_internal/.vault_secrets.json"

    #: Relative names the uploaded sandbox client reads back out of the
    #: emitted block. Only these are emitted: the emission rides
    #: ``MCP_CLIENT_CODEGEN_VERSION``, so a rename of a name the runtime never
    #: reads would re-sync every warm sandbox in the fleet for nothing.
    RUNTIME_CONSTANTS: ClassVar[tuple[str, ...]] = (
        "INTERNAL_DIR",
        "INTERNAL_SRC_DIR",
        "VAULT_SECRETS_FILE",
        "EGRESS_RELAY_FILE",
        "MCP_TOKENS_FILE",
        "UNION_LEDGER_FILE",
    )

    @classmethod
    def default(cls) -> SandboxLayout:
        return cls(DEFAULT_SANDBOX_ROOT)

    @classmethod
    def for_root(cls, root: str | None) -> SandboxLayout:
        normalized = (root or "").rstrip("/")
        return cls(normalized or DEFAULT_SANDBOX_ROOT)

    def for_workspace(self, dir_name: str | None = None) -> WorkspaceLayout:
        return WorkspaceLayout(self.root, dir_name or "")

    def join(self, *parts: str) -> str:
        return "/".join((self.root, *(p.strip("/") for p in parts if p)))

    @property
    def tools(self) -> str:
        return self.join(self.TOOLS_DIR)

    @property
    def tools_docs(self) -> str:
        return self.join(self.TOOLS_DOCS_DIR)

    @property
    def union_ledger(self) -> str:
        return self.join(self.UNION_LEDGER_FILE)

    @property
    def union_lock(self) -> str:
        return self.join(self.UNION_LOCK_FILE)

    @property
    def tools_init(self) -> str:
        return self.join(self.TOOLS_INIT_FILE)

    @property
    def mcp_client(self) -> str:
        return self.join(self.MCP_CLIENT_FILE)

    @property
    def mcp_servers(self) -> str:
        return self.join(self.MCP_SERVERS_DIR)

    @property
    def mcp_manifest(self) -> str:
        return self.join(self.MCP_MANIFEST_FILE)

    @property
    def agents(self) -> str:
        return self.join(self.AGENTS_DIR)

    @property
    def skills(self) -> str:
        return self.join(self.SKILLS_DIR)

    @property
    def skills_manifest(self) -> str:
        return self.join(self.SKILLS_MANIFEST_FILE)

    @property
    def user(self) -> str:
        return self.join(self.USER_DIR)

    @property
    def memory_user(self) -> str:
        return self.join(self.MEMORY_USER_DIR)

    @property
    def memo_user(self) -> str:
        return self.join(self.MEMO_USER_DIR)

    @property
    def user_profile(self) -> str:
        return self.join(self.USER_PROFILE_DIR)

    @property
    def workflows(self) -> str:
        return self.join(self.WORKFLOWS_DIR)

    @property
    def system(self) -> str:
        return self.join(self.SYSTEM_DIR)

    @property
    def system_code(self) -> str:
        return self.join(self.SYSTEM_CODE_DIR)

    @property
    def system_trace(self) -> str:
        return self.join(self.SYSTEM_TRACE_DIR)

    @property
    def internal(self) -> str:
        return self.join(self.INTERNAL_DIR)

    @property
    def internal_src(self) -> str:
        return self.join(self.INTERNAL_SRC_DIR)

    @property
    def packs(self) -> str:
        return self.join(self.PACKS_DIR)

    @property
    def wsfiles(self) -> str:
        return self.join(self.WSFILES_DIR)

    @property
    def manifest(self) -> str:
        return self.join(self.MANIFEST_FILE)

    @property
    def mcp_tokens(self) -> str:
        return self.join(self.MCP_TOKENS_FILE)

    @property
    def egress_relay(self) -> str:
        return self.join(self.EGRESS_RELAY_FILE)

    @property
    def vault_secrets(self) -> str:
        return self.join(self.VAULT_SECRETS_FILE)

    @property
    def setup_dirs(self) -> tuple[str, ...]:
        """Deliverable directories belong to WorkspaceLayout.setup_dirs, not the machine."""
        return (
            self.tools,
            self.tools_docs,
            self.system_code,
            self.system_trace,
            self.skills,
            self.internal_src,
        )

    @property
    def allowed_directories(self) -> list[str]:
        return [self.root, "/tmp"]

    @property
    def denied_directories(self) -> list[str]:
        return [self.internal]

    def as_constants(self) -> dict[str, dict[str, str]]:
        """Class-name keys preserve which tier owns each emitted relative path."""
        return {
            "SandboxLayout": _class_constants(
                SandboxLayout, SandboxLayout.RUNTIME_CONSTANTS
            ),
            "WorkspaceLayout": _class_constants(
                WorkspaceLayout, WorkspaceLayout.RUNTIME_CONSTANTS
            ),
        }


@dataclass(frozen=True, slots=True)
class WorkspaceLayout:
    """root remains the computer root; an empty dir_name denotes an unsplit workspace."""

    root: str
    dir_name: str = ""

    AGENTS_DIR: ClassVar[str] = ".agents"
    SKILLS_DIR: ClassVar[str] = ".agents/skills"
    MEMORY_DIR: ClassVar[str] = ".agents/memory"
    TOOLS_DIR: ClassVar[str] = ".agents/tools"
    MCP_CLIENT_CONFIG_FILE: ClassVar[str] = ".agents/tools/mcp_client_config.json"
    # A turn's scratch belongs to the project it ran for, so deleting the
    # workspace takes it along instead of leaving it flat on the machine.
    THREADS_DIR: ClassVar[str] = ".agents/threads"
    LARGE_TOOL_RESULTS_DIR: ClassVar[str] = ".agents/large_tool_results"
    AGENT_MD_FILE: ClassVar[str] = "agent.md"
    # Task directories sit directly in the folder; only the shared dataset
    # directory has a fixed name.
    DATA_DIR: ClassVar[str] = "data"

    #: See ``SandboxLayout.RUNTIME_CONSTANTS``.
    RUNTIME_CONSTANTS: ClassVar[tuple[str, ...]] = (
        "TOOLS_DIR",
        "MCP_CLIENT_CONFIG_FILE",
    )

    @property
    def workspace(self) -> str:
        return workspace_root(self.root, self.dir_name)

    def join(self, *parts: str) -> str:
        return "/".join((self.workspace, *(p.strip("/") for p in parts if p)))

    @property
    def agents(self) -> str:
        return self.join(self.AGENTS_DIR)

    @property
    def skills(self) -> str:
        return self.join(self.SKILLS_DIR)

    @property
    def memory(self) -> str:
        return self.join(self.MEMORY_DIR)

    @property
    def tools(self) -> str:
        return self.join(self.TOOLS_DIR)

    @property
    def tools_docs(self) -> str | None:
        """This folder's doc tree, or None when the union's docs already are it.

        A workspace that owns the computer root reads the union directory
        itself, and there is no second spelling of it: an overlay sweep that
        could name the union docs would delete a sibling's docs on every sync.
        """
        if not self.dir_name:
            return None
        return self.join(SandboxLayout.TOOLS_DOCS_DIR)

    @property
    def mcp_client_config(self) -> str:
        return self.join(self.MCP_CLIENT_CONFIG_FILE)

    @property
    def threads(self) -> str:
        return self.join(self.THREADS_DIR)

    @property
    def large_tool_results(self) -> str:
        return self.join(self.LARGE_TOOL_RESULTS_DIR)

    @property
    def agent_md(self) -> str:
        return self.join(self.AGENT_MD_FILE)

    @staticmethod
    def thread_subdir(thread_id: str, *parts: str) -> str:
        """A thread's scratch directory, or something under it, workspace-relative.

        Relative on purpose: these strings go into prompt text and into backend
        writes, and both resolve against the turn's own folder. A caller with no
        bound turn wants the absolute ``thread_dir`` instead.
        """
        return "/".join((WorkspaceLayout.THREADS_DIR, thread_id, *parts))

    def thread_dir(self, thread_id: str) -> str:
        """One thread's scratch directory inside this folder, absolute."""
        return self.join(self.thread_subdir(thread_id))

    @property
    def setup_dirs(self) -> tuple[str, ...]:
        """Match the v3-to-v4 migration so fresh and migrated folders have the same shape."""
        return (
            self.workspace,
            self.join(self.DATA_DIR),
            self.skills,
            self.memory,
            self.tools,
            self.threads,
        )

    def pythonpath(self, computer: SandboxLayout) -> list[str]:
        """Workspace .agents must precede shared runtime so its tools package shadows the union."""
        return [self.agents, computer.internal, computer.internal_src]

    def denied_directories(
        self, computer: SandboxLayout, sibling_dir_names: Sequence[str] = ()
    ) -> list[str]:
        """Keep sibling files readable but deny their tools to prevent importing disabled servers."""
        denied = [computer.internal]
        for name in sibling_dir_names:
            if not name or name == self.dir_name:
                continue
            denied.append(WorkspaceLayout(self.root, name).tools)
        return denied

    def as_constants(self) -> dict[str, str]:
        return _class_constants(WorkspaceLayout, WorkspaceLayout.RUNTIME_CONSTANTS)


DEFAULT_LAYOUT: SandboxLayout = SandboxLayout.default()

# Agent infrastructure is toggleable in listings but hidden in completions.
AGENT_SYSTEM_DIRS: frozenset[str] = frozenset({
    SandboxLayout.SYSTEM_DIR,
    SandboxLayout.MCP_SERVERS_DIR,
    SandboxLayout.AGENTS_DIR,
    ".self-improve",
})

# Leave .agents out so .agents/skills is backed up.
BACKUP_EXCLUDE_DIRS: frozenset[str] = frozenset({
    SandboxLayout.SYSTEM_DIR,
    SandboxLayout.MCP_SERVERS_DIR,
    ".self-improve",
})

# Exclude ephemeral agent data from backup. Both tiers appear because the
# names are matched workspace-relative and the two tiers can share a folder.
# The tool package is here because every byte of it (wrappers, docs, config)
# is re-emitted by the MCP sync into whatever sandbox the restore lands in.
BACKUP_EXCLUDE_AGENT_SUBDIRS: tuple[str, ...] = (
    WorkspaceLayout.THREADS_DIR,
    WorkspaceLayout.TOOLS_DIR,
    SandboxLayout.USER_DIR,
    SandboxLayout.WORKFLOWS_DIR,
    WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
)

# Virtual paths route through CompositeFilesystemBackend to LangGraph BaseStore,
# not sandbox files; expose them here for tooling.
MEMORY_USER_DIR: str = SandboxLayout.MEMORY_USER_DIR
MEMORY_INDEX_FILENAME: str = "memory.md"

# User-owned memos: server API writes, agent filesystem tools only read.
MEMO_USER_DIR: str = SandboxLayout.MEMO_USER_DIR
MEMO_INDEX_FILENAME: str = "memo.md"

# Writes fork shipped workflows into the user store, shadowing the shipped copy.
WORKFLOW_DIR: str = SandboxLayout.WORKFLOWS_DIR

# DB-backed user_portfolios, watchlists, watchlist_items, and user_preferences;
# agent JSON writes validate and apply diffs atomically.
USER_PROFILE_DATA_DIR: str = SandboxLayout.USER_PROFILE_DIR
USER_PROFILE_PORTFOLIO_FILE: str = "portfolio.json"
USER_PROFILE_WATCHLIST_FILE: str = "watchlist.json"
USER_PROFILE_PREFERENCE_FILE: str = "preference.json"

HIDDEN_DIR_NAMES: frozenset[str] = frozenset({SandboxLayout.INTERNAL_DIR})

# Match segments at any depth so nested dependencies also stay hidden and unsynced.
ALWAYS_HIDDEN_DIR_NAMES: frozenset[str] = frozenset({
    "node_modules",
    ".venv",
    "venv",
    "vendor",
    ".next",
    ".nuxt",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".git",
    ".npm",
    ".local",
    ".config",
    ".ipython",
})

ALWAYS_HIDDEN_PATH_SEGMENTS: tuple[str, ...] = ("/__pycache__/",)
ALWAYS_HIDDEN_BASENAMES: tuple[str, ...] = (
    "__init__.py",
    ".bash_logout",
    ".bashrc",
    ".profile",
)
ALWAYS_HIDDEN_SUFFIXES: tuple[str, ...] = (".pyc",)



# Root-level names that belong to the computer and never move into a workspace
# folder. The v3-to-v4 migration reads it to decide what stays at the root, and
# a test asserts every first segment of a SandboxLayout path is in it -- the
# set is knowledge about the layout, so it lives with the layout.
COMPUTER_ROOT_ENTRIES: frozenset[str] = frozenset({
    SandboxLayout.INTERNAL_DIR,
    SandboxLayout.MCP_SERVERS_DIR,
    SandboxLayout.AGENTS_DIR,
    SandboxLayout.SYSTEM_DIR,
    LEGACY_ROOT_TOOLS_DIR,
})


# Fold shared, store-backed user mounts onto the computer root or writes
# bypass the store. Workspace skills, memory, tools and thread scratch are
# excluded because they belong to the project folder.
COMPUTER_AGENT_SUBTREES: tuple[str, ...] = (
    SandboxLayout.USER_DIR,
    SandboxLayout.WORKFLOWS_DIR,
    SandboxLayout.TMP_DIR,
)


def computer_tier_relative_path(path: str | None) -> str | None:
    """Accept mount-relative spellings; absolute sandbox paths already identify their tier."""
    candidate = (path or "").strip()
    if candidate.startswith("/"):
        candidate = candidate.lstrip("/")
    while candidate.startswith("./"):
        candidate = candidate[2:]
    for subtree in COMPUTER_AGENT_SUBTREES:
        if candidate == subtree or candidate.startswith(f"{subtree}/"):
            return candidate
    return None


def _root_entry_in_folder(
    path: str, *, workspace: str, root: str, siblings: Sequence[str] | None
) -> str | None:
    """A loose root entry re-read as this project's own file, or None to keep the path.

    The computer root's real entries are the reserved tiers and one folder per
    project, so an absolute root path naming anything else is a pre-split
    spelling of this project's file: a turn resumed from before the folder
    existed, or a link stored when the workspace was the whole machine. Left at
    the root the write lands beside the folders, where no route serves it and
    no mirror scans it. A sibling's folder is spelled as itself, since a turn
    may write there on purpose, which is why ``siblings`` of None -- a caller
    that cannot enumerate the machine's folders -- leaves every path alone
    rather than reading a sibling's as its own.
    """
    if siblings is None or workspace == root or not path.startswith(f"{root}/"):
        return None
    relative = path[len(root) + 1 :]
    first = relative.split("/", 1)[0]
    if (
        not first
        or first in COMPUTER_ROOT_ENTRIES
        or first in siblings
        or first == posixpath.basename(workspace)
    ):
        return None
    return lexical_path(f"{workspace}/{relative}")


def resolve_agent_path(
    path: str | None,
    *,
    workspace: str,
    root: str,
    allowed: Sequence[str],
    sibling_dir_names: Sequence[str] | None = None,
) -> str:
    """Fold an agent's spelling of a path onto the directory it names.

    The one funnel every agent path reaches the sandbox and the validator
    through, so every result is collapsed (``lexical_path``): a ``..`` left in
    place would name a sibling's folder while still passing a prefix test
    against the computer root. ``workspace`` is the turn's folder and ``root``
    the computer; the split matters because the store-backed user mounts are
    keyed on the root-anchored prefix, so a computer-tier name folded into the
    folder would miss every route and write a real file the store never sees.
    An absolute path that lands on an unowned root entry folds too, per
    :func:`_root_entry_in_folder`.
    """
    if path in (None, "", ".", "/"):
        return workspace
    assert path is not None
    path = path.strip()
    for allowed_dir in allowed:
        if path == allowed_dir or path.startswith(allowed_dir.rstrip("/") + "/"):
            collapsed = lexical_path(path)
            return (
                _root_entry_in_folder(
                    collapsed,
                    workspace=workspace,
                    root=root,
                    siblings=sibling_dir_names,
                )
                or collapsed
            )
    computer_relative = computer_tier_relative_path(path)
    if computer_relative is not None:
        return lexical_path(f"{root}/{computer_relative}")
    if path.startswith("/"):
        return lexical_path(f"{workspace}{path}")
    return lexical_path(f"{workspace}/{path}")


def virtual_agent_path(path: str, *, workspace: str, root: str) -> str:
    """The inverse of :func:`resolve_agent_path`, where one exists.

    A path only gets a virtual spelling when that spelling folds back onto the
    same file. The virtual namespace is anchored on the turn's folder, so the
    two that qualify are a file inside the folder and a computer-tier name.
    Everything else (a sibling's folder, the root itself, ``/tmp``) keeps its
    absolute path, which is already a spelling both directions agree on.
    """
    path = lexical_path(path)
    if path == workspace:
        return "/"
    if path.startswith(workspace + "/"):
        virtual = path[len(workspace) :]
        if workspace != root and computer_tier_relative_path(virtual) is not None:
            # A real file left inside the folder under a computer-tier name has
            # no virtual spelling of its own: that spelling belongs to the
            # mount at the root.
            return path
        return virtual
    if path.startswith(root + "/"):
        virtual = path[len(root) :]
        if computer_tier_relative_path(virtual) is not None:
            return virtual
    return path


def workspace_relative_path(path: str | None, work_dir: str) -> str:
    """Agent spellings /agent.md, agent.md, and the absolute workspace path name one file.

    Use removeprefix: lstrip would eat the leading dot of .agents paths.
    """
    normalized = path or ""
    prefix = work_dir.rstrip("/") + "/"
    if normalized.startswith(prefix):
        normalized = normalized[len(prefix) :]
    return normalized.removeprefix("./").removeprefix("/")
