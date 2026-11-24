"""Canonical computer and workspace paths, shared without I/O or sandbox imports.

SandboxLayout owns machine-wide paths; WorkspaceLayout adds a project folder.
Emit constants for frontend and uploaded sandbox sources that cannot import
these classes, preventing duplicate path definitions.
"""

from __future__ import annotations

import posixpath
import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

DEFAULT_SANDBOX_ROOT: str = "/home/workspace"

# Accept legacy roots from reused sandboxes and previously generated links.
LEGACY_SANDBOX_ROOTS: tuple[str, ...] = ("/home/daytona",)

SANDBOX_ROOTS: tuple[str, ...] = (DEFAULT_SANDBOX_ROOT, *LEGACY_SANDBOX_ROOTS)


def name_fold(path: str) -> str:
    """*path* as a Mac's disk compares names, ignoring letter case and Unicode
    normalization, so two spellings of one folder compare equal.

    A Linux disk tells them apart, but a computer never holds two folders of
    its own that differ only so (``workspace_layout``), so a check folded this
    way refuses no folder it would otherwise allow there.
    """
    return unicodedata.normalize("NFC", unicodedata.normalize("NFC", path).casefold())


def computer_roots(own: str | None) -> tuple[str, ...]:
    """A computer's own root, then the stock roots, longest first and deduped.

    Path folds (provenance, link qualifying) must agree on one list: the own
    root leads so it wins where it is a shorter prefix of a stock root.
    """
    own = (own or "").rstrip("/")
    stock = sorted(SANDBOX_ROOTS, key=len, reverse=True)
    return tuple(dict.fromkeys([own, *stock] if own else stock))


#: LangAlpha's own folder in a user's own computer's config home
#: (``$XDG_CONFIG_HOME`` when absolute, else ``~/.config``), outside the
#: computer root and refused to the agent's own tools. Each computer's Claude
#: Code setup token is the file named by its computer id in the second folder.
LANGALPHA_CONFIG_DIR: str = "langalpha"
CLAUDE_CODE_TOKEN_DIR: str = f"{LANGALPHA_CONFIG_DIR}/claude-code"


def langalpha_config_dir(home: str, config_home: str | None = None) -> str:
    """LangAlpha's folder in the config home of a login whose home is ``home``
    and whose ``$XDG_CONFIG_HOME`` is ``config_home``; a relative one is
    ignored, as the XDG spec has it."""
    base = config_home if config_home and config_home.startswith("/") else home.rstrip("/") + "/.config"
    return f"{base.rstrip('/')}/{LANGALPHA_CONFIG_DIR}"


_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

#: How a command or a listing spells LangAlpha's config folder: the folder of
#: the default config home or of one named by the variable, as a whole path
#: component, and a Claude Code token file under any config home, a custom one
#: included, by its folder and its name (a computer id, or a staged copy of
#: one). A workspace folder named like them holds no file so named. Text
#: matching is all a tool call's arguments and output allow, so this stands
#: beside the exact path `langalpha_config_dir` gives the file tools; matched
#: case-insensitively, as a Mac's disk matches names.
LANGALPHA_CONFIG_SPELLINGS: re.Pattern[str] = re.compile(
    rf"(?:(?<![\w.-])\.config|\$xdg_config_home|\$\{{xdg_config_home\}})/{re.escape(LANGALPHA_CONFIG_DIR)}(?![\w.-])"
    rf"|{re.escape(CLAUDE_CODE_TOKEN_DIR)}/(?:\.staged\.)?{_UUID}",
    re.IGNORECASE,
)


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
    whatever the host is. POSIX leaves a leading ``//`` to the system and
    ``normpath`` keeps it, but Linux reads it as ``/``: kept, it would walk
    past every prefix test written for ``/``.
    """
    normal = posixpath.normpath(path)
    return "/" + normal.lstrip("/") if normal.startswith("//") else normal


# Name prefixes, not paths, so not on SandboxLayout, whose every constant is a
# path under the root. A removal's trash in ``SandboxLayout.LEFTOVERS_DIR``,
# which it deletes detached:
LEFTOVERS_TRASH_PREFIX = "removed."
# The script an SSH computer writes at its root to run code, deleted after:
EXEC_SCRIPT_PREFIX = "_exec_"


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
    AUTOMATIONS_DIR: ClassVar[str] = ".agents/user/automations"
    # The user's subagents, one markdown file each, plus a read-only main.md
    # for the agent itself.
    USER_AGENTS_DIR: ClassVar[str] = ".agents/user/agents"
    # The user's chat-app settings, kept by the channel gateway. Only the file
    # tools reach it: it is no mount tier and no user data dir, and Bash and
    # code are refused a command that names it.
    CHANNELS_DIR: ClassVar[str] = ".agents/user/channels"
    WORKFLOWS_DIR: ClassVar[str] = ".agents/workflows"
    # The Drive: one folder per space the user reaches.
    DRIVE_DIR: ClassVar[str] = ".agents/drive"
    TMP_DIR: ClassVar[str] = ".agents/tmp"

    SYSTEM_DIR: ClassVar[str] = ".system"
    SYSTEM_CODE_DIR: ClassVar[str] = ".system/code"
    SYSTEM_TRACE_DIR: ClassVar[str] = ".system/trace"
    # Detached commands' state (pid, exit code), one directory each. Kept by
    # the computer rather than a workspace folder, so a folder renamed while a
    # command runs never makes it look lost.
    SYSTEM_JOBS_DIR: ClassVar[str] = ".system/jobs"

    # Runtime paths must remain hidden from agents and denied by path validation.
    INTERNAL_DIR: ClassVar[str] = "_internal"
    INTERNAL_SRC_DIR: ClassVar[str] = "_internal/src"
    PACKS_DIR: ClassVar[str] = "_internal/packs"
    WSFILES_DIR: ClassVar[str] = "_internal/.wsfiles"
    MANIFEST_FILE: ClassVar[str] = "_internal/.sandbox_manifest.json"
    MCP_TOKENS_FILE: ClassVar[str] = "_internal/.mcp_tokens.json"
    EGRESS_RELAY_FILE: ClassVar[str] = "_internal/.egress_relay.json"
    VAULT_SECRETS_FILE: ClassVar[str] = "_internal/.vault_secrets.json"
    # The token the `la` CLI calls the API with, as the computer's user.
    CLI_TOKEN_FILE: ClassVar[str] = "_internal/.cli_token.json"
    # The `la` shim, shipped with the internal packages; on every job's PATH.
    CLI_BIN_DIR: ClassVar[str] = "_internal/src/la/bin"
    # An MCP check's files, which hold a copy of the server's vault secrets.
    MCP_CHECK_PREFIX: ClassVar[str] = "_internal/.mcp_check_"
    # Folders of the owner's set aside out of a workspace folder's way, and
    # deleted workspaces' folders waiting to go or kept for the owner.
    LEFTOVERS_DIR: ClassVar[str] = "_internal/leftovers"
    # What the Claude Code runtime keeps on a user's own computer: the CLI
    # release it runs and each turn's private config dir.
    CLAUDE_CODE_DIR: ClassVar[str] = "_internal/claude-code"

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
    def drive(self) -> str:
        return self.join(self.DRIVE_DIR)

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
    def system_jobs(self) -> str:
        return self.join(self.SYSTEM_JOBS_DIR)

    @property
    def internal(self) -> str:
        return self.join(self.INTERNAL_DIR)

    @property
    def internal_src(self) -> str:
        return self.join(self.INTERNAL_SRC_DIR)

    @property
    def claude_code(self) -> str:
        return self.join(self.CLAUDE_CODE_DIR)

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
    def cli_token(self) -> str:
        return self.join(self.CLI_TOKEN_FILE)

    @property
    def cli_bin(self) -> str:
        return self.join(self.CLI_BIN_DIR)

    @property
    def credential_files(self) -> tuple[str, ...]:
        """The files the server writes credentials to for the computer's
        processes; an MCP check's copies match ``MCP_CHECK_PREFIX``."""
        return (self.mcp_tokens, self.egress_relay, self.vault_secrets, self.cli_token)

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
    # The agent's own per-thread working files, when the scratchpad feature
    # is on. Unlike thread scratch it is backed up, and it leaves the sandbox
    # when its thread is archived.
    SCRATCHPAD_DIR: ClassVar[str] = ".agents/scratchpad"
    SCRATCHPAD_NOTE_DIR: ClassVar[str] = "note"
    # Rendered from each thread's checkpoint and served by the file mount.
    TRANSCRIPTS_DIR: ClassVar[str] = ".agents/transcripts"
    # In a shared workspace, a member's own memory, one folder per member
    # named by their short id. Served read-only to the file tools on that
    # member's turns alone, from the store; nothing of it is ever on disk.
    MEMBERS_DIR: ClassVar[str] = ".agents/members"
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
    def scratchpad(self) -> str:
        return self.join(self.SCRATCHPAD_DIR)

    @property
    def transcripts(self) -> str:
        return self.join(self.TRANSCRIPTS_DIR)

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

    @staticmethod
    def large_results_subdir(thread_id: str) -> str:
        """A thread's evicted tool results, workspace-relative.

        Outside ``thread_subdir`` on purpose: these are backed up, while thread
        scratch is regenerable and is not.
        """
        return "/".join((WorkspaceLayout.LARGE_TOOL_RESULTS_DIR, thread_id))

    @staticmethod
    def scratchpad_subdir(thread_id: str, *parts: str) -> str:
        """A thread's scratchpad, or something under it, workspace-relative."""
        return "/".join((WorkspaceLayout.SCRATCHPAD_DIR, thread_id, *parts))

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

# The folder under the Drive that holds the user's own space. It names a
# space, not a place on the computer, so it is no part of the layout.
DRIVE_PERSONAL: str = "personal"

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

# Virtual paths route through CompositeFilesystemBackend to LangGraph BaseStore,
# not sandbox files; expose them here for tooling.
MEMORY_USER_DIR: str = SandboxLayout.MEMORY_USER_DIR
MEMORY_INDEX_FILENAME: str = "memory.md"

# User-owned memos: server API writes, agent filesystem tools only read.
MEMO_USER_DIR: str = SandboxLayout.MEMO_USER_DIR
MEMO_INDEX_FILENAME: str = "memo.md"

# Writes fork shipped workflows into the user store, shadowing the shipped copy.
WORKFLOW_DIR: str = SandboxLayout.WORKFLOWS_DIR

# The directories whose files are rows in Postgres, each beside a README.md:
# the user's portfolio, watchlists, preferences and account under the fixed
# names in USER_DATA_FILES, one file per automation under a name the agent
# picks, which AUTOMATION_FILE_NAME matches, and one file per subagent named
# after it, which SUBAGENT_FILE_NAME matches. The routes, the file panel and
# the browser's path classifier all read these.
USER_DATA_FILES: dict[str, tuple[str, ...]] = {
    SandboxLayout.USER_PROFILE_DIR: ("portfolio.json", "watchlist.json", "preference.json", "user.json"),
}
USER_DATA_DIRS: tuple[str, ...] = (
    SandboxLayout.USER_PROFILE_DIR,
    SandboxLayout.AUTOMATIONS_DIR,
    SandboxLayout.USER_AGENTS_DIR,
)
AUTOMATION_FILE_NAME = r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\.json"
# A subagent's name, which is also its file's name before ``.md``.
SUBAGENT_NAME = r"[a-z0-9][a-z0-9-]{0,47}"
SUBAGENT_FILE_NAME = SUBAGENT_NAME + r"\.md"

# The tails of the folders whose files are named by the user, the same in a
# sandbox path and a mount path.
_NAMED_FILE_TAILS = tuple(
    folder.removeprefix(SandboxLayout.AGENTS_DIR)
    for folder in (SandboxLayout.AUTOMATIONS_DIR, SandboxLayout.USER_AGENTS_DIR)
)


def logged_path(path: str) -> str:
    """``path`` as an operator log keeps it. An automation's file is named
    after the automation and a subagent's after the subagent, which the logs
    leave out, so a path in either folder keeps the folder alone."""
    # Normalized first, so ``user/./automations/`` and ``//`` spellings fold
    # too, and a ``..`` that walks out of the folder keeps no name either.
    normalized = posixpath.normpath(path)
    for tail in _NAMED_FILE_TAILS:
        folder, marker, _ = normalized.partition(tail + "/")
        if marker:
            return folder + tail
    return normalized


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
#: What an OS leaves in any folder a person opens in a file browser (Finder's
#: view settings, Explorer's thumbnails). Never the person's work, so it is
#: neither listed nor backed up.
OS_LITTER_BASENAMES: tuple[str, ...] = (".DS_Store", "Thumbs.db")
ALWAYS_HIDDEN_BASENAMES: tuple[str, ...] = (
    "__init__.py",
    ".bash_logout",
    ".bashrc",
    ".profile",
    *OS_LITTER_BASENAMES,
)
ALWAYS_HIDDEN_SUFFIXES: tuple[str, ...] = (".pyc",)
#: Every transient file the transfer runtime or the server writes into a
#: workspace folder sits at its root under this prefix: a download on its way
#: in, a staged upload. There, and only there, the name is never a user's file
#: (``wsfiles_transfer_runtime._TEMP_PREFIX``, which ships stdlib-only).
TRANSIENT_ROOT_PREFIX = ".wsfiles-"

#: How a thread's scratch, results, scratchpad and transcript directories are
#: named: the first 8 characters of its id.
THREAD_DIR_NAME = re.compile(r"^[0-9a-f]{8}$")

#: Where a prune moves dead thread dirs to delete them, workspace-relative;
#: each prune's folder adds its own suffix. A backup never reads it, so what a
#: failed delete leaves cannot come back with a restore.
THREAD_DIRS_SET_ASIDE = f"{WorkspaceLayout.AGENTS_DIR}/.pruned."

#: The per-thread dirs that end when their thread is archived and left idle;
#: every other one lives as long as its thread.
ARCHIVE_SCOPED_THREAD_DIRS: tuple[str, ...] = (WorkspaceLayout.SCRATCHPAD_DIR,)

# Not the backup exclusions: these hide harness scratch from the agent's globs,
# while a backup skips mounted and regenerated trees. Keep them separate.
# What the harness wrote about past turns: per-thread scratch (evicted messages,
# saved attachments, offloaded results, scripts) and the thread-less fallback for
# large results. The agent reaches each through a pointer that names its path,
# so its own broad globs skip them rather than drown project files in copies of
# them. Matched as a child of .agents, so a project's own threads/ stays visible.
# The scratchpads are here for the same reason: every thread's working files,
# which a turn reaches by the path its context names. Membership also keeps a
# read of one out of provenance, and lets Tier 1 hide an old read of one even
# when it is the latest: a note is the agent's own working file, corrected in
# place and read again by path when it matters, not a source a deliverable
# rests on.
AGENT_HISTORY_DIRS: tuple[str, ...] = (
    WorkspaceLayout.THREADS_DIR,
    WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
    WorkspaceLayout.TRANSCRIPTS_DIR,
    WorkspaceLayout.SCRATCHPAD_DIR,
)



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


def grep_working_dir(search_path: str, *, folder: str, root: str) -> str:
    """The directory Grep runs rg in, which is where rg matches a ``glob``
    filter from: the computer root for a search of the root's ``.agents`` or
    below, and the turn's folder for any other.

    rg matches the filter against each path relative to its directory. The
    root's ``.agents`` holds the computer tier, whose files Grep prints as
    ``/.agents/...``, spelled from the root, so ``.agents/tmp/*.csv`` matches
    them as printed; from the folder they would sit outside rg's directory
    and meet the filter by their whole absolute path. The root itself and the
    other folders keep the folder, where Bash starts.
    """
    root = root.rstrip("/")
    agents = f"{root}/{SandboxLayout.AGENTS_DIR}"
    path = search_path.rstrip("/")
    if path == agents or path.startswith(f"{agents}/"):
        return root or "/"
    return folder


def strip_previous_dir_name(
    relative: str, previous_dir_names: Sequence[str]
) -> str | None:
    """The rest of a root-relative path that starts in a folder the workspace left.

    None when the first segment names none of them. Casefold, because workspace
    names are unique per user by their casefolded key and a folder is named
    after its workspace.
    """
    first, _, rest = relative.partition("/")
    folded = first.casefold()
    if first and any(folded == name.casefold() for name in previous_dir_names):
        return rest
    return None


def is_agent_notes_path(relative_path: str | None) -> bool:
    """Whether a folder-relative path names the workspace's own notes file.

    Only the file at the folder root: the agent keeps runtime context there for
    itself, so it is not a deliverable, whereas a nested ``docs/agent.md`` is a
    file the user asked for. Takes the folder-relative spelling, which is what
    every route judges after folding an absolute sandbox path onto the folder,
    so ``<dir_name>/agent.md`` here is a nested file and not the notes.
    """
    return (relative_path or "").lstrip("/") == WorkspaceLayout.AGENT_MD_FILE


def workspace_relative_path(path: str | None, work_dir: str) -> str:
    """Agent spellings /agent.md, agent.md, and the absolute workspace path name one file.

    Use removeprefix: lstrip would eat the leading dot of .agents paths.
    """
    normalized = path or ""
    prefix = work_dir.rstrip("/") + "/"
    if normalized.startswith(prefix):
        normalized = normalized[len(prefix) :]
    return normalized.removeprefix("./").removeprefix("/")
