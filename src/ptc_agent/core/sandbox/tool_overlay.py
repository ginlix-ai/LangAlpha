"""The computer's wrapper union and one workspace's overlay on it.

Two tiers. ``_internal/tools/`` holds one wrapper module and one doc tree per
server any workspace on this computer enabled, plus the composed client at
``_internal/src/mcp_client.py``; the agent can reach none of it.
``<ws>/.agents/tools/`` is the tier it does reach: a ``tools`` package of
relative symlinks to the wrappers this workspace enabled, its docs, and the
``mcp_client_config.json`` naming that set. A disabled server has no symlink,
no doc and no config entry, so nothing the agent can read claims a capability
the workspace does not have.

Every mutation of the shared tiers happens in :data:`_SCRIPT`, one in-sandbox
pass holding an flock on the union. The claim ledger is a read-modify-write
over a file several workspaces share, and two turns on one computer sync
concurrently, so doing it host-side loses whichever merge lands second: the
losing workspace's claims vanish and the servers it just dropped keep an
immortal claim nothing will ever release. The version the merge decides is
also what the workspace's own config is stamped with, which is why the stamp
happens inside the lock rather than being guessed before it.
"""

from __future__ import annotations

import base64
import json
import posixpath
import textwrap
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import structlog

from ..mcp_sanitize import is_untrusted_server, sanitize_tool_name
from ..paths import SandboxLayout, WorkspaceLayout
from ..project_context import ProjectContext
from ..tool_generator import union_key
from .retry import RetryPolicy
from .vault_helper import VAULTS_DIR, workspace_vault_path

if TYPE_CHECKING:
    from .ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

#: Body of the tool package's ``__init__.py``, at both tiers.
_TOOLS_INIT_BODY = '"""Tool wrappers for the MCP servers this workspace enabled."""\n'
#: Names inside a workspace's tool package that are not server wrappers.
_WS_DOCS_DIRNAME = SandboxLayout.TOOLS_DOCS_DIR.rsplit("/", 1)[-1]
_WS_CONFIG_BASENAME = WorkspaceLayout.MCP_CLIENT_CONFIG_FILE.rsplit("/", 1)[-1]


class ToolOverlayError(Exception):
    """The in-sandbox reconcile failed; the union is unchanged."""


def _relative_link_target(link_path: str, target_path: str) -> str:
    """``target_path`` as ``link_path`` should spell it.

    Relative, never absolute: the mirror stores a link's target verbatim, so an
    absolute target only survives a restore into a differently-rooted computer
    by luck. Computed rather than hardcoded because a workspace that owns the
    whole root sits one level nearer ``_internal`` than a folder does.
    """
    return posixpath.relpath(target_path, posixpath.dirname(link_path))


def doc_name(tool_name: str, untrusted: bool) -> str:
    """The filename a tool's documentation is written under.

    Shared by the writer and by the sweep that removes what the writer no longer
    produces. Two derivations of one name is exactly how a doc for a tool the
    agent may not call survives a pass meant to delete it.

    Untrusted names could carry ``..`` or ``/`` and traverse out of the docs
    directory, so there the sanitized identifier is the filename; a builtin's
    name is already a valid identifier.
    """
    if untrusted:
        return sanitize_tool_name(tool_name) or "_invalid_tool"
    return tool_name


def union_ledger_holds(ledger: dict, claim: str) -> bool:
    """Whether a workspace already has a view of the computer's union.

    The claim is the only per-workspace fact on the computer; the asset
    manifest carries none, which is why a workspace joining a computer whose
    union is already current needs this to know it still owes itself an
    overlay.
    """
    claims = ledger.get("claims") or {}
    return any(
        isinstance(holders, list) and claim in holders for holders in claims.values()
    )


async def read_union_ledger(sandbox: "PTCSandbox", layout: SandboxLayout) -> dict:
    """The union ledger, or an empty one.

    Never raises: an absent or corrupt ledger reads as "no claims yet", which
    rebuilds the union from the syncing workspace rather than failing a sync
    over a bookkeeping file. Read-only, so it needs no lock; the merge that
    writes it runs in the sandbox instead.
    """
    try:
        raw = await sandbox.adownload_file_bytes(layout.union_ledger)
    except Exception:  # noqa: BLE001 - a missing ledger is the common case
        return {}
    if not raw:
        return {}
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        parsed = json.loads(text)
    except (UnicodeDecodeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def overlay_claim_missing(sandbox: "PTCSandbox", project: ProjectContext) -> bool:
    """Whether this workspace still owes itself a tool overlay.

    ``tool_modules`` hashes the computer-wide union and carries no workspace
    identity, so a workspace that joins a computer whose union is already
    current never reaches the install and would run with an empty
    ``.agents/tools``. The union ledger's claim is the per-workspace fact that
    answers it, and it is on-disk truth rather than process memory, so every
    worker reads the same answer.
    """
    if not project.dir_name or sandbox.mcp_registry is None:
        return False
    ledger = await read_union_ledger(sandbox, SandboxLayout(sandbox._work_dir))
    return not union_ledger_holds(ledger, project.claim)


def overlay_link_plan(
    layout: SandboxLayout,
    workspace: WorkspaceLayout,
    server_names: list[str],
    union_keys: Mapping[str, str] | None = None,
) -> list[tuple[str, str]]:
    """``(link, relative target)`` for one workspace's wrappers and docs.

    A link is named by the server's display name and points at the union file
    named by its key, so ``from tools.crm import ...`` reads the same in every
    workspace while two workspaces' ``crm`` stay two files.
    """
    plan: list[tuple[str, str]] = []
    docs_dir = workspace.tools_docs
    for name in server_names:
        key = (union_keys or {}).get(name, name)
        link = f"{workspace.tools}/{name}.py"
        plan.append((link, _relative_link_target(link, f"{layout.tools}/{key}.py")))
        if docs_dir is None:
            continue
        doc_link = f"{docs_dir}/{name}"
        plan.append(
            (doc_link, _relative_link_target(doc_link, f"{layout.tools_docs}/{key}"))
        )
    return plan


# A plain string, not an f-string: the arguments travel as a JSON file uploaded
# in the same batch as the wrappers, and the whole script is base64-wrapped
# into the exec command line, which sidesteps shell quoting entirely.
_SCRIPT = textwrap.dedent(r'''
import fcntl, json, os, shutil, sys, time

with open("__ARGS_PATH__", encoding="utf-8") as _fh:
    ARGS = json.load(_fh)
os.unlink("__ARGS_PATH__")

LEDGER = ARGS["ledger"]
LOCK = ARGS["lock"]
CLAIM = ARGS["claim"]
ROOT = ARGS["root"]
DIR_NAME = ARGS["dirName"]
SERVERS = ARGS["servers"]
UNION_TOOLS = ARGS["unionTools"]
UNION_DOCS = ARGS["unionDocs"]
WS_TOOLS = ARGS["wsTools"]
WS_DOCS = ARGS["wsDocs"]
WS_KEEP = set(ARGS["wsKeep"])
WS_DOCS_KEEP = set(ARGS.get("wsDocsKeep") or SERVERS)
VAULTS_DIR = ARGS.get("vaultsDir") or ""
WS_CONFIG_PATH = ARGS["wsConfigPath"]
WS_CONFIG = ARGS["wsConfig"]
EXPECTED_DOCS = ARGS["expectedDocs"]
LINKS = ARGS["links"]
LEGACY_CLIENT = ARGS["legacyClient"]


def fail(msg):
    print(json.dumps({"status": "error", "error": msg}))
    sys.exit(1)


def acquire_flock():
    fh = open(LOCK, "w")
    deadline = time.time() + 30
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fh
        except OSError:
            if time.time() > deadline:
                fail("union flock timeout")
            time.sleep(0.2)


def read_ledger():
    try:
        with open(LEDGER, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path, data):
    tmp = "%s.%d.tmp" % (path, os.getpid())
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, sort_keys=True, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def dead_claims(dirs):
    """Claims whose workspace folder is gone from this computer.

    A deleted project takes its folder with it, so the folder is the only
    liveness signal available in here -- and without one a claim outlives every
    workspace that could retract it, because a sync can only speak for itself.
    A claim with no recorded folder predates this key and is kept: unknowable
    is not the same as dead. The worst case of a false positive is a sibling
    re-adding its own claim on its next sync.
    """
    dead = set()
    for claim, dir_name in dirs.items():
        if claim == CLAIM or not dir_name:
            continue
        if not os.path.isdir(os.path.join(ROOT, dir_name)):
            dead.add(claim)
    return dead


def merge_ledger(ledger):
    """Fold this workspace's server set into the computer's union.

    The claim list is what makes the union safe to sweep: a workspace syncing
    its own set must not delete wrappers a sibling still imports, and a server
    the user removed must not linger forever either. Each entry carries the
    server's client config so the union client keeps one for a sibling's
    server between syncs.
    """
    old_claims = {}
    for name, holders in (ledger.get("claims") or {}).items():
        if isinstance(holders, list):
            old_claims[name] = [c for c in holders if isinstance(c, str)]
    old_entries = {}
    for name, entry in (ledger.get("servers") or {}).items():
        if isinstance(entry, dict):
            old_entries[name] = entry
    old_dirs = {}
    for claim, dir_name in (ledger.get("dirs") or {}).items():
        if isinstance(dir_name, str):
            old_dirs[claim] = dir_name
    dirs = dict(old_dirs)
    dirs[CLAIM] = DIR_NAME
    retracted = dead_claims(dirs) | {CLAIM}
    claims = {}
    for name, holders in old_claims.items():
        kept = sorted(set(holders) - retracted)
        if kept:
            claims[name] = kept
    for name in SERVERS:
        claims[name] = sorted(set(claims.get(name, ())) | {CLAIM})
    orphaned = sorted(set(old_claims) - set(claims))
    entries = {}
    for name in sorted(claims):
        entries[name] = SERVERS.get(name) or old_entries.get(name) or {}
    executable_keys = (
        "command",
        "args",
        "env",
        "env_keys",
        "url",
        "headers",
        "transport",
        "untrusted",
        "relay_bound",
        "vault_file",
        "credential_files",
        "name",
    )
    old_executable = {
        name: {key: entry.get(key) for key in executable_keys if key in entry}
        for name, entry in old_entries.items()
    }
    executable = {
        name: {key: entry.get(key) for key in executable_keys if key in entry}
        for name, entry in entries.items()
    }
    holders = set()
    for held in claims.values():
        holders.update(held)
    dirs = {claim: dirs[claim] for claim in sorted(holders) if claim in dirs}
    version = int(ledger.get("union_version") or 0)
    if claims != old_claims or entries != old_entries or dirs != old_dirs:
        version += 1
    if "config_version" in ledger:
        config_version = int(ledger.get("config_version") or 0)
    else:
        config_version = int(ledger.get("union_version") or 0)
    if executable != old_executable:
        config_version += 1
    merged = {
        "schema_version": 1,
        "union_version": version,
        "config_version": config_version,
        "claims": {name: claims[name] for name in sorted(claims)},
        "dirs": dirs,
        "servers": entries,
        "tool_versions": {
            claim: version
            for claim, version in (ledger.get("tool_versions") or {}).items()
            if claim in dirs
        },
    }
    return merged, orphaned


def remove(path):
    """Delete a file, a link or a whole tree; True when something went."""
    try:
        if os.path.islink(path) or os.path.isfile(path):
            os.unlink(path)
            return True
        if os.path.isdir(path):
            shutil.rmtree(path)
            return True
    except OSError as exc:
        fail("cannot remove %s: %s" % (path, exc))
    return False


def listdir(path):
    try:
        return sorted(os.listdir(path))
    except FileNotFoundError:
        return []
    except OSError as exc:
        fail("cannot list %s: %s" % (path, exc))


def prune_union(orphaned, dead=()):
    """Drop what lost its last claim, plus docs for tools no longer exposed.

    A dead claim's vault goes with it: the folder that could still read those
    secrets is gone, and nothing else on the computer is meant to.
    """
    gone = []
    for name in orphaned:
        for path in (os.path.join(UNION_TOOLS, name + ".py"),
                     os.path.join(UNION_DOCS, name)):
            if remove(path):
                gone.append(path)
    for claim in sorted(dead):
        if VAULTS_DIR and remove(os.path.join(VAULTS_DIR, claim + ".json")):
            gone.append(os.path.join(VAULTS_DIR, claim + ".json"))
    # Inside a server this workspace does own, capability consent can withdraw
    # individual tools. The wrapper module is rewritten whole, the doc
    # directory is not, and the tool guide sends the agent here to find out
    # what a server can do -- so a doc left behind is the agent reading that it
    # can place live orders and telling the user so, on a connection whose
    # owner declined exactly that.
    for name, expected in EXPECTED_DOCS.items():
        server_docs = os.path.join(UNION_DOCS, name)
        keep = set(expected)
        for entry in listdir(server_docs):
            if entry.endswith(".md") and entry not in keep:
                path = os.path.join(server_docs, entry)
                if remove(path):
                    gone.append(path)
    # The client moved to _internal/src so the wrappers can import it by an
    # absolute name; the copy beside them is unreachable.
    if remove(LEGACY_CLIENT):
        gone.append(LEGACY_CLIENT)
    return gone


def sweep_overlay():
    """Withdraw from this workspace what it no longer enables.

    The enforcing half: a server this workspace stopped enabling loses its
    symlink and its docs here even though the union keeps them for a sibling.
    """
    gone = []
    for entry in listdir(WS_TOOLS):
        if entry in WS_KEEP:
            continue
        path = os.path.join(WS_TOOLS, entry)
        if remove(path):
            gone.append(path)
    if WS_DOCS:
        for entry in listdir(WS_DOCS):
            if entry in WS_DOCS_KEEP:
                continue
            path = os.path.join(WS_DOCS, entry)
            if remove(path):
                gone.append(path)
    return gone


def link_overlay():
    """Point this workspace's tool package at the union's wrappers and docs."""
    if WS_DOCS:
        os.makedirs(WS_DOCS, exist_ok=True)
    for link, target in LINKS:
        # A symlink planted over a real directory of the same name lands
        # inside it, so an older overlay's copied doc tree has to go first.
        remove(link)
        try:
            os.symlink(target, link)
        except OSError as exc:
            fail("cannot link %s -> %s: %s" % (link, target, exc))
    return len(LINKS)


handle = acquire_flock()
try:
    before = read_ledger()
    dead = dead_claims({
        claim: dir_name
        for claim, dir_name in (before.get("dirs") or {}).items()
        if isinstance(dir_name, str)
    })
    ledger, orphaned = merge_ledger(before)
    config = dict(WS_CONFIG)
    config["computer_config_version"] = ledger["config_version"]
    write_json(WS_CONFIG_PATH, config)
    pruned = prune_union(orphaned, dead)
    swept = sweep_overlay()
    linked = link_overlay()
    if ARGS.get("toolVersion"):
        ledger["tool_versions"][CLAIM] = ARGS["toolVersion"]
    else:
        ledger["tool_versions"].pop(CLAIM, None)
    write_json(LEDGER, ledger)
finally:
    fcntl.flock(handle, fcntl.LOCK_UN)
    handle.close()

print(json.dumps({
    "status": "ok",
    "unionVersion": ledger["union_version"],
    "configVersion": ledger["config_version"],
    "unionServers": sorted(ledger["claims"]),
    "orphaned": orphaned,
    "pruned": pruned,
    "swept": swept,
    "linked": linked,
}))
''')


def _build_command(args_path: str) -> str:
    script_b64 = base64.b64encode(
        _SCRIPT.replace("__ARGS_PATH__", args_path).encode()
    ).decode()
    return (
        f"python3 -c \"import base64;exec(base64.b64decode('{script_b64}').decode())\""
    )


async def install_tool_modules(
    sandbox: "PTCSandbox", *, project: ProjectContext, tool_version: str | None = None
) -> None:
    """Publish this workspace's wrappers and reconcile the computer's union.

    Two round trips: one batch upload of everything this workspace generates,
    then one in-sandbox pass that merges the claim ledger, prunes what lost its
    last claim, and relinks the overlay, all under the union's flock.
    """
    layout = SandboxLayout(sandbox._work_dir)
    workspace = layout.for_workspace(project.dir_name)

    # Trust is computed ONCE here, per server, and passed across the codegen
    # boundary as a bool -- codegen never re-derives it from the raw source
    # string (that duplication is how user-level servers once slipped through
    # the workspace-only gates).
    untrusted_by_name = {
        s.name: is_untrusted_server(s) for s in sandbox.config.mcp.servers
    }

    assert sandbox.mcp_registry is not None
    assert sandbox.runtime is not None
    tools_by_server = sandbox.mcp_registry.get_all_tools()
    server_names = sorted(tools_by_server)
    # Union keys: a workspace-local server is filed under its owner's claim so
    # a sibling's same-named server never replaces it. The overlay links the
    # display name to the keyed file.
    keys_by_name = {
        s.name: union_key(s, project.claim) for s in sandbox.config.mcp.servers
    }
    key_of = {name: keys_by_name.get(name, name) for name in server_names}
    vault_file = workspace_vault_path(layout.root, project.claim)

    uploads: list[tuple[bytes, str]] = []

    # The client ships with this workspace's own server entries as a floor and
    # folds in the union at import, reading the ledger the reconcile writes.
    # Embedding the merged set here instead would mean composing the client
    # against a ledger read before the lock was taken.
    enabled_servers = [
        server for server in sandbox.config.mcp.servers if server.enabled
    ]
    my_config = sandbox.tool_generator.generate_client_config(
        enabled_servers,
        working_dir=sandbox._work_dir,
        claim=project.claim,
        vault_file=vault_file,
    )
    my_keys = set(key_of.values())
    my_entries = {
        key: entry
        for key, entry in (my_config.get("servers") or {}).items()
        if key in my_keys
    }
    uploads.append(
        (
            sandbox.tool_generator.compose_mcp_client_code(my_config).encode("utf-8"),
            f"{layout.internal_src}/mcp_client.py",
        )
    )

    expected_docs: dict[str, list[str]] = {}
    for server_name, tools in tools_by_server.items():
        # Fail closed: a server present in the registry but missing from the
        # trust map (config drift mid-sync) is treated as untrusted -- a wrong
        # guess here costs sanitization, not a docstring breakout.
        untrusted = untrusted_by_name.get(server_name, True)
        key = key_of[server_name]
        uploads.append(
            (
                sandbox.tool_generator.generate_tool_module(
                    server_name, tools, untrusted=untrusted, union_key=key
                ).encode("utf-8"),
                f"{layout.tools}/{key}.py",
            )
        )
        names: list[str] = []
        for tool in tools:
            name = f"{doc_name(tool.name, untrusted)}.md"
            names.append(name)
            uploads.append(
                (
                    sandbox.tool_generator.generate_tool_documentation(
                        tool, untrusted=untrusted
                    ).encode("utf-8"),
                    f"{layout.tools_docs}/{key}/{name}",
                )
            )
        expected_docs[key] = names

    # ``__init__.py`` for the union directory. Nothing imports it by that name
    # any more (the wrappers reach the client as a top-level ``mcp_client``),
    # but a sandbox whose overlay has not been built yet still resolves
    # ``tools.<server>`` here, and that is the whole bridge for a warm reuse.
    uploads.append((_TOOLS_INIT_BODY.encode("utf-8"), layout.tools_init))
    uploads.append((_TOOLS_INIT_BODY.encode("utf-8"), f"{workspace.tools}/__init__.py"))

    args_path = f"{layout.internal}/.union_args.{uuid.uuid4().hex}.json"
    args: dict[str, Any] = {
        "ledger": layout.union_ledger,
        "lock": layout.union_lock,
        "claim": project.claim,
        "toolVersion": tool_version,
        "root": layout.root,
        "dirName": project.dir_name or "",
        "servers": my_entries,
        "unionTools": layout.tools,
        "unionDocs": layout.tools_docs,
        "wsTools": workspace.tools,
        "wsDocs": workspace.tools_docs,
        "wsKeep": sorted(
            {f"{name}.py" for name in server_names}
            | {"__init__.py", _WS_DOCS_DIRNAME, _WS_CONFIG_BASENAME}
        ),
        "wsDocsKeep": server_names,
        "vaultsDir": f"{layout.root}/{VAULTS_DIR}",
        "wsConfigPath": workspace.mcp_client_config,
        "wsConfig": sandbox.tool_generator.generate_workspace_tool_config(
            project.workspace_id,
            project.dir_name or "",
            sorted(my_keys),
            labels={key: name for name, key in key_of.items()},
            vault_file=vault_file,
        ),
        "expectedDocs": expected_docs,
        "links": [
            list(pair)
            for pair in overlay_link_plan(layout, workspace, server_names, key_of)
        ],
        "legacyClient": f"{layout.tools}/mcp_client.py",
    }
    uploads.append((json.dumps(args).encode("utf-8"), args_path))

    await sandbox._runtime_call(
        sandbox.runtime.upload_files,
        uploads,
        retry_policy=RetryPolicy.SAFE,
    )

    result = await sandbox._runtime_call(
        sandbox.runtime.exec,
        _build_command(args_path),
        retry_policy=RetryPolicy.SAFE,
    )
    stdout = (getattr(result, "stdout", "") or "").strip()
    try:
        payload = json.loads(stdout.splitlines()[-1]) if stdout else {}
    except (json.JSONDecodeError, IndexError):
        raise ToolOverlayError(
            f"unparsable tool-overlay output: {stdout[:500]!r}"
        ) from None
    if payload.get("status") != "ok":
        raise ToolOverlayError(
            payload.get("error") or "the tool-overlay reconcile failed"
        )

    logger.info(
        "Tool modules installed",
        servers=len(tools_by_server),
        tools=sum(len(t) for t in tools_by_server.values()),
        union_servers=len(payload.get("unionServers") or ()),
        union_version=payload.get("unionVersion"),
        config_version=payload.get("configVersion"),
        orphaned=payload.get("orphaned") or [],
        workspace_dir=project.dir_name or "<root>",
    )
