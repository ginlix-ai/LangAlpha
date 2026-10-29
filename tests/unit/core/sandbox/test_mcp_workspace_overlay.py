"""The per-workspace tool overlay on the computer-wide wrapper union.

Pure functions and a temporary directory: no Docker, no provider, no network.
The four contracts it pins are the ones a warm sandbox silently depends on --
the symlink targets, the union ledger, the wrapper preamble, and the layout
names transcribed into the uploaded client. The ledger's merge runs inside the
sandbox now, so it is compiled out of the reconcile script and driven over a
real root rather than called as a host-side function.
"""

import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

from ptc_agent.agent.middleware.tool.code_validation import CodeValidationMiddleware
from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core import tool_generator as tg
from ptc_agent.core.mcp_schema import MCPToolInfo
from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from ptc_agent.core.project_context import ROOT_CLAIM, ProjectContext
from ptc_agent.core.sandbox import mcp_client_runtime as runtime
from ptc_agent.core.sandbox.tool_overlay import (
    _SCRIPT,
    _build_command,
    _relative_link_target,
    overlay_link_plan,
)
from ptc_agent.core.sandbox.vault_helper import VAULT_MODULE_SOURCE
from ptc_agent.core.tool_generator import (
    MCP_CLIENT_CODEGEN_VERSION,
    ToolFunctionGenerator,
)

ROOT = "/home/workspace"
DIR_NAME = "acme-a1b2"


def _resolve(link: str, target: str) -> str:
    """Where a symlink at ``link`` holding ``target`` actually lands."""
    return os.path.normpath(os.path.join(os.path.dirname(link), target))


class TestOverlaySymlinkRelativity:
    """The mirror stores a link's target verbatim, so a target has to be
    relative and has to resolve to the union from its own depth."""

    def test_split_computer_targets(self):
        layout = SandboxLayout(ROOT)
        plan = overlay_link_plan(layout, layout.for_workspace(DIR_NAME), ["market"])
        assert plan == [
            (
                f"{ROOT}/{DIR_NAME}/.agents/tools/market.py",
                "../../../_internal/tools/market.py",
            ),
            (
                f"{ROOT}/{DIR_NAME}/.agents/tools/docs/market",
                "../../../../.agents/tools/docs/market",
            ),
        ]

    def test_an_unsplit_root_has_no_docs_tier_of_its_own(self):
        # Its overlay and the union would be one directory, so the workspace
        # tier declines to name it at all: a doc link would point at itself,
        # and the sweep that reads the same name would take a sibling's docs
        # with it. The plan is wrappers only.
        layout = SandboxLayout(ROOT)
        workspace = layout.for_workspace("")
        assert workspace.tools_docs is None
        plan = overlay_link_plan(layout, workspace, ["market"])
        assert plan == [
            (f"{ROOT}/.agents/tools/market.py", "../../_internal/tools/market.py"),
        ]

    @pytest.mark.parametrize("dir_name", ["", DIR_NAME])
    def test_every_target_resolves_to_the_union(self, dir_name):
        # The property that actually matters: the literals above are only one
        # spelling of it, and this holds at both tiers.
        layout = SandboxLayout(ROOT)
        names = ["market", "sec"]
        workspace = layout.for_workspace(dir_name)
        plan = overlay_link_plan(layout, workspace, names)
        union_paths = []
        for name in names:
            union_paths.append(f"{layout.tools}/{name}.py")
            if workspace.tools_docs is not None:
                union_paths.append(f"{layout.tools_docs}/{name}")
        assert len(plan) == len(union_paths)
        for (link, target), union_path in zip(plan, union_paths):
            assert not target.startswith("/"), target
            assert _resolve(link, target) == union_path

    def test_plan_keeps_server_order_and_pairs_a_doc_per_server(self):
        layout = SandboxLayout(ROOT)
        names = ["zeta", "alpha", "market"]
        plan = overlay_link_plan(layout, layout.for_workspace(DIR_NAME), names)
        assert [link for link, _ in plan[::2]] == [
            f"{ROOT}/{DIR_NAME}/.agents/tools/{name}.py" for name in names
        ]
        assert [link for link, _ in plan[1::2]] == [
            f"{ROOT}/{DIR_NAME}/.agents/tools/docs/{name}" for name in names
        ]

    def test_relative_link_target_never_returns_an_absolute_path(self):
        link = f"{ROOT}/{DIR_NAME}/.agents/tools/market.py"
        union_path = f"{ROOT}/_internal/tools/market.py"
        target = _relative_link_target(link, union_path)
        assert not target.startswith("/")
        assert _resolve(link, target) == union_path


_STDIO_ENTRY = {
    "transport": "stdio",
    "untrusted": False,
    "command": "uv",
    "args": ["run", "python", "/home/workspace/mcp_servers/market.py"],
}
_HTTP_ENTRY = {
    "transport": "http",
    "untrusted": True,
    "url": "https://example.com/mcp",
}


class _Union:
    """The reconcile's own merge, over a real computer root.

    The merge lives in the sandbox now, under the union's flock, so there is no
    host-side copy left to import; the functions are compiled out of the script
    source so these assertions land on the code that actually runs rather than
    on a transcription of it. The root is a real directory because a claim's
    liveness is its folder being there, which is the one thing the merge asks
    the filesystem.
    """

    def __init__(self, root: str):
        self._root = root
        module = ast.parse(_SCRIPT)
        wanted = {"dead_claims", "merge_ledger"}
        fns = [
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name in wanted
        ]
        assert {fn.name for fn in fns} == wanted
        # The script reads its inputs as module globals, filled from the args
        # file the host uploads beside the wrappers.
        self._ns: dict = {"os": os, "ROOT": root, "HELD": set()}
        exec(
            compile(ast.Module(body=fns, type_ignores=[]), "<tool_overlay>", "exec"),
            self._ns,
        )

    @staticmethod
    def dir_name(claim: str) -> str:
        return f"{claim}-ab12"

    def sync(self, ledger: dict, claim: str, servers: dict) -> tuple[dict, list[str]]:
        """One workspace's reconcile pass. Its folder exists by then: the sync
        that runs this is the same one that created it."""
        os.makedirs(os.path.join(self._root, self.dir_name(claim)), exist_ok=True)
        self._ns.update(CLAIM=claim, DIR_NAME=self.dir_name(claim), SERVERS=servers)
        return self._ns["merge_ledger"](ledger)

    def delete_workspace(self, claim: str) -> None:
        os.rmdir(os.path.join(self._root, self.dir_name(claim)))


@pytest.fixture
def union(tmp_path) -> _Union:
    return _Union(str(tmp_path))


class TestUnionLedger:
    def test_the_first_claim_creates_the_entry(self, union):
        ledger, orphaned = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        assert ledger["schema_version"] == 1
        assert ledger["union_version"] == 1
        assert ledger["config_version"] == 1
        assert ledger["claims"] == {"market": ["ws1"]}
        assert ledger["servers"] == {"market": _STDIO_ENTRY}
        # The folder rides along because it is the claim's only liveness
        # signal: see the retraction test below.
        assert ledger["dirs"] == {"ws1": union.dir_name("ws1")}
        assert orphaned == []

    def test_a_second_workspace_adds_its_claim_and_bumps_the_version(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        ledger, orphaned = union.sync(first, "ws2", {"market": _STDIO_ENTRY})
        assert ledger["claims"] == {"market": ["ws1", "ws2"]}
        assert ledger["union_version"] == 2
        assert ledger["config_version"] == first["config_version"]
        assert orphaned == []

    def test_changing_an_entry_bumps_the_config_version(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        changed = {**_STDIO_ENTRY, "command": "uvx"}

        ledger, _ = union.sync(first, "ws1", {"market": changed})

        assert ledger["union_version"] == first["union_version"] + 1
        assert ledger["config_version"] == first["config_version"] + 1

    def test_old_ledger_uses_union_version_as_its_config_version(self, union):
        old = {
            "schema_version": 1,
            "union_version": 7,
            "claims": {"market": ["ws1"]},
            "dirs": {"ws1": union.dir_name("ws1")},
            "servers": {"market": _STDIO_ENTRY},
        }

        ledger, _ = union.sync(old, "ws1", {"market": _STDIO_ENTRY})

        assert ledger["union_version"] == 7
        assert ledger["config_version"] == 7

    def test_dropping_the_last_claim_orphans_the_server(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        ledger, orphaned = union.sync(first, "ws1", {})
        assert orphaned == ["market"]
        assert ledger["claims"] == {}
        assert ledger["servers"] == {}

    def test_a_sibling_claim_keeps_the_entry_alive(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        shared, _ = union.sync(first, "ws2", {"market": _STDIO_ENTRY})
        ledger, orphaned = union.sync(shared, "ws1", {})
        assert orphaned == []
        assert ledger["claims"] == {"market": ["ws2"]}
        # The config survives the dropping workspace's sync, or the sibling's
        # wrappers would raise "Unknown MCP server" until it syncs again.
        assert ledger["servers"]["market"] == _STDIO_ENTRY

    def test_re_running_the_same_merge_changes_nothing(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        again, orphaned = union.sync(first, "ws1", {"market": _STDIO_ENTRY})
        assert again == first
        assert again["union_version"] == first["union_version"]
        assert orphaned == []

    def test_the_ledger_round_trips_through_json(self, union):
        ledger, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY, "sec": _HTTP_ENTRY})
        assert json.loads(json.dumps(ledger, sort_keys=True, indent=2)) == ledger

    def test_the_callers_ledger_is_not_mutated(self, union):
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY})
        before = json.dumps(first, sort_keys=True)
        union.sync(first, "ws2", {"sec": _HTTP_ENTRY})
        assert json.dumps(first, sort_keys=True) == before

    def test_a_deleted_workspaces_claim_is_retracted_by_a_siblings_sync(self, union):
        """The reason the merge had to move into the sandbox.

        Host-side it could only ever drop the claim of the workspace it was
        syncing for, so a deleted project's claim pinned its wrappers in the
        union for the life of the computer and nothing left could release them.
        In here the folder's absence is the liveness signal, and any sibling's
        pass collects it: the shared server keeps only the live holder, and the
        one the dead workspace held alone is reported orphaned for the prune.
        """
        first, _ = union.sync({}, "ws1", {"market": _STDIO_ENTRY, "sec": _HTTP_ENTRY})
        shared, _ = union.sync(first, "ws2", {"market": _STDIO_ENTRY})
        union.delete_workspace("ws1")

        ledger, orphaned = union.sync(shared, "ws2", {"market": _STDIO_ENTRY})

        assert ledger["claims"] == {"market": ["ws2"]}
        assert orphaned == ["sec"]
        assert "ws1" not in ledger["dirs"]

    def test_the_ledger_path_is_the_one_warm_computers_carry(self):
        # A rename reads as "no claims yet" on every existing computer, which
        # rebuilds the union from one workspace and sweeps its siblings. The
        # constant is the whole relative path now, because the uploaded client
        # resolves it against the work dir with no union directory to join it
        # to.
        assert SandboxLayout.UNION_LEDGER_FILE == "_internal/tools/.union.json"
        assert SandboxLayout(ROOT).union_ledger == f"{ROOT}/_internal/tools/.union.json"

    def test_a_computer_with_no_folder_claims_under_the_root_name(self):
        assert ProjectContext("", "").claim == ROOT_CLAIM
        assert ProjectContext("ws-1", DIR_NAME).claim == "ws-1"


def _tool(name: str = "get_quote") -> MCPToolInfo:
    return MCPToolInfo(
        name=name,
        description="Quote for a ticker.\n\nReturns:\n    dict: quote payload",
        input_schema={
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        },
        server_name="market",
    )


class TestWrapperPreamble:
    """A wrapper reaches the client as a top-level ``mcp_client``, which is the
    half of the contract that pairs with ``_internal/src`` on PYTHONPATH."""

    def test_the_client_import_is_absolute(self):
        module = ToolFunctionGenerator().generate_tool_module("market", [_tool()])
        assert "from mcp_client import _call_mcp_tool" in module
        assert "from .mcp_client" not in module
        assert "from ..mcp_client" not in module

    def test_the_wrapper_calls_through_that_import(self):
        module = ToolFunctionGenerator().generate_tool_module("market", [_tool()])
        assert "def get_quote(" in module
        assert '_call_mcp_tool("market", "get_quote", arguments)' in module


class TestWorkspaceToolConfig:
    def _config(self, server_names):
        return ToolFunctionGenerator().generate_workspace_tool_config(
            "ws-1", DIR_NAME, server_names
        )

    def test_shape(self):
        cfg = self._config(["sec", "market"])
        assert cfg["schema_version"] == 1
        assert cfg["workspace_id"] == "ws-1"
        assert cfg["dir_name"] == DIR_NAME
        assert set(cfg["servers"]) == {"market", "sec"}
        assert all(entry["enabled"] is True for entry in cfg["servers"].values())

    def test_the_host_stamps_no_version(self):
        # The union's version is decided by the merge inside the lock, and the
        # reconcile writes it here as ``computer_config_version``. A number
        # guessed host-side would name a union this workspace never saw.
        cfg = self._config(["market"])
        assert "config_version" not in cfg
        assert "computer_config_version" not in cfg

    def test_a_server_this_workspace_did_not_enable_is_absent(self):
        cfg = self._config(["market"])
        assert set(cfg["servers"]) == {"market"}
        assert "sec" not in cfg["servers"]

    def test_json_serializable_with_sorted_keys(self):
        cfg = self._config(["sec", "market"])
        assert json.loads(json.dumps(cfg, sort_keys=True)) == cfg


class TestLayoutTranscriptionDrift:
    """The uploaded client cannot import the layout, so it carries a fallback
    copy of both tiers and reads the host's emitted block over it. Hold the
    fallback equal to the object, and the emission equal to the set the runtime
    actually reads."""

    def test_the_transcription_names_its_source_classes(self):
        assert runtime._LAYOUT_CLASS == SandboxLayout.__name__
        assert runtime._WS_LAYOUT_CLASS == WorkspaceLayout.__name__

    @pytest.mark.parametrize(
        ("layout_class", "fallback"),
        [
            (SandboxLayout, runtime._DEFAULT_LAYOUT),
            (WorkspaceLayout, runtime._DEFAULT_WS_LAYOUT),
        ],
    )
    def test_every_name_the_runtime_reads_is_transcribed_verbatim(
        self, layout_class, fallback
    ):
        for name in layout_class.RUNTIME_CONSTANTS:
            assert fallback[name] == getattr(layout_class, name), name

    def test_the_emitted_block_is_exactly_what_the_runtime_reads(self):
        # Over RUNTIME_CONSTANTS rather than a hand-written list: the emission
        # feeds the codegen version, so a name added to it re-syncs every warm
        # sandbox, and this has to move with the contract instead of pinning a
        # snapshot of it.
        emitted = SandboxLayout(ROOT).as_constants()
        assert set(emitted) == {runtime._LAYOUT_CLASS, runtime._WS_LAYOUT_CLASS}
        assert set(emitted[runtime._LAYOUT_CLASS]) == set(
            SandboxLayout.RUNTIME_CONSTANTS
        )
        assert set(emitted[runtime._WS_LAYOUT_CLASS]) == set(
            WorkspaceLayout.RUNTIME_CONSTANTS
        )

    def test_the_union_ledger_travels_in_the_emitted_block(self):
        # The client folds the union into its server map at import by this
        # name. Left unemitted it would silently fall back to the transcribed
        # default, and a computer whose ledger moved would read no union at all.
        assert "UNION_LEDGER_FILE" in SandboxLayout.RUNTIME_CONSTANTS
        assert {"TOOLS_DIR", "MCP_CLIENT_CONFIG_FILE"} <= set(
            WorkspaceLayout.RUNTIME_CONSTANTS
        )


class TestCodegenVersion:
    def test_shape(self):
        # The major comes from the source, not a literal: it is hand-set for a
        # deliberate architecture shift, and a copy here would fail the whole
        # suite on the shift rather than on a regression.
        assert re.fullmatch(
            rf"{re.escape(tg._WRAPPER_CODEGEN_MAJOR)}\.[0-9a-f]{{12}}",
            MCP_CLIENT_CODEGEN_VERSION,
        )

    def test_derivation_is_deterministic(self):
        def derive() -> str:
            payload = tg.client_runtime_source() + tg._emission_probe_text()
            return "{}.{}".format(
                tg._WRAPPER_CODEGEN_MAJOR,
                hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12],
            )

        first = derive()
        assert first == derive()
        assert first == MCP_CLIENT_CODEGEN_VERSION

    def test_the_emission_probe_is_stable(self):
        assert tg._emission_probe_text() == tg._emission_probe_text()


class TestOverlayIsAgentReadable:
    """The overlay is the tier the agent reads; the union behind it is not."""

    @pytest.mark.parametrize(
        "code",
        [
            "open('acme-a1b2/.agents/tools/docs/market/get_quote.md').read()",
            "from tools.market import get_quote",
        ],
    )
    def test_overlay_reads_pass_validation(self, code):
        assert CodeValidationMiddleware()._check_code(code) is None

    @pytest.mark.parametrize(
        "code",
        [
            "open('/home/workspace/_internal/tools/market.py').read()",
            "open('_internal/src/mcp_client.py').read()",
            "open('.mcp_tokens.json').read()",
        ],
    )
    def test_the_union_and_the_token_file_stay_protected(self, code):
        assert CodeValidationMiddleware()._check_code(code) is not None


# ---------------------------------------------------------------------------
# Several workspaces syncing onto one computer, and the vault they share.
# ---------------------------------------------------------------------------


def _user_server(name, url):
    return MCPServerConfig(
        name=name, transport="http", url=url, source="user", headers={"X-K": "k"}
    )


class _Computer:
    """Two workspaces syncing onto one real root, through the real script."""

    def __init__(self, root: str):
        self.root = root
        self.layout = SandboxLayout(root)
        self.gen = ToolFunctionGenerator()
        self.legacy_vaults = f"{self.layout.internal}/vaults"
        for path in (
            self.layout.tools,
            self.layout.tools_docs,
            self.layout.internal_src,
        ):
            os.makedirs(path, exist_ok=True)

    def sync(
        self,
        project: ProjectContext,
        servers: list[MCPServerConfig],
        *,
        tool_version: str | None = None,
        held: tuple[str, ...] = (),
    ) -> dict:
        ws = self.layout.for_workspace(project.dir_name)
        os.makedirs(ws.tools, exist_ok=True)
        names = sorted(s.name for s in servers)
        cfg = self.gen.generate_client_config(servers, working_dir=self.root)
        for name in names:
            with open(f"{self.layout.tools}/{name}.py", "w", encoding="utf-8") as fh:
                fh.write(self.gen.generate_tool_module(name, [_tool()], untrusted=True))
            os.makedirs(f"{self.layout.tools_docs}/{name}", exist_ok=True)
        args = {
            "ledger": self.layout.union_ledger,
            "lock": self.layout.union_lock,
            "claim": project.claim,
            "toolVersion": tool_version,
            "root": self.root,
            "dirName": project.dir_name or "",
            "servers": cfg["servers"],
            "held": list(held),
            "unionTools": self.layout.tools,
            "unionDocs": self.layout.tools_docs,
            "wsTools": ws.tools,
            "wsDocs": ws.tools_docs,
            "wsKeep": sorted(
                {f"{n}.py" for n in names}
                | {"__init__.py", "docs", "mcp_client_config.json"}
            ),
            "wsDocsKeep": names,
            "wsConfigPath": ws.mcp_client_config,
            "wsConfig": self.gen.generate_workspace_tool_config(
                project.workspace_id, project.dir_name or "", names
            ),
            "expectedDocs": {name: [] for name in names},
            "links": [list(pair) for pair in overlay_link_plan(self.layout, ws, names)],
            "legacyClient": f"{self.layout.tools}/mcp_client.py",
        }
        args_path = f"{self.layout.internal}/.union_args.json"
        with open(args_path, "w", encoding="utf-8") as fh:
            json.dump(args, fh)
        out = subprocess.run(
            _build_command(args_path), shell=True, capture_output=True, text=True
        )
        assert out.returncode == 0, out.stdout + out.stderr
        return json.loads(out.stdout.strip().splitlines()[-1])

    def ledger(self) -> dict:
        with open(self.layout.union_ledger, encoding="utf-8") as fh:
            return json.load(fh)


A = ProjectContext(workspace_id="ws-a", dir_name="alpha-1")
B = ProjectContext(workspace_id="ws-b", dir_name="beta-2")


@pytest.fixture
def computer(tmp_path) -> _Computer:
    root = os.path.realpath(str(tmp_path))
    for project in (A, B):
        os.makedirs(f"{root}/{project.dir_name}")
    return _Computer(root)


class TestWorkspacesShareOneUnion:
    def test_failed_link_does_not_publish_install_version(self, computer, monkeypatch):
        server = _user_server("sec", "https://s")
        computer.sync(A, [server], tool_version="installed")
        monkeypatch.setattr(
            sys.modules[__name__],
            "overlay_link_plan",
            lambda *args: [(f"{computer.root}/missing-parent/link", "target")],
        )

        with pytest.raises(AssertionError, match="cannot link"):
            computer.sync(A, [server], tool_version="not-installed")

        assert computer.ledger()["tool_versions"] == {A.claim: "installed"}

    def test_install_versions_belong_to_each_workspace(self, computer):
        server = _user_server("sec", "https://s")
        computer.sync(A, [server], tool_version="discovered-a")
        computer.sync(B, [server], tool_version="discovered-b")
        assert computer.ledger()["tool_versions"] == {
            A.claim: "discovered-a",
            B.claim: "discovered-b",
        }

        computer.sync(A, [server], tool_version="rediscovered-a")
        assert computer.ledger()["tool_versions"] == {
            A.claim: "rediscovered-a",
            B.claim: "discovered-b",
        }
        shutil.rmtree(f"{computer.root}/{A.dir_name}")
        computer.sync(B, [server], tool_version="discovered-b")
        assert computer.ledger()["tool_versions"] == {B.claim: "discovered-b"}

    def test_a_shared_server_stays_one_entry(self, computer):
        computer.sync(A, [_user_server("sec", "https://s")])
        computer.sync(B, [_user_server("sec", "https://s")])
        ledger = computer.ledger()
        assert set(ledger["servers"]) == {"sec"}
        assert ledger["union_version"] == 2
        assert ledger["config_version"] == 1
        view = computer.layout.for_workspace(B.dir_name).mcp_client_config
        with open(view, encoding="utf-8") as fh:
            assert json.load(fh)["computer_config_version"] == 1
        assert ledger["claims"]["sec"] == ["ws-a", "ws-b"]

    def test_a_deleted_workspace_takes_its_wrapper(self, computer):
        computer.sync(A, [_user_server("crm", "https://a")])
        computer.sync(B, [_user_server("sec", "https://s")])

        shutil.rmtree(f"{computer.root}/{A.dir_name}")
        computer.sync(B, [_user_server("sec", "https://s")])

        assert set(computer.ledger()["servers"]) == {"sec"}
        assert not os.path.exists(f"{computer.layout.tools}/crm.py")
        assert os.path.exists(f"{computer.layout.tools}/sec.py")


class TestAServerTheSyncingWorkerNeverReached:
    """A worker whose boot connect to a built-in server failed has no tools for
    it, while a sibling worker that reached it installed them for the agents it
    serves. The unhealed worker's sync holds that server rather than withdraw
    it, and holding adds nothing the sandbox did not already have."""

    @staticmethod
    def _enabled(computer, project) -> set[str]:
        ws = computer.layout.for_workspace(project.dir_name)
        with open(ws.mcp_client_config, encoding="utf-8") as fh:
            return set(json.load(fh)["servers"])

    def test_a_held_server_keeps_its_wrapper_claim_and_config(self, computer):
        market = MCPServerConfig(name="market", transport="http", url="https://m/mcp")
        crm = _user_server("crm", "https://c")
        computer.sync(A, [market, crm])

        computer.sync(A, [crm], held=("market",))

        ws = computer.layout.for_workspace(A.dir_name)
        assert os.path.exists(f"{ws.tools}/market.py")
        assert os.path.exists(f"{ws.tools_docs}/market")
        ledger = computer.ledger()
        assert ledger["claims"]["market"] == [A.claim]
        assert "market" in ledger["servers"]
        assert self._enabled(computer, A) == {"crm", "market"}

        # Not held, the same sync is the workspace dropping it.
        computer.sync(A, [crm])
        assert not os.path.lexists(f"{ws.tools}/market.py")
        assert not os.path.exists(f"{computer.layout.tools}/market.py")
        assert self._enabled(computer, A) == {"crm"}

    def test_holding_installs_nothing(self, computer):
        computer.sync(A, [_user_server("crm", "https://c")], held=("market",))

        ws = computer.layout.for_workspace(A.dir_name)
        assert not os.path.lexists(f"{ws.tools}/market.py")
        assert "market" not in computer.ledger()["claims"]
        assert self._enabled(computer, A) == {"crm"}


class TestAnOlderUnionHeals:
    """What a warm computer carries from when a workspace could hold its own
    copy of a server: wrappers keyed ``name@<claim>`` and a vault file per
    workspace. Each workspace's own sync retires its keyed entries through the
    claim ledger. The vault files stay: the version that reads them can still
    be serving the computer."""

    @staticmethod
    def _plant_keyed(computer, project, name):
        key = f"{name}@{project.claim}"
        ws = computer.layout.for_workspace(project.dir_name)
        os.makedirs(ws.tools_docs, exist_ok=True)
        with open(f"{computer.layout.tools}/{key}.py", "w", encoding="utf-8") as fh:
            fh.write("# keyed wrapper\n")
        os.makedirs(f"{computer.layout.tools_docs}/{key}")
        for link, target in (
            (f"{ws.tools}/{name}.py", f"{computer.layout.tools}/{key}.py"),
            (f"{ws.tools_docs}/{name}", f"{computer.layout.tools_docs}/{key}"),
        ):
            os.symlink(_relative_link_target(link, target), link)
        vault_file = f"{computer.legacy_vaults}/{project.claim}.json"
        with open(ws.mcp_client_config, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "workspace_id": project.workspace_id,
                    "servers": {key: {"enabled": True, "name": name}},
                    "vault_file": vault_file,
                },
                fh,
            )
        with open(vault_file, "w", encoding="utf-8") as fh:
            json.dump({"API_KEY": project.workspace_id}, fh)
        return key, {**_HTTP_ENTRY, "name": name, "vault_file": vault_file}

    def test_each_workspace_retires_its_own_keyed_entry(self, computer):
        os.makedirs(computer.legacy_vaults)
        key_a, entry_a = self._plant_keyed(computer, A, "crm")
        key_b, entry_b = self._plant_keyed(computer, B, "crm")
        with open(computer.layout.union_ledger, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "schema_version": 1,
                    "union_version": 3,
                    "config_version": 3,
                    "claims": {key_a: [A.claim], key_b: [B.claim]},
                    "dirs": {A.claim: A.dir_name, B.claim: B.dir_name},
                    "servers": {key_a: entry_a, key_b: entry_b},
                    "tool_versions": {A.claim: "old", B.claim: "old"},
                },
                fh,
            )

        # The migration promoted B's copy under a new name; A kept "crm".
        result = computer.sync(A, [_user_server("crm", "https://a")])

        assert result["orphaned"] == [key_a]
        ledger = computer.ledger()
        assert set(ledger["servers"]) == {"crm", key_b}
        # The executable set moved, so a supervisor holding the old one
        # learns it is superseded.
        assert ledger["config_version"] == 4
        assert not os.path.exists(f"{computer.layout.tools}/{key_a}.py")
        assert not os.path.exists(f"{computer.layout.tools_docs}/{key_a}")
        ws_a = computer.layout.for_workspace(A.dir_name)
        assert os.path.realpath(f"{ws_a.tools}/crm.py") == (
            f"{computer.layout.tools}/crm.py"
        )
        assert os.path.realpath(f"{ws_a.tools_docs}/crm") == (
            f"{computer.layout.tools_docs}/crm"
        )
        with open(ws_a.mcp_client_config, encoding="utf-8") as fh:
            view = json.load(fh)
        assert view["servers"] == {"crm": {"enabled": True}}
        assert "vault_file" not in view
        assert computer.legacy_vaults not in result["pruned"]
        assert os.path.exists(f"{computer.legacy_vaults}/{B.claim}.json")
        # B's folder is alive, so its keyed wrapper keeps serving it until B
        # syncs for itself.
        assert os.path.exists(f"{computer.layout.tools}/{key_b}.py")

        result = computer.sync(B, [_user_server("crm_2", "https://b")])

        assert result["orphaned"] == [key_b]
        assert set(computer.ledger()["servers"]) == {"crm", "crm_2"}
        assert not os.path.exists(f"{computer.layout.tools}/{key_b}.py")
        ws_b = computer.layout.for_workspace(B.dir_name)
        assert not os.path.lexists(f"{ws_b.tools}/crm.py")
        assert os.path.realpath(f"{ws_b.tools}/crm_2.py") == (
            f"{computer.layout.tools}/crm_2.py"
        )


class TestRuntimeReadsTheRootVault:
    """Every server resolves ``${vault:NAME}`` against the computer root's one
    file, and the daemon's rotation stamp watches that file."""

    def test_a_stale_entry_gets_no_vault(self, computer):
        # A sibling's entry an older host wrote into the ledger, still naming
        # a per-workspace file. The root vault is the account's, where the
        # same name can hold another value, so the call fails instead.
        stale_vault = f"{computer.legacy_vaults}/ws-a.json"
        os.makedirs(computer.legacy_vaults)
        with open(stale_vault, "w") as fh:
            json.dump({"API_KEY": "a-tier"}, fh)
        with open(computer.layout.vault_secrets, "w") as fh:
            json.dump({"API_KEY": "user-tier"}, fh)
        entry = {
            **_HTTP_ENTRY,
            "headers": {"Authorization": "${vault:API_KEY}"},
            "name": "crm",
            "vault_file": stale_vault,
        }
        claimed = {**entry}
        del claimed["vault_file"]
        servers = {"crm@ws-a": entry, "crm": {**entry, "name": "crm"}, "erp@ws-a": claimed}
        with open(computer.layout.union_ledger, "w", encoding="utf-8") as fh:
            json.dump({"config_version": 1, "servers": servers}, fh)
        try:
            runtime._apply_config_dict({"working_dir": computer.root})
            for name in servers:
                cfg = runtime._server_cfg(name)
                with pytest.raises(RuntimeError, match="earlier version"):
                    runtime._resolve_all(cfg, ["${vault:API_KEY}"])
                assert runtime._resolve_all(
                    cfg, ["${vault:API_KEY}"], discovery=True
                ) == [""]
            assert runtime.server_secret_dependencies("crm@ws-a") == [
                computer.layout.vault_secrets
            ]
            stamped = [row[0] for row in runtime.secret_fingerprint()]
            assert computer.layout.vault_secrets in stamped
            assert stale_vault not in stamped
        finally:
            runtime._apply_config_dict({})

    def test_the_data_token_file_is_stamped_only_for_a_server_naming_it(
        self, computer
    ):
        """The server writes a fresh market-data token every half hour and
        its reader picks it up itself; stamping the file for every server
        would respawn each trusted one on every write."""
        tokens = computer.layout.mcp_tokens
        trusted = {"transport": "stdio", "untrusted": False, "command": "builtin"}
        reader = {
            "transport": "stdio",
            "untrusted": True,
            "command": "reader",
            "env": {"DATA_TOKEN_FILE": tokens},
        }
        try:
            runtime._apply_config_dict(
                {
                    "working_dir": computer.root,
                    "fold_union": False,
                    "servers": {"builtin": trusted},
                }
            )
            assert tokens not in [row[0] for row in runtime.secret_fingerprint()]

            runtime._apply_config_dict(
                {
                    "working_dir": computer.root,
                    "fold_union": False,
                    "servers": {"builtin": trusted, "reader": reader},
                }
            )
            assert runtime.server_secret_dependencies("reader") == [tokens]
            assert tokens in [row[0] for row in runtime.secret_fingerprint()]
        finally:
            runtime._apply_config_dict({})

    def test_a_discovery_client_does_not_fold_the_union(self, computer):
        computer.sync(A, [_user_server("crm", "https://a.example/pre-edit")])
        edited = _user_server("crm", "https://a.example/edited")
        try:
            runtime._apply_config_dict(
                computer.gen.generate_client_config(
                    [edited], working_dir=computer.root, fold_union=False
                )
            )
            assert set(runtime._SERVER_CONFIGS) == {"crm"}
            assert runtime._server_cfg("crm").url == "https://a.example/edited"
            # Folding the ledger in reads the pre-edit copy back over it.
            runtime._apply_config_dict(
                computer.gen.generate_client_config([edited], working_dir=computer.root)
            )
            assert runtime._server_cfg("crm").url == "https://a.example/pre-edit"
        finally:
            runtime._apply_config_dict({})

    def test_secret_dependencies_are_per_server(self, computer):
        try:
            runtime._apply_config_dict(
                {
                    "working_dir": computer.root,
                    "fold_union": False,
                    "servers": {
                        "a": {
                            "transport": "stdio",
                            "untrusted": True,
                            "command": "a",
                            "env": {"TOKEN": "${vault:A}"},
                        },
                        "b": {
                            "transport": "http",
                            "untrusted": True,
                            "url": "https://b",
                            "headers": {"Authorization": "${vault:B}"},
                        },
                        "public": {
                            "transport": "http",
                            "untrusted": True,
                            "url": "https://public",
                        },
                        "file-auth": {
                            "transport": "stdio",
                            "untrusted": True,
                            "command": "file-auth",
                            "env": {"CREDENTIAL_FILE": f"{computer.root}/x.json"},
                        },
                        "builtin": {
                            "transport": "stdio",
                            "untrusted": False,
                            "command": "builtin",
                        },
                    },
                }
            )

            vault = computer.layout.vault_secrets
            assert runtime.server_secret_dependencies("a") == [vault]
            assert runtime.server_secret_dependencies("b") == [vault]
            assert runtime.server_secret_dependencies("public") == []
            assert runtime.server_secret_dependencies("file-auth") == [
                f"{computer.root}/x.json"
            ]
            assert runtime.server_secret_dependencies("builtin") is None
        finally:
            runtime._apply_config_dict({})

    def test_old_union_ledger_uses_union_version_for_the_runtime(self, computer):
        old = {
            "schema_version": 1,
            "union_version": 9,
            "servers": {},
        }
        with open(computer.layout.union_ledger, "w", encoding="utf-8") as fh:
            json.dump(old, fh)
        try:
            runtime._apply_config_dict({"working_dir": computer.root})
            assert runtime._CONFIG_VERSION == 9
        finally:
            runtime._apply_config_dict({})

    def test_http_negotiation_holds_the_servers_lock(self, monkeypatch):
        seen = {}

        def negotiate(server_name, discovery):
            seen["owned"] = runtime._get_server_lock(server_name)._is_owned()
            return {"ok": True}

        monkeypatch.setattr(runtime, "_negotiate_http_server", negotiate)
        assert runtime._ensure_http_server("any") == {"ok": True}
        assert seen["owned"] is True


class TestVaultHelperReadsTheRootVault:
    def test_every_current_folder_reads_the_root_vault(self, computer):
        with open(computer.layout.vault_secrets, "w") as fh:
            json.dump({"API_KEY": "user-tier"}, fh)
        # A folder view an older host wrote still names a per-workspace file,
        # while its sibling's view is the current shape.
        stale_vault = f"{computer.legacy_vaults}/ws-a.json"
        os.makedirs(computer.legacy_vaults)
        with open(stale_vault, "w") as fh:
            json.dump({"API_KEY": "a-tier"}, fh)
        for spec, view in (
            (A, {"servers": {}, "vault_file": stale_vault}),
            (B, {"servers": {}}),
        ):
            ws = computer.layout.for_workspace(spec.dir_name)
            os.makedirs(ws.tools)
            with open(ws.mcp_client_config, "w") as fh:
                json.dump(view, fh)
            os.makedirs(f"{computer.root}/{spec.dir_name}/reports/q3")
        with open(f"{computer.layout.internal_src}/vault.py", "w") as fh:
            fh.write(VAULT_MODULE_SOURCE)

        def read(cwd):
            return subprocess.run(
                [sys.executable, "-c", "import vault; print(vault.get('API_KEY'))"],
                cwd=cwd,
                env={**os.environ, "PYTHONPATH": computer.layout.internal_src},
                capture_output=True,
                text=True,
            )

        for cwd in (f"{computer.root}/{B.dir_name}/reports/q3", computer.root):
            out = read(cwd)
            assert out.returncode == 0, out.stderr
            assert out.stdout.strip() == "user-tier"
        # Neither the account's value nor the old file answers for the stale
        # folder: the first can differ from the workspace's own.
        out = read(f"{computer.root}/{A.dir_name}/reports/q3")
        assert out.returncode != 0
        assert "earlier version" in out.stderr
