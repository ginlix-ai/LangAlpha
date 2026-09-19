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
from ptc_agent.core.sandbox.vault_helper import (
    VAULT_MODULE_SOURCE,
    workspace_vault_path,
)
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
        self._ns: dict = {"os": os, "ROOT": root}
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
# Same-name servers from different workspaces, and each workspace's own vault.
# ---------------------------------------------------------------------------


def _ws_server(name, url):
    return MCPServerConfig(
        name=name,
        transport="http",
        url=url,
        source="workspace",
        headers={"Authorization": "Bearer ${vault:API_KEY}"},
    )


def _user_server(name, url):
    return MCPServerConfig(
        name=name, transport="http", url=url, source="user", headers={"X-K": "k"}
    )


class TestUnionKeys:
    """A workspace-local server is keyed by its owner; everything shared is not."""

    def test_only_a_workspace_local_server_is_qualified(self):
        assert tg.union_key(_ws_server("crm", "https://a"), "ws-a") == "crm@ws-a"
        assert tg.union_key(_user_server("crm", "https://a"), "ws-a") == "crm"
        builtin = MCPServerConfig(name="market", transport="stdio", command="uv")
        assert tg.union_key(builtin, "ws-a") == "market"

    def test_the_link_keeps_the_display_name_and_targets_the_key(self):
        layout = SandboxLayout(ROOT)
        workspace = layout.for_workspace(DIR_NAME)
        plan = overlay_link_plan(layout, workspace, ["crm"], {"crm": "crm@ws-a"})
        assert [link for link, _ in plan] == [
            f"{ROOT}/{DIR_NAME}/.agents/tools/crm.py",
            f"{ROOT}/{DIR_NAME}/.agents/tools/docs/crm",
        ]
        layout = SandboxLayout(ROOT)
        assert [_resolve(link, target) for link, target in plan] == [
            f"{layout.tools}/crm@ws-a.py",
            f"{layout.tools_docs}/crm@ws-a",
        ]

    def test_the_wrapper_calls_the_key_and_documents_the_name(self):
        gen = ToolFunctionGenerator()
        code = gen.generate_tool_module(
            "crm", [_tool()], untrusted=True, union_key="crm@ws-a"
        )
        assert "_call_mcp_tool('crm@ws-a', 'get_quote', arguments)" in code
        assert "MCP server: crm\n" in code
        assert "crm@ws-a MCP server" not in code

    def test_the_client_config_keys_by_claim_and_names_the_vault(self):
        gen = ToolFunctionGenerator()
        cfg = gen.generate_client_config(
            [_ws_server("crm", "https://a"), _user_server("sec", "https://s")],
            working_dir=ROOT,
            claim="ws-a",
            vault_file=f"{ROOT}/_internal/vaults/ws-a.json",
        )
        assert set(cfg["servers"]) == {"crm@ws-a", "sec"}
        assert cfg["servers"]["crm@ws-a"]["name"] == "crm"
        assert cfg["servers"]["crm@ws-a"]["vault_file"] == (
            f"{ROOT}/_internal/vaults/ws-a.json"
        )
        # A shared server reads the root vault, so it carries no file of its own.
        assert "name" not in cfg["servers"]["sec"]
        assert "vault_file" not in cfg["servers"]["sec"]
        assert cfg["fold_union"] is True

    def test_without_a_claim_the_config_keeps_bare_names(self):
        cfg = ToolFunctionGenerator().generate_client_config(
            [_ws_server("crm", "https://a")], working_dir=ROOT, fold_union=False
        )
        assert set(cfg["servers"]) == {"crm"}
        assert cfg["fold_union"] is False

    def test_the_workspace_view_labels_keys_and_names_the_vault(self):
        view = ToolFunctionGenerator().generate_workspace_tool_config(
            "ws-a",
            DIR_NAME,
            ["crm@ws-a", "sec"],
            labels={"crm@ws-a": "crm", "sec": "sec"},
            vault_file=f"{ROOT}/_internal/vaults/ws-a.json",
        )
        assert view["servers"] == {
            "crm@ws-a": {"enabled": True, "name": "crm"},
            "sec": {"enabled": True},
        }
        assert view["vault_file"] == f"{ROOT}/_internal/vaults/ws-a.json"


class _Computer:
    """Two workspaces syncing onto one real root, through the real script."""

    def __init__(self, root: str):
        self.root = root
        self.layout = SandboxLayout(root)
        self.gen = ToolFunctionGenerator()
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
    ) -> dict:
        ws = self.layout.for_workspace(project.dir_name)
        os.makedirs(ws.tools, exist_ok=True)
        keys = {s.name: tg.union_key(s, project.claim) for s in servers}
        names = sorted(keys)
        vault_file = workspace_vault_path(self.root, project.claim)
        cfg = self.gen.generate_client_config(
            servers, working_dir=self.root, claim=project.claim, vault_file=vault_file
        )
        for name, key in keys.items():
            with open(f"{self.layout.tools}/{key}.py", "w", encoding="utf-8") as fh:
                fh.write(
                    self.gen.generate_tool_module(
                        name, [_tool()], untrusted=True, union_key=key
                    )
                )
            os.makedirs(f"{self.layout.tools_docs}/{key}", exist_ok=True)
        args = {
            "ledger": self.layout.union_ledger,
            "lock": self.layout.union_lock,
            "claim": project.claim,
            "toolVersion": tool_version,
            "root": self.root,
            "dirName": project.dir_name or "",
            "servers": cfg["servers"],
            "unionTools": self.layout.tools,
            "unionDocs": self.layout.tools_docs,
            "wsTools": ws.tools,
            "wsDocs": ws.tools_docs,
            "wsKeep": sorted(
                {f"{n}.py" for n in names}
                | {"__init__.py", "docs", "mcp_client_config.json"}
            ),
            "wsDocsKeep": names,
            "vaultsDir": f"{self.root}/_internal/vaults",
            "wsConfigPath": ws.mcp_client_config,
            "wsConfig": self.gen.generate_workspace_tool_config(
                project.workspace_id,
                project.dir_name or "",
                sorted(keys.values()),
                labels={v: k for k, v in keys.items()},
                vault_file=vault_file,
            ),
            "expectedDocs": {key: [] for key in keys.values()},
            "links": [
                list(pair) for pair in overlay_link_plan(self.layout, ws, names, keys)
            ],
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


class TestSameNameServersAcrossWorkspaces:
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

    def test_both_workspaces_keep_their_own_crm(self, computer):
        computer.sync(A, [_ws_server("crm", "https://a.example/mcp")])
        computer.sync(B, [_ws_server("crm", "https://b.example/mcp")])

        servers = computer.ledger()["servers"]
        assert servers["crm@ws-a"]["url"] == "https://a.example/mcp"
        assert servers["crm@ws-b"]["url"] == "https://b.example/mcp"
        for project in (A, B):
            link = f"{computer.root}/{project.dir_name}/.agents/tools/crm.py"
            assert os.path.realpath(link) == (
                f"{computer.layout.tools}/crm@{project.workspace_id}.py"
            )
            with open(link, encoding="utf-8") as fh:
                assert f"_call_mcp_tool('crm@{project.workspace_id}'" in fh.read()

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

    def test_a_deleted_workspace_takes_its_vault_and_its_wrapper(self, computer):
        os.makedirs(f"{computer.root}/_internal/vaults")
        for project in (A, B):
            with open(workspace_vault_path(computer.root, project.claim), "w") as fh:
                json.dump({"API_KEY": project.workspace_id}, fh)
        computer.sync(A, [_ws_server("crm", "https://a")])
        computer.sync(B, [_ws_server("crm", "https://b")])

        shutil.rmtree(f"{computer.root}/{A.dir_name}")
        computer.sync(B, [_ws_server("crm", "https://b")])

        assert not os.path.exists(workspace_vault_path(computer.root, "ws-a"))
        assert os.path.exists(workspace_vault_path(computer.root, "ws-b"))
        assert not os.path.exists(f"{computer.layout.tools}/crm@ws-a.py")
        assert os.path.exists(f"{computer.layout.tools}/crm@ws-b.py")


class TestRuntimeReadsEachWorkspacesVault:
    """The client resolves a keyed server against its owner's vault, a shared
    one against the root, and the daemon's rotation stamp covers both."""

    def _apply(self, computer, project, servers, **options):
        cfg = computer.gen.generate_client_config(
            servers,
            working_dir=computer.root,
            claim=project.claim,
            vault_file=workspace_vault_path(computer.root, project.claim),
            **options,
        )
        runtime._apply_config_dict(cfg)

    def test_secret_names_shadow_per_workspace(self, computer):
        os.makedirs(f"{computer.root}/_internal/vaults")
        with open(computer.layout.vault_secrets, "w") as fh:
            json.dump({"API_KEY": "user-tier"}, fh)
        with open(workspace_vault_path(computer.root, "ws-a"), "w") as fh:
            json.dump({"API_KEY": "a-tier"}, fh)
        try:
            self._apply(
                computer,
                A,
                [_ws_server("crm", "https://a"), _ws_server("sec", "https://s")],
            )
            keyed = runtime._server_cfg("crm@ws-a")
            assert keyed.label == "crm"
            assert runtime._resolve_all(keyed, ["${vault:API_KEY}"]) == ["a-tier"]
            shared = runtime._normalize("sec", {"transport": "http", "url": "u"})
            assert runtime._resolve_all(shared, ["${vault:API_KEY}"]) == ["user-tier"]
            stamped = [row[0] for row in runtime.secret_fingerprint()]
            assert workspace_vault_path(computer.root, "ws-a") in stamped
            assert computer.layout.vault_secrets in stamped
        finally:
            runtime._apply_config_dict({})

    def test_a_discovery_client_does_not_fold_the_union(self, computer):
        computer.sync(A, [_ws_server("crm", "https://a.example/pre-edit")])
        try:
            edited = _ws_server("crm", "https://a.example/edited")
            runtime._apply_config_dict(
                computer.gen.generate_client_config(
                    [edited], working_dir=computer.root, fold_union=False
                )
            )
            assert set(runtime._SERVER_CONFIGS) == {"crm"}
            assert runtime._server_cfg("crm").url == "https://a.example/edited"
            # The default still folds the ledger in.
            self._apply(computer, A, [edited])
            assert "crm@ws-a" in runtime._SERVER_CONFIGS
        finally:
            runtime._apply_config_dict({})

    def test_secret_dependencies_are_per_server(self, computer):
        vault_a = workspace_vault_path(computer.root, "ws-a")
        vault_b = workspace_vault_path(computer.root, "ws-b")
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
                            "vault_file": vault_a,
                        },
                        "b": {
                            "transport": "http",
                            "untrusted": True,
                            "url": "https://b",
                            "headers": {"Authorization": "${vault:B}"},
                            "vault_file": vault_b,
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

            assert runtime.server_secret_dependencies("a") == [vault_a]
            assert runtime.server_secret_dependencies("b") == [vault_b]
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


class TestVaultHelperFollowsTheFolder:
    def test_a_folder_reads_its_own_vault_and_the_root_reads_the_users(self, computer):
        os.makedirs(f"{computer.root}/_internal/vaults")
        with open(computer.layout.vault_secrets, "w") as fh:
            json.dump({"API_KEY": "user-tier"}, fh)
        with open(workspace_vault_path(computer.root, "ws-a"), "w") as fh:
            json.dump({"API_KEY": "a-tier"}, fh)
        computer.sync(A, [_ws_server("crm", "https://a")])
        with open(f"{computer.layout.internal_src}/vault.py", "w") as fh:
            fh.write(VAULT_MODULE_SOURCE)
        deep = f"{computer.root}/{A.dir_name}/reports/q3"
        os.makedirs(deep)

        def read(cwd):
            out = subprocess.run(
                [sys.executable, "-c", "import vault; print(vault.get('API_KEY'))"],
                cwd=cwd,
                env={**os.environ, "PYTHONPATH": computer.layout.internal_src},
                capture_output=True,
                text=True,
            )
            assert out.returncode == 0, out.stderr
            return out.stdout.strip()

        assert read(deep) == "a-tier"
        assert read(computer.root) == "user-tier"
