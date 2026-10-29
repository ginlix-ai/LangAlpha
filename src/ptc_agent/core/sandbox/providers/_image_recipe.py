"""The sandbox image, stated once for every provider that builds it from steps.

Daytona bakes it into a snapshot and E2B into a template. Both builders take the
same ordered list of shell commands, environment settings and a working
directory, so the recipe lives here and each provider only translates the steps
into its own builder. ``Dockerfile.sandbox`` mirrors the same steps by hand.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from ptc_agent.core.mcp_sanitize import is_untrusted_server
from ptc_agent.core.sandbox._defaults import (
    DEFAULT_DEPENDENCIES,
    SANDBOX_FALLBACK_CPU,
    SANDBOX_IMAGE_ENV,
    SANDBOX_NODE_VERSION,
    SANDBOX_PLAYWRIGHT_VERSION,
    sandbox_thread_env,
)

if TYPE_CHECKING:
    from ptc_agent.config.core import CoreConfig

BASE_IMAGE = "ubuntu:24.04"


def builtin_mcp_packages(config: CoreConfig) -> list[str]:
    """NPM packages of the enabled built-in stdio MCP servers.

    They are part of the image's identity, so a template prebuilt outside a
    sandbox has to name the same list the create path does.
    """
    # Built-ins only: user-server npx/uvx packages are fetched at call time
    # inside the sandbox, never pre-installed globally on the host path.
    mcp_packages = []
    for server in config.mcp.servers:
        if is_untrusted_server(server) or not server.enabled:
            continue
        if server.transport == "stdio" and server.command == "npx":
            # Extract package name from npx arguments
            # Format: ["npx", "-y", "package-name", ...]
            if len(server.args) >= 2 and server.args[0] == "-y":
                mcp_packages.append(server.args[1])
    return mcp_packages


class Sized(Protocol):
    """A cpu/memory/disk size: a resource tier, or a provider SDK's equivalent."""

    cpu: Any
    memory: Any
    disk: Any


@dataclass(frozen=True, slots=True)
class Run:
    """Shell commands, one image layer each."""

    commands: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Env:
    values: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class Workdir:
    path: str


Step = Run | Env | Workdir


def image_identity(steps: Sequence[Step], resources: Sized | None) -> dict[str, Any]:
    """Everything that identifies a built image, hashed into its name.

    The steps are hashed as data, so any pin, package or environment value
    they carry renames the image when it changes, with nothing kept beside
    them to fall out of step. ``resources`` (cpu/memory/disk) are part of the
    identity because both providers fix a sandbox's size when the image is
    built, so retuning a tier must yield a new name instead of silently
    reusing the stale-sized image.
    """
    return {
        "base_image": BASE_IMAGE,
        "steps": [_step_data(step) for step in steps],
        "resources": (
            {
                "cpu": resources.cpu,
                "memory": resources.memory,
                "disk": resources.disk,
            }
            if resources is not None
            else None
        ),
    }


def _step_data(step: Step) -> list[Any]:
    if isinstance(step, Run):
        return ["run", list(step.commands)]
    if isinstance(step, Env):
        return ["env", dict(step.values)]
    return ["workdir", step.path]


def config_hash(config: Mapping[str, Any]) -> str:
    """The 8-char name suffix for an ``image_identity``."""
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:8]


def image_steps(
    *,
    working_dir: str,
    mcp_packages: Sequence[str] | None,
    cpu: int | float | None,
    dependencies: Sequence[str] = DEFAULT_DEPENDENCIES,
) -> list[Step]:
    """The build steps, in order, for an image born at ``cpu`` vCPUs.

    ``cpu`` sizes the baked-in BLAS/OpenMP thread caps. An image is already per
    tier, so the tier's cpu count is fixed for every sandbox born from it and
    the image is the right place to carry it. The MCP packages are sorted, so
    the order servers are configured in does not rename the image.

    Every cache is emptied in the same command that filled it: each command is
    its own layer, so a trailing cleanup command only adds a whiteout and leaves
    the bytes in the earlier layer.
    """
    pkgs = sorted(mcp_packages or [])
    cpu = cpu if cpu is not None else SANDBOX_FALLBACK_CPU
    browsers_path = SANDBOX_IMAGE_ENV["PLAYWRIGHT_BROWSERS_PATH"]

    return [
        Run(
            (
                "echo 'debconf debconf/frontend select Noninteractive'"
                " | debconf-set-selections",
                "apt-get update && apt-get install -y"
                " python3 python3-pip python3-venv"
                " gcc gfortran build-essential"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*",
                "ln -sf /usr/bin/python3 /usr/bin/python",
                "ln -sf /usr/bin/pip3 /usr/bin/pip",
                "rm -f /usr/lib/python*/EXTERNALLY-MANAGED",
            )
        ),
        Run(
            (
                # --no-install-recommends, then name what conversion actually
                # needs. The `libreoffice` metapackage's recommends were most of
                # the office stack's footprint: a JRE the headless converter never
                # calls, Mesa/LLVM Vulkan drivers, and the full Noto font set.
                "apt-get update"
                " && apt-get install -y --no-install-recommends"
                " ca-certificates curl ripgrep jq git unzip gcc"
                " poppler-utils pandoc qpdf fuse3"
                " libreoffice-writer libreoffice-calc libreoffice-impress"
                " libreoffice-draw"
                " fonts-noto-cjk fonts-dejavu-core fonts-opensymbol"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*",
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
                # Relocate BOTH uv and uvx — the MCP command allowlist permits
                # `uvx`, so it must be on PATH too (mirrors Dockerfile.sandbox).
                "mv /root/.local/bin/uv /root/.local/bin/uvx /usr/local/bin/",
                # Node.js: pinned direct binary (matches Dockerfile.sandbox;
                # avoids apt-mirror flakiness and unpinned-version drift).
                'NODE_ARCH=$([ "$(dpkg --print-architecture)" = "arm64" ]'
                " && echo arm64 || echo x64)"
                f" && curl -fsSL https://nodejs.org/dist/v{SANDBOX_NODE_VERSION}/"
                f"node-v{SANDBOX_NODE_VERSION}-linux-${{NODE_ARCH}}.tar.xz"
                " -o /tmp/node.tar.xz"
                " && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1"
                " && rm /tmp/node.tar.xz",
                *[f"npm install -g {shlex.quote(pkg)}" for pkg in pkgs],
                # Same pins as Dockerfile.sandbox: the pptx skill and its checks
                # target 4.0.1, and playwright rides the shared version so the npm
                # side resolves the same browser revision as the Python side.
                "npm install -g docx pptxgenjs@4.0.1"
                f" playwright@{SANDBOX_PLAYWRIGHT_VERSION}"
                " && npm cache clean --force",
                "GH_ARCH=$(dpkg --print-architecture)"
                " && curl -fsSL https://github.com/cli/cli/releases/download/"
                "v2.87.3/gh_2.87.3_linux_${GH_ARCH}.tar.gz -o /tmp/gh.tar.gz"
                " && tar -xzf /tmp/gh.tar.gz -C /tmp"
                " && mv /tmp/gh_2.87.3_linux_${GH_ARCH}/bin/gh /usr/local/bin/gh"
                " && rm -rf /tmp/gh.tar.gz /tmp/gh_2.87.3_linux_${GH_ARCH}",
                "POLY_ARCH=$(uname -m)"
                " && curl -fsSL https://github.com/Polymarket/polymarket-cli/"
                "releases/download/v0.1.4/"
                "polymarket-v0.1.4-${POLY_ARCH}-unknown-linux-gnu.tar.gz"
                " -o /tmp/polymarket.tar.gz"
                " && tar -xzf /tmp/polymarket.tar.gz -C /tmp"
                " && mv /tmp/polymarket /usr/local/bin/polymarket"
                " && rm -rf /tmp/polymarket.tar.gz",
                # --with-deps runs its own apt-get update, so the emptied lists
                # above are not a problem; clean again on the way out.
                f"PLAYWRIGHT_BROWSERS_PATH={browsers_path}"
                " npx playwright install --with-deps chromium"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*"
                " && npm cache clean --force && rm -rf /root/.npm/_npx",
                # -- Docker Engine (for interactive-dashboard complex tier) --
                "install -m 0755 -d /etc/apt/keyrings"
                " && curl -fsSL https://download.docker.com/linux/ubuntu/gpg"
                " -o /etc/apt/keyrings/docker.asc"
                " && chmod a+r /etc/apt/keyrings/docker.asc",
                'echo "deb [arch=$(dpkg --print-architecture)'
                " signed-by=/etc/apt/keyrings/docker.asc]"
                " https://download.docker.com/linux/ubuntu"
                ' $(. /etc/os-release && echo $VERSION_CODENAME) stable"'
                " > /etc/apt/sources.list.d/docker.list",
                # docker-ce keeps its recommends: the compose plugin is one of
                # them and the interactive-dashboard tier uses it.
                "apt-get update"
                " && apt-get install -y docker-ce docker-ce-cli containerd.io"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*",
            )
        ),
        # Same values Dockerfile.sandbox sets with ENV, and the same ones
        # PTCSandbox injects per sandbox at create time. Both layers are
        # load-bearing; see SANDBOX_IMAGE_ENV.
        Env(SANDBOX_IMAGE_ENV),
        Env(sandbox_thread_env(cpu)),
        Run(
            (
                # yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14.
                # Override resolves the conflict (tested, yfinance works with 0.14+).
                # Written in the same command: E2B's builder does not carry /tmp
                # from a cached step into the next one.
                # uv writes no bytecode by default, so every new sandbox would
                # compile pandas, yfinance and the rest on its first import.
                "echo 'curl_cffi>=0.14' > /tmp/overrides.txt"
                " && uv pip install --system --compile-bytecode"
                " --override /tmp/overrides.txt "
                + " ".join(dependencies)
                + " && rm /tmp/overrides.txt"
                + " && uv cache clean && rm -rf /root/.cache/pip",
                # Scrapling browser setup (Camoufox for StealthyFetcher). Its
                # Chromium is the one npx already placed in the shared
                # PLAYWRIGHT_BROWSERS_PATH, because both pins are the same version.
                "scrapling install || true",
                # A version drift between the two Playwright pins is invisible at
                # build time and doubles the browser payload, so fail here instead.
                'n=$(ls -d "'
                + browsers_path
                + '"/chromium-*/ 2>/dev/null | wc -l); [ "$n" -eq 1 ]',
            )
        ),
        Run(
            (
                'python -c "'
                "import matplotlib as mpl; "
                "mpl_dir = mpl.get_configdir(); "
                "import os; os.makedirs(mpl_dir, exist_ok=True); "
                "open(os.path.join(mpl_dir, 'matplotlibrc'), 'w').write("
                "'font.sans-serif: Noto Sans CJK SC, DejaVu Sans\\n'); "
                "import matplotlib.font_manager; "
                "matplotlib.font_manager._load_fontmanager(try_read_cache=False)"
                '"',
            )
        ),
        Workdir(working_dir),
    ]
