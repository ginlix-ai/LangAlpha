"""Shared constants for sandbox providers and PTCSandbox.

NOTE: `Dockerfile.sandbox` (the Docker provider's image) hand-mirrors
`DEFAULT_DEPENDENCIES`, `SANDBOX_NODE_VERSION`, `SANDBOX_PLAYWRIGHT_VERSION`,
`SANDBOX_IMAGE_ENV` and `sandbox_thread_env` below; it cannot import this module
at build time. Keep both in sync when editing either.
"""

from ptc_agent.core.sandbox.browser_runtime import protocol as _browser

SANDBOX_NODE_VERSION = "24.22.0"  # Pinned; mirrored in Dockerfile.sandbox.

# One version for both language ports of Playwright. The npm package and the
# Python package resolve the browser revision from their own version number, so
# leaving either unpinned lets them drift apart and bake two Chromium revisions
# (plus two headless shells) into the image. scrapling[all] requires >= 1.62.0.
SANDBOX_PLAYWRIGHT_VERSION = "1.64.0"

# The agent's browser CLI, as the release's one binary per platform rather than
# the npm package, which carries every platform's (18 MB against 120 MB). Its
# browser is the Chrome for Testing the Playwright pin above already bakes in.
# The digests are the release's own, keyed as ``dpkg --print-architecture``
# names the machine; mirrored in Dockerfile.sandbox.
SANDBOX_AGENT_BROWSER_VERSION = "0.41.1"
SANDBOX_AGENT_BROWSER_SHA256 = {
    "amd64": "2a89295b62ddd497986a9a80b52e1dbe4c1f6bd68ffde67eaae82eec2206af61",
    "arm64": "f0b3bdaa40abf2a5111ec08e6d242110aaf56045ef5108cbc633bacc8e53ca17",
}
SANDBOX_AGENT_BROWSER_BIN = _browser.ENGINE_BIN
SANDBOX_CHROME_BIN = _browser.CHROME_BIN
# Playwright's "chromium" is Chrome for Testing, which reads policy here and
# silently ignores /etc/chromium.
SANDBOX_CHROME_POLICY_PATH = (
    "/etc/opt/chrome_for_testing/policies/managed/langalpha.json"
)
# What the browser may reach is enforced by the proxy every request of it goes
# through (``browser_runtime/proxy.py``), by the address a name resolves to.
# These ask the same of Chrome itself where it can: the list stops a navigation
# to the sandbox's metadata services and the agents beside it on loopback by
# name, local network access stays gated (on by default in the pinned Chrome,
# whose only switch for it is this opt-out), and WebRTC sends nothing the proxy
# does not carry. The rest of loopback stays open, where the agent's own apps
# run. Nothing asks to be remembered.
SANDBOX_CHROME_POLICY = {
    "URLBlocklist": [
        # An IPv6 address is matched as a URL writes it, in brackets.
        *(f"[{host}]" if ":" in host else host for host in _browser.METADATA_HOSTS),
        *_browser.DOCKER_HOSTS,
        # localhost and the address are one place reached by two names, so
        # each port is listed for both.
        *(
            f"{host}:{port}"
            for host in ("localhost", "127.0.0.1", "[::1]")
            for port in _browser.SERVICE_PORTS
        ),
    ],
    "LocalNetworkAccessRestrictionsTemporaryOptOut": False,
    "WebRtcIPHandling": "disable_non_proxied_udp",
    "PasswordManagerEnabled": False,
    "AutofillAddressEnabled": False,
    "AutofillCreditCardEnabled": False,
    "BrowserSignin": 0,
    "SyncDisabled": True,
}

# Thread cap for a snapshot built without resolved tier resources (an unknown
# tier falls back to a platform-default-sized sandbox). One thread never
# oversubscribes, whatever that default turns out to be.
SANDBOX_FALLBACK_CPU = 1

# Environment every sandbox process needs, delivered twice on purpose: baked
# into the snapshot image, and injected again as per-sandbox env vars at create
# time. The image layer alone is not enough, since a snapshot is reused whenever its
# config hash is unchanged, so a sandbox can be born on an image built before
# one of these was added. The create-time injection reaches those too.
# Commands run as non-login, non-interactive shells that inherit PID 1's
# environment, so /etc/profile.d would not be read; this is the only path.
SANDBOX_IMAGE_ENV = {
    # One Playwright browser dir shared by the npm-side `playwright` (npx) and
    # the Python `playwright` that Scrapling drives, instead of the per-user
    # ~/.cache default the two disagree on.
    "PLAYWRIGHT_BROWSERS_PATH": "/usr/local/ms-playwright",
    # `npm install -g` puts docx and pptxgenjs in the global tree, which Node
    # never searches: resolution only walks node_modules up from the script, so
    # `require("pptxgenjs")` from /home/workspace misses without this.
    "NODE_PATH": "/usr/local/lib/node_modules",
}


def sandbox_thread_env(cpu: int) -> dict[str, str]:
    """BLAS/OpenMP thread caps for a sandbox whose cgroup allows *cpu* cores.

    NumPy and its BLAS size their thread pools from the host's visible CPU count,
    which the cgroup does not mask: on the hosted container class the sandbox sees
    48 CPUs inside a 2-CPU quota, and the resulting 24x oversubscription turned a
    2000x2000 matmul from 0.14 s into 1.07 s. These belong to the image rather
    than to SANDBOX_IMAGE_ENV because the value is per tier, and a snapshot is
    already per tier, so the cpu count is fixed for every sandbox born from it.
    """
    threads = str(max(1, cpu))
    return {
        "OMP_NUM_THREADS": threads,
        "OPENBLAS_NUM_THREADS": threads,
        "MKL_NUM_THREADS": threads,
        "NUMEXPR_NUM_THREADS": threads,
    }


DEFAULT_DEPENDENCIES = [
    # Core
    # Exact pin, never a range: mcp_setup joins this list into a shell
    # command, where "<" would parse as a redirect.
    "mcp==2.0.0",
    "fastapi",
    "pandas",
    "requests",
    "aiohttp",
    "httpx[http2]",
    # Data science
    "numpy",
    "scipy",
    "scikit-learn",
    "statsmodels",
    # Financial data
    # Pinned to the version uv.lock gives the server, so a rebuilt image
    # cannot move the sandbox's Yahoo client ahead of it.
    "yfinance==1.8.0",
    # market_protocol's session calendars, which tell the market-data servers
    # whether a bar has settled.
    "exchange-calendars==4.13.2",
    # Visualization
    "matplotlib",
    "seaborn",
    "plotly",
    # Image analysis
    "pillow",
    "opencv-python-headless",
    "scikit-image",
    # File formats
    "openpyxl",
    "xlrd",
    "python-docx",
    "pypdf",
    "beautifulsoup4",
    "lxml",
    "pyyaml",
    # Office skill dependencies
    "defusedxml",
    "pdfplumber",
    "reportlab",
    "python-pptx",
    "ironcalc",
    "firecrawl-anydoc",
    "markitdown[docx,pptx,xlsx]",
    # Web scraping. 0.5 removed the Playwright browser tier that
    # mcp_servers/_browser.py and the web-scraping skill drive.
    "scrapling[all]==0.4.15",
    # 3.5 added the exclude_selectors that mcp_servers/_extract.py passes.
    "html-to-markdown>=3.5,<4",
    "trafilatura",
    "youtube-transcript-api",
    # Browser automation
    f"playwright=={SANDBOX_PLAYWRIGHT_VERSION}",
    # Utilities
    "tqdm",
    "tabulate",
]
