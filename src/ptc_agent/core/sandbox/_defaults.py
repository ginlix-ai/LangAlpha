"""Shared constants for sandbox providers and PTCSandbox.

NOTE: `Dockerfile.sandbox` (the Docker provider's image) hand-mirrors
`DEFAULT_DEPENDENCIES`, `SANDBOX_NODE_VERSION`, `SANDBOX_PLAYWRIGHT_VERSION`,
`SANDBOX_IMAGE_ENV` and `sandbox_thread_env` below; it cannot import this module
at build time. Keep both in sync when editing either.
"""

SNAPSHOT_PYTHON_VERSION = "3.12"  # Intentionally pinned for stability/compatibility.
SANDBOX_NODE_VERSION = "24.14.1"  # Pinned; mirrored in Dockerfile.sandbox.

# One version for both language ports of Playwright. The npm package and the
# Python package resolve the browser revision from their own version number, so
# leaving either unpinned lets them drift apart and bake two Chromium revisions
# (plus two headless shells) into the image. scrapling[all] requires >= 1.62.0.
SANDBOX_PLAYWRIGHT_VERSION = "1.63.0"

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
    "yfinance",
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
    # Web scraping
    "scrapling[all]",
    "html-to-markdown",
    "trafilatura",
    "youtube-transcript-api",
    # Browser automation
    f"playwright=={SANDBOX_PLAYWRIGHT_VERSION}",
    # Utilities
    "tqdm",
    "tabulate",
]
