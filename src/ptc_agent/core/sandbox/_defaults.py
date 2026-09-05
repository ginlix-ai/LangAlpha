"""Shared constants for sandbox providers and PTCSandbox.

NOTE: `Dockerfile.sandbox` (the Docker provider's image) hand-mirrors
`DEFAULT_DEPENDENCIES` and `SANDBOX_NODE_VERSION` below — it cannot import this
module at build time. Keep both in sync when editing either.
"""

SNAPSHOT_PYTHON_VERSION = "3.12"  # Intentionally pinned for stability/compatibility.
SANDBOX_NODE_VERSION = "24.14.1"  # Pinned; mirrored in Dockerfile.sandbox.

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
    "markitdown[pptx]",
    # Web scraping
    "scrapling[all]",
    "html-to-markdown",
    "trafilatura",
    "youtube-transcript-api",
    # Browser automation
    "playwright",
    # Utilities
    "tqdm",
    "tabulate",
]
