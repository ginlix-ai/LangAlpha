"""Shared constants for sandbox providers and PTCSandbox."""

SNAPSHOT_PYTHON_VERSION = "3.12"  # Intentionally pinned for stability/compatibility.

DEFAULT_DEPENDENCIES = [
    # Core
    "mcp",
    "fastmcp",
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
    # Utilities
    "tqdm",
    "tabulate",
]
