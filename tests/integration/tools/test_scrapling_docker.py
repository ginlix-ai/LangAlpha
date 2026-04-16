"""Integration tests for Scrapling inside Docker sandbox.

Builds the sandbox Docker image and verifies Scrapling, yfinance, and
related tools work correctly inside the container environment.

Run with:
    uv run pytest tests/integration/tools/test_scrapling_docker.py -m integration -v --timeout=300

Requires: Docker daemon running.
"""

from __future__ import annotations

import os
import subprocess
import pytest

pytestmark = [pytest.mark.integration]

_GHCR_IMAGE = "ghcr.io/ginlix-ai/langalpha/sandbox:latest"
_IMAGE = "langalpha-sandbox:test-scrapling"
_DOCKERFILE = "Dockerfile.sandbox"
_TIMEOUT = 120  # seconds per container command


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _run_in_container(cmd: str, timeout: int = _TIMEOUT) -> subprocess.CompletedProcess:
    """Run a command inside the sandbox container and return result."""
    return subprocess.run(
        ["docker", "run", "--rm", _IMAGE, "bash", "-c", cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture(scope="module")
def docker_image():
    """Resolve the sandbox Docker image once for all tests in this module.

    Prefers the pre-built GHCR image (fast pull) and falls back to a local
    ``docker build`` when the registry image is unavailable.  Set
    ``DOCKER_SANDBOX_IMAGE`` to force a specific image tag.
    """
    if not _docker_available():
        pytest.skip("Docker not available")

    # Allow CI or the user to pin a specific image.
    explicit = os.environ.get("DOCKER_SANDBOX_IMAGE")
    if explicit:
        # Tag locally so _run_in_container always uses _IMAGE.
        subprocess.run(
            ["docker", "tag", explicit, _IMAGE],
            capture_output=True, check=False,
        )
        yield _IMAGE
        return

    # Try pulling the pre-built GHCR image first (~30s vs ~20min build).
    pull = subprocess.run(
        ["docker", "pull", _GHCR_IMAGE],
        capture_output=True, text=True, timeout=120,
    )
    if pull.returncode == 0:
        subprocess.run(["docker", "tag", _GHCR_IMAGE, _IMAGE], check=True)
        yield _IMAGE
        return

    # Fallback: build locally.
    result = subprocess.run(
        ["docker", "build", "-f", _DOCKERFILE, "-t", _IMAGE, "."],
        capture_output=True,
        text=True,
        timeout=1800,
        cwd=subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True,
        ).stdout.strip(),
    )
    if result.returncode != 0:
        pytest.fail(f"Docker build failed:\n{result.stderr[-2000:]}")
    yield _IMAGE


# ---------------------------------------------------------------------------
# Python package availability
# ---------------------------------------------------------------------------


class TestSandboxPackages:
    """Verify required Python packages are installed in the sandbox."""

    def test_scrapling_import(self, docker_image):
        r = _run_in_container("python -c 'import scrapling; print(scrapling.__version__)'")
        assert r.returncode == 0, f"scrapling import failed: {r.stderr}"
        assert r.stdout.strip(), "scrapling version should be non-empty"

    def test_scrapling_fetchers_import(self, docker_image):
        r = _run_in_container(
            "python -c 'from scrapling.fetchers import Fetcher, AsyncFetcher, "
            "DynamicFetcher, StealthyFetcher; print(\"OK\")'"
        )
        assert r.returncode == 0, f"scrapling fetchers import failed: {r.stderr}"
        assert "OK" in r.stdout

    def test_html2text_import(self, docker_image):
        r = _run_in_container("python -c 'import html2text; print(\"OK\")'")
        assert r.returncode == 0, f"html2text import failed: {r.stderr}"
        assert "OK" in r.stdout

    def test_curl_cffi_version(self, docker_image):
        """curl_cffi >= 0.14 is required for scrapling[fetchers]."""
        r = _run_in_container(
            "python -c 'import curl_cffi; print(curl_cffi.__version__)'"
        )
        assert r.returncode == 0, f"curl_cffi import failed: {r.stderr}"
        version = r.stdout.strip()
        major, minor = version.split(".")[:2]
        assert (int(major), int(minor)) >= (0, 14), (
            f"curl_cffi version {version} < 0.14"
        )

    def test_yfinance_import(self, docker_image):
        r = _run_in_container("python -c 'import yfinance; print(yfinance.__version__)'")
        assert r.returncode == 0, f"yfinance import failed: {r.stderr}"

    def test_yfinance_with_curl_cffi(self, docker_image):
        """Verify yfinance works with the overridden curl_cffi version."""
        r = _run_in_container(
            "python -c '"
            "import yfinance as yf; "
            "t = yf.Ticker(\"AAPL\"); "
            "info = t.fast_info; "
            "print(f\"price={info.last_price}\")'"
        )
        assert r.returncode == 0, f"yfinance with curl_cffi failed: {r.stderr}"
        assert "price=" in r.stdout


# ---------------------------------------------------------------------------
# Scrapling CLI
# ---------------------------------------------------------------------------


class TestSandboxScraplingCLI:
    """Verify Scrapling CLI works inside the sandbox."""

    def test_scrapling_cli_available(self, docker_image):
        r = _run_in_container("scrapling --help")
        assert r.returncode == 0, f"scrapling CLI not available: {r.stderr}"

    def test_scrapling_extract_get(self, docker_image):
        """Test CLI extraction of a static page."""
        r = _run_in_container(
            "scrapling extract get 'https://example.com' /tmp/out.md && cat /tmp/out.md"
        )
        assert r.returncode == 0, f"scrapling extract failed: {r.stderr}"
        assert "example" in r.stdout.lower(), "Should contain example.com content"


# ---------------------------------------------------------------------------
# Scrapling MCP server
# ---------------------------------------------------------------------------


class TestSandboxScraplingMCP:
    """Verify Scrapling MCP server starts inside the sandbox."""

    def test_mcp_server_starts(self, docker_image):
        """Verify 'scrapling mcp' starts without error (kill after 3s)."""
        r = _run_in_container(
            "timeout 3 scrapling mcp 2>&1 || true"
        )
        # MCP server should start and wait for stdio input; timeout kills it
        # We just verify it doesn't crash immediately with an import error
        assert "Traceback" not in r.stdout, f"MCP server crashed: {r.stdout}"
        assert "ModuleNotFoundError" not in r.stdout, f"Missing module: {r.stdout}"


# ---------------------------------------------------------------------------
# Scrapling Tier 1 fetch inside container
# ---------------------------------------------------------------------------


class TestSandboxScraplingFetch:
    """Verify Scrapling fetching works inside the sandbox."""

    def test_tier1_http_fetch(self, docker_image):
        """Test Tier 1 (HTTP-only) fetch inside container."""
        script = "\n".join([
            "import asyncio",
            "from scrapling.fetchers import AsyncFetcher",
            "async def main():",
            "    page = await AsyncFetcher.get('https://example.com', stealthy_headers=True)",
            "    print('status=' + str(page.status))",
            "    print('len=' + str(len(page.body)))",
            "asyncio.run(main())",
        ])
        r = _run_in_container(f"python3 << 'PYEOF'\n{script}\nPYEOF")
        assert r.returncode == 0, f"Tier 1 fetch failed: {r.stderr}"
        assert "status=200" in r.stdout
        assert "len=" in r.stdout
        body_len = int(r.stdout.split("len=")[1].strip())
        assert body_len > 100, f"Body too short: {body_len} bytes"

    def test_html2text_conversion(self, docker_image):
        """Test HTML to markdown conversion inside container."""
        r = _run_in_container(
            "python -c \""
            "import html2text; "
            "h = html2text.HTML2Text(); "
            "h.body_width = 0; "
            "md = h.handle('<h1>Hello</h1><p>World</p>'); "
            "print(md)\""
        )
        assert r.returncode == 0, f"html2text failed: {r.stderr}"
        assert "hello" in r.stdout.lower()
        assert "world" in r.stdout.lower()
