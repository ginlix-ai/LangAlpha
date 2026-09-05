"""Tests for ptc_agent.core.sandbox dataclasses, enums, and helper functions.

Tests the public data structures and pure utility functions without
instantiating the full PTCSandbox (which requires Daytona credentials).
"""

import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from ptc_agent.core.sandbox import (
    ChartData,
    ExecutionResult,
    SandboxTransientError,
    SyncResult,
    _DaytonaRetryPolicy,
    _hash_dict,
    _resolve_local_path,
    _sha256_file,
)


class TestDataclasses:
    """Tests for sandbox dataclasses."""

    def test_chart_data_defaults(self):
        chart = ChartData(type="bar", title="Revenue")
        assert chart.type == "bar"
        assert chart.title == "Revenue"
        assert chart.png_base64 is None
        assert chart.elements == []

    def test_chart_data_with_values(self):
        chart = ChartData(
            type="line",
            title="Prices",
            png_base64="iVBORw0KGgo=",
            elements=[{"x": 1, "y": 2}],
        )
        assert chart.png_base64 == "iVBORw0KGgo="
        assert len(chart.elements) == 1

    def test_execution_result_defaults(self):
        result = ExecutionResult(
            success=True,
            stdout="hello",
            stderr="",
            duration=1.5,
            execution_id="exec-1",
            code_hash="abc123",
        )
        assert result.success is True
        assert result.stdout == "hello"
        assert result.charts == []

    def test_execution_result_with_charts(self):
        chart = ChartData(type="scatter", title="Test")
        result = ExecutionResult(
            success=False,
            stdout="",
            stderr="error",
            duration=0.1,
            execution_id="exec-2",
            code_hash="def456",
            charts=[chart],
        )
        assert result.success is False
        assert len(result.charts) == 1
        assert result.charts[0].title == "Test"

    def test_sync_result(self):
        result = SyncResult(refreshed_modules=["yfinance", "fmp"], forced=True)
        assert result.refreshed_modules == ["yfinance", "fmp"]
        assert result.forced is True


class TestEnums:
    """Tests for sandbox enums."""

    def test_daytona_retry_policy_values(self):
        assert _DaytonaRetryPolicy.SAFE.value == "safe"
        assert _DaytonaRetryPolicy.UNSAFE.value == "unsafe"

    def test_sandbox_transient_error_is_runtime_error(self):
        err = SandboxTransientError("transport failed")
        assert isinstance(err, RuntimeError)
        assert str(err) == "transport failed"


class TestHelperFunctions:
    """Tests for pure utility functions."""

    def test_sha256_file(self, tmp_path):
        p = tmp_path / "test.txt"
        content = b"hello world"
        p.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert _sha256_file(p) == expected

    def test_hash_dict_deterministic(self):
        d = {"b": "2", "a": "1"}
        h1 = _hash_dict(d)
        h2 = _hash_dict(d)
        assert h1 == h2
        # Order should not matter since keys are sorted
        d2 = {"a": "1", "b": "2"}
        assert _hash_dict(d2) == h1

    def test_hash_dict_different_values(self):
        d1 = {"a": "1"}
        d2 = {"a": "2"}
        assert _hash_dict(d1) != _hash_dict(d2)

    def test_resolve_local_path_absolute(self, tmp_path):
        """Absolute paths that exist are returned directly."""
        f = tmp_path / "exists.txt"
        f.write_text("data")
        result = _resolve_local_path(str(f), config_dir=None)
        assert result == str(f)

    def test_resolve_local_path_relative_in_config_dir(self, tmp_path):
        """Relative paths resolved via config_dir first."""
        f = tmp_path / "relative.txt"
        f.write_text("data")
        result = _resolve_local_path("relative.txt", config_dir=tmp_path)
        assert result is not None
        assert Path(result).name == "relative.txt"

    def test_resolve_local_path_not_found(self, tmp_path):
        """Returns None when file does not exist."""
        result = _resolve_local_path("nonexistent.txt", config_dir=tmp_path)
        assert result is None
