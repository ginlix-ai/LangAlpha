"""Tests for sandbox health monitoring utilities."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from ptc_cli.sandbox.health import EmptyResultTracker, check_sandbox_health


class TestCheckSandboxHealth:
    """Tests for check_sandbox_health function."""

    @pytest.mark.asyncio
    async def test_check_sandbox_health_returns_true_for_running_sandbox(self):
        """Test check_sandbox_health returns True for running sandbox."""
        # Create mock session with sandbox
        mock_session = Mock()
        mock_daytona_sandbox = Mock()
        mock_daytona_sandbox.state = "started"
        mock_daytona_sandbox.refresh_data = Mock()

        mock_sandbox = Mock()
        mock_sandbox.sandbox = mock_daytona_sandbox

        mock_session.sandbox = mock_sandbox

        # Mock asyncio.to_thread to actually call the function
        async def mock_to_thread(func, *args, **kwargs):
            func(*args, **kwargs)

        with patch("asyncio.to_thread", new=mock_to_thread):
            result = await check_sandbox_health(mock_session)

        assert result is True
        mock_daytona_sandbox.refresh_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_sandbox_health_returns_false_for_stopped_sandbox(self):
        """Test check_sandbox_health returns False for stopped sandbox."""
        mock_session = Mock()
        mock_daytona_sandbox = Mock()
        mock_daytona_sandbox.state = "stopped"
        mock_daytona_sandbox.refresh_data = Mock()

        mock_sandbox = Mock()
        mock_sandbox.sandbox = mock_daytona_sandbox

        mock_session.sandbox = mock_sandbox

        # Mock asyncio.to_thread to actually call the function
        async def mock_to_thread(func, *args, **kwargs):
            func(*args, **kwargs)

        with patch("asyncio.to_thread", new=mock_to_thread):
            result = await check_sandbox_health(mock_session)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_sandbox_health_returns_false_for_no_sandbox(self):
        """Test check_sandbox_health returns False when no sandbox exists."""
        mock_session = Mock()
        mock_session.sandbox = None

        result = await check_sandbox_health(mock_session)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_sandbox_health_returns_false_for_none_session(self):
        """Test check_sandbox_health returns False for None session."""
        result = await check_sandbox_health(None)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_sandbox_health_returns_false_on_refresh_error(self):
        """Test check_sandbox_health returns False on refresh error."""
        mock_session = Mock()
        mock_daytona_sandbox = Mock()
        mock_daytona_sandbox.refresh_data = Mock(side_effect=Exception("Connection error"))

        mock_sandbox = Mock()
        mock_sandbox.sandbox = mock_daytona_sandbox

        mock_session.sandbox = mock_sandbox

        with patch("asyncio.to_thread", new=AsyncMock(side_effect=Exception("Connection error"))):
            result = await check_sandbox_health(mock_session)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_sandbox_health_with_state_enum(self):
        """Test check_sandbox_health with state as enum object."""
        mock_session = Mock()
        mock_daytona_sandbox = Mock()

        # Mock state as enum-like object with .value attribute
        mock_state = Mock()
        mock_state.value = "started"
        mock_daytona_sandbox.state = mock_state
        mock_daytona_sandbox.refresh_data = Mock()

        mock_sandbox = Mock()
        mock_sandbox.sandbox = mock_daytona_sandbox

        mock_session.sandbox = mock_sandbox

        # Mock asyncio.to_thread to actually call the function
        async def mock_to_thread(func, *args, **kwargs):
            func(*args, **kwargs)

        with patch("asyncio.to_thread", new=mock_to_thread):
            result = await check_sandbox_health(mock_session)

        assert result is True


class TestEmptyResultTracker:
    """Tests for EmptyResultTracker class."""

    def test_initial_state(self):
        """Test EmptyResultTracker initial state."""
        tracker = EmptyResultTracker()
        assert tracker._count == 0

    def test_non_sensitive_tools_dont_increment(self):
        """Test non-sensitive tools don't increment count."""
        tracker = EmptyResultTracker()

        result = tracker.record("Read", "")
        assert result is False
        assert tracker._count == 0

        result = tracker.record("Write", "")
        assert result is False
        assert tracker._count == 0

    def test_sensitive_tools_with_empty_results_increment(self):
        """Test sensitive tools with empty results increment count."""
        tracker = EmptyResultTracker()

        # First empty result - below threshold
        result = tracker.record("Glob", "")
        assert result is False
        assert tracker._count == 1

        # Second empty result - at threshold
        result = tracker.record("Grep", "")
        assert result is True
        assert tracker._count == 2

    def test_threshold_exceeded_returns_true(self):
        """Test threshold exceeded returns True."""
        tracker = EmptyResultTracker()

        tracker.record("Glob", "")  # count = 1
        result = tracker.record("Grep", "[]")  # count = 2, threshold met
        assert result is True

    def test_non_empty_results_reset_count(self):
        """Test non-empty results reset count."""
        tracker = EmptyResultTracker()

        tracker.record("Glob", "")  # count = 1
        assert tracker._count == 1

        # Non-empty result should reset
        tracker.record("Grep", "file1.txt\nfile2.txt")
        assert tracker._count == 0

    def test_reset_method(self):
        """Test reset method."""
        tracker = EmptyResultTracker()

        tracker.record("Glob", "")
        tracker.record("Grep", "")
        assert tracker._count == 2

        tracker.reset()
        assert tracker._count == 0

    def test_empty_json_results(self):
        """Test empty JSON results are considered empty."""
        tracker = EmptyResultTracker()

        result = tracker.record("Glob", "[]")
        assert result is False
        assert tracker._count == 1

        result = tracker.record("Grep", "{}")
        assert result is True
        assert tracker._count == 2

    def test_whitespace_only_results(self):
        """Test whitespace-only results are considered empty."""
        tracker = EmptyResultTracker()

        result = tracker.record("Glob", "   \n\t  ")
        assert result is False
        assert tracker._count == 1

    def test_none_content(self):
        """Test None content is considered empty."""
        tracker = EmptyResultTracker()

        result = tracker.record("Glob", None)
        assert result is False
        assert tracker._count == 1

    def test_sensitive_tools_case_variations(self):
        """Test sensitive tools work with PascalCase."""
        tracker = EmptyResultTracker()

        tracker.record("Glob", "")  # PascalCase
        assert tracker._count == 1

        tracker.record("Grep", "")  # PascalCase
        assert tracker._count == 2

        tracker.reset()

        tracker.record("GLOB", "")  # Should not match (uppercase)
        assert tracker._count == 0  # Not in SENSITIVE_TOOLS set

    def test_ls_tool(self):
        """Test ls tool is tracked as sensitive."""
        tracker = EmptyResultTracker()

        result = tracker.record("ls", "")
        assert result is False
        assert tracker._count == 1

        result = tracker.record("ls", "")
        assert result is True
        assert tracker._count == 2
