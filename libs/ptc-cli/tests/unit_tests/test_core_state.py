"""Unit tests for SessionState class."""

import uuid


class TestSessionStateInit:
    """Test SessionState initialization."""

    def test_init_defaults(self, session_state):
        """Test SessionState initializes with correct defaults."""
        assert session_state.auto_approve is False
        assert session_state.no_splash is False
        assert session_state.persist_session is True
        assert session_state.reusing_sandbox is False
        assert session_state.exit_hint_until is None
        assert session_state.exit_hint_handle is None
        assert isinstance(session_state.thread_id, str)
        # Verify thread_id is a valid UUID
        assert uuid.UUID(session_state.thread_id)

    def test_init_with_custom_values(self):
        """Test SessionState initializes with custom values."""
        from ptc_cli.core.state import SessionState

        state = SessionState(
            auto_approve=True,
            no_splash=True,
            persist_session=False,
        )
        assert state.auto_approve is True
        assert state.no_splash is True
        assert state.persist_session is False


class TestToggleAutoApprove:
    """Test toggle_auto_approve method."""

    def test_toggle_from_false(self, session_state):
        """Test toggling auto-approve from False to True."""
        assert session_state.auto_approve is False
        result = session_state.toggle_auto_approve()
        assert result is True
        assert session_state.auto_approve is True

    def test_toggle_from_true(self, session_state_with_auto_approve):
        """Test toggling auto-approve from True to False."""
        assert session_state_with_auto_approve.auto_approve is True
        result = session_state_with_auto_approve.toggle_auto_approve()
        assert result is False
        assert session_state_with_auto_approve.auto_approve is False

    def test_toggle_twice(self, session_state):
        """Test toggling auto-approve twice returns to original state."""
        original_state = session_state.auto_approve
        session_state.toggle_auto_approve()
        session_state.toggle_auto_approve()
        assert session_state.auto_approve == original_state


class TestResetThread:
    """Test reset_thread method."""

    def test_generates_new_uuid(self, session_state):
        """Test that reset_thread generates a new UUID."""
        original_thread_id = session_state.thread_id
        new_thread_id = session_state.reset_thread()

        # Verify new thread_id is different
        assert new_thread_id != original_thread_id

        # Verify new thread_id is a valid UUID
        assert uuid.UUID(new_thread_id)

        # Verify thread_id is updated
        assert session_state.thread_id == new_thread_id

    def test_multiple_resets_generate_unique_ids(self, session_state):
        """Test that multiple resets generate unique thread IDs."""
        thread_ids = set()

        # Collect 10 thread IDs
        for _ in range(10):
            thread_id = session_state.reset_thread()
            thread_ids.add(thread_id)

        # All 10 should be unique
        assert len(thread_ids) == 10

        # All should be valid UUIDs
        for thread_id in thread_ids:
            assert uuid.UUID(thread_id)

    def test_returns_current_thread_id(self, session_state):
        """Test that reset_thread returns the new thread_id."""
        new_thread_id = session_state.reset_thread()
        assert session_state.thread_id == new_thread_id
