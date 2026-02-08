"""Tests for ToolCallChunkBuffer from ptc_cli.streaming.tool_buffer."""


from ptc_cli.streaming.tool_buffer import ToolCallChunkBuffer


class TestToolCallChunkBuffer:
    """Tests for ToolCallChunkBuffer class."""

    def test_initial_state_is_empty(self):
        """Test initial state is empty."""
        buffer = ToolCallChunkBuffer()

        assert buffer._buffers == {}
        assert buffer._displayed_ids == set()

    def test_add_chunk_with_complete_tool_call_returns_result(self):
        """Test add_chunk with complete tool call returns result."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "name": "Read",
            "id": "tool_1",
            "args": {"file_path": "/path/to/file.txt"},
        }

        result = buffer.add_chunk(chunk)

        assert result is not None
        assert result["name"] == "Read"
        assert result["id"] == "tool_1"
        assert result["args"] == {"file_path": "/path/to/file.txt"}

    def test_add_chunk_with_chunked_args_accumulates(self):
        """Test add_chunk with chunked args accumulates."""
        buffer = ToolCallChunkBuffer()

        # First chunk - name and partial args
        chunk1 = {
            "name": "Read",
            "id": "tool_1",
            "args": '{"file',
        }
        result1 = buffer.add_chunk(chunk1)
        assert result1 is None  # Incomplete JSON

        # Second chunk - more args
        chunk2 = {
            "id": "tool_1",
            "args": '_path": "/path',
        }
        result2 = buffer.add_chunk(chunk2)
        assert result2 is None  # Still incomplete JSON

        # Third chunk - complete args
        chunk3 = {
            "id": "tool_1",
            "args": '/to/file.txt"}',
        }
        result3 = buffer.add_chunk(chunk3)

        assert result3 is not None
        assert result3["name"] == "Read"
        assert result3["id"] == "tool_1"
        assert result3["args"] == {"file_path": "/path/to/file.txt"}

    def test_add_chunk_by_index_when_id_not_available(self):
        """Test add_chunk by index when id not available."""
        buffer = ToolCallChunkBuffer()

        # Use index instead of id
        chunk1 = {
            "name": "Read",
            "index": 0,
            "args": '{"file',
        }
        result1 = buffer.add_chunk(chunk1)
        assert result1 is None

        chunk2 = {
            "index": 0,
            "args": '_path": "/test.txt"}',
        }
        result2 = buffer.add_chunk(chunk2)

        assert result2 is not None
        assert result2["name"] == "Read"
        assert result2["args"] == {"file_path": "/test.txt"}

    def test_was_displayed_returns_false_initially_true_after_mark(self):
        """Test was_displayed returns False initially, True after mark."""
        buffer = ToolCallChunkBuffer()

        assert buffer.was_displayed("tool_1") is False

        buffer.mark_displayed("tool_1")

        assert buffer.was_displayed("tool_1") is True

    def test_mark_displayed_adds_to_set(self):
        """Test mark_displayed adds to set."""
        buffer = ToolCallChunkBuffer()

        assert "tool_1" not in buffer._displayed_ids

        buffer.mark_displayed("tool_1")

        assert "tool_1" in buffer._displayed_ids

        # Mark another
        buffer.mark_displayed("tool_2")

        assert "tool_2" in buffer._displayed_ids
        assert "tool_1" in buffer._displayed_ids

    def test_invalid_json_args_returns_none(self):
        """Test invalid JSON args returns None."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "name": "Read",
            "id": "tool_1",
            "args": '{"invalid json',
        }

        result = buffer.add_chunk(chunk)

        assert result is None

    def test_empty_args_returns_none(self):
        """Test empty args returns None."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "name": "Read",
            "id": "tool_1",
            "args": "",
        }

        result = buffer.add_chunk(chunk)

        assert result is None

    def test_none_args_returns_none(self):
        """Test None args returns None."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "name": "Read",
            "id": "tool_1",
            "args": None,
        }

        result = buffer.add_chunk(chunk)

        assert result is None

    def test_chunk_without_name_returns_none(self):
        """Test chunk without name returns None."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "id": "tool_1",
            "args": '{"file_path": "/test.txt"}',
        }

        result = buffer.add_chunk(chunk)

        assert result is None

    def test_buffer_cleanup_after_completion(self):
        """Test buffer is cleaned up after completion."""
        buffer = ToolCallChunkBuffer()

        chunk = {
            "name": "Read",
            "id": "tool_1",
            "args": {"file_path": "/test.txt"},
        }

        result = buffer.add_chunk(chunk)

        assert result is not None
        # Buffer should be cleaned up
        assert "tool_1" not in buffer._buffers

    def test_multiple_tool_calls_simultaneously(self):
        """Test handling multiple tool calls simultaneously."""
        buffer = ToolCallChunkBuffer()

        # First tool call - chunk 1
        chunk1 = {
            "name": "Read",
            "id": "tool_1",
            "args": '{"file',
        }
        buffer.add_chunk(chunk1)

        # Second tool call - chunk 1
        chunk2 = {
            "name": "Write",
            "id": "tool_2",
            "args": '{"file',
        }
        buffer.add_chunk(chunk2)

        # Complete first tool call
        chunk3 = {
            "id": "tool_1",
            "args": '_path": "/read.txt"}',
        }
        result1 = buffer.add_chunk(chunk3)

        assert result1 is not None
        assert result1["name"] == "Read"
        assert result1["id"] == "tool_1"

        # Complete second tool call
        chunk4 = {
            "id": "tool_2",
            "args": '_path": "/write.txt", "content": "test"}',
        }
        result2 = buffer.add_chunk(chunk4)

        assert result2 is not None
        assert result2["name"] == "Write"
        assert result2["id"] == "tool_2"

    def test_duplicate_args_chunk_deduplication(self):
        """Test duplicate args chunks are deduplicated."""
        buffer = ToolCallChunkBuffer()

        # Sometimes streaming sends duplicate chunks
        chunk1 = {
            "name": "Read",
            "id": "tool_1",
            "args": '{"file',
        }
        buffer.add_chunk(chunk1)

        # Same args again (duplicate)
        chunk2 = {
            "id": "tool_1",
            "args": '{"file',
        }
        buffer.add_chunk(chunk2)

        # Continue with new args
        chunk3 = {
            "id": "tool_1",
            "args": '_path": "/test.txt"}',
        }
        result = buffer.add_chunk(chunk3)

        # Should handle deduplication and complete successfully
        assert result is not None
        assert result["args"] == {"file_path": "/test.txt"}

    def test_non_dict_args_wrapped_in_value_key(self):
        """Test non-dict args are wrapped in value key."""
        buffer = ToolCallChunkBuffer()

        # Note: The code path for non-dict parsed_args after JSON parsing
        # is harder to trigger naturally, but let's test the logic

        # Simulate a scenario where args parse to a list
        chunk = {
            "name": "some_tool",
            "id": "tool_1",
            "args": '["value1", "value2"]',
        }

        result = buffer.add_chunk(chunk)

        assert result is not None
        assert result["args"] == {"value": ["value1", "value2"]}

    def test_buffer_key_fallback_when_no_id_or_index(self):
        """Test buffer key fallback when no id or index."""
        buffer = ToolCallChunkBuffer()

        # No id or index - should use fallback
        chunk1 = {
            "name": "tool_1",
            "args": '{"test',
        }
        buffer.add_chunk(chunk1)

        # Should create a buffer with "unknown-0" key
        assert "unknown-0" in buffer._buffers

        # Another chunk without id/index
        chunk2 = {
            "name": "tool_2",
            "args": '{"test2',
        }
        buffer.add_chunk(chunk2)

        # Should create another buffer with "unknown-1" key
        assert "unknown-1" in buffer._buffers

    def test_id_assignment_updates_buffer(self):
        """Test that id gets assigned when provided in later chunks."""
        buffer = ToolCallChunkBuffer()

        # First chunk with name but no id
        chunk1 = {
            "name": "Read",
            "index": 0,
            "args": '{"file',
        }
        buffer.add_chunk(chunk1)

        # Second chunk provides id
        chunk2 = {
            "id": "tool_1",
            "index": 0,
            "args": '_path": "/test.txt"}',
        }
        result = buffer.add_chunk(chunk2)

        assert result is not None
        assert result["id"] == "tool_1"
        assert result["name"] == "Read"

    def test_dict_args_override_string_args(self):
        """Test that dict args override accumulated string args."""
        buffer = ToolCallChunkBuffer()

        # First chunk with string args
        chunk1 = {
            "name": "Read",
            "id": "tool_1",
            "args": '{"partial',
        }
        buffer.add_chunk(chunk1)

        # Second chunk with complete dict args (overrides)
        chunk2 = {
            "id": "tool_1",
            "args": {"file_path": "/test.txt"},
        }
        result = buffer.add_chunk(chunk2)

        assert result is not None
        assert result["args"] == {"file_path": "/test.txt"}
