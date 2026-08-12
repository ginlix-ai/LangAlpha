"""
Tests for find_malformed_route_ids (src/server/utils/api.py).

TEMP diagnostic helper (malformed-id-diag): flags non-UUID workspace/thread ids so a
middleware can log them with their Referer. Remove with the helper once the
frontend writer that feeds file/dir names into id slots is fixed.
"""

import uuid

from src.server.utils.api import find_malformed_route_ids


def test_valid_uuid_path_is_not_flagged():
    wid = str(uuid.uuid4())
    assert find_malformed_route_ids(f"/api/v1/workspaces/{wid}") == []
    assert find_malformed_route_ids(f"/api/v1/workspaces/{wid}/files") == []
    assert find_malformed_route_ids(f"/api/v1/threads/{wid}/status") == []


def test_uppercase_uuid_is_not_flagged():
    # Pins the load-bearing re.IGNORECASE on _ROUTE_UUID_RE: without it every
    # uppercase UUID would be logged as malformed and spam the diagnostic.
    wid = str(uuid.uuid4()).upper()
    assert find_malformed_route_ids(f"/api/v1/workspaces/{wid}") == []
    assert find_malformed_route_ids("/api/v1/threads", f"workspace_id={wid}".encode()) == []


def test_non_uuid_workspace_path_is_flagged():
    findings = find_malformed_route_ids(
        "/api/v1/workspaces/my_notes.md/files"
    )
    assert findings == [("workspace_path_id", "my_notes.md")]


def test_non_uuid_thread_path_is_flagged():
    # The directory-name variant: GET /threads/results.
    assert find_malformed_route_ids("/api/v1/threads/results") == [
        ("thread_path_id", "results")
    ]


def test_workspace_id_query_param_is_flagged():
    findings = find_malformed_route_ids(
        "/api/v1/threads", b"workspace_id=my_notes.md&limit=20"
    )
    assert findings == [("workspace_id_param", "my_notes.md")]


def test_valid_workspace_id_query_param_is_not_flagged():
    wid = str(uuid.uuid4())
    assert find_malformed_route_ids("/api/v1/threads", f"workspace_id={wid}".encode()) == []


def test_literal_endpoint_segments_allowlisted():
    # POST /threads/messages and /workspaces/{flash,reorder} are endpoints, not ids.
    assert find_malformed_route_ids("/api/v1/threads/messages") == []
    assert find_malformed_route_ids("/api/v1/threads/dispatches/liveness") == []
    assert find_malformed_route_ids("/api/v1/workspaces/flash") == []
    assert find_malformed_route_ids("/api/v1/workspaces/reorder") == []


def test_list_endpoints_without_id_are_not_flagged():
    assert find_malformed_route_ids("/api/v1/workspaces") == []
    assert find_malformed_route_ids("/api/v1/threads") == []


def test_already_decoded_segment_is_flagged_verbatim():
    # ASGI delivers scope["path"] already percent-decoded, so the function
    # receives literal characters (a space, not %20) and flags the value
    # verbatim — no second decode.
    findings = find_malformed_route_ids("/api/v1/workspaces/my file.md")
    assert findings == [("workspace_path_id", "my file.md")]
