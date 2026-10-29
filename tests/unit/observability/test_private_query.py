"""A subagent's name leaves the access line and the server span; a private query goes too."""

from __future__ import annotations

import logging

import pytest

from src.observability.private_query import AccessLogQueryFilter, drop_span_query


def _logged(target: str) -> str:
    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:1", "PUT", target, "1.1", 200), None,
    )
    AccessLogQueryFilter().filter(record)
    return record.args[2]


@pytest.mark.parametrize(
    ("target", "logged"),
    [
        pytest.param("/api/v1/subagents/fb-rest", "/api/v1/subagents/{name}", id="name-folded"),
        pytest.param(
            "/api/v1/subagents/fb-rest?version=abc", "/api/v1/subagents/{name}?version=abc", id="query-kept-name-folded"
        ),
        pytest.param("/api/v1/subagents/team/research", "/api/v1/subagents/{name}", id="slashed-name-folded"),
        pytest.param("/api/v1/subagents", "/api/v1/subagents", id="list-unchanged"),
        pytest.param("/api/v1/livefs/write?path=a.md", "/api/v1/livefs/write", id="mount-query-dropped"),
        pytest.param(
            "/api/v1/workspaces/w1/files/read?path=.agents%2Fuser%2Fagents%2Fx.md",
            "/api/v1/workspaces/w1/files/read",
            id="subagent-file-query-dropped",
        ),
        pytest.param("/api/v1/threads?limit=5", "/api/v1/threads?limit=5", id="other-unchanged"),
    ],
)
def test_the_access_line_folds_a_subagent_name_out_of_the_path(target, logged):
    assert _logged(target) == logged


class _Span:
    def __init__(self, attributes: dict[str, str]) -> None:
        self.attributes = attributes

    def is_recording(self) -> bool:
        return True

    def set_attribute(self, key: str, value: str) -> None:
        self.attributes[key] = value


def test_a_subagent_requests_span_keeps_its_query_and_loses_the_name():
    span = _Span({
        "http.url": "http://test/api/v1/subagents/fb-rest?version=abc",
        "url.full": "http://test/api/v1/subagents/fb-rest?version=abc",
        "http.target": "/api/v1/subagents/fb-rest?version=abc",
        "url.path": "/api/v1/subagents/fb-rest",
        "url.query": "version=abc",
    })

    drop_span_query(span, {"path": "/api/v1/subagents/fb-rest", "query_string": b"version=abc"})

    assert span.attributes == {
        "http.url": "http://test/api/v1/subagents/{name}?version=abc",
        "url.full": "http://test/api/v1/subagents/{name}?version=abc",
        "http.target": "/api/v1/subagents/{name}?version=abc",
        "url.path": "/api/v1/subagents/{name}",
        "url.query": "version=abc",
    }


def test_a_span_of_the_subagent_list_is_left_as_it_was():
    sent = {"http.url": "http://test/api/v1/subagents", "url.path": "/api/v1/subagents"}
    span = _Span(dict(sent))

    drop_span_query(span, {"path": "/api/v1/subagents", "query_string": b""})

    assert span.attributes == sent
