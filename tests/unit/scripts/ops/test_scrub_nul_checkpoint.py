"""Unit tests for the NUL-scrub ops script's delta-awareness.

`scrub_nul_checkpoint._strip_nul` walks deserialized checkpoint structures and
strips NUL bytes. A `_DeltaSnapshot` is a `NamedTuple`; the generic tuple branch
must reconstruct it with the SAME type, not flatten it to a plain tuple —
otherwise `DeltaChannel.from_checkpoint` stops recognizing the snapshot and the
`messages` channel corrupts on resume.
"""

from __future__ import annotations

from typing import NamedTuple

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import _DeltaSnapshot

from scripts.ops.scrub_nul_checkpoint import _strip_nul


def test_strip_nul_preserves_delta_snapshot_type():
    """A `_DeltaSnapshot` containing a NUL stays a `_DeltaSnapshot` and is cleaned."""
    snap = _DeltaSnapshot(value="msg\x00one")

    cleaned, count = _strip_nul(snap)

    # (a) Same NamedTuple type, not flattened to a plain tuple.
    assert isinstance(cleaned, _DeltaSnapshot)
    assert type(cleaned).__name__ == "_DeltaSnapshot"
    assert type(cleaned) is _DeltaSnapshot
    # (b) NUL removed from the field; the field is still accessible by name.
    assert cleaned.value == "msgone"
    assert "\x00" not in cleaned.value
    assert count == 1


def test_strip_nul_delta_snapshot_with_nested_structure():
    """NUL inside a list/dict carried by the snapshot is scrubbed, type preserved."""
    snap = _DeltaSnapshot(value=["a\x00b", {"k": "v\x00al"}])

    cleaned, count = _strip_nul(snap)

    assert isinstance(cleaned, _DeltaSnapshot)
    assert cleaned.value == ["ab", {"k": "val"}]
    assert count == 2


def test_strip_nul_delta_snapshot_survives_serde_roundtrip():
    """The cleaned snapshot must re-serialize via the same serde the script uses
    and rehydrate as a `_DeltaSnapshot` — the property the script relies on."""
    serde = JsonPlusSerializer()
    snap = _DeltaSnapshot(value="poison\x00ed")

    cleaned, _ = _strip_nul(snap)
    type_str, blob = serde.dumps_typed(cleaned)
    rehydrated = serde.loads_typed((type_str, blob))

    assert isinstance(rehydrated, _DeltaSnapshot)
    assert rehydrated.value == "poisoned"


def test_strip_nul_preserves_generic_namedtuple():
    """Any NamedTuple (not just `_DeltaSnapshot`) keeps its type after scrubbing."""

    class _Pair(NamedTuple):
        left: str
        right: str

    cleaned, count = _strip_nul(_Pair(left="x\x00y", right="z"))

    assert isinstance(cleaned, _Pair)
    assert cleaned._fields == ("left", "right")
    assert cleaned.left == "xy"
    assert cleaned.right == "z"
    assert count == 1


def test_strip_nul_snapshot_with_message_object_scrubs_content():
    """The ACTUAL production shape: a `_DeltaSnapshot` whose value is a list of
    LangChain message objects, with a NUL in `ToolMessage.content`.

    The module docstring names message content as the dominant NUL carrier, but
    the existing snapshot tests only carry str / list-of-str/dict. This drives the
    message-object branch (`hasattr(value, "__dict__")`) nested inside the
    NamedTuple reconstruction: the NUL is stripped from `content`, the object stays
    a `ToolMessage`, and the wrapper stays a `_DeltaSnapshot`."""
    from langchain_core.messages import ToolMessage

    snap = _DeltaSnapshot(
        value=[ToolMessage(content="bad\x00data", tool_call_id="c1", id="t1")]
    )

    cleaned, count = _strip_nul(snap)

    assert isinstance(cleaned, _DeltaSnapshot)
    assert type(cleaned.value[0]) is ToolMessage
    assert cleaned.value[0].content == "baddata"
    assert cleaned.value[0].id == "t1"
    assert count == 1


def test_strip_nul_plain_tuple_stays_plain_tuple():
    """A plain tuple (no `_fields`) is still scrubbed and returned as a plain tuple."""
    cleaned, count = _strip_nul(("a\x00", "b"))

    assert type(cleaned) is tuple
    assert cleaned == ("a", "b")
    assert count == 1


def test_strip_nul_no_nul_is_noop():
    """No NUL anywhere → value unchanged, count 0, type preserved."""
    snap = _DeltaSnapshot(value=["clean", {"k": "v"}])

    cleaned, count = _strip_nul(snap)

    assert isinstance(cleaned, _DeltaSnapshot)
    assert cleaned.value == ["clean", {"k": "v"}]
    assert count == 0
