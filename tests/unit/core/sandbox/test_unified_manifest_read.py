"""A manifest the sync's diff cannot read is read as no manifest at all.

The file sits in the sandbox, where any process can rewrite it. A sync that
raised on a malformed one would never write it again, and the restore would
fail on every start; read as missing, the next sync refreshes everything.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox.assets import _read_unified_manifest


def _sandbox(manifest):
    sandbox = MagicMock()
    sandbox._runtime_call = AsyncMock(return_value=json.dumps(manifest).encode())
    return sandbox


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest",
    [
        {"schema_version": 1, "modules": ["skills"]},
        {"schema_version": 1, "modules": {"skills": ["invalid"]}},
        {"schema_version": 1, "modules": {"skills": {"version": "a"}, "tokens": None}},
        {"schema_version": 2, "modules": {}},
        ["not", "a", "manifest"],
    ],
    ids=["modules-list", "module-list", "module-null", "other-schema", "not-an-object"],
)
async def test_a_manifest_the_diff_cannot_read_is_missing(manifest):
    assert await _read_unified_manifest(_sandbox(manifest)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest",
    [
        {"schema_version": 1, "modules": {"skills": {"version": "a", "files": {}}}},
        {"schema_version": 1},
    ],
    ids=["modules", "no-modules"],
)
async def test_a_well_formed_manifest_is_returned(manifest):
    assert await _read_unified_manifest(_sandbox(manifest)) == manifest
