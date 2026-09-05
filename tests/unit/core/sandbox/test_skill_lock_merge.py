"""The sandbox-side authoritative merge must match lock.py's merge_lock_files.

The asset-sync path used to compute merge_lock_files on the host from a stale
lock snapshot and blind-upload the result; the ``merge_authoritative`` script
mode re-reads under the reconciler's flock instead. Its merge logic is a
deliberate inline copy of the lock.py predicates, so this test binds the two:
for any lock state, script merge == host merge — plus the one divergence the
script adds on purpose (a racing agent/linked claim on an authoritative name
keeps the claim and reports it, rather than being overwritten).
"""

from __future__ import annotations

import ast
import fcntl
import json
import os
import time

from ptc_agent.agent.middleware.skills.lock import (
    LOCK_FILE_VERSION,
    merge_lock_files,
)
from ptc_agent.core.sandbox.skill_sync import _SCRIPT


def _run_merge(tmp_path, entries: dict, auth: dict) -> tuple[dict, dict]:
    """Execute run_merge_authoritative from the script source against a real
    lock file in tmp_path; returns (printed_payload, lock_file_on_disk)."""
    module = ast.parse(_SCRIPT)
    wanted = {"read_lock", "write_lock", "acquire_flock", "run_merge_authoritative"}
    fns = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    base = str(tmp_path)
    lock_path = os.path.join(base, "skills-lock.json")
    with open(lock_path, "w") as f:
        json.dump({"version": LOCK_FILE_VERSION, "skills": entries}, f)

    printed: list[str] = []
    ns: dict = {
        "os": os,
        "json": json,
        "fcntl": fcntl,
        "time": time,
        "BASE": base,
        "LOCK_PATH": lock_path,
        "FLOCK_PATH": os.path.join(base, ".skills-sync.flock"),
        "LOCK_VERSION": LOCK_FILE_VERSION,
        "ARGS": {"entries": auth},
        "print": lambda s: printed.append(s),
    }
    exec(compile(ast.Module(body=fns, type_ignores=[]), "<skill_sync>", "exec"), ns)
    ns["run_merge_authoritative"]()
    with open(lock_path) as f:
        return json.loads(printed[-1]), json.load(f)


_AGENT = {"owner": "user", "sourceType": "agent"}
_LINKED = {
    "owner": "user",
    "sourceType": "langalpha-user",
    "sync": {"linkedSkillId": "abc"},
}
_PLATFORM_OLD = {"owner": "platform", "sourceType": "langalpha"}
_PLATFORM_NEW = {"owner": "platform", "sourceType": "langalpha", "v": 2}
_MANAGED = {"owner": "user", "sourceType": "langalpha-user"}


def test_script_merge_matches_host_merge(tmp_path):
    entries = {
        "agent-skill": _AGENT,
        "linked-skill": _LINKED,
        "stale-platform": _PLATFORM_OLD,
        "managed-old": _MANAGED,
    }
    auth = {"fresh-platform": _PLATFORM_NEW, "managed-old": _MANAGED}

    payload, on_disk = _run_merge(tmp_path, entries, auth)
    host = merge_lock_files(auth, entries)

    assert payload["skills"] == host["skills"]
    assert on_disk == {"version": LOCK_FILE_VERSION, "skills": host["skills"]}
    assert payload["skipped"] == []
    # Stale platform + managed entries purged, protected owners preserved.
    assert set(payload["skills"]) == {
        "agent-skill",
        "linked-skill",
        "fresh-platform",
        "managed-old",
    }


def test_racing_protected_claim_keeps_the_claim_and_reports_it(tmp_path):
    # The one deliberate divergence from merge_lock_files: an agent/linked
    # entry that appeared under an authoritative name since the caller's
    # snapshot wins the lock entry; the reconciler arbitrates the bytes.
    entries = {"contested": _AGENT}
    auth = {"contested": _PLATFORM_NEW, "other": _PLATFORM_NEW}

    payload, on_disk = _run_merge(tmp_path, entries, auth)

    assert payload["skipped"] == ["contested"]
    assert payload["skills"]["contested"] == _AGENT
    assert payload["skills"]["other"] == _PLATFORM_NEW
    assert on_disk["skills"] == payload["skills"]


def test_empty_authoritative_set_purges_only_unprotected(tmp_path):
    entries = {"agent-skill": _AGENT, "stale-platform": _PLATFORM_OLD}
    payload, _ = _run_merge(tmp_path, entries, {})
    assert set(payload["skills"]) == {"agent-skill"}
