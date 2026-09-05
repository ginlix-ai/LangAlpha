"""A provider name without a bucket is not a store.

The checked-in config selects "s3" while the bucket lives in .env, so a
deployment that never filled it in must read as disabled rather than route
every upload, workspace file backups included, into a store that fails.
The facade decides at import, so each case loads it in a fresh interpreter.
"""

from __future__ import annotations

import os
import subprocess
import sys

# The facade reads agent_config.yaml first, so the checked-in "s3" (or a
# local edit to "none") would decide these cases instead of the env. The
# child sees an empty config so the env var under test is what answers.
_PROBE = (
    "import yaml; yaml.safe_load = lambda *a, **k: {}; "
    "from src.utils import storage; "
    "print(storage.get_provider_id(), storage.is_storage_enabled())"
)
_BUCKET_VARS = ("STORAGE_BUCKET_NAME", "S3_BUCKET_NAME", "OSS_BUCKET_NAME")


def _probe(**env: str) -> str:
    clean = {k: v for k, v in os.environ.items() if k not in _BUCKET_VARS}
    clean.update(env)
    out = subprocess.run(
        [sys.executable, "-c", _PROBE],
        env=clean,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def test_a_provider_without_a_bucket_reads_as_disabled():
    assert _probe(STORAGE_PROVIDER="s3") == "none False"


def test_a_provider_with_a_bucket_stays_enabled():
    assert _probe(STORAGE_PROVIDER="s3", STORAGE_BUCKET_NAME="b") == "s3 True"
