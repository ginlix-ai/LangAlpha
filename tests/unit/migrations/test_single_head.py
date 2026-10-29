"""The revision chain has one head.

Two branches can each add the next number; after a merge both claim it and
``alembic upgrade head`` refuses to start the server.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

_ROOT = Path(__file__).resolve().parents[3]


def test_the_migration_chain_has_one_head():
    config = Config(str(_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(_ROOT / "migrations"))
    script = ScriptDirectory.from_config(config)

    assert len(script.get_heads()) == 1, script.get_heads()
