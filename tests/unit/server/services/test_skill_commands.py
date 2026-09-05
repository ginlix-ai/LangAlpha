"""Pins for the editable slash-command contracts: the alias charset, the
frontmatter ``command:`` seed, and the per-turn trigger fold."""

import io
import zipfile
from types import SimpleNamespace

from src.server.handlers.chat.request_prep import user_skill_commands
from src.server.services.user_skills.commands import free_seed, taken_triggers
from src.server.services.user_skills.validate import (
    valid_command,
    validate_skill_archive,
)


def _zip(name: str, skill_md: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"{name}/SKILL.md", skill_md)
    return buf.getvalue()


def _skill_md(name: str, command: str | None = None) -> str:
    front = f"name: {name}\ndescription: a test skill"
    if command is not None:
        front += f"\ncommand: {command}"
    return f"---\n{front}\n---\n\n# {name}\n"


class TestValidCommand:
    def test_accepts_name_shaped_aliases(self):
        assert valid_command("a")
        assert valid_command("abc-def-2")
        assert valid_command("x" * 64)

    def test_rejects_bad_charset_and_length(self):
        for bad in ("Upper", "under_score", "-lead", "trail-", "a b", "", "x" * 65):
            assert not valid_command(bad)


class TestFrontmatterCommandSeed:
    def test_valid_command_is_parsed(self):
        v = validate_skill_archive(_zip("probe", _skill_md("probe", "pb")))
        assert v.command == "pb"

    def test_missing_command_is_none(self):
        v = validate_skill_archive(_zip("probe", _skill_md("probe")))
        assert v.command is None

    def test_invalid_command_degrades_to_none(self):
        v = validate_skill_archive(_zip("probe", _skill_md("probe", "Bad_Name")))
        assert v.command is None


class TestUserSkillCommandsFold:
    def test_none_config_and_empty_config(self):
        assert user_skill_commands(None) is None
        assert user_skill_commands(SimpleNamespace()) is None

    def test_folds_overrides_and_user_skills(self):
        config = SimpleNamespace(
            skill_command_overrides={"market-watch": "mw"},
            user_skills=[
                SimpleNamespace(name="probe", command="pb"),
                SimpleNamespace(name="plain", command="plain"),
            ],
        )
        assert user_skill_commands(config) == {
            "mw": "market-watch",
            "pb": "probe",
            "plain": "plain",
        }

    def test_user_skill_wins_key_collision(self):
        config = SimpleNamespace(
            skill_command_overrides={"market-watch": "pb"},
            user_skills=[SimpleNamespace(name="probe", command="pb")],
        )
        assert user_skill_commands(config) == {"pb": "probe"}

    def test_workspace_row_beats_user_row_on_same_trigger(self):
        # Specs arrive name-sorted with tiers interleaved; the fold must sort
        # workspace-tier rows last so they win regardless of name order.
        config = SimpleNamespace(
            skill_command_overrides={},
            user_skills=[
                SimpleNamespace(name="alpha", command="foo", workspace_scoped=True),
                SimpleNamespace(name="zebra", command="foo", workspace_scoped=False),
            ],
        )
        assert user_skill_commands(config) == {"foo": "alpha"}


class TestFreeSeed:
    """The consolidated seed policy shared by upload and sandbox import."""

    def _validated(self, name="probe", command="zz-alias"):
        return SimpleNamespace(name=name, command=command)

    def test_free_seed_survives(self):
        assert free_seed(self._validated(), [], {}) == "zz-alias"

    def test_own_name_and_missing_seed_drop(self):
        assert free_seed(self._validated(command=None), [], {}) is None
        assert free_seed(self._validated(command="probe"), [], {}) is None

    def test_row_trigger_and_hidden_name_both_block(self):
        # The alias is the row's live trigger; its name stays reserved too,
        # so a later alias-clear always lands on a free trigger.
        rows = [{"name": "other", "command": "zz-alias", "workspace_id": None}]
        assert free_seed(self._validated(), rows, {}) is None
        assert free_seed(self._validated(command="other"), rows, {}) is None

    def test_builtin_override_blocks(self):
        assert free_seed(self._validated(), [], {"market-watch": "zz-alias"}) is None

    def test_exclude_skips_the_replaced_row(self):
        rows = [{"name": "probe", "command": "zz-alias", "workspace_id": None}]
        assert (
            free_seed(self._validated(), rows, {}, exclude=("probe", None))
            == "zz-alias"
        )
        assert taken_triggers(rows, exclude=("probe", None)) == set()
