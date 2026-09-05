"""What a user skill may not be called, and how that answer is arrived at.

The reservation exists so an upload cannot take a name a shipped or operator
skill already ships from disk: the sync overwrites last-source-wins, so the
name a user takes is the one the agent then loads. Both cases here are about
the listing half of that answer, because a reservation that comes back short
reserves nothing for whatever it missed.
"""

import pytest

from src.server.services.user_skills.validate import (
    SkillNamesUnavailable,
    _registered_skill_names,
    _reserved_skill_names,
)


@pytest.fixture
def drop_in(tmp_path, monkeypatch):
    """An operator drop-in root, which is writable while the server runs."""
    root = tmp_path / "skills"
    root.mkdir()
    monkeypatch.setattr(
        "src.server.services.user_skills.validate.host_skill_dirs",
        lambda *a: [root],
    )
    return root


class TestTheListingIsReadEveryTime:
    """Memoizing the whole set would defeat the thing it protects.

    The operator's drop-in root is writable at runtime, which is the point of
    it, so a set cached per directory path lets a warm worker keep answering
    from a listing taken before the operator's skill landed.
    """

    def test_a_skill_added_after_the_first_call_is_reserved(self, drop_in):
        assert "late" not in _reserved_skill_names(str(drop_in))

        (drop_in / "late").mkdir()

        assert "late" in _reserved_skill_names(str(drop_in))

    def test_the_registry_half_is_still_cached(self):
        # Module-level Python, so it cannot change under a running process
        # and is the one part worth memoizing.
        assert _registered_skill_names() is _registered_skill_names()


class TestADirectoryThatWillNotOpen:
    """``is_dir`` swallows OSError; ``iterdir`` is the one that raises.

    Answering "nothing is reserved here" would be the quieter half of the same
    bug, so this refuses instead and the upload route turns it into a 503.
    """

    def test_it_refuses_rather_than_reserving_nothing(self, drop_in, monkeypatch):
        from pathlib import Path

        real = Path.iterdir

        def _blocked(self):
            if self == drop_in:
                raise PermissionError(13, "Permission denied")
            return real(self)

        monkeypatch.setattr(Path, "iterdir", _blocked)

        with pytest.raises(SkillNamesUnavailable):
            _reserved_skill_names(str(drop_in))

    def test_a_readable_directory_still_answers(self, drop_in):
        (drop_in / "ours").mkdir()

        assert "ours" in _reserved_skill_names(str(drop_in))
