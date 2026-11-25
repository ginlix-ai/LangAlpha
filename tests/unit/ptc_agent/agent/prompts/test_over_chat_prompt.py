"""The standing rules for talking with the user over chat, in the system prompt.

A turn from a chat app brings that app's own delivery rules; what stays the
same across every app (reply first, keep them posted, write like a colleague,
never reroute a refused send) is fixed text in `<working_with_the_user>`, and
only where the build has the messaging tools. It sits in the cached prefix, so
it has to read the same for every turn, role and guidance level.
"""

from pathlib import Path

import pytest

from ptc_agent.agent.prompts import get_loader, guidance_template_vars

REPO_ROOT = Path(__file__).resolve().parents[5]
HEADING = "## Over Chat"
POINTER = ".agents/skills/langalpha-doc/references/chat.md"


def _prompt(**kwargs) -> str:
    return get_loader().get_system_prompt(current_time="2026-10-08 09:00 ET", **kwargs)


def _section(prompt: str) -> str:
    """The Over Chat section, up to the end of `<working_with_the_user>`."""
    start = prompt.index(HEADING)
    return prompt[start : prompt.index("</working_with_the_user>", start)]


@pytest.mark.parametrize("guidance", ["lean", "detailed"])
@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
class TestTheSectionFollowsTheMessagingTools:
    def test_it_renders_inside_working_with_the_user(self, guidance, role):
        prompt = _prompt(
            channels_enabled=True, role=role, **guidance_template_vars(guidance)
        )

        block = prompt[
            prompt.index("<working_with_the_user>") : prompt.index(
                "</working_with_the_user>"
            )
        ]
        assert HEADING in block
        assert "reply with\n`send_message`" in block
        # The page on how each app shows a message is named with the other
        # langalpha-doc pages, in the section that lists them.
        assert POINTER not in block

    def test_its_page_is_named_with_the_doc_skill_s_others(self, guidance, role):
        prompt = _prompt(
            channels_enabled=True,
            langalpha_doc_enabled=True,
            role=role,
            **guidance_template_vars(guidance),
        )

        doc = prompt[prompt.index("<langalpha_doc>") : prompt.index("</langalpha_doc>")]
        assert POINTER in doc
        assert prompt.count(POINTER) == 1

    @pytest.mark.parametrize("off", [{"channels_enabled": False}, {}])
    def test_it_is_absent_without_them(self, guidance, role, off):
        prompt = _prompt(
            role=role, langalpha_doc_enabled=True, **guidance_template_vars(guidance), **off
        )

        assert HEADING not in prompt
        assert POINTER not in prompt


def test_it_is_the_same_text_for_every_role_and_level():
    """Standing rules are facts the model needs at either level, and the
    prefix is shared, so nothing in the section may move with the build."""
    sections = {
        _section(
            _prompt(channels_enabled=True, role=role, **guidance_template_vars(level))
        )
        for role in ("analyst", "chief_of_staff")
        for level in ("lean", "detailed")
    }

    assert len(sections) == 1
    (section,) = sections
    for rule in ("Reply first.", "Keep them posted.", "If a send is refused"):
        assert rule in section
    assert "{" not in section


def test_the_pointer_names_a_file_the_skill_ships():
    """`.agents/skills/<skill>/` is the sandbox copy of the bundle's skill
    folder, so the page the section names has to be in the repo."""
    relative = POINTER.removeprefix(".agents/skills/")
    shipped = REPO_ROOT / "plugins" / "langalpha_service" / "skills" / relative

    assert shipped.is_file()
