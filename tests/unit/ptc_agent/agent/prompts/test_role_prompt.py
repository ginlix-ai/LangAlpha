"""The analyst's and the Chief of Staff's prompts keep to their own wording.

Nothing here pins the analyst's prompt: it is edited often, and a snapshot
would only be upgrade noise. What these check is the seam between the roles,
because a shared section written for one role argues with the other's, and the
model then follows both.
"""

from ptc_agent.agent.prompts import get_loader

_BASE = dict(current_time="2026-10-03 11:00 ET", subagent_summary="")


def test_only_the_chief_of_staff_reads_the_role_section():
    """A fork's Chief of Staff side points at `<role>`; leaked into the
    analyst's prompt, it points at a section that is not there."""
    loader = get_loader()
    for guidance in ("lean", "detailed"):
        assert "<role>" not in loader.get_system_prompt(**_BASE, guidance=guidance)
    prompt = loader.get_system_prompt(**_BASE, role="chief_of_staff")
    assert "<role>" in prompt
    assert "delegate_to_analyst" in prompt


def test_lean_keeps_the_role_and_drops_its_examples():
    loader = get_loader()
    lean = loader.get_system_prompt(**_BASE, role="chief_of_staff", guidance="lean")
    assert "delegate_to_analyst" in lean
    assert "## Examples" not in lean
    assert "## Examples" in loader.get_system_prompt(**_BASE, role="chief_of_staff")


def test_no_shared_section_tells_the_chief_of_staff_to_keep_a_workspace_index():
    """Home's agent.md is a notebook of what the user is working on. A shared
    section that still describes the analyst's index argues with `<role>`, and
    the model kept both layouts side by side when one did."""
    loader = get_loader()
    analyst = loader.get_system_prompt(**_BASE)
    chief = loader.get_system_prompt(**_BASE, role="chief_of_staff")
    for analyst_only in (
        "index of what each task produced",
        "Key findings, what each task produced",
        "`agent.md` is about the workspace",
        "What this workspace is doing",
        "project_q2_rebalance_thesis.md",
    ):
        assert analyst_only in analyst
        assert analyst_only not in chief


def test_an_empty_home_notebook_is_not_seeded_as_a_workspace_index():
    """The empty agent.md line is the first layout the model sees, so it has
    to send each role to its own."""
    loader = get_loader()

    def empty(**role):
        return loader.render(
            "envelope/baseline_agentmd.md.j2", path="/agent.md", content="", **role
        )

    assert "Thread Index" in empty()
    chief = empty(role="chief_of_staff")
    assert "Thread Index" not in chief
    assert "<role>" in chief
