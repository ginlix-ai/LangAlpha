"""Contract for the runtime-context templates.

Three properties, one per failure mode this design can have:

* the lean/detailed split holds fragment by fragment, not just on the assembled
  system prompt (``test_lean_subset_invariant.py`` renders whole surfaces, and
  the envelope fragments never appear in one);
* the baseline fragments are byte-deterministic for equal inputs, because they
  land inside the cached per-thread prefix and any wobble there is a full cache
  miss on every remaining turn of the thread;
* the static components actually reach the system prompt at both levels.
"""

from collections import Counter

import pytest
import yaml

from ptc_agent.agent.prompts import guidance_template_vars, init_loader
from ptc_agent.agent.prompts.loader import PromptLoader

# Representative kwargs per template. Values are stand-ins, but the *shapes*
# are the contract the envelope middleware renders against, so keep them in
# step with `envelope:` in config/prompts.yaml.
ENVELOPE_CASES: dict[str, dict] = {
    "envelope/header.md.j2": {},
    "envelope/turn.md.j2": {
        "opened": "3:02 PM EDT, Monday, September 8, 2026",
        "local_tz": "America/New_York",
        "market_line": "US: after-hours, next open Tue 09:30 ET",
        "elapsed_human": "19h",
        "sessions_closed": 1,
        "market": "US",
        "surface": "market_view",
        "symbol": "AAPL",
        "origin": "automation",
        "is_subagent": False,
        "surface_rules": "Surface market_view: ...",
    },
    "envelope/surface_rules.md.j2": {
        "surface": "slack",
        "symbol": None,
        "origin": "automation",
        "is_subagent": False,
        # A channel's wording comes from the client that renders it, so the
        # representative case for this fragment is caller-supplied text.
        "surface_rules": "Surface slack: plain text, one message per answer.",
        "handoff": False,
        "reset": False,
    },
    "envelope/updates.md.j2": {
        "updates": [
            {
                "kind": "market_watch",
                "text": "AAPL 232.10 (+1.2%) as of 15:01 ET",
                "provenance": "market_watch",
                "created_at": "2026-09-08T19:01Z",
                "schema_version": 1,
            },
            {
                "kind": "agentmd_changed",
                "text": "agent.md gained a Thread Index section.",
                "provenance": "another thread in this workspace",
                "created_at": "2026-09-08T14:02Z",
                "schema_version": 1,
            },
        ]
    },
    "envelope/update_row.md.j2": {
        "update": {
            "kind": "agent_md_changed",
            "text": "agent.md gained a Thread Index section.",
            "provenance": {"source": "sandbox", "writer": "subagent:research"},
            "created_at": "2026-09-08T14:02:00+00:00",
            "schema_version": 1,
        }
    },
    "envelope/baseline_agentmd.md.j2": {
        "path": "/agent.md",
        "content": "# Notes\n- a thing",
    },
    "envelope/baseline_memory.md.j2": {
        "path": ".agents/user/memory/memory.md",
        "content": "- [Style](feedback_style.md) how they want reports written",
        "readonly": False,
    },
    "envelope/baseline_identity.md.j2": {
        "name": "Alan",
        "timezone": "America/New_York",
        "locale": "en-US",
        "preferred_market": "US",
    },
    "envelope/baseline_files.md.j2": {},
    "envelope/baseline_mcp_servers.md.j2": {
        "content": "### market_data\n- get_quote: the last trade",
    },
    "envelope/baseline_skills.md.j2": {
        "content": "## Available Skills\n\n- **pdf**: read and write PDFs",
    },
}

STATIC_CASES: dict[str, dict] = {
    "components/time_rules.md.j2": {},
}

ALL_CASES = {**STATIC_CASES, **ENVELOPE_CASES}

BASELINE_TEMPLATES = [
    "envelope/baseline_agentmd.md.j2",
    "envelope/baseline_memory.md.j2",
    "envelope/baseline_identity.md.j2",
    "envelope/baseline_files.md.j2",
    "envelope/baseline_mcp_servers.md.j2",
    "envelope/baseline_skills.md.j2",
]

# Baseline fragments that must render their wrapper even with nothing to put in
# it, because the absence is the news: the model has to learn the file it is
# expected to keep does not exist yet. The harness blocks are the other case,
# so they are deliberately not here: nobody was supposed to write them, and an
# empty <mcp-servers> would state that there are no servers when in fact
# nothing was read.
ALWAYS_LABELLED = [
    "envelope/baseline_agentmd.md.j2",
    "envelope/baseline_memory.md.j2",
    "envelope/baseline_identity.md.j2",
    "envelope/baseline_files.md.j2",
]


def _render(template: str, level: str) -> str:
    return init_loader().render(
        template, **guidance_template_vars(level), **ALL_CASES[template]
    )


def _significant(text: str) -> Counter:
    return Counter(line.strip() for line in text.splitlines() if line.strip())


@pytest.mark.parametrize("template", sorted(ALL_CASES))
class TestLeanIsASubsetOfDetailed:
    def test_lean_introduces_no_new_line(self, template):
        extra = _significant(_render(template, "lean")) - _significant(
            _render(template, "detailed")
        )
        assert not extra, (
            f"{template}: lean renders text detailed never sees. Move it outside "
            f"the fence:\n" + "\n".join(f"  + {line}" for line in sorted(extra))
        )

    def test_lean_is_not_larger(self, template):
        assert len(_render(template, "lean")) <= len(_render(template, "detailed"))

    def test_both_levels_render_something(self, template):
        """A typo in a fence *value* silently empties a block at both levels and
        still passes the subset check, because lean adds nothing."""
        for level in ("lean", "detailed"):
            assert _render(template, level).strip(), f"{template} is empty at {level}"


def test_the_split_is_not_vacuous():
    """If no fragment differs between levels, the subset checks prove nothing."""
    differing = [t for t in ALL_CASES if _render(t, "lean") != _render(t, "detailed")]
    assert differing, "no runtime-context template renders lean differently"


def test_state_only_fragments_are_level_agnostic():
    """Fragments the contract calls one-liners, and the baseline blocks, carry
    no steering at all, so a guidance flip must not move them. This is what
    keeps a mid-thread model switch from rewriting the cached prefix."""
    for template in [
        *BASELINE_TEMPLATES,
        "envelope/turn.md.j2",
        "envelope/surface_rules.md.j2",
        "envelope/header.md.j2",
        "envelope/update_row.md.j2",
    ]:
        assert _render(template, "lean") == _render(template, "detailed"), (
            f"{template} varies with the guidance level"
        )


class TestBaselineDeterminism:
    """These land in the cached per-thread prefix; a byte of wobble costs the
    whole remaining thread."""

    @pytest.mark.parametrize("template", BASELINE_TEMPLATES)
    def test_repeated_renders_are_identical(self, template):
        renders = {_render(template, "detailed") for _ in range(5)}
        assert len(renders) == 1

    @pytest.mark.parametrize("template", BASELINE_TEMPLATES)
    def test_kwarg_order_does_not_change_the_bytes(self, template):
        kwargs = ALL_CASES[template]
        forward = init_loader().render(template, guidance="detailed", **kwargs)
        reversed_kwargs = dict(reversed(list(kwargs.items())))
        assert (
            init_loader().render(template, guidance="detailed", **reversed_kwargs)
            == forward
        )

    @pytest.mark.parametrize("template", BASELINE_TEMPLATES)
    def test_a_fresh_loader_renders_the_same_bytes(self, template):
        """The loader stamps a session time into every render context. A
        baseline block that picked it up would differ per worker process."""
        from datetime import UTC, datetime

        early = PromptLoader(session_start_time=datetime(2020, 1, 1, tzinfo=UTC))
        late = PromptLoader(session_start_time=datetime(2030, 6, 1, tzinfo=UTC))
        kwargs = ALL_CASES[template]
        assert early.render(template, guidance="detailed", **kwargs) == late.render(
            template, guidance="detailed", **kwargs
        )

    def test_baseline_blocks_keep_their_wrapper_tags(self):
        """The tag names are the trust boundary `baseline_files` and
        `time_rules` name by hand; renaming one silently unlabels the content."""
        assert '<agentmd path="/agent.md">' in _render(
            "envelope/baseline_agentmd.md.j2", "detailed"
        )
        assert '<memory path=".agents/user/memory/memory.md">' in _render(
            "envelope/baseline_memory.md.j2", "detailed"
        )
        assert "<user_identity>" in _render(
            "envelope/baseline_identity.md.j2", "detailed"
        )

    def test_the_harness_blocks_keep_their_wrapper_tags(self):
        """`<mcp-servers>` and `<skills>` are what a change row names by hand,
        and what the tool guide points the model at."""
        servers = _render("envelope/baseline_mcp_servers.md.j2", "detailed")
        assert "<mcp-servers>" in servers and "</mcp-servers>" in servers
        assert ".agents/tools/docs/<server_name>/" in servers, "the import lead is gone"
        assert "get_quote" in servers
        skills = _render("envelope/baseline_skills.md.j2", "detailed")
        assert "<skills>" in skills and "</skills>" in skills
        assert "## Available Skills" in skills, "the manifest is stated verbatim"

    @pytest.mark.parametrize(
        "template",
        ["envelope/baseline_mcp_servers.md.j2", "envelope/baseline_skills.md.j2"],
    )
    def test_a_harness_block_with_nothing_in_it_renders_nothing(self, template):
        """Unlike a file, nobody was supposed to write these. An empty labelled
        block would state that there are no servers or no skills, which is not
        the same as a read that has not answered."""
        assert init_loader().render(template, content="").strip() == ""

    def test_empty_content_still_renders_the_wrapper(self):
        """An absent agent.md or memory index has to arrive as an empty labelled
        block, not as nothing: the model needs to know the file is missing."""
        loader = init_loader()
        agentmd = loader.render(
            "envelope/baseline_agentmd.md.j2", path="/agent.md", content=""
        )
        assert "<agentmd" in agentmd and "No agent.md exists yet" in agentmd
        memory = loader.render(
            "envelope/baseline_memory.md.j2",
            path=".agents/user/memory/memory.md",
            content="",
        )
        assert "<memory" in memory and "does not exist yet" not in memory
        assert "exists yet" in memory

    def test_a_build_with_no_filesystem_is_not_told_to_write_memory(self):
        """Flash reads the index but has no tool that could create the file."""
        loader = init_loader()
        absent = loader.render(
            "envelope/baseline_memory.md.j2",
            path=".agents/user/memory/memory.md",
            content="",
            readonly=True,
        )
        assert "exists yet" in absent and "create a typed detail file" not in absent
        present = loader.render(
            "envelope/baseline_memory.md.j2",
            path=".agents/user/memory/memory.md",
            content="- [Style](feedback_style.md) how they want reports written",
            readonly=True,
        )
        assert "Read-only here" in present
        writable = loader.render(
            "envelope/baseline_memory.md.j2",
            path=".agents/user/memory/memory.md",
            content="- [Style](feedback_style.md) how they want reports written",
        )
        assert "Read-only" not in writable


class TestMissingKwargsDropTheLine:
    """A fragment whose values never arrived renders as nothing.

    Jinja resolves an unknown name to the empty string, so the tempting failure
    is a line that renders its punctuation around nothing (`` ()``, ``Surface
    ````) and reads as authoritative while saying nothing. An absent line is
    caught by eye on the first render; a blank stamp is not caught at all.
    """

    @pytest.mark.parametrize("template", sorted(ENVELOPE_CASES))
    def test_no_kwargs_renders_empty_or_static(self, template):
        rendered = init_loader().render(template, **guidance_template_vars("detailed"))
        if template in ALWAYS_LABELLED:
            # A file baseline is a labelled wrapper: an absent file still has
            # to arrive as an empty labelled block.
            assert rendered.strip()
        elif template == "envelope/header.md.j2":
            assert rendered.strip()
        else:
            assert rendered.strip() == "", (
                f"{template} renders a headed blank when nothing is supplied:\n{rendered}"
            )

    def test_a_partial_run_line_still_reads(self):
        loader = init_loader()
        assert loader.render("envelope/turn.md.j2", is_subagent=True).strip() == (
            "You are a subagent reporting to a parent agent"
        )
        assert loader.render("envelope/turn.md.j2", surface="web").strip() == (
            "Surface `web`"
        )
        assert loader.render("envelope/turn.md.j2", origin="automation").strip() == (
            "Started by automation"
        )

    def test_a_turn_row_drops_the_lines_it_has_no_values_for(self):
        """A first turn has no gap and a subagent has no market; both render as
        an absent line rather than as a blank heading."""
        rendered = (
            init_loader()
            .render(
                "envelope/turn.md.j2", opened="9:00 AM EDT, Tuesday, September 8, 2026"
            )
            .strip()
        )
        assert rendered == "9:00 AM EDT, Tuesday, September 8, 2026"

    def test_one_closed_session_is_singular(self):
        loader = init_loader()
        one = loader.render(
            "envelope/turn.md.j2", elapsed_human="19h", sessions_closed=1, market="US"
        )
        many = loader.render(
            "envelope/turn.md.j2", elapsed_human="3d", sessions_closed=2, market="US"
        )
        assert "(1 US session closed in between)" in one
        assert "(2 US sessions closed in between)" in many


class TestUpdates:
    def test_market_watch_text_renders_verbatim(self):
        """The watch stamp is already formatted for the model; decorating it
        would break the format the market-watch skill teaches."""
        rendered = _render("envelope/updates.md.j2", "detailed")
        assert "\nAAPL 232.10 (+1.2%) as of 15:01 ET\n" in rendered

    def test_other_kinds_carry_their_provenance(self):
        rendered = _render("envelope/updates.md.j2", "lean")
        assert "agentmd_changed" in rendered
        assert "another thread in this workspace" in rendered

    def test_provenance_may_be_a_mapping(self):
        """The writer side records provenance as a bag of fields; a caller that
        hands over a bare label reads the same. Both have to land as text."""
        rendered = init_loader().render(
            "envelope/updates.md.j2",
            updates=[
                {
                    "kind": "memory_changed",
                    "text": "Two pointers added to the user memory index.",
                    "provenance": {"source": "the workspace tier"},
                    "created_at": "2026-09-08T14:02Z",
                    "schema_version": 2,
                }
            ],
        )
        assert "the workspace tier" in rendered
        assert "{" not in rendered, "a provenance mapping leaked its repr"

    def test_schema_version_is_not_rendered(self):
        """It is dispatch machinery for the envelope, not prompt surface."""
        assert "schema_version" not in _render("envelope/updates.md.j2", "detailed")


class TestHarnessRows:
    """The two rows that report a harness block rather than a file."""

    @pytest.mark.parametrize(
        ("kind", "subject", "block"),
        [
            ("mcp_servers_changed", "MCP server list", "<mcp-servers>"),
            ("skills_changed", "skills manifest", "<skills>"),
        ],
    )
    def test_the_row_names_its_subject_and_block(self, kind, subject, block):
        rendered = init_loader().render(
            "envelope/update_row.md.j2",
            update={
                "kind": kind,
                "text": "--- a\n+++ b\n+a new server",
                "provenance": {"source": "harness"},
                "created_at": "2026-09-08T14:02:00+00:00",
                "schema_version": 1,
            },
        )
        assert f"The {subject} changed after the copy in {block} was frozen" in rendered
        assert "+a new server" in rendered
        # No writer to credit, and no file to send the model off to read.
        assert "last edit by" not in rendered
        assert "the file" not in rendered
        assert f"**{kind}**" not in rendered, "the labelled fallback, not the prose"


class TestTheRosterSplit:
    """Where the MCP roster is stated depends on whether there is a baseline.

    The main agents freeze it into theirs, so their prompt points at the
    element and stays byte-stable while servers come and go. A subagent has no
    baseline, so the component still renders the roster it is handed.
    """

    def test_the_main_prompt_points_at_the_baseline_element(self):
        prompt = init_loader().get_system_prompt(subagent_summary="")
        assert "`<mcp-servers>` element of the runtime-context block" in prompt

    def test_a_subagent_states_its_roster_inline(self):
        rendered = init_loader().render(
            "components/tool_guide.md.j2",
            tool_summary="- yfinance: quotes and fundamentals",
        )
        assert "- yfinance: quotes and fundamentals" in rendered
        assert "<mcp-servers>" not in rendered


class TestStaticComponentsReachTheSystemPrompt:
    @pytest.mark.parametrize("level", ["lean", "detailed"])
    def test_sections_are_included(self, level):
        prompt = init_loader().get_system_prompt(
            subagent_summary="", **guidance_template_vars(level)
        )
        for tag in ("<time_rules>",):
            assert tag in prompt, f"{tag} missing from the {level} system prompt"

    @pytest.mark.parametrize("level", ["lean", "detailed"])
    def test_the_runtime_context_framing_survives_the_lean_render(self, level):
        """Trust framing is not scaffolding: stripping it at lean would leave a
        small model reading machine state as if the user had typed it."""
        prompt = init_loader().get_system_prompt(
            subagent_summary="", **guidance_template_vars(level)
        )
        assert "frozen for the turn" in prompt
        assert "on the same footing as `<agentmd>` and `<memory>`" in prompt

    def test_the_stamp_sentence_names_every_carrier_shape(self):
        """The prefix is cached while a mid-turn fallback can change the
        model, so the sentence has to hold for whichever shape the stamp takes."""
        prompt = init_loader().get_system_prompt(
            subagent_summary="", **guidance_template_vars("lean")
        )
        assert "as a system or developer message just below the user's message" in prompt
        assert "`<system-reminder>` block at the end of it" in prompt
        assert prompt.count("Each turn opens with a stamp") == 1

    def test_the_baseline_file_blocks_carry_their_own_trust_preface(self):
        """The rule sits next to the text it governs, which is why it is no
        longer a section of the prefix the model reads long before the files."""
        from ptc_agent.agent.middleware.runtime_context import (
            BaselineContextMiddleware,
            BaselineEpoch,
        )

        block = BaselineContextMiddleware(guidance="lean")._render_block(
            BaselineEpoch.from_state(
                {"agent_md": {"path": "/agent.md", "text": "# Notes\n- a thing"}}
            )
        )

        assert "The file blocks below" in block
        assert block.index("The file blocks below") < block.index("<agentmd")

    def test_the_stamp_itself_is_not_in_the_static_prompt(self):
        """Time rules are static and cacheable; the clock is not. Putting the
        stamp back in a system template is the defect this slice removes."""
        prompt = init_loader().get_system_prompt(subagent_summary="")
        assert "Current Date/Time" not in prompt


def test_prompts_yaml_documents_every_envelope_template():
    """The yaml block is what task-A wiring reads for kwarg names, so a template
    added without an entry there is a template nobody can call correctly."""
    loader = init_loader()
    config = yaml.safe_load(
        (loader.templates_dir.parent / "config" / "prompts.yaml").read_text()
    )
    documented = {entry["template"] for entry in config["envelope"].values()}
    on_disk = {
        f"envelope/{path.name}"
        for path in (loader.templates_dir / "envelope").glob("*.md.j2")
    }
    assert documented == on_disk
    assert on_disk == set(ENVELOPE_CASES), "an envelope template has no test case"
