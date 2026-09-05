from __future__ import annotations

import re
import time
import uuid

import pytest

from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowScriptError,
    compile_check,
)


@pytest.mark.parametrize(
    "declaration",
    [
        "export const meta = { name: 'demo', description: 'test' };",
        "const meta = { name: 'demo', description: 'test' };",
        "\n  export   const meta = { name: 'demo', description: 'test' };",
    ],
)
def test_compile_check_strips_optional_export(declaration: str) -> None:
    meta = compile_check(f"{declaration}\nreturn 1;")
    assert meta.name == "demo"
    assert meta.description == "test"


def test_meta_may_follow_other_statements() -> None:
    meta = compile_check(
        "const helper = 1;\n"
        "export const meta = { name: 'later', description: 'not first' };\n"
        "return helper;"
    )
    assert meta.name == "later"


def test_meta_extraction_skips_nested_and_quoted_braces() -> None:
    script = r"""
export const meta = {
  name: "nested",
  description: "braces } { and escaped quote \" survive",
  phases: [{ title: '{one}' }, { title: `template } { literal` }],
  // ignored comment braces }}}
  whenToUse: 'comments', /* block comment { } */
};
return 1;
"""
    # A scan that stopped at any of those braces would extract an unbalanced
    # literal, which never evaluates — so compiling at all is the assertion.
    meta = compile_check(script)
    assert meta.name == "nested"
    assert meta.description == 'braces } { and escaped quote " survive'


def test_regex_literal_before_meta_does_not_hide_it() -> None:
    """A regex literal is not a string: quote and brace characters inside one
    must not shift where the declaration and its literal are found."""
    meta = compile_check(
        'const pattern = /["{]/;\n'
        "export const meta = { name: 'after-regex', description: 'found' };\n"
        "return pattern.source;"
    )
    assert meta.name == "after-regex"


def test_comment_containing_fake_meta_is_ignored() -> None:
    meta = compile_check(
        "// const meta = { name: 'fake', description: 'fake' };\n"
        "export const meta = { name: 'real', description: 'real' };\n"
        "return null;"
    )
    assert meta.name == "real"


def test_missing_meta_is_actionable() -> None:
    with pytest.raises(WorkflowScriptError, match="export const meta"):
        compile_check("return 1;")


def test_non_literal_meta_is_rejected() -> None:
    with pytest.raises(WorkflowScriptError, match="pure object literal"):
        compile_check(
            "const workflowName = 'dynamic';\n"
            "export const meta = { name: workflowName, description: 'bad' };\n"
            "return 1;"
        )


def test_meta_function_call_is_not_mistaken_for_its_object_argument() -> None:
    with pytest.raises(WorkflowScriptError, match="pure object literal"):
        compile_check(
            "export const meta = makeMeta({ name: 'bad', description: 'bad' });\n"
            "return 1;"
        )


@pytest.mark.parametrize("name", ["", "has space", "_leading", "x" * 65])
def test_bad_meta_name_is_rejected(name: str) -> None:
    with pytest.raises(WorkflowScriptError, match="meta.name"):
        compile_check(
            f"export const meta = {{ name: {name!r}, description: 'test' }};"
        )


def test_empty_description_is_rejected() -> None:
    with pytest.raises(WorkflowScriptError, match="meta.description"):
        compile_check("export const meta = { name: 'demo', description: '   ' };")


def test_oversized_description_is_rejected() -> None:
    """`meta.name` is bounded by its pattern; the description was bounded only
    by the script cap, yet it is pinned for the life of a compile-cache entry
    and copied into the registry, lifecycle frames and checkpoint records."""
    padded = "d" * 4096
    with pytest.raises(WorkflowScriptError, match="meta.description is 4096 chars"):
        compile_check(
            f"export const meta = {{ name: 'demo', description: '{padded}' }};"
        )


def test_a_normal_description_still_compiles() -> None:
    """The cap sits well past any real one-line summary."""
    summary = "Fan out research across tickers, then synthesize a brief. " * 4
    meta = compile_check(
        f"export const meta = {{ name: 'demo', description: '{summary}' }};"
    )
    assert meta.description == summary


def test_syntax_error_is_reported_with_js_detail() -> None:
    with pytest.raises(WorkflowScriptError, match="JavaScript syntax error") as exc_info:
        compile_check(
            "export const meta = { name: 'demo', description: 'test' };\n"
            "if ("
        )
    # _js_detail appends the parser's own "(at <source>:line:col)" — the part
    # that makes the message actionable for the authoring agent.
    assert re.search(r"\(at .+:\d+:\d+\)", str(exc_info.value))


def test_compile_check_does_not_execute_body() -> None:
    meta = compile_check(
        "export const meta = { name: 'demo', description: 'test' };\n"
        "throw new Error('runtime only');"
    )
    assert meta.name == "demo"


def test_closing_the_wrapper_early_does_not_reach_top_level() -> None:
    """A script that closes the async wrapper itself would put the rest of its
    body at the top level of whatever compiles it. Compiling has to stay a
    parse, or listing workflows would run their code."""
    script = (
        "});\n"
        "const t0 = Date.now(); while (Date.now() - t0 < 1500) {}\n"
        "export const meta = { name: 'demo', description: 'test' };\n"
        "void (async function () {\n"
    )
    started = time.monotonic()
    with pytest.raises(WorkflowScriptError):
        compile_check(script)
    # The spin is 1.5s; anything near it means the body executed.
    assert time.monotonic() - started < 0.75


def test_meta_cpu_burn_is_a_validation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CPU-burning meta initializer maps to WorkflowScriptError instead of
    leaking quickjs TimeoutError as a server exception."""
    from ptc_agent.agent.middleware.background_subagent.workflow import engine

    monkeypatch.setattr(engine, "_COMPILE_TIMEOUT", 0.1)
    with pytest.raises(WorkflowScriptError, match="compile CPU budget"):
        compile_check(
            "export const meta = { name: 'demo', description: 'd', "
            "_: (() => { while (true) {} })() };\nreturn 1;"
        )


@pytest.mark.asyncio
async def test_acompile_check_memoizes_by_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ptc_agent.agent.middleware.background_subagent.workflow import engine

    calls = 0
    real = engine.compile_check

    def counting(script: str):
        nonlocal calls
        calls += 1
        return real(script)

    monkeypatch.setattr(engine, "compile_check", counting)
    nonce = uuid.uuid4().hex
    script = f"const meta = {{ name: 'memo', description: '{nonce}' }};\nreturn 1;"
    first = await engine.acompile_check(script)
    second = await engine.acompile_check(script)
    assert first.description == nonce
    assert second.description == nonce
    assert calls == 1

    bad = f"const meta = {{ name: 'memo-{nonce}' }};\nreturn 1;"
    for _ in range(2):
        with pytest.raises(WorkflowScriptError, match="meta.description"):
            await engine.acompile_check(bad)
    assert calls == 2


@pytest.mark.parametrize(
    ("label", "quoted"),
    [
        ("block comment", "/*\nexport const meta = {{ name: 'old', description: 'x' }}\n*/"),
        ("template literal", "const quoted = `\nexport const meta = {{ name: 'old', description: 'x' }}\n`;"),
        ("line comment", "// export const meta = {{ name: 'old', description: 'x' }}"),
    ],
)
def test_a_quoted_meta_export_does_not_shadow_the_live_one(
    label: str, quoted: str
) -> None:
    """Stripping the first textual `export` leaves the real one inside the
    async wrapper, where nothing parses — so a script that merely *mentions*
    an earlier metadata block would be rejected as a syntax error it does not
    have. Which declaration is code is the parser's answer, not a regex's.
    """
    script = (
        f"{quoted.format()}\n"
        "export const meta = { name: 'live-one', description: 'the real one' };\n"
        "return 1;"
    )
    meta = compile_check(script)
    assert meta.name == "live-one", label
    assert meta.description == "the real one"


def test_a_quoted_export_keeps_its_own_text() -> None:
    """The scan blanks candidates in a throwaway copy. Blanking the script
    itself would silently rewrite a string the workflow goes on to use.
    """
    script = (
        "const quoted = 'export const meta = zzz';\n"
        "export const meta = { name: 'intact', description: 'd' };\n"
        "return quoted;"
    )
    assert compile_check(script).name == "intact"


def test_a_script_that_cannot_parse_reports_its_own_error() -> None:
    """No live declaration is locatable in a script that does not parse, and
    the useful thing to say about it is the syntax error — not that its
    metadata could not be found.
    """
    script = "export const meta = { name: 'x', description: 'd' };\nreturn 1 +;"
    with pytest.raises(WorkflowScriptError, match="syntax error"):
        compile_check(script)
