from __future__ import annotations

from pathlib import Path

from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    PrebuiltWorkflowRegistry,
)


def _seed(root: Path, name: str, description: str) -> str:
    source = (
        f"export const meta = {{ name: '{name}', description: '{description}' }};\n"
        "return args;\n"
    )
    directory = root / "workflows" / name
    directory.mkdir(parents=True)
    (directory / "workflow.js").write_text(source)
    return source


def test_registry_loads_and_sorts_seed_workflows(tmp_path: Path) -> None:
    second = _seed(tmp_path, "second", "the second one")
    first = _seed(tmp_path, "first", "the first one")

    registry = PrebuiltWorkflowRegistry(tmp_path)

    assert registry.names() == ["first", "second"]
    assert registry.meta("first").description == "the first one"
    assert registry.get("second") == second
    # Flat <name>.js: mount keys share the user tier's shape so an edited
    # script lands on the name it runs under.
    assert registry.files() == {"first.js": first, "second.js": second}


def test_invalid_and_name_mismatched_seeds_are_skipped(
    tmp_path: Path, capsys
) -> None:
    syntax_dir = tmp_path / "workflows" / "syntax-bad"
    syntax_dir.mkdir(parents=True)
    (syntax_dir / "workflow.js").write_text("export const meta = {")
    mismatch_dir = tmp_path / "workflows" / "expected"
    mismatch_dir.mkdir(parents=True)
    (mismatch_dir / "workflow.js").write_text(
        "export const meta = { name: 'different', description: 'bad' };"
    )
    valid_dir = tmp_path / "workflows" / "valid"
    valid_dir.mkdir(parents=True)
    (valid_dir / "workflow.js").write_text(
        "export const meta = { name: 'valid', description: 'good' }; return 1;"
    )

    registry = PrebuiltWorkflowRegistry(tmp_path)

    assert registry.names() == ["valid"]
    assert "Skipping invalid prebuilt workflow" in capsys.readouterr().out


def test_absent_workflows_directory_is_empty(tmp_path: Path) -> None:
    registry = PrebuiltWorkflowRegistry(tmp_path)
    assert registry.names() == []
    assert registry.files() == {}
    assert registry.get("missing") is None
    assert registry.meta("missing") is None
