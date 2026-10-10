"""The projection and slice digests cover the code they key.

Stored lines carry no expiry, so a module that shapes them but sits outside
``projection_sources()`` would let a deploy keep serving what the old code
projected; a stored slice is trusted while its key matches, so code that
decides its bytes outside ``slices.slice_sources()`` would leave the slices
cut by the old rule. Every import of ours a digested module makes is
digested too, or excluded here with the reason its code cannot change what
the stored rows hold.
"""

from __future__ import annotations

import ast
from pathlib import Path

from src.server.services.history import slices
from src.server.services.history.replay import keys

ROOT = keys._SRC.parent
ANY = frozenset({"*"})

# Module -> (names a digested module may import from it, why its code cannot
# change stored lines). ANY admits every name; a narrower set flags the next
# name imported, which may well shape lines.
EXCLUDED: dict[str, tuple[frozenset[str], str]] = {
    "src/server/services/history/reader.py": (
        ANY,
        "Finds the checkpoints slices are cut between, whose ids a stored "
        "row is checked against.",
    ),
    "src/server/services/history/slices.py": (
        ANY,
        "Cuts and encodes the slices lines are projected from, digested into "
        "the slice key, which the lines key carries.",
    ),
    "src/server/database/conversation/turn_slices.py": (
        ANY,
        "Stores and reads the slices and lines themselves.",
    ),
    "src/server/database/pool.py": (ANY, "Connections."),
    "src/server/database/runs/subagent_runs.py": (
        ANY,
        "The run ledger, read as data: a run's status decides whether lines "
        "are stored; the statuses themselves are in contracts/status.py.",
    ),
    "src/server/database/conversation/replay_rows.py": (
        frozenset({"ThreadRows"}),
        "The rows a projection reads, whose versions are in the lines key.",
    ),
    "src/server/database/replay_facts.py": (
        frozenset({"LEGACY_KEY"}),
        "Names the key legacy facts sit under: those resolve the row's stored "
        "events, so where they sit cannot change what lines hold.",
    ),
    "src/server/utils/pg_sanitize.py": (
        frozenset({"SafeJson", "strip_pg_nul_str"}),
        "Binds provenance rows on write; the projection reads them back.",
    ),
    "src/server/services/history/task_streams.py": (
        ANY,
        "Whether a task's stream sealed, which decides whether lines are "
        "stored, never what they hold.",
    ),
    "src/config/settings.py": (
        frozenset({"get_redis_ttl_workflow_events"}),
        "How long a sealed stream outlives its task, same decision.",
    ),
    "src/server/services/subagent_liveness.py": (
        ANY,
        "A task card's live status, stamped at read time (``finish_lines``).",
    ),
    "src/utils/storage/__init__.py": (
        frozenset({"get_bytes"}),
        "Offloaded widget data, inlined at read time.",
    ),
    "src/server/services/history/replay/errors.py": (
        ANY,
        "Error types: why a read gives up, never what a line holds.",
    ),
    "src/server/services/runs/sse_producer.py": (
        frozenset({"resolve_token_threshold"}),
        "Its value, not its code, is in the lines key.",
    ),
    "src/llms/pricing_utils.py": (
        frozenset({"calculate_total_cost"}),
        "token_counter's cost tracking; the projection calls only "
        "extract_token_usage, which reads neither.",
    ),
    "src/llms/llm.py": (
        frozenset({"LLM"}),
        "token_counter's cost tracking; the projection calls only "
        "extract_token_usage, which reads neither.",
    ),
}

# The same, for the slice key's sources.
SLICE_EXCLUDED: dict[str, tuple[frozenset[str], str]] = {
    "src/server/services/history/reader.py": (
        frozenset({"TurnAnchor", "CheckpointHistoryReader"}),
        "Type annotations; a turn slice carries its anchor, and the cut "
        "reads only whether it is a resume, a fact of the walk.",
    ),
    "src/server/database/conversation/turn_slices.py": (
        frozenset({"StoredSlice"}),
        "A type annotation: the stored row a slice is read back from.",
    ),
}


def _module_file(module: str) -> Path | None:
    # ptc_agent is a top-level package that lives under src/.
    parts = module.split(".")
    base = ROOT.joinpath(*parts) if parts[0] == "src" else ROOT.joinpath("src", *parts)
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.exists():
            return candidate
    return None


def _package_of(path: Path, level: int) -> str:
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    package = parts[:-1]
    return ".".join(package[: len(package) - (level - 1)])


def _imports(path: Path, roots: tuple[str, ...]) -> list[tuple[Path, str]]:
    """``(module file, name)`` for every import in ``path`` from a package
    under ``roots``; the name is ``*`` when the import is of the module
    itself."""
    out: list[tuple[Path, str]] = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in roots:
                    found = _module_file(alias.name)
                    assert found, f"{path}: unresolved import {alias.name}"
                    out.append((found, "*"))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                package = _package_of(path, node.level)
                module = f"{package}.{module}" if module else package
            if module.split(".")[0] not in roots:
                continue
            for alias in node.names:
                submodule = _module_file(f"{module}.{alias.name}")
                if submodule is not None:
                    out.append((submodule, "*"))
                    continue
                found = _module_file(module)
                assert found, f"{path}: unresolved import {module}"
                out.append((found, alias.name))
    return out


def _unaccounted(
    digested: list[Path],
    excluded: dict[str, tuple[frozenset[str], str]],
    roots: tuple[str, ...],
) -> list[str]:
    out: list[str] = []
    for path in sorted(digested):
        for target, name in _imports(path, roots):
            if target in digested:
                continue
            key = str(target.relative_to(ROOT))
            names, _why = excluded.get(key, (frozenset(), ""))
            if names is not ANY and name not in names:
                out.append(f"{path.relative_to(ROOT)} imports {key}:{name}")
    return out


def _imported(digested: list[Path], roots: tuple[str, ...]) -> set[str]:
    return {
        str(target.relative_to(ROOT))
        for path in digested
        for target, _name in _imports(path, roots)
    }


def test_every_src_import_of_the_projection_is_digested_or_excluded():
    unaccounted = _unaccounted(keys.projection_sources(), EXCLUDED, ("src",))
    assert not unaccounted, (
        "Add each module to keys.projection_sources(), or to EXCLUDED with "
        "why its code cannot change stored lines:\n" + "\n".join(unaccounted)
    )


def test_every_exclusion_is_still_imported():
    """An exclusion nothing imports any more would quietly admit the module
    when something imports it again for a reason that does shape lines."""
    imported = _imported(keys.projection_sources(), ("src",))
    assert sorted(set(EXCLUDED) - imported) == []


# The slice key follows ptc_agent imports too: the cut calls the transcript
# classifier, which no lines version would catch.
_SLICE_ROOTS = ("src", "ptc_agent")


def test_every_import_of_the_slice_cut_is_digested_or_excluded():
    unaccounted = _unaccounted(slices.slice_sources(), SLICE_EXCLUDED, _SLICE_ROOTS)
    assert not unaccounted, (
        "Add each module to slices.slice_sources(), or to SLICE_EXCLUDED "
        "with why its code cannot change a slice's bytes:\n"
        + "\n".join(unaccounted)
    )


def test_a_dropped_slice_dependency_is_caught():
    """The cut finds a span's input by the transcript classifier, which the
    key once left out: a change there moved what slices are cut without
    moving the key."""
    without = [p for p in slices.slice_sources() if p.name != "classify.py"]

    assert any(
        line.startswith(
            "src/server/services/history/slices.py imports "
            "src/ptc_agent/agent/transcript/classify.py"
        )
        for line in _unaccounted(without, SLICE_EXCLUDED, _SLICE_ROOTS)
    )


def test_every_slice_exclusion_is_still_imported():
    imported = _imported(slices.slice_sources(), _SLICE_ROOTS)
    assert sorted(set(SLICE_EXCLUDED) - imported) == []
