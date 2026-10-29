"""What a typical benchmark task costs on a model, at each reasoning level.

``manifest/task_costs.json`` holds a measured dollar cost per task for each
model and reasoning level, taken from the benchmark source the file names. A
dollar figure ages with every price change and means little on its own, so
what reaches the model list is each figure divided by one baseline's: a model
and a level, both named in the file, chosen as that model's default so that
1.0 reads as what the baseline costs out of the box. The figure is the whole
task, reasoning and answer together, which is why it can rise with the level
while the per-token rate stays the same.

The source does not measure every level a model offers, and the gaps are
filled from above. A higher level is a larger reasoning budget, so an
unmeasured level is read as costing no more than the nearest measured level
above it, and it carries that figure marked as not measured: an upper bound
the reader can show as one, rather than an estimate that could understate. A
level with nothing measured above it has no bound and is left out.

Where the source skips the levels that matter for a model, ``model_sources``
names another that measured them, often the provider's own launch chart. That
chart's dollars are not the source's, since two benchmarks put different tasks
behind a dollar, so only its ratios between levels are taken: they scale a
level the source did measure, and the figures stay comparable across models.

Some models are measured once without naming a level. Those carry a single
``"any"`` figure in place of a per-level breakdown.

A model the source never measured can still be bounded, when it runs the same
tokens as one it did, at no more than a known multiple of that model's rates:
a faster tier of the same model, say. The file's ``bounds`` names the measured
model and the multiple, and every level carries that model's figure times the
multiple, marked as not measured, for the same reason a gap is.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

TASK_COSTS_PATH = Path(__file__).parent / "manifest" / "task_costs.json"

#: The key for a figure measured without naming a reasoning level.
ANY_EFFORT = "any"

#: Per model, per level, the figure as a multiple of the baseline's.
RelativeTable = dict[str, dict[str, float]]


@dataclass(frozen=True, slots=True)
class TaskCosts:
    """A usable file: the baseline and every figure as a multiple of it."""

    #: ``{"model": <manifest key>, "effort": <level>}``, the figure that reads as 1.0.
    baseline: dict[str, str]
    relative: RelativeTable
    #: Per unmeasured model, the measured model whose figures bound it and the multiple.
    bounds: dict[str, tuple[str, float]] = field(default_factory=dict)


def _usd(value: Any) -> Optional[float]:
    """A dollar figure that can be divided by, or None.

    A bool is an int to Python but never a cost, and a figure that is zero or
    negative would turn every ratio built on it into nonsense, so neither
    counts.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return float(value)


def parse_task_costs(data: Any) -> Optional[TaskCosts]:
    """The baseline, and every model's figures divided by its figure.

    None when the file cannot be read as a table at all or the baseline has no
    usable figure, since every value is a ratio to that one number. A single
    bad figure costs only its own level, which then reads as unmeasured.
    """
    if not isinstance(data, dict):
        logger.warning("task_costs.json is not an object; task costs are omitted")
        return None
    models = data.get("models")
    baseline = data.get("baseline")
    if not isinstance(models, dict) or not isinstance(baseline, dict):
        logger.warning(
            "task_costs.json needs a 'models' object and a 'baseline' object; "
            "task costs are omitted"
        )
        return None

    base_model, base_effort = baseline.get("model"), baseline.get("effort")
    base_figures = models.get(base_model)
    base = _usd(base_figures.get(base_effort)) if isinstance(base_figures, dict) else None
    if base is None:
        logger.warning(
            f"task_costs.json baseline {base_model!r} at {base_effort!r} has no "
            "usable figure; task costs are omitted"
        )
        return None

    table: RelativeTable = {}
    for model, figures in models.items():
        if not isinstance(figures, dict):
            logger.warning(f"Ignoring task costs for {model!r}: not an object")
            continue
        relative = {}
        for effort, value in figures.items():
            usd = _usd(value)
            if usd is None:
                logger.warning(f"Ignoring task cost {value!r} for {model!r} at {effort!r}")
                continue
            relative[effort] = usd / base
        table[model] = relative
    return TaskCosts(
        baseline={"model": base_model, "effort": base_effort},
        relative=table,
        bounds=_bounds(data.get("bounds"), table),
    )


def _bounds(data: Any, table: RelativeTable) -> dict[str, tuple[str, float]]:
    """Each usable entry of the file's ``bounds``, as (measured model, multiple).

    A bound must point at a model with figures of its own, so a chain of bounds
    or a misspelled name drops the one entry rather than showing a figure no
    measurement stands behind.
    """
    if data is None:
        return {}
    if not isinstance(data, dict):
        logger.warning("Ignoring task cost bounds: not an object")
        return {}
    bounds = {}
    for model, bound in data.items():
        source = bound.get("model") if isinstance(bound, dict) else None
        times = _usd(bound.get("times")) if isinstance(bound, dict) else None
        if not isinstance(source, str) or not table.get(source) or times is None:
            logger.warning(f"Ignoring task cost bound for {model!r}: {bound!r}")
            continue
        bounds[model] = (source, times)
    return bounds


def _read_json(path: Path) -> Any:
    """The file's parsed contents, or None with a warning when unreadable."""
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        logger.warning(f"No task costs at {path}; task costs are omitted")
    except (OSError, ValueError) as e:
        logger.warning(f"Could not read task costs at {path}: {e}; task costs are omitted")
    return None


@lru_cache(maxsize=1)
def _shipped() -> Optional[TaskCosts]:
    """The shipped file, read and parsed once per process, or None with a
    warning when it is unusable.

    Once, like the rest of the manifest: the file ships with the code and
    changes only with a deploy. The model list is served from these figures
    and must keep working without them, so no failure here goes past a log
    line.
    """
    return parse_task_costs(_read_json(TASK_COSTS_PATH))


def task_cost_baseline() -> Optional[dict[str, str]]:
    """The model and level every relative figure is a multiple of, or None.

    ``{"model": <manifest key>, "effort": <level>}`` from the shipped file,
    present only when its figures are usable, so the baseline and the figures
    never disagree about whether there are any.
    """
    shipped = _shipped()
    return dict(shipped.baseline) if shipped is not None else None


def task_cost(
    model_name: str,
    efforts: Sequence[str],
    costs: Optional[TaskCosts] = None,
) -> Optional[dict[str, dict[str, Any]]]:
    """The relative task cost at each level this model offers, or None.

    ``efforts`` is the model's own ladder in ascending order, and the result
    follows it, keyed only by levels the model offers: a level the source
    measured but the model does not expose is never shown. Each value is
    ``{"value": float, "measured": bool}``, where an unmeasured level carries
    the figure of the nearest measured level above it. A model measured once
    gets ``{"any": ...}`` instead, and one with no figure that fits gets None.

    ``costs`` defaults to the shipped file, read once.
    """
    if costs is None:
        costs = _shipped()
    if costs is None:
        return None
    figures = costs.relative.get(model_name)
    if not figures:
        if model_name not in costs.bounds:
            return None
        source, times = costs.bounds[model_name]
        bounded = task_cost(source, efforts, costs)
        if bounded is None:
            return None
        return {
            effort: {"value": cost["value"] * times, "measured": False}
            for effort, cost in bounded.items()
        }

    # Walk down from the top so each gap can take the last figure seen above it.
    filled: dict[str, dict[str, Any]] = {}
    ceiling: Optional[float] = None
    for effort in reversed(efforts):
        if effort in figures:
            ceiling = figures[effort]
            filled[effort] = {"value": ceiling, "measured": True}
        elif ceiling is not None:
            filled[effort] = {"value": ceiling, "measured": False}
    if filled:
        return {effort: filled[effort] for effort in efforts if effort in filled}

    if ANY_EFFORT in figures:
        return {ANY_EFFORT: {"value": figures[ANY_EFFORT], "measured": True}}
    return None
