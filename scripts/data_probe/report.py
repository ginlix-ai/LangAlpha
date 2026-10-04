"""Terminal rendering. Uses ``rich`` when it happens to be installed.

The probe is a diagnostic tool run by hand, so the table is the product; plain
aligned text is the contract and rich is a cosmetic upgrade, never a dependency.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

from market_protocol.routing.ruleset import Cell, ProbedProvider, Ruleset


def _rich_console() -> Optional[Any]:
    try:
        from rich.console import Console
    except ImportError:
        return None
    return Console()


def render_table(headers: Sequence[str], rows: Iterable[Sequence[str]], title: str = "") -> str:
    """Aligned plain-text table (the fallback, and what the tests read)."""
    body = [[("" if c is None else str(c)) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in body:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = []
    if title:
        lines.append(title)
    lines.append("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip())
    lines.append("  ".join("-" * w for w in widths).rstrip())
    for row in body:
        lines.append("  ".join(row[i].ljust(widths[i]) for i in range(len(headers))).rstrip())
    return "\n".join(lines)


def print_table(headers: Sequence[str], rows: Iterable[Sequence[str]], title: str = "") -> None:
    rows = [list(r) for r in rows]
    console = _rich_console()
    if console is None:
        print(render_table(headers, rows, title))
        return
    from rich.table import Table

    table = Table(title=title or None, header_style="bold")
    for h in headers:
        # Never wrap: a cell name split across four lines is less readable than
        # a table that scrolls sideways.
        table.add_column(h, no_wrap=True)
    for row in rows:
        table.add_row(*["" if c is None else str(c) for c in row])
    console.width = max(console.width, _plain_width(headers, rows) + 4)
    console.print(table)


def _plain_width(headers, rows) -> int:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len("" if cell is None else str(cell)))
    return sum(widths) + 3 * len(widths)


def _fmt(value: Any, absent: str = "-") -> str:
    if value is None:
        return absent
    if isinstance(value, bool):
        return "yes" if value else "NO"
    return str(value)


def provider_rows(cell: Cell) -> list[list[str]]:
    return [_provider_row(cell, p) for p in cell.providers]


# Column values for price_treatment: the full enum spellings make the table
# three times wider than the decision they inform.
_TREATMENT_SHORT = {
    "raw": "raw", "split_adjusted": "split", "dividend_adjusted": "div",
}


def _provider_row(cell: Cell, p: ProbedProvider) -> list[str]:
    return [
        cell.key.as_str(),
        p.name,
        f"{p.coverage_hit}/{p.coverage_total}",
        _fmt(p.entitled),
        _fmt(p.session_complete),
        _fmt(p.anchors_ok),
        _TREATMENT_SHORT.get(p.price_treatment or "", p.price_treatment or "-"),
        _fmt(p.lag_s),
        _fmt(p.tier),
        _fmt(p.calls_per_minute),
        _fmt(p.cost_calls),
        p.excluded_reason or "routable",
    ]


PROVIDER_HEADERS = (
    "cell", "provider", "cov", "entitled", "complete", "anchors", "adj",
    "lag_s", "tier", "cpm", "cost", "status",
)


def print_cells(cells: Sequence[Cell], title: str = "") -> None:
    rows: list[list[str]] = []
    for cell in cells:
        rows.extend(provider_rows(cell))
    print_table(PROVIDER_HEADERS, rows, title)


def print_ruleset(ruleset: Ruleset, *, verbose: bool = False) -> None:
    rows = [
        [
            c.key.as_str(),
            c.phase_at_probe,
            " > ".join(c.routable_names()) or "(none)",
            ", ".join(f"{p.name}:{p.excluded_reason}" for p in c.providers if not p.routable) or "-",
        ]
        for c in ruleset.cells
    ]
    print_table(("cell", "phase", "order", "excluded"), rows,
                f"data routing ruleset, generated {ruleset.generated_at:%Y-%m-%d %H:%M %Z}")

    provider_lines = [
        f"{name}: configured={info.configured} fingerprint={info.fingerprint or '-'}"
        for name, info in sorted(ruleset.providers.items())
    ]
    if provider_lines:
        print("\nproviders")
        for line in provider_lines:
            print(f"  {line}")

    if not verbose:
        return
    for cell in ruleset.cells:
        print(f"\n{cell.key.as_str()}  phase={cell.phase_at_probe}  "
              f"canaries={', '.join(cell.canaries)}")
        print_table(PROVIDER_HEADERS[1:], [row[1:] for row in provider_rows(cell)])
        for p in cell.providers:
            for note in p.notes:
                print(f"    {p.name}: {note}")
