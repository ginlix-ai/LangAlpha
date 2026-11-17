/**
 * Comments on a sheet's cells, in a workbook or a CSV. A spot is a range of
 * one sheet's cells: a click picks a cell (a merge whole), a drag the cells
 * it covers. A comment holds one range or several, each saved by its A1 name
 * and the values it showed, and found again by that name on its sheet: in a
 * grid the address is what stays put, while the values are what changed.
 *
 * A range is not one element, so each range a click, a drag or a comment
 * names stands as one of its own: an empty element in `holder`, made once per
 * range and kept in reading order, which the page holds, orders and drops as
 * it does any spot. How ranges nest is the strategy's (`holds`), as the DOM
 * cannot say it. A token is made on demand and they all go when the sheet
 * shown changes, so one never outlives what it names.
 */
import type { CellsAnchor } from '@/types/artifacts';
import { formatRange, parseRange, type CellBox } from '@/pages/ChatAgent/utils/a1';
import { MAX_CELL_RANGES } from '../../html/pageProtocol';
import { MAX_QUOTE, clip, clipWords, squash } from '../spotKit';
import type { Edges, Found, SpotStrategy } from '../spotStrategy';

/** A range of one sheet's cells; `sheet` is null in a CSV, which has one sheet and no name. */
interface CellSpan {
  sheet: string | null;
  box: CellBox;
}

/** The sheet a viewer draws, as the strategy reads it: rows and columns 1-based, as A1 counts them. */
export interface SheetView {
  /** The sheet shown, null for a CSV. */
  sheet: string | null;
  /** The workbook's sheets by name, none for a CSV. */
  sheets: readonly string[];
  /** How many rows and columns are drawn. */
  rows: number;
  cols: number;
  /** What a cell shows, empty for an empty one. */
  text(row: number, col: number): string;
  /** Words rather than a number: what can name a row or a column. */
  isText(row: number, col: number): boolean;
}

export interface SheetSource {
  /** The sheet drawn now, null while there is none. */
  view(): SheetView | null;
  /** The tab another sheet is shown by: where its comments wait while it is not drawn. */
  tabOf(sheet: string): Element | null;
  /** Draw that sheet with the range in view, for a comment on it scrolled to from the list. */
  reveal(sheet: string, box: CellBox): void;
}

/** A row or column name past this is cut: it labels a range, it does not quote it. */
const MAX_NAME = 32;

function sameSheet(a: string | null | undefined, b: string | null | undefined): boolean {
  return (a ?? null) === (b ?? null);
}

function covers(outer: CellBox, inner: CellBox): boolean {
  return outer.top <= inner.top && outer.left <= inner.left && outer.bottom >= inner.bottom && outer.right >= inner.right;
}

/**
 * What a range showed, row by row, comma-joined: empty cells are left out,
 * and a large range stops where the anchor would cut it anyway.
 */
export function rangeValues(view: SheetView, box: CellBox, limit = MAX_QUOTE): string {
  const out: string[] = [];
  let length = -2;
  // Past the limit by one value is enough for the cut to show; a blank
  // stretch adds nothing, so the cells read are capped too.
  let read = 0;
  const most = limit * 20;
  for (let r = box.top; r <= Math.min(box.bottom, view.rows) && length <= limit && read < most; r++) {
    for (let c = box.left; c <= Math.min(box.right, view.cols) && length <= limit && read < most; c++, read++) {
      const text = squash(view.text(r, c));
      if (!text) continue;
      out.push(text);
      length += text.length + 2;
    }
  }
  return clipWords(out.join(', '), limit);
}

/**
 * A short name for what a range holds, where the sheet makes one obvious: a
 * column by its header (the first row's words above it), a row by the first
 * words left of it. A single cell has both; a block of rows and columns
 * none, as it has no one header.
 */
export function rangeLabel(view: SheetView, box: CellBox): string | null {
  const name = (row: number, col: number) => (view.isText(row, col) ? clip(squash(view.text(row, col)), MAX_NAME) : '');
  const column = box.left === box.right && box.top > 1 ? name(1, box.left) : '';
  let row = '';
  if (box.top === box.bottom) {
    for (let c = 1; c < box.left && !row; c++) row = name(box.top, c);
  }
  return [row, column].filter(Boolean).join(', ') || null;
}

/** Where a row or a column is drawn: a row by its number cell, a column by its header. */
function rowEdge(root: Element, row: number): DOMRect | null {
  const cell = root.querySelector(`[data-r="${row}"]:not([data-c])`);
  return cell?.closest('tr')?.getBoundingClientRect() ?? null;
}

function colEdge(root: Element, col: number): DOMRect | null {
  return root.querySelector(`thead [data-c="${col}"]`)?.getBoundingClientRect() ?? null;
}

function overlap(lo: number, hi: number, box: { lo: number; hi: number }): boolean {
  return lo < box.hi && hi > box.lo;
}

/** The first and last index drawn across `box`, among `edges` keyed by index. */
function spanOf(edges: Iterable<[number, DOMRect]>, lo: number, hi: number, axis: 'y' | 'x'): [number, number] | null {
  let first = 0;
  let last = 0;
  for (const [at, r] of edges) {
    const touched = axis === 'y' ? overlap(r.top, r.bottom, { lo, hi }) : overlap(r.left, r.right, { lo, hi });
    if (!touched) continue;
    if (!first || at < first) first = at;
    if (at > last) last = at;
  }
  return first ? [first, last] : null;
}

function* numbered(cells: NodeListOf<Element>, key: 'r' | 'c'): Iterable<[number, DOMRect]> {
  for (const el of cells) {
    const n = Number((el as HTMLElement).dataset[key]);
    if (n > 0) yield [n, (key === 'r' ? (el.closest('tr') ?? el) : el).getBoundingClientRect()];
  }
}

function before(a: CellBox, b: CellBox): boolean {
  return a.top !== b.top ? a.top < b.top : a.left !== b.left ? a.left < b.left : a.bottom !== b.bottom ? a.bottom < b.bottom : a.right < b.right;
}

/**
 * `root` holds the grid: a table whose cells carry `data-r` and `data-c`,
 * each row a number cell with `data-r` alone, and a header row whose column
 * heads carry `data-c`. `holder` is an element outside it, hidden, connected,
 * which holds the ranges' tokens.
 */
export function cellSpots(root: Element, holder: Element, source: SheetSource): SpotStrategy {
  const spans = new WeakMap<Element, CellSpan>();
  /** The tokens in `holder`'s order, with their ranges, so where one is or goes is found by halves. */
  let ordered: { token: Element; box: CellBox }[] = [];
  let tokensOf: string | null | undefined;

  /** The token a range of the sheet shown stands as. */
  const tokenFor = (view: SheetView, box: CellBox): Element => {
    if (tokensOf !== view.sheet) {
      holder.replaceChildren();
      ordered = [];
      tokensOf = view.sheet;
    }
    let lo = 0;
    let hi = ordered.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (before(ordered[mid].box, box)) lo = mid + 1;
      else hi = mid;
    }
    const next = ordered[lo];
    if (next && !before(box, next.box)) return next.token;
    const token = document.createElement('i');
    token.dataset.range = formatRange(box);
    spans.set(token, { sheet: view.sheet, box });
    holder.insertBefore(token, next?.token ?? null);
    ordered.splice(lo, 0, { token, box });
    return token;
  };

  /** The range a token stands for, while its sheet is the one shown. */
  const shownSpan = (token: Element): { span: CellSpan; view: SheetView } | null => {
    const span = spans.get(token);
    const view = source.view();
    return span && view && sameSheet(view.sheet, span.sheet) ? { span, view } : null;
  };

  const cellToken = (cell: Element): Element | null => {
    const el = cell as HTMLTableCellElement;
    const row = Number(el.dataset.r);
    const col = Number(el.dataset.c);
    const view = source.view();
    if (!view || !(row > 0) || !(col > 0)) return null;
    // A merge is picked whole, as the grid draws it.
    return tokenFor(view, { top: row, left: col, bottom: row + Math.max(1, el.rowSpan || 1) - 1, right: col + Math.max(1, el.colSpan || 1) - 1 });
  };

  /** A stored range on the sheet as drawn now, cut to what is drawn; null once none of it is. */
  const tokenNow = (view: SheetView, range: string): Element | null => {
    const box = parseRange(range);
    if (!box || box.top > view.rows || box.left > view.cols) return null;
    return tokenFor(view, { ...box, bottom: Math.min(box.bottom, view.rows), right: Math.min(box.right, view.cols) });
  };

  // Words in a cell are the cell's: a sheet is commented on by its cells.
  return {
    find(anchor): Found | null {
      if (anchor.kind !== 'cells') return null;
      const view = source.view();
      if (!view) return null;
      // No sheet named is the one sheet a CSV has.
      if (anchor.sheet && view.sheet !== null && anchor.sheet !== view.sheet) {
        const { sheet } = anchor;
        const tab = view.sheets.includes(sheet) ? source.tabOf(sheet) : null;
        const box = parseRange(anchor.ranges[0]?.range ?? '');
        return tab && { parked: tab, ...(box ? { show: () => source.reveal(sheet, box) } : {}) };
      }
      const parts = anchor.ranges.map((r) => {
        const token = tokenNow(view, r.range);
        return token && [token];
      });
      return parts.some(Boolean) ? { parts } : null;
    },

    spots: {
      spotAt(target) {
        const cell = target.closest('[data-r][data-c]');
        return cell && root.contains(cell) ? cellToken(cell) : null;
      },

      // One range: every row and column the box crosses, as a drag over a sheet selects.
      spotsIn(box: Edges) {
        const view = source.view();
        if (!view) return [];
        const rows = spanOf(numbered(root.querySelectorAll('tr > [data-r]:not([data-c])'), 'r'), box.top, box.bottom, 'y');
        const cols = spanOf(numbered(root.querySelectorAll('thead [data-c]'), 'c'), box.left, box.right, 'x');
        if (!rows || !cols) return [];
        return [tokenFor(view, { top: rows[0], bottom: rows[1], left: cols[0], right: cols[1] })];
      },

      labelOf(token) {
        const shown = shownSpan(token);
        if (!shown) return null;
        const name = formatRange(shown.span.box);
        const label = rangeLabel(shown.view, shown.span.box);
        return label ? `${name} · ${label}` : name;
      },

      anchorOf(spots): CellsAnchor | null {
        const first = spots[0] ? shownSpan(spots[0]) : null;
        if (!first) return null;
        const ranges = spots
          .flatMap((token) => {
            const span = spans.get(token);
            return span && sameSheet(span.sheet, first.span.sheet) ? [span.box] : [];
          })
          .slice(0, MAX_CELL_RANGES)
          .map((box) => {
            const values = rangeValues(first.view, box);
            return { range: formatRange(box), ...(values ? { values } : {}) };
          });
        return { kind: 'cells', ...(first.span.sheet ? { sheet: first.span.sheet } : {}), ranges };
      },

      rectsOf(token) {
        const shown = shownSpan(token);
        if (!shown) return [];
        const { box } = shown.span;
        const top = rowEdge(root, box.top);
        const bottom = rowEdge(root, box.bottom);
        const left = colEdge(root, box.left);
        const right = colEdge(root, box.right);
        if (!top || !bottom || !left || !right) return [];
        return [new DOMRect(left.left, top.top, right.right - left.left, bottom.bottom - top.top)];
      },

      holds(part, spot) {
        const a = spans.get(part);
        const b = spans.get(spot);
        return part === spot || (!!a && !!b && sameSheet(a.sheet, b.sheet) && covers(a.box, b.box));
      },
    },
  };
}
