/**
 * Comments on a code or text file, drawn read only with its lines numbered
 * (`data-line`). A spot is a line, and a run of neighbouring lines is one
 * part: a click picks a line, a drag over the lines a run of them, and
 * several runs are one comment's parts. A run is saved as a Markdown block
 * is, by its lines and its text, since it is found again the same way: by
 * its text, its lines telling apart runs that read the same, as an edit
 * above it moves its lines and not its words. Words selected in it are a
 * `text` anchor that names their lines.
 */
import {
  MAX_LABEL, MAX_SELECTION, clip, laidOutBox, linesAnchor, linesText, nearestByQuote, normal, overlaps, parseLines, quoteFinder,
  squash, wordsAround,
} from '../spotKit';
import type { Found, SpotStrategy } from '../spotStrategy';

const LINE = '[data-line]';
/** The line's number, drawn inside it and never selected. */
const NUMBER = '.linenumber';
const TAB = '    ';

function lineNo(line: Element | null): number {
  return Number(line?.getAttribute('data-line')) || 0;
}

/** A line's own text, without its number or its newline. */
export function lineText(line: Element): string {
  let text = '';
  for (const child of line.childNodes) {
    if (!(child instanceof Element && child.matches(NUMBER))) text += child.textContent ?? '';
  }
  return text.replace(/\r?\n$/, '');
}

/** What a range holds without the line numbers inside it, which the reader never sees as selected. */
function textOf(range: Range): string {
  const copy = range.cloneContents();
  for (const n of copy.querySelectorAll(NUMBER)) n.remove();
  return copy.textContent ?? '';
}

function indentOf(text: string): number {
  return /^\s*/.exec(text.replace(/\t/g, TAB))![0].length;
}

/** Lines in document order cut into runs of neighbours, each a part of a comment. */
export function runsOf(lines: Element[]): Element[][] {
  const runs: Element[][] = [];
  for (const line of lines) {
    const run = runs[runs.length - 1];
    if (run && lineNo(line) === lineNo(run[run.length - 1]) + 1) run.push(line);
    else runs.push([line]);
  }
  return runs;
}

export function codeSpots(root: Element): SpotStrategy {
  const lines = () => [...root.querySelectorAll(LINE)];

  /** The line a range's end sits in; an end between lines (an element's offset) takes the node it is beside. */
  const lineAt = (node: Node, offset: number, end: boolean): Element | null => {
    let at: Node | null = node;
    if (node instanceof Element && !node.closest(LINE)) at = node.childNodes[end ? offset - 1 : offset] ?? null;
    const el = at instanceof Element ? at : at?.parentElement;
    const line = el?.closest(LINE) ?? null;
    return line && root.contains(line) ? line : null;
  };

  /**
   * A line is named by the scope it is in: the nearest line above it that is
   * indented less, a function's or a class's head in most languages. A line
   * at the outer edge, or a blank one, has no name.
   */
  const labelOf = (spot: Element): string | null => {
    const own = lineText(spot);
    if (!own.trim()) return null;
    const indent = indentOf(own);
    for (let prev = spot.previousElementSibling; prev && indent > 0; prev = prev.previousElementSibling) {
      if (!prev.matches(LINE)) continue;
      const text = lineText(prev);
      if (text.trim() && indentOf(text) < indent) return clip(squash(text), MAX_LABEL);
    }
    return null;
  };

  /**
   * The run a stored one is now: as many lines as it had, the one
   * `nearestByQuote` picks. A run with no words (blank lines) is found by
   * its lines alone.
   */
  // Every line's words end to end, and where each line starts in them: read
  // once a pass of lookups (`fresh`), as an area looks for each of its parts.
  let read: { all: Element[]; text: string; starts: number[] } | null = null;
  const lineIndex = () => {
    if (read) return read;
    const all = lines();
    const texts = all.map((l) => normal(lineText(l)));
    const starts = [0];
    for (const t of texts) starts.push(starts[starts.length - 1] + t.length);
    return (read = { all, text: texts.join(''), starts });
  };
  const quotes = quoteFinder(root, NUMBER);

  const findRun = (stored: { lines?: string; quote?: string }): Element[] | null => {
    const span = parseLines(stored.lines);
    if (!span) return null;
    const n = span[1] - span[0] + 1;
    const { all, text, starts } = lineIndex();
    if (!stored.quote) {
      const at = all.findIndex((l) => lineNo(l) === span[0]);
      return at === -1 ? null : all.slice(at, at + n);
    }
    const runs = Array.from({ length: Math.max(0, all.length - n + 1) }, (_, i) => i);
    const best = nearestByQuote(runs, { lines: stored.lines, quote: stored.quote }, (i) => lineNo(all[i]), (i) => [text, starts[i], starts[i + n]]);
    return best === null ? null : all.slice(best, best + n);
  };

  return {
    spots: {
      spotAt(target) {
        const line = target.closest(LINE);
        return line && root.contains(line) ? line : null;
      },

      spotsIn(box) {
        return lines().filter((line) => overlaps(line.getBoundingClientRect(), box));
      },

      labelOf,

      anchorOf(spots) {
        return linesAnchor(runsOf(spots), (run) => ({
          lines: linesText(lineNo(run[0]), lineNo(run[run.length - 1])),
          label: labelOf(run[0]),
          quote: squash(run.map(lineText).join('\n')),
        }));
      },

      rectsOf: laidOutBox,

      partsIn: runsOf,
    },

    words(range) {
      const quote = squash(textOf(range));
      const start = lineAt(range.startContainer, range.startOffset, false);
      let end = lineAt(range.endContainer, range.endOffset, true) ?? start;
      if (!quote || !start || !end) return null;
      // Words that stop at the very start of a line, past the newline before it, end on the line above.
      if (end !== start) {
        const tail = document.createRange();
        tail.setStart(end, 0);
        tail.setEnd(range.endContainer, range.endOffset);
        if (!textOf(tail).trim() && end.previousElementSibling?.matches(LINE)) end = end.previousElementSibling;
      }
      const label = labelOf(start);
      return {
        kind: 'text',
        quote: clip(quote, MAX_SELECTION),
        lines: linesText(lineNo(start), Math.max(lineNo(start), lineNo(end))),
        ...(label ? { label } : {}),
        ...wordsAround(range, start, end, textOf),
      };
    },

    fresh() {
      read = null;
      quotes.fresh();
    },

    find(anchor): Found | null {
      if (anchor.kind === 'block') {
        const run = findRun(anchor);
        return run?.length ? { parts: [run] } : null;
      }
      if (anchor.kind === 'area') {
        const parts = anchor.elements.map((e) => (e.lines ? findRun(e) : null));
        return parts.some(Boolean) ? { parts } : null;
      }
      if (anchor.kind === 'text') {
        const range = quotes.find(anchor, (node) => lineNo(node.parentElement?.closest(LINE) ?? null));
        return range ? { range } : null;
      }
      return null;
    },
  };
}
