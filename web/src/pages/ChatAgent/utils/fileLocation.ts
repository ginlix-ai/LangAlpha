/**
 * Where inside a file a reference points: `report.md#valuation`,
 * `model.py#L40-L55`, `model.py:42`, `filing.pdf#page=12`,
 * `model.xlsx#Model!B4:D9`.
 */
import { parseLocator } from './a1';

export interface FileLocation {
  line?: number;
  lineEnd?: number;
  page?: number;
  anchor?: string;
  /**
   * A spreadsheet cell or range, sheet-qualified when the reference named a
   * sheet. Additive: a fragment that reads as a cell keeps its `anchor` too, so
   * a heading called `B7` in a markdown file resolves exactly as it always did.
   */
  cell?: string;
}

/**
 * `rooted` says the reference named a starting point of its own, a workspace
 * qualifier or the sandbox root, so it is not written relative to whatever file
 * quotes it. Only the file panel reads it, to decide whether joining against
 * the open file's directory is a reading the reference invited. Optional, so
 * every handler that only opens a path stays assignable.
 */
export type OpenFileHandler = (
  path: string,
  workspaceId?: string,
  location?: FileLocation,
  /** `rooted`: the reference named where it starts, so it is not joined onto
   *  the viewing file's directory. `pin`: open in a tab of its own rather than
   *  the preview slot. */
  opts?: { rooted?: boolean; pin?: boolean },
) => void;

// Uppercase only, as GitHub writes it: `#l2` is a heading slug ("L2"), not line 2.
const LINE_FRAGMENT_RE = /^L(\d+)(?:-L?(\d+))?$/;
const PAGE_FRAGMENT_RE = /(?:^|&)page=(\d+)(?:&|$)/i;
// A1 with an optional sheet qualifier (`B7`, `Model!B4:D9`, `'My Sheet'!B:D`),
// read by the shared grammar. Uppercase columns only, for the same reason `#l2`
// stays a heading slug: a lowercase run is prose far more often than it is a
// column. `L42` never reaches this test anyway; the line fragment above claims
// it first.
function isCellFragment(frag: string): boolean {
  const range = frag.slice(frag.lastIndexOf('!') + 1);
  return !/[a-z]/.test(range) && parseLocator(frag) !== null;
}

// `name.ext:42`, `name.ext:40-55`, `name.ext:42:7` (the column is ignored).
// The extension must hold a letter, so `localhost:8000` and `127.0.0.1:8000`
// stay ports. A host with a lettered TLD (`example.com:8080`) still matches,
// so only call this on a destination already known not to be a URL.
const LINE_SUFFIX_RE = /(\.(?=[a-z0-9]{0,7}[a-z])[a-z0-9]{1,8}):(\d+)(?:-(\d+)|:\d+)?$/i;

function lineRange(start: string, end?: string): FileLocation | null {
  const a = Number(start);
  const b = end ? Number(end) : a;
  if (!(a >= 1)) return null;
  const lo = Math.min(a, b >= 1 ? b : a);
  const hi = Math.max(a, b >= 1 ? b : a);
  return hi > lo ? { line: lo, lineEnd: hi } : { line: lo };
}

/** Read a fragment (without `#`) as a line range, a PDF page, or a heading anchor. */
export function parseFragment(fragment: string): FileLocation | null {
  let frag = fragment;
  try {
    frag = decodeURIComponent(fragment);
  } catch {
    // keep the raw text
  }
  frag = frag.trim();
  if (!frag) return null;
  const line = frag.match(LINE_FRAGMENT_RE);
  if (line) return lineRange(line[1], line[2]);
  const page = frag.match(PAGE_FRAGMENT_RE);
  if (page) return Number(page[1]) >= 1 ? { page: Number(page[1]) } : null;
  return isCellFragment(frag) ? { cell: frag, anchor: frag } : { anchor: frag };
}

/** Split a link destination into the file part and the location it points at. */
export function splitFileLocation(href: string): { path: string; location: FileLocation | null } {
  const hash = href.indexOf('#');
  if (hash >= 0) {
    return { path: href.slice(0, hash), location: parseFragment(href.slice(hash + 1)) };
  }
  const suffix = href.match(LINE_SUFFIX_RE);
  if (suffix && suffix.index !== undefined) {
    return {
      path: href.slice(0, suffix.index + suffix[1].length),
      location: lineRange(suffix[2], suffix[3]),
    };
  }
  return { path: href, location: null };
}

export function hasLineSuffix(href: string): boolean {
  return LINE_SUFFIX_RE.test(href);
}

/** GitHub's heading slug: lowercase, punctuation dropped, each space a hyphen. */
export function slugifyHeading(text: string): string {
  return text.trim().toLowerCase().replace(/[^\p{L}\p{M}\p{N}\p{Pc} -]/gu, '').replace(/ /g, '-');
}

/** Slugs for headings in document order, with GitHub's `-1`, `-2` suffixes on repeats. */
export function headingSlugs(texts: string[]): string[] {
  const seen = new Map<string, number>();
  return texts.map((text) => {
    const base = slugifyHeading(text);
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return n ? `${base}-${n}` : base;
  });
}

/** Index of the heading an anchor names, trying it as written and as a slug. */
export function findHeadingIndex(texts: string[], anchor: string): number {
  const wanted = anchor.replace(/^user-content-/, '');
  const slugs = headingSlugs(texts);
  const exact = slugs.indexOf(wanted.toLowerCase());
  if (exact >= 0) return exact;
  const slug = slugs.indexOf(slugifyHeading(wanted));
  if (slug >= 0) return slug;
  // "Revenue & Margin" slugs to `revenue--margin`; a hand-written anchor has one hyphen.
  const collapse = (s: string) => s.replace(/-+/g, '-');
  return slugs.map(collapse).indexOf(collapse(slugifyHeading(wanted)));
}

export function countLines(content: string): number {
  if (!content) return 0;
  const n = content.split('\n').length;
  return content.endsWith('\n') ? n - 1 : n;
}

// A fence run, with the character and length a closer has to match, and
// whatever follows on the line.
const FENCE_RE = /^\s{0,3}((`|~)\2{2,})([^\n]*)$/;
const ATX_HEADING_RE = /^\s{0,3}#{1,6}\s+\S/;
const BLOCK_START_RE = /^\s{0,3}(?:[-*+]\s|\d+[.)]\s|\||>)/;
const MARKUP_SPLIT_RE = /[*_`~$<>[\]()|\\]+/;
const MARKUP_GLOBAL_RE = /[*_`~$<>[\]()|\\]+/g;

interface SourceLine {
  text: string;
  index: number;
  /** Inside a fenced code block (the fence lines themselves are not). */
  inFence: boolean;
  isFence: boolean;
  heading: boolean;
  /** Begins a block of its own in the rendered view, rather than continuing the one above. */
  startsBlock: boolean;
}

/**
 * Source lines as the renderer reads them, with a leading front-matter block
 * left out (Markdown strips it), so a YAML `# comment` never counts as a heading.
 */
function scanLines(source: string): (SourceLine | null)[] {
  const lines = source.split('\n');
  let body = 0;
  if (lines[0]?.replace(/\r$/, '') === '---') {
    const close = lines.findIndex((l, i) => i > 0 && l.startsWith('---'));
    if (close > 0) body = close + 1;
  }
  // The opener's own run, because CommonMark closes a block only on a run of
  // the same character, at least as long, carrying no info string. Toggling on
  // any fence-like line ended a ```` block at the first ``` example inside it,
  // and the example's headings then counted as rendered ones, so a `#L`
  // reference landed in the wrong section or reported the line missing. The
  // secretary's `_CODE_RE` reads the same rule on the other side of the wire.
  let fence: string | null = null;
  return lines.map((text, index) => {
    if (index < body) return null;
    const run = FENCE_RE.exec(text);
    const closes = !!run && fence !== null
      && run[2] === fence[0] && run[1].length >= fence.length && !run[3].trim();
    const isFence = !!run && (fence === null || closes);
    const heading = !fence && !isFence && ATX_HEADING_RE.test(text);
    const startsBlock = isFence || heading || !text.trim() || (!fence && BLOCK_START_RE.test(text));
    const line = { text, index, inFence: !!fence && !isFence, isFence, heading, startsBlock };
    if (isFence) fence = fence === null ? run![1] : null;
    return line;
  });
}

/** The markdown source line at `line` (or the next non-blank one). */
function sourceLineAt(scan: (SourceLine | null)[], line: number): SourceLine | null {
  for (let i = Math.max(0, line - 1); i < Math.min(scan.length, line + 3); i++) {
    const at = scan[i];
    if (at && at.text.trim() && !at.isFence) return at;
  }
  return null;
}

/**
 * A run of plain text from a markdown source line that should appear verbatim
 * in the rendered block: link destinations and markup are dropped and the
 * longest remaining piece is kept, since inline formatting splits the rest.
 */
export function markdownLineProbe(source: string, line: number): string | null {
  const at = sourceLineAt(scanLines(source), line);
  return at ? probeOf(at) : null;
}

function probeOf(at: SourceLine): string | null {
  if (at.inFence) {
    const code = at.text.trim().replace(/\s+/g, ' ');
    return code.length >= 3 ? code : null;
  }
  const pieces = plainText(at.text).split(MARKUP_SPLIT_RE).map((p) => p.replace(/\s+/g, ' ').trim());
  const longest = pieces.reduce((best, p) => (p.length > best.length ? p : best), '');
  return longest.length >= 3 ? longest : null;
}

function plainText(raw: string): string {
  return raw
    .trim()
    .replace(/^#{1,6}\s+/, '')
    .replace(/^(?:>\s*)+/, '')
    .replace(/^(?:[-*+]|\d+[.)])\s+(?:\[[ xX]\]\s+)?/, '')
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1');
}

/**
 * What to look for in the rendered view for a markdown source line: its probe
 * text, and how many earlier blocks also contain that text, counted from the
 * start of its section and of the document. The rendered blocks carry no line
 * numbers, so a repeated sentence is told apart by its position among repeats.
 */
export interface MarkdownLineTarget {
  probe: string;
  /** Index among ATX headings of the section holding the line, or -1 before the first. */
  section: number;
  repeatsInSection: number;
  repeatsInDocument: number;
}

export function markdownLineTarget(source: string, line: number): MarkdownLineTarget | null {
  const scan = scanLines(source);
  const at = sourceLineAt(scan, line);
  const probe = at ? probeOf(at) : null;
  if (!at || !probe) return null;
  let section = -1;
  let inSection = 0;
  let inDocument = 0;
  // A paragraph wrapped over several lines is one rendered block, so it counts once.
  let blockCounted = false;
  for (const l of scan.slice(0, at.index + 1)) {
    if (!l) continue;
    if (l.startsBlock) blockCounted = false;
    if (l.heading) {
      section += 1;
      inSection = 0;
    }
    if (l === at) break;
    const text = l.inFence ? l.text : plainText(l.text).replace(MARKUP_GLOBAL_RE, '');
    if (!blockCounted && !l.isFence && text.replace(/\s+/g, ' ').includes(probe)) {
      inSection += 1;
      inDocument += 1;
      blockCounted = true;
    }
  }
  const sameBlock = blockCounted ? 1 : 0;
  return {
    probe,
    section,
    repeatsInSection: Math.max(0, inSection - sameBlock),
    repeatsInDocument: Math.max(0, inDocument - sameBlock),
  };
}

export function countHeadings(source: string): number {
  return headingIndexAbove(source, Infinity) + 1;
}

/** Position, among the document's ATX headings, of the last one at or above `line`. */
export function headingIndexAbove(source: string, line: number): number {
  return scanLines(source).slice(0, Math.max(0, line)).filter((l) => l?.heading).length - 1;
}
