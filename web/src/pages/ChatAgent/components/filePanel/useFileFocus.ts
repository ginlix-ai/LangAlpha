import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { RefObject } from 'react';
import {
  countHeadings,
  countLines,
  findHeadingIndex,
  headingIndexAbove,
  markdownLineTarget,
  type FileLocation,
  type MarkdownLineTarget,
} from '../../utils/fileLocation';

/** Which viewer is showing the selected file, as far as a location can reach into it. */
export type FocusViewer = 'code' | 'markdown' | 'pdf' | 'html' | 'excel' | 'other';

interface FileFocus {
  path: string;
  location: FileLocation;
  /** Bumped on every visit so a repeat click scrolls and plays the highlight again. */
  seq: number;
  /** A tab come back to the front putting the reader back where they were, not a new ask. */
  replay?: boolean;
}

export interface FocusChipState {
  kind: 'line' | 'page' | 'section';
  line?: number;
  lineEnd?: number;
  page?: number;
  anchor?: string;
  title?: string;
  missing: boolean;
  /** The line's text wasn't found, so the highlight marks the section holding it. */
  near?: boolean;
  /** Past the part of a long file that was loaded, so there is nothing to show. */
  beyond?: boolean;
  /** In the file, but nothing in the rendered markdown could be matched to it. */
  unplaced?: boolean;
}

interface UseFileFocusArgs {
  selectedFile: string | null;
  viewer: FocusViewer;
  /** The viewer has rendered the file: not loading, no error, not editing. */
  ready: boolean;
  editing: boolean;
  content: string | null;
  /** The read stopped short of the end, so a line past it may still exist. */
  truncated: boolean;
  pageCount: number | null;
  containerRef: RefObject<HTMLElement | null>;
}

const HEADING_SELECTOR = 'h1,h2,h3,h4,h5,h6';
const BLOCK_SELECTOR = `${HEADING_SELECTOR},p,li,td,th,pre,blockquote,dt,dd`;
const BLOCK_CLASS = 'file-focus-block';
const ENTER_CLASS = 'file-focus-enter';
const MAX_ANIMATED_LINES = 200;
// The code highlighter's grammar loads on first use and renders unnumbered until then.
const CODE_WAIT_MS = 4000;
const MARKDOWN_WAIT_MS = 1000;

const normalizeText = (s: string | null) => (s ?? '').replace(/\s+/g, ' ').trim();

interface MarkdownTarget {
  el: HTMLElement;
  title?: string;
  near?: boolean;
}

/** What a markdown location needs from the source, worked out once rather than per frame. */
interface MarkdownQuery {
  location: FileLocation;
  line: MarkdownLineTarget | null;
  headingCount: number;
  sectionAbove: number;
}

function inSection(el: HTMLElement, headings: HTMLElement[], section: number): boolean {
  const start = headings[section];
  const end = headings[section + 1];
  const afterStart = !start || start === el || !!(start.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_FOLLOWING);
  const beforeEnd = !end || !!(end.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_PRECEDING);
  return afterStart && beforeEnd;
}

function findMarkdownTarget(root: HTMLElement, query: MarkdownQuery): MarkdownTarget | null {
  const { location, line } = query;
  // GFM's footnote section carries a visually hidden heading the source never wrote.
  const headings = Array.from(root.querySelectorAll<HTMLElement>(HEADING_SELECTOR))
    .filter((h) => !h.closest('[data-footnotes], .footnotes'));
  const scoped = headings.length === query.headingCount;
  if (location.anchor) {
    const i = findHeadingIndex(headings.map((h) => h.textContent ?? ''), location.anchor);
    if (i >= 0) return { el: headings[i], title: normalizeText(headings[i].textContent) };
    // Raw HTML anchors keep their id behind rehype-sanitize's prefix.
    const id = `user-content-${location.anchor}`;
    const raw = Array.from(root.querySelectorAll<HTMLElement>('[id],[name]'))
      .find((el) => el.id === id || el.getAttribute('name') === id);
    return raw ? { el: raw } : null;
  }
  if (location.line) {
    // The rendered view has no line numbers. Find the innermost blocks holding
    // the line's text, keep those in its section when the headings line up with
    // the source, and take the one at the line's position among the repeats.
    if (line) {
      const matches = Array.from(root.querySelectorAll<HTMLElement>(BLOCK_SELECTOR)).filter((el) =>
        normalizeText(el.textContent).includes(line.probe)
        && !Array.from(el.querySelectorAll<HTMLElement>(BLOCK_SELECTOR)).some((inner) => normalizeText(inner.textContent).includes(line.probe))
        && (!scoped || inSection(el, headings, line.section)));
      if (matches.length) {
        const nth = scoped ? line.repeatsInSection : line.repeatsInDocument;
        const el = matches[Math.min(nth, matches.length - 1)];
        return { el: el.closest<HTMLElement>('tr') ?? el };
      }
    }
    const heading = scoped ? headings[query.sectionAbove] : undefined;
    if (heading) return { el: heading, near: true };
  }
  return null;
}

/** Put a target near the top of the panel with a little context above it. */
function scrollIntoPanel(container: HTMLElement, el: HTMLElement) {
  const offset = el.getBoundingClientRect().top - container.getBoundingClientRect().top;
  container.scrollTop += offset - Math.min(72, container.clientHeight / 4);
}

function playEnter(elements: HTMLElement[]) {
  for (const el of elements) {
    el.classList.remove(ENTER_CLASS);
    void el.offsetWidth; // restart the animation on a repeat visit
    el.classList.add(ENTER_CLASS);
    const done = () => el.classList.remove(ENTER_CLASS);
    el.addEventListener('animationend', done, { once: true });
    el.addEventListener('animationcancel', done, { once: true });
  }
}

/** Retry a DOM lookup across frames, since lazy viewers commit after the effect runs. */
function whenPresent<T>(find: () => T | null, onFound: (found: T) => void, onGiveUp: () => void, budgetMs: number): () => void {
  const deadline = performance.now() + budgetMs;
  let raf = 0;
  const tick = () => {
    const found = find();
    if (found) return onFound(found);
    if (performance.now() > deadline) return onGiveUp();
    raf = requestAnimationFrame(tick);
  };
  tick();
  return () => cancelAnimationFrame(raf);
}

/**
 * The spot inside the open file that a reference pointed at: scrolled to and
 * highlighted on arrival, kept until dismissed or another file opens.
 */
export function useFileFocus({
  selectedFile,
  viewer,
  ready,
  editing,
  content,
  truncated,
  pageCount,
  containerRef,
}: UseFileFocusArgs) {
  const [focus, setFocus] = useState<FileFocus | null>(null);
  // What the markdown lookup landed on, for the chip: a section's title, or a line found only near.
  const [landing, setLanding] = useState<{ seq: number; title?: string; found: boolean; near?: boolean } | null>(null);
  const seqRef = useRef(0);

  const focusAt = useCallback((path: string, location: FileLocation | null | undefined, replay = false) => {
    setFocus(location ? { path, location, seq: ++seqRef.current, ...(replay && { replay }) } : null);
  }, []);
  // A jump is the reader asking again, so it is never a replay.
  const jump = useCallback(() => setFocus((f) => f && { path: f.path, location: f.location, seq: ++seqRef.current }), []);
  const dismiss = useCallback(() => setFocus(null), []);

  useEffect(() => {
    if (focus && (focus.path !== selectedFile || editing)) setFocus(null);
  }, [focus, selectedFile, editing]);

  const active = focus && focus.path === selectedFile && ready ? focus : null;
  const loc = active?.location;
  const lineCount = useMemo(() => (content != null ? countLines(content) : 0), [content]);
  const result = landing?.seq === active?.seq ? landing : null;

  let chip: FocusChipState | null = null;
  let lineRange: [number, number] | null = null;
  if (active && loc) {
    if (loc.line && (viewer === 'code' || viewer === 'markdown')) {
      const beyond = loc.line > lineCount && truncated;
      const missing = loc.line > lineCount;
      const unplaced = !missing && viewer === 'markdown' && !!result && !result.found;
      // The chip names the range the highlight covers, so the end it advertises
      // is the last line there is. `#L19990-L20020` against a preview that stops
      // at 20000, or against a file that simply ends first, claimed twenty lines
      // nobody was shown while the highlight quietly stopped short. A range that
      // clamps back to its own start is one line, and says so.
      const end = loc.lineEnd != null && lineCount > 0 ? Math.min(loc.lineEnd, lineCount) : loc.lineEnd;
      const lineEnd = end != null && end > loc.line ? end : undefined;
      chip = { kind: 'line', line: loc.line, lineEnd, missing: missing || unplaced, beyond, unplaced, near: !missing && !!result?.near };
      if (!missing && viewer === 'code') lineRange = [loc.line, Math.min(loc.lineEnd ?? loc.line, lineCount)];
    } else if (loc.page && viewer === 'pdf') {
      chip = { kind: 'page', page: loc.page, missing: pageCount != null && loc.page > pageCount };
    } else if (loc.anchor && viewer === 'markdown') {
      chip = { kind: 'section', anchor: loc.anchor, title: result?.title, missing: result ? !result.found : false };
    }
  }

  const activeSeq = active?.seq ?? null;
  const rangeStart = lineRange?.[0] ?? null;
  const rangeEnd = lineRange?.[1] ?? null;
  const lineMissing = chip?.kind === 'line' && chip.missing;

  useEffect(() => {
    const container = containerRef.current;
    if (!active || !container) return;
    const location = active.location;

    if (viewer === 'code' && rangeStart != null && rangeEnd != null) {
      return whenPresent(
        () => container.querySelector<HTMLElement>(`[data-line="${rangeStart}"]`),
        (first) => {
          scrollIntoPanel(container, first);
          const last = Math.min(rangeEnd, rangeStart + MAX_ANIMATED_LINES);
          const lines = Array.from(container.querySelectorAll<HTMLElement>('[data-line]')).filter((el) => {
            const n = Number(el.dataset.line);
            return n >= rangeStart && n <= last;
          });
          playEnter(lines);
        },
        () => {},
        CODE_WAIT_MS,
      );
    }

    if (viewer === 'markdown' && (location.anchor || (location.line && !lineMissing))) {
      let target: HTMLElement | null = null;
      const seq = active.seq;
      const source = content ?? '';
      const query: MarkdownQuery = {
        location,
        line: location.line ? markdownLineTarget(source, location.line) : null,
        headingCount: countHeadings(source),
        sectionAbove: location.line ? headingIndexAbove(source, location.line) : -1,
      };
      const cancel = whenPresent(
        () => findMarkdownTarget(container, query),
        (found) => {
          target = found.el;
          found.el.classList.add(BLOCK_CLASS);
          scrollIntoPanel(container, found.el);
          playEnter([found.el]);
          setLanding({ seq, title: found.title, found: true, near: found.near });
        },
        () => setLanding({ seq, found: false }),
        MARKDOWN_WAIT_MS,
      );
      return () => {
        cancel();
        target?.classList.remove(BLOCK_CLASS, ENTER_CLASS);
      };
    }
  }, [activeSeq, viewer, rangeStart, rangeEnd, lineMissing, content]); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    focusAt,
    jump,
    dismiss,
    chip,
    lineRange,
    focusPage: loc?.page && viewer === 'pdf' ? loc.page : null,
    htmlAnchor: loc?.anchor && viewer === 'html' ? loc.anchor : null,
    // No chip beside it: whether `Model!B4:D9` names a sheet that exists is a
    // question only the spreadsheet can answer, and a chip that cannot say
    // `missing` would claim every reference landed.
    // `#L42` parses as a line, and to a spreadsheet that is cell L42: only a
    // single line reads that way, since `L4-L9` names rows, not a range.
    focusCell: viewer !== 'excel' ? null
      : loc?.cell ? loc.cell
      : loc?.line && !loc.lineEnd ? `L${loc.line}`
      : null,
    seq: activeSeq,
    replayed: !!active?.replay,
  };
}
