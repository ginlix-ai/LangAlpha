/**
 * Fades streamed text in as the typewriter reveals it.
 *
 * What is fresh is tracked in the markdown source, not on screen. The parser
 * re-shapes the tail as it grows (a `**` closing into bold, pipes turning into
 * a table) and the live block remounts on every newline, so the span made for
 * a word is often thrown away and rebuilt a tick later. Each reveal is a mark
 * over the source range it added; a rehype pass wraps the text a mark covers
 * in a span naming it, and the fade is anchored to the mark's own start time
 * on the document timeline, so a rebuilt span resumes its fade instead of
 * starting it over.
 */
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { CATCH_UP_CHARS, LIVE_TAIL_CHARS, TICK_MS } from '@/components/ui/animated-text';
import { cssEase, DURATION, EASE_REVEAL } from '@/lib/motion';
import { isPageUnseen } from '@/lib/pageVisibility';
import { prefersReducedMotion } from '@/lib/reducedMotion';

export interface RevealMark {
  from: number;
  to: number;
  id: number;
}

export interface RevealState {
  /** The source the marks are offsets into. */
  text: string;
  /** The source was streaming when it was last read. */
  live: boolean;
  marks: readonly RevealMark[];
  seq: number;
}

const FADE_MS = DURATION.reveal * 1000;
const FADE_KEYFRAMES: Keyframe[] = [{ opacity: 0 }, { opacity: 1 }];
// Backwards fill: a fade anchored a few ms ahead of the frame clock must hold
// its first keyframe until then, not show the word at full strength.
const FADE_TIMING: KeyframeAnimationOptions = {
  duration: FADE_MS, easing: cssEase(EASE_REVEAL), fill: 'backwards',
};
/** How long after the stream ends its marks are dropped: past the last fade. */
export const REVEAL_SETTLE_MS = FADE_MS + 100;

const NO_MARKS: readonly RevealMark[] = [];
// Marks are dropped in batches, down to KEEP_MARKS once there are MAX_MARKS.
// KEEP_MARKS outlasts the fade at the typewriter's fastest cadence, with a few
// ticks to spare: a mark dropped while its text still fades would pop. Dropping
// one per tick instead unwrapped the oldest span in a paragraph on every tick,
// which shifts the keys of every span after it and rebuilds them all.
export const KEEP_MARKS = Math.ceil(FADE_MS / TICK_MS) + 4;
export const MAX_MARKS = KEEP_MARKS * 3;
// A reply that mounts mid-stream (a thread switch, a remount) fades in only
// as much of its tail as the typewriter leaves to type of a replay: what is
// above it was already read or arrived at once.
export const MOUNT_TAIL = LIVE_TAIL_CHARS;

export function initialReveal(text: string, live: boolean): RevealState {
  const from = Math.max(0, text.length - MOUNT_TAIL);
  const marks = live && text.length > from ? [{ from, to: text.length, id: 1 }] : NO_MARKS;
  return { text, live, marks, seq: 1 };
}

/**
 * Tracks while streaming, plus the one read that ends it: the typewriter's
 * last tick and `streaming` turning off arrive in the same render. `hold` is
 * the newest reveal a selection in the reply could be anchored in. Marks up to
 * it stay, since dropping one unwraps its spans and loses the selection; the
 * ones after it are still pruned, so a selection left standing through a long
 * reply does not keep a mark for every tick.
 *
 * A source that is not a plain extension was re-shaped at its tail by a pass
 * that rewrites prose (a currency escape, a padded table row). Its marks keep
 * their positions, so the error is bounded by the length of that rewrite and
 * mostly lands on text already mid-fade.
 */
export function nextReveal(prev: RevealState, text: string, streaming: boolean, hold: number | null = null): RevealState {
  if (!streaming && !prev.live) {
    return { text, live: false, marks: text === prev.text ? prev.marks : NO_MARKS, seq: prev.seq };
  }
  let { marks, seq } = prev;
  if (text !== prev.text) {
    marks = marks.flatMap((m) => (m.from >= text.length ? [] : [m.to > text.length ? { ...m, to: text.length } : m]));
    if (text.length > prev.text.length) {
      seq += 1;
      marks = [...marks, { from: freshFrom(prev.text.length, text), to: text.length, id: seq }];
      const held = hold === null ? 0 : marks.filter((m) => m.id <= hold).length;
      if (marks.length - held > MAX_MARKS) marks = [...marks.slice(0, held), ...marks.slice(-KEEP_MARKS)];
    }
  }
  return { text, live: streaming, marks, seq };
}

/**
 * Where an update's text starts fading. One past CATCH_UP_CHARS is not typed
 * (a reconnect replay): it landed at once, and fading all of it would start a
 * fade in every paragraph of the backlog. It fades from the paragraph its last
 * CATCH_UP_CHARS begin in, so a long paragraph released whole (paragraph mode)
 * still fades whole.
 */
function freshFrom(prevLength: number, text: string): number {
  const cap = text.length - CATCH_UP_CHARS;
  if (cap <= prevLength) return prevLength;
  const paragraph = text.lastIndexOf('\n\n', cap);
  return Math.max(prevLength, paragraph < 0 ? 0 : paragraph + 2);
}

/**
 * The marks over each block, rebased to it, as a string so a block with
 * nothing fresh keeps its memo: '' for those, which is every block above the
 * last one or two.
 */
export function blockFreshKeys(blocks: readonly string[], marks: readonly RevealMark[]): string[] {
  if (!marks.length) return blocks.map(() => '');
  // Marks run in source order, so no block ending before the first holds one.
  const first = marks[0].from;
  let end = 0;
  return blocks.map((block) => {
    const start = end;
    end += block.length;
    if (end <= first) return '';
    const parts: string[] = [];
    for (const m of marks) {
      if (m.to <= start || m.from >= end) continue;
      parts.push(`${Math.max(m.from, start) - start}:${Math.min(m.to, end) - start}:${m.id}`);
    }
    return parts.join(' ');
  });
}

export function parseFreshKey(key: string): RevealMark[] {
  if (!key) return [];
  return key.split(' ').map((part) => {
    const [from, to, id] = part.split(':').map(Number);
    return { from, to, id };
  });
}

// --- rehype pass ---

interface Point { offset?: number }
interface Position { start?: Point; end?: Point }
interface HastText { type: 'text'; value: string; position?: Position }
interface HastElement {
  type: 'element';
  tagName: string;
  properties: Record<string, unknown>;
  children: HastNode[];
  position?: Position;
}
type HastNode = HastText | HastElement | { type: string; children?: HastNode[]; position?: Position };
interface HastParent { children: HastNode[]; position?: Position }

// Never descended into: a fence types its own line (CodeBlock), and the rest
// hold no prose. KaTeX's visible half, beside its MathML, carries no source
// positions, so math is never marked either and pops in whole.
const OPAQUE = new Set(['pre', 'script', 'style', 'svg', 'math', 'textarea']);
// Faded whole and in place, from the reveal that began them: their components
// read their children as a string, or have none, and pass the attribute on to
// their own element. A wrapper would remount them when their mark goes, which
// drops a focused citation's focus and its hover card.
const UNITS = new Set(['code', 'cite-bubble']);

// A span never starts inside a word, or inside a run of symbols: either side
// of the cut would be shaped apart, which breaks a ligature (fi, an arrow
// drawn from ->) while the spans stand and restores it as they go, and under
// a link's break-all Chrome drew "filing" as "fiil" at the wrap. Where a word
// meets punctuation it may, so a URL or a long number still fades as it
// types. Ideographs and kana break anywhere, so a run of them still splits.
const WORD = /[\p{L}\p{M}\p{N}]/u;
const SYMBOL = /[\p{P}\p{S}]/u;
const BREAKS_ANYWHERE = /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]/u;
const glued = (a: string, b: string): boolean =>
  !BREAKS_ANYWHERE.test(a) && !BREAKS_ANYWHERE.test(b)
  && ((WORD.test(a) && WORD.test(b)) || (SYMBOL.test(a) && SYMBOL.test(b)));
// Nor inside one drawn character, which the typewriter can stop in: an emoji
// and its variation selector, skin tone or joined parts, a flag's two
// letters, a surrogate pair. Cut apart, each half is drawn on its own.
const GRAPHEMES = new Intl.Segmenter(undefined, { granularity: 'grapheme' });

const freshSpan = (id: number, children: HastNode[]): HastElement => ({
  type: 'element', tagName: 'span', properties: { dataFresh: String(id) }, children,
});

// How far ahead a character's source is looked for: past an escape, an
// entity or a trimmed indent. A longer fold reads as a miss.
const MAX_FOLD = 12;

/**
 * Source offset of each character of a text node. The value is shorter than
 * its source span where an escape, an entity or a trimmed indent was folded
 * in, so each character is matched forward to its source character. A miss
 * keeps the earlier offset: a word read as older pops in, while one read as
 * newer would fade in a second time.
 */
function sourceOffsets(value: string, source: string, start: number, end: number): number[] {
  const out: number[] = new Array(value.length);
  let p = start;
  for (let j = 0; j < value.length; j++) {
    let q = p;
    while (q < end && q - p < MAX_FOLD && source[q] !== value[j]) q++;
    if (q < end && source[q] === value[j]) {
      out[j] = q;
      p = q + 1;
    } else {
      // Held, not advanced: a character with no source of its own (the newline
      // parse5 merges in after a hard break) would push the rest one newer.
      out[j] = Math.min(p, end - 1);
    }
  }
  return out;
}

/**
 * The id of the range holding each offset, for offsets that never go back.
 * Marks are sorted and disjoint, so it walks them once instead of searching
 * them all for every character.
 */
function idCursor(ranges: readonly RevealMark[]): (offset: number) => number {
  let k = 0;
  return (offset) => {
    while (k < ranges.length && ranges[k].to <= offset) k++;
    return k < ranges.length && ranges[k].from <= offset ? ranges[k].id : 0;
  };
}

/**
 * Where a parent's child lies in the source. A text node parse5 merged out of
 * a synthetic one (after a hard break, in a task item, before a nested list)
 * has no position of its own; it lies between its neighbours, and matching it
 * forward from the earlier bound can only read it as older.
 */
function sourceSpan(parent: HastParent, i: number): { start?: number; end?: number; exact: boolean } {
  const own = parent.children[i].position;
  if (own?.start?.offset !== undefined && own.end?.offset !== undefined) {
    return { start: own.start.offset, end: own.end.offset, exact: true };
  }
  return {
    start: parent.children[i - 1]?.position?.end?.offset ?? parent.position?.start?.offset,
    end: parent.children[i + 1]?.position?.start?.offset ?? parent.position?.end?.offset,
    exact: false,
  };
}

/** Wraps the text a block's fresh key (`blockFreshKeys`) covers. */
export function rehypeFreshText(key: string) {
  const ranges = parseFreshKey(key);

  return (tree: HastParent, file: { value?: unknown }) => {
    const source = typeof file.value === 'string' ? file.value : '';

    const splitText = (node: HastText, { start, end, exact }: ReturnType<typeof sourceSpan>): HastNode[] | null => {
      if (start === undefined || end === undefined || !node.value) return null;
      if (!ranges.some((r) => r.from < end && r.to > start)) return null;
      const offsets = exact && node.value.length === end - start ? null : sourceOffsets(node.value, source, start, end);
      const idAt = idCursor(ranges);
      let clusters: Intl.Segments | undefined;
      const cuts = (j: number) => !glued(node.value[j - 1], node.value[j])
        && (clusters ??= GRAPHEMES.segment(node.value)).containing(j)?.index === j;
      const pieces: HastNode[] = [];
      let from = 0;
      let id = idAt(offsets ? offsets[0] : start);
      const flush = (to: number) => {
        const value = node.value.slice(from, to);
        pieces.push(id && value.trim() ? freshSpan(id, [{ type: 'text', value }]) : { type: 'text', value });
      };
      for (let j = 1; j < node.value.length; j++) {
        const next = idAt(offsets ? offsets[j] : start + j);
        // Inside a word, the reveal it began in holds.
        if (next === id || !cuts(j)) continue;
        flush(j);
        from = j;
        id = next;
      }
      flush(node.value.length);
      return pieces.length === 1 && pieces[0].type === 'text' ? null : pieces;
    };

    const walk = (parent: HastParent) => {
      let out: HastNode[] | null = null;
      parent.children.forEach((child, i) => {
        let replaced: HastNode[] | null = null;
        if (child.type === 'text') {
          replaced = splitText(child as HastText, sourceSpan(parent, i));
        } else if (child.type === 'element') {
          const el = child as HastElement;
          if (UNITS.has(el.tagName)) {
            const at = el.position?.start?.offset;
            const id = at === undefined ? 0 : idCursor(ranges)(at);
            if (id) el.properties.dataFresh = String(id);
          } else if (!OPAQUE.has(el.tagName)) {
            walk(el);
          }
        }
        if (replaced && !out) out = parent.children.slice(0, i);
        if (out) out.push(...(replaced ?? [child]));
      });
      if (out) parent.children = out;
    };

    walk(tree);
  };
}

// --- the fade itself ---

// The mark each span is fading for, and its fade. React reuses a span element
// for other text when the spans before it change (its keys count spans per
// parent), so an element seen again under another mark is re-anchored. The
// fade is kept here rather than read back with getAnimations(), which flushes
// style on every call.
const anchored = new WeakMap<Element, { id: number; fade?: Animation }>();

/**
 * While a selection reaches into the reply, the newest reveal on screen there:
 * the selection can only be anchored in text up to it. Null without one.
 */
function heldReveal(root: HTMLElement | null): number | null {
  const selection = document.getSelection();
  // WebKit reports a selection that runs into a shadow tree as uncollapsed
  // with no range, and getRangeAt(0) throws on it.
  if (!root || !selection || selection.isCollapsed || !selection.rangeCount) return null;
  if (!selection.getRangeAt(0).intersectsNode(root)) return null;
  let newest = 0;
  for (const el of root.querySelectorAll<HTMLElement>('[data-fresh]')) newest = Math.max(newest, Number(el.dataset.fresh));
  return newest;
}

/**
 * The reveals of `text` still fading in, for `blockFreshKeys`. Runs the fades
 * for the spans they produce under `rootRef`, and drops the marks once the
 * stream has ended and the last fade is done, so a settled reply renders the
 * same DOM as one that never streamed.
 */
export function useRevealFade(
  rootRef: React.RefObject<HTMLElement | null>,
  text: string,
  streaming: boolean,
): readonly RevealMark[] {
  const [reveal, setReveal] = useState(() => initialReveal(text, streaming));
  // A selection reaching into the reply holds the marks it could be anchored
  // in, mid-stream and at the settle: dropping one unwraps its spans, which
  // replaces the text nodes the selection is anchored in.
  const [hold, setHold] = useState<number | null>(null);
  let current = reveal;
  if (reveal.text !== text || reveal.live !== streaming) {
    current = nextReveal(reveal, text, streaming, hold);
    setReveal(current);
  }
  // Read once per reply: no spans at all rather than spans that never fade.
  const [still] = useState(prefersReducedMotion);
  const marks = still ? NO_MARKS : current.marks;
  const timesRef = useRef<Map<number, number> | null>(null);

  // After every commit: any block that re-rendered may have rebuilt its spans.
  useLayoutEffect(() => {
    const root = rootRef.current;
    const times = (timesRef.current ??= new Map());
    if (!root || !marks.length || typeof root.animate !== 'function') {
      times.clear();
      return;
    }
    const now = performance.now();
    // Text applied while unseen was never watched arriving: it lands settled.
    const settled = isPageUnseen();
    const ids = new Set<number>();
    for (const m of marks) {
      ids.add(m.id);
      if (!times.has(m.id)) times.set(m.id, settled ? -Infinity : now);
    }
    for (const id of times.keys()) if (!ids.has(id)) times.delete(id);

    for (const el of root.querySelectorAll<HTMLElement>('[data-fresh]')) {
      const id = Number(el.dataset.fresh);
      const prev = anchored.get(el);
      if (prev?.id === id) continue;
      prev?.fade?.cancel();
      const at = times.get(id) ?? -Infinity;
      let fade: Animation | undefined;
      if (now - at < FADE_MS) {
        fade = el.animate(FADE_KEYFRAMES, FADE_TIMING);
        // The document timeline shares performance.now()'s origin.
        fade.startTime = at;
      }
      anchored.set(el, { id, fade });
    }
  });

  const tracking = marks.length > 0;
  useEffect(() => {
    if (!tracking) return;
    const sync = () => setHold(heldReveal(rootRef.current));
    document.addEventListener('selectionchange', sync);
    return () => {
      document.removeEventListener('selectionchange', sync);
      setHold(null);
    };
  }, [tracking, rootRef]);

  useEffect(() => {
    if (reveal.live || !reveal.marks.length || hold !== null) return;
    const timer = setTimeout(() => setReveal((r) => (r.live ? r : { ...r, marks: NO_MARKS })), REVEAL_SETTLE_MS);
    return () => clearTimeout(timer);
  }, [reveal, hold]);

  return marks;
}
