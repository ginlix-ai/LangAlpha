/**
 * A live reply's text, one segment per run instead of one per chunk.
 *
 * Every walk over a message's segments (grouping, sorting, the copy text, the
 * minimap) runs per chunk, so a segment per chunk made each chunk cost the
 * length of the reply so far. A chunk now extends the text segment it follows.
 * The segment keeps where each chunk ended, which is everything the per-chunk
 * shape knew: a steering rollback or a segment that sorts inside the run later
 * still cuts it at the chunk it belongs after, exactly as before.
 */
import type { ContentSegment, TextChunkMark, TextSegment } from '@/types/chat';

/** A segment as the readers take it: they walk every segment of a message, and only live text is a run with marks to cut at. */
type MaybeRun = Pick<TextSegment, 'order' | 'chunks'> & Partial<Pick<TextSegment, 'content'>>;

/**
 * The segments with `content` added as the newest chunk, at `order`. Extends
 * the last segment when it is text of the same phase with an older last chunk;
 * otherwise starts a segment. A chunk older than the newest text already here
 * (orders are event ids, so only a mixed-up stream does this) gets a segment
 * with no marks, one per chunk as before: runs must never overlap, or grouping
 * could not put their chunks back in order. Always a new array and a new last
 * segment: the projection cache compares by identity.
 */
export function appendTextChunk(
  segments: readonly ContentSegment[],
  content: string,
  order: number,
  phase?: TextSegment['phase'],
): ContentSegment[] {
  const last = segments[segments.length - 1];
  if (last?.type === 'text' && last.chunks && last.phase === phase && order > last.chunks.order) {
    const text = last.content + content;
    return [...segments.slice(0, -1), { ...last, content: text, chunks: { order, end: text.length, prev: last.chunks } }];
  }
  const segment: TextSegment = { type: 'text', content, order, ...(phase ? { phase } : {}) };
  if (order > newestChunkOrder(segments)) segment.chunks = { order, end: content.length, prev: null };
  return [...segments, segment];
}

function newestChunkOrder(segments: readonly ContentSegment[]): number {
  for (let i = segments.length - 1; i >= 0; i--) {
    const segment = segments[i];
    if (segment.type === 'text' && segment.chunks) return segment.chunks.order;
  }
  return -Infinity;
}

/** The order of a text segment's newest chunk, or its own order without marks. */
export function lastChunkOrder(segment: Pick<TextSegment, 'order' | 'chunks'>): number {
  return segment.chunks?.order ?? segment.order;
}

/**
 * A text segment cut after its last chunk at or before `order`: the chunks up
 * to there, and the rest as a segment of its own at its first chunk's order.
 * Null when the whole segment falls on one side, or it has no marks to cut at.
 */
export function splitTextAt<T extends MaybeRun>(segment: T, order: number): [T, T] | null {
  const after: TextChunkMark[] = [];
  let mark = segment.chunks ?? null;
  while (mark && mark.order > order) {
    after.push(mark);
    mark = mark.prev;
  }
  if (!mark || !after.length) return null;
  const cut = mark.end;
  const content = segment.content ?? '';
  let rest: TextChunkMark | null = null;
  for (let i = after.length - 1; i >= 0; i--) rest = { order: after[i].order, end: after[i].end - cut, prev: rest };
  return [
    { ...segment, content: content.slice(0, cut), chunks: mark },
    { ...segment, content: content.slice(cut), order: after[after.length - 1].order, chunks: rest! },
  ];
}

/** Segments in `order`, sorted only when they are not already: a streaming reply arrives in order, so the common case is a single pass rather than a copy and a sort on every chunk. */
function inOrder<T extends { order: number }>(segments: readonly T[]): readonly T[] {
  for (let i = 1; i < segments.length; i++) {
    if (segments[i].order < segments[i - 1].order) return [...segments].sort((a, b) => a.order - b.order);
  }
  return segments;
}

/** Segments in the order their chunks arrived. A live text segment holds a run
 *  of chunks, and one written later can sort inside it (a notification is
 *  debounced); the run is cut there, as if every chunk were its own segment.
 *  Nothing is cut in the common case, which is one pass over the segments. */
export function inChunkOrder<T extends MaybeRun>(segments: readonly T[]): readonly T[] {
  const sorted = inOrder(segments);
  let cut = false;
  for (let i = 1; i < sorted.length && !cut; i++) cut = lastChunkOrder(sorted[i - 1]) > sorted[i].order;
  if (!cut) return sorted;
  const out: T[] = [];
  for (const segment of sorted) {
    let rest: T | undefined;
    const tail = out[out.length - 1];
    if (tail && lastChunkOrder(tail) > segment.order) {
      const parts = splitTextAt(tail, segment.order);
      if (parts) [out[out.length - 1], rest] = parts;
      else rest = out.pop();
    }
    out.push(segment);
    if (rest) out.push(rest);
  }
  return out;
}
