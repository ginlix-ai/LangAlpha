/**
 * A live reply keeps one text segment per run of chunks instead of one per
 * chunk. Every reader must see the same reply either way: seeded random
 * streams are built both ways and compared through grouping (what renders,
 * under which key), the copy text, and a steering rollback at every boundary.
 */
import { describe, it, expect } from 'vitest';
import type { ContentSegment, NotificationSegment, TextSegment, ToolCallSegment } from '@/types/chat';
import { appendTextChunk } from '../textChunks';
import { keepSegmentsThrough } from '../steeringRollback';
import { groupSegments } from '../../../components/messageList/buildRenderBlocks';
import { assistantText } from '../../../components/messageList/messageText';
import type { ContentSegmentRecord } from '../../../components/messageList/types';

function rng(seed: number) {
  return () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 2 ** 32;
  };
}

/** The same stream twice: one segment per chunk, as the handler used to write
 *  it, and through `appendTextChunk`. */
function stream(seed: number): { perChunk: ContentSegment[]; merged: ContentSegment[] } {
  const r = rng(seed);
  let perChunk: ContentSegment[] = [];
  let merged: ContentSegment[] = [];
  let order = 1;
  let phase: TextSegment['phase'];
  const texts: number[] = [];
  for (let i = 0; i < 60; i++) {
    const roll = r();
    if (roll < 0.7) {
      if (r() < 0.05) phase = phase ? undefined : 'final_answer';
      // Now and then a chunk arrives with an order older than the one before.
      const at = r() < 0.03 && order > 3 ? order - 2 + 0.5 : order++;
      const content = `c${i}${r() < 0.2 ? '\n\n' : ' '}`;
      perChunk = [...perChunk, { type: 'text', content, order: at, ...(phase ? { phase } : {}) }];
      merged = appendTextChunk(merged, content, at, phase);
      texts.push(at);
    } else if (roll < 0.85) {
      const tool: ToolCallSegment = { type: 'tool_call', toolCallId: `t${i}`, order: order++ };
      perChunk = [...perChunk, tool];
      merged = [...merged, tool];
    } else if (texts.length) {
      // Written late, with the order of an event that preceded later text
      // (the offload notification is debounced).
      const back = texts[Math.max(0, texts.length - 1 - Math.floor(r() * 4))];
      const late: NotificationSegment = { type: 'notification', content: `n${i}`, order: back + (r() < 0.5 ? 0 : 0.25) };
      perChunk = [...perChunk, late];
      merged = [...merged, late];
    }
  }
  return { perChunk, merged };
}

const plain = (segments: ContentSegmentRecord[]) => segments.map(({ type, content, order, toolCallId }) => ({ type, content, order, toolCallId }));

describe('text segments merged per run', () => {
  it('renders, copies and rolls back like one segment per chunk', () => {
    for (let seed = 1; seed <= 300; seed++) {
      const { perChunk, merged } = stream(seed);
      expect(merged.length).toBeLessThanOrEqual(perChunk.length);
      expect(plain(groupSegments(merged)), `seed ${seed}`).toEqual(plain(groupSegments(perChunk)));
      expect(assistantText({ contentSegments: merged })).toBe(assistantText({ contentSegments: perChunk }));
      const orders = [...new Set(perChunk.map((s) => s.order))];
      for (const boundary of [...orders, ...orders.map((o) => o + 0.1)]) {
        // What the rollback keeps: the old filter over per-chunk segments.
        const kept = perChunk.filter((s) => s.order <= boundary);
        expect(plain(groupSegments(keepSegmentsThrough(merged, boundary))), `seed ${seed} boundary ${boundary}`).toEqual(plain(groupSegments(kept)));
      }
    }
  });

  it('extends the last segment only for text of the same phase, newer than its last chunk', () => {
    let segments = appendTextChunk([], 'a', 1);
    segments = appendTextChunk(segments, 'b', 2);
    expect(segments).toHaveLength(1);
    expect(segments[0]).toMatchObject({ content: 'ab', order: 1 });
    expect(appendTextChunk(segments, 'c', 3, 'final_answer')).toHaveLength(2);
    expect(appendTextChunk(segments, 'c', 2)).toHaveLength(2);
    expect(appendTextChunk([...segments, { type: 'tool_call', toolCallId: 't', order: 3 }], 'c', 4)).toHaveLength(3);
    // A text segment without marks (history) is whole: it is not extended.
    expect(appendTextChunk([{ type: 'text', content: 'x', order: 1 }], 'y', 2)).toHaveLength(2);
  });

  it('never changes the segment it extends', () => {
    const one = appendTextChunk([], 'a', 1);
    const two = appendTextChunk(one, 'b', 2);
    expect(one[0]).toMatchObject({ content: 'a' });
    expect(two).not.toBe(one);
    expect(two[0]).not.toBe(one[0]);
  });
});
