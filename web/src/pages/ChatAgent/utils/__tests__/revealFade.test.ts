import { CATCH_UP_CHARS, TICK_MS } from '@/components/ui/animated-text';
import { DURATION } from '@/lib/motion';
import {
  KEEP_MARKS, MAX_MARKS, MOUNT_TAIL, blockFreshKeys, initialReveal, nextReveal, parseFreshKey, type RevealState,
} from '../revealFade';

const range = (from: number, to: number): number[] => Array.from({ length: to - from + 1 }, (_, i) => from + i);
const extents = (state: RevealState) => state.marks.map(({ from, to }) => [from, to]);

function stream(texts: string[], state = initialReveal('', true)): RevealState {
  return texts.reduce((s, text) => nextReveal(s, text, true), state);
}

describe('initialReveal', () => {
  it('fades in only the tail of a reply that mounts mid-stream', () => {
    const long = 'word '.repeat(200);
    expect(extents(initialReveal(long, true))).toEqual([[long.length - MOUNT_TAIL, long.length]]);
    expect(extents(initialReveal('Short reply.', true))).toEqual([[0, 12]]);
  });

  it('marks nothing for a reply that mounts settled', () => {
    expect(initialReveal('word '.repeat(200), false).marks).toEqual([]);
  });
});

describe('nextReveal', () => {
  it('marks exactly the range each growth added, under an increasing id', () => {
    const state = stream(['He', 'Hello', 'Hello world']);
    expect(extents(state)).toEqual([[0, 2], [2, 5], [5, 11]]);
    const [a, b, c] = state.marks.map((m) => m.id);
    expect(b).toBeGreaterThan(a);
    expect(c).toBeGreaterThan(b);
  });

  // The typewriter's last tick and `streaming` turning off land in one render.
  it('marks the growth that arrives with the end of the stream', () => {
    const state = nextReveal(stream(['Hello']), 'Hello world', false);
    expect(extents(state)).toEqual([[0, 5], [5, 11]]);
    expect(state.live).toBe(false);
  });

  it('keeps the marks of an ended stream until its text changes', () => {
    const ended = nextReveal(stream(['Hello']), 'Hello world', false);
    expect(nextReveal(ended, 'Hello world', false).marks).toBe(ended.marks);
    expect(nextReveal(ended, 'Hello world!', false).marks).toEqual([]);
  });

  it('clamps marks to a shrunk text and drops those wholly past it', () => {
    const state = nextReveal(stream(['He', 'Hello', 'Hello world']), 'Hell', true);
    expect(extents(state)).toEqual([[0, 2], [2, 4]]);
  });

  it('prunes marks in batches, keeping the newest', () => {
    // Batches on purpose: do not simplify to a per-tick slice(-N). Dropping the
    // oldest mark every tick unwraps the first span of a paragraph each time,
    // which shifts the keys of every span after it and rebuilds them all.
    // Between batches no mark may go, which is what these counts pin.
    const ticks = 2 * MAX_MARKS - KEEP_MARKS + 4;
    let state = initialReveal('', true);
    let text = '';
    const counts: number[] = [];
    for (let i = 0; i < ticks; i++) {
      text += 'a';
      state = nextReveal(state, text, true);
      counts.push(state.marks.length);
    }
    expect(counts).toEqual([...range(1, MAX_MARKS), ...range(KEEP_MARKS, MAX_MARKS), ...range(KEEP_MARKS, KEEP_MARKS + 2)]);
    expect(extents(state)).toEqual(range(ticks - KEEP_MARKS - 2, ticks - 1).map((from) => [from, from + 1]));
  });

  // Dropping a mark unwraps its spans, and with them a selection anchored there.
  it('keeps the marks a selection holds and still prunes the ones after it', () => {
    let state = initialReveal('', true);
    let text = '';
    const grow = (hold: number | null) => {
      text += 'a';
      state = nextReveal(state, text, true, hold);
    };
    for (let i = 0; i < 10; i++) grow(null);
    const held = state.marks;
    const counts: number[] = [];
    for (let i = 0; i < 3 * MAX_MARKS; i++) {
      grow(held.at(-1)!.id);
      counts.push(state.marks.length);
    }
    expect(state.marks.slice(0, held.length)).toEqual(held);
    expect(Math.max(...counts)).toBe(held.length + MAX_MARKS);
    expect(nextReveal(state, text + 'a', true).marks).toHaveLength(KEEP_MARKS);
  });

  // A mark dropped while its text still fades pops in, so the marks kept have
  // to span more than a fade at the typewriter's fastest cadence.
  it('keeps marks past the fade at the fastest tick', () => {
    expect(KEEP_MARKS * TICK_MS).toBeGreaterThan(DURATION.reveal * 1000);
  });

  // An update the typewriter does not type (a reconnect replay) landed at
  // once: fading every paragraph of it would start hundreds of fades.
  it('fades only the end of an update that arrived at once, from a paragraph start', () => {
    const backlog = 'A paragraph of the backlog.\n\n'.repeat(100);
    const last = 'The last paragraph. '.repeat(10);
    const text = 'Hi.\n\n' + backlog + last;
    const [, mark] = nextReveal(stream(['Hi.\n\n']), text, true).marks;
    expect(mark.to).toBe(text.length);
    expect(text.slice(mark.from - 2, mark.from)).toBe('\n\n');
    expect(mark.to - mark.from).toBeGreaterThanOrEqual(CATCH_UP_CHARS);
    expect(mark.to - mark.from).toBeLessThan(CATCH_UP_CHARS + 'A paragraph of the backlog.\n\n'.length);
  });

  // Paragraph mode releases a paragraph whole; a long one still fades whole.
  it('fades a long paragraph that arrived at once whole', () => {
    const paragraph = 'One long paragraph that keeps going. '.repeat(40);
    expect(paragraph.length).toBeGreaterThan(CATCH_UP_CHARS);
    const state = nextReveal(stream(['Intro.\n\n']), 'Intro.\n\n' + paragraph, true);
    expect(extents(state)).toEqual([[0, 8], [8, 8 + paragraph.length]]);
  });
});

describe('blockFreshKeys', () => {
  // Starts at 0, 6 and 12; 19 long.
  const blocks = ['One.\n\n', 'Two.\n\n', 'Three.\n'];

  it("gives '' to every block no mark overlaps, so its memo holds", () => {
    expect(blockFreshKeys(blocks, [])).toEqual(['', '', '']);
    expect(blockFreshKeys(blocks, [{ from: 0, to: 6, id: 1 }])).toEqual(['0:6:1', '', '']);
    expect(blockFreshKeys(blocks, [{ from: 13, to: 15, id: 2 }])).toEqual(['', '', '1:3:2']);
  });

  it('splits a mark across the blocks it spans', () => {
    expect(blockFreshKeys(blocks, [{ from: 4, to: 8, id: 3 }])).toEqual(['4:6:3', '0:2:3', '']);
  });

  it('round-trips through parseFreshKey as marks rebased to the block', () => {
    const keys = blockFreshKeys(blocks, [
      { from: 2, to: 4, id: 1 },
      { from: 4, to: 9, id: 2 },
      { from: 14, to: 19, id: 3 },
    ]);
    expect(keys.map(parseFreshKey)).toEqual([
      [{ from: 2, to: 4, id: 1 }, { from: 4, to: 6, id: 2 }],
      [{ from: 0, to: 3, id: 2 }],
      [{ from: 2, to: 7, id: 3 }],
    ]);
  });
});
