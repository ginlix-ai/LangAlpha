/**
 * Words lose their mark on the exit beat the box written about them leaves
 * on: they move to a highlight of their own that fades, rather than going a
 * frame into the box's fade. jsdom has no CSS Highlight API, so it is stood in.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { TextMarks } from '../textMarks';

class FakeHighlight extends Set<Range> {
  constructor(...ranges: Range[]) {
    super(ranges);
  }
}

let highlights: Map<string, FakeHighlight>;

function words(text: string, from: number, to: number): Range {
  const p = document.createElement('p');
  p.textContent = text;
  document.body.append(p);
  const range = document.createRange();
  range.setStart(p.firstChild!, from);
  range.setEnd(p.firstChild!, to);
  return range;
}

function sameWords(range: Range): Range {
  const again = document.createRange();
  again.setStart(range.startContainer, range.startOffset);
  again.setEnd(range.endContainer, range.endOffset);
  return again;
}

const names = () => [...highlights.keys()];
const leaving = () => names().find((n) => n.endsWith('-leaving'));

beforeEach(() => {
  highlights = new Map();
  vi.stubGlobal('CSS', { highlights });
  vi.stubGlobal('Highlight', FakeHighlight);
});

afterEach(() => {
  vi.unstubAllGlobals();
  document.body.replaceChildren();
  document.head.querySelectorAll('style').forEach((s) => s.remove());
});

describe('TextMarks', () => {
  it('fades words that lose their mark, then lets them go', async () => {
    const marks = new TextMarks();
    const range = words('revenue grew', 0, 7);
    marks.paint([range]);
    expect(names()).toHaveLength(1);

    marks.paint([]);
    const fading = leaving();
    expect(fading).toBeDefined();
    expect([...highlights.get(fading!)!]).toEqual([range]);
    await vi.waitFor(() => expect(leaving()).toBeUndefined(), { timeout: 2000 });
    marks.dispose();
  });

  it('keeps a mark found again on the same words, with nothing fading', () => {
    const marks = new TextMarks();
    const range = words('revenue grew', 0, 7);
    marks.paint([range]);
    marks.paint([sameWords(range)]);
    expect(leaving()).toBeUndefined();
    marks.dispose();
  });

  it('lets the mark go at once for a reader who asked for less motion', () => {
    vi.stubGlobal('matchMedia', (query: string) => ({
      matches: query.includes('reduce'),
      media: query,
      addEventListener: () => {},
      removeEventListener: () => {},
    }));
    const marks = new TextMarks();
    marks.paint([words('revenue grew', 0, 7)]);
    marks.paint([]);
    expect(names()).toEqual([]);
    marks.dispose();
  });
});
