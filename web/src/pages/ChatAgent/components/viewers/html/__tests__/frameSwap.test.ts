// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { frameSwap, isReady, isShown, NO_SWAP, type FramedDoc, type FrameSwap, type FrameSwapAction } from '../frameSwap';

const doc = (key: string): FramedDoc => ({ key, src: `https://x.test/${key}`, origin: 'null', sandbox: 'allow-scripts' });

function run(...actions: FrameSwapAction[]): FrameSwap {
  return actions.reduce(frameSwap, NO_SWAP);
}

describe('frameSwap', () => {
  it('shows a first document as soon as it has loaded', () => {
    const loading = run({ type: 'frame', doc: doc('a') });
    expect(isShown(loading)).toBe(false);
    const loaded = frameSwap(loading, { type: 'loaded', key: 'a' });
    expect(isShown(loaded)).toBe(true);
    expect(loaded.behind).toBeNull();
  });

  it('keeps the shown document behind the next until the next is built, then held', () => {
    let state = run({ type: 'frame', doc: doc('a') }, { type: 'loaded', key: 'a' }, { type: 'frame', doc: doc('b') });
    expect(state.behind?.key).toBe('a');
    state = frameSwap(state, { type: 'loaded', key: 'b' });
    expect(isReady(state)).toBe(false);
    for (const fact of ['settled', 'landed', 'overdue'] as const) {
      const ready = frameSwap(state, { type: fact, key: 'b' });
      expect(isReady(ready)).toBe(true);
      expect(isShown(ready)).toBe(false);
      expect(isShown(frameSwap(ready, { type: 'held', key: 'b' }))).toBe(true);
    }
  });

  it('keeps the one on screen behind when the document replacing it is replaced before it was shown', () => {
    const state = run(
      { type: 'frame', doc: doc('a') }, { type: 'loaded', key: 'a' },
      { type: 'frame', doc: doc('b') }, { type: 'loaded', key: 'b' },
      { type: 'frame', doc: doc('c') },
    );
    expect(state.current?.doc.key).toBe('c');
    expect(state.behind?.key).toBe('a');
  });

  it('takes the document behind back, shown, when it is asked for again', () => {
    const state = run({ type: 'frame', doc: doc('a') }, { type: 'loaded', key: 'a' }, { type: 'frame', doc: doc('b') }, { type: 'frame', doc: doc('a') });
    expect(state.current?.doc.key).toBe('a');
    expect(state.behind).toBeNull();
    expect(isShown(state)).toBe(true);
  });

  it('ignores facts about a document no longer loading, and a drop of another', () => {
    const state = run({ type: 'frame', doc: doc('a') }, { type: 'loaded', key: 'a' }, { type: 'frame', doc: doc('b') });
    expect(frameSwap(state, { type: 'loaded', key: 'a' })).toBe(state);
    expect(frameSwap(state, { type: 'drop', key: 'b' })).toBe(state);
    expect(frameSwap(state, { type: 'drop', key: 'a' }).behind).toBeNull();
    expect(frameSwap(state, { type: 'frame', doc: doc('b') })).toBe(state);
  });
});
