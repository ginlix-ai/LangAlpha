import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

interface Leg {
  onUpdate: (v: number) => void;
  onComplete: () => void;
  stop: ReturnType<typeof vi.fn>;
}
const legs: Leg[] = [];

vi.mock('../framer', () => ({
  animate: (_from: number, _to: number, opts: Omit<Leg, 'stop'>) => {
    const leg = { ...opts, stop: vi.fn() };
    legs.push(leg);
    return { stop: leg.stop };
  },
}));

const { followHeight } = await import('../followHeight');

let resize: () => void = () => {};
const disconnect = vi.fn();
const observe = vi.fn();

beforeEach(() => {
  legs.length = 0;
  disconnect.mockClear();
  observe.mockClear();
  vi.stubGlobal(
    'ResizeObserver',
    class {
      constructor(cb: () => void) {
        resize = cb;
      }
      observe = observe;
      disconnect = disconnect;
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

function setup() {
  const box = document.createElement('div');
  const content = document.createElement('div');
  let measured = 100;
  content.getBoundingClientRect = () => ({ height: measured }) as DOMRect;
  const onLand = vi.fn();
  const stop = followHeight(box, content, { from: 0, leg: () => ({}), onLand });
  return { box, onLand, stop, resizeTo: (h: number) => ((measured = h), resize()) };
}

describe('followHeight', () => {
  // The stop of a retargeted leg can finish it, and a leg framer skipped
  // lands a frame late: neither may release the box under the next leg.
  it('lands only the current leg', () => {
    const { onLand, resizeTo } = setup();
    resizeTo(160);
    expect(legs).toHaveLength(2);

    legs[0].onComplete();
    expect(onLand).not.toHaveBeenCalled();
    expect(disconnect).not.toHaveBeenCalled();

    legs[1].onComplete();
    expect(onLand).toHaveBeenCalledTimes(1);
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('ignores a leg that reports after the stop', () => {
    const { box, onLand, stop } = setup();
    legs[0].onUpdate(40);
    stop();
    legs[0].onUpdate(80);
    legs[0].onComplete();
    expect(box.style.height).toBe('40px');
    expect(onLand).not.toHaveBeenCalled();
  });
});
