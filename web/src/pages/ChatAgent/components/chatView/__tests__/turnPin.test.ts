import { describe, expect, it, vi } from 'vitest';
import type { StreamFollow } from '../streamFollow';
import { createTurnPin } from '../turnPin';

// A transcript whose rows sit at the given offsets from the scroller's top.
function transcript(rows: Array<{ attrs: Record<string, string>; top: number }>) {
  const c = document.createElement('div');
  Object.defineProperty(c, 'clientHeight', { get: () => 800, configurable: true });
  c.getBoundingClientRect = () => ({ top: 0, height: 800 }) as DOMRect;
  for (const { attrs, top } of rows) {
    const el = document.createElement('div');
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
    el.getBoundingClientRect = () => ({ top, height: 40 }) as DOMRect;
    c.append(el);
  }
  document.body.append(c);
  return c;
}

function landing(c: HTMLElement, restart: boolean) {
  const engine = { land: vi.fn() } as unknown as StreamFollow & { land: ReturnType<typeof vi.fn> };
  const pin = createTurnPin();
  pin.start(c, restart);
  pin.settle(c, engine);
  const [target] = engine.land.mock.calls[0] as [() => number | null];
  return target();
}

describe('createTurnPin', () => {
  const user = { 'data-message-role': 'user', 'data-message-id': 'u1' };
  const reply = { 'data-message-role': 'assistant', 'data-message-id': 'a1' };

  it('pins the reader\'s message a restart leaves last', () => {
    const c = transcript([{ attrs: user, top: 1200 }, { attrs: reply, top: 1300 }]);
    expect(landing(c, true)).toBe(1200 - 16);
    c.remove();
  });

  // Regenerating a wake turn's reply: the turn opens with the wakeup's row,
  // and the last user bubble is the turn before's question.
  it('pins the row a wake turn opens with, not the turn before\'s question', () => {
    const c = transcript([
      { attrs: user, top: 100 },
      { attrs: reply, top: 200 },
      { attrs: { 'data-wakeup': 'at' }, top: 1500 },
      { attrs: { ...reply, 'data-message-id': 'a2' }, top: 1560 },
    ]);
    expect(landing(c, true)).toBe(1500 - 16);
    c.remove();
  });
});
