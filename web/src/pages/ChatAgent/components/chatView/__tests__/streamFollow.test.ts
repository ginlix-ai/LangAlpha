import { afterEach, describe, expect, it, vi } from 'vitest';
import { createStreamFollow, movesView, towardStart } from '../streamFollow';
import { createTranscriptFollow } from '../transcriptFollow';

// A scroller the test moves by hand: the end is scrollHeight - clientHeight.
function scroller(top: number, height: number) {
  const c = document.createElement('div');
  const box = { top, height };
  Object.defineProperty(c, 'scrollTop', { get: () => box.top, configurable: true });
  Object.defineProperty(c, 'scrollHeight', { get: () => box.height, configurable: true });
  Object.defineProperty(c, 'clientHeight', { get: () => 1000, configurable: true });
  return { c, box };
}

afterEach(() => vi.restoreAllMocks());

describe('createStreamFollow', () => {
  it('reads a clamp as layout: the follow holds', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    // A row below folds 200px; the browser clamps the view onto the new end.
    box.height = 1800;
    box.top = 800;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });

  // A notch up while a row below folds: the fold takes back the distance the
  // notch put between the view and the end.
  it('reads a move up as the reader leaving while they pull up', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    engine.pulled();
    box.height = 1950;
    box.top = 900;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  // A row closing clamps the view at the end, and a line lands before the
  // clamp's scroll event: the end is further away than the move up.
  it('keeps following through growth that lands with a clamp', () => {
    const { c, box } = scroller(1, 1001);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    box.height = 1010;
    box.top = 0;
    engine.scrolled();
    expect(following.current).toBe(true);
  });

  // Growth landing with a move up reads the same as a fold under it: the
  // reader's input is what says the move was theirs.
  it('reads a move up with growth as the reader leaving when they scrolled', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    engine.touched();
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  // A row folds 30px as a line 54px tall lands, with no input from the reader.
  it('reads a move up with growth and no input as layout: the follow holds', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });

  it('lets the input lapse', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    const now = vi.spyOn(performance, 'now').mockReturnValue(1000);
    engine.touched();
    now.mockReturnValue(2000);
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });

  it('reads a move up with growth as the reader leaving just inside the input window', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    const now = vi.spyOn(performance, 'now').mockReturnValue(1000);
    engine.touched();
    now.mockReturnValue(1299);
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  // A scrollbar drag held past the input window: still the reader's until the
  // pointer lifts.
  it('reads a move up with growth as the reader leaving while a pointer is held', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    const now = vi.spyOn(performance, 'now').mockReturnValue(1000);
    engine.touched();
    engine.pressed(true);
    now.mockReturnValue(3000);
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  // A row below folds while the reader holds the scrollbar: the move reads as
  // a clamp, but the pointer says it is theirs.
  it('reads a shrinking move up as the reader leaving while a pointer is held', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    engine.pressed(true);
    box.height = 1800;
    box.top = 800;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  it('reads a move up with growth as layout once the pointer lifts', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    const now = vi.spyOn(performance, 'now').mockReturnValue(1000);
    engine.touched();
    engine.pressed(true);
    engine.pressed(false);
    now.mockReturnValue(3000);
    box.height = 2024;
    box.top = 970;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });

  // A reader 100px from the end scrolls 50px down as 50px lands: the end is
  // as far as before, which is anchoring's shape too, but they scrolled.
  it('reads a move down with growth as the reader rejoining when they scrolled', () => {
    const { c, box } = scroller(900, 2000);
    const following = { current: false };
    const engine = createStreamFollow(c, following);
    engine.touched();
    box.height = 2050;
    box.top = 950;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(true);
  });

  // Growth with the view where it was is the baseline: a jump after it has
  // nothing grown with it.
  it('measures a later move up from a height-only resize', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    box.height = 2040;
    engine.resized();
    box.top = 400;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  // Find in page, or an anchor: nothing grew, so the distance alone decides.
  it('reads a move up with nothing grown as the reader leaving, input or not', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    box.top = 400;
    expect(engine.scrolled()).toBe(true);
    expect(following.current).toBe(false);
  });

  it('reads a move down that leaves the end as far as layout: the follow holds', () => {
    const { c, box } = scroller(900, 2040);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    // A row above the view grows 24px; anchoring holds the view on its content.
    box.height = 2064;
    box.top = 924;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });

  it('lets the pull lapse', () => {
    const { c, box } = scroller(1000, 2000);
    const following = { current: true };
    const engine = createStreamFollow(c, following);
    const now = vi.spyOn(performance, 'now').mockReturnValue(1000);
    engine.pulled();
    now.mockReturnValue(2000);
    box.height = 1800;
    box.top = 800;
    expect(engine.scrolled()).toBe(false);
    expect(following.current).toBe(true);
  });
});

describe('towardStart', () => {
  it.each([
    [new WheelEvent('wheel', { deltaY: -40 }), true],
    [new WheelEvent('wheel', { deltaY: 40 }), false],
    [new KeyboardEvent('keydown', { key: 'ArrowUp' }), true],
    [new KeyboardEvent('keydown', { key: 'PageUp' }), true],
    [new KeyboardEvent('keydown', { key: ' ', shiftKey: true }), true],
    [new KeyboardEvent('keydown', { key: ' ' }), false],
    [new KeyboardEvent('keydown', { key: 'Enter' }), false],
    [new Event('pointerdown'), false],
    [new Event('touchstart'), false],
  ])('%s -> %s', (e, up) => {
    expect(towardStart(e)).toBe(up);
  });
});

// Both transcripts attach through here, so the lift that ends a held pointer
// is heard wherever the follow runs.
describe('createTranscriptFollow', () => {
  it.each(['pointerup', 'pointercancel', 'touchend', 'touchcancel', 'blur'])('releases a held pointer on window %s', (type) => {
    const follow = createTranscriptFollow();
    const detach = follow.attach(document.createElement('div'));
    const pressed = vi.spyOn(follow.engine!, 'pressed');
    window.dispatchEvent(new Event(type));
    expect(pressed).toHaveBeenCalledWith(false);
    detach();
    pressed.mockClear();
    window.dispatchEvent(new Event(type));
    expect(pressed).not.toHaveBeenCalled();
  });
});

describe('movesView', () => {
  // As the scroller's listener hears it: dispatched on `target` inside it.
  const heard = (e: Event, on: 'scroller' | 'row' | 'button') => {
    const scroller = document.createElement('div');
    const row = document.createElement('div');
    const button = document.createElement('button');
    row.appendChild(button);
    scroller.appendChild(row);
    let moves: boolean | null = null;
    scroller.addEventListener(e.type, (ev) => { moves = movesView(ev); });
    ({ scroller, row, button })[on].dispatchEvent(e);
    return moves;
  };

  it.each([
    [new WheelEvent('wheel', { deltaY: 40, bubbles: true }), 'row', true],
    [new Event('touchstart', { bubbles: true }), 'row', true],
    [new PointerEvent('pointerdown', { bubbles: true }), 'scroller', true],
    [new PointerEvent('pointerdown', { bubbles: true }), 'row', false],
    [new PointerEvent('pointerdown', { bubbles: true, button: 1 }), 'row', true],
    [new KeyboardEvent('keydown', { key: 'PageDown', bubbles: true }), 'row', true],
    [new KeyboardEvent('keydown', { key: ' ', bubbles: true }), 'row', true],
    [new KeyboardEvent('keydown', { key: ' ', bubbles: true }), 'button', false],
    [new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }), 'button', true],
    [new KeyboardEvent('keydown', { key: 'a', bubbles: true }), 'row', false],
  ] as const)('%s on %s -> %s', (e, on, moves) => {
    expect(heard(e, on)).toBe(moves);
  });
});
