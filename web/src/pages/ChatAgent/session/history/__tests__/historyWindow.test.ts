/**
 * The window over a paged transcript: which turns it holds, the one older page
 * in flight, and the token that refuses a page something else overtook.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { scrollMemory } from '@/lib/scrollMemory';
import { runAuthResets } from '@/lib/authResets';
import { createHistoryWindow, type HistoryWindow } from '../historyWindow';
import { HISTORY_PAGE_TURNS, HISTORY_RELOAD_MARGIN_TURNS, screenPageBudget } from '../pageBudget';
import type { LoadedHistoryPage, OlderHistoryPage } from '../olderPages';

const pageFrom = (firstTurnIndex: number, hasMore = true, lastTurnIndex = 80): LoadedHistoryPage => ({
  firstTurnIndex,
  lastTurnIndex,
  hasMore,
  carry: { events: [], subagents: new Map(), steeredAgentIds: new Set() },
});

/** A window that loaded the newest page of thread-1, starting at `first`. */
function opened(first = 61) {
  const window = createHistoryWindow();
  window.land(window.beginReload(), 'thread-1', pageFrom(first));
  return window;
}

// A desktop screen 1000px tall: a floor of 5 turns, 3 MB.
const SCREEN = screenPageBudget(false, 1000);

/** The limit the newest load of `tid` asks for. */
const reloadLimit = (w: HistoryWindow, tid: string, newest: number) => w.newestPageBudget(tid, newest, SCREEN).limit;

describe('createHistoryWindow', () => {
  afterEach(() => scrollMemory.clear());

  it('has nothing to page back from before the first load lands', () => {
    const window = createHistoryWindow();
    expect(window.snapshot().status).toBe('end');
    window.beginReload();
    expect(window.snapshot().status).toBe('reloading');
    expect(window.request('thread-1')).toBeNull();
  });

  it('starts the page before the oldest loaded turn, one at a time', () => {
    const window = opened();
    expect(window.snapshot().status).toBe('idle');
    const request = window.request('thread-1');
    expect(request).toMatchObject({ beforeTurn: 61 });
    expect(window.snapshot().status).toBe('loading');
    expect(window.request('thread-1')).toBeNull();

    window.land(request!.token, 'thread-1', pageFrom(0, false));
    expect(window.snapshot().status).toBe('end');
  });

  it('refuses the landing of a page a reload overtook, and stops its fetch', () => {
    const window = opened();
    const request = window.request('thread-1')!;
    const reload = window.beginReload();
    expect(request.signal.aborted).toBe(true);
    expect(window.isCurrent(request.token)).toBe(false);

    window.land(request.token, 'thread-1', pageFrom(41));
    window.fail(request.token);
    expect(window.snapshot().status).toBe('reloading');
    window.land(reload, 'thread-1', pageFrom(61));
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('refuses a page for another thread', () => {
    expect(opened().request('thread-2')).toBeNull();
  });

  it('holds the top while a fork cuts, dropping the page on its way', () => {
    const window = opened();
    const request = window.request('thread-1')!;
    window.hold();
    expect(request.signal.aborted).toBe(true);
    expect(window.snapshot().status).toBe('held');
    expect(window.request('thread-1')).toBeNull();

    window.release();
    expect(window.snapshot().status).toBe('idle');
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('offers a retry after a page failed', () => {
    const window = opened();
    window.fail(window.request('thread-1')!.token);
    expect(window.snapshot().status).toBe('failed');
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('tells subscribers when the snapshot changes, and only then, keeping its identity otherwise', () => {
    const window = opened();
    let calls = 0;
    const unsubscribe = window.subscribe(() => {
      calls += 1;
    });
    const before = window.snapshot();
    window.release();
    expect(calls).toBe(0);
    expect(window.snapshot()).toBe(before);
    window.request('thread-1');
    expect(calls).toBe(1);
    unsubscribe();
    window.reset();
    expect(calls).toBe(1);
  });

  it('reaches back on a return to a thread left mid-way, and only then', () => {
    opened(41);
    const fresh = createHistoryWindow();
    // Left at the bottom, or never scrolled: the newest page is enough.
    expect(reloadLimit(fresh, 'thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
    scrollMemory.set('thread:thread-1', 'bottom');
    expect(reloadLimit(fresh, 'thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
    // Left mid-way: the offset is measured from turn 41 down, so the page
    // starts there again, with no margin above it.
    scrollMemory.set('thread:thread-1', 1200);
    expect(reloadLimit(fresh, 'thread-1', 80)).toBe(80 - 41 + 1);
    // A thread the window holds reloads what it holds.
    expect(reloadLimit(opened(70), 'thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
  });

  it('reaches back to the oldest turn paged in at every reload, with one margin', () => {
    const window = opened(41);
    const limit = reloadLimit(window, 'thread-1', 80);
    expect(limit).toBe(80 - 41 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
    // The reload's page starts the margin above turn 41, and older pages go
    // on above that; the next reload still reaches back only to 41.
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - limit + 1));
    expect(reloadLimit(window, 'thread-1', 80)).toBe(limit);
    const request = window.request('thread-1')!;
    expect(request).toMatchObject({ beforeTurn: 41 - HISTORY_RELOAD_MARGIN_TURNS });

    // An older page moves the reach to where it starts.
    window.land(request.token, 'thread-1', pageFrom(16));
    expect(reloadLimit(window, 'thread-1', 80)).toBe(80 - 16 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
    // So does a reload whose page starts after it, as when more turns
    // landed than the margin covers.
    window.land(window.beginReload(), 'thread-1', pageFrom(20));
    expect(reloadLimit(window, 'thread-1', 80)).toBe(80 - 20 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
  });

  it("keeps the reach, not a reload's margin, across a return to the thread", () => {
    const window = opened(41);
    const limit = reloadLimit(window, 'thread-1', 80);
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - limit + 1));
    // Left mid-way, its transcript starting the margin above turn 41.
    window.reset();
    scrollMemory.set('thread:thread-1', 1200);
    // The return starts the page where the saved offset is measured from,
    const back = reloadLimit(window, 'thread-1', 80);
    expect(back).toBe(limit);
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - back + 1));
    // and its next reload still reaches back only to turn 41.
    expect(reloadLimit(window, 'thread-1', 80)).toBe(limit);
  });

  it('starts the reach again where a reload starts when the branch now ends before it', () => {
    const window = opened(41);
    // Forked elsewhere at turn 10: the reload holds turns 0..10, and turn 41
    // is gone.
    window.land(window.beginReload(), 'thread-1', pageFrom(0, false, 10));
    // Grown to turn 20 since, the next reload still holds turn 0.
    expect(reloadLimit(window, 'thread-1', 20)).toBe(20 - 0 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
  });

  it('forgets the reach on sign-out, as it does the offsets', () => {
    opened(41);
    runAuthResets();
    // Another account's offset for the same id reaches back to nothing.
    scrollMemory.set('thread:thread-1', 1200);
    expect(reloadLimit(createHistoryWindow(), 'thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
  });
});

describe('the page each newest load asks for', () => {
  afterEach(() => scrollMemory.clear());

  it('asks a first open for a page of turns, cut by the bytes, at least what fills the screen', () => {
    expect(createHistoryWindow().newestPageBudget('thread-1', 80, SCREEN))
      .toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 5, maxBytes: 3_000_000 });
  });

  it('asks a reload for every turn back to where it reached, whatever their bytes', () => {
    // Turns 70..80 on screen, and the margin.
    expect(opened(70).newestPageBudget('thread-1', 80, SCREEN))
      .toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 11 + HISTORY_RELOAD_MARGIN_TURNS, maxBytes: 3_000_000 });
    scrollMemory.clear();
    expect(opened(41).newestPageBudget('thread-1', 80, SCREEN))
      .toEqual({ limit: 45, minTurns: 45, maxBytes: 3_000_000 });
  });

  it('asks a return to a thread left mid-way for every turn back to where it started', () => {
    opened(76);
    scrollMemory.set('thread:thread-1', 1200);
    expect(createHistoryWindow().newestPageBudget('thread-1', 80, SCREEN))
      .toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 5, maxBytes: 3_000_000 });
    scrollMemory.clear();
    scrollMemory.set('thread:thread-1', 1200);
    opened(61);
    expect(createHistoryWindow().newestPageBudget('thread-1', 80, SCREEN))
      .toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 20, maxBytes: 3_000_000 });
  });
});

describe('filling in behind a newest page cut short on its bytes', () => {
  afterEach(() => scrollMemory.clear());

  /** A replayed older page, as far as the window reads it. */
  const olderPage = (first: number, last: number, hasMore = true) =>
    ({ page: pageFrom(first, hasMore, last) }) as unknown as OlderHistoryPage;

  /** Lands the next older page through the window, as the hook does. */
  function landOlder(w: HistoryWindow, first: number, last: number, hasMore = true) {
    const request = w.request('thread-1')!;
    w.land(request.token, 'thread-1', pageFrom(first, hasMore, last));
  }

  it('owes nothing to a newest page that holds a page of turns, or the whole thread', () => {
    const full = createHistoryWindow();
    full.land(full.beginReload(), 'thread-1', pageFrom(61, true, 80));
    expect(full.backfill('thread-1', false)).toBeNull();
    expect(full.backfill('thread-1', true)).toBeNull();
    const whole = createHistoryWindow();
    whole.land(whole.beginReload(), 'thread-1', pageFrom(76, false, 80));
    expect(whole.backfill('thread-1', false)).toBeNull();
  });

  it('fills a desktop back to a page of turns, then stops', () => {
    const w = createHistoryWindow();
    w.land(w.beginReload(), 'thread-1', pageFrom(76, true, 80));
    expect(w.backfill('thread-1', false)).toBe('commit');
    landOlder(w, 66, 75);
    expect(w.backfill('thread-1', false)).toBe('commit');
    landOlder(w, 61, 65);
    expect(w.backfill('thread-1', false)).toBeNull();
  });

  it('stops at the start of the thread', () => {
    const w = createHistoryWindow();
    w.land(w.beginReload(), 'thread-1', pageFrom(76, true, 80));
    landOlder(w, 70, 75, false);
    expect(w.backfill('thread-1', false)).toBeNull();
  });

  it('starts nothing while a page is on its way, a fork holds the top, or for another thread', () => {
    const w = createHistoryWindow();
    w.land(w.beginReload(), 'thread-1', pageFrom(76, true, 80));
    expect(w.backfill('thread-2', false)).toBeNull();
    const request = w.request('thread-1')!;
    expect(w.backfill('thread-1', false)).toBeNull();
    w.hold();
    expect(w.isCurrent(request.token)).toBe(false);
    expect(w.backfill('thread-1', false)).toBeNull();
    w.release();
    expect(w.backfill('thread-1', false)).toBe('commit');
  });

  it('gives a phone one page, then keeps the next one held ahead for the trigger', () => {
    const w = createHistoryWindow();
    w.land(w.beginReload(), 'thread-1', pageFrom(78, true, 80));
    expect(w.backfill('thread-1', true)).toBe('commit');
    landOlder(w, 75, 77);
    expect(w.backfill('thread-1', true)).toBe('ahead');

    const request = w.request('thread-1')!;
    expect(request.beforeTurn).toBe(75);
    const ahead = olderPage(72, 74);
    w.keepAhead(request.token, ahead);
    expect(w.snapshot()).toEqual({ status: 'idle', aligned: true, hasAhead: true });
    expect(w.backfill('thread-1', true)).toBeNull();

    // The trigger takes it, once, and its landing moves the start. Taking it
    // tells subscribers, with the status idle throughout, so the backfill
    // asks again for the next.
    let calls = 0;
    w.subscribe(() => {
      calls += 1;
    });
    const taken = w.takeAhead('thread-1')!;
    expect(taken.older).toBe(ahead);
    expect(calls).toBe(1);
    expect(w.snapshot()).toEqual({ status: 'idle', aligned: true, hasAhead: false });
    expect(w.takeAhead('thread-1')).toBeNull();
    w.land(taken.token, 'thread-1', taken.older.page);
    expect(w.request('thread-1')).toMatchObject({ beforeTurn: 72 });
  });

  it('drops a page held ahead at a reload, a thread switch, a fork, or a page asked for over it', () => {
    const holding = () => {
      const w = createHistoryWindow();
      w.land(w.beginReload(), 'thread-1', pageFrom(78, true, 80));
      landOlder(w, 75, 77);
      const request = w.request('thread-1')!;
      w.keepAhead(request.token, olderPage(72, 74));
      return w;
    };
    const reloaded = holding();
    reloaded.beginReload();
    expect(reloaded.takeAhead('thread-1')).toBeNull();

    const left = holding();
    left.reset();
    expect(left.takeAhead('thread-1')).toBeNull();

    const forked = holding();
    forked.hold();
    forked.release();
    expect(forked.takeAhead('thread-1')).toBeNull();

    const asked = holding();
    asked.request('thread-1');
    expect(asked.takeAhead('thread-1')).toBeNull();
  });

  it('refuses to hold a page something overtook on its way', () => {
    const w = createHistoryWindow();
    w.land(w.beginReload(), 'thread-1', pageFrom(78, true, 80));
    landOlder(w, 75, 77);
    const request = w.request('thread-1')!;
    const reload = w.beginReload();
    w.keepAhead(request.token, olderPage(72, 74));
    w.land(reload, 'thread-1', pageFrom(78, true, 80));
    expect(w.takeAhead('thread-1')).toBeNull();
  });
});

describe('bottom-aligning a thread view', () => {
  afterEach(() => scrollMemory.clear());

  it('is decided by the first load of a thread, kept across its reloads, and decided again after a switch', () => {
    const w = createHistoryWindow();
    let calls = 0;
    w.subscribe(() => {
      calls += 1;
    });
    w.land(w.beginReload(), 'thread-1', pageFrom(76, true, 80));
    expect(w.snapshot().aligned).toBe(true);
    // A reload that now holds the whole thread does not move it to the top.
    w.land(w.beginReload(), 'thread-1', pageFrom(0, false, 80));
    expect(w.snapshot().aligned).toBe(true);

    calls = 0;
    w.reset();
    expect(w.snapshot().aligned).toBe(false);
    expect(calls).toBe(1);
    w.land(w.beginReload(), 'thread-2', pageFrom(0, false, 3));
    expect(w.snapshot().aligned).toBe(false);
    // And the other way round: a thread that opened whole stays at the top.
    w.land(w.beginReload(), 'thread-2', pageFrom(2, true, 30));
    expect(w.snapshot().aligned).toBe(false);
  });
});
