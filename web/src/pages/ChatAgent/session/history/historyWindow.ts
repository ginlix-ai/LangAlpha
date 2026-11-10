/**
 * Which turns a paged transcript holds, and whether it can page back further.
 *
 * A thread opens on its newest turns, as many as a page's bytes allow, and
 * older pages go above them as the reader scrolls up, or behind first paint
 * when that page stopped short on its bytes (`backfill`). The one jump to a
 * turn that is not on screen, a navigation naming it (`focusTurn`, the command
 * palette's search hit), pages back to it the same way, in older pages of a
 * bounded size until one reaches it (useFocusTurn), so the transcript still
 * runs unbroken from its
 * oldest loaded turn to the newest; the minimap, sources, files and tool links
 * all point at loaded bubbles, and a reload's place is a loaded bubble the
 * reload's floor keeps. The window holds the page the newest
 * load or the last older page reported, the one page in flight or held ahead,
 * and the token that refuses the landing of a page a reload, a thread switch
 * or a fork overtook.
 */
import { scrollMemory } from '@/lib/scrollMemory';
import type { HistoryCarry, LoadedHistoryPage, OlderHistoryPage } from './olderPages';
import {
  HISTORY_PAGE_TURNS,
  HISTORY_RELOAD_MARGIN_TURNS,
  newestPageReach,
  pageBudget,
  type PageBudget,
  type ScreenBudget,
} from './pageBudget';

/**
 * What the top of the transcript can do: `reloading` while the newest load
 * rebuilds it, `held` while a fork cuts it, `idle` with older turns to ask
 * for, `loading` while a page is on its way, `failed` after one did not
 * arrive, and `end` at the start of the thread or before anything loaded.
 */
export type OlderHistoryStatus = 'reloading' | 'held' | 'idle' | 'loading' | 'failed' | 'end';

/** An older page the window let start, and what it lands or fails with. */
export interface OlderPageRequest {
  beforeTurn: number;
  carry: HistoryCarry;
  signal: AbortSignal;
  token: number;
}

// Beside the scroll offset useChatScroll saves under `thread:<id>`: the turn
// the transcript started at, which that offset is measured from, so a return
// to the thread starts there again; and the reach, which the return keeps, so
// the margin a reload added above it is not taken as reach and asked again.
const startKey = (threadId: string) => `thread-start:${threadId}`;
const reachKey = (threadId: string) => `thread-reach:${threadId}`;

/**
 * What the window owes a transcript whose newest load stopped short of a
 * page's turns on its bytes, once the reader can see it: `commit` an older
 * page through the reader's hold, as the scroll trigger would, or replay the
 * next one `ahead` without showing it, ready for the trigger.
 */
export type BackfillStep = 'commit' | 'ahead' | null;

/**
 * What the view renders from the window: the top's status, whether the
 * transcript sits at the bottom of a screen it does not fill, and whether a
 * page is held ahead, whose taking re-arms the backfill (useHistoryBackfill).
 * A new object only when a field changes, as `useSyncExternalStore` needs.
 */
export interface HistoryWindowSnapshot {
  readonly status: OlderHistoryStatus;
  readonly aligned: boolean;
  readonly hasAhead: boolean;
}

export function createHistoryWindow() {
  let threadId: string | null = null;
  let page: LoadedHistoryPage | null = null;
  // The oldest turn the reader paged in, which a reload reaches back to. The
  // reload's page can start up to the margin before it; taken as the reach,
  // that margin would be asked for again on top of itself at every reload.
  let reach: number | null = null;
  // Turns the newest load holds, and turns loaded since it: its own, then
  // each older page's. A newest load short of a page's turns stopped on its
  // bytes, so the depth a page used to give is filled in behind it.
  let newestTurns = 0;
  let span = 0;
  // Whether this thread view opened on a page with older turns behind it, so
  // a transcript shorter than the screen sits at its bottom and a page landing
  // above it never moves the newest reply. Decided by the first load of the
  // thread in this view: a reload, which may hold more or fewer turns, keeps
  // it, so the transcript never jumps between the two.
  let aligned = false;
  // An older page replayed but not shown (a phone keeps the next one ready).
  // Whatever overtakes the window drops it, as it drops a page in flight.
  let ahead: OlderHistoryPage | null = null;
  let base: Exclude<OlderHistoryStatus, 'held'> = 'end';
  let held = false;
  let token = 0;
  let inflight: AbortController | null = null;
  let shown: HistoryWindowSnapshot = Object.freeze({ status: 'end', aligned: false, hasAhead: false });
  const listeners = new Set<() => void>();

  const publish = () => {
    const status = held && (base === 'idle' || base === 'failed') ? 'held' : base;
    const hasAhead = ahead !== null;
    if (status === shown.status && aligned === shown.aligned && hasAhead === shown.hasAhead) return;
    shown = Object.freeze({ status, aligned, hasAhead });
    for (const listener of listeners) listener();
  };
  // Stops the page in flight and refuses its landing, and drops a page held
  // ahead, which was replayed against the transcript this overtakes.
  const overtake = () => {
    inflight?.abort();
    inflight = null;
    ahead = null;
    token += 1;
  };

  return {
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    snapshot: (): HistoryWindowSnapshot => shown,

    /** The page for the newest load of `tid`: one page's worth on this screen,
     *  or for a reload or a return to a thread left mid-way, every turn back
     *  to where it reached, whatever their bytes. */
    newestPageBudget(tid: string, newestTurn: number, screen: ScreenBudget): PageBudget {
      if (tid === threadId) {
        return pageBudget(screen, newestPageReach(reach, newestTurn, HISTORY_RELOAD_MARGIN_TURNS));
      }
      const start = scrollMemory.get(startKey(tid));
      const resumed = typeof start === 'number' && typeof scrollMemory.get(`thread:${tid}`) === 'number';
      return pageBudget(screen, newestPageReach(resumed ? start : null, newestTurn));
    },
    /** A newest load starts: no older page goes on a transcript it rebuilds. */
    beginReload(): number {
      overtake();
      base = 'reloading';
      publish();
      return token;
    },
    /** Where the transcript starts now, from the load or page `t` named. */
    land(t: number, tid: string, next: LoadedHistoryPage | null): void {
      if (t !== token) return;
      const opened = tid !== threadId;
      const newest = opened || base === 'reloading';
      const saved = scrollMemory.get(reachKey(tid));
      const prior = opened ? (typeof saved === 'number' ? saved : null) : newest ? reach : null;
      inflight = null;
      threadId = tid;
      page = next;
      // A reload of this thread, or a return to it, keeps the reach while its
      // page holds that turn. A branch cut short of it elsewhere starts the
      // reach again where the page starts.
      reach = prior !== null && next && next.firstTurnIndex <= prior && prior <= next.lastTurnIndex
        ? prior
        : (next?.firstTurnIndex ?? null);
      if (next && reach !== null) {
        scrollMemory.set(startKey(tid), next.firstTurnIndex);
        scrollMemory.set(reachKey(tid), reach);
      }
      const turns = next ? Math.max(0, next.lastTurnIndex - next.firstTurnIndex + 1) : 0;
      if (newest) newestTurns = span = turns;
      else span += turns;
      if (opened) aligned = !!next?.hasMore;
      base = next?.hasMore ? 'idle' : 'end';
      publish();
    },
    /** Starts the page before the oldest loaded one, or null when none may start. */
    request(tid: string): OlderPageRequest | null {
      if (held || tid !== threadId || !page?.hasMore) return null;
      if (base !== 'idle' && base !== 'failed') return null;
      overtake();
      inflight = new AbortController();
      base = 'loading';
      publish();
      return { beforeTurn: page.firstTurnIndex, carry: page.carry, signal: inflight.signal, token };
    },
    isCurrent: (t: number): boolean => t === token,
    fail(t: number): void {
      if (t !== token) return;
      inflight = null;
      base = 'failed';
      publish();
    },
    /** What a cut-short load is still owed, on a phone or not; null when
     *  nothing is, or when nothing may start now. */
    backfill(tid: string, phone: boolean): BackfillStep {
      if (held || tid !== threadId || !page?.hasMore || base !== 'idle') return null;
      if (newestTurns >= HISTORY_PAGE_TURNS) return null;
      // A desktop fills back to a page's turns; a phone takes one page, and
      // then only keeps the next one ready, as its link and screen are small.
      if (phone ? span === newestTurns : span < HISTORY_PAGE_TURNS) return 'commit';
      return phone && !ahead ? 'ahead' : null;
    },
    /** The page `t` replayed lands held, not shown: the top is idle again, and
     *  the next ask for an older page takes it. */
    keepAhead(t: number, older: OlderHistoryPage): void {
      if (t !== token) return;
      inflight = null;
      ahead = older;
      base = 'idle';
      publish();
    },
    /** The page held ahead, once, with the token its landing names; null when
     *  none is, or when no page may go on the transcript now. */
    takeAhead(tid: string): { older: OlderHistoryPage; token: number } | null {
      if (!ahead || held || tid !== threadId || base !== 'idle') return null;
      const older = ahead;
      ahead = null;
      publish();
      return { older, token };
    },
    /** A fork cuts the transcript at an index, so nothing goes above the cut
     *  until it is made, and a page held ahead of it may be gone with it. */
    hold(): void {
      if (base === 'loading') {
        overtake();
        base = 'idle';
      }
      ahead = null;
      held = true;
      publish();
    },
    release(): void {
      held = false;
      publish();
    },
    /** The thread was left: its window, and its page in flight, go with it. */
    reset(): void {
      overtake();
      threadId = null;
      page = null;
      reach = null;
      newestTurns = 0;
      span = 0;
      aligned = false;
      base = 'end';
      held = false;
      publish();
    },
  };
}

export type HistoryWindow = ReturnType<typeof createHistoryWindow>;
