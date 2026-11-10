/**
 * Paging older history in above the transcript: what the scroll trigger and
 * the backfill behind first paint run, against the window that says which
 * page may start and refuses one something overtook.
 */
import { flushSync } from 'react-dom';
import type { HistoryRuntime } from '../runtime';
import type { HistoryWindow } from './historyWindow';
import {
  commitOlderHistoryPage,
  loadOlderHistoryPage,
  type OlderHistoryPage,
  type OlderPageCommitDeps,
} from './olderPages';
import { currentScreenBudget, jumpPageBudget, pageBudget } from './pageBudget';

/** Holds the reader's place across a page put above them: handed the page's
 *  bubble count and the commit, which renders before it returns. */
export type HoldPlace = (prepended: number, commit: () => void) => void;

/** An older page replayed, with the token its landing names. */
interface FetchedPage {
  older: OlderHistoryPage;
  token: number;
}

export function createOlderHistory({
  window: historyWindow,
  runtime,
  threadId,
  ...commitDeps
}: {
  window: HistoryWindow;
  runtime: HistoryRuntime;
  /** The thread on screen now, read when a step starts. */
  threadId: () => string;
} & OlderPageCommitDeps) {
  /** Replays the page before the oldest one loaded, when the window lets one
   *  start, into a transcript of its own, reaching toward turn `reachTo` when
   *  one is named (a jump's page, which may take several to get there). Null
   *  when none started, or when a reload, a thread switch or a fork overtook
   *  it on its way. */
  const fetchOlderPage = async (tid: string, reachTo?: number): Promise<FetchedPage | null> => {
    const request = historyWindow.request(tid);
    if (!request) return null;
    const { beforeTurn, carry, signal, token } = request;
    const screen = currentScreenBudget();
    const page = reachTo === undefined ? pageBudget(screen) : jumpPageBudget(screen, Math.max(0, beforeTurn - reachTo));
    let older: OlderHistoryPage | null = null;
    try {
      older = await loadOlderHistoryPage(
        runtime,
        { threadId: tid, beforeTurn, page, signal },
        carry,
      );
    } catch (err) {
      console.warn('[History] Older page failed to load:', err);
    }
    if (!historyWindow.isCurrent(token)) return null;
    if (!older) {
      historyWindow.fail(token);
      return null;
    }
    return { older, token };
  };

  /** Puts a replayed page above the transcript, under `place`, in one task:
   *  no scroll can land between the place read and the page shown. Whether a
   *  live stream still writes a task, or has closed it, is read now, so a page
   *  held ahead is committed against the stream as it is, not as it was. */
  const commitOlderPage = (tid: string, { older, token }: FetchedPage, place?: HoldPlace) => {
    const commit = () => flushSync(() => commitOlderHistoryPage(runtime, commitDeps, older));
    if (place) place(older.messages.length, commit);
    else commit();
    historyWindow.land(token, tid, older.page);
  };

  /** Puts the page before the oldest one loaded above the transcript: the page
   *  held ahead at once, or one replayed now, reaching toward turn `reachTo`
   *  when a jump to it names one (useFocusTurn, which asks again until the
   *  turn is in). A page something overtook is dropped, and the trigger asks
   *  again. */
  const load = async (place?: HoldPlace, reachTo?: number) => {
    const tid = threadId();
    const page = historyWindow.takeAhead(tid) ?? await fetchOlderPage(tid, reachTo);
    if (page) commitOlderPage(tid, page, place);
  };

  /** One step of what a newest load cut short on its bytes is owed
   *  (historyWindow.backfill): a page put up under `place`, as the trigger
   *  would, or the next one replayed and held ahead. Each step moves the
   *  window's snapshot, the view's cue to ask for the next. */
  const backfill = async (place: HoldPlace | undefined, phone: boolean) => {
    const tid = threadId();
    const step = historyWindow.backfill(tid, phone);
    if (step === 'commit') return load(place);
    if (step === 'ahead') {
      const fetched = await fetchOlderPage(tid);
      if (fetched) historyWindow.keepAhead(fetched.token, fetched.older);
    }
  };

  return { load, backfill };
}
