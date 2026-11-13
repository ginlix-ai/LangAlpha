/**
 * The desktop shell's notifications and dock badge, driven from the user feed.
 *
 * Mounted only by the shell's menu bar page, the one page that posts them
 * (the main window never does, so two windows cannot post the same alert),
 * beside the feed that page holds open.
 *
 * - A background run's start posts a silent notification, which its result
 *   replaces in place (same id: the thread's). A run whose end brings no
 *   result (cancelled, superseded, reported elsewhere) takes it down itself.
 * - A run's end the server decided to tell the user about posts its alert.
 * - A thread read anywhere takes its notification down.
 * - The dock badge and attention list follow the store, debounced.
 * - "Mark as read" on a notification stamps the thread seen.
 */
import { useEffect, type ReactElement } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import i18n from '@/i18n';
import {
  canNotify,
  clearDesktopNotifications,
  desktopNotify,
  desktopSetAttention,
  onNotificationAction,
} from '@/lib/desktop';
import { queryKeys } from '@/lib/queryKeys';
import { threadRowsOf } from '@/lib/threadListCache';
import { subscribeFeedNotifications } from '@/lib/threadLifecycle/feedClient';
import { markThreadSeen } from '@/lib/threadLifecycle/seen';
import {
  getEffectiveObservation,
  getSeenThroughSeq,
  getThreadAttention,
  subscribeThreadLifecycle,
  TERMINAL_FAMILY,
} from '@/lib/threadLifecycle/store';
import { threadDetailQuery } from '@/pages/ChatAgent/utils/threadQueries';
import type { Thread } from '@/types/api';
import {
  ATTENTION_LIST_MAX,
  buildAttention,
  fate,
  readFeedNotification,
  startBehindResult,
  startedDeadline,
  subscribeThreadTitles,
  toDesktopNotification,
  type PostedNotification,
  type ThreadView,
} from './notifications';

const ATTENTION_DEBOUNCE_MS = 250;
/** Delivery is at least once; this many recent deliveries are remembered to drop a repeat. */
const DELIVERED_MEMORY = 200;

function threadView(threadId: string): ThreadView {
  const eff = getEffectiveObservation(threadId);
  return {
    known: eff !== undefined,
    seenThrough: getSeenThroughSeq(threadId),
    runSeq: eff?.runSeq,
    status: eff?.status,
    running: getThreadAttention().running.has(threadId),
    needsInput: getThreadAttention().needsInput.has(threadId),
  };
}

/** A thread's title from whatever the cache already holds: its detail, or a row in any list. */
function cachedTitle(qc: QueryClient, threadId: string): string | undefined {
  const detail = qc.getQueryData<Thread>(queryKeys.threads.detail(threadId));
  if (detail?.title) return detail.title;
  for (const [, data] of qc.getQueriesData<unknown>({ queryKey: queryKeys.threads.all })) {
    const title = threadRowsOf(data).find((row) => row?.thread_id === threadId)?.title;
    if (title) return title;
  }
  return undefined;
}

function startNotifier(qc: QueryClient): () => void {
  const posted = new Map<string, PostedNotification>();
  const titles = new Map<string, string>();
  const delivered = new Set<string>();
  const lookedUp = new Set<string>();
  let previousUnseen: ReadonlySet<string> = getThreadAttention().unseen;
  let attentionTimer: ReturnType<typeof setTimeout> | null = null;
  let wakeTimer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  let lastAttention = '';

  const titleOf = (threadId: string): string =>
    cachedTitle(qc, threadId) ?? titles.get(threadId) ?? i18n.t('desktopApp.untitledThread');

  /** Read the titles the attention list is missing, once each, then list again. */
  const lookUpTitles = (ids: readonly string[]) => {
    for (const id of ids) {
      if (lookedUp.has(id) || titles.has(id) || cachedTitle(qc, id)) continue;
      lookedUp.add(id);
      qc.fetchQuery(threadDetailQuery(id)).then(
        () => {
          if (!stopped) scheduleAttention();
        },
        () => {
          // Unreadable (deleted, no access): the list keeps the generic name.
        },
      );
    }
  };

  const pushAttention = () => {
    attentionTimer = null;
    if (stopped) return;
    const attention = buildAttention(getThreadAttention(), titleOf);
    // Thread lists refetch often; the shell hears only a list that changed.
    const sent = JSON.stringify(attention);
    if (sent !== lastAttention) {
      lastAttention = sent;
      desktopSetAttention(attention);
    }
    lookUpTitles(attention.threads.slice(0, ATTENTION_LIST_MAX).map((th) => th.id));
  };

  function scheduleAttention(): void {
    if (attentionTimer !== null || stopped) return;
    attentionTimer = setTimeout(pushAttention, ATTENTION_DEBOUNCE_MS);
  }

  /** Take down what no longer applies: notifications this page posted, and
   *  any thread that went from unread to read (one posted before a reload).
   *  The only place a notification comes down. */
  function reconcile(): void {
    if (stopped) return;
    const now = Date.now();
    const spent: string[] = [];
    let wakeAt = Infinity;
    for (const [threadId, p] of posted) {
      const view = threadView(threadId);
      // Posted before the store had its thread: known from here, so its later
      // removal still takes the notice down.
      if (view.known) p.known = true;
      p.clearAt = startedDeadline(p, view, now);
      if (fate(p, view, now) === 'clear') {
        posted.delete(threadId);
        spent.push(threadId);
      } else if (p.clearAt !== null) {
        wakeAt = Math.min(wakeAt, p.clearAt);
      }
    }
    const { unseen, running, needsInput } = getThreadAttention();
    if (unseen !== previousUnseen) {
      for (const threadId of previousUnseen) {
        if (unseen.has(threadId) || running.has(threadId) || needsInput.has(threadId)) continue;
        if (!spent.includes(threadId) && !posted.has(threadId)) spent.push(threadId);
      }
      previousUnseen = unseen;
    }
    clearDesktopNotifications(spent);
    if (wakeTimer !== null) clearTimeout(wakeTimer);
    wakeTimer = wakeAt === Infinity ? null : setTimeout(reconcile, wakeAt - now);
  }

  const offFeed = subscribeFeedNotifications((raw) => {
    const n = readFeedNotification(raw);
    if (!n) return;
    const key = `${n.threadId}:${n.runId ?? ''}:${n.kind}`;
    if (delivered.has(key)) return;
    delivered.add(key);
    if (delivered.size > DELIVERED_MEMORY) delivered.delete(delivered.values().next().value as string);
    if (n.title) titles.set(n.threadId, n.title);
    const view = threadView(n.threadId);
    // The server tells only of a thread's newest run, so one about an older
    // run (delivered late, after the next began) would replace that run's news.
    if (n.runSeq !== null && view.runSeq !== undefined && view.runSeq > n.runSeq) return;
    if (startBehindResult(n, posted.get(n.threadId))) return;
    const record: PostedNotification = { kind: n.kind, runSeq: n.runSeq, runId: n.runId, known: view.known, clearAt: null };
    // Read already (a quiet check that carried the cursor, or a finish the
    // user watched in another window), or a start for a run already cancelled
    // or overtaken (a late delivery): nothing to tell.
    if (fate(record, view, Date.now()) === 'clear') return;
    // Whatever was up for the thread is replaced in place.
    posted.set(n.threadId, record);
    void desktopNotify(toDesktopNotification(n));
    reconcile();
    scheduleAttention();
  });
  const offStore = subscribeThreadLifecycle(() => {
    reconcile();
    scheduleAttention();
  });
  const offTitles = subscribeThreadTitles(qc, scheduleAttention);
  const offAction = onNotificationAction(({ id, action }) => {
    if (action !== 'mark-read' || !id) return;
    // An explicit act, from a window nobody is looking at: unguarded on purpose.
    // The run the notice told of, which a late feed delivery can leave the
    // store behind; the server refuses one that is not over.
    const p = posted.get(id);
    const eff = getEffectiveObservation(id);
    const runId = p?.runId ?? (eff?.runId && TERMINAL_FAMILY.has(eff.status) ? eff.runId : null);
    if (runId) markThreadSeen(id, runId);
    if (p) p.dismissed = true;
    reconcile();
  });

  scheduleAttention();

  return () => {
    stopped = true;
    if (attentionTimer !== null) clearTimeout(attentionTimer);
    if (wakeTimer !== null) clearTimeout(wakeTimer);
    offFeed();
    offStore();
    offAction();
    offTitles();
    // Signed out, or the page is going: no badge, and no notices, for an
    // account not shown (a click on one would open its thread for the next).
    desktopSetAttention({ count: 0, threads: [] });
    clearDesktopNotifications([...posted.keys()]);
  };
}

function ActiveNotifier(): null {
  const queryClient = useQueryClient();
  useEffect(() => startNotifier(queryClient), [queryClient]);
  return null;
}

/** Renders nothing; does nothing where the shell cannot post notifications. */
export function DesktopNotifier(): ReactElement | null {
  return canNotify() ? <ActiveNotifier /> : null;
}
