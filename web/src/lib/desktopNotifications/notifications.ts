/**
 * What the desktop shell is told about threads: the notification a feed event
 * becomes, when a posted one comes down, and the attention summary behind the
 * dock badge and dock menu. Pure, so the rules are pinned apart from the
 * timers and the bridge.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { AttentionThread, DesktopNotification, DesktopNotificationKind } from '@/lib/desktop';
import { queryKeys } from '@/lib/queryKeys';
import { TERMINAL_FAMILY, type PublicRunStatus } from '@/lib/threadLifecycle/store';

/** A notification the user feed delivered. */
export interface FeedNotification {
  threadId: string;
  runId: string | null;
  runSeq: number | null;
  kind: DesktopNotificationKind;
  /** Server-worded; empty when the server sent none. */
  title: string;
  body: string;
}

const KINDS: Record<DesktopNotificationKind, true> = {
  started: true,
  turn: true,
  needs_input: true,
  wakeup: true,
  automation: true,
};

const isKind = (value: unknown): value is DesktopNotificationKind => typeof value === 'string' && Object.hasOwn(KINDS, value);

/**
 * The notification a `thread_lifecycle` payload carries under `notification`,
 * or null. Two events carry one: a background run's `run_started` holds its
 * start, and a `notification` event is the server's decision that a run's end
 * is worth telling. Read loosely: a payload missing its thread or naming a kind
 * this build does not know is no notification, never an error.
 */
export function readFeedNotification(raw: Record<string, unknown>): FeedNotification | null {
  const threadId = raw.thread_id;
  if (typeof threadId !== 'string' || !threadId) return null;
  const isStart = raw.type === 'run_started';
  if (!isStart && raw.type !== 'notification') return null;
  const source = raw.notification;
  if (!source || typeof source !== 'object') return null;
  const { kind, title, body } = source as Record<string, unknown>;
  // A start rides its run_started, so only the start event may name one.
  if (!isKind(kind) || (kind === 'started') !== isStart) return null;
  return {
    threadId,
    runId: typeof raw.run_id === 'string' && raw.run_id ? raw.run_id : null,
    runSeq: typeof raw.run_seq === 'number' ? raw.run_seq : null,
    kind,
    title: typeof title === 'string' ? title : '',
    body: typeof body === 'string' ? body : '',
  };
}

/** The app path that opens a thread; the chat route finds its workspace from the id alone. */
export function threadPath(threadId: string): string {
  return `/chat/t/${encodeURIComponent(threadId)}`;
}

/** The server words every notification; this is only for one it sent without a title. */
const FALLBACK_TITLE = 'LangAlpha';

/**
 * One per thread (the id is the thread's), so a background run's start is
 * replaced by its result; the start is silent, since nothing has happened yet
 * that needs the user.
 */
export function toDesktopNotification(n: FeedNotification): DesktopNotification {
  return {
    id: n.threadId,
    kind: n.kind,
    title: n.title || FALLBACK_TITLE,
    body: n.body,
    path: threadPath(n.threadId),
    ...(n.kind === 'started' ? { silent: true } : {}),
  };
}

/** What a posted notification was about, to know when it no longer applies. */
export interface PostedNotification {
  kind: DesktopNotificationKind;
  runSeq: number | null;
  /** The run it tells of, which its "Mark as read" stamps: the store may not have reached it yet. */
  runId: string | null;
  /** Whether the store knew the thread when it was posted, so its going means deleted. */
  known: boolean;
  /** For a start whose run ended: when it comes down if no result replaced it. */
  clearAt: number | null;
  /** Its own "Mark as read" was pressed. */
  dismissed?: boolean;
}

/** The store's view of one thread, as far as clearing its notification needs. */
export interface ThreadView {
  /** Whether the store knows the thread at all; false once it was deleted or archived. */
  known: boolean;
  /** Newest run seq the user has seen. */
  seenThrough: number;
  /** The thread's newest run seq, when known. */
  runSeq: number | undefined;
  /** The status of that newest run, when known. */
  status: PublicRunStatus | undefined;
  /** Whether the store counts the thread as running. A snapshot that found the
   *  run over when its settle never arrived (a sleep, a dropped stream) clears
   *  this while `status` still says running. */
  running: boolean;
  needsInput: boolean;
}

/**
 * How long a start stays up after its run ended, so the result can still
 * replace it in place. The server holds a result back 8s to learn whether the
 * user watched the finish, then its outbox delivers it; past this, none is
 * coming.
 */
export const STARTED_NOTICE_GRACE_MS = 20_000;

/**
 * When a background run's start comes down on its own, or null to leave it up.
 *
 * Its result replaces it, but some ends bring none: a cancelled run, one a
 * newer run took over (the server tells only of a thread's latest run), and
 * one that reports elsewhere (an analyst's hand-off) or ran for a chat app.
 * So once the store shows the run over, the start stays only for the grace a
 * real result needs, counted from the first time it was seen over; a
 * cancelled or overtaken run gets no result, so it goes now.
 */
export function startedDeadline(posted: PostedNotification, view: ThreadView, now: number): number | null {
  if (posted.kind !== 'started' || posted.runSeq === null || view.runSeq === undefined) return null;
  if (view.runSeq > posted.runSeq) return now;
  // The store has not caught up to the run yet.
  if (view.runSeq < posted.runSeq) return null;
  if (view.status === 'cancelled') return now;
  const over = !view.running || (view.status && (TERMINAL_FAMILY.has(view.status) || view.status === 'interrupted'));
  if (over) {
    return posted.clearAt ?? now + STARTED_NOTICE_GRACE_MS;
  }
  return null;
}

/**
 * Whether a posted notification stays up. It comes down once the user read the
 * thread at or past the run it was about, answered the question it asked (a
 * newer run started, or that run is no longer waiting), the thread is gone,
 * its own Mark as read was pressed, or it is a start past its deadline.
 *
 * A store that has not caught up to the notified run (it can arrive just
 * before the run_settled it follows, while the store still has it running)
 * never clears it.
 */
export function fate(posted: PostedNotification, view: ThreadView, now: number): 'keep' | 'clear' {
  if (posted.dismissed || (posted.known && !view.known)) return 'clear';
  const seq = posted.runSeq;
  if (seq !== null) {
    if (view.seenThrough >= seq) return 'clear';
    if (posted.kind === 'needs_input' && view.runSeq !== undefined) {
      // A run the store still has running has not reached the interrupt yet:
      // the notice and the run_settled ride separate outbox jobs, either first.
      if (view.runSeq > seq || (view.runSeq === seq && !view.needsInput && !view.running)) return 'clear';
    }
  }
  const deadline = startedDeadline(posted, view, now);
  return deadline !== null && now >= deadline ? 'clear' : 'keep';
}

/**
 * A start delivered after its own run's result: the result already said more,
 * and replacing it would drop the run's news for a silent "running".
 */
export function startBehindResult(n: FeedNotification, up: PostedNotification | undefined): boolean {
  return n.kind === 'started' && up !== undefined && up.kind !== 'started' && n.runId !== null && up.runId === n.runId;
}

/** How many threads the attention list carries; the count covers them all. */
export const ATTENTION_LIST_MAX = 20;

/**
 * The dock badge and the threads behind it. The count is what wants the user
 * (unread plus waiting for input); running threads are listed after them, so
 * the dock menu can show what is underway without counting it.
 */
export function buildAttention(
  sets: { running: ReadonlySet<string>; needsInput: ReadonlySet<string>; unseen: ReadonlySet<string> },
  titleOf: (threadId: string) => string,
): { count: number; threads: AttentionThread[] } {
  const threads: AttentionThread[] = [];
  const add = (ids: ReadonlySet<string>, state: AttentionThread['state']) => {
    for (const id of ids) {
      if (threads.length >= ATTENTION_LIST_MAX) return;
      threads.push({ id, title: titleOf(id), path: threadPath(id), state });
    }
  };
  add(sets.needsInput, 'needs_input');
  add(sets.unseen, 'unread');
  add(sets.running, 'running');
  return { count: sets.unseen.size + sets.needsInput.size, threads };
}

/**
 * Calls `onChange` whenever cached thread data lands (a fetch or a patch, such
 * as a rename): the attention list's titles are read from that cache, and a
 * rename moves no lifecycle state that would otherwise list it again.
 */
export function subscribeThreadTitles(qc: QueryClient, onChange: () => void): () => void {
  const root = queryKeys.threads.all[0];
  return qc.getQueryCache().subscribe((event) => {
    if (event.type === 'updated' && event.action.type === 'success' && event.query.queryKey[0] === root) onChange();
  });
}
