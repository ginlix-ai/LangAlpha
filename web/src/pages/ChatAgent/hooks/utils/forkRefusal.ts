/**
 * An edit or regenerate the server refuses at admission: the thread is busy,
 * a concurrent request claimed the turn, or the branch moved after this view
 * read it (`stale_fork`). The route refuses with a 409, and once the stream is
 * open the refusal arrives as an SSE error frame. Either way the server
 * truncated nothing, so the view puts back what the fork cut and reloads the
 * thread. An answer to a pause a fork elsewhere replaced is refused as
 * `stale_fork` too, and recovers the same way, as does a Retry refused because
 * the chat changed, its reply never failed, or its input has to be sent again.
 */
import type { ChatMessage } from '@/types/chat';
import type { StructuredError } from '@/utils/rateLimitError';
import type { EditDraft } from './editDrafts';

/**
 * The codes a request is refused with before the server kept anything of it,
 * as a route 409 or as a stream's frame. `duplicate_request` is not one: that
 * request was admitted, and the view adopts its run instead.
 */
const REFUSAL_CODES: ReadonlySet<string> = new Set([
  'running',
  'stopping',
  'compacting',
  'not_running',
  'request_cancelled',
  'stale_retry',
  'not_retryable',
  'attempt_conflict',
  'turn_conflict',
  'stale_fork',
  'resend_required',
  'writer_capacity',
]);

function readForkRefusal(source: unknown): { code: unknown; message: string } | null {
  if (!source || typeof source !== 'object') return null;
  const { errorInfo, error_type: errorType, error, code } = source as {
    errorInfo?: Record<string, unknown>;
    error_type?: unknown;
    error?: unknown;
    code?: unknown;
  };
  if (typeof errorInfo?.code === 'string' && REFUSAL_CODES.has(errorInfo.code)) {
    return typeof errorInfo.message === 'string' ? { code: errorInfo.code, message: errorInfo.message } : null;
  }
  // A frame says `admission_conflict` for most refusals, but not for one at
  // capacity, so its code decides.
  const refused = errorType === 'admission_conflict' || (typeof code === 'string' && REFUSAL_CODES.has(code));
  return refused && typeof error === 'string' ? { code, message: error } : null;
}

/**
 * The server's message when `source`, a refused request's error or an SSE
 * error frame, is an admission refusal.
 */
export function forkRefusalMessage(source: unknown): string | null {
  return readForkRefusal(source)?.message ?? null;
}

// A fork's branch or a retry's run is no longer the thread's newest: the chat
// changed after this view read it.
const isStaleCode = (code: unknown): boolean => code === 'stale_fork' || code === 'stale_retry';

/** Refused because a reply in the chat was running or stopping, or the chat
 *  was compacting. */
export type BusyCode = 'running' | 'stopping' | 'compacting';

/**
 * The view's words for a busy refusal, by its code. The server's name the
 * CLI's commands, and say the turn is still running when the reload that
 * follows may find it over.
 */
export const BUSY_REFUSAL_KEYS: Readonly<Record<BusyCode, string>> = {
  running: 'chat.busyRunning',
  stopping: 'chat.busyStopping',
  compacting: 'chat.busyCompacting',
};

/** The same, in the failed reply of a send the refusal stopped, whose Retry
 *  sends the message again. */
export const BUSY_REPLY_KEYS: Readonly<Record<BusyCode, string>> = {
  ...BUSY_REFUSAL_KEYS,
  running: 'chat.busyRunningSend',
};

export const isBusyCode = (code: unknown): code is BusyCode =>
  typeof code === 'string' && Object.hasOwn(BUSY_REFUSAL_KEYS, code);

/** Whether `source` refuses its request because the chat changed after this
 *  view read it. */
export function isStaleForkRefusal(source: unknown): boolean {
  return isStaleCode(readForkRefusal(source)?.code);
}

/** Whether `source` refuses a Retry in a way the view recovers from: the chat
 *  changed, the reply was never failed, or only the user can send its input. */
export function isRecoverableRetryRefusal(source: unknown): boolean {
  const code = readForkRefusal(source)?.code;
  return isStaleCode(code) || code === 'not_retryable' || code === 'resend_required';
}

/** What the user did that cut the transcript, and what a refusal gives back. */
export interface ForkCut {
  /** `resume` is an answer to a pause, which cuts nothing but its own reply,
   *  and `retry` replaces a failed reply. */
  kind: 'edit' | 'regenerate' | 'resume' | 'retry';
  /** The transcript before the cut. An automatic resend of the fork carries
   *  it too, since the one it was handed is already cut. */
  transcript: readonly ChatMessage[];
  /** What an edit was typed with; null for a regenerate. */
  draft: EditDraft | null;
  /** The edit boxes open on the messages the cut removed. */
  dropped?: readonly EditDraft[];
  /** The text of the message that opened the turn, which goes back to the
   *  composer when the server needs the turn's input sent again. */
  asked?: string | null;
}

export interface ForkRefusalRecovery {
  /** Recovers from `source` when it is an admission refusal; false otherwise. */
  recover: (source: unknown, cut: ForkCut) => boolean;
  /** Recovers a fork this view found the chat changed under before sending
   *  it, as one the server refused as stale. */
  stale: (cut: ForkCut) => void;
  /** Runs when a thread reload shows its transcript; `live` when it shows a
   *  run in progress, which it then follows. */
  reveal: (ok: boolean, live: boolean) => void;
}

export function createForkRefusalRecovery(deps: {
  setMessages: (messages: ChatMessage[]) => void;
  setMessageError: (error: string | StructuredError) => void;
  /** Reloads the thread once the fork's stream has let go of it. */
  requestReload: () => void;
  /** Moves on a new turn and on a thread switch. */
  sessionEpoch: () => number;
  /** The view's own words for a fork the chat changed under. */
  staleMessage: (kind: ForkCut['kind']) => string | StructuredError;
  /** Gives an edit's text back to its box, which outlives the reload; the
   *  box takes the focus unless `focus` is false. */
  returnDraft: (draft: EditDraft, focus?: boolean) => void;
  /** The view's own words for a request refused because the chat was busy. */
  busyMessage: (code: BusyCode) => string | StructuredError;
  /** The view's own words for a turn whose input the user has to send again;
   *  `givenBack` when its question went back to the composer. */
  resendMessage: (givenBack: boolean) => string | StructuredError;
  /** Puts text in the composer, beside what is typed there. */
  giveBack: (text: string) => void;
  /** Marks every bubble on screen as one the server holds, which a reload
   *  replaces. */
  markPersisted: () => void;
}): ForkRefusalRecovery {
  let notice: { message: string | StructuredError; epoch: number; busy: boolean } | null = null;
  const refuse = (
    message: string | StructuredError | null,
    cut: ForkCut,
    { persisted = false, busy = false }: { persisted?: boolean; busy?: boolean } = {},
  ) => {
    // The reload rebuilds only the bubbles a replay made, so the fork's own
    // would stay under it; the snapshot is also what a failed reload leaves.
    deps.setMessages([...cut.transcript]);
    if (persisted) deps.markPersisted();
    // A busy refusal waits for the reload, which alone knows if the turn it
    // names is still running. One still running says so itself.
    if (message !== null && !busy) deps.setMessageError(message);
    // The edit itself goes back last, so its box is the one with the focus.
    for (const draft of cut.dropped ?? []) deps.returnDraft(draft, false);
    if (cut.draft) deps.returnDraft(cut.draft);
    notice = message === null ? null : { message, epoch: deps.sessionEpoch(), busy };
    deps.requestReload();
  };
  return {
    recover(source, cut) {
      const refusal = readForkRefusal(source);
      if (refusal === null) return false;
      if (isStaleCode(refusal.code)) {
        refuse(deps.staleMessage(cut.kind), cut);
      } else if (refusal.code === 'not_retryable' && cut.kind === 'retry') {
        // The latest run did not fail, so the reply this view marked failed is
        // not the server's: the reload shows the one it holds in its place.
        refuse(null, cut, { persisted: true });
      } else if (refusal.code === 'resend_required') {
        // What the turn carried besides its text is kept nowhere, so the user
        // sends it again. The text goes back to the composer when the reply
        // sits right under it; a reply to an answered pause has none to give.
        if (cut.asked) deps.giveBack(cut.asked);
        refuse(deps.resendMessage(Boolean(cut.asked)), cut);
      } else if (isBusyCode(refusal.code)) {
        refuse(deps.busyMessage(refusal.code), cut, { busy: true });
      } else {
        refuse(refusal.message, cut);
      }
      return true;
    },
    stale(cut) {
      refuse(deps.staleMessage(cut.kind), cut);
    },
    reveal(ok, live) {
      // The reload's replay clears the banner. A failed reload shows its own
      // error, and after a new turn or on another thread this one is stale.
      if (ok && notice && notice.epoch === deps.sessionEpoch() && !(notice.busy && live)) {
        deps.setMessageError(notice.message);
      }
      notice = null;
    },
  };
}
