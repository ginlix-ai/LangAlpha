/**
 * Where each comment's last send stands, by comment id: one store for every
 * place that sends comments or shows them waiting. The chat provides it
 * (`useChatCommentSends`), so the composer's deck, a file's toolbar, its
 * cards and the chat's own sends read the same marks; a view with no chat
 * (`/a/`, the gallery's panel) keeps its own. Each send has one writer: the
 * chat for what it carries, `sendBatches` for what a file view posts.
 *
 * The queue itself is the server's. A mark only bridges the moment between a
 * send and the next read of the lists: `sending` is on its way, held for as
 * long as its request is open; `taken` the server has; `refused` came back
 * with the reason the deck and the card say.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import type { ArtifactComment } from '@/types/api';

/**
 * How long a taken mark speaks for a comment after the server took it, when
 * no fresh list shows it moved, so a read that never comes cannot hold it out
 * of the queue forever.
 */
export const TAKEN_MS = 20_000;

/**
 * `at` is the comment's `updated_at` when it was sent: the mark speaks for
 * that version of its row only, so a list showing the row moved on retires
 * it. Null where the sender never saw the row (a resend by id), which holds
 * until it expires or is refused.
 */
export type CommentSendMark =
  | { phase: 'sending' | 'taken'; at: string | null }
  | { phase: 'refused'; reason: string };

/** A comment a mark is written for, with its row's version where the writer has it. */
export interface SendSubject {
  comment_id: string;
  updated_at?: string | null;
}

/**
 * The writes, stable for the store's life. Each keeps the version a mark
 * already holds for a writer that has none (the chat knows comments by id).
 */
export interface CommentSendWriter {
  /** On their way, or parked by the chat until it can send them: held, with no bound, until an answer or `release`. */
  sending: (comments: readonly SendSubject[]) => void;
  /** The server has them: held for {@link TAKEN_MS} from now, unless a list shows it first. */
  taken: (comments: readonly SendSubject[]) => void;
  refused: (commentIds: readonly string[], reason: string) => void;
  /** Kept as a note, or queued again: no send of theirs stands any more. */
  clear: (commentIds: readonly string[]) => void;
  /** Their request closed with no answer: those still on their way go back to what the lists say. True when any did. */
  release: (commentIds: readonly string[]) => boolean;
}

export interface CommentSendStore extends CommentSendWriter {
  get: (commentId: string) => CommentSendMark | undefined;
}

export type Marks = ReadonlyMap<string, CommentSendMark>;

export type MarkAction =
  | { type: 'sending' | 'taken'; comments: readonly SendSubject[] }
  | { type: 'refused'; ids: readonly string[]; reason: string }
  | { type: 'clear'; ids: readonly string[] }
  /** A request closed unanswered; only marks still on their way go. */
  | { type: 'release'; ids: readonly string[] }
  /** A taken mark's time ran out; a mark that moved on since stays. */
  | { type: 'expire'; id: string };

export function nextMarks(prev: Marks, action: MarkAction): Marks {
  const next = new Map(prev);
  switch (action.type) {
    case 'sending':
    case 'taken':
      for (const c of action.comments) {
        const was = prev.get(c.comment_id);
        const held = was && was.phase !== 'refused' ? was.at : null;
        next.set(c.comment_id, { phase: action.type, at: c.updated_at ?? held });
      }
      return next;
    case 'refused':
      for (const id of action.ids) next.set(id, { phase: 'refused', reason: action.reason });
      return next;
    case 'clear':
      for (const id of action.ids) next.delete(id);
      return next.size === prev.size ? prev : next;
    case 'release':
      for (const id of action.ids) if (prev.get(id)?.phase === 'sending') next.delete(id);
      return next.size === prev.size ? prev : next;
    case 'expire':
      if (prev.get(action.id)?.phase !== 'taken') return prev;
      next.delete(action.id);
      return next;
  }
}

type Row = Pick<ArtifactComment, 'updated_at' | 'delivery' | 'status'>;

/** The mark is on its way or taken, for the version of the row on screen. */
function inFlight(mark: CommentSendMark | undefined, row: Row): mark is Extract<CommentSendMark, { at: string | null }> {
  return !!mark && mark.phase !== 'refused' && (mark.at === null || mark.at === row.updated_at) && row.delivery !== 'sent';
}

/** A send has the comment, so the composer's next send must not carry it again. */
export function leftQueue(mark: CommentSendMark | undefined, row: Row): boolean {
  return inFlight(mark, row);
}

/** Where a send stands for one comment, as its pin and card draw it (`describeComment`). */
export interface SendLook {
  /** Drawn as sent before the server's row says so: the server flips it on the same request. */
  sending: boolean;
  /** A queued comment whose send is on its way, which its Send now button shows. */
  pending: boolean;
  /** Why its last send was refused, while it still waits. */
  refusal: string | null;
}

/**
 * A queued comment shows its send on its button until the server has it; a
 * note has no button, so it draws as sent from the click, since a pin that
 * showed it a note for that moment would read as refused.
 */
export function sendLook(mark: CommentSendMark | undefined, row: Row): SendLook {
  const out = inFlight(mark, row);
  return {
    sending: out && (mark.phase === 'taken' || row.delivery === 'note'),
    pending: out && mark.phase === 'sending' && row.delivery === 'queued',
    refusal: mark?.phase === 'refused' && row.status === 'open' && row.delivery !== 'sent' ? mark.reason : null,
  };
}

const NO_MARKS: Marks = new Map();

export function useCommentSendStore(): CommentSendStore {
  const [marks, setMarks] = useState<Marks>(NO_MARKS);
  // The marks as of the last write, for the writes, which run outside render;
  // `marks` is the same map as the render saw it.
  const latest = useRef<Marks>(NO_MARKS);
  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());
  // The latest taken write per comment, so only its own timer expires it.
  const takenAt = useRef(new Map<string, number>());
  const serial = useRef(0);
  useEffect(() => {
    const all = timers.current;
    return () => {
      for (const timer of all) clearTimeout(timer);
      all.clear();
    };
  }, []);

  const later = useCallback((run: () => void, ms: number): void => {
    const timer = setTimeout(() => {
      timers.current.delete(timer);
      run();
    }, ms);
    timers.current.add(timer);
  }, []);

  const apply = useCallback((action: MarkAction): boolean => {
    const prev = latest.current;
    const next = nextMarks(prev, action);
    if (next === prev) return false;
    latest.current = next;
    setMarks(next);
    return true;
  }, []);

  const writer = useMemo<CommentSendWriter>(() => ({
    sending: (comments) => { if (comments.length) apply({ type: 'sending', comments }); },
    taken: (comments) => {
      if (comments.length === 0) return;
      apply({ type: 'taken', comments });
      for (const { comment_id: id } of comments) {
        const mine = ++serial.current;
        takenAt.current.set(id, mine);
        later(() => {
          if (takenAt.current.get(id) !== mine) return;
          takenAt.current.delete(id);
          apply({ type: 'expire', id });
        }, TAKEN_MS);
      }
    },
    refused: (ids, reason) => { if (ids.length) apply({ type: 'refused', ids, reason }); },
    clear: (ids) => { if (ids.length) apply({ type: 'clear', ids }); },
    release: (ids) => ids.length > 0 && apply({ type: 'release', ids }),
  }), [apply, later]);

  return useMemo(() => ({ ...writer, get: (id: string) => marks.get(id) }), [writer, marks]);
}

/** The chat's store, for every view beside it. */
export const CommentSendsContext = createContext<CommentSendStore | null>(null);

/** The chat's store where one sits beside this view, else a store of the view's own. */
export function useSharedCommentSends(): CommentSendStore {
  const shared = useContext(CommentSendsContext);
  const own = useCommentSendStore();
  return shared ?? own;
}
