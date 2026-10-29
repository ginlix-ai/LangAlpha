/**
 * The edit boxes a chat has open, held by the session rather than by the
 * bubbles. A reload rebuilds the transcript and remounts every bubble, so text
 * kept in a bubble went with it; kept here, an edit reopens on its message
 * once the reload shows it again. A draft over a stored message is found by
 * its thread, its turn and the text it was typed over, never by a bubble id:
 * replayed ids repeat from thread to thread.
 */
import { useState, useSyncExternalStore } from 'react';

/** The message a box is open over. */
export interface EditDraftKey {
  /** Null over a message the server never stored, which has no turn yet. */
  turnIndex: number | null;
  /** The message's text when the box opened, which finds a stored message again. */
  original: string;
  /** Finds a message with no turn, whose text another unstored send may
   *  repeat. Its id stays until the server stores it, reloads included. */
  messageId?: string;
}

export interface EditDraft extends EditDraftKey {
  threadId: string;
  text: string;
}

/** Whether two keys name the same message of a thread. */
export function sameDraftMessage(a: EditDraftKey, b: EditDraftKey): boolean {
  if (a.turnIndex !== b.turnIndex) return false;
  // A replayed message holds the text the server stored, which a send may
  // have trimmed.
  if (a.turnIndex !== null) return a.original.trim() === b.original.trim();
  return a.messageId !== undefined && a.messageId === b.messageId;
}

export interface EditDraftStore {
  /** The text of the open box over this message of the shown thread. */
  get: (key: EditDraftKey) => string | undefined;
  /** Opens a box or rewrites its text; without a thread, on the shown one. */
  put: (draft: Omit<EditDraft, 'threadId'> & { threadId?: string }) => void;
  drop: (key: EditDraftKey, threadId?: string) => void;
  /** The open boxes of a thread. */
  of: (threadId: string) => EditDraft[];
  /** Has the box over this message take the focus when it next shows: it
   *  held the focus when a reload took it away, or its edit came back. */
  markFocus: (key: EditDraftKey, threadId?: string) => void;
  /** Whether the box over this message is to take the focus; asking clears it. */
  takeFocus: (key: EditDraftKey) => boolean;
  subscribe: (listener: () => void) => () => void;
}

export function createEditDraftStore(shownThread: () => string | null): EditDraftStore {
  // Thread, then turn, then the boxes over that turn's messages.
  const byThread = new Map<string, Map<number | null, EditDraft[]>>();
  let focus: (EditDraftKey & { threadId: string }) | null = null;
  const listeners = new Set<() => void>();
  const notify = () => {
    for (const listener of listeners) listener();
  };
  const find = (threadId: string | null, key: EditDraftKey) => {
    const drafts = threadId === null ? undefined : byThread.get(threadId)?.get(key.turnIndex);
    if (!drafts) return { drafts, at: -1 };
    return { drafts, at: drafts.findIndex((draft) => sameDraftMessage(draft, key)) };
  };
  return {
    get(key) {
      const { drafts, at } = find(shownThread(), key);
      return at === -1 ? undefined : drafts![at].text;
    },
    put({ threadId = shownThread() ?? undefined, turnIndex, original, messageId, text }) {
      if (threadId === undefined) return;
      const draft: EditDraft = { threadId, turnIndex, original: original.trim(), messageId, text };
      const { drafts, at } = find(threadId, draft);
      if (at !== -1) {
        if (drafts![at].text === text) return;
        drafts![at] = draft;
      } else if (drafts) {
        drafts.push(draft);
      } else {
        let turns = byThread.get(threadId);
        if (!turns) byThread.set(threadId, (turns = new Map()));
        turns.set(turnIndex, [draft]);
      }
      notify();
    },
    drop(key, threadId = shownThread() ?? undefined) {
      if (threadId === undefined) return;
      const { drafts, at } = find(threadId, key);
      if (at === -1) return;
      drafts!.splice(at, 1);
      if (drafts!.length === 0) byThread.get(threadId)!.delete(key.turnIndex);
      notify();
    },
    of(threadId) {
      return [...(byThread.get(threadId)?.values() ?? [])].flat();
    },
    markFocus({ turnIndex, original, messageId }, threadId = shownThread() ?? undefined) {
      focus = threadId === undefined ? null : { threadId, turnIndex, original, messageId };
    },
    takeFocus(key) {
      const asked = focus !== null && focus.threadId === shownThread() && sameDraftMessage(focus, key);
      if (asked) focus = null;
      return asked;
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}

/**
 * The store a bubble keeps its edit in: its host's, or one of its own where
 * the host provides none, which keeps the edit only while the bubble lives.
 */
export function useEditDraftStore(hostStore: EditDraftStore | undefined): EditDraftStore {
  const [own] = useState(() => createEditDraftStore(() => ''));
  return hostStore ?? own;
}

/** The text of the open box over a message, read from `store`. */
export function useEditDraft(store: EditDraftStore, key: EditDraftKey | null): string | undefined {
  return useSyncExternalStore(store.subscribe, () => (key === null ? undefined : store.get(key)));
}
