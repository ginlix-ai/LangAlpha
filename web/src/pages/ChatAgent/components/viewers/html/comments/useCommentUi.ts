import { useCallback, useMemo, useRef, useState } from 'react';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useSharedCommentSends, type CommentSendStore } from '../../../artifactComments/commentSendStore';

/**
 * A chat line asked to show this comment, on the file at `path`
 * (workspace-relative). `key` tells two asks for one comment apart.
 */
export interface CommentFocus {
  path: string;
  commentId: string;
  key: number;
}

/** A viewer's layer that opens a box on words in it: which words are in it, and the opening. */
export interface WordsTaker {
  holds: (range: Range) => boolean;
  take: (range: Range) => void;
}

/** Words a page's chip asks about in the chat, with where they show on screen, in client coordinates. */
export interface AskedWords {
  text: string;
  rect: { left: number; top: number; bottom: number };
  /** The document they are in, workspace-relative, where a link moved the frame off the file the tab opened. */
  path?: string;
}

/**
 * The comment controls an HTML file's tab shares between its toolbar, in the
 * crumb row, and its layer, over the frame: whether clicks pick spots,
 * whether the list is open, a comment the chat asked to show, the file the
 * frame is on, and the sends on their way. Held by the panel, per tab id, and
 * never stored with the tabs: a strip restored tomorrow should not open in
 * comment mode, and an ask is consumed when it is shown.
 */
export interface CommentUi {
  /** The tab whose clicks pick spots; one file is in front at a time. */
  modeTab: string | null;
  setMode: (tabId: string, on: boolean) => void;
  listTab: string | null;
  setList: (tabId: string, open: boolean) => void;
  focus: CommentFocus | null;
  requestFocus: (focus: CommentFocus) => void;
  /** The layer showed the ask with this key. */
  focusHandled: (key: number) => void;
  /**
   * The selection toolbar's Comment on words a viewer in the app draws: the
   * layer over that tab's viewer opens a box on them. Only a layer whose
   * viewer takes words registers to (`registerWordsTaker`).
   */
  takesWords: (tabId: string, range: Range) => boolean;
  commentOnWords: (tabId: string, range: Range) => void;
  registerWordsTaker: (tabId: string, taker: WordsTaker) => () => void;
  /** Quotes words a page's chip offers into the chat; null where there is no chat to ask in. */
  askInChat: ((words: AskedWords) => void) | null;
  /**
   * The file a tab's frame is on (workspace-relative): the one `opened`, or
   * another its links led to, as the layer last heard it. The toolbar counts
   * and sends that file's comments, as the layer shows them.
   */
  docOf: (tabId: string, opened: string) => string;
  showDoc: (tabId: string, opened: string, doc: string) => void;
  /** Where each comment's send stands: the chat's store beside it, so the deck and the cards agree. */
  sends: CommentSendStore;
}

export function useCommentUi(onAskInChat?: ((words: AskedWords) => void) | null): CommentUi {
  const [modeTab, setModeTab] = useState<string | null>(null);
  const [listTab, setListTab] = useState<string | null>(null);
  const [focus, setFocus] = useState<CommentFocus | null>(null);
  // Called, not stored as state: an ask re-renders nothing. Which tabs take
  // words is state, since the toolbar is drawn by it.
  const takers = useRef(new Map<string, WordsTaker>());
  const [wordTabs, setWordTabs] = useState<ReadonlySet<string>>(() => new Set());
  const [docs, setDocs] = useState<ReadonlyMap<string, { opened: string; doc: string }>>(() => new Map());
  const sends = useSharedCommentSends();

  const setMode = useCallback((tabId: string, on: boolean) => {
    setModeTab((prev) => (on ? tabId : prev === tabId ? null : prev));
  }, []);
  const setList = useCallback((tabId: string, open: boolean) => {
    setListTab((prev) => (open ? tabId : prev === tabId ? null : prev));
  }, []);
  const requestFocus = useCallback((next: CommentFocus) => setFocus(next), []);
  const focusHandled = useCallback((key: number) => {
    setFocus((prev) => (prev?.key === key ? null : prev));
  }, []);
  const takesWords = useCallback(
    (tabId: string, range: Range) => wordTabs.has(tabId) && !!takers.current.get(tabId)?.holds(range),
    [wordTabs],
  );
  const commentOnWords = useCallback((tabId: string, range: Range) => takers.current.get(tabId)?.take(range), []);
  const registerWordsTaker = useCallback((tabId: string, taker: WordsTaker) => {
    takers.current.set(tabId, taker);
    setWordTabs((prev) => (prev.has(tabId) ? prev : new Set(prev).add(tabId)));
    return () => {
      if (takers.current.get(tabId) !== taker) return;
      takers.current.delete(tabId);
      setWordTabs((prev) => {
        const next = new Set(prev);
        return next.delete(tabId) ? next : prev;
      });
    };
  }, []);
  const ask = useStableHandler((words: AskedWords) => onAskInChat?.(words));
  const askInChat = onAskInChat ? ask : null;
  // Keyed by what the tab opened too, so a tab that opens another file starts on it.
  const docOf = useCallback((tabId: string, opened: string) => {
    const shown = docs.get(tabId);
    return shown?.opened === opened ? shown.doc : opened;
  }, [docs]);
  const showDoc = useCallback((tabId: string, opened: string, doc: string) => {
    setDocs((prev) => {
      const was = prev.get(tabId);
      if (was?.opened === opened && was.doc === doc) return prev;
      return new Map(prev).set(tabId, { opened, doc });
    });
  }, []);

  return useMemo(
    () => ({
      modeTab, setMode, listTab, setList, focus, requestFocus, focusHandled, takesWords, commentOnWords, registerWordsTaker, askInChat, docOf,
      showDoc, sends,
    }),
    [
      modeTab, setMode, listTab, setList, focus, requestFocus, focusHandled, takesWords, commentOnWords, registerWordsTaker, askInChat, docOf,
      showDoc, sends,
    ],
  );
}
