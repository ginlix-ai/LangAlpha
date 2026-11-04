import { useCallback, useMemo, useState } from 'react';
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
   * The file a tab's frame is on (workspace-relative): the one `opened`, or
   * another its links led to, as the layer last heard it. The toolbar counts
   * and sends that file's comments, as the layer shows them.
   */
  docOf: (tabId: string, opened: string) => string;
  showDoc: (tabId: string, opened: string, doc: string) => void;
  /** Where each comment's send stands: the chat's store beside it, so the deck and the cards agree. */
  sends: CommentSendStore;
}

export function useCommentUi(): CommentUi {
  const [modeTab, setModeTab] = useState<string | null>(null);
  const [listTab, setListTab] = useState<string | null>(null);
  const [focus, setFocus] = useState<CommentFocus | null>(null);
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
    () => ({ modeTab, setMode, listTab, setList, focus, requestFocus, focusHandled, docOf, showDoc, sends }),
    [modeTab, setMode, listTab, setList, focus, requestFocus, focusHandled, docOf, showDoc, sends],
  );
}
