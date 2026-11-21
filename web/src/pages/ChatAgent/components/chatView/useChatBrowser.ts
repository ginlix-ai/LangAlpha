import { useCallback, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import type { Computer } from '@/types/api';
import type { RouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import type { BrowserUse } from '../../session/stream/browserUse';
import { toastBrowserRefusal } from '../filePanel/browserRefusal';
import type { BrowserViewSession } from '../filePanel/browserStream';
import { PREVIEW_MAX_RATIO } from '../filePanel/detailWidth';
import type { UnsequencedTarget } from '../filePanel/types';
import { hasBrowser } from '../../utils/computerStatus';
import { useBrowserViews } from '../filePanel/useBrowserView';

const WIDE = { maxRatio: PREVIEW_MAX_RATIO };

/**
 * The chat's side of its computer's browser. The views live here, one per
 * workspace, so what the agent does and a link the reader opens reach the chat's
 * own view directly, whatever the panel shows; the panel is asked only to bring
 * the tab up, which is the one thing it owns.
 */
export function useChatBrowser({
  workspaceId,
  threadId,
  computer,
  shared,
  browserOn,
  isMobile,
  panelOpen,
  filePanelWorkspaceId,
  setFilePanelWorkspaceId,
  askPanel,
  land,
  leave,
}: {
  workspaceId: string;
  threadId: string | null;
  /** The chat's computer; unknown while it loads. */
  computer: Computer | null;
  /** The chat's workspace is shared with the reader, whose computer they are not sent. */
  shared: boolean;
  /** The reader turned the browser on in Settings. */
  browserOn: boolean;
  isMobile: boolean;
  /** The file panel is open, so a tab comes up beside what it shows. */
  panelOpen: boolean;
  /** The sibling the panel shows in place of the chat's own workspace. */
  filePanelWorkspaceId: string | null;
  setFilePanelWorkspaceId: (workspaceId: string | null) => void;
  /** Asks an open panel without opening it or changing its size. */
  askPanel: (target: UnsequencedTarget) => void;
  /** Opens the panel on the target, at the size asked for. */
  land: (target: UnsequencedTarget, opts?: { maxRatio?: number }) => void;
  /** The panel's leave guard, for a switch that drops a sibling's drafts. */
  leave: RouteLeaveGuard;
}): {
  viewFor: (workspaceId: string) => BrowserViewSession;
  handleBrowserUse: (use: BrowserUse) => void;
  /** Null where there is no browser to open a link in: on a phone, where the
   *  panel would cover the chat, or on a computer without one. */
  openInBrowser: ((url: string) => void) | null;
} {
  const { t } = useTranslation();
  const { viewFor, each: eachView } = useBrowserViews();
  const browserHere = browserOn && hasBrowser(computer, shared);
  const showingSibling = !!filePanelWorkspaceId && filePanelWorkspaceId !== workspaceId;

  // The first use in a thread brings the tab up, on the panel it opens or beside
  // what an open panel is showing; after that the reader's arrangement stands.
  // Not on mobile, where the panel would cover the chat, and not over a sibling's
  // files, whose browser may be another computer's.
  const browserSeenInThread = useRef<string | null>(null);
  const handleBrowserUse = useCallback((use: BrowserUse) => {
    if (!workspaceId) return;
    const own = viewFor(workspaceId);
    own.agentUsed(use);
    // One Chrome serves a computer, and a sibling the panel shows may share it:
    // its view looks again, which costs one status read where it does not.
    for (const view of eachView()) if (view !== own) view.poke();
    if (!browserOn || isMobile || showingSibling || (computer && !hasBrowser(computer))) return;
    const thread = threadId ?? '';
    if (browserSeenInThread.current === thread) return;
    browserSeenInThread.current = thread;
    if (panelOpen) askPanel({ kind: 'browser', front: false });
    else land({ kind: 'browser', front: true }, WIDE);
  }, [workspaceId, viewFor, eachView, browserOn, isMobile, showingSibling, computer, threadId, panelOpen, askPanel, land]);

  // A link the reader asked to open in the computer's browser: the panel comes
  // back to this workspace's files, whose browser it is, with the tab in front,
  // and the chat's own view goes there, never a sibling's the panel was showing.
  // The tab says why a browser could not be opened; the rest is a toast.
  const openInBrowser = useCallback((url: string) => {
    const go = () => {
      if (showingSibling) setFilePanelWorkspaceId(null);
      land({ kind: 'browser', front: true }, WIDE);
      void viewFor(workspaceId).visit(url).then((refusal) => {
        if (refusal && refusal !== 'not_running') toastBrowserRefusal(t, refusal);
      });
    };
    if (showingSibling) leave(go); else go();
  }, [showingSibling, setFilePanelWorkspaceId, land, viewFor, workspaceId, t, leave]);

  return {
    viewFor,
    handleBrowserUse,
    openInBrowser: !isMobile && workspaceId && browserHere ? openInBrowser : null,
  };
}
