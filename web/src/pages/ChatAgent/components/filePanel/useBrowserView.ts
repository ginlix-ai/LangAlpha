import { useCallback, useEffect, useMemo, useState, useSyncExternalStore } from 'react';
import { isPageUnseen, onPageUnseenChange } from '@/lib/pageVisibility';
import type { Computer } from '@/types/api';
import { useThisMac } from '../../hooks/useRemoteControl';
import { getBrowserView, navigateBrowser, setBrowserControl } from '../../utils/api';
import { BrowserViewSession, type BrowserViewState } from './browserStream';

/**
 * One view per workspace, held by whatever hosts the panel rather than by the
 * panel: the panel swaps workspaces under one mount, and a view must carry only
 * its own browser's page, caption and visit. A visit or the agent's activity can
 * reach a view before any panel shows it.
 */
export interface BrowserViews {
  viewFor: (workspaceId: string) => BrowserViewSession;
  /** Every view made so far. */
  each: () => Iterable<BrowserViewSession>;
}

export function useBrowserViews(): BrowserViews {
  const [views] = useState(() => new Map<string, BrowserViewSession>());
  const viewFor = useCallback((workspaceId: string) => {
    let view = views.get(workspaceId);
    if (!view) {
      view = new BrowserViewSession();
      views.set(workspaceId, view);
    }
    return view;
  }, [views]);
  const each = useCallback(() => views.values(), [views]);
  return useMemo(() => ({ viewFor, each }), [viewFor, each]);
}

/**
 * Watches a workspace's view while someone is looking, on the computer it is on:
 * a workspace moved to another one starts the watch over there, rather than
 * keep the moved-from browser's stream beside holds placed on the new one. The stream runs only while
 * the tab is in front of a reader: encoding frames held a one-CPU sandbox at 60 to
 * 80 percent of a core, so a hidden page, a tab in the background or a closed panel
 * closes the socket, and coming back opens another. The person's hold on the page
 * goes with it: looking away hands it back, since a hold nobody is watching only
 * keeps the agent out. The desktop app on the Mac the workspace is on watches it
 * there, past the server (`BrowserServer.local`).
 */
export function useBrowserView(
  view: BrowserViewSession | null,
  workspaceId: string,
  /** The computer the workspace is on, from the workspace, which every reader has. */
  computerId: string | null,
  /** Its row, which only its owner is sent: what tells this Mac apart. */
  computer: Computer | null | undefined,
  tabInFront: boolean,
): void {
  const pageHidden = useSyncExternalStore(onPageUnseenChange, isPageUnseen);
  const watching = tabInFront && !pageHidden && !!workspaceId;
  const thisMac = useThisMac({ enabled: computer?.kind === 'ssh', poll: false });
  const local = !!computer && thisMac.of(computer) !== null;

  useEffect(() => {
    if (!watching || !view) return;
    return view.watch({
      ask: (want) => getBrowserView(workspaceId, want),
      control: (holder, action, releases) => setBrowserControl(workspaceId, holder, action, releases),
      navigate: (holder, navigation) => navigateBrowser(workspaceId, holder, navigation),
      local,
    });
  }, [view, watching, workspaceId, computerId, local]);
}

export function useBrowserViewState(view: BrowserViewSession): BrowserViewState {
  return useSyncExternalStore(view.subscribe, view.getSnapshot);
}
