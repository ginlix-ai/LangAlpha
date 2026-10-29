import { useEffect, useRef } from 'react';
import type { PanelTarget } from './types';
import type { FileTabsApi } from './useFileTabs';
import type { PreviewsApi } from './usePreviews';
import type { useFileRefOpen } from './useFileRefOpen';

interface PanelTargetArgs {
  target: PanelTarget | null;
  tabs: FileTabsApi;
  previews: PreviewsApi;
  openFileRef: ReturnType<typeof useFileRefOpen>['openFileRef'];
  /** Drops a reference still resolving, so it cannot land over the tab this ask opens. */
  cancelPending: () => void;
  clearSearch: () => void;
  setTreeOpen: (open: boolean) => void;
  setScopeDir: (dir: string | null) => void;
  /** Called with the ask's `seq` for a kind consumed on arrival (`ONE_SHOT_KINDS`);
   *  a memory or memo target is cleared by its tab's body instead, once it has the entry. */
  onTargetHandled?: (seq?: number) => void;
}

/**
 * What the chat asked the panel to show, acted on as it arrives. A folder
 * from chat points the tree, which stays on screen beside the open file, so
 * nothing has to be closed to honour it. A parent that leaves the target in
 * place re-runs this on every render of it, so the ask is keyed: a target
 * that only shed its path must not re-open a tree the reader has since
 * folded away, only a new ask does.
 */
export function usePanelTarget({
  target, tabs, previews, openFileRef, cancelPending, clearSearch, setTreeOpen, setScopeDir, onTargetHandled,
}: PanelTargetArgs): void {
  const lastDirAsk = useRef<string | null>(null);
  useEffect(() => {
    if (!target) return;
    switch (target.kind) {
      case 'file': {
        if (target.dir != null) {
          setScopeDir(target.dir);
          const ask = `${target.seq ?? 0}:${target.dir}`;
          if (lastDirAsk.current !== ask) {
            lastDirAsk.current = ask;
            clearSearch();
            setTreeOpen(true);
            // The tree only shows beside a listing tab; a folder asked for
            // over a chart or a tool result has to move off it first. A path
            // in the same ask lands on a file tab of its own.
            if (!target.path) tabs.showListing();
          }
        }
        if (target.path) void openFileRef(target.path, { location: target.location ?? null, pin: !!target.pin });
        onTargetHandled?.(target.seq);
        return;
      }
      case 'preview':
        // The agent published an app: its tab, and a URL fresh enough to load.
        cancelPending();
        tabs.openPreview(target);
        previews.open(target);
        onTargetHandled?.(target.seq);
        return;
      case 'chart':
        cancelPending();
        tabs.openChart(target);
        onTargetHandled?.(target.seq);
        return;
      case 'tool':
        cancelPending();
        tabs.openTool(target);
        onTargetHandled?.(target.seq);
        return;
      case 'sources':
        cancelPending();
        tabs.openSources(target.messageId);
        onTargetHandled?.(target.seq);
        return;
      case 'status':
        cancelPending();
        tabs.openStatus();
        onTargetHandled?.(target.seq);
        return;
      // The entry a memory or memo target names is read by the tab's body,
      // which clears the target once its list has resolved and selected it.
      case 'memory':
        cancelPending();
        tabs.openMemory();
        return;
      case 'memo':
        cancelPending();
        tabs.openMemo();
        return;
      default:
        return target satisfies never;
    }
  }, [target]); // eslint-disable-line react-hooks/exhaustive-deps
}
