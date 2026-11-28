import { useCallback, useRef, useState } from 'react';
import { computeAgentArtifactRouting, type ArtifactDestination, type PanelDestination } from '../../utils/agentArtifactRouting';
import type { ComputerFolders } from '../../utils/agentPaths';
import { isValidUuid } from '../../utils/uuid';
import { isOneShotKind, stampTarget, type PanelTarget, type UnsequencedTarget } from '../filePanel/types';
import type { FileLocation, OpenFileHandler } from '../../utils/fileLocation';
import type { RouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';

/**
 * The workspace override the file panel holds after a routed open; null shows
 * the view's own workspace. A view names a sibling's files only through a
 * link that names that workspace, so an open that names no other workspace
 * returns to its own.
 */
export function nextPanelOverride(named: string | null, ownWorkspaceId: string): string | null {
  return named && named !== ownWorkspaceId ? named : null;
}

/**
 * What a file panel host has asked the panel to show, and the guard the
 * panel's drafts leave through. Shared by the chat's panel and the gallery's,
 * so a link opens the same way beside either.
 */
export function usePanelAsks() {
  // Single source of truth for what the file panel is pointed at. Exactly one
  // target is ever set (file/preview/chart/tool/sources/memory/memo/status);
  // the panel opens or focuses the tab that owns `.kind`. Each self-clears once
  // consumed (the handled callbacks): on arrival for most kinds, once the entry
  // is selected for memory and memo.
  const [panelTarget, setPanelTarget] = useState<PanelTarget | null>(null);
  // Counts every ask, so the same folder, port or symbol asked for twice
  // arrives twice. See `PanelTarget`.
  const targetSeqRef = useRef(0);
  const askPanel = useCallback((target: UnsequencedTarget) => {
    setPanelTarget(stampTarget(target, ++targetSeqRef.current));
  }, []);
  // The asks so far, so an open that waits on a fetch can drop itself when a
  // newer ask landed first.
  const askCount = useCallback(() => targetSeqRef.current, []);
  // Stable handlers: these land in useEffect deps in MemoryPanel/MemoPanel/
  // FilePanel. Inline arrows would create a new identity on every host
  // render, re-triggering those effects on every streaming chunk (the
  // `targetKey == null` guard makes them no-ops, but the wakeup is wasted).
  // Each clears the target only if it is still the ask that was consumed. An
  // ask lands between the consumer's commit and its callback (the panel's
  // effect runs after render, the store bodies after a fetch), and a clear
  // by kind alone would drop that newer ask unread.
  const handleTargetHandled = useCallback((seq?: number) => setPanelTarget((pt) => (isOneShotKind(pt?.kind) && pt?.seq === seq ? null : pt)), []);
  const handleTargetMemoryHandled = useCallback((seq?: number) => setPanelTarget((pt) => (pt?.kind === 'memory' && pt.seq === seq ? null : pt)), []);
  const handleTargetMemoHandled = useCallback((seq?: number) => setPanelTarget((pt) => (pt?.kind === 'memo' && pt.seq === seq ? null : pt)), []);

  // The file panel holds its drafts in memory, and closing the panel unmounts
  // it. The exits the host owns go through the panel's own leave guard, the
  // one its close button uses, so they ask in the same dialog and judge the
  // draft as typed rather than as last rendered. A ref, not state: nothing
  // here renders on it. No guard means no panel, and so no draft to lose.
  const filesLeaveGuardRef = useRef<RouteLeaveGuard | null>(null);
  const handleFilesLeaveGuardChange = useCallback((guard: RouteLeaveGuard | null) => {
    filesLeaveGuardRef.current = guard;
  }, []);
  const leaveFiles = useCallback<RouteLeaveGuard>((go) => {
    const guard = filesLeaveGuardRef.current;
    if (guard) guard(go); else go();
  }, []);

  return {
    panelTarget,
    askPanel,
    askCount,
    handleTargetHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    handleFilesLeaveGuardChange,
    leaveFiles,
  };
}

/** What a click adds to the target it opens: where in a file, kept in a tab of its own, a comment to bring forward. */
interface PanelClick {
  location?: FileLocation | null;
  pin?: boolean;
  comment?: string;
}

export type PanelOpen = (dest: ArtifactDestination, click?: PanelClick) => void;

function clicked(target: PanelDestination, { location, pin, comment }: PanelClick): UnsequencedTarget {
  if (target.kind === 'drive') return { ...target, pin: !!pin };
  if (target.kind === 'file' && 'path' in target) {
    return { ...target, location: location ?? null, pin: !!pin, ...(comment && { comment }) };
  }
  return target;
}

/**
 * Opens a destination in the panel, in the workspace it names: a sibling's
 * file opens in the panel in place of the view's own (`nextPanelOverride`),
 * and anything that names no workspace returns the panel to the view's own.
 * The result becomes the one panel target, which replaces whatever ask was
 * pending.
 */
export function usePanelOpen({ workspaceId, override, setOverride, land, leave }: {
  /** The view's own workspace. */
  workspaceId: string;
  /** The sibling the panel shows in place of the view's own (usePanelFiles). */
  override: string | null;
  setOverride: (workspaceId: string | null) => void;
  /** Points the panel at the target and opens it. */
  land: (target: UnsequencedTarget) => void;
  /** The panel's leave guard (usePanelAsks). */
  leave: RouteLeaveGuard;
}): PanelOpen {
  return useCallback((dest, click = {}) => {
    if (dest.workspaceId && !isValidUuid(dest.workspaceId)) {
      console.warn('[FilePanel] ignoring artifact ref with invalid workspace id', dest.workspaceId);
      return;
    }
    const target = clicked(dest.target, click);
    // Another workspace's strip replaces this one's, drafts included. Only a
    // change in the workspace the panel shows does that; clearing an unset
    // override keeps the strip.
    const shown = (id: string | null) => id || workspaceId;
    const nextOverride = nextPanelOverride(dest.workspaceId, workspaceId);
    const switching = shown(nextOverride) !== shown(override);
    const go = () => {
      if (nextOverride !== override) setOverride(nextOverride);
      land(target);
    };
    if (switching) leave(go); else go();
  }, [land, setOverride, leave, workspaceId, override]);
}

/**
 * Routes a click on an agent artifact to the panel tab that owns its domain,
 * in the workspace the link names, through `open` (usePanelOpen). The pure
 * decision is computed by computeAgentArtifactRouting; `folders` are the
 * view's, which a path naming a sibling by its folder is read against.
 */
export function usePanelLinkOpen(open: PanelOpen, folders: ComputerFolders | null): OpenFileHandler {
  return useCallback<OpenFileHandler>((rawPath, targetWorkspaceId, location, opts) => {
    open(computeAgentArtifactRouting(rawPath, targetWorkspaceId, folders), { location, pin: opts?.pin, comment: opts?.comment });
  }, [open, folders]);
}
