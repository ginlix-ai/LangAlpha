import React, { Suspense, useCallback } from 'react';
import type { OpenFileHandler } from '../../utils/fileLocation';
import type { MarketWatchState } from '../../session/marketWatchEvents';
import type { PanelTarget } from './types';

const MemoryPanel = React.lazy(() => import('../MemoryPanel'));
const MemoPanel = React.lazy(() => import('../MemoPanel'));
const StatusPanel = React.lazy(() => import('../StatusPanel'));

interface StoreTabProps {
  kind: 'memory' | 'memo' | 'status';
  workspaceId: string;
  /** Read for the entry a memory or memo target names; the body clears it once selected. */
  target: PanelTarget | null;
  onTargetMemoryHandled?: (seq?: number) => void;
  onTargetMemoHandled?: (seq?: number) => void;
  onOpenFile: OpenFileHandler | null;
  marketWatch: MarketWatchState | null;
  /** The thread whose wakeups the status tab lists, and whether its chat is in front. */
  threadId: string | null;
  isActive: boolean;
}

/**
 * The body of a memory, memo or status tab: the store's own panel, or the
 * live watch and the thread's wakeups. A memory or memo target is not consumed on arrival like a
 * file's: the body selects the entry once the store's list resolves, and says
 * so then, naming the ask it had so a later one is not cleared with it.
 */
export function StoreTab({ kind, workspaceId, target, onTargetMemoryHandled, onTargetMemoHandled, onOpenFile, marketWatch, threadId, isActive }: StoreTabProps): React.ReactElement {
  const onOpen = onOpenFile ?? undefined;
  // Stable per ask: the bodies keep these in effect deps.
  const seq = target?.seq;
  const memoryHandled = useCallback(() => onTargetMemoryHandled?.(seq), [onTargetMemoryHandled, seq]);
  const memoHandled = useCallback(() => onTargetMemoHandled?.(seq), [onTargetMemoHandled, seq]);
  const body = (): React.ReactNode => {
    switch (kind) {
      case 'memory':
        return (
          <MemoryPanel
            workspaceId={workspaceId}
            targetKey={target?.kind === 'memory' ? target.key : null}
            targetTier={target?.kind === 'memory' ? target.tier : null}
            onTargetHandled={memoryHandled}
            onOpenFile={onOpen}
          />
        );
      case 'memo':
        return (
          <MemoPanel
            targetKey={target?.kind === 'memo' ? target.key : null}
            onTargetHandled={memoHandled}
            onOpenFile={onOpen}
          />
        );
      case 'status':
        return <StatusPanel marketWatch={marketWatch} threadId={threadId} active={isActive} />;
      default:
        return kind satisfies never;
    }
  };
  return <Suspense fallback={null}>{body()}</Suspense>;
}
