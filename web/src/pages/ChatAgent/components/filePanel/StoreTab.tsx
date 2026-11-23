import React, { Suspense, useCallback } from 'react';
import type { OpenFileHandler } from '../../utils/fileLocation';
import type { PanelTarget } from './types';
import { withCopy } from '@/i18n';

const MemoryPanel = React.lazy(() => import('../MemoryPanel'));
const MemoPanel = React.lazy(withCopy(() => import('../MemoPanel'), ['memoPanel']));
const StatusPanel = React.lazy(withCopy(() => import('../StatusPanel'), ['automation']));

interface StoreTabProps {
  kind: 'memory' | 'memo' | 'status';
  workspaceId: string;
  /** Read for the entry a memory or memo target names; the body clears it once selected. */
  target: PanelTarget | null;
  onTargetMemoryHandled?: (seq?: number) => void;
  onTargetMemoHandled?: (seq?: number) => void;
  onOpenFile: OpenFileHandler | null;
}

/**
 * The body of a memory, memo or status tab: the store's own panel, or the
 * chat's thread status. A memory or memo target is not consumed on arrival
 * like a file's: the body selects the entry once the store's list resolves, and says
 * so then, naming the ask it had so a later one is not cleared with it.
 */
export function StoreTab({ kind, workspaceId, target, onTargetMemoryHandled, onTargetMemoHandled, onOpenFile }: StoreTabProps): React.ReactElement {
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
        return <StatusPanel />;
      default:
        return kind satisfies never;
    }
  };
  return <Suspense fallback={null}>{body()}</Suspense>;
}
