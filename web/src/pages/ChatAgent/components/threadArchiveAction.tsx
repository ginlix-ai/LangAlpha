import React, { useCallback, useState } from 'react';
import { isThreadRunning } from '@/lib/threadLifecycle/store';
import ArchiveThreadConfirmDialog from './ArchiveThreadConfirmDialog';

export interface ThreadArchiveConfirm {
  /**
   * Archive `threadId` — immediately when nothing is running, else after the
   * user confirms. `archive` carries whatever the host's archive path needs
   * (cache patch, navigate-away, PATCH), so every trigger keeps its own.
   */
  requestArchive: (threadId: string, archive: () => void) => void;
  /** Render once at the host's root. */
  dialog: React.ReactNode;
}

/**
 * The single confirm choke point for archiving a thread. Archiving stays
 * allowed while a run is live (product call: the run survives, the row just
 * leaves the lists), so the gate is a confirm, not a block — and it exists
 * because both triggers are one-click hover affordances with no undo in view.
 *
 * Liveness comes from the thread lifecycle store, the frontend's only
 * authority on run status — read at click time, since a hook subscription
 * can't be taken per row from a shared handler.
 */
export function useArchiveThreadConfirm(): ThreadArchiveConfirm {
  const [pending, setPending] = useState<{ threadId: string; archive: () => void } | null>(null);

  const requestArchive = useCallback((threadId: string, archive: () => void) => {
    if (!isThreadRunning(threadId)) {
      archive();
      return;
    }
    setPending({ threadId, archive });
  }, []);

  const handleConfirm = useCallback(() => {
    if (!pending) return;
    pending.archive();
    setPending(null);
  }, [pending]);

  const handleCancel = useCallback(() => setPending(null), []);

  return {
    requestArchive,
    dialog: (
      <ArchiveThreadConfirmDialog
        open={!!pending}
        onCancel={handleCancel}
        onConfirm={handleConfirm}
      />
    ),
  };
}
