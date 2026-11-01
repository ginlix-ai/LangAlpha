import React, { Suspense, useCallback, useRef, useState } from 'react';
import { preloadableLazy } from '@/lib/lazyWithMotion';

const PublishDialog = preloadableLazy(() => import('../PublishDialog'));

interface Publishing {
  workspaceId: string;
  path: string;
  /** Kept through the close, so the dialog's exit plays. */
  open: boolean;
}

/**
 * The Publish dialog a transcript's file cards open: one for the whole chat,
 * its code fetched on the first Publish rather than carried by the chat's.
 * The dialog opens once its code is in, so it never draws through a fallback.
 * `onPublishFile` resolves when the dialog closes, which a card's deck waits
 * on to hold still under it as it does under its own menu.
 */
export function usePublishFile(ownWorkspaceId: string | null): {
  onPublishFile: (path: string, workspaceId?: string) => Promise<void>;
  dialog: React.ReactNode;
} {
  const [publishing, setPublishing] = useState<Publishing | null>(null);
  const closedRef = useRef<(() => void) | null>(null);

  const onPublishFile = useCallback(async (path: string, workspaceId?: string) => {
    const target = workspaceId ?? ownWorkspaceId;
    if (!target) return;
    await PublishDialog.preload();
    closedRef.current?.();
    setPublishing({ workspaceId: target, path, open: true });
    await new Promise<void>((resolve) => { closedRef.current = resolve; });
  }, [ownWorkspaceId]);

  const close = useCallback(() => {
    setPublishing((shown) => (shown ? { ...shown, open: false } : shown));
    closedRef.current?.();
    closedRef.current = null;
  }, []);

  const dialog = publishing && (
    <Suspense fallback={null}>
      <PublishDialog
        key={`${publishing.workspaceId}/${publishing.path}`}
        open={publishing.open}
        workspaceId={publishing.workspaceId}
        path={publishing.path}
        onClose={close}
      />
    </Suspense>
  );
  return { onPublishFile, dialog };
}
