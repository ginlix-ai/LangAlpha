import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import DeleteConfirmModal from './DeleteConfirmModal';

export interface ThreadDeleteConfirm {
  /**
   * Ask before deleting the thread titled `title`. `remove` runs on confirm;
   * a rejection keeps the dialog open with the error, and success closes it.
   */
  requestDelete: (title: string, remove: () => Promise<unknown>) => void;
  /** Render once at the host's root. */
  dialog: React.ReactNode;
}

/** The one confirm every thread delete goes through (gallery card, sidebar menu). */
export function useThreadDeleteConfirm(): ThreadDeleteConfirm {
  const { t } = useTranslation();
  const [pending, setPending] = useState<{ title: string; remove: () => Promise<unknown> } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const requestDelete = useCallback((title: string, remove: () => Promise<unknown>) => {
    setPending({ title, remove });
    setError(null);
  }, []);

  const handleConfirm = async () => {
    if (!pending || busy) return;
    setBusy(true);
    setError(null);
    try {
      await pending.remove();
      setPending(null);
    } catch (err) {
      console.error('Error deleting thread:', err);
      const detail = (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
      setError(
        (typeof detail === 'string' && detail)
          || (err instanceof Error && err.message)
          || t('thread.failedDeleteThread'),
      );
    } finally {
      setBusy(false);
    }
  };

  return {
    requestDelete,
    dialog: (
      <DeleteConfirmModal
        isOpen={!!pending}
        workspaceName={pending?.title ?? ''}
        onConfirm={() => void handleConfirm()}
        onCancel={() => { setPending(null); setError(null); }}
        isDeleting={busy}
        error={error}
        itemType="thread"
      />
    ),
  };
}
