import React, { useCallback, useState } from 'react';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';

export interface ConfirmRequest {
  title: string;
  message: string;
  confirmLabel: string;
}

/** Asks, then runs `onConfirm` only if the reader confirms. */
export type AskConfirm = (request: ConfirmRequest, onConfirm: () => void) => void;

/**
 * The file panel's confirmation, asked through the app's dialog. A native
 * confirm() froze the whole page until answered, the chat's stream included,
 * so the action now runs from the dialog's answer instead.
 */
export function usePanelConfirm(): { ask: AskConfirm; dialog: React.ReactNode } {
  // Kept after `open` drops, so the copy holds while the dialog closes.
  const [request, setRequest] = useState<(ConfirmRequest & { onConfirm: () => void }) | null>(null);
  const [open, setOpen] = useState(false);

  const ask = useCallback<AskConfirm>((next, onConfirm) => {
    setRequest({ ...next, onConfirm });
    setOpen(true);
  }, []);

  return {
    ask,
    dialog: (
      <ConfirmDialog
        open={open}
        title={request?.title ?? ''}
        message={request?.message ?? ''}
        confirmLabel={request?.confirmLabel}
        onConfirm={request?.onConfirm}
        onOpenChange={setOpen}
        focusConfirm
      />
    ),
  };
}
