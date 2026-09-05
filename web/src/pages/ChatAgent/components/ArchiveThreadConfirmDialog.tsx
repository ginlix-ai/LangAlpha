import { useTranslation } from 'react-i18next';
import { Archive } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

interface ArchiveThreadConfirmDialogProps {
  open: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}

/**
 * Confirm archiving a thread whose run is still live. Archiving is deliberately
 * allowed mid-run — the run keeps going server-side — so this asks rather than
 * blocks, and only ever opens for a live thread (see useArchiveThreadConfirm).
 */
function ArchiveThreadConfirmDialog({ open, onCancel, onConfirm }: ArchiveThreadConfirmDialogProps) {
  const { t } = useTranslation();

  return (
    <Dialog open={open} onOpenChange={(next) => { if (!next) onCancel(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Archive aria-hidden className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            {t('chat.archiveConfirm.title', 'Archive a running thread?')}
          </DialogTitle>
          <DialogDescription>
            {t(
              'chat.archiveConfirm.body',
              "This thread has a run in progress. Archiving hides it from your lists while the run keeps going — you'll find it under Archived, with its results waiting.",
            )}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="ghost" onClick={onCancel}>
            {t('common.cancel')}
          </Button>
          <Button onClick={onConfirm}>
            {t('chat.archiveConfirm.confirm', 'Archive')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ArchiveThreadConfirmDialog;
