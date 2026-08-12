import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import type { Workspace } from '@/types/api';

interface DuplicateWorkspaceDialogProps {
  /** Only the name is read — narrowed so nav-tree rows open this without a cast. */
  target: Partial<Pick<Workspace, 'name'>> | null;
  onClose: () => void;
  onConfirm: () => void;
  busy: boolean;
}

/** Confirm duplicating a workspace (copies files; always-on starts off on the copy). */
function DuplicateWorkspaceDialog({ target, onClose, onConfirm, busy }: DuplicateWorkspaceDialogProps) {
  const { t } = useTranslation();

  return (
    <Dialog open={!!target} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle>{t('workspace.duplicate', 'Duplicate')}</DialogTitle>
          <DialogDescription>
            {t('workspace.duplicateConfirm', {
              name: target?.name ?? '',
              defaultValue: 'Create a copy of "{{name}}"? Files are copied; always-on starts off on the copy.',
            })}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button onClick={onConfirm} disabled={busy}>
            {busy ? t('workspace.duplicating', 'Duplicating…') : t('workspace.duplicate', 'Duplicate')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default DuplicateWorkspaceDialog;
