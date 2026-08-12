import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import type { Workspace } from '@/types/api';

interface RenameWorkspaceDialogProps {
  target: Workspace | null;
  onClose: () => void;
  onSubmit: (name: string) => void;
  busy: boolean;
}

/** Rename a workspace. Owns the draft; parent persists via onSubmit. */
function RenameWorkspaceDialog({ target, onClose, onSubmit, busy }: RenameWorkspaceDialogProps) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState('');

  // Seed the draft each time the dialog opens onto a workspace.
  useEffect(() => {
    if (target) setDraft(target.name);
  }, [target]);

  const trimmed = draft.trim();
  const canSubmit = !busy && !!trimmed && trimmed !== target?.name;

  return (
    <Dialog open={!!target} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle>{t('workspace.rename')}</DialogTitle>
          <DialogDescription>{t('workspace.renameDescription')}</DialogDescription>
        </DialogHeader>
        <Input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={t('workspace.workspaceName')}
          aria-label={t('workspace.rename')}
          autoFocus
          onKeyDown={(e) => { if (e.key === 'Enter' && !busy) { e.preventDefault(); onSubmit(trimmed); } }}
        />
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button onClick={() => onSubmit(trimmed)} disabled={!canSubmit}>
            {busy ? t('common.saving') : t('common.save')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default RenameWorkspaceDialog;
