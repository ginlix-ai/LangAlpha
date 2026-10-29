/**
 * Confirm turning off a machine's file backup. Nothing is deleted, but a
 * folder lost on the machine afterwards can only come back as it was at the
 * last backup, which is worth one plain sentence before the switch moves.
 *
 * Opened over the Computers dialog, which makes the change; the switch then
 * moves at once and a refusal is toasted, so this closes on confirm.
 */
import { useId } from 'react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';

interface FilesBackupOffDialogProps {
  name: string;
  onClose: () => void;
  onConfirm: () => void;
}

function FilesBackupOffDialog({ name, onClose, onConfirm }: FilesBackupOffDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose}>
        {t('common.cancel')}
      </Button>
      <Button type="button" onClick={onConfirm}>
        {t('computer.filesBackup.offConfirm', 'Stop backing up')}
      </Button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.filesBackup.offTitle', 'Stop backing up files?')}
      onClose={onClose}
      width="narrow"
      density="form"
      stacked
      footer={footer}
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {t('computer.filesBackup.offBody', {
          name,
          defaultValue:
            'The last backup of each workspace on {{name}} is kept but stops updating. A folder lost on this machine could then only be restored as it was then.',
        })}
      </p>
    </ModalShell>
  );
}

export default FilesBackupOffDialog;
