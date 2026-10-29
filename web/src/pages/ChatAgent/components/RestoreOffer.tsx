/**
 * The offer to restore a workspace whose folder is gone from a machine the
 * user brought: copy its backup back, or keep the folder empty.
 *
 * The server never restores on its own, since a folder the user emptied on
 * purpose would otherwise come back every time. It records the offer on the
 * workspace row when it finds the folder gone (a turn or a file listing
 * starting the machine), and this shows it wherever the user works: above the
 * chat input and in the file panel. Both read one answer (see
 * `useFilesRestoreChoice`), so either can make it.
 */
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FolderX } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useWorkspace } from '@/hooks/useWorkspace';
import { cn } from '@/lib/utils';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';

import { useComputers } from '../hooks/useComputers';
import { useFilesRestoreChoice } from '../hooks/useFilesRestoreChoice';

import { backupAge } from './backupAge';

const ACTION_CLASS = 'h-7 px-2.5 text-xs';
const TINT = 'var(--color-warning-soft)';

interface RestoreOfferProps {
  workspaceId: string;
  /** `banner` above the chat input; `notice` in the file panel's narrow strip. */
  variant: 'banner' | 'notice';
  className?: string;
}

export function RestoreOffer({ workspaceId, variant, className }: RestoreOfferProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { data: workspace } = useWorkspace(workspaceId);
  const offeredAt = workspace?.files_restore_offered_at;
  const { data: computerData } = useComputers({ enabled: !!offeredAt });
  const now = useNow(60_000, !!offeredAt);
  const choice = useFilesRestoreChoice(workspaceId, t('workspace.restoreOffer.failed', "Couldn't restore the files"));
  const [confirmingKeep, setConfirmingKeep] = useState(false);

  if (!offeredAt) return null;

  const computerName = computerData?.computers?.find((c) => c.computer_id === workspace?.computer_id)?.name;
  const backupTime = backupAge(workspace?.files_backed_up_at, locale, now);
  const notice = variant === 'notice';

  const missing = computerName
    ? t('workspace.restoreOffer.missing', "This workspace's folder is missing on {{computer}}.", { computer: computerName })
    : t('workspace.restoreOffer.missingUnnamed', "This workspace's folder is missing.");
  const prompt = backupTime
    ? t('workspace.restoreOffer.prompt', 'Restore it from the backup taken {{time}}?', { time: backupTime })
    : t('workspace.restoreOffer.promptUndated', 'Restore it from its backup?');

  const actions = choice.pending !== null ? (
    <span className="flex items-center gap-1.5 text-xs" role="status" style={{ color: 'var(--color-text-secondary)' }}>
      <Loader size={12} className="text-(--color-accent-primary)" />
      {choice.pending
        ? t('workspace.restoreOffer.restoring', 'Restoring files…')
        : t('workspace.restoreOffer.keeping', 'Keeping the folder empty…')}
    </span>
  ) : (
    <div className="flex shrink-0 flex-wrap items-center gap-1.5">
      <Button size="sm" variant="ghost" className={ACTION_CLASS} onClick={() => setConfirmingKeep(true)}>
        {t('workspace.restoreOffer.keepEmpty', 'Keep it empty')}
      </Button>
      <Button size="sm" className={ACTION_CLASS} onClick={() => choice.choose(true)}>
        {t('workspace.restoreOffer.restore', 'Restore files')}
      </Button>
    </div>
  );

  return (
    <div
      data-testid="restore-offer"
      className={cn(
        'flex items-start gap-2 border text-sm',
        notice ? 'mx-2 mt-2 rounded-md px-2.5 py-2' : 'rounded-lg px-3 py-2.5',
        className,
      )}
      // Opaque, like the disk warning: above the composer the transcript
      // scrolls under it.
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: `linear-gradient(${TINT}, ${TINT})`,
        borderColor: 'var(--color-warning)',
        color: 'var(--color-text-primary)',
      }}
    >
      <FolderX className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-warning)' }} aria-hidden="true" />
      {/* The panel is narrow, so its actions always sit below the text; above
          the chat input they sit beside it where the row is wide enough. */}
      <div className={cn('flex flex-1 min-w-0 flex-col gap-2', !notice && 'sm:flex-row sm:items-center')}>
        <div className="flex-1 min-w-0">
          <p className="font-medium">{missing}</p>
          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-secondary)' }}>{prompt}</p>
          {choice.error && (
            <p className="text-xs mt-1" role="alert" style={{ color: 'var(--color-loss)' }}>{choice.error}</p>
          )}
        </div>
        {actions}
      </div>
      <ConfirmDialog
        open={confirmingKeep}
        onOpenChange={setConfirmingKeep}
        title={t('workspace.restoreOffer.keepEmptyTitle', 'Keep the folder empty?')}
        message={t(
          'workspace.restoreOffer.keepEmptyBody',
          "The backup then follows the folder as it is now, so the files in it can't be restored afterwards.",
        )}
        confirmLabel={t('workspace.restoreOffer.keepEmptyConfirm', 'Keep it empty')}
        onConfirm={() => choice.choose(false)}
      />
    </div>
  );
}
