import { useState } from 'react';
import { useLocation } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Info, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { readSharedGone } from '@/lib/sharedGone';

/** Where someone lands after a shared workspace they had open stopped being
 *  theirs (removed, their access ended, or the workspace deleted), or a
 *  thread of it they had open was deleted or made private: one quiet line
 *  saying so, which they can put away. Nothing for any other arrival. */
export function SharedGoneNotice({ className }: { className?: string }) {
  const { t } = useTranslation();
  const location = useLocation();
  const reason = readSharedGone(location.state);
  const copy = reason === 'deleted' ? 'sharedWorkspace.deleted'
    : reason === 'thread' ? 'sharedWorkspace.threadDeleted'
      : reason === 'private' ? 'sharedWorkspace.threadPrivate' : 'sharedWorkspace.gone';
  const [dismissedKey, setDismissedKey] = useState<string | null>(null);
  if (!reason || dismissedKey === location.key) return null;
  return (
    <div
      role="status"
      className={cn('flex items-start gap-2.5 rounded-lg border px-3.5 py-3 text-sm', className)}
      style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)', color: 'var(--color-text-primary)' }}
      data-testid="shared-gone-notice"
    >
      <Info aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="font-medium">{t(`${copy}.title`)}</span>
        <span style={{ color: 'var(--color-text-secondary)' }}>{t(`${copy}.body`)}</span>
      </div>
      <button
        type="button"
        onClick={() => setDismissedKey(location.key)}
        className="tap-target -m-1 rounded-md p-1 hover:bg-(--color-bg-hover) focus-visible:ring-2 focus-visible:ring-ring"
        aria-label={t('common.close')}
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <X className="h-4 w-4" />
      </button>
    </div>
  );
}
