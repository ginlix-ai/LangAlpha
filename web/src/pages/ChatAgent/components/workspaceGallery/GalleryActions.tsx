import { Plus } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { cn } from '@/lib/utils';

interface GalleryActionsProps {
  onNewWorkspace: () => void;
  /** Fill the row (the narrow header). */
  stacked?: boolean;
}

/**
 * The gallery's header action. One component because the desktop header and
 * the mobile one show the same button, and a copy is how they drift apart.
 * The computers are reached from the toolbar's computer control, not here.
 */
export function GalleryActions({ onNewWorkspace, stacked = false }: GalleryActionsProps) {
  const { t } = useTranslation();
  return (
    <button
      onClick={onNewWorkspace}
      className={cn(
        'flex items-center gap-1.5 py-2 h-9 rounded-lg press hover:opacity-90',
        stacked ? 'w-full min-w-0 justify-center px-3' : 'px-4',
      )}
      style={{
        backgroundColor: 'var(--color-btn-primary-bg)',
        color: 'var(--color-btn-primary-text)',
      }}
    >
      <Plus className="h-4 w-4 shrink-0" />
      <span className="truncate whitespace-nowrap text-sm font-medium">{t('workspace.newWorkspace')}</span>
    </button>
  );
}

export default GalleryActions;
