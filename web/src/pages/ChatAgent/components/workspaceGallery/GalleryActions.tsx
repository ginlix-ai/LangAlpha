import { Plus, Server } from 'lucide-react';
import { useTranslation } from 'react-i18next';

interface GalleryActionsProps {
  onNewWorkspace: () => void;
  onOpenComputers: () => void;
  /** Fill the row, the primary action taking the larger share (the mobile header). */
  stacked?: boolean;
}

/**
 * The gallery's two header actions. One component because the desktop header
 * and the mobile one show the same pair, and a copy of a button group is how
 * they drift apart.
 */
export function GalleryActions({ onNewWorkspace, onOpenComputers, stacked = false }: GalleryActionsProps) {
  const { t } = useTranslation();
  const fill = stacked ? ' flex-1 justify-center' : '';
  return (
    <div className={stacked ? 'flex w-full items-center gap-2' : 'flex items-center gap-2'}>
      <button
        onClick={onOpenComputers}
        className={`flex items-center gap-1.5 px-3 py-2 h-9 rounded-lg border transition-all hover:bg-foreground/5 active:scale-[0.985]${fill}`}
        style={{
          borderColor: 'var(--color-border-muted)',
          color: 'var(--color-text-secondary)',
        }}
      >
        <Server className="h-4 w-4" />
        <span className="text-sm font-medium">{t('computer.computers', 'Computers')}</span>
      </button>
      <button
        onClick={onNewWorkspace}
        className={`flex items-center gap-1.5 px-4 py-2 h-9 rounded-lg transition-all hover:opacity-90 active:scale-[0.985]${stacked ? ' flex-[1.4] justify-center' : ''}`}
        style={{
          backgroundColor: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
        }}
      >
        <Plus className="h-4 w-4" />
        <span className="text-sm font-medium">{t('workspace.newWorkspace')}</span>
      </button>
    </div>
  );
}

export default GalleryActions;
