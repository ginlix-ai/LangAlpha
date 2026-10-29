import { Plus, SlidersHorizontal } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { useWelcomeOffer } from '@/pages/Onboarding/welcome/useWelcomeOffer';

interface GalleryEmptyStateProps {
  /** A search or filter that matched nothing says so; it does not re-pitch the product. */
  isFiltered: boolean;
  /** The search that matched nothing, named back with a way to clear it. */
  query?: string;
  onClearSearch?: () => void;
  onNewWorkspace: () => void;
}

/**
 * What the gallery shows in place of cards. With none at all, a dotted panel
 * (the ground DESIGN.md keeps for empty states) says what a workspace is for
 * and how to start one. Its buttons are both secondary: the Chief of Staff's
 * New thread above is the page's one primary action for a new account.
 */
export function GalleryEmptyState({ isFiltered, query, onClearSearch, onNewWorkspace }: GalleryEmptyStateProps) {
  const { t } = useTranslation();
  const welcomeOffer = useWelcomeOffer();

  if (isFiltered) {
    return (
      <div className="flex flex-col items-center gap-2.5 text-center">
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {query ? t('workspace.noWorkspacesMatch', { query }) : t('workspace.noWorkspacesFound')}
        </p>
        {query && onClearSearch && (
          <button
            type="button"
            onClick={onClearSearch}
            className="text-sm underline underline-offset-[3px] transition-opacity hover:opacity-80"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('workspace.clearSearch')}
          </button>
        )}
      </div>
    );
  }

  const secondary = 'flex h-9 items-center gap-2 rounded-lg border px-4 text-sm font-medium press hover:bg-foreground/5';
  const secondaryStyle = { borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)', backgroundColor: 'var(--color-bg-page)' };
  return (
    <div
      className="dot-grid flex flex-col items-center rounded-xl px-6 py-11 text-center"
      style={{ backgroundColor: 'var(--color-bg-page)', border: '1px solid var(--color-border-default)' }}
    >
      <p className="title-font text-[17px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>
        {t('workspace.emptyTitle')}
      </p>
      <p className="mt-1.5 mb-5 max-w-md text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
        {t('workspace.emptyDesc')}
      </p>
      <div className="flex flex-col items-center gap-2.5 sm:flex-row">
        {welcomeOffer && (
          <button type="button" onClick={() => welcomeOffer.open()} className={secondary} style={secondaryStyle}>
            <SlidersHorizontal className="h-4 w-4" />
            {t('onboarding.welcome.entry')}
          </button>
        )}
        <button type="button" onClick={onNewWorkspace} className={secondary} style={secondaryStyle}>
          <Plus className="h-4 w-4" />
          {t('workspace.createWorkspace')}
        </button>
      </div>
    </div>
  );
}

export default GalleryEmptyState;
