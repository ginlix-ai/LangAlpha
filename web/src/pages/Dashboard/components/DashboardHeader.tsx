import { Pencil } from 'lucide-react';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { DashboardPaletteField, DashboardSymbolField } from './DashboardSearch';

interface DashboardHeaderProps {
  onScrollToTop?: () => void;
  /** The layout's Edit button; absent where the layout can't be edited (a phone). */
  edit?: {
    editMode: boolean;
    onEditModeChange: (edit: boolean) => void;
  };
  /** The dashboard switcher and the shown dashboard's own actions, before Edit. */
  actions?: React.ReactNode;
}

const DashboardHeader: React.FC<DashboardHeaderProps> = ({ onScrollToTop, edit, actions }) => {
  const isMobile = useIsMobile();
  const { t } = useTranslation();

  return (
    <>
      {/* Doubles as titlebar in the desktop shell. The search field and the
          buttons declare no-drag for themselves, so it is the space between
          them that moves the window. */}
      <div
        data-chrome="drag"
        className="sticky top-0 z-30 flex items-center justify-between px-4 sm:px-6 py-3"
        style={{
          backgroundColor: 'var(--color-bg-page)',
          // The 40px search row centres at the shared first-row depth (--page-top).
          paddingTop: 'calc(var(--page-top) - 6px)',
          borderBottom: '1px solid var(--color-border-muted)',
          backdropFilter: 'blur(12px)',
          WebkitBackdropFilter: 'blur(12px)',
          cursor: isMobile ? 'pointer' : undefined,
        }}
        onClick={(e) => {
          if (!isMobile) return;
          if ((e.target as HTMLElement).closest('button, a, input, form')) return;
          onScrollToTop?.();
        }}
      >
        {/* Search */}
        <div className="flex-1 min-w-0 max-w-xl">
          {isMobile ? <DashboardPaletteField /> : <DashboardSymbolField />}
        </div>

        {/* Right actions */}
        <div className="flex items-center gap-1 sm:gap-3 ml-3 min-w-0">
          {actions}
          {edit && (
            <button
              type="button"
              onClick={() => edit.onEditModeChange(!edit.editMode)}
              className="p-2 rounded-md transition-colors"
              style={{
                color: edit.editMode ? 'var(--color-btn-primary-text)' : 'var(--color-text-secondary)',
                backgroundColor: edit.editMode ? 'var(--color-btn-primary-bg)' : 'transparent',
              }}
              aria-label={t(edit.editMode ? 'dashboard.header.exitEditLayout' : 'dashboard.header.editLayout')}
              title={t(edit.editMode ? 'dashboard.header.exitEditLayout' : 'dashboard.header.editLayout')}
            >
              <Pencil size={16} />
            </button>
          )}
        </div>
      </div>
    </>
  );
};

export default DashboardHeader;
