import { HelpCircle, Mail, Pencil } from 'lucide-react';
import React, { useState, useEffect, useRef } from 'react';
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
  const [showHelpPopover, setShowHelpPopover] = useState(false);
  const helpRef = useRef<HTMLDivElement>(null);

  // Close help popover on outside click
  useEffect(() => {
    if (!showHelpPopover) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (helpRef.current && !helpRef.current.contains(e.target as Node)) {
        setShowHelpPopover(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showHelpPopover]);

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

          {/* Help — hidden on mobile to save space */}
          <div className="relative hidden sm:block" ref={helpRef}>
            <button
              className="p-2 transition-colors"
              style={{ color: showHelpPopover ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
              onClick={() => setShowHelpPopover((prev) => !prev)}
              onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--color-text-primary)')}
              onMouseLeave={(e) => {
                if (!showHelpPopover) e.currentTarget.style.color = 'var(--color-text-secondary)';
              }}
            >
              <HelpCircle size={20} />
            </button>
            {showHelpPopover && (
              <div
                className="absolute right-0 top-full mt-2 z-50 rounded-lg shadow-lg"
                style={{
                  backgroundColor: 'var(--color-bg-elevated)',
                  border: '1px solid var(--color-border-elevated)',
                  width: '280px',
                  maxWidth: 'calc(100vw - 32px)',
                  padding: '16px',
                }}
              >
                <p
                  className="text-sm font-medium mb-3"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {t('dashboard.contactMessage')}
                </p>
                {((import.meta.env.VITE_CONTACT_EMAILS as string) || '').split(',').filter(Boolean).map((email: string, idx: number, arr: string[]) => (
                  <a
                    key={email}
                    href={`mailto:${email.trim()}`}
                    // An anchor, not a div with an onClick: this row is a link,
                    // and the element brings focus, Enter activation and the
                    // browser's own address menu with it. It also sits inside the
                    // header's drag region, which chrome.css exempts by element,
                    // and `a` is on that list where a bare div is not: as a div
                    // the shell read a click on the row as dragging the window.
                    className="flex items-center gap-2 px-3 py-2 rounded-md cursor-pointer transition-colors hover:opacity-80"
                    style={{ backgroundColor: 'var(--color-bg-input)', marginBottom: idx < arr.length - 1 ? '8px' : undefined }}
                    onClick={() => setShowHelpPopover(false)}
                  >
                    <Mail className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    <div className="min-w-0">
                      <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.header.emailLabel')}</p>
                      <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{email.trim()}</p>
                    </div>
                  </a>
                ))}
              </div>
            )}
          </div>

        </div>
      </div>
    </>
  );
};

export default DashboardHeader;
