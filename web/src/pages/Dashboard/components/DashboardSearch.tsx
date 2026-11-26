import { Search } from 'lucide-react';
import type { CSSProperties } from 'react';
import { useTranslation } from 'react-i18next';
import { PaletteSearchField } from '@/components/nav/PaletteSearchField';
import './DashboardHeader.css';

const FIELD_CLASS =
  'dashboard-search-form owns-its-edge relative flex items-center gap-2 h-10 px-3 rounded-[var(--cs-radius-control,0.75rem)] border';

// The open card styles draw it as their other fields, open and flat, and
// focus darkens its edge to ink rather than lighting it amber (the focus
// edge is the shared rule's, tokens.css).
const FIELD_STYLE = {
  '--focus-edge': 'var(--color-shell-ink, var(--color-accent-primary))',
  '--focus-halo': 'var(--cs-frame-shadow, 0 0 0 1px var(--color-accent-soft))',
  backgroundColor: 'var(--color-shell-fill, var(--color-bg-input))',
  borderColor: 'var(--color-shell-edge, var(--color-border-muted))',
  boxShadow: 'none',
} as CSSProperties;

/** The `/` that brings the field up. */
function SlashBadge() {
  return (
    <span
      aria-hidden
      className="text-xs border rounded px-1.5 py-0.5 shrink-0"
      style={{
        color: 'var(--color-text-quaternary, var(--color-text-secondary))',
        borderColor: 'var(--color-border-default)',
      }}
    >
      /
    </span>
  );
}

/**
 * The phone's field: the command palette's, at Tickers. One search box
 * rather than a dropdown under a header the keyboard crowds out; a picked
 * ticker opens the same Market view.
 */
export function DashboardPaletteField() {
  const { t } = useTranslation();
  return (
    <PaletteSearchField
      start={{ kind: 'tickers' }}
      aria-label={t('dashboard:searchLabel')}
      className={`${FIELD_CLASS} text-left`}
      style={FIELD_STYLE}
    >
      <Search className="dashboard-search-icon" style={{ color: 'var(--color-icon-muted)' }} />
      <span className="dashboard-search-input dashboard-search-button-text">
        <span className="dashboard-search-placeholder">{t('dashboard:searchPlaceholder')}</span>
      </span>
      <SlashBadge />
    </PaletteSearchField>
  );
}
