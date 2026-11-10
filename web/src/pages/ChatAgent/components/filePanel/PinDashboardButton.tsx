import React from 'react';
import { House } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';

interface PinDashboardButtonProps {
  pinned: boolean;
  onClick: () => void;
  /** Pinning: it stays focusable and says so rather than dropping out of the tab order. */
  pending?: boolean;
  /** Nothing to pin yet (an app whose link is still minting). */
  disabled?: boolean;
}

/**
 * The panel's "set as dashboard" button. Pinned, it stays in the tab order
 * and says so; a click does nothing.
 */
export function PinDashboardButton({ pinned, onClick, pending = false, disabled }: PinDashboardButtonProps): React.ReactElement {
  const { t } = useTranslation();
  const label = pinned ? t('customDashboard.isDashboard') : t('customDashboard.setAsDashboard');
  return (
    <button
      type="button"
      onClick={onClick}
      className="file-panel-icon-btn shrink-0"
      title={label}
      aria-label={label}
      aria-pressed={pinned}
      disabled={disabled}
      aria-disabled={pending || undefined}
      aria-busy={pending || undefined}
    >
      {pending
        ? <Loader size={14} />
        : <House className="h-3.5 w-3.5" style={pinned ? { color: 'var(--color-accent-primary)' } : undefined} />}
    </button>
  );
}
