/**
 * The way out of a refusal for want of a computer Home can live on, wherever
 * it is said: a banner's link, the New workspace dialog, and the card on the
 * user's only computer when they try to remove it.
 *
 * `needsComputer`: the user has none, so the way out is adding one. In the
 * desktop app on a Mac that can pair itself, that is this Mac first.
 * `onlyComputer`: adding another, so this one can go. `needsBackup`: the
 * user's computer has backup off, so the way out is that computer's details,
 * where backup is turned on.
 */
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { canAddThisMac } from '@/lib/desktop';
import { cn } from '@/lib/utils';

import type { ComputersPanelView } from '../hooks/computerPanelStore';
import { useOpenComputers } from '../hooks/useOpenComputers';

export type ComputerRefusalKind =
  | { kind: 'needsComputer' }
  | { kind: 'onlyComputer' }
  | { kind: 'needsBackup'; computerId: string; name: string };

const LINK_STYLE = { textDecoration: 'underline', fontWeight: 500 } as const;

interface ComputerRefusalProps {
  refusal: ComputerRefusalKind;
  /**
   * The refusal in the surface's words, drawn in a card above its buttons.
   * Without it only the first way out is drawn, as a link, for a banner that
   * says the rest.
   */
  message?: string;
  /** Opens the way out in place of the Computers dialog's store, for a surface that must close first. */
  onOpen?: (panel: ComputersPanelView) => void;
  /** The card answers what the user just did, so it is announced. */
  alert?: boolean;
  disabled?: boolean;
  className?: string;
}

export function ComputerRefusal({ refusal, message, onOpen, alert = false, disabled = false, className = '' }: ComputerRefusalProps) {
  const { t } = useTranslation();
  const openComputers = useOpenComputers();
  const open = onOpen ?? openComputers;

  const ways: { label: string; panel: ComputersPanelView }[] = [];
  if (refusal.kind === 'needsBackup') {
    ways.push({
      label: t('homePlacement.openComputer', { name: refusal.name }),
      panel: { view: 'details', computerId: refusal.computerId },
    });
  } else {
    if (refusal.kind === 'needsComputer' && canAddThisMac()) {
      ways.push({ label: t('homePlacement.setUpThisMac'), panel: { view: 'create', pick: 'thisMac' } });
    }
    ways.push({ label: t('homePlacement.addComputer'), panel: { view: 'create' } });
  }

  if (message === undefined) {
    const [first] = ways;
    return (
      <button
        type="button"
        className="tap-target cursor-pointer"
        disabled={disabled}
        onClick={() => open(first.panel)}
        style={LINK_STYLE}
      >
        {first.label}
      </button>
    );
  }
  return (
    <div
      role={alert ? 'alert' : undefined}
      className={cn('flex flex-col items-start gap-2 rounded-md px-3 py-2.5 text-sm', className)}
      style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
    >
      <p>{message}</p>
      <div className="flex flex-wrap gap-2">
        {ways.map((way) => (
          <Button key={way.label} type="button" variant="outline" size="sm" disabled={disabled} onClick={() => open(way.panel)}>
            {way.label}
          </Button>
        ))}
      </div>
    </div>
  );
}
