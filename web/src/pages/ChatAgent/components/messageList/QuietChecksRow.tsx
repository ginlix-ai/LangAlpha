import { useState, type ReactElement } from 'react';
import { useTranslation } from 'react-i18next';
import { AlarmClock, ChevronDown } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useLoadedHomeTimezone } from '@/hooks/useHomeTimezone';
import { announceAnchoredToggle } from '../../utils/anchoredToggle';
import type { QuietCheck } from './quietChecks';
import { quietEntryText, quietHeaderText } from './wakeupRowLabels';

interface QuietChecksRowProps {
  /** Back-to-back wake turns that found nothing new, oldest first. */
  checks: readonly QuietCheck[];
}

const RULE = { borderTop: '1px solid var(--color-border-muted)' };

/**
 * Wake turns that ended quietly, drawn as one divider: a check that found
 * nothing has nothing to read, so its turn is not shown. Opening it lists one
 * line per check, with what the agent said it checked.
 */
export function QuietChecksRow({ checks }: QuietChecksRowProps): ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const zone = useLoadedHomeTimezone();
  // Only which day is today moves the labels.
  const now = useNow(60 * 60_000);
  const [open, setOpen] = useState(false);
  return (
    <div className="my-1" data-quiet-checks={checks.length}>
      <div className="flex items-center gap-3 py-2">
        <div className="min-w-4 flex-1" style={RULE} />
        <button
          type="button"
          aria-expanded={open}
          onClick={(e) => { announceAnchoredToggle(e.currentTarget); setOpen((v) => !v); }}
          className="inline-flex min-w-0 items-center gap-1.5 bg-transparent border-0 p-0 text-xs tabular-nums cursor-pointer transition-colors hover:text-foreground"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <AlarmClock aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
          <span>{quietHeaderText(checks, t, locale, now)}</span>
          <ChevronDown
            aria-hidden="true"
            className="h-3.5 w-3.5 shrink-0 transition-transform"
            style={{ transform: open ? 'rotate(180deg)' : undefined }}
          />
        </button>
        <div className="min-w-4 flex-1" style={RULE} />
      </div>
      {open && (
        <ul
          className="mx-auto mb-2 max-w-3xl space-y-1 px-3 text-xs tabular-nums"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {checks.map((check) => (
            <li key={check.turnIndex} className="wrap-break-word">{quietEntryText(check, t, locale, now, zone)}</li>
          ))}
        </ul>
      )}
    </div>
  );
}
