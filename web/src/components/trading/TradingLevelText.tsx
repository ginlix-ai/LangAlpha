import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';
import { DEFAULT_TRADING_LEVEL, TRADING_LEVEL_INFO, skipsApproval } from '@/lib/tradingPermission';
import { cn } from '@/lib/utils';
import type { TradingLevel } from '@/types/api';

/**
 * A trading level's name, with the tag on the default level, as every choice
 * of levels names it: Settings' list and the welcome flow's cards. The card
 * around it, and the description under it, are each surface's own.
 *
 * `flagWarning` draws the warning a level carries on its name, for a card with
 * no glyph of its own to carry it. One that has a glyph per level (the welcome
 * flow's) shows the warning there, and the name stays plain beside it.
 */
export function TradingLevelName({
  level,
  id,
  flagWarning = false,
  className,
}: {
  level: TradingLevel;
  /** For a control named by the label alone, without the tag. */
  id?: string;
  flagWarning?: boolean;
  className?: string;
}) {
  const { t } = useTranslation();
  const info = TRADING_LEVEL_INFO[level];
  const warning = flagWarning && info.warning;
  return (
    <span className={cn('flex flex-wrap items-center gap-x-2 gap-y-1', className)}>
      <span
        id={id}
        className="inline-flex items-center gap-1.5"
        style={{ color: warning ? 'var(--color-warning)' : 'var(--color-text-primary)' }}
      >
        {warning && <AlertTriangle className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />}
        <span>{t(info.labelKey)}</span>
      </span>
      {level === DEFAULT_TRADING_LEVEL && (
        <span className="rounded-full border border-(--color-border-muted) px-1.5 text-[0.625rem] font-medium leading-4 text-(--color-text-tertiary)">
          {t('settings.tradingPermission.defaultTag')}
        </span>
      )}
    </span>
  );
}

/**
 * When the agreement was accepted, under the chosen level when that level is
 * one that needed it; nothing on any other. Its colour is the caller's, a step
 * quieter than the description it sits under.
 */
export function TradingLevelAgreedOn({
  level,
  chosen,
  agreedDate,
  className,
}: {
  level: TradingLevel;
  chosen: boolean;
  agreedDate: string | null;
  className?: string;
}) {
  const { t } = useTranslation();
  if (!chosen || !skipsApproval(level) || !agreedDate) return null;
  return (
    <span className={cn('mt-1.5 block text-[0.6875rem]', className)}>
      {t('settings.tradingPermission.agreedOn', { date: agreedDate })}
    </span>
  );
}
