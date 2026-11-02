import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import type { NextSummary } from '../../utils/api';
import { ComposerRow } from './ComposerRow';

const ACTION =
  'text-xs font-medium whitespace-nowrap rounded-md px-1.5 py-0.5 shrink-0 outline-hidden transition-colors ' +
  'underline decoration-(--color-border-elevated) underline-offset-4 text-(--color-text-secondary) ' +
  'hover:text-(--color-text-primary) hover:decoration-current focus-visible:ring-2 focus-visible:ring-ring';

/* Says, above the composer, that the next send summarizes the earlier
   conversation first. That is the plan, so the line leads and the way out is
   a quiet action beside it. After a break (`cold`) the user may skip it for
   that send, and the line then says what that costs, with the way back; at
   the threshold, or for a rebuild, the summary has to run, so the row only
   says so. */
export function NextSummaryRow({
  reason,
  skipped,
  onToggleSkip,
}: {
  reason: NextSummary['reason'];
  skipped: boolean;
  onToggleSkip: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <ComposerRow text={t(skipped ? 'chat.nextSummary.skipped' : 'chat.nextSummary.line')}>
      {reason === 'cold' && (skipped ? (
        <button type="button" onClick={onToggleSkip} className={ACTION}>
          {t('chat.nextSummary.undo')}
        </button>
      ) : (
        <TooltipProvider delayDuration={300}>
          <Tooltip>
            <TooltipTrigger asChild>
              <button type="button" onClick={onToggleSkip} className={ACTION}>
                {t('chat.nextSummary.skip')}
              </button>
            </TooltipTrigger>
            <TooltipContent side="top" align="end">{t('chat.nextSummary.skipHint')}</TooltipContent>
          </Tooltip>
        </TooltipProvider>
      ))}
    </ComposerRow>
  );
}
