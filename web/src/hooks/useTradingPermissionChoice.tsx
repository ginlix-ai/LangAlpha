import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { TradingAgreementDialog } from '@/components/trading/TradingAgreementDialog';
import { useToast } from '@/components/ui/use-toast';
import { useTradingPermission, useUpdateTradingPermission } from '@/hooks/useTradingPermission';
import { useLocale } from '@/hooks/useLocale';
import { mediumDate } from '@/lib/format';
import { TRADING_AGREEMENT_VERSION, skipsApproval } from '@/lib/tradingPermission';
import type { AgreementLevel, TradingLevel } from '@/types/api';

function formatAgreedAt(ts: string | null, locale: string): string | null {
  if (!ts) return null;
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? null : mediumDate(d, locale);
}

/**
 * Choosing the trading permission, wherever the levels are drawn. The two
 * levels that ask save on the click; the two that let orders through save only
 * once the agreement is accepted, so nothing ever shows a level the server has
 * not taken. Settings and the welcome flow draw the levels each their own way
 * and share this, so the consent behind them cannot drift apart.
 *
 * `dialog` is the agreement, to render beside the levels.
 */
export function useTradingPermissionChoice({
  onPreparing,
}: {
  /** Handed each save as it starts, settling once the server has answered it
   *  and never rejecting: a failure is the toast's to say. The welcome flow's
   *  Start waits on it, so the agent's first turn is not sent while the server
   *  still holds the level it replaces. */
  onPreparing?: (work: Promise<void>) => void;
} = {}) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const locale = useLocale();
  const permission = useTradingPermission();
  const update = useUpdateTradingPermission();
  const [agreeing, setAgreeing] = useState<AgreementLevel | null>(null);
  const data = permission.data;

  const saveFailed = () => {
    toast({
      variant: 'destructive',
      title: t('common.error'),
      description: t('settings.tradingPermission.saveFailed'),
    });
  };

  // Each save is handed on as it starts, its failure already caught.
  const started = (saving: Promise<unknown>) => {
    onPreparing?.(saving.then(() => {}));
  };

  // A pick made while a save is in flight is dropped rather than queued. The
  // inputs stay enabled through it: disabling the one that holds focus would
  // throw a keyboard user's focus back to the page on every arrow key.
  const select = (level: TradingLevel) => {
    if (!data || level === data.level || update.isPending) return;
    if (skipsApproval(level)) setAgreeing(level);
    else started(update.mutateAsync({ level }).catch(saveFailed));
  };

  // The version sent is the one of the text the dialog showed, never the
  // server's: echoing the server's would accept whatever it moved to since
  // this bundle loaded. A failure leaves the dialog open for the retry.
  const agree = (level: AgreementLevel) => {
    started(
      update
        .mutateAsync({ level, agreement_version: TRADING_AGREEMENT_VERSION })
        .then(() => setAgreeing(null), saveFailed),
    );
  };

  // A level that saves on the click shows as chosen while it is on its way,
  // and goes back if the server refuses it. One behind the agreement is shown
  // by its dialog instead.
  const pendingLevel =
    update.isPending && update.variables && !skipsApproval(update.variables.level) ? update.variables.level : null;

  const dialog = (
    <AnimatePresence>
      {agreeing && (
        <TradingAgreementDialog
          key={agreeing}
          level={agreeing}
          pending={update.isPending}
          outdated={data !== undefined && data.agreement_version !== TRADING_AGREEMENT_VERSION}
          onAgree={() => agree(agreeing)}
          onCancel={() => setAgreeing(null)}
        />
      )}
    </AnimatePresence>
  );

  return {
    permission,
    /** The level to draw as chosen. */
    shown: pendingLevel ?? data?.level,
    /** A level saving on the click, for a "Saving…" beside the title. */
    pendingLevel,
    /** When the current agreement was accepted, formatted for the reader. */
    agreedDate: formatAgreedAt(data?.agreed_at ?? null, locale),
    select,
    dialog,
  };
}
