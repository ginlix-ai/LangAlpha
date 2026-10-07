import { useEffect, useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import { AlertTriangle } from 'lucide-react';
import { AnimatePresence } from '@/lib/framer';
import { RadioChoice } from '@/components/ui/choice';
import { useToast } from '@/components/ui/use-toast';
import { useTradingPermission, useUpdateTradingPermission } from '@/hooks/useTradingPermission';
import { useLocale } from '@/hooks/useLocale';
import { mediumDate } from '@/lib/format';
import {
  DEFAULT_TRADING_LEVEL,
  TRADING_AGREEMENT_VERSION,
  TRADING_LEVELS,
  TRADING_LEVEL_INFO,
  TRADING_PERMISSION_ANCHOR,
  skipsApproval,
} from '@/lib/tradingPermission';
import { cn } from '@/lib/utils';
import type { AgreementLevel, TradingLevel } from '@/types/api';
import { TradingAgreementDialog } from './TradingAgreementDialog';

function formatAgreedAt(ts: string | null, locale: string): string | null {
  if (!ts) return null;
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? null : mediumDate(d, locale);
}

/**
 * The user's trading permission: how far the agent may go with orders at their
 * brokerages. The two levels that ask save on the click; the two that let
 * orders through save only once the agreement is accepted, so nothing here
 * ever shows a level the server has not taken.
 */
export function TradingPermissionSection() {
  const { t } = useTranslation();
  const { toast } = useToast();
  const locale = useLocale();
  const { hash } = useLocation();
  const headingId = useId();
  const descId = useId();
  const groupName = useId();
  const sectionRef = useRef<HTMLElement>(null);
  const permission = useTradingPermission();
  const update = useUpdateTradingPermission();
  const [agreeing, setAgreeing] = useState<AgreementLevel | null>(null);
  const data = permission.data;
  const loaded = data !== undefined;

  // A deep link lands on the section once it has its height. Before that the
  // cards are not drawn, and a scroll to a short page stops short of it.
  useEffect(() => {
    if (!loaded || hash !== `#${TRADING_PERMISSION_ANCHOR}`) return;
    sectionRef.current?.scrollIntoView?.({ block: 'start' });
  }, [loaded, hash]);

  const saveFailed = () => {
    toast({
      variant: 'destructive',
      title: t('common.error'),
      description: t('settings.tradingPermission.saveFailed'),
    });
  };

  // A pick made while a save is in flight is dropped rather than queued. The
  // inputs stay enabled through it: disabling the one that holds focus would
  // throw a keyboard user's focus back to the page on every arrow key.
  const select = (level: TradingLevel) => {
    if (!data || level === data.level || update.isPending) return;
    if (skipsApproval(level)) setAgreeing(level);
    else void update.mutateAsync({ level }).catch(saveFailed);
  };

  // The version sent is the one of the text the dialog showed, never the
  // server's: echoing the server's would accept whatever it moved to since
  // this bundle loaded. A failure leaves the dialog open for the retry.
  const agree = (level: AgreementLevel) => {
    void update
      .mutateAsync({ level, agreement_version: TRADING_AGREEMENT_VERSION })
      .then(() => setAgreeing(null), saveFailed);
  };

  // A level that saves on the click shows as chosen while it is on its way,
  // and goes back if the server refuses it. One behind the agreement is shown
  // by its dialog instead.
  const pendingLevel =
    update.isPending && update.variables && !skipsApproval(update.variables.level)
      ? update.variables.level
      : null;
  const shown = pendingLevel ?? data?.level;
  const agreedDate = formatAgreedAt(data?.agreed_at ?? null, locale);

  return (
    <section
      ref={sectionRef}
      id={TRADING_PERMISSION_ANCHOR}
      aria-labelledby={headingId}
      className="scroll-mt-4 space-y-3"
    >
      <div>
        <div className="flex items-baseline justify-between gap-3">
          <h3 id={headingId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('settings.tradingPermission.title')}
          </h3>
          {pendingLevel && (
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('common.saving')}
            </span>
          )}
        </div>
        <p id={descId} className="mt-0.5 text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('settings.tradingPermission.desc')}
        </p>
      </div>

      {/* Also over cards already drawn: a reread after a save whose answer was
          lost keeps the old level on screen, and the save may have landed. */}
      {permission.isError && (
        <div className="flex items-center gap-2 text-xs">
          <span style={{ color: 'var(--color-loss)' }}>{t('settings.tradingPermission.loadFailed')}</span>
          <button
            type="button"
            onClick={() => void permission.refetch()}
            className="underline underline-offset-2"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('common.retry')}
          </button>
        </div>
      )}

      {permission.isLoading && (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.loading')}</p>
      )}

      {loaded && (
        <div role="radiogroup" aria-labelledby={headingId} aria-describedby={descId} className="flex flex-col gap-2">
          {TRADING_LEVELS.map((level) => (
            <LevelCard
              key={level}
              name={groupName}
              level={level}
              checked={shown === level}
              agreedDate={shown === level && skipsApproval(level) ? agreedDate : null}
              onSelect={select}
            />
          ))}
        </div>
      )}

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
    </section>
  );
}

function LevelCard({
  name,
  level,
  checked,
  agreedDate,
  onSelect,
}: {
  name: string;
  level: TradingLevel;
  checked: boolean;
  /** Set on the chosen card of a level that needed the agreement. */
  agreedDate: string | null;
  onSelect: (level: TradingLevel) => void;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const descId = useId();
  const info = TRADING_LEVEL_INFO[level];

  // Named by its label alone, so the description is read as one rather than
  // as part of the name.
  return (
    <RadioChoice
      name={name}
      value={level}
      checked={checked}
      onSelect={onSelect}
      aria-labelledby={labelId}
      aria-describedby={descId}
      markClassName="mt-0.5"
      className={cn(
        'items-start gap-3 rounded-lg px-3 py-2.5 transition-colors',
        checked ? 'bg-(--color-bg-tag)' : 'bg-(--color-bg-card) hover:bg-(--color-bg-hover)',
      )}
      style={{ border: `1px solid ${checked ? 'var(--color-text-primary)' : 'var(--color-border-muted)'}` }}
    >
      <span className="min-w-0 flex-1">
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span
            id={labelId}
            className="inline-flex items-center gap-1.5 text-[0.8125rem] font-medium"
            style={{ color: info.warning ? 'var(--color-warning)' : 'var(--color-text-primary)' }}
          >
            {info.warning && <AlertTriangle className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />}
            {t(info.labelKey)}
          </span>
          {level === DEFAULT_TRADING_LEVEL && (
            <span
              className="rounded-full px-1.5 text-[0.625rem] font-medium leading-4"
              style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
            >
              {t('settings.tradingPermission.defaultTag')}
            </span>
          )}
        </span>
        <span id={descId} className="mt-0.5 block text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
          {t(info.descriptionKey)}
        </span>
        {agreedDate && (
          <span className="mt-1.5 block text-[0.6875rem]" style={{ color: 'var(--color-text-quaternary)' }}>
            {t('settings.tradingPermission.agreedOn', { date: agreedDate })}
          </span>
        )}
      </span>
    </RadioChoice>
  );
}
