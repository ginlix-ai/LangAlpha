import { useEffect, useEffectEvent, useId, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { AlertTriangle, ArrowRight, Ban, ListChecks, Monitor, ShieldCheck, type LucideIcon } from 'lucide-react';
import { ListSkeleton } from '@/components/mcp/McpPrimitives';
import { TradingLevelAgreedOn, TradingLevelName } from '@/components/trading/TradingLevelText';
import { OptionCard, OptionGroup } from '@/components/ui/option-card';
import { WizardSection } from '@/components/ui/wizard';
import { useLocale } from '@/hooks/useLocale';
import { useTradingPermissionChoice } from '@/hooks/useTradingPermissionChoice';
import { brokerageArt } from '@/lib/brandArt';
import { formatList } from '@/lib/format';
import { TRADING_LEVELS, TRADING_LEVEL_INFO } from '@/lib/tradingPermission';
import { cn } from '@/lib/utils';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { BrokerageCard } from '@/pages/Plugins/components/BrokerageCard';
import { DesktopDownloadLink } from '@/pages/Plugins/components/DesktopDownloadLink';
import { LoadFailed } from '@/pages/Plugins/components/LoadFailed';
import { RowNote } from '@/pages/Plugins/components/RowNote';
import type { TradingLevel } from '@/types/api';
import { useBrokerageConnect } from '@/pages/Plugins/hooks/useBrokerageConnect';
import { spendConnectLanding, useWelcomeLanding } from '@/pages/Plugins/hooks/useConnectLanding';
import { welcomeUrl } from '../paths';
import { useBrokers, type ResolvedBroker } from '../useBrokers';

/**
 * Connect a brokerage, or pass. Each broker is the Brokerages tab's own card,
 * so what the agent could reach there and how far it could go with an order
 * read here as they do there, before the choice rather than after it.
 *
 * Connecting leaves the app for the vendor and comes back to this screen (the
 * draft keeps the answers around it), so this is also where the round trip
 * lands: a connect that worked shows its card as connected, one that failed
 * or was backed out of says so under the cards.
 *
 * Under the cards, a broker only the desktop app can connect is named with
 * where to get it. Once one is connected, the trading permission it is held
 * to is asked too: the Settings choice itself, so it is set before the first
 * order rather than found after it. Until then the way on is not a skip in
 * the footer but the question under the cards, which leads to asking where
 * the user's accounts are instead (`onElsewhere`).
 */
export function BrokerageStep({
  labelledBy,
  onElsewhere,
  onPreparing,
}: {
  labelledBy: string;
  onElsewhere: () => void;
  /** Handed each trading level save as it starts, for Start to wait on. */
  onPreparing: (work: Promise<void>) => void;
}) {
  const { t } = useTranslation();
  const brokers = useBrokers();
  const { rows, desktopOnly, shellOutdated, loading, offersError, catalogError } = brokers;
  const [failure, setFailure] = useState<{ server: string; reasonKey: string | null } | null>(null);
  const { oauth, requestConnect, toggle, togglingName } = useBrokerageConnect({
    returnTo: welcomeUrl({ step: 'brokerage' }),
  });

  // What came of a connect this screen sent out, as the shell read it off the
  // landing. A failure with no reason is one backed out of at the vendor.
  const landed = useWelcomeLanding();
  const take = useEffectEvent((taken: NonNullable<typeof landed>) => {
    if (taken.outcome === 'failed') {
      setFailure({ server: taken.server, reasonKey: taken.reasonKey });
      return;
    }
    setFailure(null);
    // The card turns connected on the catalog's next read.
    void brokers.refetchCatalog();
  });
  useEffect(() => {
    if (landed && spendConnectLanding(landed.id)) take(landed);
  }, [landed]);

  const listError = offersError ?? catalogError;
  const failedLabel = failure ? (brokers.resolved.get(failure.server)?.label ?? failure.server) : null;
  // What the agent may do with orders means something once there is an
  // account for it to act in, so it is asked only then.
  const anyConnected = !listError && brokers.connected.length > 0;

  return (
    <div className="@container">
      {listError ? (
        <LoadFailed
          what={t(offersError ? 'plugins.loadFailed.brokerages' : 'plugins.loadFailed.mcp')}
          error={listError}
          onRetry={() => {
            if (offersError) void brokers.refetchOffers();
            if (catalogError) void brokers.refetchCatalog();
          }}
        />
      ) : loading ? (
        <ListSkeleton rows={3} />
      ) : rows.length === 0 ? (
        // Brokers this window cannot connect are said under the list instead.
        !shellOutdated &&
        desktopOnly.length === 0 && (
          <p className="text-sm text-(--color-text-secondary)">{t('onboarding.welcome.brokerage.none')}</p>
        )
      ) : (
        <ul
          aria-labelledby={labelledBy}
          // Two across once the card under the agent's line has the room,
          // read off the card rather than the window: it sits in the chat's
          // column at any window width.
          className={cn('grid grid-cols-1 gap-3', rows.length > 1 && '@[38rem]:grid-cols-2')}
        >
          {rows.map((broker) => (
            // The card fills its cell, so cards on one row end level.
            <li key={broker.b.name} className="flex [&>*]:flex-1">
              <BrokerageCard
                broker={broker}
                connecting={oauth.connectingName === broker.b.name}
                toggling={togglingName === broker.b.name}
                onConnect={() => {
                  setFailure(null);
                  requestConnect(broker.b, broker.row);
                }}
                // Picking a broker that was switched off is the ask to use it,
                // and its tokens are intact, so it comes back on without a
                // second consent.
                onToggle={(enabled) => {
                  if (!enabled) return;
                  setFailure(null);
                  void toggle(broker.b, broker.row, true);
                }}
              />
            </li>
          ))}
        </ul>
      )}

      {failedLabel && (
        <div className="mt-3 flex flex-col gap-0.5" role="alert">
          <RowNote icon={AlertTriangle} tone="warning">
            {t('onboarding.welcome.brokerage.failed', { brokerage: failedLabel })}
          </RowNote>
          {failure?.reasonKey && (
            <p className="pl-4 text-[0.6875rem] text-(--color-text-tertiary)">{t(failure.reasonKey)}</p>
          )}
        </div>
      )}
      {!loading && !listError && <DesktopNote brokers={desktopOnly} outdated={shellOutdated} />}
      {anyConnected && <TradingPermissionPick onPreparing={onPreparing} />}
      {!loading && !anyConnected && <ElsewhereRow onClick={onElsewhere} />}
    </div>
  );
}

/** The way on for a user who connects none of these, set apart under the
 *  cards so it reads as the other answer rather than as giving up. */
function ElsewhereRow({ onClick }: { onClick: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="mt-5 flex flex-col gap-3 border-t border-(--color-border-muted) pt-4 @[32rem]:flex-row @[32rem]:items-center @[32rem]:justify-between @[32rem]:gap-6">
      <div className="min-w-0">
        <p className="text-sm font-medium text-(--color-text-primary)">
          {t('onboarding.welcome.brokerage.elsewhereTitle')}
        </p>
        <p className="mt-0.5 text-[0.8125rem] text-(--color-text-secondary)">
          {t('onboarding.welcome.brokerage.elsewhereBody')}
        </p>
      </div>
      <button
        type="button"
        onClick={onClick}
        data-testid="welcome-elsewhere"
        className="group inline-flex h-10 shrink-0 items-center justify-center gap-1.5 rounded-md border border-(--color-border-default) px-4 text-sm font-medium text-(--color-text-primary) press hover:bg-(--color-bg-hover)"
      >
        <span>{t('onboarding.welcome.brokerage.elsewhereAction')}</span>
        <ArrowRight
          aria-hidden="true"
          className="h-3.5 w-3.5 transition-transform motion-safe:group-hover:translate-x-0.5"
        />
      </button>
    </div>
  );
}

/**
 * Where a broker this window cannot connect can be: Robinhood accepts a
 * sign-in only from the desktop app, and a desktop app too old to finish one
 * connects nothing. Said under the cards rather than as a card of its own,
 * whose Connect would do nothing; the broker's mark leads, so someone looking
 * for theirs finds it.
 */
function DesktopNote({ brokers, outdated }: { brokers: ResolvedBroker[]; outdated: boolean }) {
  const locale = useLocale();
  if (!outdated && brokers.length === 0) return null;
  const names = formatList(
    brokers.map((b) => b.label),
    locale,
  );
  return (
    <p
      data-testid="welcome-desktop-note"
      className="mt-4 flex items-center gap-2.5 text-[0.8125rem] text-(--color-text-secondary)"
    >
      {outdated ? (
        <Monitor aria-hidden="true" className="h-4 w-4 shrink-0 text-(--color-text-tertiary)" />
      ) : (
        <span aria-hidden="true" className="flex shrink-0 -space-x-1.5">
          {brokers.map(({ b, vendor }) => (
            <BrandMark key={b.name} name={b.name} kind="server" size="xs" art={brokerageArt(vendor)} />
          ))}
        </span>
      )}
      <span>
        <Trans
          i18nKey={
            outdated ? 'onboarding.welcome.brokerage.desktopOutdated' : 'onboarding.welcome.brokerage.desktopOnly'
          }
          values={{ brokerages: names }}
          // Not `link`: the Trans parser reads that as the void HTML element
          // and drops the words inside it.
          components={{ download: <DesktopDownloadLink /> }}
        />
      </span>
    </p>
  );
}

/** A glyph per level, in the order they give the agent room: the last one is
 *  the warning the level carries everywhere it is named. */
const LEVEL_ICONS: Record<TradingLevel, LucideIcon> = {
  no_trading: Ban,
  approve_each: ShieldCheck,
  plan_first: ListChecks,
  autonomous: AlertTriangle,
};

/**
 * How far the agent may go with orders, asked once a broker is connected and
 * so beside the account it applies to. The Settings choice itself
 * (`useTradingPermissionChoice`): a pick saves as it does there, and the two
 * levels that let orders skip approval ask for the same agreement first. Drawn
 * as this flow's own cards, two across where the brokers are three, around the
 * level's name and agreed-on line as Settings writes them (`TradingLevelText`).
 */
function TradingPermissionPick({ onPreparing }: { onPreparing: (work: Promise<void>) => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  const { permission, shown, agreedDate, select, dialog } = useTradingPermissionChoice({ onPreparing });
  // The default holds until a level is read, and a level that could not be
  // read is Settings' to retry: this is a question the flow can do without.
  if (permission.data === undefined) return null;
  return (
    // Fades in when a broker switched back on here is the first connected.
    <WizardSection title={t('settings.tradingPermission.title')} titleId={titleId} className="wizard-fade-in mt-10">
      <p className="-mt-1.5 mb-3 text-[0.8125rem] text-(--color-text-secondary)">
        {t('onboarding.welcome.brokerage.permissionHelper')}
      </p>
      <OptionGroup<TradingLevel>
        value={shown ?? null}
        onChange={select}
        labelledBy={titleId}
        // Two across where the card has the room, as the brokers are.
        className="@[38rem]:grid-cols-2"
      >
        {TRADING_LEVELS.map((level) => {
          const info = TRADING_LEVEL_INFO[level];
          const Icon = LEVEL_ICONS[level];
          return (
            <OptionCard
              key={level}
              value={level}
              icon={<Icon className="h-4 w-4" style={info.warning ? { color: 'var(--color-warning)' } : undefined} />}
              // The glyph carries the warning, so the name stays plain.
              title={<TradingLevelName level={level} />}
              description={t(info.descriptionKey)}
              testId={`welcome-trading-${level}`}
            >
              <TradingLevelAgreedOn
                level={level}
                chosen={shown === level}
                agreedDate={agreedDate}
                className="text-(--color-text-tertiary)"
              />
            </OptionCard>
          );
        })}
      </OptionGroup>
      {dialog}
    </WizardSection>
  );
}
