import { useId, type Dispatch, type ReactNode, type SetStateAction } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { ImageUp, Search } from 'lucide-react';
import { ChipGroup } from '@/components/ui/chip-group';
import { Input } from '@/components/ui/input';
import { OptionCard, OptionGroup } from '@/components/ui/option-card';
import { WizardSection } from '@/components/ui/wizard';
import { useLocale } from '@/hooks/useLocale';
import { formatList } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { ElsewhereAnswers } from '../draft';
import {
  BROKERAGES,
  HOLDINGS_METHODS,
  PROFILE_FIELDS,
  isOneOf,
  pickBrokerages,
  type BrokerageKey,
  type HoldingsMethod,
  type InvestingProfile,
} from '../profile';
import type { Screenshot } from '../screenshots';
import { useBrokers } from '../useBrokers';
import { HoldingsEntry } from './HoldingsEntry';
import { ScreenshotDrop } from './ScreenshotDrop';

const METHOD_ICONS: Record<HoldingsMethod, ReactNode> = {
  screenshot: <ImageUp className="h-4 w-4" />,
  search: <Search className="h-4 w-4" />,
};

interface OtherBrokerageStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  onChange: (patch: Partial<InvestingProfile>) => void;
  /** The name typed for a brokerage the list lacks, the way the holdings come
   *  in, and the screenshots added. */
  answers: ElsewhereAnswers;
  onAnswers: Dispatch<SetStateAction<ElsewhereAnswers>>;
  /** Handed each screenshot add and each holdings write as it starts, for
   *  Start to wait on. */
  onPreparing: (work: Promise<void>) => void;
  /** Back to the brokers that can be connected. */
  onConnect: () => void;
}

/**
 * The brokerage screen's other half, for a user who connects none: where
 * their accounts are, and how what they hold reaches the agent instead.
 * Screenshots lead, since a brokerage app shows positions and watchlists on
 * one screen each and the agent can read them; search adds one row at a time.
 * Nothing is picked to begin with, and picking the picked way again puts it
 * down; picking neither leaves the holdings for chat later. Nothing here is
 * required.
 *
 * The brokerages are saved with the profile, by key; one the list lacks is
 * named only in the opening message. Screenshots ride that message too, so
 * they are held for this visit alone. A searched holding is written as it is
 * added, as on the dashboard.
 */
export function OtherBrokerageStep({
  labelledBy,
  profile,
  onChange,
  answers: { otherName, method, shots },
  onAnswers,
  onPreparing,
  onConnect,
}: OtherBrokerageStepProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const otherId = useId();
  const methodsId = useId();
  const { rows } = useBrokers();
  const picked = profile.brokerages;
  const setMethod = (next: HoldingsMethod | null) => onAnswers((a) => ({ ...a, method: next }));
  const setShots: Dispatch<SetStateAction<Screenshot[]>> = (update) =>
    onAnswers((a) => ({ ...a, shots: typeof update === 'function' ? update(a.shots) : update }));

  // One picked here that this window can connect is better connected: the
  // agent then reads the holdings itself, and keeps reading them.
  const connectable = rows.filter(
    ({ b, connected }) => !connected && isOneOf(BROKERAGES, b.name) && picked.includes(b.name),
  );

  return (
    <>
      <ChipGroup<BrokerageKey>
        values={picked}
        onChange={(next) => onChange({ brokerages: pickBrokerages(picked, next) })}
        labelledBy={labelledBy}
        options={BROKERAGES.map((key) => ({ value: key, label: PROFILE_FIELDS.brokerages.label(t, key) }))}
      />
      {picked.includes('other') && (
        <div className="wizard-fade-in mt-3 sm:max-w-[320px]">
          <label htmlFor={otherId} className="sr-only">
            {t('onboarding.welcome.elsewhere.otherLabel')}
          </label>
          <Input
            id={otherId}
            value={otherName}
            onChange={(e) => onAnswers((a) => ({ ...a, otherName: e.target.value }))}
            placeholder={t('onboarding.welcome.elsewhere.otherPlaceholder')}
            autoComplete="off"
            maxLength={80}
            data-testid="welcome-elsewhere-other"
          />
        </div>
      )}
      {connectable.length > 0 && (
        <p
          data-testid="welcome-elsewhere-connectable"
          className="wizard-fade-in mt-3 text-[0.8125rem] text-(--color-text-secondary)"
        >
          <Trans
            i18nKey="onboarding.welcome.elsewhere.connectable"
            values={{ brokerages: formatList(connectable.map((b) => b.label), locale) }}
            components={{
              connect: (
                <button
                  type="button"
                  onClick={onConnect}
                  className="font-medium text-(--color-text-primary) underline decoration-(--color-border-default) underline-offset-2 transition-colors hover:decoration-(--color-text-primary)"
                />
              ),
            }}
          />
        </p>
      )}

      <WizardSection
        title={t('onboarding.welcome.elsewhere.holdingsTitle')}
        titleId={methodsId}
        hint={t('onboarding.welcome.optional')}
        className="mt-10"
      >
        <OptionGroup<HoldingsMethod>
          value={method}
          onChange={setMethod}
          onClear={() => setMethod(null)}
          labelledBy={methodsId}
        >
          {HOLDINGS_METHODS.map((value) => (
            <OptionCard
              key={value}
              value={value}
              icon={METHOD_ICONS[value]}
              title={t(`onboarding.welcome.elsewhere.methods.${value}.title`)}
              description={t(`onboarding.welcome.elsewhere.methods.${value}.desc`)}
              testId={`welcome-method-${value}`}
            />
          ))}
        </OptionGroup>
        {/* Both ways in share one cell under the cards, the one not picked
            kept in place unseen, so the cell is as tall as the taller and a
            switch only fades one into the other: nothing above it moves, and
            nothing below the pointer jumps when a card is picked. With none
            picked there is no cell, or it would stand empty at the taller
            panel's height; it fades in when a way is picked. */}
        {method !== null && (
          <div className="wizard-fade-in mt-3 grid">
            <MethodPanel value="screenshot" open={method === 'screenshot'}>
              <ScreenshotDrop
                shots={shots}
                onChange={setShots}
                onPreparing={onPreparing}
                active={method === 'screenshot'}
              />
            </MethodPanel>
            <MethodPanel value="search" open={method === 'search'}>
              <HoldingsEntry onPreparing={onPreparing} />
            </MethodPanel>
          </div>
        )}
      </WizardSection>
    </>
  );
}

/** One way in's panel, in the shared cell: hidden ones keep their room but
 *  are out of reach of the keyboard and screen readers. */
function MethodPanel({ value, open, children }: { value: HoldingsMethod; open: boolean; children: ReactNode }) {
  return (
    <div
      data-testid={`welcome-method-${value}-panel`}
      data-state={open ? 'open' : 'closed'}
      inert={!open}
      aria-hidden={open ? undefined : true}
      className={cn(
        '[grid-area:1/1] transition-[opacity,translate,visibility] ease-reveal motion-reduce:transition-none',
        // The one leaving is nearly gone before the next comes in, so the two
        // are never read over each other.
        open
          ? 'visible translate-y-0 opacity-100 duration-quick delay-(--motion-exit)'
          : 'invisible opacity-0 duration-exit motion-safe:translate-y-1',
      )}
    >
      {children}
    </div>
  );
}
