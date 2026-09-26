import { useId, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import TimezonePicker from '@/components/TimezonePicker';
import { Input } from '@/components/ui/input';
import { useHomeTimezone } from '@/hooks/useHomeTimezone';
import { fixed2, formatTimezoneName, signedFixed2 } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { TriggerType } from '@/types/automation';
import { usePriceWatch } from '../hooks/usePriceWatch';
import { cronToHuman, DEFAULT_CRON, parseSchedule, scheduleDays } from '../utils/cron';
import { type FormPatch, type FormState, formToPriceConfig } from '../utils/form';
import {
  PRICE_CONDITION_TYPES,
  PRICE_REFERENCE_OPTIONS,
  distanceLabel,
  isPctCondition,
  quoteMove,
} from '../utils/price';
import { formatDateTimeShort } from '../utils/time';
import { MeterMark, OnceMark, WeekStrip } from './CadenceMarks';
import CronScheduleBuilder from './CronScheduleBuilder';
import DateTimePicker from './DateTimePicker';
import FormRow from './FormRow';
import OptionSelect from './OptionSelect';
import { PriceMeter } from './PriceMeter';
import TickerAutocomplete from './TickerAutocomplete';

const TRIGGERS: { id: TriggerType; labelKey: string }[] = [
  { id: 'cron', labelKey: 'automation.triggerSchedule' },
  { id: 'once', labelKey: 'automation.once' },
  { id: 'price', labelKey: 'automation.triggerPrice' },
];

/** The three ways an automation can start, each drawn with the mark the
 *  rest of the page uses for it. The schedule's week follows the schedule
 *  being built below it.
 *
 *  An edit shows only the kind it has, fixed: the server keeps an
 *  automation's kind for its life, so the other two are not choices. */
function TriggerChoice({
  value,
  cron,
  labelledBy,
  locked,
  onChange,
}: {
  value: TriggerType;
  cron: string;
  labelledBy: string;
  locked: boolean;
  onChange: (value: TriggerType) => void;
}) {
  const { t } = useTranslation();
  const shown = locked ? TRIGGERS.filter((tr) => tr.id === value) : TRIGGERS;
  return (
    <div className="automation-trigger-choice" role="group" aria-labelledby={labelledBy}>
      {shown.map((tr) => (
        <button
          key={tr.id}
          type="button"
          aria-pressed={value === tr.id}
          disabled={locked}
          data-locked={locked || undefined}
          className="automation-trigger-option"
          onClick={() => onChange(tr.id)}
        >
          {tr.id === 'cron' ? (
            <WeekStrip days={scheduleDays(parseSchedule(cron || DEFAULT_CRON))} />
          ) : tr.id === 'once' ? (
            <OnceMark />
          ) : (
            <MeterMark />
          )}
          <span className="automation-trigger-label">{t(tr.labelKey)}</span>
        </button>
      ))}
    </div>
  );
}

/** The ticker, the kind of move, and how far it has to go. */
function PriceFields({ form, patch }: { form: FormState; patch: FormPatch }) {
  const { t } = useTranslation();
  const pct = isPctCondition(form.price_condition_type);
  return (
    <div className="flex flex-wrap items-center gap-2">
      <TickerAutocomplete
        label={t('automation.priceSymbol')}
        value={form.price_symbol}
        onChange={(symbol) => patch('price_symbol', symbol)}
      />
      <OptionSelect
        aria-label={t('automation.priceCondition')}
        value={form.price_condition_type}
        options={PRICE_CONDITION_TYPES}
        onChange={(type) => patch('price_condition_type', type)}
        className="w-44"
      />
      <div className="relative w-32">
        <Input
          type="number"
          step="any"
          min={0}
          aria-label={t('automation.priceValue')}
          value={form.price_value}
          onChange={(e) => patch('price_value', e.target.value)}
          placeholder={pct ? '5' : '150.00'}
          required
          className={cn('automation-form-level font-mono tabular-nums', pct ? 'pr-7' : 'pl-6')}
        />
        <span className={cn('automation-mono automation-form-unit', pct ? 'right-2.5' : 'left-2.5')} aria-hidden="true">
          {pct ? '%' : '$'}
        </span>
      </div>
      {/* One unit, so a wrap never strands the reference on a line alone. */}
      {pct && (
        <span className="flex items-center gap-2">
          <span className="automation-form-joiner">{t('automation.pctFrom')}</span>
          <OptionSelect
            aria-label={t('automation.priceReference')}
            value={form.price_reference}
            options={PRICE_REFERENCE_OPTIONS}
            onChange={(reference) => patch('price_reference', reference)}
            className="w-40"
          />
        </span>
      )}
    </div>
  );
}

/** What the trigger as set would do, in words: the schedule and its time
 *  zone, the moment of a one-time run, or how far a price has to travel. */
function TriggerReadout({ form }: { form: FormState }) {
  useTranslation();
  if (form.trigger_type === 'price') return <PriceReadout form={form} />;
  if (form.trigger_type === 'once') {
    // Until a moment is picked the field's own placeholder asks for one.
    const when = formatDateTimeShort(form.next_run_at, form.timezone);
    if (!when) return null;
    return <p className="automation-form-readout">{[when, formatTimezoneName(form.timezone)].join(' · ')}</p>;
  }
  if (!form.cron_expression) return null;
  return (
    <p className="automation-form-readout">
      {[cronToHuman(form.cron_expression), formatTimezoneName(form.timezone)].filter(Boolean).join(' · ')}
    </p>
  );
}

/** Once a ticker is picked the line shows where it trades, before any level
 *  is set; with a level it becomes the meter. A ticker the quote layer has
 *  nothing for says so, which is the typo check the field otherwise lacks. */
function PriceReadout({ form }: { form: FormState }) {
  const { t } = useTranslation();
  const cfg = useMemo(() => formToPriceConfig(form), [form]);
  const watch = usePriceWatch(cfg);

  if (watch.reading) {
    return (
      <div className="automation-form-meter">
        <p className="automation-form-readout">{distanceLabel(watch.reading, t)}</p>
        <PriceMeter reading={watch.reading} symbol={cfg.symbol} />
      </div>
    );
  }

  if (watch.symbol && watch.quote?.price) {
    const move = quoteMove(cfg, watch.quote);
    return (
      <p className="automation-form-readout automation-form-quote">
        <span className="automation-mono automation-form-quote-symbol">{watch.symbol}</span>
        <span className="automation-mono automation-form-quote-price">{fixed2(watch.quote.price)}</span>
        {move && (
          <span>
            <span
              className="automation-mono"
              style={{ color: move.pct >= 0 ? 'var(--color-profit)' : 'var(--color-loss)' }}
            >
              {signedFixed2(move.pct)}%
            </span>{' '}
            {t(move.from === 'day_open' ? 'automation.quoteSinceOpen' : 'automation.quoteToday')}
          </span>
        )}
      </p>
    );
  }

  if (watch.symbol && !watch.settled) {
    return (
      <p className="automation-form-readout automation-form-quote">
        <span className="automation-mono automation-form-quote-symbol">{watch.symbol}</span>
        <span aria-hidden="true">…</span>
      </p>
    );
  }

  return (
    <p className="automation-form-readout">
      {watch.symbol ? t('automation.noQuote', { symbol: watch.symbol }) : t('automation.priceHint')}
    </p>
  );
}

interface TriggerSectionProps {
  form: FormState;
  patch: FormPatch;
  /** Editing an automation, whose kind of trigger cannot change. */
  isEdit: boolean;
  /** A zone change moves more than the zone (see `withZone`), so it is the
   *  form's to apply. */
  onZone: (timezone: string) => void;
}

/** The "When" row: which kind of trigger, its settings, and a line saying
 *  what they add up to. */
export default function TriggerSection({ form, patch, isEdit, onZone }: TriggerSectionProps) {
  const { t } = useTranslation();
  const labelId = useId();
  const homeZone = useHomeTimezone();
  const zoneSelect = (
    <TimezonePicker
      value={form.timezone}
      onChange={onZone}
      home={{ zone: homeZone, label: t('timezone.yours') }}
      className="min-w-40 max-w-64"
    />
  );

  return (
    <FormRow label={t('automation.fieldWhen')} labelId={labelId}>
      <TriggerChoice
        value={form.trigger_type}
        cron={form.cron_expression}
        labelledBy={labelId}
        locked={isEdit}
        onChange={(type) => patch('trigger_type', type)}
      />

      <div className="automation-form-trigger">
        {form.trigger_type === 'cron' && (
          <CronScheduleBuilder
            value={form.cron_expression}
            onChange={(cron) => patch('cron_expression', cron)}
            zone={zoneSelect}
            labelledBy={labelId}
          />
        )}

        {form.trigger_type === 'once' && (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <DateTimePicker
              value={form.next_run_at}
              onChange={(iso) => patch('next_run_at', iso)}
              timeZone={form.timezone}
              labelledBy={labelId}
            />
            <div className="flex items-center gap-2">
              <span className="automation-form-joiner">{t('automation.inZone')}</span>
              {zoneSelect}
            </div>
          </div>
        )}

        {form.trigger_type === 'price' && <PriceFields form={form} patch={patch} />}

        <TriggerReadout form={form} />
      </div>
    </FormRow>
  );
}
