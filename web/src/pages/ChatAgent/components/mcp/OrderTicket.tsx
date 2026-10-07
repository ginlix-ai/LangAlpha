import React from 'react';
import { useTranslation } from 'react-i18next';
import { instrumentLabel } from '@/components/orders/instrument';
import { OPTION_RIGHT_KEY, orderWordKey } from '@/components/orders/labels';
import { cn } from '@/lib/utils';
import type { OrderInstrument } from '@/types/orders';
import { ORDER_REF_KEY, type OrderPart, type OrderTicketView } from './orderSummary';

/**
 * The order itself, the same on the card that asks and on the receipt: which
 * way and on what as a headline, how much and at what price as figures beside
 * it, and everything else in a quieter line under the headline.
 *
 * The figures sit beside the headline while the card is wide enough and drop
 * under it when it is not, rather than squeezing: a price split across two
 * lines is easy to misread, and a narrow thread is where people approve from
 * a phone.
 */
export function OrderTicket({ ticket }: { ticket: OrderTicketView }): React.ReactElement | null {
  const { side, instrument, targetRef, meta, figures } = ticket;
  const headline = !!(side || instrument || targetRef);
  if (!headline && meta.length === 0 && figures.length === 0) return null;
  return (
    <div
      className="flex flex-wrap items-start justify-between gap-x-7 gap-y-2.5"
      data-testid="order-ticket"
    >
      <div className="min-w-0 grow basis-[220px]">
        {headline && <OrderHeadline side={side} instrument={instrument} targetRef={targetRef} />}
        {meta.length > 0 && <OrderMeta parts={meta} />}
      </div>
      {figures.length > 0 && <OrderFigures parts={figures} />}
    </div>
  );
}

/** A part's text: the catalog's word or sentence when there is one, and the
 *  raw value otherwise, so a term the catalog lacks still says something. */
export function OrderPartValue({ part }: { part: OrderPart }): React.ReactElement {
  const { t } = useTranslation();
  return <>{part.valueKey ? t(part.valueKey, part.valueParams) : part.value}</>;
}

/**
 * The vendor id an amend acts on, drawn whole: this is the string the user
 * matches against the order in the broker's own app, and an elided one cannot
 * be matched.
 */
export function OrderTargetRef({ value }: { value: string }): React.ReactElement {
  return (
    <span className="font-mono break-all" data-testid="order-target-ref">
      {value}
    </span>
  );
}

/**
 * The side in words and in the primary ink, never in red or green: on this
 * app those mean loss and profit, and a Chinese-market reader takes red for
 * up, so a coloured "Buy" says something different to each of them.
 */
function OrderHeadline({
  side,
  instrument,
  targetRef,
}: {
  side: OrderPart | null;
  instrument: OrderInstrument | null;
  targetRef: string | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const subject = instrument ? (
    <InstrumentName instrument={instrument} />
  ) : targetRef ? (
    // A cancel or a confirm names no instrument, and without the id the card
    // would ask a person to approve cancelling nothing in particular.
    <span className="font-medium">
      {t(ORDER_REF_KEY)} <OrderTargetRef value={targetRef} />
    </span>
  ) : null;
  return (
    <div
      className="text-[0.9375rem] font-semibold leading-snug [overflow-wrap:anywhere]"
      style={{ color: 'var(--color-text-primary)' }}
      data-testid="order-headline"
    >
      {side && <OrderPartValue part={side} />}
      {side && subject && ' '}
      {subject}
    </div>
  );
}

/** The instrument at headline weight. An option's contract (expiry, strike,
 *  call or put) is one thing to check, so it is set as one, a step lighter
 *  than the underlying it is a contract on. */
function InstrumentName({ instrument }: { instrument: OrderInstrument }): React.ReactElement {
  const { t } = useTranslation();
  switch (instrument.kind) {
    case 'equity':
      return <>{instrument.symbol}</>;
    case 'option': {
      const rightKey = orderWordKey(OPTION_RIGHT_KEY, instrument.right);
      const contract = [
        instrument.expiration,
        instrument.strike,
        rightKey ? t(rightKey) : instrument.right,
      ]
        .filter(Boolean)
        .join(' ');
      return (
        <>
          {instrument.underlying}
          {contract && (
            <>
              {' '}
              <span className="font-medium">{contract}</span>
            </>
          )}
        </>
      );
    }
    case 'future':
      return <>{[instrument.symbol, instrument.contract_month].filter(Boolean).join(' ')}</>;
    default:
      return <>{instrumentLabel(instrument)}</>;
  }
}

/** Everything about the order that is not a figure: what kind of thing it is,
 *  where, for how long, and the note. Quiet, because a person checks these
 *  after the side, the size and the price, not before.
 *
 *  The note gets a line of its own. It is free text of any length, and run in
 *  with the short words it wraps mid-list and leaves a separator dangling at
 *  the start of the next line. */
function OrderMeta({ parts }: { parts: OrderPart[] }): React.ReactElement {
  const { t } = useTranslation();
  const words = parts.filter((part) => part.field !== 'note');
  const note = parts.find((part) => part.field === 'note');
  return (
    <div className="mt-0.5 text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
      {words.length > 0 && (
        // Every word carries its dot in front, and the row starts one dot to
        // the left of the clip, so a line the row wraps onto never opens on a
        // dot. The dot sits in the word's padding, which keeps a word that
        // wraps inside itself clear of the clip.
        <div className="overflow-hidden">
          <div className="-ml-4 flex flex-wrap items-baseline">
            {words.map((part) => (
              <span key={part.field} className="relative min-w-0 pl-4 [overflow-wrap:anywhere]">
                <span
                  aria-hidden="true"
                  className="absolute left-0 top-0 w-4 text-center"
                  style={{ color: 'var(--color-text-quaternary)' }}
                >
                  ·
                </span>
                {part.labelKey && <>{t(part.labelKey)} </>}
                {/* The label stays quiet; the word is what gets read. */}
                <span style={{ color: 'var(--color-text-secondary)' }}>
                  {part.field === 'target_ref' ? (
                    <OrderTargetRef value={part.value} />
                  ) : (
                    <OrderPartValue part={part} />
                  )}
                </span>
              </span>
            ))}
          </div>
        </div>
      )}
      {note && (
        <p className="[overflow-wrap:anywhere]" data-testid="order-note">
          {note.labelKey && <>{t(note.labelKey)} </>}
          <span style={{ color: 'var(--color-text-secondary)' }}>{note.value}</span>
        </p>
      )}
    </div>
  );
}

/** Label over value, one column per figure, in a fixed order so the same kind
 *  of number sits in the same place on every card in a thread. */
function OrderFigures({ parts }: { parts: OrderPart[] }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <dl className="flex flex-wrap gap-x-6 gap-y-2 min-w-0" data-testid="order-figures">
      {parts.map((part) => {
        const total = part.field === 'total';
        return (
          <div key={part.field} className="min-w-0">
            <dt
              className="text-[0.6875rem] leading-4 whitespace-nowrap"
              style={{ color: total ? 'var(--color-text-secondary)' : 'var(--color-text-tertiary)' }}
            >
              {part.labelKey && t(part.labelKey)}
            </dt>
            <dd
              className={cn('text-sm tabular-nums [overflow-wrap:anywhere]', total && 'font-medium')}
              style={{ color: 'var(--color-text-primary)' }}
            >
              <OrderPartValue part={part} />
            </dd>
          </div>
        );
      })}
    </dl>
  );
}
