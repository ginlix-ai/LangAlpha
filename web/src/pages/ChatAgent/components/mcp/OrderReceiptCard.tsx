import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle } from 'lucide-react';
import { OrderActionLink } from '@/components/orders/OrderActionLink';
import { OrdersPageLink } from '@/components/orders/OrdersPageLink';
import type { TFunction } from 'i18next';
import { orderStatusTone } from '@/components/orders/OrderStatusPill';
import { ORDER_TYPE_KEY, orderWordKey } from '@/components/orders/labels';
import { ORDER_MODE_KEY } from '@/components/orders/mode';
import { orderStatusLabel, orderStatusShortLabel } from '@/components/orders/status';
import type { InlineCardProps } from '../charts/cardReaders';
import { TermCard, TermKV, TermStatus, TermTag } from '../charts/TermCard';
import { useCardNarrow } from '../charts/termContext';
import { DirectToolRowMark } from './DirectToolMark';
import { InstrumentName, OrderPartValue, OrderTargetRef } from './OrderTicket';
import { ORDER_ACTION_KEY, ORDER_REF_KEY, type OrderPart, type OrderTicketView } from './orderSummary';
import { useOrderReceipt, type OrderReceiptView } from './useOrderReceipt';
import './orderReceiptCard.css';

/**
 * What happened to one order, drawn from the attempt's receipt.
 *
 * The receipt rides the tool result, so this is the same card live and on a
 * reload, and it is the only place in chat where an order's end state is
 * stated in words. A rejection, a refusal and a transport failure all carry a
 * receipt too, which is why the outcome is a named verdict rather than a
 * success-or-not: "you rejected this" and "the brokerage rejected this" are
 * different facts and a person reading back a thread needs to tell them apart.
 *
 * It is drawn as the transcript's other data cards are, in `TermCard`, rather
 * than through `OrderCard`: the approval card a person answers keeps its own
 * shape and buttons, and the receipt is a record read beside quotes and
 * filings. The two share the ticket's parts, so the same order reads the same
 * on both. A click opens the call in the panel, where the frame that was sent
 * and the body that came back are; the two ways off it, the vendor's own
 * client and the same attempt on the Orders page, are links and keep their
 * own clicks. `useOrderReceipt` decides what is on it; this draws that.
 */
export function OrderReceiptCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const narrow = useCardNarrow();
  const receipt = useOrderReceipt(artifact);
  if (!receipt) return null;
  const { status, mode, vendorNamedEnd } = receipt;
  const owner = [receipt.vendorLabel, receipt.account].filter(Boolean).join(' · ');
  const ownerLine = owner && (
    <>
      {/* On a card about an order, whose money moves is the first thing to
          read, and the vendor's name alone is quiet. */}
      {receipt.vendor && <DirectToolRowMark server={receipt.vendor} className="order-term-mark h-3 w-3" />}
      {owner}
    </>
  );
  const lead = receipt.ticket ? ticketLead(receipt.ticket) : {};
  // A phone's column is one figure wide, and the ticket line above it already
  // says the size and the price.
  const figures = receiptFigures(receipt).filter(
    (part) =>
      // The ticket line names the order's type whenever it has one.
      part.field !== 'order_type' && (!narrow || (part !== lead.qty && part !== lead.at)),
  );
  const word = t(orderStatusShortLabel(status, mode, vendorNamedEnd));
  const modeWord = mode ? t(ORDER_MODE_KEY[mode]) : null;
  const answered = !!(
    receipt.outcome.decisionMessage ||
    receipt.outcome.failure ||
    receipt.outcome.fill.length ||
    receipt.actionUrl ||
    receipt.attemptId
  );
  return (
    <TermCard
      title={t(ORDER_ACTION_KEY[receipt.action] || ORDER_ACTION_KEY.place)}
      // A phone's edge has room for the action and its state, not the owner
      // too, so the owner opens the body there.
      sub={narrow ? undefined : ownerLine || undefined}
      meta={
        <>
          {/* Said once: a staged instruction's state is itself "Staged". */}
          {mode && modeWord?.toLowerCase() !== word.toLowerCase() && (
            // Amber only for live: it is the one that spends real money.
            <span title={t('plugins.detail.orderModeTitle')}>
              <TermTag tone={mode === 'live' ? 'warning' : undefined}>{modeWord}</TermTag>
            </span>
          )}
          {/* Inline, as the mode tag is, so the pill hugs its word rather than the edge's line. */}
          <span>
            <TermStatus tone={orderStatusTone(status, 'receipt')} testid={`order-status-${status}`}>{word}</TermStatus>
          </span>
        </>
      }
      onClick={onClick}
      testid="order-receipt"
    >
      {narrow && ownerLine && <div className="order-term-owner">{ownerLine}</div>}
      {receipt.ticket && (
        <ReceiptTicket ticket={receipt.ticket} lead={lead} />
      )}
      {(figures.length > 0 || receipt.outcome.orderId || answered) && (
        <div className="order-term-rest">
          <ReceiptFigures
            figures={figures}
            orderId={receipt.outcome.orderId}
            isApproval={receipt.outcome.orderIdIsApproval}
            cols={narrow ? 1 : 3}
          />
          <ReceiptOutcome receipt={receipt} />
        </div>
      )}
    </TermCard>
  );
}

/** A figure's name in a column, shorter where the full label would leave its
 *  value no room. */
function columnLabel(part: OrderPart, t: TFunction): string | null {
  switch (part.field) {
    case 'limit_price':
      return t('toolArtifact.directTool.orderTerm.limit');
    case 'stop_price':
      return t('toolArtifact.directTool.orderTerm.stop');
    case 'avg_fill_price':
      return t('toolArtifact.directTool.orderTerm.avgFill');
    default:
      return part.labelKey ? t(part.labelKey) : null;
  }
}

/** Every figure the receipt has, the order's and the fill's, in one grid. A
 *  fill written as a sentence is not a figure: it is said under the grid. */
function receiptFigures(receipt: OrderReceiptView): OrderPart[] {
  return [
    ...(receipt.ticket?.figures ?? []),
    ...receipt.outcome.fill.filter((part) => part.field !== 'fill'),
  ];
}

interface TicketLead {
  qty?: OrderPart;
  at?: OrderPart;
}

/** The figures the ticket line states: the size, and the price it trades at,
 *  which is the limit where the type takes one (a price the vendor ignores is
 *  not a limit) and the stop otherwise. */
function ticketLead(ticket: OrderTicketView): TicketLead {
  const figure = (field: string) => ticket.figures.find((part) => part.field === field);
  return { qty: figure('qty'), at: figure('limit_price') ?? figure('stop_price') };
}

/**
 * The order as one line, `BUY 10 AAPL @ $227.50 LIMIT`, and what kind of order
 * it is after it in the quietest ink. The side is a word in the primary ink,
 * never red or green: here those mean loss and profit, and a Chinese-market
 * reader takes red for up.
 */
function ReceiptTicket({
  ticket,
  lead: { qty, at },
}: {
  ticket: OrderTicketView;
  lead: TicketLead;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const { side, instrument, targetRef, orderType, meta } = ticket;
  const typeKey = orderWordKey(ORDER_TYPE_KEY, orderType);
  const typeWord = typeKey ? t(typeKey) : orderType;
  const subject = instrument ? (
    <InstrumentName instrument={instrument} />
  ) : targetRef ? (
    // A cancel names no instrument, and without the id the card would say
    // it cancelled nothing in particular.
    <>
      {t(ORDER_REF_KEY)} <OrderTargetRef value={targetRef} />
    </>
  ) : null;
  const words = meta.filter((part) => part.field !== 'note');
  const note = meta.find((part) => part.field === 'note');
  const headline = !!(side || qty || subject);
  if (!headline && !at && !typeWord && words.length === 0 && !note) return null;
  return (
    <div className="order-term-ticket" data-testid="order-ticket">
      <div className="term-line">
        {headline && (
          <span className="term-big order-term-headline" data-testid="order-headline">
            {side && (
              <span className="term-caps">
                <OrderPartValue part={side} />
              </span>
            )}
            {side && (qty || subject) && ' '}
            {qty && <OrderPartValue part={qty} />}
            {qty && subject && ' '}
            {subject}
          </span>
        )}
        {(at || typeWord) && (
          <span className="term-t2">
            {at && (
              <>
                @ <OrderPartValue part={at} />
              </>
            )}
            {at && typeWord && ' '}
            {typeWord && <span className="term-caps">{typeWord}</span>}
          </span>
        )}
        {words.length > 0 && (
          <span className="term-t3 order-term-words">
            {words.map((part, i) => (
              <React.Fragment key={part.field}>
                {i > 0 && <span aria-hidden="true"> · </span>}
                {part.labelKey && <>{t(part.labelKey)} </>}
                {part.field === 'target_ref' ? (
                  // Matched character for character against the broker's
                  // app, so it keeps its own case.
                  <span className="term-code">
                    <OrderTargetRef value={part.value} />
                  </span>
                ) : (
                  <OrderPartValue part={part} />
                )}
              </React.Fragment>
            ))}
          </span>
        )}
      </div>
      {/* Free text of any length, so on a line of its own. */}
      {note && (
        <p className="order-term-note" data-testid="order-note">
          {note.labelKey && (
            <>
              <span className="term-k">{t(note.labelKey)}</span>{' '}
            </>
          )}
          <span className="term-t2">{note.value}</span>
        </p>
      )}
    </div>
  );
}

/** Ids up to this long sit in a column; a longer one would be cut. */
const ORDER_ID_IN_COLUMN = 12;

/**
 * The figures in label-leader-value columns, in a fixed order so the same kind
 * of number sits in the same place on every receipt in a thread.
 */
function ReceiptFigures({
  figures,
  orderId,
  isApproval,
  cols,
}: {
  figures: OrderPart[];
  orderId: string | null;
  isApproval: boolean;
  cols: number;
}): React.ReactElement | null {
  const { t } = useTranslation();
  if (figures.length === 0 && !orderId) return null;
  const idLabel = t(
    isApproval
      ? 'toolArtifact.directTool.orderOutcome.approvalId'
      : 'toolArtifact.directTool.orderOutcome.orderId',
  );
  // The vendor's id is matched by eye against its own app, so one too long
  // for a column takes a row of its own rather than being cut.
  const idInColumn = !!orderId && orderId.length <= ORDER_ID_IN_COLUMN;
  return (
    <div className="order-term-figures" data-testid="order-figures">
      <TermKV
        cols={cols}
        pairs={[
          ...figures.map((part) => [columnLabel(part, t), <OrderPartValue key={part.field} part={part} />] as const),
          idInColumn && ([idLabel, orderId] as const),
        ]}
      />
      {orderId && !idInColumn && (
        <TermKV
          cols={1}
          pairs={[[idLabel, <span key="id" className="order-term-id">{orderId}</span>]]}
        />
      )}
    </div>
  );
}

/**
 * The answer under the order: the reason the person gave, why the brokerage
 * refused, and a closing line with the state in words and the links off the
 * card.
 */
function ReceiptOutcome({ receipt }: { receipt: OrderReceiptView }): React.ReactElement | null {
  const { t } = useTranslation();
  const { status, mode, vendorLabel, attemptId, actionUrl, vendorNamedEnd } = receipt;
  const { decisionMessage, failure, fill } = receipt.outcome;
  const fillSentence = fill.filter((part) => part.field === 'fill');
  const word = t(orderStatusShortLabel(status, mode, vendorNamedEnd));
  const { key, params } = orderStatusLabel(status, mode, vendorLabel, vendorNamedEnd);
  const sentence = t(key, params);
  // The edge names the state in a word. The sentence is said here when it
  // says more than that word, and not beside a fill, which says it with its
  // figures.
  const said =
    fillSentence.length === 0 && sentence.toLowerCase() !== word.toLowerCase() ? sentence : null;
  const foot = !!(said || fillSentence.length || actionUrl || attemptId);
  if (!decisionMessage && !failure && !foot) return null;
  return (
    <div className="order-term-outcome" data-testid="order-outcome">
      {decisionMessage && (
        <p className="order-term-prose">
          <span className="term-k">{t('toolArtifact.directTool.orderOutcome.decisionMessage')}</span>{' '}
          {decisionMessage}
        </p>
      )}
      {failure && (
        // The vendor's own sentence first, since it is what a person can act
        // on; the code is for matching against the broker's documentation.
        <p className="order-term-prose order-term-failure" data-testid="order-failure">
          <AlertCircle className="order-term-failure-mark" aria-hidden="true" />
          <span>
            {failure.message}
            {failure.message && failure.code && ' '}
            {failure.code && (
              <span className="term-t3 whitespace-nowrap">
                {t('toolArtifact.directTool.orderOutcome.failureCode', { code: failure.code })}
              </span>
            )}
          </span>
        </p>
      )}
      {foot && (
        <div className="order-term-foot">
          {said && (
            <span className="order-term-prose term-t2" data-testid="order-status-sentence">
              {said}
            </span>
          )}
          {fillSentence.length > 0 && (
            <span className="order-term-prose" data-testid="order-fill">
              {fillSentence.map((part) => (
                <React.Fragment key={part.field}>
                  {part.labelKey && <span className="term-t3">{t(part.labelKey)} </span>}
                  <OrderPartValue part={part} />
                </React.Fragment>
              ))}
            </span>
          )}
          <span className="order-term-links">
            <OrderActionLink
              href={actionUrl}
              vendorLabel={vendorLabel}
              status={status}
              className="order-term-link order-term-action"
            />
            {attemptId && <OrdersPageLink attemptId={attemptId} className="order-term-link" />}
          </span>
        </div>
      )}
    </div>
  );
}
