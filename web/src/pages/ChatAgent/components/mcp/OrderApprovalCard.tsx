import React, { useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Check, ChevronRight, MinusCircle, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { OrderStatusPill } from '@/components/orders/OrderStatusPill';
import { OrdersPageLink } from '@/components/orders/OrdersPageLink';
import { useOrder } from '@/hooks/useOrders';
import { useNoteOrderExists } from '@/hooks/useOrdersVisible';
import type { ToolApprovalState } from '@/types/chat';
import type { OrderStatus } from '@/types/orders';
import type { OrderProposal } from '@/types/sse';
import { ArgsTable } from './ArgsTable';
import { StatusPill } from '@/components/mcp/McpPrimitives';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { OrderCard } from './OrderCard';
import { ORDER_ACTION_KEY, orderSummaryRows } from './orderSummary';
import { SettledToolStep } from './SettledToolStep';
import { useDirectToolVendorLabel } from './useDirectToolVendor';

/**
 * The question a person answers before an order is sent, asked in the shape of
 * the receipt it becomes.
 *
 * An order has one card in a thread and it is the receipt, so the card that
 * asks looks like the record that answers: the same header, the same field
 * list, the same footer line. Only the pill and the footer change as the order
 * moves, which is what makes approving feel like watching one thing settle
 * rather than one card being replaced by another.
 *
 * Unlike the receipt, the card draws sizes and prices even while
 * `readValuesHidden` says to hide them: a person approving an order has to see
 * the size and prices they are approving.
 *
 * The arguments are not on the card. They are the exact frame the vendor will
 * see and they stay one click away, but the summary above them is the question
 * being answered, and a JSON dump beside a live order buries it.
 *
 * Once settled the step is a one-line record like every other tool call, with
 * the arguments behind its fold. The receipt above it is the copy of the trade,
 * so an always-open card here would state the same order twice.
 */
export function OrderApprovalCard({
  data,
  order,
  onApprove,
  onReject,
  resultPending = false,
  resultLost = false,
}: {
  data: ToolApprovalState;
  order: OrderProposal;
  onApprove?: () => void;
  onReject?: (message?: string) => void;
  /** The approved call has not produced its result yet, so no receipt exists to
   *  settle against. Only the host rendering the transcript can know this. */
  resultPending?: boolean;
  /** The approved call produced no result and no longer can, so no receipt will
   *  ever state the outcome. Only the host rendering the transcript can know
   *  this either. */
  resultLost?: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  const [reason, setReason] = useState('');
  const [argsOpen, setArgsOpen] = useState(false);
  // The order names its own vendor; the server row is the fallback for a
  // vendor the shipped brokerage list does not carry.
  const vendorLabel = useDirectToolVendorLabel(order.vendor || data.server || '');
  // The gate writes the attempt before it raises the interrupt that drew this
  // card, so an attempt id here is already a row the Orders page can show.
  useNoteOrderExists(!!data.attemptId);

  const isApproved = data.status === 'approved';
  const isRejected = data.status === 'rejected';
  // An approved order whose call never answered has no receipt to hand the
  // outcome to, and the sweep may have settled it against the brokerage's own
  // book since. Read the ledger for that one case only: the id is nulled
  // otherwise, which is what keeps this off every ordinary settled card.
  const orphaned = isApproved && resultLost && !resultPending;
  const { data: ledgerRow } = useOrder(orphaned ? data.attemptId ?? null : null, { poll: true });
  const canAct = !!onApprove || !!onReject;
  const argsTable = (
    <ArgsTable
      args={data.args || {}}
      emptyLabel={t('toolArtifact.directTool.noArguments')}
      hideEmpty
    />
  );

  if (isRejected || (isApproved && !resultPending)) {
    return (
      <SettledOrderStep
        order={order}
        approved={isApproved}
        reason={data.reason}
        args={argsTable}
        // The verdict alone would read as the whole story on a card whose
        // order may have filled. The pill rides the verdict line; the link out
        // cannot, because that line is a button and a link inside one fights
        // the fold it opens.
        status={ledgerRow?.status}
        attemptId={ledgerRow ? data.attemptId : undefined}
      />
    );
  }

  const pill = isApproved ? (
    // The window between the click and the brokerage's answer. The receipt
    // owns every state after it, and this is the same word it uses for this
    // one, so nothing changes as the card hands over.
    <OrderStatusPill status="submitting" surface="receipt" />
  ) : canAct ? (
    <OrderStatusPill status="proposed" surface="receipt" />
  ) : (
    // A pending card on a replayed turn: a record of a stop nobody can answer
    // now, so it names that rather than asking.
    <StatusPill
      icon={MinusCircle}
      label={t('toolArtifact.directTool.orderApproval.notAnswered')}
      color="var(--color-text-tertiary)"
      bg="var(--color-bg-tag)"
      testid="order-status-unanswered"
    />
  );

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
      data-testid="order-approval-card"
      data-order-state={isApproved ? 'sending' : canAct ? 'pending' : 'unanswered'}
    >
      <OrderCard
        vendor={order.vendor || data.server || ''}
        action={order.action}
        targetRef={order.target_ref}
        mode={order.mode}
        vendorLabel={vendorLabel}
        pill={pill}
        rows={orderSummaryRows(order)}
        testid="order-approval"
        footer={
          canAct && !isApproved ? (
            <div className="flex items-center gap-2 flex-wrap">
              <input
                type="text"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                maxLength={200}
                placeholder={t('toolArtifact.directTool.orderApproval.reasonPlaceholder')}
                aria-label={t('toolArtifact.directTool.orderApproval.reasonPlaceholder')}
                className="flex-1 min-w-[10rem] text-sm px-3 py-1.5 rounded-md bg-transparent"
                style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
              />
              <motion.button
                type="button"
                onClick={(e: React.MouseEvent) => {
                  e.stopPropagation();
                  onReject?.(reason.trim() || undefined);
                }}
                disabled={!onReject}
                className="flex items-center gap-1.5 text-sm px-4 py-1.5 rounded-md font-medium transition-colors disabled:opacity-50"
                style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <X className="h-3.5 w-3.5" />
                {t('toolArtifact.directTool.reject')}
              </motion.button>
              <motion.button
                type="button"
                onClick={(e: React.MouseEvent) => {
                  e.stopPropagation();
                  onApprove?.();
                }}
                disabled={!onApprove}
                className="flex items-center gap-1.5 text-sm px-4 py-1.5 rounded-md font-medium transition-colors hover:brightness-110 disabled:opacity-50"
                style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <Check className="h-3.5 w-3.5 stroke-[2.5]" />
                {t('toolArtifact.directTool.approve')}
              </motion.button>
            </div>
          ) : null
        }
      />
      <ArgsDisclosure open={argsOpen} onToggle={() => setArgsOpen((v) => !v)}>
        {argsTable}
      </ArgsDisclosure>
    </motion.div>
  );
}

/**
 * What is left of the step once the order has an answer: the shared settled
 * row, saying which order was answered and how it was to be placed.
 */
function SettledOrderStep({
  order,
  approved,
  reason,
  args,
  status,
  attemptId,
}: {
  order: OrderProposal;
  approved: boolean;
  reason?: string | null;
  args: React.ReactNode;
  /** What the ledger says became of the order. Drawn only where no receipt
   *  will ever say it, so the verdict is not left standing as the whole story. */
  status?: OrderStatus;
  /** The row the rest of the outcome is written on. Present only beside a
   *  ledger status, since without one there is nothing more to read. */
  attemptId?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const verb = approved
    ? t('toolArtifact.directTool.orderApproval.approved')
    : t('toolArtifact.directTool.orderApproval.rejected');
  const step = (
    <SettledToolStep
      approved={approved}
      order
      reason={reason}
      label={`${verb} · ${t(ORDER_ACTION_KEY[order.action] || ORDER_ACTION_KEY.place)}`}
      badge={
        <>
          <OrderModeBadge mode={order.mode} />
          {status && <OrderStatusPill status={status} mode={order.mode} />}
        </>
      }
    >
      <div className="rounded-lg px-4 py-3" style={{ border: '1px solid var(--color-border-muted)' }}>
        {args}
      </div>
    </SettledToolStep>
  );
  if (!attemptId) return step;
  // Outside the row, because the row is the button that opens the fold and a
  // link inside one fights the click it sits in.
  return (
    <div data-testid="order-approval-orphaned">
      {step}
      <div className="pl-6 pt-0.5">
        <OrdersPageLink attemptId={attemptId} />
      </div>
    </div>
  );
}

/** The frame the vendor sees, one click under the card that summarizes it. */
function ArgsDisclosure({
  open,
  onToggle,
  children,
}: {
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="pt-1.5">
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onToggle();
        }}
        className="flex items-center gap-1 text-xs py-0.5 cursor-pointer"
        style={{ color: 'var(--color-text-tertiary)' }}
        aria-expanded={open}
      >
        <motion.span
          animate={{ rotate: open ? 90 : 0 }}
          transition={{ duration: 0.2 }}
          className="inline-flex"
        >
          <ChevronRight className="h-3 w-3" style={{ color: 'var(--color-icon-muted)' }} />
        </motion.span>
        {t('toolArtifact.directTool.orderApproval.arguments')}
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="pt-2 pl-4">{children}</div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
