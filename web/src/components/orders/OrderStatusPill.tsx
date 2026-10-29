import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  Activity,
  AlertCircle,
  Ban,
  CheckCircle2,
  CircleDot,
  Clock,
  CornerUpRight,
  HelpCircle,
  Hourglass,
  MinusCircle,
  Send,
  ShieldCheck,
  XCircle,
} from 'lucide-react';
import { StatusPill } from '@/components/mcp/McpPrimitives';
import type { OrderMode, OrderStatus } from '@/types/orders';
import { orderStatusLabel, orderStatusShortLabel, vendorNamedEnd } from './status';

/**
 * What an order's state is called and what it looks like, for every surface
 * that draws one. The chat receipt and the Orders row are the same order seen
 * twice, so a state whose wording or verdict changed on one of them and not
 * the other would read as two different orders.
 */

export type OrderStatusTone = 'muted' | 'warning' | 'profit' | 'loss';

const TONE: Record<OrderStatusTone, { color: string; bg: string }> = {
  muted: { color: 'var(--color-text-tertiary)', bg: 'var(--color-bg-tag)' },
  warning: { color: 'var(--color-warning)', bg: 'var(--color-warning-soft)' },
  profit: { color: 'var(--color-profit)', bg: 'var(--color-profit-soft)' },
  loss: { color: 'var(--color-loss)', bg: 'var(--color-loss-soft)' },
};

interface StatusMeta {
  tone: OrderStatusTone;
  icon: React.ComponentType<{ className?: string }>;
}

/**
 * One entry per status, written out rather than derived, so a state added to
 * the ledger is a compile error here and not a raw glyph on a row about money
 * that already moved.
 *
 * The verdict is carried by the glyph and the words together. Colour only
 * separates the three ends a reader acts on differently: filled is done,
 * anything refused or failed needs looking at, and a proposal or a broker
 * confirmation is waiting on a person.
 */
const STATUS_META: Record<OrderStatus, StatusMeta> = {
  proposed: { tone: 'warning', icon: Clock },
  approved: { tone: 'muted', icon: ShieldCheck },
  rejected_by_user: { tone: 'muted', icon: MinusCircle },
  refused: { tone: 'loss', icon: XCircle },
  submitting: { tone: 'muted', icon: Send },
  submitted: { tone: 'muted', icon: Send },
  pending_confirm: { tone: 'warning', icon: Clock },
  working: { tone: 'muted', icon: Activity },
  partially_filled: { tone: 'muted', icon: CircleDot },
  filled: { tone: 'profit', icon: CheckCircle2 },
  cancelled: { tone: 'muted', icon: MinusCircle },
  expired: { tone: 'muted', icon: Hourglass },
  rejected_by_vendor: { tone: 'loss', icon: AlertCircle },
  handed_off: { tone: 'muted', icon: CornerUpRight },
  failed: { tone: 'loss', icon: AlertCircle },
  unknown: { tone: 'muted', icon: HelpCircle },
};

/**
 * Where the card in a thread differs, and why.
 *
 * The ledger is a list a person scans for the rows that need them, so it
 * spends colour on picking those out and gives each state its own glyph. A
 * receipt is one order the reader is already looking at: the states on the way
 * to an answer are all the same waiting, so they wear one clock and no colour,
 * and the two that are genuinely unresolved (a partial fill, an answer we
 * could not read) are the ones the card raises its voice for.
 */
const RECEIPT_STATUS_META: Partial<Record<OrderStatus, Partial<StatusMeta>>> = {
  proposed: { tone: 'muted' },
  approved: { icon: Clock },
  rejected_by_user: { icon: XCircle },
  refused: { tone: 'muted', icon: Ban },
  submitting: { icon: Clock },
  submitted: { icon: Clock },
  working: { icon: Clock },
  partially_filled: { tone: 'warning', icon: CheckCircle2 },
  unknown: { tone: 'warning' },
};

/**
 * The tone a status wears on a surface, for a card that draws the verdict in
 * its own type rather than as this pill.
 */
export function orderStatusTone(
  status: OrderStatus,
  surface: 'ledger' | 'receipt' | 'detail' = 'ledger',
): OrderStatusTone {
  const base = STATUS_META[status] ?? STATUS_META.unknown;
  return (surface === 'receipt' && RECEIPT_STATUS_META[status]?.tone) || base.tone;
}

export function OrderStatusPill({
  status,
  mode,
  vendorLabel,
  unlistedAt,
  surface = 'ledger',
}: {
  status: OrderStatus;
  /** Staged is the one mode that changes what a state is called: the order
   *  never reached a market, it is waiting in the broker's own client. */
  mode?: OrderMode | null;
  vendorLabel?: string | null;
  /** The ledger's `unlisted_at`, which says whether the vendor named how a
   *  handed-off order ended. Left out, the pill says we do not know. */
  unlistedAt?: string | null;
  /** The ledger row wears the one-word verdict and carries the sentence as
   *  its tooltip; a receipt and the detail drawer have room for the sentence. */
  surface?: 'ledger' | 'receipt' | 'detail';
}): React.ReactElement {
  const { t } = useTranslation();
  const icon =
    (surface === 'receipt' && RECEIPT_STATUS_META[status]?.icon) ||
    (STATUS_META[status] ?? STATUS_META.unknown).icon;
  const tone = TONE[orderStatusTone(status, surface)];
  const named = vendorNamedEnd(unlistedAt);
  const { key, params } = orderStatusLabel(status, mode, vendorLabel, named);
  const sentence = t(key, params);
  const short = surface === 'ledger';
  return (
    <StatusPill
      icon={icon}
      label={short ? t(orderStatusShortLabel(status, mode, named)) : sentence}
      title={short ? sentence : undefined}
      color={tone.color}
      bg={tone.bg}
      testid={`order-status-${status}`}
    />
  );
}
