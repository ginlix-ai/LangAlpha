/**
 * Where the order card learns that its answer has landed.
 *
 * The card cannot see it: an approval and the result it unblocks are two
 * different records, joined only by the tool call the interrupt named. The
 * transcript holds both, so it is the transcript that decides whether the card
 * is still waiting on a brokerage or has a receipt to settle against. Without
 * that join the step would claim a verdict the moment it was clicked, before
 * anything had been sent.
 *
 * The join is the call's own in-progress flag, not the stream's: a turn sitting
 * on its interrupt is not streaming, so reading that flag would settle every
 * card on the click, which is the bug this exists to avoid. History rebuilds
 * every call as complete, so a turn killed before its result settles too rather
 * than claiming an order is still on its way.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';
import type { OrderAttempt } from '@/pages/ChatAgent/utils/api';
import type { ToolApprovalState } from '@/types/chat';
import type { OrderStatus } from '@/types/orders';
import { MessageContentSegments } from '../MessageContentSegments';

vi.mock('@/hooks/useMcpServers', () => ({
  useBrokerages: () => ({ data: [{ name: 'moomoo', label: 'moomoo' }] }),
}));

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

const APPROVED: ToolApprovalState = {
  status: 'approved',
  toolName: 'mcp__moomoo__sim_trade_input_order',
  server: 'moomoo',
  tool: 'sim_trade_input_order',
  args: { acc_id: '12345678', code: 'US.AAPL', qty: '1', price: '50' },
  interruptId: 'int-1',
  actionIndex: 0,
  actionCount: 1,
  toolCallId: 'call-1',
  attemptId: 'attempt-1',
  order: {
    action: 'place',
    mode: 'paper',
    vendor: 'moomoo',
    account_ref: '12345678',
    instrument: { kind: 'equity', symbol: 'US.AAPL' },
    side: 'buy',
    qty: '1',
  },
};

function props(
  approval: ToolApprovalState,
  toolCallProcesses: SegmentsProps['toolCallProcesses'],
  isStreaming: boolean,
): SegmentsProps {
  return {
    segments: [{ type: 'tool_approval', order: 0, proposalId: 'int-1' }] as SegmentsProps['segments'],
    reasoningProcesses: {},
    toolCallProcesses,
    todoListProcesses: {},
    subagentTasks: {},
    toolApprovals: { 'int-1': approval },
    isStreaming,
    isAssistant: true,
  };
}

const IN_FLIGHT = {
  'call-1': { toolName: APPROVED.toolName, isInProgress: true, toolCallResult: null },
};
const ANSWERED = {
  'call-1': {
    toolName: APPROVED.toolName,
    isInProgress: false,
    toolCallResult: { artifact: { type: 'order_receipt' } },
  },
};
/** What history rebuilds for a turn killed between the approval and the result. */
const ABANDONED = {
  'call-1': { toolName: APPROVED.toolName, isInProgress: false, toolCallResult: null },
};

/**
 * A client already holding the ledger's answer for the attempt.
 *
 * The abandoned card is the one case that reads the ledger, so seeding it keeps
 * these tests off the network and makes the pill synchronous.
 */
function clientHoldingLedgerRow(status: OrderStatus) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  client.setQueryData<OrderAttempt>(
    queryKeys.orders.detail('attempt-1'),
    { attempt_id: 'attempt-1', status } as OrderAttempt,
  );
  return client;
}

describe('an approved order in the transcript', () => {
  it('is still sending while its call is out', () => {
    renderWithProviders(<MessageContentSegments {...props(APPROVED, IN_FLIGHT, false)} />);
    expect(screen.getByTestId('order-approval-card')).toHaveAttribute('data-order-state', 'sending');
    expect(screen.queryByTestId('tool-approval-settled')).toBeNull();
  });

  it('settles once the call has answered', () => {
    renderWithProviders(<MessageContentSegments {...props(APPROVED, ANSWERED, true)} />);
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
    expect(screen.queryByTestId('order-approval-card')).toBeNull();
  });

  // A turn that ended without a result leaves nothing to wait for, so the step
  // settles rather than claiming an order is on its way forever.
  it('settles on a replayed turn whose call never answered', () => {
    renderWithProviders(<MessageContentSegments {...props(APPROVED, ABANDONED, false)} />, {
      queryClient: clientHoldingLedgerRow('unknown'),
    });
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
  });

  // No receipt will ever state this order's outcome, so the verdict alone would
  // read as the whole story while the sweep has it filled.
  it('reads that turn\'s outcome off the ledger and points at the row', () => {
    renderWithProviders(<MessageContentSegments {...props(APPROVED, ABANDONED, false)} />, {
      queryClient: clientHoldingLedgerRow('filled'),
    });
    expect(screen.getByTestId('order-status-filled')).toBeInTheDocument();
    // By wrapper rather than by name: this suite renders real copy, so an
    // accessible-name match would pin the English wording of the link.
    expect(
      within(screen.getByTestId('order-approval-orphaned')).getByRole('link'),
    ).toHaveAttribute('href', '/orders?detail=order:attempt-1');
  });

  // The answered card defers to its receipt, which owns every state after the
  // click, so nothing here asks the ledger a second time.
  it('leaves an answered call to its receipt', () => {
    renderWithProviders(<MessageContentSegments {...props(APPROVED, ANSWERED, true)} />, {
      queryClient: clientHoldingLedgerRow('filled'),
    });
    expect(screen.queryByTestId('order-status-filled')).toBeNull();
    expect(screen.queryByTestId('order-approval-orphaned')).toBeNull();
  });

  // Without the tool call the interrupt named there is no join to wait on, and
  // a card that can never learn its answer must not sit on "sending".
  it('settles when the interrupt named no tool call', () => {
    renderWithProviders(
      <MessageContentSegments {...props({ ...APPROVED, toolCallId: undefined }, {}, true)} />,
    );
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
  });
});
