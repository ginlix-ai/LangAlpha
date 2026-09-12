/**
 * The tool approval card shows every argument the call carries, approves with a
 * bare decision, and rejects with whatever reason was typed (or none). The one
 * field it does not print is the account id, which is masked wherever the chat
 * draws a direct call.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import ToolApprovalCard from '../ToolApprovalCard';
import type { ToolApprovalState } from '@/types/chat';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => {
      if (opts && typeof opts === 'object') {
        let out = key;
        for (const [k, v] of Object.entries(opts)) {
          out = out.replace(new RegExp(`{{\\s*${k}\\s*}}`, 'g'), String(v));
        }
        return out;
      }
      return key;
    },
  }),
}));

vi.mock('@/hooks/useMcpServers', () => ({
  useBrokerages: () => ({ data: [{ name: 'moomoo', label: 'moomoo' }] }),
}));

const pending: ToolApprovalState = {
  status: 'pending',
  toolName: 'mcp__moomoo__trading_order_place',
  server: 'moomoo',
  tool: 'trading_order_place',
  args: { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1', price: '100' },
  interruptId: 'int-1',
  actionIndex: 0,
  actionCount: 1,
};

describe('ToolApprovalCard', () => {
  it('shows the vendor, the tool and the arguments, with the account masked', () => {
    renderWithProviders(<ToolApprovalCard data={pending} onApprove={() => {}} onReject={() => {}} />);
    expect(screen.getByText('toolArtifact.directTool.approvalTitle')).toBeInTheDocument();
    expect(screen.getByText('moomoo · Trading order place')).toBeInTheDocument();
    expect(screen.queryByText('12345678')).toBeNull();
    expect(screen.getByText('••••5678')).toBeInTheDocument();
    expect(screen.getByText('US.AAPL')).toBeInTheDocument();
  });

  it('approves with a bare decision', () => {
    const onApprove = vi.fn();
    renderWithProviders(<ToolApprovalCard data={pending} onApprove={onApprove} onReject={() => {}} />);
    fireEvent.click(screen.getByText('toolArtifact.directTool.approve'));
    expect(onApprove).toHaveBeenCalledTimes(1);
  });

  it('rejects with the typed reason, or none', () => {
    const onReject = vi.fn();
    const { unmount } = renderWithProviders(<ToolApprovalCard data={pending} onApprove={() => {}} onReject={onReject} />);
    fireEvent.change(screen.getByLabelText('toolArtifact.directTool.reasonPlaceholder'), { target: { value: 'wrong account' } });
    fireEvent.click(screen.getByText('toolArtifact.directTool.reject'));
    expect(onReject).toHaveBeenCalledWith('wrong account');
    unmount();

    const bare = vi.fn();
    renderWithProviders(<ToolApprovalCard data={pending} onApprove={() => {}} onReject={bare} />);
    fireEvent.click(screen.getByText('toolArtifact.directTool.reject'));
    expect(bare).toHaveBeenCalledWith(undefined);
  });

  it('offers no controls read-only, so a replayed card cannot resubmit a decision', () => {
    // Replayed history renders the card with neither handler (MessageContentSegments
    // drops both when readOnly). A still-pending card from a past turn would
    // otherwise show live Approve/Reject on an interrupt nothing is waiting on,
    // and ask a question over a spinner that is waiting for nothing.
    renderWithProviders(<ToolApprovalCard data={pending} />);
    expect(screen.getByText('toolArtifact.directTool.unansweredTitle')).toBeInTheDocument();
    expect(screen.queryByText('toolArtifact.directTool.approvalTitle')).not.toBeInTheDocument();
    expect(screen.queryByText('toolArtifact.directTool.approve')).not.toBeInTheDocument();
    expect(screen.queryByText('toolArtifact.directTool.reject')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('toolArtifact.directTool.reasonPlaceholder')).not.toBeInTheDocument();
  });

  it('renders a settled card as a status row that opens to the arguments', () => {
    renderWithProviders(<ToolApprovalCard data={{ ...pending, status: 'rejected', reason: 'wrong account' }} />);
    expect(screen.getByText('toolArtifact.directTool.rejectedAction')).toBeInTheDocument();
    expect(screen.getByText('wrong account')).toBeInTheDocument();
    expect(screen.queryByText('code')).not.toBeInTheDocument();
    fireEvent.click(screen.getByText('toolArtifact.directTool.rejectedAction'));
    expect(screen.getByText('code')).toBeInTheDocument();
  });

  // The exception an order gets is keyed on the order summary, not on the tool
  // name, so a call that carries none keeps the fold every other tool has.
  it('keeps the fold on a settled call that carries no order', () => {
    renderWithProviders(<ToolApprovalCard data={{ ...pending, status: 'approved' }} />);
    expect(screen.getByRole('button')).toBeInTheDocument();
    expect(screen.queryByText('code')).not.toBeInTheDocument();
  });

  it('leaves a null argument on a call that carries no order', () => {
    renderWithProviders(
      <ToolApprovalCard
        data={{ ...pending, args: { code: 'US.AAPL', stop_price: null } }}
        onApprove={() => {}}
      />,
    );
    expect(screen.getByText('stop_price')).toBeInTheDocument();
  });
});
