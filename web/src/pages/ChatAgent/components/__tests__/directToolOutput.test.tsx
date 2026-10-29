/**
 * The panel an order receipt opens is the raw call: the arguments sent and the
 * vendor's answer. It carries no receipt of its own, since the card it was
 * opened from is still in the thread beside it, and two copies read as two
 * orders.
 *
 * A brokerage echoes the account back in its answer, so the Output is masked as
 * the Input above it is, or the panel prints in full the number the receipt and
 * the arguments both hide.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { DirectToolResultView } from '../mcp/DirectToolDetail';
import ToolCallDetailView from '../ToolCallDetailView';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <div>{content}</div>,
  CodeBlock: ({ code }: { code: string }) => <pre>{code}</pre>,
}));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

vi.mock('@/hooks/useMcpServers', async (importActual) => ({
  ...(await importActual<typeof import('@/hooks/useMcpServers')>()),
  useBrokerages: () => ({ data: [{ name: 'moomoo', label: 'moomoo' }] }),
}));

/** Fake, and long enough that its tail is not the whole of it. */
const ACCOUNT = '5QR12345';

describe('the output of a direct tool call', () => {
  it('masks an account id in a JSON body', () => {
    const { container } = render(
      <DirectToolResultView
        result={{ kind: 'json', json: { account_number: ACCOUNT, state: 'queued' } }}
      />,
    );
    expect(container).not.toHaveTextContent(ACCOUNT);
    expect(container).toHaveTextContent('••••2345');
    expect(screen.getByText(/queued/)).toBeInTheDocument();
  });

  it('masks it in a JSON block of a multi-part body', () => {
    const { container } = render(
      <DirectToolResultView
        result={{
          kind: 'blocks',
          blocks: [
            { type: 'text', text: '', json: { order: { rhs_account_number: ACCOUNT } } },
            { type: 'text', text: 'placed' },
          ],
        }}
      />,
    );
    expect(container).not.toHaveTextContent(ACCOUNT);
    expect(container).toHaveTextContent('placed');
  });
});

describe('the panel an order receipt opens', () => {
  const TOOL = 'mcp__moomoo__sim_trade_input_order';
  const PROCESS = {
    toolName: TOOL,
    toolCall: { id: 'call-1', name: TOOL, args: { acc_id: ACCOUNT, code: 'US.AAPL', qty: 1 } },
    toolCallResult: {
      content: JSON.stringify({ ret_code: 0, data: { order_id: '841644', acc_id: ACCOUNT } }),
      artifact: {
        type: 'order_receipt',
        direct_mcp: { server: 'moomoo', tool: 'sim_trade_input_order' },
        order_receipt: {
          attempt_id: 'attempt-1',
          vendor: 'moomoo',
          tool: 'sim_trade_input_order',
          action: 'place',
          mode: 'paper',
          account_ref: ACCOUNT,
          outcome: { status: 'submitted' },
        },
      },
    },
    isComplete: true,
  };

  it('is the call, with no second receipt and the account masked throughout', () => {
    const { container } = renderWithProviders(<ToolCallDetailView toolCallProcess={PROCESS as never} />);

    expect(screen.queryByTestId('order-receipt')).toBeNull();
    expect(container).toHaveTextContent('US.AAPL');
    expect(container).toHaveTextContent('841644');
    expect(container).not.toHaveTextContent(ACCOUNT);
  });
});
