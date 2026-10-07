/**
 * Every inline card is a door to the tool detail beside it: the frame that was
 * sent, the body that came back.
 *
 * An order receipt is one too. It says in words what happened to the order,
 * but it is the only row its call has in the turn, so without the click the
 * vendor's own answer is nowhere a person can reach. It is a settled record,
 * so a stray click opens a panel and moves nothing. Its links still go where
 * they say.
 *
 * Both kinds run here through the REAL registry on the surface that actually
 * draws them, since only that surface can say whether a click reaches the card.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { MessageContentSegments } from '../MessageList';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <div>{content}</div>,
  CodeBlock: ({ children }: { children?: React.ReactNode }) => <pre>{children}</pre>,
}));

// Partial, so the rest of the transcript's hooks stay real, only the brokerage
// list the vendor mark reaches for is answered locally.
vi.mock('@/hooks/useMcpServers', async (importActual) => ({
  ...(await importActual<typeof import('@/hooks/useMcpServers')>()),
  useBrokerages: () => ({ data: [{ name: 'moomoo', label: 'moomoo' }] }),
}));

const RECEIPT_ID = 'call-receipt';
const SEARCH_ID = 'call-search';
const QUERY = 'aapl earnings date';

const RECEIPT_ARTIFACT = {
  type: 'order_receipt',
  direct_mcp: { server: 'moomoo', tool: 'sim_trade_input_order' },
  order_receipt: {
    attempt_id: 'attempt-1',
    vendor: 'moomoo',
    tool: 'sim_trade_input_order',
    action: 'place',
    mode: 'paper',
    account_ref: '1234567',
    order: {
      action: 'place',
      mode: 'paper',
      vendor: 'moomoo',
      account_ref: '1234567',
      instrument: { kind: 'equity', symbol: 'AAPL' },
      side: 'buy',
      qty: '1',
      order_type: 'limit',
      limit_price: '50',
    },
    outcome: { status: 'submitted' },
  },
};

const SEARCH_ARTIFACT = {
  type: 'web_search',
  query: QUERY,
  results: [{ title: 'Apple Q3 results', url: 'https://example.com/aapl' }],
};

function proc(toolName: string, toolCallId: string, artifact: Record<string, unknown>) {
  return {
    toolName,
    toolCallId,
    toolCall: { name: toolName, args: {} },
    toolCallResult: { content: '', tool_call_id: toolCallId, artifact },
    isInProgress: false,
    isComplete: true,
    isFailed: false,
    order: 0,
  };
}

function renderTranscript(onToolCallDetailClick: () => void) {
  return renderWithProviders(
    <MessageActionsProvider actions={{ onToolCallDetailClick }}>
      <MessageContentSegments
        segments={[
          { type: 'tool_call', toolCallId: RECEIPT_ID },
          { type: 'tool_call', toolCallId: SEARCH_ID },
        ] as never}
        reasoningProcesses={{}}
        toolCallProcesses={{
          [RECEIPT_ID]: proc('mcp__moomoo__sim_trade_input_order', RECEIPT_ID, RECEIPT_ARTIFACT),
          [SEARCH_ID]: proc('web_search', SEARCH_ID, SEARCH_ARTIFACT),
        } as never}
        todoListProcesses={{}}
        subagentTasks={{}}
        isStreaming={false}
      />
    </MessageActionsProvider>,
  );
}

describe('clicking an inline artifact card', () => {
  it('opens the detail panel for an order receipt, but not from its links', () => {
    const onToolCallDetailClick = vi.fn();
    renderTranscript(onToolCallDetailClick);

    const card = screen.getByTestId('order-receipt');
    expect(card.className).toContain('cursor-pointer');
    fireEvent.click(screen.getByRole('link'));
    expect(onToolCallDetailClick).not.toHaveBeenCalled();

    fireEvent.click(card);
    expect(onToolCallDetailClick).toHaveBeenCalledWith(RECEIPT_ID);
  });

  it('opens the detail panel for every other card kind', () => {
    const onToolCallDetailClick = vi.fn();
    renderTranscript(onToolCallDetailClick);

    fireEvent.click(screen.getByText(QUERY));

    expect(onToolCallDetailClick).toHaveBeenCalledTimes(1);
  });

  it('draws the vendor mark on the receipt it renders', () => {
    renderTranscript(vi.fn());

    const mark = screen.getByTestId('order-receipt').querySelector('img');
    expect(mark?.getAttribute('src')).toContain('/api/v1/mcp/brokerages/moomoo/icon');
  });
});
