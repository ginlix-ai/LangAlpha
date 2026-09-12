/**
 * A direct MCP tool call the connection's consent no longer covers comes back
 * as a `ToolMessage` with `status: "error"`, which the backend puts on the
 * `tool_call_result` event. These lock that one signal all the way through:
 * the stream handler stamps `isFailed`, the render pipeline flips the row to
 * its failed state, the timeline row wears the badge, and the detail header
 * says so too. Before, only the collapsed row noticed, because it re-derived
 * failure from the "Refused:" prose on its own.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { handleToolCallResult } from '../../session/stream/mainEventHandlers';
import { buildRenderBlocks } from '../messageList/buildRenderBlocks';
import ActivityBlock from '../ActivityBlock';
import DetailPanel from '../DetailPanel';
import { isToolResultFailure } from '../../session/subagents/subagentStatus';

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

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <div>{content}</div>,
  CodeBlock: ({ children }: { children?: React.ReactNode }) => <pre>{children}</pre>,
}));

vi.mock('../charts/InlineArtifactCards', () => ({
  INLINE_ARTIFACT_TOOLS: new Set<string>(),
  isInlineArtifactReady: () => false,
  INLINE_ARTIFACT_MAP: {},
  InlineStockPriceCard: () => null,
  InlineCompanyOverviewCard: () => null,
  InlineMarketIndicesCard: () => null,
  InlineSectorPerformanceCard: () => null,
  InlineMarketOverviewCard: () => null,
  InlineSecFilingCard: () => null,
  InlineStockScreenerCard: () => null,
  InlineWebSearchCard: () => null,
}));

vi.mock('../charts/InlineAutomationCards', () => ({ InlineAutomationCard: () => null }));
vi.mock('../charts/InlinePreviewCard', () => ({ InlinePreviewCard: () => null }));
vi.mock('../ToolCallDetailView', () => ({ default: () => <div data-testid="detail-body" /> }));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

// The vendor mark reaches for the brokerage list over React Query; the icon
// identity is not what these tests are about.
vi.mock('../mcp/useDirectToolVendor', () => ({
  useDirectToolVendor: () => null,
  useDirectToolVendorLabel: () => 'moomoo',
}));

const TOOL_NAME = 'mcp__moomoo__trading_order_place';
const TOOL_CALL_ID = 'call-refused-1';
const REFUSAL = 'Refused: trading is not enabled for this connection.';

/** The proc the stream handler produces for a direct MCP result, wire status and all. */
function procFor(content: string, status: string): Record<string, unknown> {
  let messages: Array<Record<string, unknown>> = [
    {
      id: 'assistant-1',
      toolCallProcesses: {
        [TOOL_CALL_ID]: {
          toolName: TOOL_NAME,
          toolCall: { name: TOOL_NAME, args: { symbol: 'AAPL', qty: 1 } },
          toolCallResult: null,
          isInProgress: true,
          isComplete: false,
          order: 0,
          _createdAt: Date.now(),
        },
      },
    },
  ];

  handleToolCallResult({
    assistantMessageId: 'assistant-1',
    toolCallId: TOOL_CALL_ID,
    result: {
      content,
      content_type: 'text',
      tool_call_id: TOOL_CALL_ID,
      status,
    },
    refs: { currentToolCallIdRef: { current: TOOL_CALL_ID } } as never,
    setMessages: ((updater: (prev: typeof messages) => typeof messages) => {
      messages = updater(messages);
    }) as never,
  });

  return (messages[0].toolCallProcesses as Record<string, Record<string, unknown>>)[TOOL_CALL_ID];
}

function refusedProc(): Record<string, unknown> {
  return procFor(REFUSAL, 'error');
}

describe('a refused direct MCP tool call', () => {
  it('is stamped failed by the live stream handler', () => {
    expect(refusedProc().isFailed).toBe(true);
  });

  it('flips the render block to its failed live state', () => {
    const proc = refusedProc();
    const { blocks } = buildRenderBlocks(
      [{ type: 'tool_call', toolCallId: TOOL_CALL_ID } as never],
      {
        reasoningProcesses: {},
        toolCallProcesses: { [TOOL_CALL_ID]: proc as never },
        isStreaming: true,
      },
    );

    const activity = blocks.find((b) => b.type === 'activity');
    expect(activity).toBeDefined();
    const item = (activity as { items: Array<Record<string, unknown>> }).items[0];
    expect(item._liveState).toBe('failed');
  });

  it('wears the failed badge in the collapsed timeline row', () => {
    const proc = refusedProc();
    const items = [
      {
        type: 'tool_call',
        id: TOOL_CALL_ID,
        toolCallId: TOOL_CALL_ID,
        ...proc,
        _liveState: 'completed',
      },
    ];

    const { container } = render(<ActivityBlock items={items as never} isStreaming={false} isFirst={false} />);
    fireEvent.click(screen.getByRole('button', { name: /toolArtifact/i }));

    const failedRow = container.querySelector('.titem.failed');
    expect(failedRow).not.toBeNull();
    expect(failedRow!.querySelector('.nrow-badge')!.getAttribute('aria-label')).toBe(
      'toolArtifact.a11y.toolCallFailed',
    );
  });

  it('says so in the detail panel header', () => {
    const proc = refusedProc();
    render(<DetailPanel toolCallProcess={proc as never} onClose={() => {}} />);
    expect(screen.getByLabelText('toolArtifact.a11y.toolCallFailed')).toBeInTheDocument();
  });

  it('leaves a successful call unmarked in the header', () => {
    const proc = { ...refusedProc(), isFailed: false };
    render(<DetailPanel toolCallProcess={proc as never} onClose={() => {}} />);
    expect(screen.queryByLabelText('toolArtifact.a11y.toolCallFailed')).toBeNull();
  });
});

/**
 * The mirror case, and the one the wire used to make unreachable: a direct MCP
 * tool whose *successful* output happens to open with "Refused:". Every prose
 * rule reads that as a refusal, so the backend's success has to survive the
 * transport and short-circuit them at the same ingress the failure uses.
 */
describe('a successful direct MCP tool call whose output opens with "Refused:"', () => {
  const SUCCESS_TEXT = 'Refused: 7 applications';

  it('is not stamped failed by the live stream handler', () => {
    expect(procFor(SUCCESS_TEXT, 'success').isFailed).toBe(false);
  });

  it('would be misread as a failure if the status had not ridden along', () => {
    expect(
      isToolResultFailure({ content: SUCCESS_TEXT, toolName: TOOL_NAME }),
    ).toBe(true);
    expect(
      isToolResultFailure({ content: SUCCESS_TEXT, toolName: TOOL_NAME, status: 'success' }),
    ).toBe(false);
  });

  it('stays unmarked in the detail panel header', () => {
    render(<DetailPanel toolCallProcess={procFor(SUCCESS_TEXT, 'success') as never} onClose={() => {}} />);
    expect(screen.queryByLabelText('toolArtifact.a11y.toolCallFailed')).toBeNull();
  });
});
