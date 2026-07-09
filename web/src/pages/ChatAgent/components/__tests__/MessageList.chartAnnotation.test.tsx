/**
 * The agent draws a chart in several `draw_chart_annotation` calls. Only the
 * LATEST draw per chart instance renders the rich preview card; earlier draws
 * fold into the activity timeline as ordinary tool-call rows so the user can
 * watch the chart get built up step by step.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageContentSegments } from '../MessageList';

vi.mock('framer-motion', async () => {
  const ReactActual = await vi.importActual<typeof import('react')>('react');
  const FRAMER_ONLY_PROPS = new Set([
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
    'onAnimationComplete', 'onAnimationStart',
  ]);
  const createEl = ReactActual.createElement as (type: unknown, props?: unknown, ...children: unknown[]) => React.ReactElement;
  const make = (Comp: React.ElementType | string) =>
    function MotionStub({ children, ...props }: { children?: React.ReactNode } & Record<string, unknown>) {
      const domProps: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(props)) {
        if (!FRAMER_ONLY_PROPS.has(k)) domProps[k] = v;
      }
      return createEl(Comp, domProps, children);
    };
  return {
    motion: new Proxy({} as Record<string, unknown>, {
      get: (_t, key: string) => (key === 'create' ? make : make(key)),
    }),
    AnimatePresence: ({ children }: { children?: React.ReactNode }) =>
      ReactActual.createElement(ReactActual.Fragment, null, children),
    animate: () => ({ stop: () => {} }),
  };
});

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
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));

// draw_chart_annotation must be a recognized inline-artifact tool for the
// latest-vs-intermediate split to engage.
vi.mock('../charts/InlineArtifactCards', () => ({
  INLINE_ARTIFACT_TOOLS: new Set<string>(['draw_chart_annotation']),
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
vi.mock('../charts/InlineChartAnnotationCard', () => ({
  // Surface the annotation count so the test can confirm the pinned card is fed
  // the LATEST cumulative artifact, not the first draw's.
  InlineChartAnnotationCard: ({ artifact }: { artifact: Record<string, unknown> }) => (
    <div
      data-testid="annotation-card"
      data-count={(artifact?.annotations as unknown[] | undefined)?.length ?? 0}
    />
  ),
}));

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

const baseProps = {
  reasoningProcesses: {},
  toolCallProcesses: {},
  todoListProcesses: {},
  subagentTasks: {},
  hasError: false,
  isAssistant: true,
  textOnly: true,
} satisfies Partial<SegmentsProps>;

// `cumulative` is the full annotation set returned by that draw — each draw is a
// superset of the previous, mirroring the backend's cumulative artifact.
// `symbol` selects the chart instance (chart_id = SYMBOL:1day) so a test can
// drive draws against more than one chart in a single turn.
function drawProc(
  annotationType: string,
  cumulative: number,
  symbol = 'NVDA',
): Record<string, unknown> {
  return {
    toolName: 'draw_chart_annotation',
    toolCall: { args: { symbol, annotation: { type: annotationType } } },
    isInProgress: false,
    isComplete: true,
    isFailed: false,
    _createdAt: Date.now(),
    toolCallResult: {
      artifact: {
        type: 'chart_annotation',
        workspace_id: 'ws1',
        chart_id: `${symbol}:1day`,
        symbol,
        timeframe: '1day',
        annotations: Array.from({ length: cumulative }, (_, i) => ({ annotation_id: `${symbol}-a${i}` })),
      },
    },
  };
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
});
afterEach(() => {
  vi.useRealTimers();
});

describe('MessageContentSegments — chart-annotation draws', () => {
  it('pins one card (fed the latest cumulative artifact); later draws fold into the accordion', () => {
    const props: SegmentsProps = {
      ...baseProps,
      segments: [
        { type: 'tool_call', order: 0, toolCallId: 'd1' },
        { type: 'tool_call', order: 1, toolCallId: 'd2' },
      ],
      toolCallProcesses: {
        d1: drawProc('trendline', 1),
        d2: drawProc('price_line', 2),
      },
      isStreaming: false,
    };

    render(<MessageContentSegments {...props} />);

    // Exactly one card, and it shows the LATEST cumulative set (2), not the
    // first draw's (1) — i.e. pinned at the first draw but fed the newest data.
    const cards = screen.getAllByTestId('annotation-card');
    expect(cards).toHaveLength(1);
    expect(cards[0]).toHaveAttribute('data-count', '2');
    // The later draw (d2) is an ordinary completed row in the steps accordion.
    const accordion = screen.getByRole('button', { name: /toolArtifact/i });
    expect(accordion).toBeInTheDocument();
    // Pin-to-FIRST: the card sits at the anchor (first) draw, so it renders
    // BEFORE the accordion holding the later draw. If the card were pinned at the
    // latest draw instead, it would follow the accordion — guard against that swap.
    expect(
      cards[0].compareDocumentPosition(accordion) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it('pins one card per distinct chart instance, each fed its own latest set', () => {
    const props: SegmentsProps = {
      ...baseProps,
      segments: [
        { type: 'tool_call', order: 0, toolCallId: 'n1' },
        { type: 'tool_call', order: 1, toolCallId: 'a1' },
      ],
      toolCallProcesses: {
        n1: drawProc('trendline', 1, 'NVDA'),
        a1: drawProc('price_line', 3, 'AAPL'),
      },
      isStreaming: false,
    };

    render(<MessageContentSegments {...props} />);

    // Two distinct charts (NVDA, AAPL) → two cards, each with its own count.
    const counts = screen
      .getAllByTestId('annotation-card')
      .map((el) => el.getAttribute('data-count'))
      .sort();
    expect(counts).toEqual(['1', '3']);
    // Both are anchors (one draw each) → no intermediate rows, no accordion.
    expect(screen.queryByRole('button', { name: /toolArtifact/i })).toBeNull();
  });

  it('renders a single draw as a card with no accordion', () => {
    const props: SegmentsProps = {
      ...baseProps,
      segments: [{ type: 'tool_call', order: 0, toolCallId: 'd1' }],
      toolCallProcesses: { d1: drawProc('trendline', 1) },
      isStreaming: false,
    };

    render(<MessageContentSegments {...props} />);

    const cards = screen.getAllByTestId('annotation-card');
    expect(cards).toHaveLength(1);
    expect(cards[0]).toHaveAttribute('data-count', '1');
    expect(screen.queryByRole('button', { name: /toolArtifact/i })).toBeNull();
  });
});
