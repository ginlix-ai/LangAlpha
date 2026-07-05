/**
 * Model-fallback divider rendering in MessageContentSegments (textOnly mode):
 * the expandable error detail — a "View error" toggle (not "View summary")
 * reveals the failed model's error text in the panel below the divider.
 * (The switch-to-working-model action lives in ChatView's suggestion pill,
 * not on the divider.)
 *
 * All data is neutral placeholder data.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageContentSegments } from '../MessageList';

// ---------------------------------------------------------------------------
// Mocks (same shape as MessageList.lifecycle.test.tsx — keep the module light)
// ---------------------------------------------------------------------------

vi.mock('framer-motion', async () => {
  const React = await vi.importActual<typeof import('react')>('react');
  const FRAMER_ONLY_PROPS = new Set([
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
    'onAnimationComplete', 'onAnimationStart',
  ]);
  const createEl = React.createElement as (type: unknown, props?: unknown, ...children: unknown[]) => React.ReactElement;
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
      get: (_target, key: string) => (key === 'create' ? make : make(key)),
    }),
    AnimatePresence: ({ children }: { children?: React.ReactNode }) =>
      React.createElement(React.Fragment, null, children),
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
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
}));

vi.mock('../charts/InlineArtifactCards', async () => {
  const React = await vi.importActual<typeof import('react')>('react');
  return {
    INLINE_ARTIFACT_TOOLS: new Set<string>(['fetch_sample_chart']),
    InlineStockPriceCard: () => React.createElement('div', { 'data-testid': 'inline-chart' }),
    InlineCompanyOverviewCard: () => null,
    InlineMarketIndicesCard: () => null,
    InlineSectorPerformanceCard: () => null,
    InlineSecFilingCard: () => null,
    InlineStockScreenerCard: () => null,
    InlineWebSearchCard: () => null,
  };
});

vi.mock('../charts/InlineAutomationCards', () => ({
  InlineAutomationCard: () => null,
}));

vi.mock('../charts/InlinePreviewCard', () => ({
  InlinePreviewCard: () => null,
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function fallbackSeg(order: number, toModel: string, over: Record<string, unknown> = {}) {
  return {
    type: 'notification' as const,
    content: `fallback to ${toModel}`,
    order,
    detail: `HTTP 503 · 4 attempts\nplaceholder upstream error for ${toModel}`,
    detailKind: 'error' as const,
    ...over,
  };
}

const textSeg = (order: number, content = 'placeholder answer text') => ({
  type: 'text' as const,
  content,
  order,
});

// ---------------------------------------------------------------------------
// Error-detail expander
// ---------------------------------------------------------------------------

describe('model_fallback divider — error detail expander', () => {
  it('uses the error toggle labels and reveals the detail text', () => {
    render(
      <MessageContentSegments
        {...baseProps}
        segments={[fallbackSeg(0, 'model-b'), textSeg(1)]}
      />,
    );

    // Error flavor, not the compaction summary flavor.
    const toggle = screen.getByRole('button', { name: 'chat.viewErrorDetail' });
    expect(screen.queryByRole('button', { name: 'chat.viewSummary' })).toBeNull();
    expect(screen.queryByText(/placeholder upstream error/)).toBeNull();

    fireEvent.click(toggle);
    expect(screen.getByText(/placeholder upstream error for model-b/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'chat.hideErrorDetail' })).toBeInTheDocument();
  });

  it('keeps the summary labels for detail without detailKind (compaction)', () => {
    render(
      <MessageContentSegments
        {...baseProps}
        segments={[
          { type: 'notification' as const, content: 'context compacted', order: 0, detail: 'summary body' },
          textSeg(1),
        ]}
      />,
    );
    expect(screen.getByRole('button', { name: 'chat.viewSummary' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'chat.viewErrorDetail' })).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// No switch action on the divider (it lives in ChatView's suggestion pill)
// ---------------------------------------------------------------------------

describe('model_fallback divider — no inline switch action', () => {
  it('renders only the error toggle, no other buttons', () => {
    render(
      <MessageContentSegments
        {...baseProps}
        segments={[fallbackSeg(0, 'model-b'), fallbackSeg(1, 'model-c'), textSeg(2)]}
      />,
    );
    const buttons = screen.getAllByRole('button');
    expect(buttons.map((b) => b.textContent)).toEqual([
      'chat.viewErrorDetail',
      'chat.viewErrorDetail',
    ]);
  });
});
