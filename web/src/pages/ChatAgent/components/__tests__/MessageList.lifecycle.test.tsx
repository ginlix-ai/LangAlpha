/**
 * Partition-timing coverage for the live → accordion lifecycle in
 * MessageContentSegments (textOnly mode).
 *
 * The partition memo in MessageList.tsx decides whether each reasoning /
 * tool-call item renders in the live zone (`active` / `completing` /
 * `failed`) or folds into the accordion (`completed`):
 *
 *  - Stream end overrides the age-based cooldown: when `isStreaming` flips
 *    false, everything folds immediately — no timer advancement required.
 *    This also guarantees history/replay items (isStreaming always false)
 *    never enter the live zone.
 *  - While streaming, a just-completed item lingers in the live zone for
 *    the MIN_LIVE_EXPOSURE_MS cooldown, then folds when the internal tick
 *    timer fires.
 *  - Inline-artifact tools with a ready artifact render as compact artifact
 *    blocks — never in the live zone, never in the accordion timeline.
 *
 * Driven through the public `MessageContentSegments` export with fake
 * timers. framer-motion is stubbed so AnimatePresence unmounts exiting
 * nodes synchronously — these tests assert partition output, not animation.
 * All data is neutral placeholder data.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageContentSegments } from '../MessageList';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Stub framer-motion: motion.* render as plain elements (framer-only props
// stripped), AnimatePresence is a passthrough (exits unmount synchronously),
// `animate` (used by useAnimatedText) is a no-op. `motion.create` mirrors the
// real API used by TextShimmer.
vi.mock('framer-motion', async () => {
  const React = await vi.importActual<typeof import('react')>('react');
  const FRAMER_ONLY_PROPS = new Set([
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
    'onAnimationComplete', 'onAnimationStart',
  ]);
  // Loosened createElement signature — the stub forwards arbitrary tag names
  // and components without modeling their prop types.
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

// Markdown is heavy and irrelevant to partition logic.
vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
}));

// Control the inline-artifact tool set so the no-flash test doesn't depend on
// production tool names. `stock_prices` is a key of INLINE_ARTIFACT_MAP in
// MessageList, so the mocked card below is what a ready artifact renders as.
vi.mock('../charts/InlineArtifactCards', async () => {
  const React = await vi.importActual<typeof import('react')>('react');
  return {
    INLINE_ARTIFACT_TOOLS: new Set<string>(['fetch_sample_chart']),
    InlineStockPriceCard: () => React.createElement('div', { 'data-testid': 'inline-chart' }),
    InlineCompanyOverviewCard: () => null,
    InlineMarketIndicesCard: () => null,
    InlineSectorPerformanceCard: () => null,
    InlineMarketOverviewCard: () => null,
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

const SUMMARY_BUTTON_RE = /toolArtifact/i;

function completedToolProc(createdAt: number, over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    toolName: 'Read',
    toolCall: { args: { file_path: 'docs/sample-notes.md' } },
    isInProgress: false,
    isComplete: true,
    isFailed: false,
    _createdAt: createdAt,
    ...over,
  };
}

beforeEach(() => {
  // Date is faked alongside setTimeout so the partition's Date.now() ages and
  // the tick timer advance together under vi.advanceTimersByTime.
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
});

afterEach(() => {
  vi.useRealTimers();
});

// ---------------------------------------------------------------------------
// Immediate fold on stream end
// ---------------------------------------------------------------------------

describe('MessageContentSegments — immediate fold on stream end', () => {
  it('folds completing items into the accordion as soon as isStreaming flips false, with no timer advance', () => {
    const now = Date.now();
    const props: SegmentsProps = {
      ...baseProps,
      segments: [
        { type: 'reasoning', order: 0, reasoningId: 'rs-1' },
        { type: 'tool_call', order: 1, toolCallId: 'tc-1' },
      ],
      reasoningProcesses: {
        'rs-1': { isReasoning: false, content: 'placeholder reasoning text', reasoningComplete: true, _completedAt: now },
      },
      toolCallProcesses: { 'tc-1': completedToolProc(now) },
      isStreaming: true,
    };

    const view = render(<MessageContentSegments {...props} />);

    // Both items just completed → inside the exposure window → live zone.
    expect(view.container.querySelector('.nrow')).not.toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // Stream ends. No timers advanced — the fold must be immediate.
    view.rerender(<MessageContentSegments {...props} isStreaming={false} />);

    expect(screen.getByRole('button', { name: SUMMARY_BUTTON_RE })).toBeInTheDocument();
    expect(view.container.querySelector('.nrow')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Always-live tools survive stream end while still in progress
// ---------------------------------------------------------------------------

describe('MessageContentSegments — always-live in-progress tools', () => {
  it('keeps an in-progress TaskOutput in the live zone after isStreaming flips false (subagent still running)', () => {
    const now = Date.now();
    const props: SegmentsProps = {
      ...baseProps,
      segments: [{ type: 'tool_call', order: 0, toolCallId: 'tc-task' }],
      toolCallProcesses: {
        // TaskOutput is in ALWAYS_LIVE_TOOLS. In-progress = the agent is waiting
        // on a background subagent. The main stream may end before the subagent
        // finishes — the indicator must stay visible, not fold into the accordion.
        'tc-task': {
          toolName: 'TaskOutput',
          toolCall: { args: {} },
          isInProgress: true,
          isComplete: false,
          isFailed: false,
          _createdAt: now,
        },
      },
      isStreaming: true,
    };

    const view = render(<MessageContentSegments {...props} />);

    // Streaming: live row present, no accordion.
    expect(view.container.querySelector('.nrow')).not.toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // Main stream ends but the tool is still in progress (subagent running).
    // It must STAY in the live zone — not fold into the accordion.
    view.rerender(<MessageContentSegments {...props} isStreaming={false} />);

    expect(view.container.querySelector('.nrow')).not.toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // When the subagent finishes, the tool completes → it folds to the accordion.
    const completedProps: SegmentsProps = {
      ...props,
      isStreaming: false,
      toolCallProcesses: {
        'tc-task': {
          toolName: 'TaskOutput', toolCall: { args: {} },
          isInProgress: false, isComplete: true, isFailed: false, _createdAt: now,
        },
      },
    };
    view.rerender(<MessageContentSegments {...completedProps} />);

    expect(view.container.querySelector('.nrow')).toBeNull();
    expect(screen.getByRole('button', { name: SUMMARY_BUTTON_RE })).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Cooldown boundary while streaming
// ---------------------------------------------------------------------------

describe('MessageContentSegments — live-zone cooldown while streaming', () => {
  it('keeps a just-completed item in the live zone through the cooldown, then folds it when the window elapses', () => {
    // Item completed 1000ms ago — still inside the exposure window. The ages
    // below track MIN_LIVE_EXPOSURE_MS (currently 1800ms): first advance stays
    // inside the window, second crosses the boundary. Adjust both together if
    // the constant is tuned.
    const createdAt = Date.now() - 1000;
    const props: SegmentsProps = {
      ...baseProps,
      segments: [{ type: 'tool_call', order: 0, toolCallId: 'tc-1' }],
      toolCallProcesses: { 'tc-1': completedToolProc(createdAt) },
      isStreaming: true,
    };

    const view = render(<MessageContentSegments {...props} />);

    // Inside the window: live row, no accordion.
    expect(view.container.querySelector('.nrow')).not.toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // Still inside the window — nothing folds.
    act(() => { vi.advanceTimersByTime(600); });
    expect(view.container.querySelector('.nrow')).not.toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // Crossing the boundary fires the tick timer and folds the item.
    act(() => { vi.advanceTimersByTime(1000); });
    expect(screen.getByRole('button', { name: SUMMARY_BUTTON_RE })).toBeInTheDocument();
    expect(view.container.querySelector('.nrow')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Inline-artifact tools never flash through live zone or accordion
// ---------------------------------------------------------------------------

describe('MessageContentSegments — inline-artifact tools', () => {
  it('renders a ready artifact as a compact chart block, never in the live zone or accordion — streaming and after stream end', () => {
    const now = Date.now();
    const props: SegmentsProps = {
      ...baseProps,
      segments: [{ type: 'tool_call', order: 0, toolCallId: 'tc-art' }],
      toolCallProcesses: {
        // Fresh completion (inside the cooldown window) with a ready artifact:
        // the artifact branch must win over the completing branch.
        'tc-art': completedToolProc(now, {
          toolName: 'fetch_sample_chart',
          toolCall: { args: {} },
          toolCallResult: { artifact: { type: 'stock_prices', data: [] } },
        }),
      },
      isStreaming: true,
    };

    const view = render(<MessageContentSegments {...props} />);

    expect(screen.getByTestId('inline-chart')).toBeInTheDocument();
    expect(view.container.querySelector('.nrow')).toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();

    // Same invariants after the stream ends.
    view.rerender(<MessageContentSegments {...props} isStreaming={false} />);

    expect(screen.getByTestId('inline-chart')).toBeInTheDocument();
    expect(view.container.querySelector('.nrow')).toBeNull();
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();
  });
});
