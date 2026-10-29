import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import ActivityBlock from '../ActivityBlock';

// ---------------------------------------------------------------------------
// Mocks, keep the component mountable in jsdom and surface i18n keys.
// ---------------------------------------------------------------------------

// Browser tests own animation frames; these assertions own row identity and disclosure state.
vi.mock('@/lib/framer', async (importOriginal) => ({
  ...await importOriginal<typeof import('@/lib/framer')>(),
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

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

vi.mock('../charts/InlineArtifactCards', () => ({
  INLINE_ARTIFACT_TOOLS: new Set<string>(),
  isInlineArtifactReady: () => false,
  InlineStockPriceCard: () => null,
  InlineCompanyOverviewCard: () => null,
  InlineMarketIndicesCard: () => null,
  InlineSectorPerformanceCard: () => null,
  InlineMarketOverviewCard: () => null,
  InlineSecFilingCard: () => null,
  InlineStockScreenerCard: () => null,
  InlineWebSearchCard: () => null,
}));

vi.mock('../charts/InlineAutomationCards', () => ({
  InlineAutomationCard: () => null,
}));

vi.mock('../charts/InlinePreviewCard', () => ({
  InlinePreviewCard: () => null,
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type ActivityItem = Parameters<typeof ActivityBlock>[0]['items'][number];
type ToolItem = Extract<ActivityItem, { type: 'tool_call' }>;

function toolItem(liveState: ActivityItem['_liveState'], opts: Partial<ToolItem> = {}): ToolItem {
  return {
    type: 'tool_call',
    id: opts.id ?? 'tc-1',
    toolCallId: opts.id ?? 'tc-1',
    toolName: 'Read',
    toolCall: { args: { file_path: 'work/sample-notes.md' } },
    _liveState: liveState,
    ...opts,
  };
}

const SUMMARY_BUTTON_RE = /toolArtifact/i;

// ---------------------------------------------------------------------------
// Live-row lifecycle states
// ---------------------------------------------------------------------------

describe('ActivityBlock, live-row lifecycle states', () => {
  it('renders an active tool row with the state-active left-rule hook and no badge', () => {
    const items = [toolItem('active', { isComplete: false })];
    const { container } = render(<ActivityBlock items={items} isStreaming={true} />);

    const row = container.querySelector('.nrow.state-active');
    expect(row).not.toBeNull();
    expect(container.querySelector('.nrow-badge')).toBeNull();
    // Active rows live in the live zone, no accordion summary yet.
    expect(screen.queryByRole('button', { name: SUMMARY_BUTTON_RE })).toBeNull();
  });

  it('renders a completing tool row without the active rule and without a badge', () => {
    const items = [toolItem('completing', { isComplete: true, _recentlyCompleted: true })];
    const { container } = render(<ActivityBlock items={items} isStreaming={true} />);

    const row = container.querySelector('.nrow');
    expect(row).not.toBeNull();
    expect(container.querySelector('.nrow.state-active')).toBeNull();
    expect(container.querySelector('.nrow-badge')).toBeNull();
  });

  it('renders the failed ✕ badge on a live failed row with the a11y label', () => {
    const items = [toolItem('failed', { isComplete: true, isFailed: true, _recentlyCompleted: true })];
    const { container } = render(<ActivityBlock items={items} isStreaming={true} />);

    const badge = container.querySelector('.nrow .nrow-badge');
    expect(badge).not.toBeNull();
    expect(badge!.getAttribute('aria-label')).toBe('toolArtifact.a11y.toolCallFailed');
    // Failed rows stay neutral, no active rule.
    expect(container.querySelector('.nrow.state-active')).toBeNull();
  });
});

describe('ActivityBlock, unified timeline', () => {
  const completed = toolItem('completed', { id: 'done', isComplete: true });
  const reasoning: ActivityItem = {
    type: 'reasoning', id: 'thought', content: '**Inspecting**\n\nThe evidence is arriving.', _liveState: 'active',
  };

  it.each([false, true])('honors live reasoning preference %s without a turn fold', (open) => {
    render(<ActivityBlock items={[reasoning]} isStreaming presentation="inline" liveReasoningOpen={open} />);
    const thought = screen.getByRole('button', { name: 'Inspecting' });
    expect(thought).toHaveAttribute('aria-expanded', String(open));
    if (!open) {
      expect(screen.queryByTestId('markdown-content')).toBeNull();
      fireEvent.click(thought);
      expect(thought).toHaveAttribute('aria-expanded', 'true');
    }
  });

  it('keeps a live reasoning row and its disclosure choice mounted across accordion toggles', () => {
    const { container } = render(<ActivityBlock items={[completed, reasoning]} isStreaming presentation="folded" liveReasoningOpen />);
    const row = container.querySelector('[data-activity-state="live"]');
    const thought = screen.getByRole('button', { name: 'Inspecting' });
    expect(thought).toHaveAttribute('aria-expanded', 'true');
    fireEvent.click(thought);
    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));
    expect(container.querySelector('[data-activity-state="live"]')).toBe(row);
    expect(screen.getByRole('button', { name: 'Inspecting' })).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));
    expect(container.querySelector('[data-activity-state="live"]')).toBe(row);
  });

  it('shows verbose reasoning inside an expanded accordion, then closes it on settlement', async () => {
    const { container, rerender } = render(<ActivityBlock items={[completed, reasoning]} isStreaming presentation="expanded" liveReasoningOpen />);
    const row = container.querySelector('[data-activity-state="live"]');
    expect(screen.getByRole('button', { name: 'Inspecting' })).toHaveAttribute('aria-expanded', 'true');
    rerender(<ActivityBlock items={[completed, { ...reasoning, _liveState: 'completed' }]} isStreaming={false} presentation="expanded" liveReasoningOpen />);
    expect(row).toHaveAttribute('data-activity-state', 'settled');
    expect(screen.getByRole('button', { name: 'Inspecting' })).toHaveAttribute('aria-expanded', 'false');
    await waitFor(() => expect(container.querySelector('.titem-reasoning-card')).toBeNull());
  });

  it('binds an anonymous preparing row to the landed tool without an exit overlay', () => {
    const { container, rerender } = render(<ActivityBlock items={[]} preparingToolCall={{ calls: [{ pendingKey: '0@1', toolName: 'Read', argsLength: 2 }] }} isStreaming presentation="folded" />);
    const row = container.querySelector('[data-activity-state="live"]');
    rerender(<ActivityBlock items={[toolItem('active')]} isStreaming presentation="folded" />);
    expect(container.querySelectorAll('[data-activity-state="live"]')).toHaveLength(1);
    expect(container.querySelector('[data-activity-state="live"]')).toBe(row);
  });

  it('paces newly arriving reasoning phases instead of jumping to the last one', () => {
    vi.useFakeTimers();
    try {
      const { rerender } = render(<ActivityBlock items={[reasoning]} isStreaming presentation="folded" />);
      rerender(<ActivityBlock items={[{ ...reasoning, content: reasoning.content + '\n\n**Comparing**\n\nThen compare.\n\n**Drafting**\n\nThen write.' }]} isStreaming presentation="folded" />);
      expect(screen.queryByText('Comparing')).toBeNull();
      expect(screen.queryByText('Drafting')).toBeNull();
      act(() => vi.advanceTimersByTime(700));
      expect(screen.getByText('Comparing')).toBeInTheDocument();
      expect(screen.queryByText('Drafting')).toBeNull();
      act(() => vi.advanceTimersByTime(700));
      expect(screen.getByText('Drafting')).toBeInTheDocument();
    } finally { vi.useRealTimers(); }
  });
});
