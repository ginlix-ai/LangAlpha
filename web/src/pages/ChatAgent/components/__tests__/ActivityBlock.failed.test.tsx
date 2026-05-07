/**
 * Coverage for the per-row failed-tool-call rendering, the memo-write
 * classification surfacing in the folded summary, and the priority-fragment
 * cap that protects high-signal slots (memoWrite, memoryUpdated) from being
 * truncated when many tool categories are present.
 *
 * Failed tool calls are rendered per-row only — the folded summary intentionally
 * does NOT include a "{N} failed" fragment. The negative assertions below
 * lock that contract in.
 *
 * Strategy: render `ActivityBlock` directly with a small set of synthesized
 * `_liveState: 'completed'` items, and assert against the rendered DOM. The
 * t() identity mock returns the i18n key as-is so the assertions can pin
 * the exact branch (e.g. `categoryCount.memoWrite`) without depending on the
 * bundled English copy.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, within, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import ActivityBlock from '../ActivityBlock';

// ---------------------------------------------------------------------------
// Mocks — keep the component mountable in jsdom and surface i18n keys.
// ---------------------------------------------------------------------------

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

// Markdown is heavy and irrelevant to these tests.
vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
}));

// Inline artifact cards aren't used by these test items but are imported by
// ActivityBlock — stub them so the module graph stays light.
vi.mock('../charts/InlineArtifactCards', () => ({
  INLINE_ARTIFACT_TOOLS: new Set<string>(),
  InlineStockPriceCard: () => null,
  InlineCompanyOverviewCard: () => null,
  InlineMarketIndicesCard: () => null,
  InlineSectorPerformanceCard: () => null,
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

function completedTool(toolName: string, opts: Partial<ActivityItem> = {}): ActivityItem {
  return {
    type: 'tool_call',
    id: opts.id ?? `${toolName}-${Math.random().toString(36).slice(2, 8)}`,
    toolName,
    toolCall: opts.toolCall ?? { args: {} },
    isComplete: true,
    _liveState: 'completed',
    ...opts,
  } as ActivityItem;
}

// ---------------------------------------------------------------------------
// Failed tool calls — per-row rendering, no summary fragment
// ---------------------------------------------------------------------------

// `summaryLabel` is title-cased before render (charAt(0).toUpperCase()), so
// the accessible name comes through with a leading capital. Use a
// case-insensitive regex when looking up the toggle by name.
const SUMMARY_BUTTON_RE = /toolArtifact/i;

describe('ActivityBlock — failed tool calls', () => {
  it('does NOT add a failed fragment to the folded accordion summary when an item is failed', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-1',
        isFailed: true,
        toolCall: { args: { file_path: 'work/scratch.md' } },
      }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);

    // Folded summary intentionally omits the failed count — failure
    // visibility lives on the per-row badge instead.
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).not.toHaveTextContent(/toolArtifact\.categoryCount\.failed/i);
  });

  it('renders the failed badge on the timeline icon when the accordion is expanded', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-1',
        isFailed: true,
        toolCall: { args: { file_path: 'work/scratch.md' } },
      }),
    ];

    const { container } = render(<ActivityBlock items={items} isStreaming={false} />);

    // Expand the accordion.
    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));

    // The failed item gets the .failed class hook + a badge with the
    // toolCallFailed a11y label.
    const failedRow = container.querySelector('.titem.failed');
    expect(failedRow).not.toBeNull();
    const badge = failedRow!.querySelector('.nrow-badge');
    expect(badge).not.toBeNull();
    expect(badge!.getAttribute('aria-label')).toBe('toolArtifact.a11y.toolCallFailed');
  });

  it('renders the failed badge on an Edit row (EditToolRow path)', () => {
    const items: ActivityItem[] = [
      completedTool('Edit', {
        id: 'e-1',
        isFailed: true,
        toolCall: {
          args: {
            file_path: 'work/scratch.md',
            old_string: 'foo',
            new_string: 'bar',
          },
        },
      }),
    ];

    const { container } = render(<ActivityBlock items={items} isStreaming={false} />);
    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));

    const failedRow = container.querySelector('.titem.failed');
    expect(failedRow).not.toBeNull();
    expect(failedRow!.querySelector('.nrow-badge')).not.toBeNull();
  });

  it('counts all reads in the fileRead bucket regardless of failure state', () => {
    const items: ActivityItem[] = [
      completedTool('Read', { id: 'r-1', toolCall: { args: { file_path: 'a.md' } } }),
      completedTool('Read', { id: 'r-2', toolCall: { args: { file_path: 'b.md' } } }),
      completedTool('Read', {
        id: 'r-3',
        isFailed: true,
        toolCall: { args: { file_path: 'c.md' } },
      }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    // Three reads in the fileRead bucket (failures don't split the count).
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.fileRead/i);
    // No standalone failed fragment.
    expect(summary).not.toHaveTextContent(/toolArtifact\.categoryCount.failed/i);
  });

  it('does not render any failed badge when no item is failed', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-1',
        toolCall: { args: { file_path: 'work/scratch.md' } },
      }),
    ];

    const { container } = render(<ActivityBlock items={items} isStreaming={false} />);
    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));
    expect(container.querySelector('.titem.failed')).toBeNull();
    expect(container.querySelector('.nrow-badge')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Memo write/edit classification in the summary
// ---------------------------------------------------------------------------

describe('ActivityBlock — memo write/edit fragment', () => {
  it('emits a memoWrite fragment when the agent writes a memo', () => {
    const items: ActivityItem[] = [
      completedTool('Write', {
        id: 'w-1',
        toolCall: { args: { file_path: '.agents/user/memo/notes.md' } },
      }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.memoWrite/i);
    // It must NOT show as a memo read.
    expect(summary).not.toHaveTextContent(/toolArtifact\.categoryCount.memo /i);
  });

  it('keeps memo reads in the existing memo fragment', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-1',
        toolCall: { args: { file_path: '.agents/user/memo/notes.md' } },
      }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.memo/i);
    expect(summary).not.toHaveTextContent(/toolArtifact\.categoryCount.memoWrite/i);
  });
});

// ---------------------------------------------------------------------------
// Priority fragments survive the FOLDED_MAX cap
// ---------------------------------------------------------------------------

describe('ActivityBlock — priority fragments survive the cap', () => {
  it('keeps memoryWrite visible even with 4+ categories present', () => {
    const items: ActivityItem[] = [
      // A memory write — the high-signal fragment we don't want to hide.
      completedTool('Write', {
        id: 'mw-1',
        toolCall: { args: { file_path: '.agents/user/memory/risk.md' } },
      }),
      // Pad with three other categories so the cap kicks in.
      completedTool('ExecuteCode', { id: 'c-1' }),
      completedTool('WebSearch', { id: 'wb-1' }),
      completedTool('Glob', { id: 'g-1' }),
      completedTool('Read', { id: 'r-1', toolCall: { args: { file_path: 'work/scratch.md' } } }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    // memoryUpdated must be in the visible 3 even with overflow.
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.memoryUpdated/i);
    // The "and more" suffix indicates the cap fired.
    expect(summary).toHaveTextContent(/toolArtifact\.andMore/i);
  });
});

// ---------------------------------------------------------------------------
// Accordion accessibility
// ---------------------------------------------------------------------------

describe('ActivityBlock — accordion a11y', () => {
  it('toggles aria-expanded on the summary button', () => {
    const items: ActivityItem[] = [
      completedTool('Read', { id: 'r-1', toolCall: { args: { file_path: 'work/scratch.md' } } }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).toHaveAttribute('aria-expanded', 'false');

    fireEvent.click(summary);
    expect(summary).toHaveAttribute('aria-expanded', 'true');
  });

  it('points aria-controls at the timeline region with matching aria-labelledby', () => {
    const items: ActivityItem[] = [
      completedTool('Read', { id: 'r-1', toolCall: { args: { file_path: 'work/scratch.md' } } }),
    ];

    const { container } = render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    fireEvent.click(summary);

    const controlsId = summary.getAttribute('aria-controls');
    expect(controlsId).toBeTruthy();
    const region = container.querySelector(`#${CSS.escape(controlsId!)}`);
    expect(region).not.toBeNull();
    expect(region!.getAttribute('role')).toBe('region');
    expect(region!.getAttribute('aria-labelledby')).toBe(summary.id);
    // The timeline region holds at least one row.
    expect(within(region as HTMLElement).getAllByRole('listitem').length).toBeGreaterThan(0);
  });
});
