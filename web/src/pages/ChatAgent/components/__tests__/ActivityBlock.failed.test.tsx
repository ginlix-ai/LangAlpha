/**
 * Coverage for the failed-tool-call surfacing logic added in Fix #3, the
 * memo-write classification surfacing in Fix #4, and the priority-fragment
 * cap in Fix #14.
 *
 * Strategy: render `ActivityBlock` directly with a small set of synthesized
 * `_liveState: 'completed'` items, and assert against the rendered DOM. The
 * t() identity mock returns the i18n key as-is so the assertions can pin
 * the exact branch (e.g. `categoryCount.failed`, `categoryCount.memoWrite`)
 * without depending on the bundled English copy.
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
// Fix #3 — failed-call surfacing
// ---------------------------------------------------------------------------

// `summaryLabel` is title-cased before render (charAt(0).toUpperCase()), so
// the accessible name comes through with a leading capital. Use a
// case-insensitive regex when looking up the toggle by name.
const SUMMARY_BUTTON_RE = /toolArtifact/i;

describe('ActivityBlock — failed tool calls (Fix #3)', () => {
  it('appends a "failed" fragment to the folded accordion summary when an item is failed', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-1',
        isFailed: true,
        toolCall: { args: { file_path: 'work/scratch.md' } },
      }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);

    // Folded summary contains the failed fragment.
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount\.failed/i);
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

  it('counts successful and failed reads distinctly in the folded summary', () => {
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
    // Three reads in the fileRead bucket (orthogonal to failure axis).
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.fileRead/i);
    // ...and one failed fragment alongside it.
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.failed/i);
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
// Fix #4 — memo write/edit classification in the summary
// ---------------------------------------------------------------------------

describe('ActivityBlock — memo write/edit fragment (Fix #4)', () => {
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
// Fix #14 — priority fragments survive the FOLDED_MAX cap
// ---------------------------------------------------------------------------

describe('ActivityBlock — priority fragments survive the cap (Fix #14)', () => {
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

  it('keeps a failed fragment visible even with 4+ categories present', () => {
    const items: ActivityItem[] = [
      completedTool('Read', {
        id: 'r-fail',
        isFailed: true,
        toolCall: { args: { file_path: 'work/scratch.md' } },
      }),
      completedTool('ExecuteCode', { id: 'c-1' }),
      completedTool('WebSearch', { id: 'wb-1' }),
      completedTool('Glob', { id: 'g-1' }),
      // A second non-priority bucket to overflow past the cap.
      completedTool('Read', { id: 'r-2', toolCall: { args: { file_path: 'work/other.md' } } }),
    ];

    render(<ActivityBlock items={items} isStreaming={false} />);
    const summary = screen.getByRole('button', { name: SUMMARY_BUTTON_RE });
    expect(summary).toHaveTextContent(/toolArtifact\.categoryCount.failed/i);
    expect(summary).toHaveTextContent(/toolArtifact\.andMore/i);
  });
});

// ---------------------------------------------------------------------------
// Fix #16 — accordion accessibility
// ---------------------------------------------------------------------------

describe('ActivityBlock — accordion a11y (Fix #16)', () => {
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
