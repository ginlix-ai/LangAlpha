/**
 * Live-row lifecycle states + replay parity for ActivityBlock.
 *
 * Live rows: `active` gets the left-rule class hook + shimmer label,
 * `completing` is a neutral dimmed row (no badge), `failed` gets the gray ✕
 * badge with the toolCallFailed a11y label — all in the live zone, no
 * accordion interaction required.
 *
 * Replay parity: with `isStreaming={false}` (history / reconnect replay),
 * completed rows must render WITHOUT the motion.li entrance wrapper —
 * `newlyCompletedIds` is gated on isStreaming, so replayed timelines are
 * animation-free. The motion.li wrapper carries `overflow: hidden` inline
 * style while the plain <li> does not; we assert on that distinction.
 *
 * Strategy mirrors ActivityBlock.failed.test.tsx: render `ActivityBlock`
 * directly with synthesized items and an identity t() mock. All data is
 * neutral placeholder data.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
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

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
}));

vi.mock('../charts/InlineArtifactCards', () => ({
  INLINE_ARTIFACT_TOOLS: new Set<string>(),
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

function toolItem(liveState: ActivityItem['_liveState'], opts: Partial<ActivityItem> = {}): ActivityItem {
  return {
    type: 'tool_call',
    id: opts.id ?? 'tc-1',
    toolName: 'Read',
    toolCall: { args: { file_path: 'work/sample-notes.md' } },
    _liveState: liveState,
    ...opts,
  } as ActivityItem;
}

const SUMMARY_BUTTON_RE = /toolArtifact/i;

// ---------------------------------------------------------------------------
// Live-row lifecycle states
// ---------------------------------------------------------------------------

describe('ActivityBlock — live-row lifecycle states', () => {
  it('renders an active tool row with the state-active left-rule hook and no badge', () => {
    const items = [toolItem('active', { isComplete: false })];
    const { container } = render(<ActivityBlock items={items} isStreaming={true} />);

    const row = container.querySelector('.nrow.state-active');
    expect(row).not.toBeNull();
    expect(container.querySelector('.nrow-badge')).toBeNull();
    // Active rows live in the live zone — no accordion summary yet.
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
    // Failed rows stay neutral — no active rule.
    expect(container.querySelector('.nrow.state-active')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Replay parity — no entrance animation on history / reconnect replay
// ---------------------------------------------------------------------------

describe('ActivityBlock — replay parity', () => {
  it('renders completed rows without the motion.li entrance wrapper when not streaming', () => {
    const items = [
      toolItem('completed', { id: 'tc-1', isComplete: true }),
      toolItem('completed', { id: 'tc-2', isComplete: true }),
    ];
    const { container } = render(<ActivityBlock items={items} isStreaming={false} />);

    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));

    const rows = container.querySelectorAll('.timeline > li');
    expect(rows.length).toBe(2);
    for (const row of rows) {
      // motion.li entrance wrapper carries overflow:hidden; plain <li> does not.
      expect((row as HTMLElement).style.overflow).not.toBe('hidden');
    }
  });

  it('renders newly completed rows WITH the entrance wrapper while streaming (contrast case)', () => {
    const items = [toolItem('completed', { id: 'tc-1', isComplete: true })];
    const { container } = render(<ActivityBlock items={items} isStreaming={true} />);

    fireEvent.click(screen.getByRole('button', { name: SUMMARY_BUTTON_RE }));

    const rows = container.querySelectorAll('.timeline > li');
    expect(rows.length).toBe(1);
    expect((rows[0] as HTMLElement).style.overflow).toBe('hidden');
  });
});
