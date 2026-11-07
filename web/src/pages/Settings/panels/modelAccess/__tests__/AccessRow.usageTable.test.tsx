/**
 * A subscription's row in the Settings summary's usage table: one with no
 * Claude plan takes the window columns for its text, as a card of the
 * Claude Code view does.
 */
import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import type { UsageSteps } from '@/components/modelAccess/usageSteps';
import { UsageTableContext } from '@/components/modelAccess/usageSteps';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (k: string) => k }) }));
vi.mock('@/pages/ChatAgent/components/mcp/BrandMark', () => ({ BrandMark: () => <span /> }));
vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'UTC', now: Date.parse('2026-11-07T15:00:00Z') }),
}));

import { AccessRow } from '../AccessRows';

const FULL: UsageSteps = {
  grid: '', window: 'flex', resets: 'block', header: 'grid', spanUsage: 'col-span-4', inline: 'hidden', row: 'grid grid-cols-subgrid',
  tail: 'contents', columns: true,
};
const PLAN = {
  five_hour: { used: 0.4, resets_at: '2099-01-01T00:00:00Z' }, seven_day: { used: 0.2, resets_at: '2099-01-01T00:00:00Z' },
  limits: [], credits: null, observed_at: '2026-11-07T15:00:00Z',
};

const row = (plan?: typeof PLAN) => render(
  <UsageTableContext.Provider value={FULL}>
    <ul><AccessRow provider="codex" name="ChatGPT" detail="you@example.com · ChatGPT Plus" plan={plan} status="connected" action={null} /></ul>
  </UsageTableContext.Provider>,
).container;

describe('AccessRow in a usage table', () => {
  it('lays a row with no plan out across the window columns, with no cells in them', () => {
    const li = row();
    expect(li.querySelector('[data-usage-cell]')).toBeNull();
    expect(li.querySelector('[data-row-detail]')?.parentElement).toHaveClass('col-span-4');
  });

  it('keeps the window columns for a row with a plan', () => {
    const li = row(PLAN);
    expect(li.querySelectorAll('[data-usage-cell]')).toHaveLength(3);
    expect(li.querySelector('[data-row-detail]')?.parentElement).not.toHaveClass('col-span-4');
  });
});
