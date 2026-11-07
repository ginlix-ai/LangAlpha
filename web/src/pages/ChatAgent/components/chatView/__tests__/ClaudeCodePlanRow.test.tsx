/**
 * The row above a Claude Code thread's composer when the Claude plan behind
 * the workspace's token is at its limit: used up under Stop, or running on
 * usage credits the user chose. Nothing on another model, nothing below the
 * limit, and a weekly Opus limit says nothing on a Sonnet thread.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { ClaudeCodePlan, ClaudeCodePlanHeld, ClaudeCodePlanWindow } from '@/api/model';
import type { PlanOut } from '@/lib/claudeCodePlan';

const h = vi.hoisted(() => ({ plan: null as ClaudeCodePlan | null, planOut: 'stop' as PlanOut }));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    metadata: {
      'claude-opus-5-5-code': { provider: 'claude-code', sdk: 'claude-code' },
      'claude-sonnet-5-5-code': { provider: 'claude-code', sdk: 'claude-code' },
      'claude-opus-5-5': { provider: 'anthropic' },
    },
  }),
}));
vi.mock('@/hooks/useWorkspaceClaudeCode', () => ({ useWorkspaceClaudeCode: () => ({ plan: h.plan }) }));
vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'America/New_York', now: Date.parse('2026-11-06T19:05:00Z') }),
  usePlanOut: () => ({ planOut: h.planOut }),
}));

import { ClaudeCodePlanRow } from '../ClaudeCodePlanRow';

const limited = (
  window: ClaudeCodePlanWindow, credits: ClaudeCodePlan['credits'], family: ClaudeCodePlanHeld['family'] = null,
): ClaudeCodePlan => ({
  five_hour: { used: 1, resets_at: '2026-11-06T20:40:00Z' }, seven_day: null,
  limits: [{ window, family, until: '2026-11-06T20:40:00Z', reset_known: true }], credits, observed_at: '2026-11-06T19:00:00Z',
});

const row = (model: string) => render(<ClaudeCodePlanRow workspaceId="ws-1" model={model} />);

describe('ClaudeCodePlanRow', () => {
  beforeEach(() => {
    h.plan = null;
    h.planOut = 'stop';
  });

  it('says the limit is used up under Stop', () => {
    h.plan = limited('five_hour', 'on');
    row('claude-opus-5-5-code');
    expect(screen.getByRole('status')).toHaveTextContent(
      "Your Claude plan's 5-hour limit is used up until 3:40 PM. Pick another model to keep going.",
    );
  });

  it('says turns run on usage credits the user chose', () => {
    h.plan = limited('five_hour', 'on');
    h.planOut = 'credits';
    row('claude-opus-5-5-code');
    expect(screen.getByRole('status')).toHaveTextContent(
      "Your Claude plan's 5-hour limit is used up, so Claude Code runs on your usage credits until it resets at 3:40 PM.",
    );
  });

  it('says nothing on another model, below the limit, or for another family', () => {
    h.plan = limited('five_hour', null);
    row('claude-opus-5-5');
    h.plan = { ...limited('five_hour', null), limits: [] };
    row('claude-opus-5-5-code');
    h.plan = limited('seven_day_opus', null, 'opus');
    row('claude-sonnet-5-5-code');
    expect(screen.queryByRole('status')).toBeNull();
  });
});
