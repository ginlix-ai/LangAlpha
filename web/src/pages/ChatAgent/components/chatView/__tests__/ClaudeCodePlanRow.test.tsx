/**
 * The row above a Claude Code thread's composer when the Claude plan behind
 * the workspace's token is at its limit: used up under Stop, or running on
 * usage credits the user chose; near it, from 80% of a window, how much is
 * used. Nothing on another model, nothing below that, and a weekly Opus
 * limit says nothing on a Sonnet thread.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { ClaudeCodePlan, ClaudeCodePlanHeld, ClaudeCodePlanWindow } from '@/api/model';
import type { PlanOut } from '@/lib/claudeCodePlan';

const h = vi.hoisted(() => ({
  plan: null as ClaudeCodePlan | null, planOut: 'stop' as PlanOut,
  meter: undefined as { credential: { kind: 'saved' } | null; checkAfter: number | null } | undefined,
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    metadata: {
      'claude-opus-5-5-code': { provider: 'claude-code', sdk: 'claude-code' },
      'claude-sonnet-5-5-code': { provider: 'claude-code', sdk: 'claude-code' },
      'claude-opus-5-5': { provider: 'anthropic' },
    },
  }),
}));
vi.mock('@/hooks/useWorkspaceClaudeCode', () => ({ useWorkspaceClaudeCode: () => ({ plan: h.plan, meter: h.meter }) }));
vi.mock('@/components/modelAccess/PlanCheck', () => ({
  CheckNowFor: ({ target }: { target: { credential: { kind: string } } }) => <button type="button">{`Check now:${target.credential.kind}`}</button>,
}));
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
    h.meter = { credential: { kind: 'saved' }, checkAfter: null };
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

  it('offers Check now where the account has the usage credits the user chose off', () => {
    h.plan = limited('five_hour', 'off');
    h.planOut = 'credits';
    row('claude-opus-5-5-code');
    expect(screen.getByRole('status')).toHaveTextContent("Your Claude plan's 5-hour limit is used up until 3:40 PM.");
    expect(screen.getByRole('button', { name: 'Check now:saved' })).toBeInTheDocument();
  });

  it('offers no Check now under Stop, on credits that pay, or without a credential to check', () => {
    h.plan = limited('five_hour', 'off');
    row('claude-opus-5-5-code');
    h.plan = limited('five_hour', 'on');
    h.planOut = 'credits';
    row('claude-opus-5-5-code');
    h.plan = limited('five_hour', 'off');
    h.meter = { credential: null, checkAfter: null };
    row('claude-opus-5-5-code');
    expect(screen.getAllByRole('status')).toHaveLength(3);
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('says how much of a window is used from 80% of it', () => {
    h.plan = { ...limited('five_hour', null), five_hour: { used: 0.8, resets_at: '2026-11-06T20:40:00Z' }, limits: [] };
    row('claude-opus-5-5-code');
    expect(screen.getByRole('status')).toHaveTextContent("You've used 80% of your Claude plan's 5-hour limit. It resets at 3:40 PM.");
  });

  it('says nothing on another model, below 80%, or for another family', () => {
    h.plan = limited('five_hour', null);
    row('claude-opus-5-5');
    h.plan = { ...limited('five_hour', null), five_hour: { used: 0.79, resets_at: '2026-11-06T20:40:00Z' }, limits: [] };
    row('claude-opus-5-5-code');
    h.plan = { ...limited('seven_day_opus', null, 'opus'), five_hour: { used: 0.5, resets_at: '2026-11-06T20:40:00Z' } };
    row('claude-sonnet-5-5-code');
    expect(screen.queryByRole('status')).toBeNull();
  });
});
