/**
 * The choice of what Claude Code turns do at a Claude plan's limit: Stop by
 * default, saved as it is picked. Where a row of the view reports its
 * account's usage credits on, a note names those rows, the saved token's
 * account once on its own row, with the link to turn them off in claude.ai.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ClaudeCodeComputer, ClaudeCodePlan } from '@/api/model';
import type { TokenRow } from '@/components/modelAccess/tokenLine';
import type { PlanOut } from '@/lib/claudeCodePlan';

const h = vi.hoisted(() => ({
  planOut: 'stop' as PlanOut,
  set: vi.fn(),
  savedPlan: null as ClaudeCodePlan | null,
  computers: [] as ClaudeCodeComputer[],
}));

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ plan: h.savedPlan }),
  useClaudeCodeComputers: () => ({ computers: h.computers }),
}));
vi.mock('@/hooks/useClaudeCodePlan', () => ({ usePlanOut: () => ({ planOut: h.planOut, set: h.set }) }));
vi.mock('../../usePreferenceWrite', () => ({ useSaveFailedToast: () => vi.fn() }));

import { ClaudeCodePlanOutChoice } from '../ClaudeCodePlanOutChoice';

const plan = (credits: ClaudeCodePlan['credits']): ClaudeCodePlan => ({
  five_hour: null, seven_day: null, limits: [], credits, observed_at: '2026-11-06T19:00:00Z',
});
const computer = (computer_id: string, name: string, source: ClaudeCodeComputer['source'], credits: ClaudeCodePlan['credits']): ClaudeCodeComputer => ({
  computer_id, name, kind: 'ssh', state: 'signed_in', source, email: null, expires_at: null, plan: plan(credits),
});
const rowOf = (key: string): TokenRow => ({
  key,
  target: key === 'saved' ? { kind: 'saved' } : { kind: 'computer', computerId: key, name: key },
  tag: null,
  line: { status: 'connected', detail: null, actions: [] },
});

describe('ClaudeCodePlanOutChoice', () => {
  beforeEach(() => {
    h.planOut = 'stop';
    h.set.mockReset().mockResolvedValue({});
    h.savedPlan = null;
    h.computers = [];
  });

  it('stops by default and saves usage credits once picked', () => {
    render(<ClaudeCodePlanOutChoice tokenRows={[rowOf('saved')]} />);
    expect(screen.getByRole('radiogroup', { name: 'When your Claude plan runs out' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Stop' })).toBeChecked();
    fireEvent.click(screen.getByRole('radio', { name: 'Use usage credits' }));
    expect(h.set).toHaveBeenCalledExactlyOnceWith('credits');
  });

  it('saves usage credits again when they are picked while already chosen, by click, Space or Enter', async () => {
    h.planOut = 'credits';
    render(<ClaudeCodePlanOutChoice tokenRows={[rowOf('saved')]} />);
    const credits = screen.getByRole('radio', { name: 'Use usage credits' });
    expect(credits).toBeChecked();
    const user = userEvent.setup();
    await user.click(credits);
    await user.keyboard(' ');
    await user.keyboard('{Enter}');
    expect(h.set.mock.calls).toEqual([['credits'], ['credits'], ['credits']]);
    await user.click(screen.getByRole('radio', { name: 'Stop' }));
    expect(h.set.mock.calls).toEqual([['credits'], ['credits'], ['credits'], ['stop']]);
  });

  it('names the rows whose account has usage credits on, the saved token once', () => {
    h.savedPlan = plan('on');
    h.computers = [
      computer('mac', 'MacBook Pro', 'machine', 'on'),
      computer('lab', 'Lab box', 'saved', 'on'),
      computer('nas', 'Home server', 'machine', 'off'),
    ];
    render(<ClaudeCodePlanOutChoice tokenRows={[rowOf('saved'), rowOf('mac'), rowOf('lab'), rowOf('nas')]} />);
    expect(screen.getByText(/^Usage credits are on/)).toHaveTextContent(
      'Usage credits are on for the Claude account on MacBook Pro and your setup token. Claude bills the first request past '
      + 'your limit to them before LangAlpha can stop, so turn them off in claude.ai if you never want them used.',
    );
    const link = screen.getByRole('link', { name: 'Open claude.ai usage settings' });
    expect(link).toHaveAttribute('href', 'https://claude.ai/settings/usage');
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('says nothing of usage credits no row reports on', () => {
    h.savedPlan = plan(null);
    h.computers = [computer('mac', 'MacBook Pro', 'machine', 'off')];
    render(<ClaudeCodePlanOutChoice tokenRows={[rowOf('saved'), rowOf('mac')]} />);
    expect(screen.queryByText(/^Usage credits are on/)).toBeNull();
  });
});
