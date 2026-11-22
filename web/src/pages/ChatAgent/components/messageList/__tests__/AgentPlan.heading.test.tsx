/**
 * The plan's heading names a step only while the turn is on one; a plan that
 * waits or ended counts what got done, beside rows that say where each
 * unfinished step stands.
 */
import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { PlanStep } from '@/types/chat';
import { AgentPlan } from '../AgentPlan';

const STEPS: PlanStep[] = [
  { title: 'Pull filings', status: 'completed' },
  { title: 'Chart margins', status: 'in_progress' },
  { title: 'Write the memo', status: 'pending' },
];

describe('AgentPlan heading', () => {
  it('names the step in progress while the turn runs', () => {
    render(<AgentPlan steps={STEPS} state="live" heading />);
    expect(screen.getByText('Plan · step 2 of 3')).toBeInTheDocument();
  });

  it('counts what got done once the turn waits, the running step saying it waits', () => {
    render(<AgentPlan steps={STEPS} state="waiting" heading />);
    expect(screen.getByText('Plan · 1 of 3 done')).toBeInTheDocument();
    expect(screen.getByText('waiting')).toBeInTheDocument();
  });

  it('counts what got done once the plan ended, every step left saying it was not done', () => {
    render(<AgentPlan steps={STEPS} state="ended" heading />);
    expect(screen.getByText('Plan · 1 of 3 done')).toBeInTheDocument();
    expect(screen.getAllByText('not done')).toHaveLength(2);
  });

  it('leaves the heading to a caller that heads the plan itself', () => {
    render(<AgentPlan steps={STEPS} state="live" />);
    expect(screen.queryByText(/^Plan ·/)).not.toBeInTheDocument();
  });
});
