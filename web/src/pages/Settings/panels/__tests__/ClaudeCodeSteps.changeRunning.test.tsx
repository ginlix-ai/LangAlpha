/**
 * Step 2 held while another change runs on Home's computer: it says that
 * change, rather than offering a resize the server would refuse.
 */
import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeSteps as Steps } from '@/hooks/useClaudeCodeSteps';

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ email: null }),
  useClaudeCodeSetup: () => ({ setup: null }),
}));

import { ClaudeCodeSteps } from '../modelAccess/ClaudeCodeSteps';

const steps: Steps = {
  status: 'pending',
  next: 'medium',
  home: null,
  plan: null,
  size: 'performance',
  largest: 'max',
  medium: {
    state: 'locked',
    change: { target_tier: 'standard', from_tier: 'performance', state: 'in_progress', started_at: '2026-11-19T00:00:00Z' },
  },
  token: { state: 'locked' },
};

describe('ClaudeCodeSteps, a change running the other way', () => {
  it('holds step 2 and names the change running', () => {
    renderWithProviders(<ClaudeCodeSteps steps={steps} onOwnComputer={() => {}} />);
    expect(screen.getByText('Changing spec to Small…')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Back up and change/ })).toBeNull();
  });
});
