/**
 * Back from the plan page, the steps say which plan the user is on now. The
 * plan named before the trip reaches render as state, never read from storage
 * in render, which the compiler may keep at its first value.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeSteps as Steps } from '@/hooks/useClaudeCodeSteps';

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ email: null }),
  useClaudeCodeSetup: () => ({ setup: null }),
}));
vi.mock('@/lib/planTrip', () => ({ planPageUrl: () => '/plans' }));
vi.mock('@/lib/desktop', () => ({ openConsolePage: vi.fn() }));
vi.mock('@/pages/ChatAgent/hooks/useTierQuota', async (orig) => ({
  ...(await orig<object>()),
  readPlanAfterChange: vi.fn(async () => {}),
}));

import { openConsolePage } from '@/lib/desktop';
import { ClaudeCodeSteps } from '../modelAccess/ClaudeCodeSteps';

const steps = (over: Partial<Steps>): Steps => ({
  status: 'pending',
  next: 'plan',
  home: null,
  plan: { done: false, current: 'Tier A' },
  size: 'performance',
  largest: 'max',
  medium: { state: 'locked', change: null },
  token: { state: 'locked' },
  ...over,
});
const back = steps({ next: 'medium', plan: { done: true, current: 'Tier B' }, medium: { state: 'active', change: null } });
const markerReads = (spy: { mock: { calls: unknown[][] } }) =>
  spy.mock.calls.filter(([key]) => String(key).includes('planBeforeTrip')).length;

describe('ClaudeCodeSteps, back from the plan page', () => {
  beforeEach(() => sessionStorage.clear());
  afterEach(() => vi.restoreAllMocks());

  it('says the plan the user is on now, back from the plan page in another tab', () => {
    const view = renderWithProviders(<ClaudeCodeSteps steps={steps({})} onOwnComputer={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Choose a plan' }));
    expect(openConsolePage).toHaveBeenCalled();
    act(() => {
      window.dispatchEvent(new Event('blur'));
      window.dispatchEvent(new Event('focus'));
    });
    view.rerender(<ClaudeCodeSteps steps={back} onOwnComputer={() => {}} />);
    expect(screen.getByText(/^You're on Tier B now\./)).toBeInTheDocument();
  });

  it('says it on a sheet the way back mounts anew, from the marker the trip left', () => {
    const first = renderWithProviders(<ClaudeCodeSteps steps={steps({})} onOwnComputer={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Choose a plan' }));
    first.unmount();
    renderWithProviders(<ClaudeCodeSteps steps={back} onOwnComputer={() => {}} />);
    expect(screen.getByText(/^You're on Tier B now\./)).toBeInTheDocument();
  });

  it('names no plan where it did not change', () => {
    const view = renderWithProviders(<ClaudeCodeSteps steps={steps({ plan: { done: false, current: 'Tier B' } })} onOwnComputer={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Choose a plan' }));
    view.rerender(<ClaudeCodeSteps steps={back} onOwnComputer={() => {}} />);
    expect(screen.queryByText(/You're on/)).toBeNull();
  });

  it('reads the marker from storage only as the sheet mounts, never in a later render', () => {
    const view = renderWithProviders(<ClaudeCodeSteps steps={steps({})} onOwnComputer={() => {}} />);
    const getItem = vi.spyOn(Storage.prototype, 'getItem');
    fireEvent.click(screen.getByRole('button', { name: 'Choose a plan' }));
    view.rerender(<ClaudeCodeSteps steps={back} onOwnComputer={() => {}} />);
    expect(markerReads(getItem)).toBe(0);
  });
});
