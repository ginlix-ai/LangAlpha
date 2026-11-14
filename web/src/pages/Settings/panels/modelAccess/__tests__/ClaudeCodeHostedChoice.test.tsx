/**
 * Under the choice of where Claude Code runs, each cloud computer too small
 * for it is named, with what gets it to Medium, until the user chooses to run
 * it on cloud computers, where the view's own list names them instead.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeCustody } from '@/api/model';
import type { ComputerSpecChange } from '@/types/api';

const h = vi.hoisted(() => ({
  custody: null as unknown,
  action: null as unknown,
  go: vi.fn(),
}));

vi.mock('@/hooks/useClaudeCode', () => ({ useClaudeCodeStatus: () => ({ enabled: true, saved: false }) }));
vi.mock('@/hooks/useClaudeCodeCustody', () => ({
  useClaudeCodeCustody: () => ({ custody: h.custody, choose: vi.fn(), choosing: null }),
}));
vi.mock('@/pages/ChatAgent/hooks/useMediumAction', () => ({
  useMediumAction: (id: string) => (id === 'c-1' ? h.action : null),
}));
vi.mock('../../usePreferenceWrite', () => ({ useSaveFailedToast: () => vi.fn() }));

import { ClaudeCodeHostedChoice } from '../ClaudeCodeHostedChoice';

const custodyOf = (over: Partial<ClaudeCodeCustody>): ClaudeCodeCustody => ({
  mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: null, ...over,
});
const TOO_SMALL = [{ computer_id: 'c-1', name: 'Cloud computer' }];
const IS_SMALL = 'Cloud computer is Small, and Claude Code runs on Medium or larger cloud computers.';
const OWN_INSTEAD = 'Or run Claude Code on your own computers: your Mac with Remote control, or a computer over SSH.';

function show(tooSmall = TOO_SMALL) {
  return renderWithProviders(<ClaudeCodeHostedChoice tooSmall={tooSmall} onAddToken={vi.fn()} onAsking={vi.fn()} />);
}

describe('ClaudeCodeHostedChoice, cloud computers too small for Claude Code', () => {
  beforeEach(() => {
    h.go.mockReset();
    h.custody = custodyOf({});
    h.action = null;
  });

  it('names the computer under the choice, with what changing it does, a button for it, and the way to use their own', () => {
    h.action = { kind: 'resize', go: h.go };
    show();
    expect(screen.getByText(`${IS_SMALL} Changing it to Medium restarts it, and its workspaces are back in a few minutes.`)).toBeInTheDocument();
    expect(screen.getByText(OWN_INSTEAD)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Change to Medium' }));
    expect(h.go).toHaveBeenCalledTimes(1);
  });

  it("says Medium isn't on the plan where the way to Medium is the plan page", () => {
    h.action = { kind: 'plan', go: h.go };
    show();
    expect(screen.getByText(`${IS_SMALL} Medium isn't on your plan.`)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Upgrade plan' }));
    expect(h.go).toHaveBeenCalledTimes(1);
  });

  it('says the change is running while it is, and offers it as the button', () => {
    const change = { target_tier: 'performance', from_tier: 'standard', state: 'in_progress', started_at: '2026-11-13T18:00:00Z' } as ComputerSpecChange;
    h.action = { kind: 'resizing', change, go: h.go };
    show();
    expect(screen.getByText(`${IS_SMALL} It's changing to Medium now.`)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^Upgrading to Medium/ })).toBeInTheDocument();
  });

  it('states the requirement alone, with no button, where nothing gets the computer to Medium', () => {
    show();
    expect(screen.getByText(IS_SMALL)).toBeInTheDocument();
    expect(screen.getByText(OWN_INSTEAD)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Medium|Upgrade/ })).toBeNull();
  });

  it.each([
    ['they run it on cloud computers, which the view lists itself', { mode: 'stored' as const }, TOO_SMALL],
    ['the server does not offer running it on cloud computers', { offered: [] }, TOO_SMALL],
    ['no computer of theirs is too small', {}, []],
  ])('says nothing where %s', (_, over, tooSmall) => {
    h.action = { kind: 'resize', go: h.go };
    h.custody = custodyOf(over);
    show(tooSmall);
    expect(screen.queryByText(/is Small, and Claude Code runs on/)).toBeNull();
    expect(screen.queryByText(OWN_INSTEAD)).toBeNull();
    expect(screen.queryByRole('button', { name: 'Change to Medium' })).toBeNull();
  });
});
