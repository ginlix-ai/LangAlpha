/**
 * A computer's details show Claude Code on it as Settings does: its status,
 * which token it uses, and the actions, which the Computers dialog opens.
 */
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeComputer } from '@/api/model';
import type { MachineActions } from '../computerParts';

const mocks = vi.hoisted(() => ({ entry: null as ClaudeCodeComputer | null }));
vi.mock('@/hooks/useClaudeCode', () => ({ useClaudeCodeComputers: () => ({ computers: mocks.entry ? [mocks.entry] : [] }) }));

import { ComputerClaudeCode } from '../ComputerClaudeCode';

const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'signed_in', source: 'machine', email: null, expires_at: null,
};

function render() {
  const actions = { claudeCodeToken: vi.fn(), removeClaudeCodeToken: vi.fn() } as unknown as MachineActions;
  const view = renderWithProviders(<ComputerClaudeCode computerId="cmp-mac" actions={actions} />);
  return { ...view, actions };
}

describe('ComputerClaudeCode', () => {
  it('says which token the computer uses, and opens its sheet for what the button does', () => {
    mocks.entry = MAC;
    const { actions } = render();
    expect(screen.getByRole('heading', { name: 'Claude Code' })).toBeInTheDocument();
    expect(screen.getByText('Connected')).toBeInTheDocument();
    expect(screen.getByText('Uses its own token')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Replace' }));
    expect(actions.claudeCodeToken).toHaveBeenCalledWith('replace');
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }));
    expect(actions.removeClaudeCodeToken).toHaveBeenCalledTimes(1);
  });

  it('offers a computer with no token one, to add', () => {
    mocks.entry = { ...MAC, state: 'no_token', source: null };
    const { actions } = render();
    fireEvent.click(screen.getByRole('button', { name: 'Add setup token' }));
    expect(actions.claudeCodeToken).toHaveBeenCalledWith('add');
  });

  it("says Claude Code can't run where an organization's policy is installed, offering only to remove a token left there", () => {
    mocks.entry = { ...MAC, state: 'policy_blocked' };
    const { rerender } = render();
    expect(screen.getByText('Unavailable')).toBeInTheDocument();
    expect(screen.getByText("Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here.")).toBeInTheDocument();
    expect(screen.getAllByRole('button').map((b) => b.textContent)).toEqual(['Remove']);
    mocks.entry = { ...MAC, state: 'policy_blocked', source: null };
    rerender(<ComputerClaudeCode computerId="cmp-mac" actions={{} as MachineActions} />);
    expect(screen.queryAllByRole('button')).toEqual([]);
  });

  it('with Claude Code off, keeps a token left on the computer to remove', () => {
    mocks.entry = { ...MAC, state: 'off' };
    render();
    expect(screen.getByText('Claude Code is off, but this computer still has its setup token.')).toBeInTheDocument();
    expect(screen.getAllByRole('button').map((b) => b.textContent)).toEqual(['Remove']);
  });

  it('shows nothing with Claude Code off and no token left, or for a computer Claude Code does not list', () => {
    mocks.entry = { ...MAC, state: 'off', source: null };
    const { container, rerender } = render();
    expect(container).toBeEmptyDOMElement();
    mocks.entry = null;
    rerender(<ComputerClaudeCode computerId="cmp-mac" actions={{} as MachineActions} />);
    expect(container).toBeEmptyDOMElement();
  });
});
