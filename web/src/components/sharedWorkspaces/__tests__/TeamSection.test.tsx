/**
 * The People dialog's team row: the team's name and how many people it
 * brings, a plain line when its people cannot be read, and the menu to name
 * another team or none only for someone who may manage the workspace.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import type { AccountTeam } from '@/types/api';
import type { AudienceTeam } from '@/hooks/useSharedWorkspaces';
import { TeamSection } from '../TeamSection';

const MINE: AccountTeam[] = [{ id: 't1', name: 'Semis desk', role: 'admin' }, { id: 't2', name: 'Macro pod', role: 'member' }];
const SEMIS: AudienceTeam = {
  id: 't1',
  name: 'Semis desk',
  people: ['a', 'b', 'c'].map((id) => ({ user_id: id, name: id, email: null, avatar_url: null, role: 'member', active: true, listed: false })),
  loading: false,
  failed: false,
};

function row(props: Partial<Parameters<typeof TeamSection>[0]> = {}) {
  const onChange = vi.fn();
  render(
    <TeamSection
      team={SEMIS} canManage myTeams={MINE} busy={false}
      onChange={onChange} {...props}
    />,
  );
  return onChange;
}

describe('TeamSection', () => {
  it('names the team and how many people it brings', () => {
    row();
    const team = screen.getByTestId('shared-team-row');
    expect(team.textContent).toContain('Semis desk');
    expect(team.textContent).toContain('3 people');
  });

  it("says when the team's people cannot be read, and still names it", () => {
    row({ team: { ...SEMIS, people: undefined, failed: true } });
    const team = screen.getByTestId('shared-team-row');
    expect(team.textContent).toContain('Semis desk');
    expect(team.textContent).toContain("can't be loaded");
  });

  it('offers no change to someone who may not manage the workspace', () => {
    row({ canManage: false });
    expect(screen.queryByRole('button')).toBeNull();
  });

  it("names another of the caller's teams, or none", () => {
    const onChange = row();
    const open = () => fireEvent.pointerDown(screen.getByRole('button', { name: /team/i }), { button: 0, ctrlKey: false });
    open();
    fireEvent.click(screen.getByRole('menuitem', { name: /Macro pod/ }));
    expect(onChange).toHaveBeenLastCalledWith('t2');
    open();
    fireEvent.click(screen.getByRole('menuitem', { name: /Remove team/ }));
    expect(onChange).toHaveBeenLastCalledWith(null);
  });

  it('offers a manager with no team named a choice of theirs', () => {
    row({ team: null });
    expect(screen.getByRole('button').textContent).toBe('Choose');
  });
});
