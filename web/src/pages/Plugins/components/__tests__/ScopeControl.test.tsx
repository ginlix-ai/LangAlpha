import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

// Inline dropdown, as in BulkScopeMenu.test: jsdom does not drive Radix portals.
vi.mock('@/components/ui/dropdown-menu', () => ({
  DropdownMenu: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  DropdownMenuContent: ({ children }: { children: React.ReactNode }) => (
    <div role="menu">{children}</div>
  ),
  DropdownMenuItem: ({
    children,
    onSelect,
    disabled,
  }: {
    children: React.ReactNode;
    onSelect?: (e?: { preventDefault: () => void }) => void;
    disabled?: boolean;
  }) => (
    <button
      role="menuitem"
      aria-disabled={disabled ? 'true' : undefined}
      onClick={() => {
        if (!disabled) onSelect?.({ preventDefault: () => {} });
      }}
    >
      {children}
    </button>
  ),
  DropdownMenuCheckboxItem: ({
    children,
    checked,
    onSelect,
    disabled,
  }: {
    children: React.ReactNode;
    checked?: boolean;
    onSelect?: (e?: { preventDefault: () => void }) => void;
    disabled?: boolean;
  }) => (
    <button
      role="menuitemcheckbox"
      aria-checked={checked}
      aria-disabled={disabled ? 'true' : undefined}
      onClick={() => {
        if (!disabled) onSelect?.({ preventDefault: () => {} });
      }}
    >
      {children}
    </button>
  ),
  DropdownMenuLabel: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSeparator: () => <hr />,
  DropdownMenuSub: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSubTrigger: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSubContent: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));

import { ScopeControl } from '../ScopeControl';

const workspaces = [
  { id: 'ws-a', name: 'Alpha' },
  { id: 'ws-b', name: 'Beta' },
];
const homeWorkspace = { id: 'home', name: 'Home' };

type Props = React.ComponentProps<typeof ScopeControl>;

function renderControl(overrides: Partial<Props> = {}) {
  return renderWithProviders(
    <ScopeControl
      workspaces={workspaces}
      scopeWorkspaceId={null}
      onSetWorkspaceDisabled={vi.fn()}
      {...overrides}
    />,
  );
}

const toggle = () => screen.getByRole('menuitemcheckbox', { name: 'On in new workspaces' });
const checked = (item: HTMLElement) => item.getAttribute('aria-checked') === 'true';

/**
 * A checklist switch changes today's workspaces only. What a workspace created
 * later starts with is a setting of the row's own, where it has one, and the
 * menu offers it in place of the hint that promised "enabled" unconditionally.
 */
describe('ScopeControl: what a workspace created later starts with', () => {
  it('offers the setting in place of the static hint, and flips it off', () => {
    const onSet = vi.fn();
    renderControl({ newWorkspacesOn: true, onSetNewWorkspacesOn: onSet });
    expect(screen.queryByText('New workspaces start enabled')).not.toBeInTheDocument();
    expect(checked(toggle())).toBe(true);

    fireEvent.click(toggle());
    expect(onSet).toHaveBeenCalledWith(false);
  });

  it('flips it back on from a row new workspaces start without', () => {
    const onSet = vi.fn();
    renderControl({ newWorkspacesOn: false, onSetNewWorkspacesOn: onSet });
    expect(checked(toggle())).toBe(false);

    fireEvent.click(toggle());
    expect(onSet).toHaveBeenCalledWith(true);
  });

  it('keeps the static hint on rows with no setting of their own', () => {
    // Builtins and skills: new workspaces always start with them on.
    renderControl();
    expect(screen.getByText('New workspaces start enabled')).toBeInTheDocument();
    expect(screen.queryByText('On in new workspaces')).not.toBeInTheDocument();
  });

  it('stays live on a row that is off account-wide, held only by a write in flight', () => {
    // The off row's checklist gives way, since a workspace re-enable would
    // 409. This setting is stored on the row and involves no re-enable.
    const { unmount } = renderControl({ off: 'self', onSetNewWorkspacesOn: vi.fn() });
    expect(screen.queryByRole('menuitemcheckbox', { name: 'Alpha' })).not.toBeInTheDocument();
    expect(toggle()).not.toHaveAttribute('aria-disabled');
    unmount();

    renderControl({ busy: true, onSetNewWorkspacesOn: vi.fn() });
    expect(toggle()).toHaveAttribute('aria-disabled', 'true');
  });

  it('is offered on a row with no workspace to list yet', () => {
    // What the next workspace starts with is then the only scope the row has.
    renderControl({ workspaces: [], onSetNewWorkspacesOn: vi.fn() });
    expect(screen.queryByText('Use in these workspaces')).not.toBeInTheDocument();
    expect(toggle()).toBeInTheDocument();
  });
});

describe('ScopeControl: the badge on a row new workspaces start without', () => {
  const trigger = (label: string) => screen.getByRole('button', { name: `Scope: ${label}` });
  const iconOf = (el: HTMLElement) => el.querySelector('svg')?.getAttribute('class') ?? '';

  it('names the one workspace the row is on in', () => {
    renderControl({ newWorkspacesOn: false, disabledWorkspaceIds: ['ws-b'] });
    expect(iconOf(trigger('Only in Alpha'))).toContain('lucide-folder-open');
  });

  it('counts the workspaces the row is on in', () => {
    renderControl({ newWorkspacesOn: false, disabledWorkspaceIds: [] });
    expect(iconOf(trigger('2 workspaces'))).toContain('lucide-folder-open');
  });

  it('says no workspaces when every one has switched it off', () => {
    renderControl({ newWorkspacesOn: false, disabledWorkspaceIds: ['ws-a', 'ws-b'] });
    expect(trigger('No workspaces')).toBeInTheDocument();
  });

  it('counts Home when the checklist offers it', () => {
    renderControl({
      newWorkspacesOn: false,
      disabledWorkspaceIds: ['ws-a', 'ws-b'],
      homeWorkspace,
    });
    expect(trigger('Only in Home')).toBeInTheDocument();
  });

  it('keeps the deny-list wording on a row new workspaces start with', () => {
    renderControl({ newWorkspacesOn: true, disabledWorkspaceIds: ['ws-b'] });
    expect(iconOf(trigger('All workspaces except 1'))).toContain('lucide-globe');
  });
});

/**
 * A row switched off account-wide is on in no workspace, whatever its
 * deny-list says, so the pill cannot read as a reach and the checklist cannot
 * be the first thing offered: no box in it can take until the row is back on.
 */
describe('ScopeControl: a row that is off before any workspace has a say', () => {
  const trigger = (label: string) => screen.getByRole('button', { name: `Scope: ${label}` });

  it('reads off everywhere and points at the switch instead of the checklist', () => {
    renderControl({ off: 'self', disabledWorkspaceIds: ['ws-b'] });
    expect(trigger('Off everywhere')).toBeInTheDocument();
    expect(screen.getByText('Switch it on to choose workspaces.')).toBeInTheDocument();
    expect(screen.queryByRole('menuitemcheckbox', { name: 'Alpha' })).not.toBeInTheDocument();
  });

  it('points at the plugin when the plugin is what holds it off', () => {
    renderControl({ off: 'plugin' });
    expect(trigger('Plugin off')).toBeInTheDocument();
    expect(screen.getByText(/Switch the plugin on to choose workspaces/)).toBeInTheDocument();
  });

  it('leaves a workspace-tier row naming its workspace', () => {
    renderControl({ scopeWorkspaceId: 'ws-a', off: 'self', onMove: vi.fn() });
    expect(trigger('Alpha')).toBeInTheDocument();
  });
});
