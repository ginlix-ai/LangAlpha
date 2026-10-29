import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

// Render the Radix dropdown inline — the real one needs portal/pointer
// machinery jsdom doesn't drive (same treatment as McpServers.test).
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
  DropdownMenuLabel: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSeparator: () => <hr />,
  DropdownMenuSub: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSubTrigger: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  DropdownMenuSubContent: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));

import { BulkScopeMenu, type BulkScopeSpec } from '../components/BulkScopeMenu';

function makeSpec(overrides: Partial<BulkScopeSpec> = {}): BulkScopeSpec {
  return {
    workspaces: [
      { id: 'ws1', name: 'Research' },
      { id: 'ws2', name: 'Trading' },
    ],
    everywhereCount: 2,
    onEverywhere: vi.fn(),
    onlyInCount: 3,
    onOnlyIn: vi.fn(),
    move: { count: 1, onMoveTo: vi.fn() },
    ...overrides,
  };
}

describe('BulkScopeMenu', () => {
  it('shows the eligible count on each entry and runs the everywhere action', () => {
    const spec = makeSpec();
    renderWithProviders(<BulkScopeMenu {...spec} />);
    expect(screen.getByText('Use in some workspaces (3)')).toBeInTheDocument();
    expect(screen.getByText('Move into one workspace (1)')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('menuitem', { name: /all workspaces \(2\)/i }));
    expect(spec.onEverywhere).toHaveBeenCalledTimes(1);
  });

  it('disables the whole trigger when nothing is eligible', () => {
    renderWithProviders(
      <BulkScopeMenu
        {...makeSpec({ everywhereCount: 0, onlyInCount: 0, move: { count: 0, onMoveTo: vi.fn() } })}
      />,
    );
    expect(screen.getByRole('button', { name: 'Choose workspaces for the selected items' })).toBeDisabled();
  });

  it('stages the workspace checklist and applies the chosen ids', () => {
    const spec = makeSpec();
    renderWithProviders(<BulkScopeMenu {...spec} />);

    // Apply refuses an empty pick: zero workspaces = "active nowhere".
    const apply = screen.getByRole('menuitem', { name: /apply to 3/i });
    expect(apply).toHaveAttribute('aria-disabled', 'true');
    fireEvent.click(apply);
    expect(spec.onOnlyIn).not.toHaveBeenCalled();

    fireEvent.click(screen.getAllByRole('menuitem', { name: 'Research' })[0]);
    fireEvent.click(screen.getByRole('menuitem', { name: /apply to 3/i }));
    expect(spec.onOnlyIn).toHaveBeenCalledWith(['ws1']);
  });

  it('unstages a workspace on the second click', () => {
    const spec = makeSpec();
    renderWithProviders(<BulkScopeMenu {...spec} />);
    const research = screen.getAllByRole('menuitem', { name: 'Research' })[0];
    fireEvent.click(research);
    fireEvent.click(research);
    expect(
      screen.getByRole('menuitem', { name: /apply to 3/i }),
    ).toHaveAttribute('aria-disabled', 'true');
  });

  it('moves into the picked workspace', () => {
    const spec = makeSpec();
    renderWithProviders(<BulkScopeMenu {...spec} />);
    // The move submenu renders the second batch of workspace items.
    fireEvent.click(screen.getAllByRole('menuitem', { name: 'Trading' })[1]);
    expect(spec.move?.onMoveTo).toHaveBeenCalledWith('ws2');
  });

  it('has no move entry for rows that only live on the account', () => {
    renderWithProviders(<BulkScopeMenu {...makeSpec({ move: undefined })} />);
    expect(screen.queryByText(/Move into one workspace/)).not.toBeInTheDocument();
  });
});
