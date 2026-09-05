/**
 * The effort/speed rows: what they show, and what a pick writes.
 *
 * The load-bearing rule is that picking the level marked "Default" clears the
 * override instead of pinning today's default as a permanent choice — that is
 * the only way back to inheriting once the word list replaced the old
 * click-the-active-icon-to-clear toggle.
 *
 * Dropdown primitives are mocked inline (same approach as McpServerRow) so
 * items are queryable without portal or pointer machinery.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

const h = vi.hoisted(() => ({ isMobile: false }));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => h.isMobile }));

vi.mock('../dropdown-menu', () => {
  const Pass = ({ children }: { children: React.ReactNode }) => <>{children}</>;
  const Box = ({ children }: { children: React.ReactNode }) => <div>{children}</div>;
  return {
    DropdownMenu: Box,
    DropdownMenuTrigger: Pass,
    DropdownMenuContent: Box,
    // Props pass through, so a caller's own role/aria survives — the real
    // primitive forwards them, and the effort options rely on it. data-menu-item
    // marks what came from the primitive rather than a hand-rolled div.
    // `variant` is the wrapper's own prop, dropped here the way the real one
    // resolves it to classes rather than passing it down to the div.
    DropdownMenuItem: ({ children, onSelect, variant: _variant, ...rest }: {
      children: React.ReactNode;
      onSelect?: (e: { preventDefault: () => void }) => void;
      variant?: string;
    } & Record<string, unknown>) => (
      <div role="menuitem" data-menu-item {...rest} onClick={(e) => onSelect?.(e)}>{children}</div>
    ),
    DropdownMenuSeparator: () => <hr />,
    DropdownMenuSub: Box,
    DropdownMenuSubTrigger: ({ children }: { children: React.ReactNode }) => (
      <div role="menuitem">{children}</div>
    ),
    DropdownMenuSubContent: Box,
  };
});

import { ChatInputModelMenu } from '../chat-input.modelMenu';

const onReasoningEffortChange = vi.fn();
const onFastModeChange = vi.fn();

function renderMenu(overrides: Record<string, unknown> = {}) {
  const props = {
    selectedModel: 'qwen3.6-flash',
    onSelectModel: vi.fn(),
    threadModels: [],
    validModelNames: new Set(['qwen3.6-flash']),
    moreModelsItems: [],
    hasStarredModels: true,
    reasoningEffort: 'high',
    inheritedEffort: 'medium',
    onReasoningEffortChange,
    fastMode: false,
    onFastModeChange,
    isCodexModel: false,
    reasoningEfforts: ['none', 'low', 'medium', 'high'],
    dropdownDirection: 'up' as const,
    containerRef: { current: null },
    ...overrides,
  };
  return render(
    <MemoryRouter>
      <ChatInputModelMenu {...(props as React.ComponentProps<typeof ChatInputModelMenu>)} />
    </MemoryRouter>,
  );
}

/** The option list is a sibling set of radio rows; find one by its word. The
 *  row above shows the same word as its value, so scope the search to the
 *  options rather than the whole menu. */
function option(label: string) {
  const found = screen.getAllByRole('menuitemradio').find((el) => within(el).queryByText(label));
  if (!found) throw new Error(`no option labelled ${label}`);
  return found;
}

beforeEach(() => {
  h.isMobile = false;
  onReasoningEffortChange.mockClear();
  onFastModeChange.mockClear();
});

describe('effort row', () => {
  it('shows the override as the current value, not the inherited level', () => {
    renderMenu();
    const row = screen.getByText('Effort').closest('[role="menuitem"]')!;
    expect(within(row as HTMLElement).getByText('High')).toBeInTheDocument();
    expect(option('High')).toHaveAttribute('aria-checked', 'true');
  });

  it('falls back to the inherited level when there is no override', () => {
    renderMenu({ reasoningEffort: null });
    const row = screen.getByText('Effort').closest('[role="menuitem"]')!;
    expect(within(row as HTMLElement).getByText('Medium')).toBeInTheDocument();
    expect(option('Medium')).toHaveAttribute('aria-checked', 'true');
  });

  it('badges the inherited level and nothing else', () => {
    renderMenu();
    expect(within(option('Medium') as HTMLElement).getByText('Default')).toBeInTheDocument();
    expect(screen.getAllByText('Default')).toHaveLength(1);
  });

  it('clears the override when the default level is picked', () => {
    renderMenu();
    fireEvent.click(option('Medium'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith(null);
  });

  it('writes the level when any other one is picked', () => {
    renderMenu();
    fireEvent.click(option('Low'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith('low');
  });

  it('renders words, never a bare level name', () => {
    renderMenu({ reasoningEfforts: ['none', 'xhigh', 'max'], reasoningEffort: 'xhigh' });
    expect(screen.getAllByRole('menuitemradio').map((el) => el.textContent))
      .toEqual(['Off', 'Extra High', 'Max']);
    expect(screen.queryByText('xhigh')).not.toBeInTheDocument();
  });

  it('is absent for a model with no ladder', () => {
    renderMenu({ reasoningEfforts: [] });
    expect(screen.queryByText('Effort')).not.toBeInTheDocument();
  });

  /* These were plain divs once, which reads fine with a mouse and strands
     anyone without one: a div outside the menu's item collection is skipped by
     roving focus, so arrowing into the list lands nowhere and Enter does
     nothing. Built on the menu-item primitive, they are reachable by the same
     path every sibling row uses. */
  it('builds each option on the menu-item primitive, not a bare div', () => {
    renderMenu();
    const options = screen.getAllByRole('menuitemradio');
    expect(options).toHaveLength(4);
    for (const el of options) expect(el).toHaveAttribute('data-menu-item');
  });

  it('keeps the setting row itself a menu item', () => {
    h.isMobile = true;
    renderMenu();
    expect(screen.getByText('Effort').closest('[role="menuitem"]')).toHaveAttribute('data-menu-item');
  });
});

describe('speed row', () => {
  it('is absent on a non-Codex model', () => {
    renderMenu();
    expect(screen.queryByText('Speed')).not.toBeInTheDocument();
  });

  it('picks fast and standard', () => {
    renderMenu({ isCodexModel: true });
    fireEvent.click(option('Fast'));
    expect(onFastModeChange).toHaveBeenCalledWith(true);

    fireEvent.click(option('Standard'));
    expect(onFastModeChange).toHaveBeenCalledWith(false);
  });
});

describe('mobile', () => {
  // Radix submenus lose a tap on mobile, so the row expands in place instead.
  it('hides the options until the row is tapped', () => {
    h.isMobile = true;
    renderMenu();
    expect(screen.queryByText('Low')).toBeNull();

    fireEvent.click(screen.getByText('Effort').closest('[role="menuitem"]')!);
    expect(option('Low')).toBeTruthy();

    fireEvent.click(option('Low'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith('low');
  });
});
