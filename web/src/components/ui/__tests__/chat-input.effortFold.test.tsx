/**
 * The effort pill folded into ⋯: a submenu on a wide screen, the levels
 * listed in ⋯ itself on a phone, where a submenu has no room to open beside
 * it. Either way a pick leaves the menu open, and its click never reaches the
 * composer, which would answer it by focusing the text box and so close the
 * menu.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, renderHook, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '../dropdown-menu';
import { useEffortToolbarItem } from '../chat-input.toolbar';
import type { EffortMenuProps } from '../chat-input.modelMenu';

const onReasoningEffortChange = vi.fn();
const onFastModeChange = vi.fn();
const onComposerClick = vi.fn();
const onOpenChange = vi.fn();

const props: EffortMenuProps = {
  effortLabel: 'High',
  reasoningEffort: 'high',
  defaultEffort: 'medium',
  reasoningEfforts: ['low', 'medium', 'high'],
  onReasoningEffortChange,
  fastMode: false,
  onFastModeChange,
  isCodexModel: false,
  dropdownDirection: 'up',
  container: null,
  taskCost: {
    low: { value: 0.5, measured: true },
    medium: { value: 1, measured: true },
    high: { value: 1.5, measured: true },
  },
};

/** ⋯ open on the folded effort item, inside a stand-in for the composer. */
function renderFolded(phone: boolean) {
  const { result } = renderHook(() => useEffortToolbarItem(props, phone));
  render(
    <MemoryRouter>
      <div onClick={onComposerClick}>
        <DropdownMenu open onOpenChange={onOpenChange} modal={false}>
          <DropdownMenuTrigger>more</DropdownMenuTrigger>
          <DropdownMenuContent>{result.current.menu()}</DropdownMenuContent>
        </DropdownMenu>
      </div>
    </MemoryRouter>,
  );
}

function level(label: string) {
  const found = screen.getAllByRole('menuitemradio').find((el) => within(el).queryByText(label));
  if (!found) throw new Error(`no level labelled ${label}`);
  return found;
}

beforeEach(() => {
  onReasoningEffortChange.mockClear();
  onComposerClick.mockClear();
  onOpenChange.mockClear();
});

describe('the folded effort item on a phone', () => {
  it('lists the levels in ⋯ itself, under a heading, with no submenu', () => {
    renderFolded(true);
    expect(screen.queryByRole('menuitem', { name: /Effort/ })).toBeNull();
    const section = screen.getByRole('group', { name: 'Effort' });
    expect(within(section).getByText('Cost per task')).toBeInTheDocument();
    expect(level('High')).toHaveAttribute('aria-checked', 'true');
    expect(within(level('Medium')).getByText('Default')).toBeInTheDocument();
    expect(within(level('Low')).getByText('0.5×')).toBeInTheDocument();
    // The note the tooltip shows a pointer is the section's description.
    expect(section).toHaveAccessibleDescription(/how a typical task's cost changes/);
  });

  it('picks a level and keeps the menu open', () => {
    renderFolded(true);
    fireEvent.click(level('Low'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith('low');
    expect(onComposerClick).not.toHaveBeenCalled();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
  });
});

describe('the effort pill in the measure row', () => {
  it('takes the room of its longest level, whichever one is picked', () => {
    const { result } = renderHook(() => useEffortToolbarItem(props, false));
    render(<>{result.current.inline({ measureOnly: true })}</>);
    const twin = screen.getByRole('button');
    for (const label of ['Low', 'Medium', 'High']) expect(within(twin).getByText(label)).toBeInTheDocument();
  });

  it('holds the level shown on a Codex model with none to pick', () => {
    const speedOnly = { ...props, reasoningEfforts: [], isCodexModel: true };
    const { result } = renderHook(() => useEffortToolbarItem(speedOnly, false));
    render(<>{result.current.inline({ measureOnly: true })}</>);
    expect(within(screen.getByRole('button')).getByText('High')).toBeInTheDocument();
  });
});

describe('the folded effort item on a wide screen', () => {
  it('keeps the levels in a submenu', () => {
    renderFolded(false);
    expect(screen.getByRole('menuitem', { name: /Effort/ })).toHaveAttribute('aria-haspopup', 'menu');
    expect(screen.queryAllByRole('menuitemradio')).toHaveLength(0);
  });

  it('picks a level in the submenu and keeps the menu open', () => {
    renderFolded(false);
    fireEvent.click(screen.getByRole('menuitem', { name: /Effort/ }));
    onComposerClick.mockClear();
    fireEvent.click(level('Low'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith('low');
    expect(onComposerClick).not.toHaveBeenCalled();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
  });
});
