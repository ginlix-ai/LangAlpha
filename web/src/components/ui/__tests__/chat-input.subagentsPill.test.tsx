/**
 * The Subagents toggle: an icon with no label. Pressed and drawn
 * connected while subagents are on, it flips through a thread host, or holds
 * the pick itself without one. It folds into the ⋯ menu like any toggle, with
 * a check while on and no ⋯ dot, since on is the default and a dot lit for it
 * would never mean anything; folded, it links to the Subagents pane under it.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createRef } from 'react';
import { fireEvent, render, renderHook, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle, type ChatInputProps } from '../chat-input';
import { useToolbarItems } from '../chat-input.toolbar';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '../dropdown-menu';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

const prefs = vi.hoisted(() => ({ isLoaded: true }));

// Read and holding no row, so the default is on, unless a test unreads it.
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: null, isLoading: !prefs.isLoaded, isLoaded: prefs.isLoaded }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(),
}));

function renderInput(props: Partial<ChatInputProps> & { ref?: React.Ref<ChatInputHandle> }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput onSend={vi.fn()} {...props} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const pill = () => screen.queryByRole('button', { name: /^Subagents$/ });

describe('ChatInput: the Subagents pill', () => {
  beforeEach(() => {
    prefs.isLoaded = true;
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('shows pressed while subagents are on, and flips through the host', () => {
    const onToggle = vi.fn();
    const { rerender } = renderInput({ subagentsAllowed: true, onToggleSubagents: onToggle });
    expect(pill()).toHaveAttribute('aria-pressed', 'true');
    expect(pill()).not.toHaveTextContent('Subagents');
    expect(pill()!.querySelector('.subagents-glyph')).toHaveAttribute('data-on', 'true');

    fireEvent.click(pill()!);
    expect(onToggle).toHaveBeenCalledWith(false);

    rerender(
      <QueryClientProvider client={new QueryClient()}>
        <MemoryRouter>
          <ChatInput onSend={vi.fn()} subagentsAllowed={false} onToggleSubagents={onToggle} />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(pill()).toHaveAttribute('aria-pressed', 'false');
    expect(pill()!.querySelector('.subagents-glyph')).toHaveAttribute('data-on', 'false');
    fireEvent.click(pill()!);
    expect(onToggle).toHaveBeenLastCalledWith(true);
  });

  it("is hidden while its thread host has not read the thread's value", () => {
    renderInput({ subagentsAllowed: null, onToggleSubagents: vi.fn() });
    expect(pill()).toBeNull();
  });

  it('stays interactive while a turn streams', () => {
    const onToggle = vi.fn();
    renderInput({ isLoading: true, subagentsAllowed: true, onToggleSubagents: onToggle });
    fireEvent.click(pill()!);
    expect(onToggle).toHaveBeenCalledWith(false);
  });

  it('holds its own pick without a host, and a send carries only that pick', () => {
    const onSend = vi.fn();
    renderInput({ onSend });
    const sendText = (text: string) => {
      fireEvent.change(screen.getByRole('textbox'), { target: { value: text } });
      fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
    };
    expect(pill()).toHaveAttribute('aria-pressed', 'true');
    sendText('left alone');
    expect(onSend.mock.calls[0][3].subagentsAllowed).toBeUndefined();

    fireEvent.click(pill()!);
    expect(pill()).toHaveAttribute('aria-pressed', 'false');
    sendText('flipped');
    expect(onSend.mock.calls[1][3]).toMatchObject({ subagentsAllowed: false });
  });

  it("hands its own pick to a start that reads the composer's options, and a host's none", () => {
    // Welcome's Start with nothing written reads the options rather than sending.
    const own = createRef<ChatInputHandle>();
    const { unmount } = renderInput({ ref: own });
    expect(own.current!.getModelOptions().subagentsAllowed).toBeUndefined();
    fireEvent.click(pill()!);
    expect(own.current!.getModelOptions()).toMatchObject({ subagentsAllowed: false });
    unmount();

    const hosted = createRef<ChatInputHandle>();
    renderInput({ ref: hosted, subagentsAllowed: false, onToggleSubagents: vi.fn() });
    expect(hosted.current!.getModelOptions().subagentsAllowed).toBeUndefined();
  });

  it('names what it shows while the default is unread, which a stored default may contradict', () => {
    prefs.isLoaded = false;
    const onSend = vi.fn();
    renderInput({ onSend });
    expect(pill()).toHaveAttribute('aria-pressed', 'true');
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'unread' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
    expect(onSend.mock.calls[0][3]).toMatchObject({ subagentsAllowed: true });
  });
});

describe('the Subagents toolbar item', () => {
  const base = {
    watchMode: false,
    setWatchMode: vi.fn(),
    marketWatchEnabled: true,
  };

  it('sits between the scope pill and Watch, and raises no ⋯ dot', () => {
    const { result } = renderHook(() => useToolbarItems({
      ...base, onScopeChange: vi.fn(), subagentsAllowed: true, onToggleSubagents: vi.fn(),
    }));
    expect(result.current.map((i) => i.id)).toEqual(['scope', 'subagents', 'watch']);
    const item = result.current.find((i) => i.id === 'subagents')!;
    expect(item).toMatchObject({ group: 'toggle', visible: true });
    expect(item.active).toBeFalsy();
  });

  it('folds into the menu with a check while on, and flips from there', () => {
    const onToggle = vi.fn();
    const { result, rerender } = renderHook(
      ({ allowed }: { allowed: boolean }) => useToolbarItems({ ...base, subagentsAllowed: allowed, onToggleSubagents: onToggle }),
      { initialProps: { allowed: true } },
    );
    const menuOf = () => render(
      <MemoryRouter>
        <DropdownMenu open>
          <DropdownMenuTrigger>more</DropdownMenuTrigger>
          <DropdownMenuContent>{result.current.find((i) => i.id === 'subagents')!.menu()}</DropdownMenuContent>
        </DropdownMenu>
      </MemoryRouter>,
    );

    const on = menuOf();
    const onItem = screen.getByRole('menuitem', { name: /Subagents/ });
    expect(onItem.querySelector('.lucide-check')).not.toBeNull();
    fireEvent.click(onItem);
    expect(onToggle).toHaveBeenCalledWith(false);
    on.unmount();

    rerender({ allowed: false });
    menuOf();
    const offItem = screen.getByRole('menuitem', { name: /Subagents/ });
    expect(offItem.querySelector('.lucide-check')).toBeNull();
    fireEvent.click(offItem);
    expect(onToggle).toHaveBeenLastCalledWith(true);
  });

  it('links to the Subagents pane under the switch', () => {
    const { result } = renderHook(() =>
      useToolbarItems({ ...base, subagentsAllowed: true, onToggleSubagents: vi.fn() }),
    );
    render(
      <MemoryRouter>
        <DropdownMenu open>
          <DropdownMenuTrigger>more</DropdownMenuTrigger>
          <DropdownMenuContent>{result.current.find((i) => i.id === 'subagents')!.menu()}</DropdownMenuContent>
        </DropdownMenu>
      </MemoryRouter>,
    );
    expect(screen.getByRole('menuitem', { name: 'Manage subagents' })).toHaveAttribute(
      'href',
      '/plugins?tab=subagents',
    );
  });
});
