/**
 * Arranging the gallery by dragging its cards: when they drag, what a drop
 * saves, and that the card's menu and the keyboard each do what they should.
 *
 * The list is read for real (the API is mocked, not the query), because a drop
 * rewrites the cached list the grid reads. dnd-kit runs for real too; the one
 * thing taken from it is the drop handler, since jsdom lays nothing out for a
 * pointer to land on.
 */
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { DndContextProps, DragEndEvent } from '@dnd-kit/core';
import type { Mock } from 'vitest';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { renderWithProviders } from '@/test/utils';
import type { Computer, Workspace } from '@/types/api';

const dnd = vi.hoisted(() => ({ props: null as DndContextProps | null }));

vi.mock('@dnd-kit/core', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@dnd-kit/core')>();
  const React = await import('react');
  return {
    ...actual,
    DndContext: (props: DndContextProps) => {
      dnd.props = props;
      return React.createElement(actual.DndContext, props);
    },
  };
});

const COMPUTER_ID = 'computer-1';
const MAC_ID = 'mac-1';

const row = (id: string, name: string, extra: Partial<Workspace> = {}): Workspace => ({
  workspace_id: id,
  name,
  status: 'running',
  is_home: false,
  computer_id: COMPUTER_ID,
  updated_at: '2026-09-16T02:23:43.210835Z',
  ...extra,
});

const HOME = row('home', 'Home', { is_home: true });
const PINNED = row('pinned', 'Pinned', { is_pinned: true } as Partial<Workspace>);
const ALPHA = row('alpha', 'Alpha');
const BRAVO = row('bravo', 'Bravo');
const CHARLIE = row('charlie', 'Charlie', { computer_id: MAC_ID });
const CUSTOM = [HOME, PINNED, ALPHA, BRAVO, CHARLIE];

vi.mock('../../utils/api', () => ({
  getWorkspaces: vi.fn(),
  getHomeWorkspace: vi.fn(),
  reorderWorkspaces: vi.fn(),
  renameWorkspace: vi.fn(),
  createWorkspace: vi.fn(),
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(async () => ({ providers: [] })),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  formatApiErrorDetail: (err: unknown) => (err as { message?: string })?.message ?? 'Request failed',
  uploadWorkspaceFile: vi.fn(),
}));

vi.mock('../workspaceActions', () => ({
  WorkspaceMenuItems: () => null,
  useWorkspaceActions: () => ({ openDuplicate: vi.fn(), openMove: vi.fn(), openDelete: vi.fn(), dialogs: null }),
}));
vi.mock('../../hooks/useRemoteControl', () => ({ useThisMac: () => ({ status: null, of: () => null }) }));
vi.mock('../../hooks/workspaceRowActions', async (importOriginal) => ({
  ...await importOriginal<typeof import('../../hooks/workspaceRowActions')>(),
  pinWorkspaceRow: vi.fn(),
}));
vi.mock('../../hooks/utils/chatSessionRestore', () => ({ clearChatSession: vi.fn() }));
vi.mock('@/components/ui/use-toast', async (importOriginal) => ({
  ...await importOriginal<typeof import('@/components/ui/use-toast')>(),
  toast: vi.fn(),
}));

import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { getComputers, getHomeWorkspace, getWorkspaces, reorderWorkspaces } from '../../utils/api';
import WorkspaceGallery from '../WorkspaceGallery';

const mockReorder = reorderWorkspaces as Mock;

const machine = (computer_id: string, name: string): Computer => ({
  computer_id,
  user_id: 'user-1',
  kind: 'daytona',
  name,
  status: 'running',
  resource_tier: 'standard',
  is_always_on: false,
  is_primary: computer_id === COMPUTER_ID,
  root_dir: '/home/workspace',
  ssh: null,
});

/** The server: the custom order is the stored one; Recent and Name are any other. */
function serve(custom: Workspace[] = CUSTOM) {
  (getWorkspaces as Mock).mockImplementation(async (_limit: number, _offset: number, sortBy: string) => {
    const rows = sortBy === 'custom' ? custom : [...custom].reverse();
    return { workspaces: rows, total: rows.length };
  });
}

/** Each card's lift-and-open element, by the name it is labelled with. */
const cardButton = (name: string) =>
  within(screen.getAllByTestId('workspace-card').find((c) => within(c).queryByText(name))!).getByRole('button', { name });
const cardNames = () => screen.getAllByTestId('workspace-card').map((c) => c.querySelector('[role="button"]')?.getAttribute('aria-label'));
const draggable = () => screen.getByTestId('workspace-card-grid').hasAttribute('data-draggable');

function drop(activeId: string, overId: string) {
  act(() => {
    dnd.props?.onDragEnd?.({ active: { id: activeId }, over: { id: overId } } as unknown as DragEndEvent);
  });
}

/**
 * A drop as dnd-kit plays it: the drop handler, then the announcement it asks
 * for. Returns what a screen reader is told.
 */
function dropAnnounced(activeId: string, overId: string): string | undefined {
  const event = { active: { id: activeId }, over: { id: overId } } as unknown as DragEndEvent;
  let said: string | undefined;
  act(() => {
    dnd.props?.onDragEnd?.(event);
    said = dnd.props?.accessibility?.announcements?.onDragEnd?.(event);
  });
  return said;
}

/** The next read of the Custom list, held open until `land` serves `rows`. */
function holdNextRead() {
  let land!: (rows: Workspace[]) => void;
  (getWorkspaces as Mock).mockImplementationOnce(() => new Promise((resolve) => {
    land = (rows) => resolve({ workspaces: rows, total: rows.length });
  }));
  return { land: (rows: Workspace[]) => act(async () => { land(rows); }) };
}

async function renderGallery(sort = 'custom') {
  localStorage.setItem('workspaceGallery.sort', sort);
  const onSelect = vi.fn();
  const view = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={onSelect} />);
  await waitFor(() => expect(screen.getAllByTestId('workspace-card')).toHaveLength(4));
  return { ...view, onSelect };
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  dnd.props = null;
  serve();
  (getHomeWorkspace as Mock).mockResolvedValue(HOME);
  (getComputers as Mock).mockResolvedValue({ computers: [machine(COMPUTER_ID, 'Alpha Research'), machine(MAC_ID, 'Mac')], total: 2 });
  mockReorder.mockResolvedValue(undefined);
});

describe('WorkspaceGallery, when cards drag', () => {
  it('drags them in Custom only, and not while a search or a filter leaves cards out', async () => {
    const user = userEvent.setup();
    await renderGallery();
    expect(draggable()).toBe(true);
    expect(cardButton('Alpha')).toHaveAttribute('aria-roledescription', 'sortable');

    await user.type(screen.getByRole('searchbox'), 'a');
    expect(draggable()).toBe(false);
    expect(cardButton('Alpha')).not.toHaveAttribute('aria-roledescription');
    await user.clear(screen.getByRole('searchbox'));
    expect(draggable()).toBe(true);

    await user.click(await screen.findByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: /Alpha Research/ }));
    expect(draggable()).toBe(false);
    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: 'All computers' }));
    expect(draggable()).toBe(true);

    await user.click(screen.getByTestId('gallery-sort'));
    await user.click(screen.getByRole('menuitemradio', { name: 'Recent' }));
    await waitFor(() => expect(cardNames()).toEqual(['Pinned', 'Charlie', 'Bravo', 'Alpha']));
    expect(draggable()).toBe(false);
  });

  it.each(['activity', 'name'])('leaves the cards still in the %s order', async (sort) => {
    await renderGallery(sort);
    expect(draggable()).toBe(false);
    drop('charlie', 'alpha');
    expect(mockReorder).not.toHaveBeenCalled();
  });
});

describe('WorkspaceGallery, a drop', () => {
  it('saves the new order with Home first and the pinned block ahead', async () => {
    await renderGallery();
    expect(cardNames()).toEqual(['Pinned', 'Alpha', 'Bravo', 'Charlie']);

    drop('charlie', 'alpha');

    // Where it was dropped at once, not after the round trip.
    expect(cardNames()).toEqual(['Pinned', 'Charlie', 'Alpha', 'Bravo']);
    await waitFor(() => expect(mockReorder).toHaveBeenCalledWith([
      { workspace_id: 'home', sort_order: 0 },
      { workspace_id: 'pinned', sort_order: 1 },
      { workspace_id: 'charlie', sort_order: 2 },
      { workspace_id: 'alpha', sort_order: 3 },
      { workspace_id: 'bravo', sort_order: 4 },
    ]));
  });

  it('is refused across the line between pinned and the rest', async () => {
    await renderGallery();
    drop('alpha', 'pinned');
    drop('pinned', 'bravo');
    expect(mockReorder).not.toHaveBeenCalled();
    expect(cardNames()).toEqual(['Pinned', 'Alpha', 'Bravo', 'Charlie']);
  });

  it('puts the cards back when the save fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    let fail!: (err: Error) => void;
    mockReorder.mockReturnValue(new Promise<void>((_, reject) => { fail = reject; }));
    await renderGallery();

    drop('charlie', 'alpha');
    expect(cardNames()).toEqual(['Pinned', 'Charlie', 'Alpha', 'Bravo']);

    await act(async () => { fail(new Error('offline')); });
    await waitFor(() => expect(cardNames()).toEqual(['Pinned', 'Alpha', 'Bravo', 'Charlie']));
    // And says so, rather than leaving the user to notice the cards went back.
    expect(toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive', title: "Couldn't save the new order" }));
  });

  it('is not announced as dropped when the save in flight turns it away', async () => {
    mockReorder.mockReturnValueOnce(new Promise<void>(() => {}));
    await renderGallery();

    expect(dropAnnounced('charlie', 'alpha')).toBe('Charlie was dropped over Alpha.');
    // Before the render that would disable the cards, too.
    expect(dropAnnounced('bravo', 'charlie')).toBe('Bravo was not moved: the last move is still being saved.');
    expect(mockReorder).toHaveBeenCalledTimes(1);
  });

  it('holds the cards still until the save lands, and ignores a drop meanwhile', async () => {
    let finish!: () => void;
    mockReorder.mockReturnValueOnce(new Promise<void>((resolve) => { finish = resolve; }));
    await renderGallery();

    drop('charlie', 'alpha');
    await waitFor(() => expect(cardButton('Alpha')).not.toHaveAttribute('aria-roledescription'));
    drop('bravo', 'charlie');
    expect(mockReorder).toHaveBeenCalledTimes(1);

    await act(async () => { finish(); });
    await waitFor(() => expect(cardButton('Alpha')).toHaveAttribute('aria-roledescription', 'sortable'));
  });
});

describe('WorkspaceGallery, while the list is read again', () => {
  // A reorder made in the sidebar is saved, and the refetch it sets off has
  // yet to land: the cached list is the order before it.
  const SIDEBAR_ORDER = [HOME, PINNED, BRAVO, ALPHA, CHARLIE];

  it('lifts no card until the read lands', async () => {
    const { queryClient } = await renderGallery();
    const read = holdNextRead();

    await act(async () => { void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() }); });
    await waitFor(() => expect(cardButton('Alpha')).not.toHaveAttribute('aria-roledescription'));
    expect(draggable()).toBe(false);

    await read.land(SIDEBAR_ORDER);
    await waitFor(() => expect(cardNames()).toEqual(['Pinned', 'Bravo', 'Alpha', 'Charlie']));
    expect(cardButton('Alpha')).toHaveAttribute('aria-roledescription', 'sortable');
    expect(draggable()).toBe(true);
  });

  it('moves a drop made meanwhile from the order the read brings, not the cached one', async () => {
    // The save stays out, so the grid shows what the drop made of the order.
    mockReorder.mockReturnValueOnce(new Promise<void>(() => {}));
    const { queryClient } = await renderGallery();
    const read = holdNextRead();
    await act(async () => { void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() }); });

    drop('charlie', 'alpha');
    // Nothing is written off the old list while the new one is on its way.
    expect(mockReorder).not.toHaveBeenCalled();
    await read.land(SIDEBAR_ORDER);

    await waitFor(() => expect(mockReorder).toHaveBeenCalledWith([
      { workspace_id: 'home', sort_order: 0 },
      { workspace_id: 'pinned', sort_order: 1 },
      { workspace_id: 'bravo', sort_order: 2 },
      { workspace_id: 'charlie', sort_order: 3 },
      { workspace_id: 'alpha', sort_order: 4 },
    ]));
    expect(cardNames()).toEqual(['Pinned', 'Bravo', 'Charlie', 'Alpha']);
  });
});

describe('WorkspaceGallery, a card', () => {
  it('is lifted by a keyboard with Space and opened with Enter', async () => {
    const { onSelect } = await renderGallery();
    const sensors = (dnd.props?.sensors ?? []).map((s) => s.sensor.name);
    expect(sensors).toEqual(expect.arrayContaining(['KeyboardSensor', 'MouseSensor', 'TouchSensor']));

    const alpha = cardButton('Alpha');
    alpha.focus();
    fireEvent.keyDown(alpha, { key: 'Enter', code: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('alpha', 'Alpha', 'running');

    onSelect.mockClear();
    fireEvent.keyDown(alpha, { key: ' ', code: 'Space' });
    await waitFor(() => expect(screen.getByTestId('workspace-card-grid').querySelector('[data-drag-state="placeholder"]')).not.toBeNull());
    expect(onSelect).not.toHaveBeenCalled();
    fireEvent.keyDown(document.activeElement ?? alpha, { key: 'Escape', code: 'Escape' });
    await waitFor(() => expect(screen.getByTestId('workspace-card-grid').querySelector('[data-drag-state]')).toBeNull());
    expect(mockReorder).not.toHaveBeenCalled();
  });

  it('opens with Space where it cannot drag', async () => {
    const { onSelect } = await renderGallery('activity');
    fireEvent.keyDown(cardButton('Alpha'), { key: ' ', code: 'Space' });
    expect(onSelect).toHaveBeenCalledWith('alpha', 'Alpha', 'running');
  });

  it('does not open with Space while a drop is saving, and still opens with Enter', async () => {
    let finish!: () => void;
    mockReorder.mockReturnValueOnce(new Promise<void>((resolve) => { finish = resolve; }));
    const { onSelect } = await renderGallery();

    drop('charlie', 'alpha');
    await waitFor(() => expect(cardButton('Bravo')).not.toHaveAttribute('aria-roledescription'));
    // The keyboard user who dropped with Space presses it again for the next card.
    const space = fireEvent.keyDown(cardButton('Bravo'), { key: ' ', code: 'Space' });
    expect(space).toBe(false);
    expect(onSelect).not.toHaveBeenCalled();
    fireEvent.keyDown(cardButton('Bravo'), { key: 'Enter', code: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('bravo', 'Bravo', 'running');

    await act(async () => { finish(); });
  });

  it('does not open with Space while the list is read again', async () => {
    const { onSelect, queryClient } = await renderGallery();
    const read = holdNextRead();
    await act(async () => { void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() }); });
    await waitFor(() => expect(cardButton('Alpha')).not.toHaveAttribute('aria-roledescription'));

    fireEvent.keyDown(cardButton('Alpha'), { key: ' ', code: 'Space' });
    expect(onSelect).not.toHaveBeenCalled();
    await read.land(CUSTOM);
  });

  it('starts no drag from its menu, while a press on the card itself does', async () => {
    await renderGallery();
    const card = screen.getAllByTestId('workspace-card').find((c) => within(c).queryByText('Alpha'))!;
    const lifted = () => screen.getByTestId('workspace-card-grid').querySelector('[data-drag-state]');

    const menu = within(card).getByTestId('workspace-card-menu');
    expect(cardButton('Alpha').contains(menu)).toBe(false);
    fireEvent.mouseDown(menu, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.mouseMove(document, { clientX: 60, clientY: 60 });
    expect(lifted()).toBeNull();
    fireEvent.mouseUp(document, { clientX: 60, clientY: 60 });

    fireEvent.mouseDown(cardButton('Alpha'), { button: 0, clientX: 10, clientY: 10 });
    fireEvent.mouseMove(document, { clientX: 60, clientY: 60 });
    await waitFor(() => expect(lifted()).not.toBeNull());
    fireEvent.keyDown(document, { key: 'Escape', code: 'Escape' });
    await waitFor(() => expect(lifted()).toBeNull());
    expect(mockReorder).not.toHaveBeenCalled();
  });
});
