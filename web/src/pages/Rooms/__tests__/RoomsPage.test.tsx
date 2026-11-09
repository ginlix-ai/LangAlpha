/**
 * The /rooms page: the workspace-scoped list, opening a room, and creating one
 * from the dialog. The hooks are stubbed so the assertions are about what the
 * page asks them to do — the data path itself is covered by useRooms.test.tsx
 * and the panel by RoomPanel.test.tsx.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { Workspace } from '@/types/api';
import type { BotInfo } from '@/api/bots';
import type { RoomInfo, RoomView } from '@/api/rooms';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  workspaces: [] as Workspace[],
  roomsByWorkspace: {} as Record<string, RoomInfo[]>,
  roomById: {} as Record<string, RoomView>,
  bots: [] as BotInfo[],
  create: vi.fn(),
  workspacesFailed: false,
  workspacesRefetch: vi.fn(),
}));

vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: () => ({
    data: h.workspacesFailed ? undefined : { workspaces: h.workspaces, total: h.workspaces.length },
    isLoading: false,
    isError: h.workspacesFailed,
    error: h.workspacesFailed ? Object.assign(new Error('Network Error'), { message: 'Network Error' }) : null,
    refetch: h.workspacesRefetch,
  }),
}));

vi.mock('@/hooks/useBots', () => ({
  useBots: () => ({
    data: { bots: h.bots, max_bots: 20 },
    isLoading: false,
    isError: false,
  }),
}));

vi.mock('@/hooks/useRooms', async () => {
  const { useMutation } = await import('@tanstack/react-query');
  return ({
  useRooms: (workspaceId: string | null) => ({
    data: {
      rooms: workspaceId ? (h.roomsByWorkspace[workspaceId] ?? []) : Object.values(h.roomsByWorkspace).flat(),
      max_bots: 12,
    },
    isLoading: false,
    isError: false,
    error: null,
    refetch: () => undefined,
  }),
  useRoom: (roomId: string | null) => ({
    data: roomId ? h.roomById[roomId] : undefined,
    isLoading: false,
    isError: false,
    error: null,
    refetch: () => undefined,
  }),
  useRoomLog: () => ({
    data: [],
    isLoading: false,
    isError: false,
    error: null,
    refetch: () => undefined,
  }),
  useRoomStream: () => ({ round: null, live: false }),
  useCreateRoom: () => useMutation({ mutationFn: (input: unknown) => h.create(input) }),
  useUpdateRoom: () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false, error: null }),
  useAddRoomMember: () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false, error: null }),
  useRemoveRoomMember: () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false, error: null }),
  useSendRoomMessage: () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false, error: null }),
  });
});

import Rooms from '../Rooms';

const RESEARCH: Workspace = { workspace_id: 'ws-1', name: 'Research', is_home: false };
const TRADING: Workspace = { workspace_id: 'ws-2', name: 'Trading', is_home: false };

const MORNING: RoomInfo = {
  room_id: 'room-1',
  workspace_id: 'ws-1',
  name: 'Morning desk',
  host_bot_id: 'bot-1',
  responder: 'everyone',
  bulletin: '',
  status: 'active',
  created_at: null,
};

const TRADING_ROOM: RoomInfo = {
  ...MORNING,
  room_id: 'room-3',
  workspace_id: 'ws-2',
  name: 'Trading desk',
};

const CREATED: RoomView = {
  room_id: 'room-new',
  workspace_id: 'ws-1',
  name: 'Rates desk',
  host_bot_id: 'bot-1',
  responder: 'everyone',
  bulletin: '',
  status: 'active',
  created_at: null,
  members: [
    { bot_id: 'bot-1', name: 'Macro analyst', role: 'analyst', status: 'active', room_role: 'host' },
  ],
};

const BOT2: BotInfo = {
  bot_id: 'bot-2',
  user_id: 'user-1',
  workspace_id: 'ws-1',
  name: 'Rates analyst',
  role: 'analyst',
  persona: '',
  avatar: {},
  provider: null,
  model: null,
  skills: [],
  tools_allow: [],
  tools_deny: [],
  approval: 'ask',
  memory_scopes: ['workspace'],
  status: 'active',
  primary_thread_id: null,
  created_at: null,
  updated_at: null,
};

const BOT: BotInfo = {
  bot_id: 'bot-1',
  user_id: 'user-1',
  workspace_id: 'ws-1',
  name: 'Macro analyst',
  role: 'analyst',
  persona: '',
  avatar: {},
  provider: null,
  model: null,
  skills: [],
  tools_allow: [],
  tools_deny: [],
  approval: 'ask',
  memory_scopes: ['workspace'],
  status: 'active',
  primary_thread_id: null,
  created_at: null,
  updated_at: null,
};

beforeEach(() => {
  h.workspaces = [RESEARCH, TRADING];
  h.roomsByWorkspace = { 'ws-1': [MORNING], 'ws-2': [TRADING_ROOM] };
  h.roomById = {
    'room-1': { ...MORNING, members: CREATED.members },
    'room-3': { ...TRADING_ROOM, members: CREATED.members },
  };
  h.bots = [BOT, BOT2];
  h.workspacesFailed = false;
  h.workspacesRefetch.mockReset();
  h.create.mockReset();
  h.create.mockImplementation(async (input: unknown) => {
    const body = input as { room_id?: string; name?: string };
    const created = { ...CREATED, name: body.name ?? CREATED.name };
    h.roomById[created.room_id] = created;
    return created;
  });
});

describe('Rooms page', () => {
  it('lists the selected workspace\'s rooms and opens one', async () => {
    renderWithProviders(<Rooms />);

    expect(await screen.findByRole('button', { name: /Morning desk/ })).toBeInTheDocument();
    expect(screen.getByLabelText('Rooms')).toBeInTheDocument();
    expect(screen.getByText('Open a room')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /Morning desk/ }));
    expect(
      await screen.findByRole('heading', { name: 'Morning desk', level: 2 }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText('Message the room…')).toBeInTheDocument();
  });

  it('narrows the list when the workspace changes', async () => {
    renderWithProviders(<Rooms />);
    expect(await screen.findByRole('button', { name: /Morning desk/ })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Workspace'), { target: { value: 'ws-2' } });

    expect(await screen.findByRole('button', { name: /Trading desk/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Morning desk/ })).toBeNull();
  });

  it('follows a deep link into the room\'s own workspace', async () => {
    renderWithProviders(<Rooms />, { route: '/rooms?room=room-3' });

    // The link named a Trading room while Research was selected: the picker
    // follows the room so the list and the panel agree.
    await waitFor(() => expect(screen.getByLabelText('Workspace')).toHaveValue('ws-2'));
    expect(await screen.findByRole('button', { name: /Trading desk/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Morning desk/ })).toBeNull();
  });

  it('surfaces a failed workspaces read instead of the empty-workspace fallback', async () => {
    h.workspacesFailed = true;
    renderWithProviders(<Rooms />);

    expect(await screen.findByText("Couldn't load your workspaces.")).toBeInTheDocument();
    expect(screen.queryByText('Create a workspace first')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(h.workspacesRefetch).toHaveBeenCalled();
  });

  it('shows the server\'s refusal sentence when a create is refused', async () => {
    h.create.mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'A room with that name already exists.' } },
      }),
    );
    renderWithProviders(<Rooms />);
    await screen.findByRole('button', { name: /Morning desk/ });

    fireEvent.click(screen.getByRole('button', { name: 'New room' }));
    const dialog = await screen.findByRole('dialog');
    fireEvent.change(within(dialog).getByLabelText('Name'), { target: { value: 'Morning desk' } });
    fireEvent.click(within(dialog).getByLabelText('Macro analyst'));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Create room' }));

    expect(await screen.findByText('A room with that name already exists.')).toBeInTheDocument();
  });

  it('falls the host back to the first member when the chosen one is unchecked', async () => {
    renderWithProviders(<Rooms />);
    await screen.findByRole('button', { name: /Morning desk/ });

    fireEvent.click(screen.getByRole('button', { name: 'New room' }));
    const dialog = await screen.findByRole('dialog');
    fireEvent.change(within(dialog).getByLabelText('Name'), { target: { value: 'Two desks' } });
    fireEvent.click(within(dialog).getByLabelText('Macro analyst'));
    fireEvent.click(within(dialog).getByLabelText('Rates analyst'));
    fireEvent.change(within(dialog).getByLabelText('Host'), { target: { value: 'bot-2' } });
    expect(within(dialog).getByLabelText('Host')).toHaveValue('bot-2');

    // Removing the host leaves the first remaining member hosting, computed in
    // the handler rather than inside a state updater.
    fireEvent.click(within(dialog).getByLabelText('Rates analyst'));
    expect(within(dialog).getByLabelText('Host')).toHaveValue('bot-1');

    fireEvent.click(within(dialog).getByRole('button', { name: 'Create room' }));
    await waitFor(() => expect(h.create).toHaveBeenCalledWith(expect.objectContaining({
      member_bot_ids: ['bot-1'],
      host_bot_id: 'bot-1',
    })));
  });

  it('creates a room with the bots and host the dialog picks', async () => {
    renderWithProviders(<Rooms />);
    await screen.findByRole('button', { name: /Morning desk/ });

    fireEvent.click(screen.getByRole('button', { name: 'New room' }));
    const dialog = await screen.findByRole('dialog');
    fireEvent.change(within(dialog).getByLabelText('Name'), { target: { value: 'Rates desk' } });
    fireEvent.click(within(dialog).getByLabelText('Macro analyst'));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Create room' }));

    await waitFor(() =>
      expect(h.create).toHaveBeenCalledWith({
        workspace_id: 'ws-1',
        name: 'Rates desk',
        member_bot_ids: ['bot-1'],
        // The first member hosts unless the dialog says otherwise.
        host_bot_id: 'bot-1',
        responder: 'everyone',
        bulletin: '',
      }),
    );
    // The new room opens in the panel.
    expect(
      await screen.findByRole('heading', { name: 'Rates desk', level: 2 }),
    ).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).toBeNull();
  });
});
