/**
 * The room panel: the member strip, the shared log (messages vs the room's own
 * status rows), the composer's optimistic send, and the live tail's two frames.
 *
 * The hooks are the real ones; only the api module is stubbed, so a row that
 * appears here is a row the SSE handler actually appended, and a send that
 * appears before its answer is the log cache's optimistic entry.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { RoomLogEntry, RoomRoundStatus, RoomView } from '@/api/rooms';

vi.mock('@/api/rooms', () => ({
  getRooms: vi.fn(),
  getRoom: vi.fn(),
  createRoom: vi.fn(),
  updateRoom: vi.fn(),
  addRoomMember: vi.fn(),
  removeRoomMember: vi.fn(),
  sendRoomMessage: vi.fn(),
  getRoomLog: vi.fn(),
  streamRoom: vi.fn(),
}));

const botsState = vi.hoisted(() => ({
  bots: [] as { bot_id: string; name: string }[],
  loading: false,
  failed: false,
  refetch: vi.fn(),
}));

vi.mock('@/hooks/useBots', () => ({
  useBots: () => ({
    data: { bots: botsState.bots, max_bots: 12 },
    isLoading: botsState.loading,
    isError: botsState.failed,
    refetch: botsState.refetch,
  }),
}));

import {
  getRoom,
  getRoomLog,
  removeRoomMember,
  sendRoomMessage,
  streamRoom,
  updateRoom,
} from '@/api/rooms';
import RoomPanel from '../components/RoomPanel';

const mockGetRoom = getRoom as unknown as ReturnType<typeof vi.fn>;
const mockGetRoomLog = getRoomLog as unknown as ReturnType<typeof vi.fn>;
const mockSendMessage = sendRoomMessage as unknown as ReturnType<typeof vi.fn>;
const mockStreamRoom = streamRoom as unknown as ReturnType<typeof vi.fn>;
const mockUpdateRoom = updateRoom as unknown as ReturnType<typeof vi.fn>;
const mockRemoveMember = removeRoomMember as unknown as ReturnType<typeof vi.fn>;

const ROOM: RoomView = {
  room_id: 'room-1',
  workspace_id: 'ws-1',
  name: 'Morning desk',
  host_bot_id: 'bot-1',
  responder: 'everyone',
  bulletin: 'Keep it brief.',
  status: 'active',
  created_at: null,
  members: [
    { bot_id: 'bot-1', name: 'Chief of Staff', role: 'chief_of_staff', status: 'active', room_role: 'host', avatar: { emoji: '🧭' } },
    { bot_id: 'bot-2', name: 'Macro analyst', role: 'analyst', status: 'active', room_role: 'member', avatar: { kind: 'mark', shape: 'hex', color: 'teal' } },
    { bot_id: 'bot-3', name: 'Rates analyst', role: 'analyst', status: 'paused', room_role: 'member', avatar: {} },
  ],
};

const LOG: RoomLogEntry[] = [
  {
    seq: 1,
    room_id: 'room-1',
    round_id: 'round-0',
    sender_type: 'user',
    sender_id: 'user-1',
    kind: 'message',
    content: 'What moved?',
    created_at: null,
  },
  {
    seq: 2,
    room_id: 'room-1',
    round_id: 'round-0',
    sender_type: 'bot',
    sender_id: 'bot-2',
    kind: 'message',
    content: 'Rates are quiet.',
    created_at: null,
  },
  {
    seq: 3,
    room_id: 'room-1',
    round_id: 'round-0',
    sender_type: 'system',
    sender_id: 'room',
    kind: 'status',
    content: 'Rates analyst was skipped: not an active member.',
    created_at: null,
  },
];

function streamHandlers() {
  const call = mockStreamRoom.mock.calls.at(-1);
  return call?.[2] as {
    onOpen: () => void;
    onMessage: (entry: RoomLogEntry) => void;
    onRoundStatus: (status: RoomRoundStatus) => void;
    onClose: (error: Error | null) => void;
  };
}

async function renderPanel() {
  const view = renderWithProviders(<RoomPanel roomId="room-1" onClose={vi.fn()} />);
  expect(await screen.findByText('Morning desk')).toBeInTheDocument();
  await waitFor(() => expect(mockStreamRoom).toHaveBeenCalled());
  return view;
}

beforeEach(() => {
  botsState.bots = [];
  botsState.loading = false;
  botsState.failed = false;
  botsState.refetch.mockReset();
  mockGetRoom.mockReset().mockResolvedValue(ROOM);
  mockGetRoomLog.mockReset().mockResolvedValue({ entries: LOG, latest_seq: 3 });
  mockSendMessage.mockReset();
  mockUpdateRoom.mockReset().mockResolvedValue({ ...ROOM, status: 'archived' });
  mockRemoveMember.mockReset().mockResolvedValue({ ok: true, bot_id: 'bot-2' });
  mockStreamRoom.mockReset().mockReturnValue(vi.fn());
});

describe('RoomPanel', () => {
  it('draws members with their roles, host and paused state', async () => {
    await renderPanel();

    expect(screen.getAllByText('Chief of Staff').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Macro analyst').length).toBeGreaterThan(0);
    expect(screen.getByText('Rates analyst')).toBeInTheDocument();
    expect(screen.getByText('Host')).toBeInTheDocument();
    expect(screen.getByText('Paused')).toBeInTheDocument();
    // The host has no remove control; the other members do.
    expect(screen.queryByRole('button', { name: 'Remove Chief of Staff' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Remove Macro analyst' })).toBeInTheDocument();
    expect(screen.getByText('Keep it brief.')).toBeInTheDocument();
  });

  it('names the speakers in the log and gives the room\'s status rows their own shape', async () => {
    await renderPanel();

    const messages = screen.getAllByTestId('room-log-message');
    expect(messages).toHaveLength(2);
    expect(messages[0]).toHaveTextContent('You');
    expect(messages[0]).toHaveTextContent('What moved?');
    expect(messages[1]).toHaveTextContent('Macro analyst');
    expect(messages[1]).toHaveTextContent('Rates are quiet.');

    const statusRow = screen.getByTestId('room-log-status');
    expect(statusRow).toHaveClass('rooms-log__status');
    expect(statusRow).toHaveTextContent('Rates analyst was skipped: not an active member.');
    expect(statusRow).not.toHaveClass('rooms-log__row');
  });

  it('draws the member BotMark in its chip, its log line and the speaking line', async () => {
    await renderPanel();

    // The member strip wears each bot's mark.
    const macroChips = screen.getAllByText('Macro analyst')
      .map((node) => node.closest('.rooms-member'))
      .filter((node): node is HTMLElement => node !== null);
    expect(macroChips).toHaveLength(1);
    const macroMark = macroChips[0].querySelector('[data-bot-mark]') as HTMLElement;
    expect(macroMark).toHaveAttribute('data-mark-kind', 'mark');
    expect(macroMark).toHaveAttribute('data-mark-shape', 'hex');
    // The host's emoji avatar resolves too.
    const cosChip = screen.getAllByText('Chief of Staff')
      .map((node) => node.closest('.rooms-member'))
      .find((node): node is HTMLElement => node !== null) as HTMLElement;
    expect(cosChip.querySelector('[data-bot-mark]')).toHaveAttribute('data-mark-kind', 'emoji');

    // The log's sender line carries the sender's mark, and the user's does not.
    const messages = screen.getAllByTestId('room-log-message');
    expect(messages[1].querySelector('[data-bot-mark]')).not.toBeNull();
    expect(messages[0].querySelector('[data-bot-mark]')).toBeNull();

    // The round line is the speaker's.
    act(() => {
      streamHandlers().onRoundStatus({
        round_id: 'round-1',
        status: 'running',
        slot_cursor: 0,
        slots: [{ bot_id: 'bot-2', name: 'Macro analyst', state: 'pending' }],
      });
    });
    await waitFor(() => {
      const line = screen.getByTestId('room-round-status');
      expect(line).toHaveTextContent('Macro analyst is speaking…');
      expect(line.querySelector('[data-bot-mark]')).not.toBeNull();
    });
  });

  it('appends a live message and tracks who is speaking from round_status', async () => {
    await renderPanel();
    const handlers = streamHandlers();

    act(() => {
      handlers.onMessage({
        seq: 4,
        room_id: 'room-1',
        round_id: 'round-1',
        sender_type: 'bot',
        sender_id: 'bot-1',
        kind: 'message',
        content: 'Fresh take from the desk.',
        created_at: null,
      });
    });
    // Query-cache notifications land on a microtask, so the row is painted on
    // the next tick rather than inside the handler.
    expect(await screen.findByText('Fresh take from the desk.')).toBeInTheDocument();
    await waitFor(() => expect(screen.getAllByTestId('room-log-message')).toHaveLength(3));

    act(() => {
      handlers.onRoundStatus({
        round_id: 'round-1',
        status: 'running',
        slot_cursor: 0,
        slots: [{ bot_id: 'bot-1', name: 'Chief of Staff', state: 'pending' }],
      });
    });
    await waitFor(() =>
      expect(screen.getByTestId('room-round-status')).toHaveTextContent('Chief of Staff is speaking…'),
    );

    act(() => {
      handlers.onRoundStatus({
        round_id: 'round-1',
        status: 'finished',
        slot_cursor: 1,
        slots: [],
      });
    });
    await waitFor(() =>
      expect(screen.getByTestId('room-round-status')).toHaveTextContent('Round finished'),
    );
  });

  it('sends the composer text optimistically, then with the API', async () => {
    let resolveSend: ((answer: unknown) => void) | null = null;
    mockSendMessage.mockImplementation(() => new Promise((resolve) => { resolveSend = resolve; }));
    await renderPanel();

    fireEvent.change(screen.getByLabelText('Message the room…'), {
      target: { value: 'What about credit?' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() =>
      expect(mockSendMessage).toHaveBeenCalledWith('room-1', 'What about credit?'),
    );
    // Drawn before the server answers, so the message never looks lost.
    expect(await screen.findByText('What about credit?')).toBeInTheDocument();
    expect(screen.getByLabelText('Message the room…')).toHaveValue('');

    await act(async () => {
      resolveSend?.({ message_seq: 4, round_id: 'round-1', round_status: 'queued' });
      await Promise.resolve();
    });
    expect(screen.getByText('What about credit?')).toBeInTheDocument();
  });

  it('removes a member and archives the room through their writes', async () => {
    await renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Remove Macro analyst' }));
    await waitFor(() => expect(mockRemoveMember).toHaveBeenCalledWith('room-1', 'bot-2'));
    await waitFor(() => expect(screen.queryByText('Macro analyst')).toBeNull());

    fireEvent.click(screen.getByRole('button', { name: 'Archive room' }));
    const dialog = await screen.findByRole('dialog');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Archive' }));

    await waitFor(() => expect(mockUpdateRoom).toHaveBeenCalledWith('room-1', { status: 'archived' }));
    expect(await screen.findByText(/This room is archived/)).toBeInTheDocument();
  });

  it('opens the live tail only once the log page has fixed the cursor', async () => {
    await renderPanel();

    // The log's highest seq is 3: a tail opened before the page resolves would
    // replay the whole history from last_seq=0.
    await waitFor(() => expect(mockStreamRoom).toHaveBeenCalledWith('room-1', 3, expect.any(Object)));
    expect(mockStreamRoom.mock.calls.map((call) => call[1])).not.toContain(0);
  });

  it('stops on a refused stream and offers Retry', async () => {
    await renderPanel();
    await waitFor(() => expect(mockStreamRoom).toHaveBeenCalled());

    act(() => {
      streamHandlers().onClose(Object.assign(new Error('HTTP error! status: 404'), { status: 404 }));
    });

    expect(await screen.findByText("The room's live updates stopped.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(mockStreamRoom).toHaveBeenCalledTimes(2));
  });

  it('restores a refused send and re-reads the room row', async () => {
    mockSendMessage.mockRejectedValue(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'This room is archived.' } },
      }),
    );
    await renderPanel();

    fireEvent.change(screen.getByLabelText('Message the room…'), {
      target: { value: 'Too late?' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    expect(await screen.findByText('This room is archived.')).toBeInTheDocument();
    expect(screen.getByLabelText('Message the room…')).toHaveValue('Too late?');
    // The refusal is the room row's news: detail is read again so the composer
    // reflects the archived state.
    await waitFor(() => expect(mockGetRoom.mock.calls.length).toBeGreaterThan(1));
  });

  it('clears a stale action banner after a later write succeeds', async () => {
    mockRemoveMember.mockRejectedValue(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'The room is busy.' } },
      }),
    );
    mockSendMessage.mockResolvedValue({ message_seq: 4, round_id: 'round-1', round_status: 'queued' });
    await renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Remove Macro analyst' }));
    expect(await screen.findByText('The room is busy.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Message the room…'), { target: { value: 'Ready now.' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => expect(screen.queryByText('The room is busy.')).toBeNull());
  });

  it('separates a members page still loading from every bot already added', async () => {
    botsState.loading = true;
    await renderPanel();

    expect(await screen.findByText("Loading the workspace's bots…")).toBeInTheDocument();
    expect(screen.queryByText('Every bot in this workspace is already in the room.')).toBeNull();
  });

  it('disables adding a member at the cap, and reports a failed bots read', async () => {
    const capped = {
      ...ROOM,
      members: Array.from({ length: 12 }, (_, index) => ({
        bot_id: `bot-${index + 1}`,
        name: `Bot ${index + 1}`,
        role: 'analyst' as const,
        status: 'active' as const,
        room_role: 'member' as const,
      })),
    };
    mockGetRoom.mockResolvedValue(capped);
    const view = await renderPanel();

    expect(await screen.findByText('A room holds at most 12 bots.')).toBeInTheDocument();
    expect(screen.queryByLabelText('Add member')).toBeNull();
    view.unmount();

    mockGetRoom.mockResolvedValue(ROOM);
    botsState.failed = true;
    await renderPanel();
    expect(await screen.findByText("Couldn't load the workspace's bots.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(botsState.refetch).toHaveBeenCalled();
  });

  it('names a single-member responder and sends its member:<id> value back unchanged', async () => {
    const single = { ...ROOM, responder: 'member:bot-2' as const };
    mockGetRoom.mockResolvedValue(single);
    await renderPanel();

    expect(await screen.findByRole('option', { name: 'Only Macro analyst replies' })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Who replies'), {
      target: { value: 'member:bot-2' },
    });
    await waitFor(() => expect(mockUpdateRoom).toHaveBeenCalledWith('room-1', {
      responder: 'member:bot-2',
    }));
  });

  it('takes no bulletin edits in an archived room and can restore it', async () => {
    mockGetRoom.mockResolvedValue({
      ...ROOM,
      status: 'archived',
      bulletin: 'Keep it brief.',
      members: ROOM.members.map((member) => ({ ...member, room_role: 'host' as const })),
    });
    await renderPanel();

    expect(await screen.findByRole('button', { name: 'Restore room' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit' })).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Restore room' }));
    await waitFor(() => expect(mockUpdateRoom).toHaveBeenCalledWith('room-1', { status: 'active' }));
  });});
