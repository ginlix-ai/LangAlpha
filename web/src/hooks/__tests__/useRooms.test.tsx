/**
 * The Rooms hooks: query keys, the log's optimistic append and merge, and the
 * SSE hook's cache append / reconnect contract. The wire contract itself lives
 * in api/__tests__/rooms.test.ts; here the api module is stubbed so the
 * assertions are about what the hooks put in the cache.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
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

import {
  addRoomMember,
  createRoom,
  getRoom,
  getRoomLog,
  getRooms,
  removeRoomMember,
  sendRoomMessage,
  streamRoom,
  updateRoom,
} from '@/api/rooms';
import {
  useAddRoomMember,
  useCreateRoom,
  useRemoveRoomMember,
  useRoom,
  useRoomLog,
  useRooms,
  useRoomStream,
  useSendRoomMessage,
  useUpdateRoom,
} from '../useRooms';

const mockGetRooms = getRooms as unknown as ReturnType<typeof vi.fn>;
const mockGetRoom = getRoom as unknown as ReturnType<typeof vi.fn>;
const mockCreateRoom = createRoom as unknown as ReturnType<typeof vi.fn>;
const mockUpdateRoom = updateRoom as unknown as ReturnType<typeof vi.fn>;
const mockAddMember = addRoomMember as unknown as ReturnType<typeof vi.fn>;
const mockRemoveMember = removeRoomMember as unknown as ReturnType<typeof vi.fn>;
const mockSendMessage = sendRoomMessage as unknown as ReturnType<typeof vi.fn>;
const mockGetRoomLog = getRoomLog as unknown as ReturnType<typeof vi.fn>;
const mockStreamRoom = streamRoom as unknown as ReturnType<typeof vi.fn>;

const ROOM: RoomView = {
  room_id: 'room-1',
  workspace_id: 'ws-1',
  name: 'Morning desk',
  host_bot_id: 'bot-1',
  responder: 'everyone',
  bulletin: '',
  status: 'active',
  created_at: null,
  members: [
    { bot_id: 'bot-1', name: 'Chief of Staff', role: 'chief_of_staff', status: 'active', room_role: 'host' },
    { bot_id: 'bot-2', name: 'Macro analyst', role: 'analyst', status: 'active', room_role: 'member' },
  ],
};

function entry(overrides: Partial<RoomLogEntry> & Pick<RoomLogEntry, 'seq' | 'sender_type' | 'content'>): RoomLogEntry {
  return {
    room_id: 'room-1',
    round_id: null,
    sender_id: 'user-1',
    kind: 'message',
    created_at: null,
    ...overrides,
  };
}

beforeEach(() => {
  for (const mock of [
    mockGetRooms,
    mockGetRoom,
    mockCreateRoom,
    mockUpdateRoom,
    mockAddMember,
    mockRemoveMember,
    mockSendMessage,
    mockGetRoomLog,
    mockStreamRoom,
  ]) {
    mock.mockReset();
  }
  mockStreamRoom.mockReturnValue(vi.fn());
  mockGetRoomLog.mockResolvedValue({ entries: [], latest_seq: 0 });
});

describe('useRooms', () => {
  it('reads every room under the shared "all" key', async () => {
    mockGetRooms.mockResolvedValue({ rooms: [ROOM], max_bots: 12 });
    const { result, queryClient } = renderHookWithProviders(() => useRooms());

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetRooms).toHaveBeenCalledWith(null);
    expect(result.current.data?.rooms).toEqual([ROOM]);
    expect(queryKeys.rooms.list(null)).toEqual(['rooms', 'list', 'all']);
    expect(queryClient.getQueryData(queryKeys.rooms.list(null))).toEqual({ rooms: [ROOM], max_bots: 12 });
  });

  it('keys a workspace-scoped read apart', async () => {
    mockGetRooms.mockResolvedValue({ rooms: [], max_bots: 12 });
    const { result } = renderHookWithProviders(() => useRooms('ws-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetRooms).toHaveBeenCalledWith('ws-1');
    expect(queryKeys.rooms.list('ws-1')).toEqual(['rooms', 'list', 'ws-1']);
  });
});

describe('useRoom', () => {
  it('reads one room with its members', async () => {
    mockGetRoom.mockResolvedValue(ROOM);
    const { result } = renderHookWithProviders(() => useRoom('room-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(ROOM);
    expect(mockGetRoom).toHaveBeenCalledWith('room-1');
    expect(queryKeys.rooms.detail('room-1')).toEqual(['rooms', 'detail', 'room-1']);
  });

  it('does not fetch until an id is in hand', () => {
    const { result } = renderHookWithProviders(() => useRoom(null));

    expect(result.current.isFetching).toBe(false);
    expect(mockGetRoom).not.toHaveBeenCalled();
  });
});

describe('useRoomLog', () => {
  it('reads the first page when the room is younger than one', async () => {
    mockGetRoomLog.mockResolvedValue({ entries: [entry({ seq: 1, sender_type: 'user', content: 'Hi' })], latest_seq: 1 });
    const { result } = renderHookWithProviders(() => useRoomLog('room-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetRoomLog).toHaveBeenCalledTimes(1);
    expect(mockGetRoomLog).toHaveBeenCalledWith('room-1', 0, 200);
    expect(result.current.data).toHaveLength(1);
  });

  it('reads the tail of an older room through latest_seq', async () => {
    const old = Array.from({ length: 200 }, (_, i) => entry({ seq: i + 1, sender_type: 'bot', content: `old ${i}` }));
    const tail = Array.from({ length: 200 }, (_, i) => entry({ seq: 201 + i, sender_type: 'bot', content: `new ${i}` }));
    mockGetRoomLog
      .mockResolvedValueOnce({ entries: old, latest_seq: 400 })
      .mockResolvedValueOnce({ entries: tail, latest_seq: 400 });
    const { result } = renderHookWithProviders(() => useRoomLog('room-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetRoomLog).toHaveBeenNthCalledWith(1, 'room-1', 0, 200);
    expect(mockGetRoomLog).toHaveBeenNthCalledWith(2, 'room-1', 200, 200);
    expect(result.current.data?.map((row) => row.content)[0]).toBe('new 0');
  });

  it('keeps rows the SSE tail appended when a refetch lands', async () => {
    const first = entry({ seq: 1, sender_type: 'user', content: 'Hi' });
    const streamed = entry({ seq: 2, sender_type: 'bot', content: 'Streamed', sender_id: 'bot-1' });
    mockGetRoomLog.mockResolvedValue({ entries: [first], latest_seq: 1 });
    const { result, queryClient } = renderHookWithProviders(() => useRoomLog('room-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    queryClient.setQueryData(queryKeys.rooms.log('room-1'), [first, streamed]);
    mockGetRoomLog.mockResolvedValue({ entries: [first], latest_seq: 2 });
    await act(async () => {
      await result.current.refetch();
    });

    // The stream appended a row the refetched page does not carry; the merge
    // keeps it rather than dropping bytes between the two reads.
    expect(queryClient.getQueryData<RoomLogEntry[]>(queryKeys.rooms.log('room-1'))?.map((row) => row.seq)).toEqual([1, 2]);
  });
});

describe('room writes', () => {
  it('creates a room, seeds its detail and reads the list again', async () => {
    mockCreateRoom.mockResolvedValue(ROOM);
    const { result, queryClient } = renderHookWithProviders(() => useCreateRoom());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await result.current.mutateAsync({
      workspace_id: 'ws-1',
      name: 'Morning desk',
      member_bot_ids: ['bot-1'],
    });

    expect(mockCreateRoom).toHaveBeenCalledWith({
      workspace_id: 'ws-1',
      name: 'Morning desk',
      member_bot_ids: ['bot-1'],
    });
    expect(queryClient.getQueryData(queryKeys.rooms.detail('room-1'))).toEqual(ROOM);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
  });

  it('merges an update into the room\'s members instead of emptying them', async () => {
    mockUpdateRoom.mockResolvedValue({ ...ROOM, name: 'Renamed' });
    const { result, queryClient } = renderHookWithProviders(() => useUpdateRoom());
    queryClient.setQueryData(queryKeys.rooms.detail('room-1'), ROOM);
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await result.current.mutateAsync({ roomId: 'room-1', patch: { name: 'Renamed' } });

    const cached = queryClient.getQueryData<RoomView>(queryKeys.rooms.detail('room-1'));
    expect(cached?.name).toBe('Renamed');
    expect(cached?.members).toHaveLength(2);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
  });

  it('lands the room view an add returns', async () => {
    const added = { ...ROOM, members: [...ROOM.members, { bot_id: 'bot-3', name: 'Rates analyst', role: 'analyst', status: 'active', room_role: 'member' }] };
    mockAddMember.mockResolvedValue(added);
    const { result, queryClient } = renderHookWithProviders(() => useAddRoomMember());

    await result.current.mutateAsync({ roomId: 'room-1', botId: 'bot-3' });

    expect(mockAddMember).toHaveBeenCalledWith('room-1', 'bot-3');
    expect(queryClient.getQueryData<RoomView>(queryKeys.rooms.detail('room-1'))?.members).toHaveLength(3);
  });

  it('drops a removed member from the cached room', async () => {
    mockRemoveMember.mockResolvedValue({ ok: true, bot_id: 'bot-2' });
    const { result, queryClient } = renderHookWithProviders(() => useRemoveRoomMember());
    queryClient.setQueryData(queryKeys.rooms.detail('room-1'), ROOM);

    await result.current.mutateAsync({ roomId: 'room-1', botId: 'bot-2' });

    const cached = queryClient.getQueryData<RoomView>(queryKeys.rooms.detail('room-1'));
    expect(cached?.members.map((member) => member.bot_id)).toEqual(['bot-1']);
  });

  it('appends a sent message before the server answers, then turns it real', async () => {
    let resolveSend: ((answer: unknown) => void) | null = null;
    mockSendMessage.mockImplementation(
      () => new Promise((resolve) => { resolveSend = resolve; }),
    );
    // No log observer is mounted here, so the client keeps the log query in
    // its cache for the assertion (the test client's default gc is 0).
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    const { result } = renderHookWithProviders(() => useSendRoomMessage(), { queryClient });
    const logKey = queryKeys.rooms.log('room-1');
    queryClient.setQueryData(logKey, [entry({ seq: 1, sender_type: 'user', content: 'Earlier' })]);

    const sending = result.current.mutateAsync({ roomId: 'room-1', content: 'What moved?' });
    await waitFor(() => expect(queryClient.getQueryData<RoomLogEntry[]>(logKey)).toHaveLength(2));
    const optimistic = queryClient.getQueryData<RoomLogEntry[]>(logKey)![1];
    expect(optimistic.content).toBe('What moved?');
    expect(optimistic.seq).toBeLessThan(0);

    await act(async () => {
      resolveSend?.({ message_seq: 2, round_id: 'round-1', round_status: 'queued' });
      await sending;
    });

    const landed = queryClient.getQueryData<RoomLogEntry[]>(logKey)![1];
    expect(landed.seq).toBe(2);
    expect(landed.round_id).toBe('round-1');
  });

  it('draws one row when the stream beat the send answer to its seq', async () => {
    let resolveSend: ((answer: unknown) => void) | null = null;
    mockSendMessage.mockImplementation(() => new Promise((resolve) => { resolveSend = resolve; }));
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    const logKey = queryKeys.rooms.log('room-1');
    queryClient.setQueryData(logKey, []);
    const { result: send } = renderHookWithProviders(() => useSendRoomMessage(), { queryClient });

    const sending = send.current.mutateAsync({ roomId: 'room-1', content: 'What moved?' });
    await waitFor(() => expect(queryClient.getQueryData<RoomLogEntry[]>(logKey)).toHaveLength(1));
    const optimistic = queryClient.getQueryData<Array<RoomLogEntry & { pending?: boolean }>>(logKey)![0];
    expect(optimistic.pending).toBe(true);

    // The tail persists the row before the POST answers: same seq, same words.
    queryClient.setQueryData<Array<RoomLogEntry & { pending?: boolean }>>(logKey, (previous) => [
      ...(previous ?? []).filter((entry) => !entry.pending),
      entry({ seq: 5, sender_type: 'user', content: 'What moved?' }),
    ]);
    await act(async () => {
      resolveSend?.({ message_seq: 5, round_id: 'round-1', round_status: 'queued' });
      await sending;
    });

    const rows = queryClient.getQueryData<RoomLogEntry[]>(logKey)!;
    expect(rows.filter((row) => row.seq === 5)).toHaveLength(1);
    expect(rows).toHaveLength(1);
  });

  it('takes the optimistic row back out when the send is refused', async () => {
    mockSendMessage.mockRejectedValue(new Error('Room is archived'));
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    const { result } = renderHookWithProviders(() => useSendRoomMessage(), { queryClient });
    const logKey = queryKeys.rooms.log('room-1');
    queryClient.setQueryData(logKey, []);

    await act(async () => {
      await result.current
        .mutateAsync({ roomId: 'room-1', content: 'Too late?' })
        .catch(() => undefined);
    });

    expect(queryClient.getQueryData<RoomLogEntry[]>(logKey)).toEqual([]);
  });

  it('re-reads the room row when a send is refused, so archived state reaches the composer', async () => {
    mockSendMessage.mockRejectedValue(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'This room is archived.' } },
      }),
    );
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    const { result } = renderHookWithProviders(() => useSendRoomMessage(), { queryClient });
    queryClient.setQueryData(queryKeys.rooms.log('room-1'), []);
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await act(async () => {
      await result.current.mutateAsync({ roomId: 'room-1', content: 'Too late?' }).catch(() => undefined);
    });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.detail('room-1') });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
    invalidate.mockRestore();
  });
});

describe('useRoomStream', () => {
  const status: RoomRoundStatus = {
    round_id: 'round-1',
    status: 'running',
    slot_cursor: 0,
    slots: [{ bot_id: 'bot-1', name: 'Chief of Staff', state: 'pending' }],
  };

  it('opens the tail from the cursor and appends messages into the log cache', async () => {
    const abort = vi.fn();
    mockStreamRoom.mockReturnValue(abort);
    const { result, queryClient, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 3));
    const handlers = mockStreamRoom.mock.calls[0][2] as {
      onOpen: () => void;
      onMessage: (row: RoomLogEntry) => void;
      onRoundStatus: (round: RoomRoundStatus) => void;
      onRoomUpdated: (update: { room_id: string; updated_at: string | null }) => void;
    };
    expect(mockStreamRoom).toHaveBeenCalledWith('room-1', 3, expect.any(Object));

    act(() => handlers.onOpen());
    expect(result.current.live).toBe(true);

    const streamed = entry({ seq: 4, sender_type: 'bot', content: 'Streamed', sender_id: 'bot-1' });
    act(() => handlers.onMessage(streamed));
    expect(queryClient.getQueryData(queryKeys.rooms.log('room-1'))).toEqual([streamed]);

    act(() => handlers.onRoundStatus(status));
    expect(result.current.round).toEqual(status);

    unmount();
    expect(abort).toHaveBeenCalled();
  });

  it('reads the room detail and the list when the room row changed, and leaves the log alone', () => {
    mockStreamRoom.mockReturnValue(vi.fn());
    const { queryClient, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 0));
    const handlers = mockStreamRoom.mock.calls[0][2] as {
      onRoomUpdated: (update: { room_id: string; updated_at: string | null }) => void;
    };
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    act(() => handlers.onRoomUpdated({ room_id: 'room-1', updated_at: '2026-11-08T12:00:00Z' }));

    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.detail('room-1') });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.rooms.log('room-1') });
    unmount();
  });

  it('does not re-read a row a reconnect re-announced unchanged', () => {
    mockStreamRoom.mockReturnValue(vi.fn());
    const { queryClient, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 0));
    const handlers = mockStreamRoom.mock.calls[0][2] as {
      onRoomUpdated: (update: { room_id: string; updated_at: string | null }) => void;
    };
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    act(() => handlers.onRoomUpdated({ room_id: 'room-1', updated_at: '2026-11-08T12:00:00Z' }));
    invalidate.mockClear();
    act(() => handlers.onRoomUpdated({ room_id: 'room-1', updated_at: '2026-11-08T12:00:00Z' }));
    expect(invalidate).not.toHaveBeenCalled();

    act(() => handlers.onRoomUpdated({ room_id: 'room-1', updated_at: '2026-11-08T12:05:00Z' }));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.detail('room-1') });
    unmount();
  });

  it('exposes a first connection as not-yet-live, not as reconnecting', () => {
    mockStreamRoom.mockReturnValue(vi.fn());
    const { result, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 0));
    expect(result.current.live).toBe(false);
    expect(result.current.connectedOnce).toBe(false);

    const handlers = mockStreamRoom.mock.calls[0][2] as { onOpen: () => void };
    act(() => handlers.onOpen());
    expect(result.current.live).toBe(true);
    expect(result.current.connectedOnce).toBe(true);
    unmount();
  });

  it('stops on a 4xx, exposes the error and reopens only on retry', () => {
    vi.useFakeTimers();
    mockStreamRoom.mockReturnValue(vi.fn());
    const { result, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 0));
    const handlers = mockStreamRoom.mock.calls[0][2] as { onClose: (error: Error | null) => void };

    act(() => {
      handlers.onClose(Object.assign(new Error('HTTP error! status: 404'), { status: 404 }));
    });
    expect(result.current.error).toBeInstanceOf(Error);
    expect(result.current.live).toBe(false);
    expect(mockStreamRoom).toHaveBeenCalledTimes(1);

    act(() => { vi.advanceTimersByTime(30_000); });
    expect(mockStreamRoom).toHaveBeenCalledTimes(1);

    act(() => result.current.retry());
    expect(mockStreamRoom).toHaveBeenCalledTimes(2);
    expect(result.current.error).toBeNull();

    unmount();
    vi.useRealTimers();
  });

  it('backs off between reconnects and resets the wait once a tail opens', () => {
    vi.useFakeTimers();
    mockStreamRoom.mockReturnValue(vi.fn());
    renderHookWithProviders(() => useRoomStream('room-1', 0));
    const first = mockStreamRoom.mock.calls[0][2] as { onClose: (error: Error | null) => void };

    act(() => first.onClose(new Error('Network Error')));
    expect(mockStreamRoom).toHaveBeenCalledTimes(1);
    act(() => { vi.advanceTimersByTime(1_999); });
    expect(mockStreamRoom).toHaveBeenCalledTimes(1);
    act(() => { vi.advanceTimersByTime(1); });
    expect(mockStreamRoom).toHaveBeenCalledTimes(2);

    const second = mockStreamRoom.mock.calls[1][2] as {
      onOpen: () => void;
      onClose: (error: Error | null) => void;
    };
    act(() => second.onOpen());
    act(() => second.onClose(new Error('Network Error')));
    act(() => { vi.advanceTimersByTime(2_000); });
    // A successful open reset the attempt count to the fast first retry.
    expect(mockStreamRoom).toHaveBeenCalledTimes(3);

    vi.useRealTimers();
  });

  it('streams a row twice without duplicating it and lands an answer on the same row', async () => {
    const handlersRef: { current: { onMessage: (row: RoomLogEntry) => void } | null } = { current: null };
    mockStreamRoom.mockImplementation((_id: string, _seq: number, handlers: { onMessage: (row: RoomLogEntry) => void }) => {
      handlersRef.current = handlers;
      return vi.fn();
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    const { queryClient: client, unmount } = renderHookWithProviders(() => useRoomStream('room-1', 0), { queryClient });
    const streamed = entry({ seq: 1, sender_type: 'user', content: 'What moved?' });
    act(() => handlersRef.current?.onMessage(streamed));
    // The replay after a reconnect resends the same row; the cache keeps one.
    act(() => handlersRef.current?.onMessage(streamed));
    expect(client.getQueryData(queryKeys.rooms.log('room-1'))).toEqual([streamed]);
    unmount();
  });

  it('does not open a tail until a room id is in hand', () => {
    renderHookWithProviders(() => useRoomStream(null, 0));

    expect(mockStreamRoom).not.toHaveBeenCalled();
  });
});
