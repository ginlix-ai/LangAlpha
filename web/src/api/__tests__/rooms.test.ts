/**
 * The Rooms API module's wire contract: every REST call the server exposes,
 * and the one raw-fetch SSE tail.
 *
 * The tail is tested against a hand-rolled response with a real `getReader()`
 * body: jsdom has no `ReadableStream` under the vm pool, and the point here is
 * the frame parser and the abort contract, not the platform's stream class.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { waitFor } from '@testing-library/react';

vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
    defaults: { baseURL: '' },
  },
}));

vi.mock('@/lib/authToken', () => ({
  getAuthHeaders: vi.fn(async () => ({ Authorization: 'Bearer test-token' })),
}));

import { api } from '@/api/client';
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
  type RoomInfo,
  type RoomLogEntry,
  type RoomView,
} from '../rooms';

const mockGet = api.get as unknown as ReturnType<typeof vi.fn>;
const mockPost = api.post as unknown as ReturnType<typeof vi.fn>;
const mockPatch = api.patch as unknown as ReturnType<typeof vi.fn>;
const mockDelete = api.delete as unknown as ReturnType<typeof vi.fn>;

const ROOM: RoomInfo = {
  room_id: 'room-1',
  workspace_id: 'ws-1',
  name: 'Morning desk',
  host_bot_id: 'bot-1',
  responder: 'everyone',
  bulletin: '',
  status: 'active',
  created_at: '2026-01-01T00:00:00Z',
};

const VIEW: RoomView = {
  ...ROOM,
  members: [
    { bot_id: 'bot-1', name: 'Chief of Staff', role: 'chief_of_staff', status: 'active', room_role: 'host' },
  ],
};

const ENTRY: RoomLogEntry = {
  seq: 4,
  room_id: 'room-1',
  round_id: 'round-1',
  sender_type: 'bot',
  sender_id: 'bot-1',
  kind: 'message',
  content: 'Rates are quiet.',
  created_at: '2026-01-01T00:01:00Z',
};

beforeEach(() => {
  mockGet.mockReset();
  mockPost.mockReset();
  mockPatch.mockReset();
  mockDelete.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('room reads', () => {
  it('asks for every room when no workspace is named', async () => {
    mockGet.mockResolvedValue({ data: { rooms: [ROOM], max_bots: 12 } });

    await expect(getRooms()).resolves.toEqual({ rooms: [ROOM], max_bots: 12 });
    expect(mockGet).toHaveBeenCalledWith('/api/v1/rooms', undefined);
  });

  it('narrows the list to one workspace', async () => {
    mockGet.mockResolvedValue({ data: { rooms: [], max_bots: 12 } });

    await getRooms('ws-1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/rooms', { params: { workspace_id: 'ws-1' } });
  });

  it('unwraps one room and its members', async () => {
    mockGet.mockResolvedValue({ data: VIEW });

    await expect(getRoom('room-1')).resolves.toEqual(VIEW);
    expect(mockGet).toHaveBeenCalledWith('/api/v1/rooms/room-1');
  });

  it('reads the log from its cursor with the page bounds the server takes', async () => {
    mockGet.mockResolvedValue({ data: { entries: [ENTRY], latest_seq: 4 } });

    await expect(getRoomLog('room-1', 3, 50)).resolves.toEqual({ entries: [ENTRY], latest_seq: 4 });
    expect(mockGet).toHaveBeenCalledWith('/api/v1/rooms/room-1/log', {
      params: { since_seq: 3, limit: 50 },
    });
  });

  it('defaults the log read to the first page', async () => {
    mockGet.mockResolvedValue({ data: { entries: [], latest_seq: 0 } });

    await getRoomLog('room-1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/rooms/room-1/log', {
      params: { since_seq: 0, limit: 100 },
    });
  });
});

describe('room writes', () => {
  it('creates a room from the dialog body', async () => {
    mockPost.mockResolvedValue({ data: VIEW });
    const input = {
      workspace_id: 'ws-1',
      name: 'Morning desk',
      member_bot_ids: ['bot-1'],
      host_bot_id: 'bot-1',
      responder: 'everyone' as const,
      bulletin: 'Stay brief.',
    };

    await expect(createRoom(input)).resolves.toEqual(VIEW);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/rooms', input);
  });

  it('patches only the fields an edit changes', async () => {
    mockPatch.mockResolvedValue({ data: { ...ROOM, status: 'archived' } });

    await updateRoom('room-1', { status: 'archived' });
    expect(mockPatch).toHaveBeenCalledWith('/api/v1/rooms/room-1', { status: 'archived' });
  });

  it('adds a member by id and unwraps the room view', async () => {
    mockPost.mockResolvedValue({ data: VIEW });

    await expect(addRoomMember('room-1', 'bot-1')).resolves.toEqual(VIEW);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/rooms/room-1/members', { bot_id: 'bot-1' });
  });

  it('removes a member from the room path', async () => {
    mockDelete.mockResolvedValue({ data: { ok: true, bot_id: 'bot-1' } });

    await expect(removeRoomMember('room-1', 'bot-1')).resolves.toEqual({ ok: true, bot_id: 'bot-1' });
    expect(mockDelete).toHaveBeenCalledWith('/api/v1/rooms/room-1/members/bot-1');
  });

  it('sends a user message, which opens a round', async () => {
    mockPost.mockResolvedValue({
      data: { message_seq: 7, round_id: 'round-1', round_status: 'queued' },
    });

    await expect(sendRoomMessage('room-1', 'What moved?')).resolves.toEqual({
      message_seq: 7,
      round_id: 'round-1',
      round_status: 'queued',
    });
    expect(mockPost).toHaveBeenCalledWith('/api/v1/rooms/room-1/messages', { content: 'What moved?' });
  });
});

// ── SSE tail ───────────────────────────────────────────────────────────────

/** A response whose body hands out the chunks in order, then EOF. */
function streamingResponse(chunks: string[]) {
  const encoder = new TextEncoder();
  const queue = chunks.map((chunk) => encoder.encode(chunk));
  const reader = {
    read: vi.fn(async () =>
      queue.length > 0 ? { done: false, value: queue.shift()! } : { done: true, value: undefined },
    ),
    cancel: vi.fn(async () => undefined),
  };
  return {
    response: { ok: true, status: 200, body: { getReader: () => reader } },
    reader,
  };
}

function stubFetch(response: unknown) {
  const fetchMock = vi.fn(async () => response);
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('streamRoom', () => {
  it('tails from the given cursor, parses known frames and ignores keepalives', async () => {
    const fetchMock = stubFetch(
      streamingResponse([
        ': keepalive\n\n',
        `event: room_message\ndata: ${JSON.stringify(ENTRY)}\n\n`,
        'event: round_status\ndata: {"round_id":"round-1","status":"running","slot_cursor":0,"slots":[{"bot_id":"bot-1","name":"Chief of Staff","state":"pending"}]}\n\n',
        'event: room_updated\ndata: {"room_id":"room-1","updated_at":"2026-11-08T12:00:00Z"}\n\n',
      ]).response,
    );
    const onOpen = vi.fn();
    const onMessage = vi.fn();
    const onRoundStatus = vi.fn();
    const onRoomUpdated = vi.fn();
    const onClose = vi.fn();

    streamRoom('room-1', 3, { onOpen, onMessage, onRoundStatus, onRoomUpdated, onClose });

    await waitFor(() => expect(onClose).toHaveBeenCalledWith(null));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('/api/v1/rooms/room-1/stream?last_seq=3');
    expect(init.headers).toMatchObject({
      Accept: 'text/event-stream',
      Authorization: 'Bearer test-token',
    });
    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(onMessage).toHaveBeenCalledWith(ENTRY);
    expect(onRoundStatus).toHaveBeenCalledWith({
      round_id: 'round-1',
      status: 'running',
      slot_cursor: 0,
      slots: [{ bot_id: 'bot-1', name: 'Chief of Staff', state: 'pending' }],
    });
    // The room row changed, so a reader holding its detail is told to read it
    // again; the log itself needs no re-read.
    expect(onRoomUpdated).toHaveBeenCalledWith({
      room_id: 'room-1',
      updated_at: '2026-11-08T12:00:00Z',
    });
  });

  it('keeps a frame whole when its bytes are split across chunks', async () => {
    stubFetch(
      streamingResponse([
        'event: room_mess',
        `age\ndata: ${JSON.stringify(ENTRY)}`,
        '\n\n',
      ]).response,
    );
    const onMessage = vi.fn();

    streamRoom('room-1', 0, { onMessage });
    await waitFor(() => expect(onMessage).toHaveBeenCalledWith(ENTRY));
  });

  it('skips a frame that is not JSON instead of failing the tail', async () => {
    stubFetch(
      streamingResponse(['event: room_message\ndata: not-json\n\n', `event: room_message\ndata: ${JSON.stringify(ENTRY)}\n\n`]).response,
    );
    const onMessage = vi.fn();
    const onClose = vi.fn();

    streamRoom('room-1', 0, { onMessage, onClose });
    await waitFor(() => expect(onMessage).toHaveBeenCalledWith(ENTRY));
    expect(onClose).toHaveBeenCalledWith(null);
  });

  it('reports a refused stream through onClose with the server text', async () => {
    stubFetch({
      ok: false,
      status: 404,
      text: async () => 'Room not found',
    });
    const onOpen = vi.fn();
    const onClose = vi.fn();

    streamRoom('room-1', 0, { onMessage: vi.fn(), onOpen, onClose });

    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
    expect(onOpen).not.toHaveBeenCalled();
    expect((onClose.mock.calls[0][0] as Error).message).toBe('Room not found');
  });

  it('aborts the fetch when the caller stops the tail, without a close callback', async () => {
    let readReject: ((error: Error) => void) | null = null;
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const signal = init?.signal as AbortSignal;
      const read = vi.fn(
        () =>
          new Promise((_resolve, reject) => {
            readReject = reject;
            signal.addEventListener('abort', () =>
              reject(Object.assign(new Error('aborted'), { name: 'AbortError' })),
            );
          }),
      );
      return { ok: true, status: 200, body: { getReader: () => ({ read }) } };
    });
    vi.stubGlobal('fetch', fetchMock);
    const onClose = vi.fn();

    const abort = streamRoom('room-1', 9, { onMessage: vi.fn(), onClose });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    abort();

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect((init.signal as AbortSignal).aborted).toBe(true);
    await waitFor(() => expect(readReject).not.toBeNull());
    expect(onClose).not.toHaveBeenCalled();
  });
});
