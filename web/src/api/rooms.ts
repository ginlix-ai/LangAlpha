/**
 * Rooms: a shared log whose members are bots.
 *
 * A room is one ordered `room_log` plus the bots that read it. A user message
 * opens a round, and the round hands each member the part it has not read yet;
 * every member answers in its own thread, and only the log is shared. The REST
 * calls here are the whole HTTP surface the server exposes (see
 * `src/server/app/rooms.py`); the live tail is an SSE stream read through raw
 * `fetch`, exactly like the app's other long-lived streams, because the shared
 * API client reads whole bodies and never gives back a stream.
 *
 * The stream is table-sourced, not a Redis fan-out: a client that misses a
 * beat reconnects with its last seq and the server replays the gap. That is why
 * `streamRoom` reports the highest seq it saw through `onMessage` callers'
 * bookkeeping rather than owning a cursor.
 */
import { api } from '@/api/client';
import { getAuthHeaders } from '@/lib/authToken';
import { watchStreamSilence } from '@/lib/streamSilence';

/** Who answers a round: every member, only the @mentioned ones, or one member. */
export type RoomResponder = 'everyone' | 'mentions' | `member:${string}`;

/** A room keeps its log readable when archived, but refuses new messages. */
export type RoomStatus = 'active' | 'archived';

/** A member answers only while its bot is active; a paused bot is skipped. */
export type RoomMemberStatus = 'active' | 'paused';

/** The host speaks first and may take extra turns in a round. */
export type RoomMemberRole = 'host' | 'member';

export type RoomRoundRunStatus = 'queued' | 'running' | 'finished' | 'failed';

export type RoomSlotState = 'pending' | 'done' | 'skipped' | 'missed' | 'needs_input';

export type RoomSenderType = 'user' | 'bot' | 'system';

export type RoomLogKind = 'message' | 'status' | 'pass' | 'system';

/** One room as the list returns it; members need the detail read. */
export interface RoomInfo {
  room_id: string;
  workspace_id: string;
  name: string;
  host_bot_id: string | null;
  responder: string;
  bulletin: string;
  status: string;
  created_at: string | null;
}

/** A member as the detail read returns it, with its bot's live status. */
export interface RoomMemberInfo {
  bot_id: string;
  name: string;
  role: string;
  status: string;
  room_role: string;
  /** The bot's avatar dict (`BotMark` reads it); absent on older servers. */
  avatar?: Record<string, unknown>;
}

export interface RoomView extends RoomInfo {
  members: RoomMemberInfo[];
}

export interface RoomListResponse {
  rooms: RoomInfo[];
  /** The server's per-room cap, for the create dialog to state. */
  max_bots: number;
}

export interface RoomLogEntry {
  seq: number;
  room_id: string;
  round_id: string | null;
  sender_type: string;
  sender_id: string;
  kind: string;
  content: string;
  created_at: string | null;
}

export interface RoomLogResponse {
  entries: RoomLogEntry[];
  latest_seq: number;
}

/** The server's acknowledgement of a user message; the round it opened may be new. */
export interface RoomSendResponse {
  message_seq: number;
  round_id: string | null;
  round_status: string | null;
}

/** One member's turn in the round, as the round row stores it. */
export interface RoomRoundSlot {
  bot_id: string;
  name?: string;
  state?: string;
}

/** The `round_status` SSE frame; `slots` is the round's whole walk order. */
export interface RoomRoundStatus {
  round_id: string;
  status: string;
  slot_cursor: number | null;
  slots: RoomRoundSlot[] | null;
}

/** The `room_updated` SSE frame: the room row itself changed (name, responder,
 *  bulletin or members), so a reader holding its detail should read it again.
 *  The log is untouched; this frame never asks for it. */
export interface RoomUpdated {
  room_id: string;
  updated_at: string | null;
}

export interface RoomCreateInput {
  workspace_id: string;
  name: string;
  member_bot_ids: string[];
  host_bot_id?: string | null;
  responder?: RoomResponder;
  bulletin?: string;
}

/** An absent field is left alone; `status: 'archived'` retires the room. */
export interface RoomUpdateInput {
  name?: string;
  responder?: RoomResponder;
  bulletin?: string;
  status?: RoomStatus;
}

export interface RoomMemberDeleteResponse {
  ok: boolean;
  bot_id: string;
}

/** `GET /api/v1/rooms`: one workspace's rooms, or every room when none is named. */
export async function getRooms(workspaceId: string | null = null): Promise<RoomListResponse> {
  const { data } = await api.get<RoomListResponse>(
    '/api/v1/rooms',
    workspaceId ? { params: { workspace_id: workspaceId } } : undefined,
  );
  return data;
}

export async function getRoom(roomId: string): Promise<RoomView> {
  const { data } = await api.get<RoomView>(`/api/v1/rooms/${encodeURIComponent(roomId)}`);
  return data;
}

export async function createRoom(input: RoomCreateInput): Promise<RoomView> {
  const { data } = await api.post<RoomView>('/api/v1/rooms', input);
  return data;
}

/** Patch the room itself; the answer carries no members, so views merge it in. */
export async function updateRoom(roomId: string, patch: RoomUpdateInput): Promise<RoomInfo> {
  const { data } = await api.patch<RoomInfo>(
    `/api/v1/rooms/${encodeURIComponent(roomId)}`,
    patch,
  );
  return data;
}

/** Add one bot; the answer is the room view, so the member list lands whole. */
export async function addRoomMember(roomId: string, botId: string): Promise<RoomView> {
  const { data } = await api.post<RoomView>(
    `/api/v1/rooms/${encodeURIComponent(roomId)}/members`,
    { bot_id: botId },
  );
  return data;
}

export async function removeRoomMember(
  roomId: string,
  botId: string,
): Promise<RoomMemberDeleteResponse> {
  const { data } = await api.delete<RoomMemberDeleteResponse>(
    `/api/v1/rooms/${encodeURIComponent(roomId)}/members/${encodeURIComponent(botId)}`,
  );
  return data;
}

/** A user message opens a round (or joins the running one). */
export async function sendRoomMessage(roomId: string, content: string): Promise<RoomSendResponse> {
  const { data } = await api.post<RoomSendResponse>(
    `/api/v1/rooms/${encodeURIComponent(roomId)}/messages`,
    { content },
  );
  return data;
}

/** One page of the log, oldest first after `sinceSeq`. */
export async function getRoomLog(
  roomId: string,
  sinceSeq = 0,
  limit = 100,
): Promise<RoomLogResponse> {
  const { data } = await api.get<RoomLogResponse>(
    `/api/v1/rooms/${encodeURIComponent(roomId)}/log`,
    { params: { since_seq: sinceSeq, limit } },
  );
  return data;
}

/** The server's keepalive interval for the room stream; past three beats the
 *  socket is gone, not quiet. */
const ROOM_STREAM_BEAT_MS = 20_000;

/** A stream that the server refused. The status is the one a caller's retry
 *  decision reads: a 4xx is the room itself (gone, not the caller's), so
 *  reopening the same tail only gets the same refusal. */
export class RoomStreamError extends Error {
  readonly status: number | null;

  constructor(message: string, status: number | null) {
    super(message);
    this.name = 'RoomStreamError';
    this.status = status;
  }
}

/** The HTTP status a stream failure carried, when it carried one. */
export function roomStreamErrorStatus(error: unknown): number | null {
  return error instanceof RoomStreamError ? error.status : null;
}

export interface RoomStreamHandlers {
  /** The response headers arrived and the body is about to be read. */
  onOpen?: () => void;
  /** One log row, in seq order. */
  onMessage: (entry: RoomLogEntry) => void;
  /** The round's state changed (or is being reported on reconnect). */
  onRoundStatus?: (status: RoomRoundStatus) => void;
  /** The room row changed; a reader holding its detail should read it again. */
  onRoomUpdated?: (update: RoomUpdated) => void;
  /**
   * The tail ended — the server closed it, the link died, or the caller
   * aborted. A caller that wants to keep following reopens with the last seq
   * it saw; `error` is null when the end was its own abort.
   */
  onClose?: (error: Error | null) => void;
}

/** The stream URL for one room, from its cursor. */
function roomStreamUrl(roomId: string, lastSeq: number): string {
  const path = `/api/v1/rooms/${encodeURIComponent(roomId)}/stream?last_seq=${Math.max(
    0,
    Math.floor(lastSeq),
  )}`;
  const base = api.defaults.baseURL.replace(/\/+$/, '');
  return base ? `${base}${path}` : path;
}

/** Decode one `event:`/`data:` SSE frame. Unknown events are ignored so the
 *  server can add frames without breaking this client. */
function dispatchFrame(
  event: string,
  data: string,
  handlers: Pick<RoomStreamHandlers, 'onMessage' | 'onRoundStatus' | 'onRoomUpdated'>,
): void {
  if (!data) return;
  let payload: unknown;
  try {
    payload = JSON.parse(data);
  } catch {
    // A truncated frame is not worth failing the stream over; the next replay
    // from the last seq sends it again.
    return;
  }
  if (event === 'room_message' && payload && typeof payload === 'object') {
    handlers.onMessage(payload as RoomLogEntry);
  } else if (event === 'round_status' && payload && typeof payload === 'object') {
    handlers.onRoundStatus?.(payload as RoomRoundStatus);
  } else if (event === 'room_updated' && payload && typeof payload === 'object') {
    handlers.onRoomUpdated?.(payload as RoomUpdated);
  }
}

/**
 * Tail one room's shared log over raw `fetch`, from `lastSeq`.
 *
 * Returns the abort function the caller keeps for the life of the tail. The
 * tail itself is created with its own AbortController: an abort, a dropped
 * link and the server closing the stream all reach `onClose`, where the caller
 * decides whether to reopen from its cursor (see `useRoomStream`).
 */
export function streamRoom(
  roomId: string,
  lastSeq: number,
  handlers: RoomStreamHandlers,
): () => void {
  const controller = new AbortController();
  let stopped = false;
  let eventName = '';
  let dataLines: string[] = [];
  let buffered = '';

  const finish = (error: Error | null) => {
    if (stopped) return;
    stopped = true;
    silence.stop();
    handlers.onClose?.(error);
  };

  // The server beats every 20s; silence past three beats is a dead socket, not
  // a quiet room. Aborting the fetch makes the read reject, and the catch below
  // reports it as a close so the caller reconnects from its cursor.
  const silence = watchStreamSilence(ROOM_STREAM_BEAT_MS, () => controller.abort());

  const feed = (chunk: string) => {
    buffered += chunk;
    let newline = buffered.indexOf('\n');
    while (newline !== -1) {
      let line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      newline = buffered.indexOf('\n');
      if (line.endsWith('\r')) line = line.slice(0, -1);
      if (line === '') {
        if (dataLines.length > 0) {
          const frame = dataLines.join('\n');
          const name = eventName || 'message';
          eventName = '';
          dataLines = [];
          dispatchFrame(name, frame, handlers);
        } else {
          eventName = '';
        }
      } else if (line.startsWith(':')) {
        // `: keepalive` — bytes count as a beat, handled by `silence.heard()`.
      } else if (line.startsWith('event:')) {
        eventName = line.slice(6).trim();
      } else if (line.startsWith('data:')) {
        dataLines.push(line.slice(5).replace(/^ /, ''));
      }
    }
  };

  const run = async () => {
    try {
      const auth = await getAuthHeaders();
      const response = await fetch(roomStreamUrl(roomId, lastSeq), {
        method: 'GET',
        headers: { Accept: 'text/event-stream', ...auth },
        signal: controller.signal,
      });
      if (!response.ok) {
        // The server explains a refused stream in the body's `detail`; the
        // status is what the caller's retry decision reads.
        const text = await response.text().catch(() => '');
        throw new RoomStreamError(text || `HTTP error! status: ${response.status}`, response.status);
      }
      handlers.onOpen?.();
      const reader = response.body?.getReader();
      if (!reader) throw new Error('The room stream had no body.');
      const decoder = new TextDecoder();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        silence.heard();
        feed(decoder.decode(value, { stream: true }));
      }
      // The server ends the tail only when it is shutting down; the stream is
      // otherwise open for the life of the page. Either way the caller reopens.
      finish(null);
    } catch (error) {
      if (stopped) return;
      // An abort from `silence` is our own liveness verdict, not a thrown
      // network error; report it as a plain close so the caller reconnects.
      if (controller.signal.aborted) {
        finish(null);
        return;
      }
      finish(error instanceof Error ? error : new Error('The room stream failed.'));
    }
  };

  void run();

  return () => {
    stopped = true;
    silence.stop();
    controller.abort();
  };
}
