/**
 * React Query hooks for Rooms.
 *
 * The room list and one room's detail are ordinary reads. The log is the one
 * piece of server state a page keeps appending to: sends land in it optimistically
 * and the SSE tail appends the persisted rows. So the log query merges what the
 * server returned with whatever the cache already holds instead of replacing it
 * — the log is append-only, and a page that cached a row from the stream must not
 * lose it when a refetch lands.
 *
 * The stream itself is a hook, not a query: it is one long-lived fetch with a
 * cursor the caller cannot see, and its reconnect is from that cursor, not from
 * a cache entry.
 */
import { useEffect, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
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
  type RoomCreateInput,
  type RoomLogEntry,
  type RoomRoundStatus,
  type RoomUpdated,
  type RoomUpdateInput,
  type RoomView,
} from '@/api/rooms';

/** The HTTP status a stream failure carried (the class lives in api/rooms so
 *  the transport and this read agree on one name). */
function streamStatus(error: Error | null): number | null {
  const status = (error as { status?: unknown } | null)?.status;
  return typeof status === 'number' ? status : null;
}

/** The server's log page cap; reading the tail means one page, not all of it. */
const ROOM_LOG_PAGE_SIZE = 200;

/** A log row the client drew before the server answered the send. */
export interface PendingRoomLogEntry extends RoomLogEntry {
  pending: true;
}

/** What the log cache holds: server rows, plus at most a few optimistic ones. */
export type RoomLogCacheEntry = RoomLogEntry & { pending?: true };

let pendingSeq = 0;

/** A seq no server row can hold, so an optimistic row sorts after every real one. */
function nextPendingSeq(): number {
  pendingSeq -= 1;
  return pendingSeq;
}

/** Real rows in seq order, then optimistic ones in the order they were sent. */
function orderEntries(entries: RoomLogCacheEntry[]): RoomLogCacheEntry[] {
  const real = entries.filter((entry) => !entry.pending).sort((a, b) => a.seq - b.seq);
  const pending = entries.filter((entry) => entry.pending);
  return [...real, ...pending];
}

/**
 * One event more, without duplicating a row the cache already has. A server row
 * that lands after its optimistic twin (same speaker and text) replaces it, so
 * the message is drawn once whether the SSE beat the send's answer or not.
 */
function appendEntry(
  previous: RoomLogCacheEntry[] | undefined,
  entry: RoomLogCacheEntry,
): RoomLogCacheEntry[] {
  const entries = previous ?? [];
  if (!entry.pending && entries.some((cached) => !cached.pending && cached.seq === entry.seq)) {
    return entries;
  }
  const withoutTwin = entry.pending
    ? entries
    : entries.filter(
        (cached) =>
          !(
            cached.pending &&
            cached.sender_type === entry.sender_type &&
            cached.content === entry.content
          ),
      );
  return orderEntries([...withoutTwin, entry]);
}

/** A fetched page merged over what the stream already appended; nothing is lost. */
function mergeLogPage(
  previous: RoomLogCacheEntry[] | undefined,
  page: RoomLogEntry[],
): RoomLogCacheEntry[] {
  const real = new Map<number, RoomLogEntry>();
  for (const entry of previous ?? []) {
    if (!entry.pending) real.set(entry.seq, entry);
  }
  for (const entry of page) real.set(entry.seq, entry);
  const pending = (previous ?? []).filter((entry) => entry.pending);
  return [...[...real.values()].sort((a, b) => a.seq - b.seq), ...pending];
}

/**
 * The room's log from the top, or its last page when the room is older than
 * one. The endpoint reads forward from `since_seq` only, so the tail is found
 * through the `latest_seq` it returns alongside the first page.
 */
async function loadRoomLog(roomId: string): Promise<RoomLogEntry[]> {
  const first = await getRoomLog(roomId, 0, ROOM_LOG_PAGE_SIZE);
  if (first.latest_seq <= ROOM_LOG_PAGE_SIZE) return first.entries;
  const tail = await getRoomLog(roomId, first.latest_seq - ROOM_LOG_PAGE_SIZE, ROOM_LOG_PAGE_SIZE);
  return tail.entries;
}

/** One workspace's rooms, or every room when `workspaceId` is null. */
export function useRooms(workspaceId: string | null = null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.rooms.list(workspaceId),
    queryFn: () => getRooms(workspaceId),
    enabled,
    staleTime: 30_000,
  });
}

/** One room with its members; disabled until an id is in hand. */
export function useRoom(roomId: string | null) {
  return useQuery({
    queryKey: queryKeys.rooms.detail(roomId ?? ''),
    queryFn: () => getRoom(roomId!),
    enabled: !!roomId,
    staleTime: 30_000,
  });
}

/** The room's shared log. Appends from the stream and from sends land here. */
export function useRoomLog(roomId: string | null) {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: queryKeys.rooms.log(roomId ?? ''),
    queryFn: async () => {
      const page = await loadRoomLog(roomId!);
      return mergeLogPage(
        queryClient.getQueryData<RoomLogCacheEntry[]>(queryKeys.rooms.log(roomId!)),
        page,
      );
    },
    enabled: !!roomId,
    // Append-only, and the stream is the live read: nothing to re-read on
    // focus. A send invalidates one key to land the server's copy.
    staleTime: Infinity,
  });
}

export function useCreateRoom() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: RoomCreateInput) => createRoom(input),
    onSuccess: (room) => {
      queryClient.setQueryData(queryKeys.rooms.detail(room.room_id), room);
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.lists() });
    },
  });
}

export function useUpdateRoom() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roomId, patch }: { roomId: string; patch: RoomUpdateInput }) =>
      updateRoom(roomId, patch),
    onSuccess: (room, { roomId }) => {
      const key = queryKeys.rooms.detail(roomId);
      const current = queryClient.getQueryData<RoomView>(key);
      // The patch answer is a RoomInfo with no members; keep the ones the
      // detail read already had instead of drawing the room empty.
      if (current) queryClient.setQueryData<RoomView>(key, { ...current, ...room, members: current.members });
      else queryClient.invalidateQueries({ queryKey: key });
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.lists() });
    },
  });
}

export function useAddRoomMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roomId, botId }: { roomId: string; botId: string }) =>
      addRoomMember(roomId, botId),
    onSuccess: (room) => {
      queryClient.setQueryData(queryKeys.rooms.detail(room.room_id), room);
    },
  });
}

export function useRemoveRoomMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roomId, botId }: { roomId: string; botId: string }) =>
      removeRoomMember(roomId, botId),
    onSuccess: (_answer, { roomId, botId }) => {
      const key = queryKeys.rooms.detail(roomId);
      queryClient.setQueryData<RoomView>(key, (current) =>
        current
          ? { ...current, members: current.members.filter((member) => member.bot_id !== botId) }
          : current,
      );
    },
  });
}

/**
 * Send the user's message, drawn before the server answers. The optimistic row
 * carries a negative seq so it sorts after the real log; the mutation's answer
 * turns it into a real row, the SSE tail confirms it, and a refused send takes
 * it back out (the caller restores the composer from its own draft).
 */
export function useSendRoomMessage() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roomId, content }: { roomId: string; content: string }) =>
      sendRoomMessage(roomId, content),
    onMutate: async ({ roomId, content }) => {
      const key = queryKeys.rooms.log(roomId);
      await queryClient.cancelQueries({ queryKey: key });
      const optimistic: PendingRoomLogEntry = {
        seq: nextPendingSeq(),
        room_id: roomId,
        round_id: null,
        sender_type: 'user',
        // The room's user is the person reading this page; the row only needs
        // to say "you", and the server's own row replaces it with the real id.
        sender_id: 'user',
        kind: 'message',
        content,
        created_at: new Date().toISOString(),
        pending: true,
      };
      queryClient.setQueryData<RoomLogCacheEntry[]>(key, (previous) =>
        appendEntry(previous, optimistic),
      );
      return { pendingSeq: optimistic.seq };
    },
    onSuccess: (answer, { roomId }, context) => {
      const key = queryKeys.rooms.log(roomId);
      queryClient.setQueryData<RoomLogCacheEntry[]>(key, (previous) => {
        if (!previous) return previous;
        const mapped = previous.map((entry) =>
          entry.seq === context?.pendingSeq
            ? {
                ...entry,
                seq: answer.message_seq,
                round_id: answer.round_id,
                pending: undefined,
              }
            : entry,
        );
        // The SSE tail can have drawn the persisted row before this answer
        // arrived; the answer's twin of it is the same row, drawn once.
        const seen = new Set<number>();
        const deduped = mapped.filter((entry) => {
          if (entry.pending) return true;
          if (seen.has(entry.seq)) return false;
          seen.add(entry.seq);
          return true;
        });
        return orderEntries(deduped);
      });
      // The server's page is the truth if the answer names a seq the stream
      // has not drawn yet.
      queryClient.invalidateQueries({ queryKey: key });
    },
    onError: (_error, { roomId }, context) => {
      const key = queryKeys.rooms.log(roomId);
      queryClient.setQueryData<RoomLogCacheEntry[]>(key, (previous) =>
        previous?.filter((entry) => entry.seq !== context?.pendingSeq),
      );
      // A send refused because the room was archived (or its members changed)
      // is the room row's news: re-read it so the composer and controls stop
      // offering a write the server just refused.
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.detail(roomId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.lists() });
    },
  });
}

export interface RoomStreamState {
  /** The last round_status frame: who is speaking, and whether it is over. */
  round: RoomRoundStatus | null;
  /** Whether the tail is currently connected. */
  live: boolean;
  /** The tail opened at least once, so a stop is a reconnect rather than a
   *  first connection still in progress. */
  connectedOnce: boolean;
  /** A refusal that asking again cannot fix (4xx): the tail stopped. */
  error: Error | null;
  /** Reopen from the cursor after `error`, or after giving up. */
  retry: () => void;
}

/** How long to wait before reopening, bounded. The first retry is quick; a
 *  long outage backs off to ten seconds. */
const reconnectDelay = (attempt: number) => Math.min(1000 * 2 ** Math.min(attempt, 4), 10_000);

/**
 * Follow one room's log and round over SSE, from `lastSeq`.
 *
 * The tail reopens itself from the highest seq it has seen (the table-sourced
 * replay makes that gapless), with a bounded backoff, and appends every
 * `room_message` into the log cache. A 4xx refusal (the room is gone, or not
 * this caller's) is not retried: it is exposed as `error` with `retry` for the
 * reader. It stops when the id goes away or the caller disables it.
 */
export function useRoomStream(roomId: string | null, lastSeq: number, enabled = true): RoomStreamState {
  const queryClient = useQueryClient();
  // Both values carry the room they belong to, so a room switch simply stops
  // matching instead of needing a synchronous reset inside the effect.
  const [roundState, setRoundState] = useState<{ roomId: string; round: RoomRoundStatus } | null>(null);
  const [liveRoom, setLiveRoom] = useState<string | null>(null);
  const [connectedRoom, setConnectedRoom] = useState<string | null>(null);
  const [streamError, setStreamError] = useState<{ roomId: string; error: Error } | null>(null);
  const [retryNonce, setRetryNonce] = useState(0);
  const cursorRef = useRef(lastSeq);
  // The last room_updated a tail announced. A reconnect re-announces the row
  // as it stands; only a genuinely newer one is worth re-reading.
  const roomUpdatedRef = useRef<Record<string, string | null>>({});
  // Raised in an effect, not in render: the tail reads this ref from a timer
  // callback, and a render-time write is what the compiler cannot memoize.
  useEffect(() => {
    cursorRef.current = Math.max(cursorRef.current, lastSeq);
  }, [lastSeq]);

  useEffect(() => {
    if (!roomId || !enabled) return;
    let closed = false;
    let abort: (() => void) | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let attempt = 0;

    const invalidateRoom = (update: RoomUpdated) => {
      const last = roomUpdatedRef.current[update.room_id];
      if (last !== undefined && last === update.updated_at) return;
      roomUpdatedRef.current[update.room_id] = update.updated_at;
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.detail(update.room_id) });
      // The list names the room and its responder too; a rename or a member
      // change reaches it as well as the detail.
      queryClient.invalidateQueries({ queryKey: queryKeys.rooms.lists() });
    };

    const open = () => {
      if (closed) return;
      abort = streamRoom(roomId, cursorRef.current, {
        onOpen: () => {
          attempt = 0;
          setLiveRoom(roomId);
          setConnectedRoom(roomId);
          setStreamError(null);
        },
        onMessage: (entry) => {
          cursorRef.current = Math.max(cursorRef.current, entry.seq);
          const key = queryKeys.rooms.log(roomId);
          queryClient.setQueryData<RoomLogCacheEntry[]>(key, (previous) =>
            appendEntry(previous, entry),
          );
        },
        onRoundStatus: (status) => {
          setRoundState({ roomId, round: status });
        },
        // The room row changed (name, responder, bulletin or members), so the
        // detail and the list are stale. The log did not move, so it is not
        // read again.
        onRoomUpdated: invalidateRoom,
        onClose: (error) => {
          setLiveRoom((current) => (current === roomId ? null : current));
          if (closed) return;
          const status = streamStatus(error);
          // A 4xx is the room (gone, archived, not this caller's): reopening
          // the same tail would only be refused the same way.
          if (status !== null && status >= 400 && status < 500) {
            setStreamError({ roomId, error: error as Error });
            return;
          }
          attempt += 1;
          retryTimer = setTimeout(open, reconnectDelay(attempt));
        },
      });
    };
    open();

    return () => {
      closed = true;
      if (retryTimer) clearTimeout(retryTimer);
      abort?.();
    };
  }, [queryClient, roomId, enabled, retryNonce]);

  const error = streamError && streamError.roomId === roomId ? streamError.error : null;
  return {
    round: roundState && roundState.roomId === roomId ? roundState.round : null,
    live: liveRoom === roomId && !error,
    connectedOnce: connectedRoom === roomId,
    error,
    retry: () => {
      if (!roomId) return;
      setStreamError(null);
      setRetryNonce((nonce) => nonce + 1);
    },
  };
}
