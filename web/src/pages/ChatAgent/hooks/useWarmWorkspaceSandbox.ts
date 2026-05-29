/**
 * useWarmWorkspaceSandbox
 *
 * Fires `warmWorkspace` on mount when a workspace id is resolved (covers
 * direct URL nav, refresh, and back-button — the click handler in
 * ChatAgent.tsx covers the gallery-click path; both go through the same
 * dedupe primitive).
 *
 * Subscribes to `/api/v1/workspaces/{id}/events` via SSE so each backend
 * status transition (stopped → starting → running) is pushed directly
 * into the React Query cache. Replaces the previous 4-second invalidate-
 * polling loop. The SSE handler closes itself on terminal status or the
 * 600 s server-side cap; backend falls back to server-side polling when
 * Redis is unavailable, so the client never needs an interval sidecar.
 *
 * Returns the current warming state derived from the stream:
 * `false` (not warming / ready) | `'starting'` | `'archived'` (slow restore
 * from cold storage). This lets the entry-time UI show the same two-level
 * spinner the chat path shows, even when a background warm — not a chat
 * message — owns the start.
 */
import { useEffect, useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';

import { getWorkspace, streamWorkspaceEvents } from '../utils/api';
import { patchWorkspaceStatusInCaches, warmWorkspace } from '../utils/warmWorkspace';

const TERMINAL_STATUSES = new Set(['running', 'error', 'deleted']);

export type WarmingState = false | 'starting' | 'archived';

export function useWarmWorkspaceSandbox(
  workspaceId: string | null,
): WarmingState {
  const queryClient = useQueryClient();
  const [warming, setWarming] = useState<WarmingState>(false);

  useEffect(() => {
    if (!workspaceId) return;
    void warmWorkspace(workspaceId, queryClient);
  }, [workspaceId, queryClient]);

  useEffect(() => {
    if (!workspaceId) return;
    setWarming(false);
    // Don't open a stream for a workspace we already know is terminal
    // (running/error/deleted) — it has nothing left to transition to, and the
    // server would only emit the current status and close. Saves a request +
    // a server-side DB read on every navigation to an already-running
    // workspace (the common case). When status is unknown (cold nav), open the
    // stream — warmWorkspace resolves the real status concurrently.
    const known = queryClient.getQueryData<{ status?: string }>(
      queryKeys.workspaces.detail(workspaceId),
    );
    if (known?.status && TERMINAL_STATUSES.has(known.status)) return;

    const controller = new AbortController();
    void (async () => {
      try {
        await streamWorkspaceEvents(
          workspaceId,
          (status, sandboxState) => {
            patchWorkspaceStatusInCaches(queryClient, workspaceId, status);
            setWarming((prev) => {
              if (status !== 'starting') return false;
              if (sandboxState === 'archived') return 'archived';
              // A plain 'starting' must not downgrade an 'archived' we already
              // saw — the refinement event arrives after the generic one.
              return prev === 'archived' ? 'archived' : 'starting';
            });
          },
          controller.signal,
        );
      } catch (err) {
        if (import.meta.env?.DEV) {
          // eslint-disable-next-line no-console
          console.warn(
            '[useWarmWorkspaceSandbox] events stream ended',
            workspaceId,
            err,
          );
        }
      }
      // The stream closed (terminal status, server timeout, or a dropped
      // connection). If we never observed a terminal status, the optimistic
      // 'starting' written by warmWorkspace could be stale — the backend may
      // have reached 'running' after the stream died.
      if (controller.signal.aborted) return;
      const current = queryClient.getQueryData<{ status?: string }>(
        queryKeys.workspaces.detail(workspaceId),
      );
      if (!current?.status || !TERMINAL_STATUSES.has(current.status)) {
        // Fetch the authoritative status and reconcile local state. A bare
        // invalidateQueries can't clear the spinner: in TanStack v5 it only
        // refetches *active* queries, and even on refetch it never updates this
        // hook's local `warming` — so a stream that ends mid-'starting' (600s
        // server timeout or a dropped connection) would pin the spinner on
        // 'starting' indefinitely.
        try {
          const fresh = await queryClient.fetchQuery({
            queryKey: queryKeys.workspaces.detail(workspaceId),
            queryFn: () => getWorkspace(workspaceId),
          });
          const status = (fresh as { status?: string })?.status;
          if (status) patchWorkspaceStatusInCaches(queryClient, workspaceId, status);
          if (status !== 'starting') setWarming(false);
        } catch {
          // Best-effort reconcile; chat-time start surfaces real errors.
          setWarming(false);
        }
      } else {
        // Reached a terminal status — clear the warming spinner.
        setWarming(false);
      }
    })();
    return () => controller.abort();
  }, [workspaceId, queryClient]);

  return warming;
}
