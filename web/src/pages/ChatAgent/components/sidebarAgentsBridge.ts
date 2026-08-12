// Bridge carrying the ACTIVE ChatView's subagent registry to the global
// AppSidebar. Desktop nav renders NavigationPanel outside the ChatAgent tree
// (components/Sidebar/AppSidebar), so it can't receive `agents` as props the
// way ChatView's own mobile drawer instance does. The visible ChatView
// publishes the exact values its drawer renders — the same row array identity
// (projected once in useSubagentTabs) and the same select/remove closures —
// so both trees are behaviorally identical by construction: this module only
// distributes, it never derives or re-computes subagent state. Row identity
// is stable across streamed chunks, so publishes (and the re-renders they
// fan out) happen only on genuine row changes.
//
// globalThis-anchored for the same HMR reasons as navExpansionStore. The
// clear is guarded by threadId because up to 5 ChatViews stay mounted and
// React may run the next view's publish before the previous view's cleanup —
// an unguarded clear would drop the fresh slice.

import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import { createEmitter, type Emitter } from '@/lib/emitter';
import type { SidebarAgentRow } from '../session/subagents/subagentStatus';

export interface SidebarAgentsSlice {
  /** Thread the slice belongs to. Consumers must match this against their own
   *  current thread id and treat a mismatch as "no agents". */
  threadId: string;
  /** The same array identity ChatView's own NavigationPanel renders. */
  agents: SidebarAgentRow[];
  activeAgentId: string;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent: (agentId: string) => void;
}

interface BridgeState {
  slice: SidebarAgentsSlice | null;
  emitter: Emitter;
}

const KEY = '__langalpha_sidebar_agents__';
const root = globalThis as unknown as Record<string, unknown>;

const state: BridgeState =
  (root[KEY] as BridgeState | undefined) ??
  ((root[KEY] = { slice: null, emitter: createEmitter() }) as BridgeState);

/** The publisher effect's deps ARE the dedup: every field of the slice is an
 *  effect dependency, so a redundant publish can't reach here. */
export function publishSidebarAgents(slice: SidebarAgentsSlice): void {
  state.slice = slice;
  state.emitter.emit();
}

/** Clears only when the slice still belongs to `threadId` — a late cleanup
 *  from a deactivated ChatView must not clobber the next view's publish. */
export function clearSidebarAgents(threadId: string): void {
  if (state.slice?.threadId !== threadId) return;
  state.slice = null;
  state.emitter.emit();
}

function subscribe(fn: () => void): () => void {
  return state.emitter.subscribe(fn);
}

function getSnapshot(): SidebarAgentsSlice | null {
  return state.slice;
}

export function useSidebarAgents(): SidebarAgentsSlice | null {
  return useSyncExternalStore(subscribe, getSnapshot);
}

registerAuthReset(() => {
  state.slice = null;
  state.emitter.emit();
});
