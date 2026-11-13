/**
 * The agent's threads that want attention, as the panel lists them.
 */
import { useMemo, useSyncExternalStore } from 'react';
import { getThreadAttention, subscribeThreadLifecycle } from '@/lib/threadLifecycle/store';

export interface AgentAttention {
  needsInput: string[];
  running: string[];
  unseen: string[];
}

/** The threads waiting on the user, running, or finished and unread. */
export function useAgentAttention(): AgentAttention {
  const { needsInput, running, unseen } = useSyncExternalStore(subscribeThreadLifecycle, getThreadAttention);
  return useMemo(() => ({ needsInput: [...needsInput], running: [...running], unseen: [...unseen] }), [needsInput, running, unseen]);
}
