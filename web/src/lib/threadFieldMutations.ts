/** The thread row fields a composer saves itself (`useThreadFieldSave`). */
export type ThreadField = 'llm_model' | 'subagents_allowed' | 'reasoning_effort' | 'fast_mode';

/**
 * The mutation identity of a save of one field on one thread, so a settle,
 * the turn-end reread and the lifecycle feed can ask whether a save there is
 * still out. Without `field` it matches a save of any field on the thread.
 * Not in `queryKeys`, which holds query keys only: mutation keys live in their
 * own cache, which query prefix invalidation never reaches.
 */
export function threadFieldMutationKey(threadId: string, field?: ThreadField) {
  return field ? ['thread-field', threadId, field] : ['thread-field', threadId];
}
