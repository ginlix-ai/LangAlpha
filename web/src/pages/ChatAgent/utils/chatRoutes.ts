/**
 * The chat's own paths. Ids go in as given: thread and workspace ids are
 * UUIDs, and a caller holding an id from elsewhere encodes it itself.
 */

/** A thread, or one of its tasks opened in it. */
export function threadPath(threadId: string, taskId?: string): string {
  return taskId ? `/chat/t/${threadId}/${taskId}` : `/chat/t/${threadId}`;
}

/** A workspace's thread list. */
export function workspacePath(workspaceId: string): string {
  return `/chat/${workspaceId}`;
}
