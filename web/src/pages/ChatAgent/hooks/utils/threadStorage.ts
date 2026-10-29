/**
 * Thread ID localStorage management utilities
 * Provides functions for persisting thread IDs per workspace
 */
import { userLocalStorage } from '@/lib/userStorage';

/**
 * Storage key prefix for thread IDs
 */
const THREAD_ID_STORAGE_PREFIX = 'workspace_thread_id_';

/**
 * Gets the stored thread ID for a workspace from localStorage
 */
export function getStoredThreadId(workspaceId: string): string {
  if (!workspaceId) return '__default__';
  const stored = userLocalStorage.getItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`);
  return stored || '__default__';
}

/**
 * Stores the thread ID for a workspace in localStorage
 */
export function setStoredThreadId(workspaceId: string, threadId: string): void {
  if (!workspaceId || !threadId || threadId === '__default__') return;
  userLocalStorage.setItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`, threadId);
}

/**
 * Removes the stored thread ID for a workspace from localStorage
 * Used when a workspace is deleted or thread is invalidated
 */
export function removeStoredThreadId(workspaceId: string): void {
  if (!workspaceId) return;
  userLocalStorage.removeItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`);
}

