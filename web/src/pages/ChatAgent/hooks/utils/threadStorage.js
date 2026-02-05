/**
 * Thread ID localStorage management utilities
 * Provides functions for persisting thread IDs per workspace
 */

/**
 * Storage key prefix for thread IDs
 */
const THREAD_ID_STORAGE_PREFIX = 'workspace_thread_id_';

/**
 * Gets the stored thread ID for a workspace from localStorage
 * @param {string} workspaceId - The workspace ID
 * @returns {string} The stored thread ID or '__default__' if not found
 */
export function getStoredThreadId(workspaceId) {
  if (!workspaceId) return '__default__';
  try {
    const stored = localStorage.getItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`);
    return stored || '__default__';
  } catch (error) {
    console.warn('Failed to read thread ID from localStorage:', error);
    return '__default__';
  }
}

/**
 * Stores the thread ID for a workspace in localStorage
 * @param {string} workspaceId - The workspace ID
 * @param {string} threadId - The thread ID to store
 */
export function setStoredThreadId(workspaceId, threadId) {
  if (!workspaceId || !threadId || threadId === '__default__') return;
  try {
    localStorage.setItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`, threadId);
  } catch (error) {
    console.warn('Failed to save thread ID to localStorage:', error);
  }
}

/**
 * Removes the stored thread ID for a workspace from localStorage
 * Used when a workspace is deleted or thread is invalidated
 * @param {string} workspaceId - The workspace ID
 */
export function removeStoredThreadId(workspaceId) {
  if (!workspaceId) return;
  try {
    localStorage.removeItem(`${THREAD_ID_STORAGE_PREFIX}${workspaceId}`);
  } catch (error) {
    console.warn('Failed to remove thread ID from localStorage:', error);
  }
}
