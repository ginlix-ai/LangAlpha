/**
 * Shared event utilities for history and streaming event handlers.
 */

/**
 * Normalize old action values to new ones for backward compatibility.
 * Old persisted events may use 'spawned', 'resumed'.
 */
export function normalizeAction(raw: string | undefined): string {
  if (raw === 'spawned') return 'init';
  if (raw === 'steering_accepted') return 'update';
  if (raw === 'resumed') return 'resume';
  return raw || 'init';
}
