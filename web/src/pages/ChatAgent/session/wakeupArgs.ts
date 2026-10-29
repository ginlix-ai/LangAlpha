/**
 * What a `schedule_wakeup` call's arguments ask for, read by the rule the
 * server marks a quiet turn changed by (`_changes` in the wakeup tools), so
 * the progress line, the quiet fold and the server read one call alike. No
 * imports: the tool display table loads this without the session stack.
 */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Whether the call lists anything under `key` (`triggers` or `cancel`). A
 *  list may arrive as its JSON text, and text that is not JSON after all
 *  still counts when it is not empty: the server reads it the same way. */
export function wakeupArgListed(args: unknown, key: 'triggers' | 'cancel'): boolean {
  if (!isRecord(args)) return false;
  let value = args[key];
  if (typeof value === 'string' && value.trim().startsWith('[')) {
    try {
      value = JSON.parse(value);
    } catch {
      // Not JSON after all: the text stands as given.
    }
  }
  if (Array.isArray(value)) return value.length > 0;
  if (isRecord(value)) return Object.keys(value).length > 0;
  if (typeof value === 'string') return value.length > 0;
  return !!value;
}

/** The line the call ends a check quietly with, or '' when it ends none. */
export function quietArg(args: unknown): string {
  return isRecord(args) && typeof args.quiet === 'string' ? args.quiet.trim() : '';
}
