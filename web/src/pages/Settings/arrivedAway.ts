import type { SettingsView } from './sections';
import { registerAuthReset } from '@/lib/authResets';

// The history entry last shown as a portal page in Settings. A window narrowed
// past the phone width remounts Settings, since Main keys its desktop routes
// and not its phone ones, so this has to outlive the mount. Read it only
// through `wasFramed`: the React Compiler takes a module binding read during
// render for a constant.
let framedEntry: string | null = null;
registerAuthReset(() => {
  framedEntry = null;
});

function wasFramed(entry: string): boolean {
  return framedEntry === entry;
}

/** Notes the history entry Settings is showing framed. */
export function noteFramed(entry: string): void {
  framedEntry = entry;
}

/**
 * Whether a portal page Settings cannot show here goes to the portal itself.
 * Not when this same history entry was just shown framed: the window was
 * narrowed on it, and the app stays, on Settings' own first section. Decided
 * for each entry, so Back or a link to a portal page on a phone still goes.
 */
export function arrivesAway(view: SettingsView, entry: string): boolean {
  return view.kind === 'away' && !wasFramed(entry);
}
