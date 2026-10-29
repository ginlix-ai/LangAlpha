/**
 * Which computer surface is open, as a tiny external store.
 *
 * The Computers dialog is opened from places that share no parent below the
 * page root: the gallery toolbar, a workspace card's machine line, and the disk
 * warnings in the gallery and in a chat. So the dialog mounts once at the root
 * and every opener writes here instead of threading callbacks down.
 */
import { useSyncExternalStore } from 'react';

import { registerAuthReset } from '@/lib/authResets';
import { holdReload } from '@/lib/reloadHolds';
import { createValueStore } from '@/lib/valueStore';

/**
 * What the Computers dialog opens onto: its rail, with `computerId`'s card in
 * the middle; one machine's details; or the New computer form, which is then
 * the destination (its cancel closes the panel too), with This Mac picked on
 * it when `pick` says so.
 */
export type ComputersPanelView =
  | { view: 'list'; computerId?: string | null }
  | { view: 'details'; computerId: string }
  | { view: 'create'; pick?: 'thisMac' };

export interface ComputersPanelState {
  /** The Computers dialog, null when closed. */
  panel: ComputersPanelView | null;
  /** Counts the openings, so one made while the dialog shows starts it afresh. */
  opening: number;
  /** The change-spec dialog for one machine, opened on its own. */
  specComputerId: string | null;
}

const INITIAL: ComputersPanelState = { panel: null, opening: 0, specComputerId: null };
const store = createValueStore<ComputersPanelState>(INITIAL);
// Survives the page unmount, so a sign-out must not reopen it for the next account.
registerAuthReset(() => set(INITIAL));

// An open request outlives the page that made it (Getting Started opens the
// dialog, then goes to chat), so a stale-build reload waits until it closes.
let releaseReload: (() => void) | null = null;

function set(next: Partial<ComputersPanelState>) {
  const state = { ...store.get(), ...next };
  store.set(state);
  if (state.panel || state.specComputerId) releaseReload ??= holdReload();
  else if (releaseReload) {
    releaseReload();
    releaseReload = null;
  }
}

export function openComputersPanel(panel: ComputersPanelView = { view: 'list' }) {
  set({ panel, opening: store.get().opening + 1 });
}

export function closeComputersPanel() {
  set({ panel: null });
}

export function openComputerSpec(computerId: string) {
  set({ specComputerId: computerId });
}

export function closeComputerSpec() {
  set({ specComputerId: null });
}

export function useComputersPanel(): ComputersPanelState {
  return useSyncExternalStore(store.subscribe, store.get);
}
