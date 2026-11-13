/**
 * Which computer surface is open, as a tiny external store.
 *
 * The Computers dialog is opened from places that share no parent below the
 * page root: the gallery toolbar, a workspace card's machine line, and the disk
 * warnings in the gallery and in a chat. So the dialog mounts once at the root
 * and every opener writes here instead of threading callbacks down.
 */
import { useEffect, useSyncExternalStore } from 'react';

import { registerAuthReset } from '@/lib/authResets';
import type { ResourceTier } from '@/types/api';
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
  /** What the opener needs of it, where it needs a size. */
  specRequest: SpecRequest | null;
}

/**
 * An opener that needs a size: the dialog opens on `tier`, and where the
 * plan keeps that size closed, says why (`leadKey`) and links the plan page
 * at `planUrl`, which sends the user back.
 */
export interface SpecRequest {
  tier: ResourceTier;
  planUrl: string | null;
  leadKey: string;
  /** The user is back from the plan page, so the server reads the plan again
   *  before it refuses the change. */
  planChanged?: boolean;
}

/** The query that opens the change-spec dialog on a computer at the size
 *  Claude Code runs at, wherever a page mounts the dialog's host: how a link
 *  that cannot run code (a refusal kept in a thread, the plan page's way
 *  back) asks for it. */
export const RESIZE_PARAM = 'resize';

/** Beside `RESIZE_PARAM` on the plan page's way back only: the plan may
 *  have just changed, so the server reads it again rather than answer from
 *  what it read before the trip. */
export const PLAN_CHANGED_PARAM = 'plan_changed';

const INITIAL: ComputersPanelState = { panel: null, opening: 0, specComputerId: null, specRequest: null };
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

export function openComputerSpec(computerId: string, request: SpecRequest | null = null) {
  set({ specComputerId: computerId, specRequest: request });
}

export function closeComputerSpec() {
  set({ specComputerId: null, specRequest: null });
}

/** A page that mounts the dialogs' host. `planInPlace`: it holds nothing a
 *  new tab would save, so the plan page opens in its tab and the way back
 *  is one tab. */
interface ComputersHost {
  planInPlace: boolean;
}

// The mounted hosts. A request made where none is mounted (a composer on the
// dashboard) would show nothing, then open unasked on the next page that
// mounts one.
const hosts: ComputersHost[] = [];

/** Registers the calling host while it stays mounted. */
export function useComputersHost(planInPlace: boolean) {
  useEffect(() => {
    const host = { planInPlace };
    hosts.push(host);
    return () => {
      hosts.splice(hosts.indexOf(host), 1);
    };
  }, [planInPlace]);
}

/** Whether a page that shows the computer dialogs is mounted. */
export function computersHostMounted(): boolean {
  return hosts.length > 0;
}

/** Whether the page on screen opens the plan page in its own tab. */
export function plansOpenInPlace(): boolean {
  return hosts.some((host) => host.planInPlace);
}

export function useComputersPanel(): ComputersPanelState {
  return useSyncExternalStore(store.subscribe, store.get);
}
