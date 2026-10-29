/**
 * Which computer outcomes this tab announces: a preparation that ended, and a
 * spec change that settled.
 *
 * Both are read off the list, since any read can be the one that sees a
 * change land (the poll, window focus, a status reconcile, an invalidation),
 * and both are announced only when this tab saw them under way, so an outcome
 * that settled before load stays quiet. What has been seen under way is kept
 * in a module store rather than in the component that announces it: the page
 * mounts that component in separate mobile and desktop branches, and a
 * remount must neither forget a change in flight nor announce one twice.
 */
import { registerAuthReset } from '@/lib/authResets';
import { createValueStore } from '@/lib/valueStore';
import type { Computer } from '@/types/api';

import { activeSpecChange } from '../components/specChangeUi';
import { isComputerPreparing } from '../utils/computerStatus';

export interface OutcomeWatch {
  /** Machines seen being prepared whose outcome has not been announced yet. */
  preparing: ReadonlySet<string>;
  /**
   * Machines announced ready. One that came up has its sandbox for good, so a
   * row that still reads as preparing afterwards is a read that left before
   * the transition, not a second preparation.
   */
  ready: ReadonlySet<string>;
  /** Spec changes seen in progress, by computer, with the change's start so a
   *  stale row that predates it is not read as its outcome. */
  specChanges: ReadonlyMap<string, string>;
}

export interface PreparationOutcome {
  computer: Computer;
  ready: boolean;
}

export interface ComputerOutcomes {
  watch: OutcomeWatch;
  /** Preparations that ended since the last read. */
  preparations: PreparationOutcome[];
  /** Rows whose watched spec change has settled; each carries its `spec_change`. */
  specChanges: Computer[];
  /** Machines whose preparation frames describe nothing any more: listed and
   *  not being prepared, or watched and gone from the list. */
  idle: string[];
}

const EMPTY_WATCH: OutcomeWatch = { preparing: new Set(), ready: new Set(), specChanges: new Map() };

/** What the announcing host has seen under way, kept across its remounts. */
export const computerOutcomeWatch = createValueStore<OutcomeWatch>(EMPTY_WATCH);

registerAuthReset(() => computerOutcomeWatch.set(EMPTY_WATCH));

/**
 * Read one list against what was seen before it. Each outcome is returned
 * once, because it leaves the watch as it is returned. A preparation that
 * ends running is ready, stopped or error is a failure, and one that settles
 * anywhere else (deleted) or leaves the list is dropped quietly, as is a spec
 * change on a machine that left the list.
 */
export function reduceComputerOutcomes(prev: OutcomeWatch, computers: readonly Computer[]): ComputerOutcomes {
  const preparing = new Set<string>();
  const ready = new Set(prev.ready);
  const specChanges = new Map<string, string>();
  const preparations: PreparationOutcome[] = [];
  const settled: Computer[] = [];
  const idle: string[] = [];
  const listed = new Set<string>();

  for (const computer of computers) {
    const id = computer.computer_id;
    listed.add(id);

    if (isComputerPreparing(computer)) {
      if (!ready.has(id)) preparing.add(id);
    } else {
      idle.push(id);
      if (prev.preparing.has(id)) {
        if (computer.status === 'running') {
          ready.add(id);
          preparations.push({ computer, ready: true });
        } else if (computer.status === 'stopped' || computer.status === 'error') {
          // A failed preparation leaves the row stopped; the error frame says it
          // first, and a stream that missed that frame reconciles to stopped.
          preparations.push({ computer, ready: false });
        }
      }
    }

    const running = activeSpecChange(computer);
    const startedAt = prev.specChanges.get(id);
    if (running) {
      specChanges.set(id, running.started_at);
    } else if (startedAt !== undefined) {
      if (computer.spec_change?.started_at === startedAt) settled.push(computer);
      else specChanges.set(id, startedAt);
    }
  }

  for (const id of prev.preparing) {
    if (!listed.has(id)) idle.push(id);
  }

  return { watch: { preparing, ready, specChanges }, preparations, specChanges: settled, idle };
}
