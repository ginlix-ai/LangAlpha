/**
 * What the status frames have said about the computers being prepared.
 *
 * The step a new sandbox is on reaches the page only as a status frame, and
 * the server never stores it, so it is held here by computer id rather than
 * on the list row, which every refetch replaces with the server's copy. The
 * failure reason is the same kind of frame-only value. Both are written by
 * the status fan-out; the step is read where the computer is shown, and the
 * reason by whoever announces the failure.
 */
import { useSyncExternalStore } from 'react';

import { registerAuthReset } from '@/lib/authResets';
import { createValueStore } from '@/lib/valueStore';

import { prepareStageIndex } from '../utils/computerStatus';

interface PrepareFrames {
  stage?: string;
  /** The reason an `error` frame gave, kept until the list shows the failure. */
  failure?: string;
}

const frames = createValueStore<Readonly<Record<string, PrepareFrames>>>({});

registerAuthReset(() => frames.set({}));

function patchFrames(computerId: string, patch: PrepareFrames): void {
  const current = frames.get();
  frames.set({ ...current, [computerId]: { ...current[computerId], ...patch } });
}

/**
 * Hold the step a frame names. Steps only move forward, so a frame for an
 * earlier one, replayed or arriving late, is dropped. A stage this build does
 * not know is held too: it is newer than anything it could be compared with.
 */
export function recordPrepareStage(computerId: string, stage: string): void {
  const held = frames.get()[computerId]?.stage;
  if (held === stage) return;
  const next = prepareStageIndex(stage);
  const prev = prepareStageIndex(held);
  if (next >= 0 && prev >= 0 && next < prev) return;
  patchFrames(computerId, { stage });
}

export function recordPrepareFailure(computerId: string, reason: string): void {
  patchFrames(computerId, { failure: reason });
}

/** The step this machine's preparation is on, if a frame has named one. */
export function usePrepareStage(computerId: string): string | undefined {
  return useSyncExternalStore(frames.subscribe, () => frames.get()[computerId]?.stage);
}

/**
 * Forget what the frames said about machines no longer being prepared, and
 * hand back the failure reasons among it. The list has the outcome by then,
 * and a later preparation of the same machine (a primary whose first start
 * failed) must count its steps from the first again.
 */
export function takePrepareFailures(computerIds: readonly string[]): Map<string, string> {
  const current = frames.get();
  const failures = new Map<string, string>();
  const rest = { ...current };
  for (const id of computerIds) {
    const failure = rest[id]?.failure;
    if (failure !== undefined) failures.set(id, failure);
    delete rest[id];
  }
  if (Object.keys(rest).length !== Object.keys(current).length) frames.set(rest);
  return failures;
}
