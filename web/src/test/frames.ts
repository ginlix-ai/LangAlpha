import { vi } from 'vitest';

/**
 * Animation frames run by hand, for code that waits on the frame queue
 * (pages/ChatAgent/session/stream/frameQueue).
 *
 * The queue keeps one frame callback and the time of its last drain for the
 * whole module, so two things carry across tests: frame times must never go
 * back, and a test must not end with a frame requested but never run, or the
 * next test's queue waits on a callback that will not come. `settleFrames`
 * after each test is what keeps the second.
 */
let frames: FrameRequestCallback[] = [];
let visibility: DocumentVisibilityState = 'visible';
let clock = 0;

/** Holds frame callbacks until `runFrame`, on a page that is visible until `setVisibility` says otherwise. */
export function mockFrames(): void {
  frames = [];
  visibility = 'visible';
  vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => frames.push(cb));
  vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
  vi.spyOn(document, 'hidden', 'get').mockImplementation(() => visibility !== 'visible');
}

/** Runs the callbacks due, `interval` ms after the last frame: a 60 Hz display's unless given. */
export function runFrame(interval = 1000 / 60): void {
  clock += interval;
  const due = frames;
  frames = [];
  for (const cb of due) cb(clock);
}

export function pendingFrames(): number {
  return frames.length;
}

export function setVisibility(next: DocumentVisibilityState): void {
  visibility = next;
  document.dispatchEvent(new Event('visibilitychange'));
}

export function settleFrames(): void {
  if (visibility !== 'visible') setVisibility('visible');
  runFrame();
}
