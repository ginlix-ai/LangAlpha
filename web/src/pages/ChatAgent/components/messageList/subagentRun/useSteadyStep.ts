import { useEffect, useRef, useState } from 'react';
import {
  STATUS_HOLD_MS,
  SUBAGENT_BETWEEN_GRACE_MS,
  SUBAGENT_STEP_DWELL_MS,
  SUBAGENT_STEP_LINGER_MS,
} from '../liveZoneTiming';

/** A step as the line names it: its text, and whether it names what the run
 *  does between calls rather than a call. */
export interface Step {
  text: string;
  between?: true;
}

/**
 * The step a live subagent row names, steadied: the run's current call comes
 * and goes as fast as its tools answer, and drawn as is, a quick call flashed
 * past and the line blanked between every two calls.
 *
 * An empty line takes a step at once. A newer step waits until the one shown
 * has had SUBAGENT_STEP_DWELL_MS and it has itself run STATUS_HOLD_MS, so a
 * call that returns at once never shows. A step that names what the run does
 * between calls waits SUBAGENT_BETWEEN_GRACE_MS instead of the hold, so a
 * quick turn between two calls leaves the last call on the line. An empty
 * step leaves the one shown for SUBAGENT_STEP_LINGER_MS. Off (`steady` false:
 * a settled row's result, or a status that says no step) the line follows
 * `step` as given.
 */
export function useSteadyStep({ text, between }: Step, steady: boolean): Step {
  const [shown, setShown] = useState<Step>({ text, between });
  if (shown.text !== text && (!steady || !shown.text)) setShown({ text, between });
  const shownAt = useRef(0);
  // Declared before the timer's effect, so a step shown in this commit is
  // stamped before the wait of the one after it is measured from it.
  useEffect(() => {
    shownAt.current = performance.now();
  }, [shown.text]);
  useEffect(() => {
    if (!steady || !shown.text || text === shown.text) return;
    const dwellLeft = SUBAGENT_STEP_DWELL_MS - (performance.now() - shownAt.current);
    const wait = !text
      ? SUBAGENT_STEP_LINGER_MS
      : Math.max(between ? SUBAGENT_BETWEEN_GRACE_MS : STATUS_HOLD_MS, dwellLeft);
    const timer = setTimeout(() => setShown({ text, between }), wait);
    return () => clearTimeout(timer);
  }, [steady, text, between, shown.text]);
  return shown;
}
