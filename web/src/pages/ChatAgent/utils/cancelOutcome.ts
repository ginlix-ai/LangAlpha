/**
 * The one shape a cancel answers in, and the single read of whether it stopped
 * anything. Dependency-free (re-exported from `./api`) so a stop path can judge
 * its own outcome even where tests mock `../utils/api`.
 */

/**
 * Why a cancel stopped nothing — absent exactly when one was dispatched.
 *
 * `already_finished` and `no_active_run` are benign: what the caller wanted
 * stopped was already stopped. `another_run_active` is not — the run named for
 * the stop is gone while the thread stayed busy, so a turn the user believes
 * they stopped may still be running.
 */
export type CancelNoOpState =
  | 'already_finished'
  | 'no_active_run'
  | 'another_run_active'
  | 'run_not_found'
  | 'task_not_found';

/** A steer of the user's the server holds unread: one a stop hands back, or
 *  one an errored turn left queued for the next. */
export interface HeldSteer {
  content: string;
  /** The key its send went out under; null for one sent with none. */
  request_key: string | null;
  /** Epoch seconds. */
  timestamp?: number;
}

export interface CancelOutcome {
  cancelled: boolean;
  thread_id?: string;
  task_id?: string;
  state?: CancelNoOpState;
  message: string;
  /** The user's steers the stop took back from the run, unread, for the
   *  composer. Older servers omit it. */
  returned_steers?: HeldSteer[];
}

/**
 * Whether a cancel request stopped nothing.
 *
 * The endpoint answers HTTP 200 with `cancelled: false` when there was nothing
 * to stop, so a resolved promise is not proof that a stop is on its way.
 */
export function isNoOpCancellation(outcome: unknown): boolean {
  return (
    typeof outcome === 'object' &&
    outcome !== null &&
    (outcome as { cancelled?: unknown }).cancelled === false
  );
}
