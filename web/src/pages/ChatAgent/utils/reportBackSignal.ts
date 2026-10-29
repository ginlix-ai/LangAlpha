/**
 * Single decode of the backend's TRI-STATE `pending_report_back` wire value, so
 * the raw `boolean | null | undefined` never leaks into UI control flow.
 * Dependency-free (re-exported from `./api`) so the report-back watch can decode
 * wire status even where the hook tests mock `../utils/api`.
 */

/**
 * `true`→pending, `false`→idle (drained), `null`→unknown (the backend's own
 * Redis read failed), absent/`undefined`→none (no signal).
 */
export type ReportBackSignal = 'pending' | 'idle' | 'unknown' | 'none';

/** Decode the backend's tri-state `pending_report_back` into a {@link ReportBackSignal}. */
export function decodeReportBackSignal(raw: boolean | null | undefined): ReportBackSignal {
  if (raw === true) return 'pending';
  if (raw === false) return 'idle';
  if (raw === null) return 'unknown';
  return 'none';
}

/**
 * Arm / keep-watching predicate: `pending` and `unknown` both keep watching.
 * Asymmetric on purpose — a watch drains only on the backend's explicit `false`.
 */
export const shouldArmReportBack = (s: ReportBackSignal): boolean =>
  s === 'pending' || s === 'unknown';

/**
 * Whether a status read leaves a report owed, which the tray names. Live tail
 * subagents count: their report-backs haven't materialized yet, so pendingness
 * alone under-reports (producer-undecided). Typed structurally to keep this
 * module dependency-free.
 */
export const reportOwedForStatus = (status: {
  pending_report_back?: boolean | null;
  active_tasks?: string[] | null;
}): boolean =>
  shouldArmReportBack(decodeReportBackSignal(status.pending_report_back)) ||
  (status.active_tasks?.length ?? 0) > 0;

/**
 * Arm-on-status predicate, shared by the load path and the watch's reactivation
 * path so the two can't drift: a report owed, or a background command still
 * running, whose end can start a turn of its own for the watch to attach. A
 * running command owes nothing the tray names, since its own row is there.
 */
export const shouldArmForStatus = (status: {
  pending_report_back?: boolean | null;
  active_tasks?: string[] | null;
  active_commands?: string[] | null;
}): boolean =>
  reportOwedForStatus(status) || (status.active_commands?.length ?? 0) > 0;
