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
