/** Where a benchmark run is filed and under what name. */
import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';

/** PERF_LABEL, else the git short sha: the column a run lands in. */
export function label() {
  if (process.env.PERF_LABEL) return process.env.PERF_LABEL;
  try { return execSync('git rev-parse --short HEAD').toString().trim(); } catch { return 'unlabeled'; }
}

/**
 * The execution mode every benchmark shares: a production build, a real
 * display, a profiler or tracer attached. Each one moves the numbers, so the
 * summary fingerprints it alongside the benchmark's own config.
 */
export function mode() {
  return {
    build: process.env.PERF_BUILD ? 'production' : 'dev',
    headed: !!process.env.PERF_HEADED,
    profile: !!process.env.PERF_PROFILE,
    trace: !!process.env.PERF_TRACE,
  };
}

/** Write one run as perf-results/<kind>-<label>-<ts>-<repeat>.json, stamped with the mode. */
export function writeRun(kind, run, testInfo) {
  const dir = path.resolve('perf-results');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${kind}-${run.label}-${Date.now()}-${testInfo.repeatEachIndex}.json`);
  fs.writeFileSync(file, JSON.stringify({ ...run, mode: mode() }, null, 2));
  return file;
}
