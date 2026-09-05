#!/usr/bin/env node
/**
 * Summarize benchmark runs from perf-results/: median per label, side by side.
 *
 *   node scripts/perf-summary.mjs                    # every label
 *   node scripts/perf-summary.mjs base opt1          # only these, in this order
 *   node scripts/perf-summary.mjs --kind typewriter  # the typewriter runs instead
 *
 * A delta only means something between runs of the same benchmark, so each
 * run's config is fingerprinted per label: if two labels were measured under
 * different flags the configs are printed and the exit code is non-zero,
 * rather than a percentage that reads as a win.
 */
import fs from 'node:fs';
import path from 'node:path';

const STREAMING_ROWS = [
  ['durationMs', 'stream duration (ms)', 'lower'],
  ['fps', 'frames per second', 'higher'],
  ['gapP50', 'frame gap p50 (ms)', 'lower'],
  ['gapP95', 'frame gap p95 (ms)', 'lower'],
  ['gapP99', 'frame gap p99 (ms)', 'lower'],
  ['gapMax', 'worst frame gap (ms)', 'lower'],
  ['framesOver33', 'frames over 33 ms', 'lower'],
  ['framesOver50', 'frames over 50 ms', 'lower'],
  ['framesOver100', 'frames over 100 ms', 'lower'],
  ['frozenMs', 'time frozen (ms)', 'lower'],
  ['loafCount', 'long animation frames', 'lower'],
  ['loafTotalMs', 'LoAF total (ms)', 'lower'],
  ['loafBlockingMs', 'LoAF blocking (ms)', 'lower'],
  ['loafMaxMs', 'LoAF worst (ms)', 'lower'],
  ['longTasks', 'long tasks', 'lower'],
  ['mutations', 'DOM mutation records', 'lower'],
  ['nodesAdded', 'DOM nodes added', 'lower'],
  ['nodesRemoved', 'DOM nodes removed', 'lower'],
  ['charDataChanges', 'text node edits', 'lower'],
  ['attrChanges', 'attribute changes', 'lower'],
];

const TYPEWRITER_ROWS = [
  ['revealMs', 'reveal duration (ms)', 'lower'],
  ['tailAfterModelMs', 'typing past model (ms)', 'lower'],
  ['stalls', 'stalls', 'lower'],
  ['stallMs', 'time stalled (ms)', 'lower'],
  ['jumps', 'visible jumps', 'lower'],
  ['maxGainPer50ms', 'worst gain per 50 ms', 'lower'],
  ['medianGainPer50ms', 'median gain per 50 ms', 'higher'],
  ['speedCv', 'reveal unevenness (cv)', 'lower'],
  ['meanCharsPerSec', 'mean chars per second', 'higher'],
  ['frameGapP95', 'frame gap p95 (ms)', 'lower'],
  ['frames', 'frames sampled', 'higher'],
  ['follow.distP50', 'follow gap p50 (px)', 'lower'],
  ['follow.distP95', 'follow gap p95 (px)', 'lower'],
  ['follow.backwardFrames', 'backward scroll frames', 'lower'],
  ['follow.moveCv', 'scroll unevenness (cv)', 'lower'],
  ['follow.maxMovePx', 'worst scroll step (px)', 'lower'],
];

// The run's execution mode (see run.js `mode`): part of the fingerprint, since a
// production build, a real display, or an attached profiler each move the numbers.
const modeText = (m) => (m ? `${m.build} build, ${m.headed ? 'headed' : 'headless'}${m.profile ? ', profiled' : ''}${m.trace ? ', traced' : ''}` : 'unknown mode');

const KINDS = {
  streaming: {
    prefix: 'streaming-',
    rows: STREAMING_ROWS,
    // One table: every streaming run is the same scenario.
    groupOf: () => '',
    configOf: (r) => ({ ...r.config, mode: r.mode }),
    describe: (c) => (c.cpuRate != null ? `${modeText(c.mode)}, cpu x${c.cpuRate}, ${c.events} events, ${c.chunkChars} chars every ${c.chunkDelayMs} ms, reply ${c.replyChars} chars` : '(no config recorded)'),
  },
  typewriter: {
    prefix: 'typewriter-',
    rows: TYPEWRITER_ROWS,
    // One table per arrival pattern: the two scenarios are different benchmarks.
    groupOf: (r) => r.scenario?.name ?? '(no scenario)',
    configOf: (r) => ({ ...r.scenario, mode: r.mode }),
    describe: (c) => (c.name ? `${modeText(c.mode)}, ${c.name} arrival, ${c.chars} chars every ${c.delayMs} ms` : '(no scenario recorded)'),
  },
};

const argv = process.argv.slice(2);
let kind = 'streaming';
const want = [];
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--kind') { kind = argv[++i]; continue; }
  if (argv[i].startsWith('--kind=')) { kind = argv[i].slice('--kind='.length); continue; }
  want.push(argv[i]);
}
const spec = KINDS[kind];
if (!spec) {
  console.error(`unknown --kind ${kind}; expected one of ${Object.keys(KINDS).join(', ')}`);
  process.exit(2);
}

const dir = path.resolve('perf-results');
const runs = fs.existsSync(dir)
  ? fs.readdirSync(dir).filter((f) => f.startsWith(spec.prefix) && f.endsWith('.json')).map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')))
  : [];

const groups = new Map();
for (const r of runs) {
  if (want.length && !want.includes(r.label)) continue;
  const g = spec.groupOf(r);
  if (!groups.has(g)) groups.set(g, new Map());
  const byLabel = groups.get(g);
  if (!byLabel.has(r.label)) byLabel.set(r.label, []);
  byLabel.get(r.label).push(r);
}
if (!groups.size) {
  if (!runs.length) console.log(`no ${kind} runs in perf-results/`);
  else console.log(`no ${kind} runs for ${want.join(', ')}; available labels: ${[...new Set(runs.map((r) => r.label))].join(', ')}`);
  process.exit(0);
}

// A null sample is a measurement the probe could not take (the typewriter
// tail when the stop control never showed), not a zero: drop it, and report
// n/a when nothing measurable is left rather than a median of zeros.
const median = (xs) => {
  const s = xs.filter((x) => typeof x === 'number').sort((a, b) => a - b);
  return s.length ? s[Math.floor(s.length / 2)] : 'n/a';
};
const get = (o, key) => key.split('.').reduce((a, k) => (a == null ? a : a[k]), o);
/** Key-order-independent JSON, so two configs written in a different order still match. */
const stable = (v) => {
  if (v === null || typeof v !== 'object') return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return `[${v.map(stable).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}`;
};

for (const [group, byLabel] of [...groups].sort((a, b) => String(a[0]).localeCompare(String(b[0])))) {
  const labels = want.length ? want.filter((l) => byLabel.has(l)) : [...byLabel.keys()];
  if (group) console.log(`\n=== ${group} ===`);
  const fingerprints = new Set();
  for (const l of labels) {
    const fps = new Set(byLabel.get(l).map((r) => stable(spec.configOf(r))));
    for (const f of fps) fingerprints.add(f);
    console.log(`config ${l}: ${spec.describe(spec.configOf(byLabel.get(l)[0]))}${fps.size > 1 ? '   (this label mixes configs)' : ''}`);
  }
  if (fingerprints.size > 1) {
    console.log('these runs were not measured the same way, so a delta would compare different benchmarks; rerun the odd label with the same flags');
    process.exitCode = 1;
    continue;
  }
  console.log('runs per label: ' + labels.map((l) => `${l}=${byLabel.get(l).length}`).join(', '));
  console.log('');

  const w = Math.max(...labels.map((l) => l.length), 10);
  const delta = (vals, better) => {
    if (labels.length < 2) return '';
    const a = vals[0]; const b = vals[vals.length - 1];
    if (!a || typeof a !== 'number' || typeof b !== 'number') return '';
    const pct = ((b - a) / a) * 100;
    const good = better === 'lower' ? pct < 0 : pct > 0;
    return `   ${pct >= 0 ? '+' : ''}${pct.toFixed(0)}% ${Math.abs(pct) < 3 ? '' : good ? 'better' : 'worse'}`;
  };
  const line = (name, vals, suffix) => console.log(name.padEnd(26) + vals.map((v) => String(v).padStart(w + 2)).join('') + suffix);

  console.log('metric'.padEnd(26) + labels.map((l) => l.padStart(w + 2)).join('') + (labels.length > 1 ? '   vs first' : ''));
  for (const [key, name, better] of spec.rows) {
    const vals = labels.map((l) => median(byLabel.get(l).map((r) => get(r.metrics, key))));
    line(name, vals, delta(vals, better));
  }

  // Requests issued during the run, when the benchmark recorded them: a
  // control that writes and refetches in a loop shows up here long before it
  // shows up in the frame numbers.
  const paths = [...new Set(labels.flatMap((l) => byLabel.get(l).flatMap((r) => (r.requests || []).map(([p]) => p))))];
  if (paths.length) {
    console.log('\nrequests (median per label)');
    const rows = paths
      .map((p) => ({ p, vals: labels.map((l) => median(byLabel.get(l).map((r) => new Map(r.requests || []).get(p) || 0))) }))
      .sort((a, b) => Math.max(...b.vals) - Math.max(...a.vals))
      .slice(0, 12);
    for (const { p, vals } of rows) line(p.length > 25 ? p.slice(0, 25) : p, vals, delta(vals, 'lower'));
  }

  for (const l of labels) {
    const top = byLabel.get(l)[0].metrics?.loafTop || [];
    if (!top.length) continue;
    console.log(`\nworst long animation frames (${l}, first run):`);
    for (const e of top) {
      const s = e.scripts.map((x) => `${x.fn || '?'}@${(x.src || '').split('/').pop().slice(0, 40)} ${x.dur}ms`).join('; ');
      console.log(`  ${String(e.dur).padStart(5)} ms  at ${String(e.at ?? '?').padStart(6)} ms  style+layout ${String(e.styleLayout ?? '?').padStart(3)} ms  ${s}`);
    }
  }
}
