# Perf benchmarks

Three benchmarks that measure how streaming *feels*, none of which run in the
default e2e suite: they are all gated behind `PERF=1`.

| Spec | Question it answers |
|---|---|
| `streaming-smoothness.spec.js` | Does a long reply at token cadence keep the frame rate up under CPU throttling? |
| `typewriter.spec.js` | Does the reveal read as steady typing, and does it finish with the model? |
| `return-glitch.spec.js` | When presentation falls behind state and then catches up, does the transcript jump, lose text or fling the view? |

## The two canonical invocations

```bash
# Smoothness, against the shipped bundle, three runs, then the comparison.
PERF=1 PERF_BUILD=1 PERF_LABEL=<label> pnpm exec playwright test e2e/perf/streaming-smoothness.spec.js --repeat-each=3
node scripts/perf-summary.mjs <baseline-label> <label>

# The catch-up repro, one scenario at a time, read from the console.
PERF=1 pnpm exec playwright test e2e/perf/return-glitch.spec.js
```

`perf-summary.mjs` reads `perf-results/`, prints the median per label side by
side, and refuses to show a delta when two labels were measured under different
flags. `--kind typewriter` summarizes the typewriter runs instead, one table per
arrival pattern.

## Flags

| Flag | Applies to | Effect |
|---|---|---|
| `PERF=1` | all three | Required. Without it every benchmark skips. |
| `PERF_BUILD=1` | web server | Serves a production build instead of the dev server, so the numbers are the shipped bundle's. Adds a few minutes for the build; ignored unless `PERF` is set. |
| `PERF_LABEL=<name>` | smoothness, typewriter | Column the run is filed under. Defaults to the git short sha. |
| `PERF_HEADED=1` | all three | Runs on the real display. Headless rAF caps near 110 fps with no vsync, and a headless tab never goes hidden, so the hidden-tab scenario only measures a real background tab here. |
| `PERF_CPU=<n>` | smoothness | CPU throttling rate (default 4). |
| `PERF_CHUNK_MS=<n>` / `PERF_CHUNK_CHARS=<n>` | smoothness | Token cadence: chars per SSE event and the gap between them (defaults 8 and 8). |
| `PERF_PROFILE=1` | smoothness | Records a V8 CPU profile and prints self time per module and per function: what to fix next. |
| `PERF_TRACE=1` | smoothness | Records a Chrome trace and sums renderer time per event kind (Layout, Paint, ...), which is where the profiler's `(program)` time goes. |
| `PERF_CAST=1` | return-glitch (reload) | Records compositor frames over the catch-up and counts the magenta band painted at the top of the transcript: the ground truth for whether a frame was ever shown away from the bottom. The band shifts layout and the encoder takes main-thread time, so a `PERF_CAST` run's layout and frame numbers are not comparable with a plain run's. |
| `PERF_SHOT=<path>` | return-glitch (reload) | Screenshots the reconnecting state after the reload. |
| `PERF_SCROLL_LOG=1` | return-glitch | Records every programmatic scroll with its caller, for finding which code moved the view. |
| `PERF_NO_CAUGHT_UP=1` | return-glitch | Omits the `caught_up` marker, as a server without it would: the client falls back to a timer and the typewriter's catch-up rule. |
| `PERF_BACKLOG_MS=<n>` | return-glitch | Gap between backlog events on reconnect (default 2). `0` lets Node coalesce the whole backlog into one socket read and one React render, which is a different test. |

## Layout

- `streamFixture.js` - the deterministic ~14 KB reply and the SSE event list built from it.
- `metrics.js` - the smoothness probe (frame gaps, LoAF, DOM churn).
- `probe.js` - the transcript sampler (text length, scroll position, layout shifts) plus its report, shared by the typewriter and catch-up specs.
- `rafFreeze.js` - parks rAF callbacks, standing in for a hidden tab where there is no display.
- `screencast.js` - compositor-frame capture and the magenta pixel count.
- `run.js` - where a run is filed and under what label.
