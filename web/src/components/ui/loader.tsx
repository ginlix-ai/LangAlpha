// beui.dev/components/motion/loader — trimmed to the one variant the app uses.
// The curated pre-cleanup set lives in git history: blob 3feea2e434deede55dd8040572d7b957c83f767a
// (was loader.reference.tsx).

import { useCallback, useSyncExternalStore, type CSSProperties } from "react";
import { cn } from "@/lib/utils";

// Terminal-style frame set — the loader CLI AI agents cycle through.
const ASCII_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

export interface LoaderProps {
  /** Glyph font size in px. */
  size?: number;
  /** Seconds per animation cycle. */
  speed?: number;
  /** Accessible label announced to screen readers. */
  label?: string;
  className?: string;
  /** Inline color/etc. — beats the default `text-foreground` class, so status
   *  tables can hand the glyph their accent (or `inherit`) directly. */
  style?: CSSProperties;
}

// ---------------------------------------------------------------------------
// Shared frame clock. A busy run renders dozens of loaders at once (thread
// rows, subagent rows, task cards) — per-instance intervals put dozens of
// unsynchronized 80ms timers on the main thread exactly when SSE traffic
// peaks. All instances at one cadence share ONE interval (and stay
// phase-synced); the interval exists only while a loader is mounted.
// ---------------------------------------------------------------------------

interface Ticker {
  frame: number;
  listeners: Set<() => void>;
  id: ReturnType<typeof setInterval>;
}

const tickers = new Map<number, Ticker>();

function subscribeFrame(stepMs: number, cb: () => void): () => void {
  let t = tickers.get(stepMs);
  if (!t) {
    const created: Ticker = {
      frame: 0,
      listeners: new Set(),
      id: setInterval(() => {
        created.frame = (created.frame + 1) % ASCII_FRAMES.length;
        created.listeners.forEach((l) => l());
      }, stepMs),
    };
    t = created;
    tickers.set(stepMs, created);
  }
  t.listeners.add(cb);
  return () => {
    t.listeners.delete(cb);
    if (t.listeners.size === 0) {
      clearInterval(t.id);
      // A later generation may already own this cadence slot (React runs all
      // destroys before creates in a commit) — only remove our own entry.
      if (tickers.get(stepMs) === t) tickers.delete(stepMs);
    }
  };
}

// One matchMedia subscription for every loader instance.
const REDUCE_QUERY = "(prefers-reduced-motion: reduce)";
const reduceListeners = new Set<() => void>();
let reduceMq: MediaQueryList | null = null;

function subscribeReduce(cb: () => void): () => void {
  if (!reduceMq && typeof window !== "undefined") {
    reduceMq = window.matchMedia(REDUCE_QUERY);
    reduceMq.addEventListener("change", notifyReduce);
  }
  reduceListeners.add(cb);
  return () => {
    reduceListeners.delete(cb);
  };
}

function notifyReduce(): void {
  reduceListeners.forEach((l) => l());
}

function getReduceSnapshot(): boolean {
  if (!reduceMq && typeof window !== "undefined") {
    reduceMq = window.matchMedia(REDUCE_QUERY);
    reduceMq.addEventListener("change", notifyReduce);
  }
  return reduceMq?.matches ?? false;
}

export function Loader({
  size = 32,
  speed = 0.8,
  label = "Loading",
  className,
  style,
}: LoaderProps) {
  // Reduced motion slows the cycle rather than stopping it — it's a glyph
  // swap, not on-screen movement.
  const reduce = useSyncExternalStore(
    subscribeReduce,
    getReduceSnapshot,
    () => false,
  );
  const stepMs = ((reduce ? speed * 2.5 : speed) / ASCII_FRAMES.length) * 1000;
  // Stable subscribe identity: an inline closure is a NEW function every
  // render, and useSyncExternalStore resubscribes whenever it changes — each
  // frame tick would tear the interval down and rebuild it at frame 0,
  // freezing the glyph while churning timers.
  const subscribe = useCallback(
    (cb: () => void) => subscribeFrame(stepMs, cb),
    [stepMs],
  );
  const frame = useSyncExternalStore(
    subscribe,
    () => tickers.get(stepMs)?.frame ?? 0,
    () => 0,
  );

  return (
    <span
      role="status"
      aria-label={label}
      className={cn(
        "inline-flex items-center justify-center text-foreground",
        className,
      )}
      style={style}
    >
      <span
        className="font-mono leading-none tabular-nums"
        style={{ fontSize: size, lineHeight: 1 }}
      >
        {ASCII_FRAMES[frame % ASCII_FRAMES.length]}
      </span>
      <span className="sr-only">{label}</span>
    </span>
  );
}

export default Loader;
