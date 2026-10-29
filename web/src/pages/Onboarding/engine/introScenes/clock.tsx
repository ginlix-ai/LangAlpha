import { createContext, Fragment, useContext, useEffect, useState, type ReactNode } from 'react';
import { useReducedMotion } from '@/lib/framer';

/*
 * One clock per scene. A scene is a script: every moving part reads the
 * scene's time and draws itself for that moment, so the whole scene is a
 * function of one number. That keeps a script deterministic, lets the dev
 * preview pin any frame, and makes reduced motion a single case: the clock
 * stands on the last frame.
 */
/* eslint-disable react-refresh/only-export-components -- the clock's context
   and hooks travel with its provider */

/** How long the finished scene stays up before it plays again. */
const HOLD_MS = 2800;
/** The finished scene fades out over the last stretch of the hold. */
const FADE_MS = 400;
/** Scenes redraw at most this often; CSS smooths what moves between frames. */
const FRAME_MS = 1000 / 30;
/** Two 60Hz vsyncs often add up to a hair under FRAME_MS; the slack counts
    them as one frame rather than waiting for a third. */
const FRAME_SLACK_MS = 4;

/** Outside a clock, a scene draws its finished frame. */
const SceneTime = createContext(Number.POSITIVE_INFINITY);

/** Before the scene starts: what happened then (a thread's earlier
    exchange, a page already built) is on the page, settled, from the first
    frame. Every `t >= at` holds and every tween from it has finished. */
export const PAST = Number.NEGATIVE_INFINITY;

/** How long a click holds the pointer, and the control it lands on, down. */
const CLICK_MS = 160;
/** How long the composer's send button holds down when a message goes out. */
export const SEND_MS = 260;

/** Pins every scene under it to one moment (ms), for the dev preview. */
export const FrozenSceneTime = createContext<number | null>(null);

/** Draws its children as their script left them: a scene that opens on the
    page an earlier scene ended on. */
export function Finished({ children }: { children: ReactNode }) {
  return <SceneTime.Provider value={Number.POSITIVE_INFINITY}>{children}</SceneTime.Provider>;
}

/** The scene's time in ms since its script started. */
export function useSceneTime(): number {
  return useContext(SceneTime);
}

/** Linear 0 to 1 progress of `t` through `[at, at + dur]`. */
export function progress(t: number, at: number, dur: number): number {
  return Math.min(Math.max((t - at) / dur, 0), 1);
}

/** `progress`, eased out: for values built from several tweens at once. */
export function ease(t: number, at: number, dur: number): number {
  return 1 - (1 - progress(t, at, dur)) ** 3;
}

/** `progress`, eased in and out: how the camera and the pointer travel. */
export function easeInOut(t: number, at: number, dur: number): number {
  const p = progress(t, at, dur);
  return p < 0.5 ? 4 * p ** 3 : 1 - (-2 * p + 2) ** 3 / 2;
}

/** Whether `t` falls in `[from, to)`. */
export function during(t: number, from: number, to: number): boolean {
  return t >= from && t < to;
}

/** Whether a press at `at` (or at any of several times) still holds at `t`. */
export function pressed(t: number, at: number | readonly number[] = [], ms = CLICK_MS): boolean {
  return (typeof at === 'number' ? [at] : at).some((a) => during(t, a, a + ms));
}

/** Eased 0 to 1 progress through `[at, at + dur]` of the scene's script. */
export function useTween(at: number, dur: number): number {
  return ease(useSceneTime(), at, dur);
}

/**
 * Runs a scene's script of `length` ms, holds the last frame, fades, and
 * replays. Each replay remounts the scene, so CSS entrances run again too.
 */
export function SceneClock({ length, children }: { length: number; children: ReactNode }) {
  const reduceMotion = useReducedMotion();
  const frozen = useContext(FrozenSceneTime);
  const still = reduceMotion || frozen != null;
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (still) return;
    let raf = 0;
    let last = Number.NEGATIVE_INFINITY;
    const start = performance.now();
    const tick = (now: number) => {
      if (now - last >= FRAME_MS - FRAME_SLACK_MS) {
        last = now;
        setElapsed(now - start);
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [still]);

  const period = length + HOLD_MS;
  const t = frozen ?? (reduceMotion ? length : elapsed % period);
  const cycle = still ? 0 : Math.floor(elapsed / period);
  const fading = !still && t > period - FADE_MS;

  return (
    // eslint-disable-next-line no-restricted-syntax -- the loop's fade, timed with the script's restart (FADE_MS)
    <div style={{ opacity: fading ? 0 : 1, transition: `opacity ${FADE_MS}ms var(--ease-out)` }}>
      <SceneTime.Provider value={t}>
        <Fragment key={cycle}>{children}</Fragment>
      </SceneTime.Provider>
    </div>
  );
}
