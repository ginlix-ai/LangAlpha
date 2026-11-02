/**
 * Notices a stream that has stopped arriving without ending.
 *
 * A connection that dies with no close (a socket iOS took from Safari in the
 * background, a move between networks) leaves `reader.read()` pending for good,
 * with nothing thrown and no event fired: an app switch on iOS reports only
 * `visibilitychange`, the same as a tab switch, so no page event can tell a dead
 * socket from a live one. Only the bytes can. The server's keepalives put a
 * bound on how long a live stream stays quiet, and past it the stream is dead.
 */

/** The longest a run stream goes between keepalives. */
export const RUN_STREAM_BEAT_MS = 15_000;
/** The longest the thread mux or the lifecycle feed goes between pings. */
export const PING_STREAM_BEAT_MS = 30_000;

const MISSED_BEATS = 3;
// A page back from being hidden or asleep need not wait out the rest of the
// bound once a beat is overdue: whatever a live socket held for it arrives at
// once, so the stream gets only the moment a phone's radio needs to wake.
const RETURN_GRACE_MS = 3_000;
// A timer this late was frozen with the page, which then woke without a
// visibility change, as a laptop does from sleep.
const ASLEEP_SLACK_MS = 1_000;

export interface StreamSilenceWatch {
  /** Bytes arrived. */
  heard: () => void;
  stop: () => void;
}

/**
 * Calls `onSilent` once, when the stream has missed the keepalives its `beatMs`
 * promises, saying whether the page found it so on coming back. That silence is
 * what being away does to a socket; one with the page in view is the stream failing.
 */
export function watchStreamSilence(beatMs: number, onSilent: (onReturn: boolean) => void): StreamSilenceWatch {
  let heardAt = Date.now();
  let graceUntil: number | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const due = () => graceUntil ?? heardAt + beatMs * MISSED_BEATS;
  const arm = () => {
    clearTimeout(timer);
    timer = setTimeout(check, Math.max(0, due() - Date.now()));
  };
  const onReturn = () => {
    if (Date.now() - heardAt >= beatMs) graceUntil = Date.now() + RETURN_GRACE_MS;
    arm();
  };
  const check = () => {
    // A hidden page is not waiting on anything, and may only be throttled.
    if (document.hidden) return;
    const now = Date.now();
    if (now < due()) return arm();
    if (graceUntil === null && now - due() > ASLEEP_SLACK_MS) return onReturn();
    stop();
    onSilent(graceUntil !== null);
  };
  const stop = () => {
    clearTimeout(timer);
    document.removeEventListener('visibilitychange', onVisibility);
  };
  const onVisibility = () => {
    if (!document.hidden) onReturn();
  };

  document.addEventListener('visibilitychange', onVisibility);
  arm();
  return {
    heard: () => {
      heardAt = Date.now();
      graceUntil = null;
    },
    stop,
  };
}
