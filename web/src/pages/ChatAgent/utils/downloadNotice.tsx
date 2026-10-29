import { useSyncExternalStore } from 'react';
import type { TFunction } from 'i18next';
import i18n from '@/i18n';
import { toast } from '@/components/ui/use-toast';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { holdReload } from '@/lib/reloadHolds';
import { DownloadProgress, DownloadStarted } from '../components/filePanel/DownloadProgress';

// Below this a save feels instant and a notice would only flash.
const NOTICE_AFTER_MS = 400;
// A large file is exported to storage before its link exists, which can take
// a minute; past this the notice says so rather than looking stuck.
const SLOW_AFTER_MS = 8000;
// The browser's own download UI is easy to miss, so a control reads as
// started for this long after the hand-off rather than inviting a second click.
const STARTED_HOLD_MS = 3000;
// Long enough to read the confirmation, short enough not to pile up.
const STARTED_NOTICE_MS = 4000;

const inFlight = new Map<string, Promise<boolean>>();
const started = new Map<string, ReturnType<typeof setTimeout>>();
const listeners = new Set<() => void>();
// Each running save's notice teardown. A save can stall past sign-out, and its
// pending timer would otherwise open a notice naming the previous account's file.
const notices = new Set<() => void>();

// A save still running at sign-out belongs to the previous account: it must
// not hand that account's file to the browser, toast, or put its state back
// after this cleared it, so each one checks its session as it lands.
registerAuthReset(() => {
  inFlight.clear();
  started.forEach((timer) => clearTimeout(timer));
  started.clear();
  notices.forEach((teardown) => teardown());
  notices.clear();
  notify();
});

function notify() {
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function workspaceDownloadKey(workspaceId: string, filePath: string): string {
  return `${workspaceId}:${filePath}`;
}

/**
 * A chat card names a file as the agent wrote it, before it resolves to a
 * workspace path, so its save is tracked under the reference itself.
 */
export function cardDownloadKey(workspaceId: string | undefined, path: string): string {
  return `card:${workspaceId ?? ''}:${path}`;
}

function markStarted(key: string) {
  clearTimeout(started.get(key));
  started.set(key, setTimeout(() => {
    started.delete(key);
    notify();
  }, STARTED_HOLD_MS));
}

/**
 * Run ``run`` once per key at a time. A repeat call while it is running joins
 * it, so a second click cannot start a second export of the same bytes, and
 * every control bound to the key can show it as busy. ``run`` resolves whether
 * it handed a save to the browser; only then does the key read as started, and
 * a repeat inside that hold is the same click again, so it does nothing.
 */
export function trackPending(key: string, run: () => Promise<boolean>): Promise<boolean> {
  const running = inFlight.get(key);
  if (running) return running;
  if (started.has(key)) return Promise.resolve(true);
  const current = authSessionCheck();
  const release = holdReload();
  const tracked: Promise<boolean> = run()
    .then((handedOff) => {
      if (handedOff && current()) markStarted(key);
      return handedOff;
    })
    .finally(() => {
      release();
      if (inFlight.get(key) === tracked) inFlight.delete(key);
      notify();
    });
  inFlight.set(key, tracked);
  notify();
  return tracked;
}

export type DownloadState = 'idle' | 'preparing' | 'started';

function stateOf(key: string | null): DownloadState {
  if (!key) return 'idle';
  if (inFlight.has(key)) return 'preparing';
  return started.has(key) ? 'started' : 'idle';
}

/** Where a save under ``key`` stands; null reads as idle. */
export function useDownloadState(key: string | null): DownloadState {
  return useSyncExternalStore(subscribe, () => stateOf(key));
}

/** The label a Download control shows for ``state``. */
export function downloadLabel(state: DownloadState, idle: string, t: TFunction): string {
  if (state === 'preparing') return t('filePanel.preparing');
  if (state === 'started') return t('filePanel.downloadStarted');
  return idle;
}

/** Reports how much of a save has arrived, once the tab is the one fetching it. */
export type DownloadProgressReport = (fraction: number) => void;

/**
 * Run a save with a notice while it is being prepared.
 *
 * The browser shows progress only once bytes flow, and a large file's first
 * click waits on the server first, so without this the click looks dead. The
 * server's export reports nothing until it ends, so its bar is indeterminate;
 * a fetch the tab makes itself reports real progress through ``report``.
 * Once the save is handed to the browser the notice turns into a confirmation,
 * shown even for a save too quick to have needed the notice, since that is
 * the case where the browser's own cue is easiest to miss.
 */
export function withDownloadNotice(
  key: string,
  fileName: string,
  save: (report: DownloadProgressReport, current: () => boolean) => Promise<void>,
): Promise<void> {
  return trackPending(key, async () => {
    const current = authSessionCheck();
    const notice: { current: ReturnType<typeof toast> | null } = { current: null };
    let fraction: number | null = null;
    let slow = false;
    const render = () => {
      const text = fraction !== null
        ? i18n.t('filePanel.downloadingProgress', { name: fileName, percent: Math.round(fraction * 100) })
        : i18n.t(slow ? 'filePanel.preparingDownloadSlow' : 'filePanel.preparingDownload', { name: fileName });
      return <DownloadProgress text={text} fraction={fraction} />;
    };
    const shownTimer = setTimeout(() => {
      notice.current = toast({ description: render(), duration: Infinity });
    }, NOTICE_AFTER_MS);
    const slowTimer = setTimeout(() => {
      slow = true;
      notice.current?.update({ description: render() });
    }, SLOW_AFTER_MS);
    const teardown = () => {
      clearTimeout(shownTimer);
      clearTimeout(slowTimer);
      notice.current?.dismiss();
      notice.current = null;
    };
    notices.add(teardown);
    const report: DownloadProgressReport = (next) => {
      const clamped = Math.min(1, Math.max(0, next));
      // One repaint per visible step, not one per network chunk.
      if (fraction !== null && Math.round(clamped * 100) === Math.round(fraction * 100)) return;
      fraction = clamped;
      notice.current?.update({ description: render() });
    };
    try {
      await save(report, current);
    } catch (err) {
      notice.current?.dismiss();
      throw err;
    } finally {
      clearTimeout(shownTimer);
      clearTimeout(slowTimer);
      notices.delete(teardown);
    }
    if (!current()) {
      notice.current?.dismiss();
      return false;
    }
    const done = <DownloadStarted text={i18n.t('filePanel.downloadStartedNotice', { name: fileName })} />;
    const shown = notice.current;
    if (shown) shown.update({ description: done });
    const confirmation = shown ?? toast({ description: done, duration: STARTED_NOTICE_MS });
    // Registered until it expires: it names the file, so a sign-out in the
    // meantime has to take it down with the rest.
    const expire = () => {
      clearTimeout(expiry);
      confirmation.dismiss();
      notices.delete(expire);
    };
    const expiry = setTimeout(expire, STARTED_NOTICE_MS);
    notices.add(expire);
    return true;
  }).then(() => undefined);
}
