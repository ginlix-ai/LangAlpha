import React, { useEffect, useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUp } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { ToggleSwitch } from '@/components/ui/switch';
import { useLocale } from '@/hooks/useLocale';
import { useRunningWork } from '@/hooks/useRunningWork';
import {
  canDesktopSettings,
  canShowUpdates,
  getUpdateState,
  hideUpdates,
  installUpdate,
  onUpdateState,
  setDesktopSettings,
  type DesktopUpdateState,
} from '@/lib/desktop';
import { calendarMonthDay } from '@/lib/format';

/**
 * The desktop app's update as its shell reports it; null outside the app or on
 * a shell that cannot say. Asking for it is what tells the shell this page
 * shows the button, so a page that is not `shown` asks and then takes it back,
 * and the shell's own prompt offers the update instead.
 */
function useAppUpdate(shown: boolean): DesktopUpdateState | null {
  const [state, setState] = useState<DesktopUpdateState | null>(null);
  useEffect(() => {
    if (!canShowUpdates()) return undefined;
    let live = true;
    const off = onUpdateState((next) => setState(next));
    void getUpdateState().then((first) => {
      if (!live) return;
      if (first) setState((held) => held ?? first);
      if (!shown) hideUpdates();
    });
    return () => {
      live = false;
      off();
      // Without a new document the shell still counts this page as showing
      // the button, as when a narrow window swaps the sidebar for tabs.
      hideUpdates();
    };
  }, [shown]);
  return state;
}

const NIGHTLY = /^(\d+\.\d+\.\d+)-nightly\.(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})$/;

/** A version as a person reads it: a nightly by its build date rather than its stamp. */
function useVersionLabel(): (version: string) => string {
  const { t } = useTranslation();
  const locale = useLocale();
  return (version) => {
    const m = NIGHTLY.exec(version);
    if (!m) return version;
    const [, base, y, mo, d, h, mi] = m;
    // The stamp is the build's UTC time, and the formatter reads it in UTC.
    const built = new Date(Date.UTC(+y, +mo - 1, +d, +h, +mi));
    return t('appUpdate:nightlyLabel', { version: base, date: calendarMonthDay(built, locale) });
  };
}

/** One line of what a restart does: the bold count, then what happens to it. */
function Consequence({ count, label, after }: { count: number; label: string; after: string }) {
  if (count <= 0) return null;
  return (
    <li className="app-update-line app-update-line--running">
      <span>
        <b>{label}</b>
        {after}
      </span>
    </li>
  );
}

/**
 * Automatic updates, switched from the panel as from Settings: the shell keeps
 * the setting and sends the new update state back. Drawn only where the shell
 * downloads updates itself, so there is something to turn off.
 */
function AutomaticSwitch({ state }: { state: DesktopUpdateState }) {
  const { t } = useTranslation();
  const labelId = useId();
  if (state.mode !== 'auto' || typeof state.automatic !== 'boolean' || !canDesktopSettings()) return null;
  const on = state.automatic;
  return (
    <div className="app-update-auto">
      <label id={labelId} htmlFor={`${labelId}-switch`}>{t('appUpdate:automatic')}</label>
      <ToggleSwitch
        id={`${labelId}-switch`}
        checked={on}
        onChange={() => void setDesktopSettings({ automaticUpdates: !on })}
        ariaLabel={t('appUpdate:automatic')}
      />
    </div>
  );
}

/** What the panel says about an update that is not ready yet. */
function pendingBodyKey(state: DesktopUpdateState): string {
  if (state.status === 'downloading') return 'appUpdate:downloadingBody';
  if (state.mode === 'notify') return 'appUpdate:notifyBody';
  // Available with automatic updates on is a download that failed. A shell
  // that does not say is one that always updates by itself.
  return state.automatic === false ? 'appUpdate:manualBody' : 'appUpdate:retryBody';
}

function UpdatePanel({ state, onLater }: { state: DesktopUpdateState; onLater: () => void }) {
  const { t } = useTranslation();
  const label = useVersionLabel();
  const ready = state.status === 'ready';
  const downloading = state.status === 'downloading';
  // A download made here stays in view, so the person sees it finish and can
  // restart from the same panel; a restart or a browser download leaves it.
  const staysOpen = state.status === 'available' && state.mode === 'auto';
  // Asked only where a restart stops the server.
  const { data: running, isError: countFailed } = useRunningWork(ready && state.stopsServer);
  const counting = ready && state.stopsServer && !running && !countFailed;
  const version = label(state.version ?? '');

  return (
    <div className="app-update-panel">
      <div className="app-update-title">
        {t(ready ? 'appUpdate:readyTitle' : 'appUpdate:availableTitle', { version })}
      </div>
      <div className="app-update-sub">
        {t('appUpdate:youHave', { version: label(state.currentVersion) })}
        {state.notesUrl && (
          <>
            {' · '}
            {/* The shell sends a page's external link to the system browser. */}
            <a className="app-update-link tap-target" href={state.notesUrl} target="_blank" rel="noreferrer">
              {t('appUpdate:whatsNew')}
            </a>
          </>
        )}
      </div>
      <ul className="app-update-lines">
        {ready ? (
          <>
            <li className="app-update-line">
              <span>{t(state.stopsServer ? 'appUpdate:relaunch' : 'appUpdate:relaunchRemote')}</span>
            </li>
            {counting && (
              <li className="app-update-line">
                <span>{t('appUpdate:countingWork')}</span>
              </li>
            )}
            {state.stopsServer && countFailed && !running && (
              <li className="app-update-line app-update-line--running">
                <span>{t('appUpdate:workUnknown')}</span>
              </li>
            )}
            {state.stopsServer && running && (
              <>
                <Consequence
                  count={running.turns}
                  label={t('appUpdate:turns', { count: running.turns })}
                  after={t('appUpdate:turnsAfter', { count: running.turns })}
                />
                <Consequence
                  count={running.background_tasks}
                  label={t('appUpdate:tasks', { count: running.background_tasks })}
                  after={t('appUpdate:tasksAfter', { count: running.background_tasks })}
                />
                <Consequence
                  count={running.local_commands}
                  label={t('appUpdate:commands', { count: running.local_commands })}
                  after={t('appUpdate:commandsAfter', { count: running.local_commands })}
                />
              </>
            )}
          </>
        ) : (
          <li className="app-update-line">
            <span>{t(pendingBodyKey(state))}</span>
          </li>
        )}
      </ul>
      <div className="app-update-buttons">
        <button type="button" className="app-update-btn tap-target" onClick={onLater}>
          {t('appUpdate:later')}
        </button>
        <button
          type="button"
          className="app-update-btn app-update-btn--primary tap-target"
          disabled={downloading || counting}
          onClick={() => {
            if (!staysOpen) onLater();
            void installUpdate();
          }}
        >
          {t(ready ? 'appUpdate:restartNow' : downloading ? 'appUpdate:downloadingButton' : 'appUpdate:download')}
        </button>
      </div>
      {ready && state.installsOnQuit !== false && <div className="app-update-foot">{t('appUpdate:installsOnQuit')}</div>}
      <AutomaticSwitch state={state} />
    </div>
  );
}

const ROW_LABEL = {
  available: 'appUpdate:available',
  downloading: 'appUpdate:downloading',
  ready: 'appUpdate:ready',
} as const;

/**
 * The desktop app's update, offered in the sidebar once there is one to take,
 * as a row above the account. The click opens what a restart will do before
 * doing it. `shown` is false while the sidebar is hidden, where the row still
 * draws for a peek but the shell's own prompt is the one the person sees.
 */
export default function AppUpdate({ shown }: { shown: boolean }) {
  const { t } = useTranslation();
  const state = useAppUpdate(shown);
  const [open, setOpen] = useState(false);
  if (!state || !state.version || state.status === 'idle') return null;
  const label = t(ROW_LABEL[state.status]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button type="button" className="sidebar-update-row tap-target">
          <span className="sidebar-update-icon" aria-hidden="true">
            <ArrowUp />
          </span>
          <span className="sidebar-update-label">{label}</span>
          <span className="sidebar-update-version">{state.version.replace(/-nightly\..*$/, '')}</span>
        </button>
      </PopoverTrigger>
      <PopoverContent side="right" align="end" sideOffset={10} className="w-72 p-3">
        <UpdatePanel state={state} onLater={() => setOpen(false)} />
      </PopoverContent>
    </Popover>
  );
}
