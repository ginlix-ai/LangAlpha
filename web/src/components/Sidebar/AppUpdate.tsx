import React, { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ArrowUp } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { getRunningWork, type RunningWork } from '@/api/runningWork';
import { useLocale } from '@/hooks/useLocale';
import { canShowUpdates, getUpdateState, installUpdate, onUpdateState, type DesktopUpdateState } from '@/lib/desktop';
import { calendarMonthDay } from '@/lib/format';
import { queryKeys } from '@/lib/queryKeys';

/** The desktop app's update as its shell reports it; null outside the app or on a shell that cannot say. */
function useAppUpdate(): DesktopUpdateState | null {
  const [state, setState] = useState<DesktopUpdateState | null>(null);
  useEffect(() => {
    if (!canShowUpdates()) return undefined;
    let live = true;
    const off = onUpdateState((next) => setState(next));
    void getUpdateState().then((first) => {
      if (live && first) setState((held) => held ?? first);
    });
    return () => {
      live = false;
      off();
    };
  }, []);
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
    return t('appUpdate.nightlyLabel', { version: base, date: calendarMonthDay(built, locale) });
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

function UpdatePanel({ state, onLater }: { state: DesktopUpdateState; onLater: () => void }) {
  const { t } = useTranslation();
  const label = useVersionLabel();
  const ready = state.status === 'ready';
  // Asked only where a restart stops the server, and fresh at each open: it is
  // what is running now that the person weighs.
  const { data: running } = useQuery<RunningWork>({
    queryKey: queryKeys.user.runningWork(),
    queryFn: getRunningWork,
    enabled: ready && state.stopsServer,
    staleTime: 0,
    gcTime: 0,
  });
  const version = label(state.version ?? '');

  return (
    <div className="app-update-panel">
      <div className="app-update-title">
        {t(ready ? 'appUpdate.readyTitle' : 'appUpdate.availableTitle', { version })}
      </div>
      <div className="app-update-sub">
        {t('appUpdate.youHave', { version: label(state.currentVersion) })}
        {state.notesUrl && (
          <>
            {' · '}
            {/* The shell sends a page's external link to the system browser. */}
            <a className="app-update-link" href={state.notesUrl} target="_blank" rel="noreferrer">
              {t('appUpdate.whatsNew')}
            </a>
          </>
        )}
      </div>
      <ul className="app-update-lines">
        {ready ? (
          <>
            <li className="app-update-line">
              <span>{t(state.stopsServer ? 'appUpdate.relaunch' : 'appUpdate.relaunchRemote')}</span>
            </li>
            {state.stopsServer && running && (
              <>
                <Consequence
                  count={running.turns}
                  label={t('appUpdate.turns', { count: running.turns })}
                  after={t('appUpdate.turnsAfter', { count: running.turns })}
                />
                <Consequence
                  count={running.background_tasks}
                  label={t('appUpdate.tasks', { count: running.background_tasks })}
                  after={t('appUpdate.tasksAfter', { count: running.background_tasks })}
                />
                <Consequence
                  count={running.local_commands}
                  label={t('appUpdate.commands', { count: running.local_commands })}
                  after={t('appUpdate.commandsAfter', { count: running.local_commands })}
                />
              </>
            )}
          </>
        ) : (
          <li className="app-update-line">
            <span>{t('appUpdate.notifyBody')}</span>
          </li>
        )}
      </ul>
      <div className="app-update-buttons">
        <button type="button" className="app-update-btn" onClick={onLater}>
          {t('appUpdate.later')}
        </button>
        <button
          type="button"
          className="app-update-btn app-update-btn--primary"
          onClick={() => {
            onLater();
            void installUpdate();
          }}
        >
          {t(ready ? 'appUpdate.restartNow' : 'appUpdate.download')}
        </button>
      </div>
      {ready && <div className="app-update-foot">{t('appUpdate.installsOnQuit')}</div>}
    </div>
  );
}

/**
 * The desktop app's update, offered in the sidebar once there is one to take:
 * a row above the account in the panel, an icon above the avatar on the rail.
 * The click opens what a restart will do before doing it.
 */
export default function AppUpdate({ variant }: { variant: 'row' | 'rail' }) {
  const { t } = useTranslation();
  const state = useAppUpdate();
  const [open, setOpen] = useState(false);
  if (!state || !state.version || (state.status !== 'ready' && state.status !== 'available')) return null;
  const label = t(state.status === 'ready' ? 'appUpdate.ready' : 'appUpdate.available');

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        {variant === 'row' ? (
          <button type="button" className="sidebar-update-row tap-target">
            <span className="sidebar-update-icon" aria-hidden="true">
              <ArrowUp />
            </span>
            <span className="sidebar-update-label">{label}</span>
            <span className="sidebar-update-version">{state.version.replace(/-nightly\..*$/, '')}</span>
          </button>
        ) : (
          <button type="button" className="sidebar-update-rail tap-target" aria-label={label} title={label}>
            <span className="sidebar-update-icon" aria-hidden="true">
              <ArrowUp />
            </span>
          </button>
        )}
      </PopoverTrigger>
      <PopoverContent side="right" align="end" sideOffset={10} className="w-72 p-3">
        <UpdatePanel state={state} onLater={() => setOpen(false)} />
      </PopoverContent>
    </Popover>
  );
}
