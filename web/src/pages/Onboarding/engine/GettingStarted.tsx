import { useEffect, useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { ArrowUpRight, Check, ChevronDown, ChevronRight } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { isDesktopShell } from '@/lib/desktop';
import { cn, safeLocalStorage } from '@/lib/utils';
import { useOnboarding, type GettingStartedTaskState } from '../OnboardingProvider';
import type { GettingStartedTaskDef } from '../registry';
import './GettingStarted.css';

/** The longest a hand-off to the account portal waits for its task's stamp. */
const STAMP_WAIT_MS = 1500;

/**
 * Whether the list is folded open; a per-browser convenience, open by default.
 * Blocked storage reads as open, and the list still folds, forgetting on reload.
 */
const OPEN_KEY = 'onboarding.guideOpen';

const readOpen = () => safeLocalStorage.getItem(OPEN_KEY) !== '0';
const writeOpen = (open: boolean) => safeLocalStorage.setItem(OPEN_KEY, open ? '1' : '0');

/**
 * Set when the rail's ring expands the sidebar. The ring unmounts with the
 * rail, so the section that replaces it takes focus to the next task rather
 * than leaving it on the page body.
 */
let focusNextOnMount = false;

/** Progress as a ring, drawn in the sidebar where an icon would be. */
function ProgressRing({
  done,
  total,
  className,
}: {
  done: number;
  total: number;
  className?: string;
}) {
  const r = 7;
  const length = 2 * Math.PI * r;
  return (
    <svg viewBox="0 0 18 18" className={className} aria-hidden>
      <circle
        cx="9"
        cy="9"
        r={r}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        opacity={0.25}
      />
      {done > 0 && (
        <circle
          cx="9"
          cy="9"
          r={r}
          fill="none"
          strokeWidth="2"
          strokeLinecap="round"
          strokeDasharray={length}
          strokeDashoffset={length * (1 - done / total)}
          transform="rotate(-90 9 9)"
          className="sidebar-guide-ring-arc"
          style={{ stroke: 'var(--color-accent-primary)' }}
        />
      )}
    </svg>
  );
}

function TaskMark({ done, next }: { done: boolean; next: boolean }) {
  if (done) {
    return (
      <span
        className="sidebar-guide-mark flex items-center justify-center"
        style={{ backgroundColor: 'var(--color-accent-primary)' }}
        aria-hidden
      >
        <Check
          className="h-2.5 w-2.5"
          strokeWidth={3.5}
          style={{ color: 'var(--color-text-on-accent)' }}
        />
      </span>
    );
  }
  return (
    <span
      className="sidebar-guide-mark border-[1.5px]"
      style={{
        borderColor: next ? 'var(--color-accent-primary)' : 'var(--color-border-strong)',
      }}
      aria-hidden
    />
  );
}

/**
 * The checklist inside the sidebar, above the account: a header with the
 * progress that folds the list away, then what is done, what comes next with
 * a line on it, and the rest by name. It lives in the sidebar so it never
 * covers the page. Presentational, so the dev preview draws it without the
 * provider.
 */
export function GettingStartedSection({
  tasks,
  doneCount,
  onSelect,
  onDismiss,
  defaultOpen,
}: {
  tasks: GettingStartedTaskState[];
  doneCount: number;
  onSelect: (def: GettingStartedTaskDef) => void;
  onDismiss: () => void;
  /** Overrides the remembered fold, for the dev preview. */
  defaultOpen?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(() => defaultOpen ?? readOpen());
  const listId = useId();
  const nextRef = useRef<HTMLButtonElement>(null);
  const next = tasks.findIndex((task) => !task.done);
  // Only the browser takes a cross-app task to a tab of its own.
  const newTab = !isDesktopShell();
  const title = t('onboarding.gettingStarted.title');
  const progress = t('onboarding.gettingStarted.progress', {
    done: doneCount,
    total: tasks.length,
  });

  useEffect(() => {
    if (!focusNextOnMount) return;
    focusNextOnMount = false;
    nextRef.current?.focus();
  }, []);

  const toggle = () => {
    setOpen(!open);
    writeOpen(!open);
  };

  return (
    <section className="sidebar-guide" aria-label={title}>
      <button
        type="button"
        className="sidebar-guide-head"
        aria-expanded={open}
        aria-controls={listId}
        aria-label={`${title}, ${progress}`}
        onClick={toggle}
      >
        <ProgressRing done={doneCount} total={tasks.length} className="sidebar-panel-item-icon" />
        <span className="min-w-0 flex-1 truncate text-left">{title}</span>
        <span className="sidebar-guide-count">
          {doneCount}/{tasks.length}
        </span>
        <ChevronDown className="sidebar-guide-chevron" aria-hidden />
      </button>

      <div
        id={listId}
        className="sidebar-guide-body"
        data-open={open}
        inert={!open}
        aria-hidden={!open}
      >
        <div className="sidebar-guide-scroll">
          <ul className="flex flex-col gap-px">
            {tasks.map(({ def, done }, i) => {
              const isNext = i === next;
              return (
                <li key={def.id}>
                  <button
                    ref={isNext ? nextRef : undefined}
                    type="button"
                    disabled={done}
                    onClick={() => onSelect(def)}
                    className="sidebar-guide-task group"
                    data-next={isNext || undefined}
                  >
                    <TaskMark done={done} next={isNext} />
                    <span className="min-w-0 flex-1">
                      <span className="sidebar-guide-task-title">{t(def.titleKey)}</span>
                      {isNext && <span className="sidebar-guide-task-desc">{t(def.descKey)}</span>}
                    </span>
                    {!done &&
                      (def.external && newTab ? (
                        <ArrowUpRight className="sidebar-guide-go" aria-hidden />
                      ) : (
                        <ChevronRight
                          className={cn(
                            'sidebar-guide-go',
                            !isNext &&
                              'opacity-0 group-hover:opacity-100 group-focus-visible:opacity-100'
                          )}
                          aria-hidden
                        />
                      ))}
                  </button>
                </li>
              );
            })}
          </ul>
          <button type="button" className="sidebar-guide-hide" onClick={onDismiss}>
            {t('onboarding.gettingStarted.dismiss')}
          </button>
        </div>
      </div>
    </section>
  );
}

/**
 * The rail's version: just the ring above the avatar. There is no room for
 * the list beside an icon rail, and a list floating over the page would cover
 * it, so the ring opens the sidebar with the list unfolded.
 */
export function GettingStartedRail({
  doneCount,
  total,
  onOpen,
}: {
  doneCount: number;
  total: number;
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const title = t('onboarding.gettingStarted.title');
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="sidebar-nav-item sidebar-guide-rail"
            aria-label={`${title}, ${t('onboarding.gettingStarted.progress', {
              done: doneCount,
              total,
            })}`}
            onClick={() => {
              writeOpen(true);
              focusNextOnMount = true;
              onOpen();
            }}
          >
            <ProgressRing done={doneCount} total={total} className="sidebar-nav-icon" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="right" sideOffset={8}>
          {title} · {doneCount}/{total}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

export type GettingStartedLauncherProps =
  | { variant: 'row' }
  | { variant: 'rail'; onExpand: () => void };

/**
 * The checklist, wired to the onboarding state: the section in the expanded
 * sidebar, the ring in the rail (`onExpand` opens the sidebar). Mounted by
 * GettingStartedGate, only while the list is visible.
 *
 * Tasks complete via route visits or a derived signal (the saved investing
 * profile, a workspace); picking a pending one goes to it.
 */
export function GettingStartedLauncher(props: GettingStartedLauncherProps) {
  const { gettingStarted } = useOnboarding();
  const navigate = useNavigate();
  const { tasks, doneCount, dismiss, completeTask } = gettingStarted;

  if (props.variant === 'rail') {
    return <GettingStartedRail doneCount={doneCount} total={tasks.length} onOpen={props.onExpand} />;
  }

  const select = (def: GettingStartedTaskDef) => {
    if (!def.external) {
      def.open?.();
      navigate(def.to);
      return;
    }
    // A task with its own signal is done by that, not by the visit.
    const stamps = !def.doneWhen;
    if (isDesktopShell()) {
      // The portal shares this window in the desktop app, as Usage & Plan
      // does, and the shell answers a window.open with a window of its own or
      // the browser. Leaving unloads the page, so the stamp goes first.
      const waited = new Promise<void>((resolve) => setTimeout(resolve, STAMP_WAIT_MS));
      void Promise.race([stamps ? completeTask(def.id) : undefined, waited]).then(() => window.location.assign(def.to));
      return;
    }
    // Cross-app page — no route of ours to observe, so a successful open is
    // the completion signal. null = popup blocked; leave the task pending so
    // the user can retry. No 'noopener' feature string: that makes open()
    // return null even on success, which would read as "blocked" and leave the
    // task permanently pending. Sever the opener on the returned handle
    // instead.
    const win = window.open(def.to, '_blank');
    if (win) {
      win.opener = null;
      if (stamps) completeTask(def.id);
    }
  };

  return (
    <GettingStartedSection
      tasks={tasks}
      doneCount={doneCount}
      onSelect={select}
      onDismiss={dismiss}
    />
  );
}
