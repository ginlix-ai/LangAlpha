import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from 'react';
import { useLocation, useNavigate } from 'react-router';
import { useUser } from '@/hooks/useUser';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { useIsMobile } from '@/hooks/useIsMobile';
import { isDesktopShell } from '@/lib/desktop';
import {
  isPersonalizationSnoozed,
  subscribePersonalizationSnooze,
} from '@/pages/Dashboard/hooks/useOnboarding';
import { useOnboardingPrefs } from './useOnboardingPrefs';
import { useFirstTurnThread } from './useFirstTurnThread';
import { useThisMacTask } from './useThisMacTask';
import { shouldAutoOpenWelcome } from './welcome/autoOpen';
import { WELCOME_PATH, isLegacyOnboardingSearch, legacyOnboardingTarget } from './welcome/paths';
import { readMirror, type OnboardingMirror } from './mirror';
import { ANNOUNCEMENTS, DEPLOYED_INTROS, DEPLOYED_TASKS, GETTING_STARTED_TASKS } from './registry';
import { unseenReleases, latestReleaseVersion } from './engine/whatsNew';
import {
  useOnboardingOrchestrator,
  type OnboardingPhase,
} from './engine/useOnboardingOrchestrator';
import type { AnnouncementDef, DoneSignal, GettingStartedTaskDef, PageIntroDef } from './registry';

export interface GettingStartedTaskState {
  def: GettingStartedTaskDef;
  done: boolean;
}

interface GettingStartedState {
  visible: boolean;
  tasks: GettingStartedTaskState[];
  doneCount: number;
  dismiss: () => void;
  /** Stamp a task done (external tasks complete on click — no route to
   *  observe). Resolves once the stamp has landed or failed; never rejects. */
  completeTask: (id: string) => Promise<void>;
}

interface OnboardingContextValue {
  phase: OnboardingPhase;
  unseen: AnnouncementDef[];
  // page intros
  activeIntro: PageIntroDef | null;
  dismissPageIntro: () => void;
  /** The open intro could not show: closes it unseen, for this session only. */
  abandonPageIntro: () => void;
  // getting-started checklist
  gettingStarted: GettingStartedState;
  // What's-New
  acknowledgeWhatsNew: () => void;
  // settings affordances — return false when the persist write was refused
  // (cold cache), so Settings can skip a "done" toast it can't honor.
  replayGuides: () => boolean;
  resetOnboarding: () => boolean;
  /** The user passed on the welcome flow: it never opens by itself again. */
  skipWelcome: () => void;
}

const OnboardingContext = createContext<OnboardingContextValue | null>(null);

// eslint-disable-next-line react-refresh/only-export-components
export function useOnboarding(): OnboardingContextValue {
  const ctx = useContext(OnboardingContext);
  if (!ctx) throw new Error('useOnboarding must be used within <OnboardingProvider>');
  return ctx;
}

export function OnboardingProvider({ children }: { children: ReactNode }) {
  const { pathname, search, state } = useLocation();
  const navigate = useNavigate();
  const { user } = useUser();
  const {
    prefs,
    isLoading,
    markPageIntroSeen,
    markTaskDone,
    dismissGettingStarted,
    replayGuides: replayGuidesPrefs,
    setLastSeenReleaseVersion,
    ensureFirstRun,
    markWelcomeSkipped,
    resetAll,
  } = useOnboardingPrefs();

  // Synchronous, read once the user resolves: anti-flash suppression source
  // for page intros. The mirror is keyed per user, so the read waits for the
  // user id; until then intros stay suppressed (mirror "not ready"), which
  // preserves the suppress-only invariant. Lazy init — useRef(readMirror())
  // would re-read+parse localStorage on every render of this shell-wrapping
  // provider (it re-renders per route change).
  const userId = user?.user_id ?? null;
  const mirrorRef = useRef<OnboardingMirror | null | undefined>(undefined);
  if (mirrorRef.current === undefined && userId) mirrorRef.current = readMirror(userId);

  // Intros opened this session. Authoritative the instant one closes, unlike
  // the async prefs write + mount-time mirror — prevents the orchestrator from
  // re-opening an intro the moment it's dismissed (loop).
  const shownIntrosRef = useRef(new Set<string>());

  const [phase, setPhase] = useState<OnboardingPhase>('idle');
  const [activeIntroId, setActiveIntroId] = useState<string | null>(null);

  const unseen = useMemo(() => unseenReleases(prefs, ANNOUNCEMENTS), [prefs]);

  // Stamp first-run once prefs resolve, so brand-new users start "caught up"
  // (page intros, not a What's-New backlog). It waits for the account too.
  // After an in-place account switch the next account's prefs can land before
  // its user row does, since the sign-in sync seeds them, and a stamp sent in
  // that gap would land before `firstRun` below could read the first visit.
  const firstRunStampedRef = useRef(false);
  useEffect(() => {
    if (isLoading || !userId || firstRunStampedRef.current) return;
    if (prefs.firstRunAt === null) {
      firstRunStampedRef.current = true;
      // Re-arm on a refused or failed write so the stamp isn't lost for the
      // whole session — the next prefs/route change retries it.
      const accepted = ensureFirstRun(latestReleaseVersion(ANNOUNCEMENTS), {
        onError: () => {
          firstRunStampedRef.current = false;
        },
      });
      if (!accepted) firstRunStampedRef.current = false;
    }
  }, [isLoading, userId, prefs.firstRunAt, ensureFirstRun]);

  // Whether the engine had never run for this account when its prefs first
  // loaded this session, which is what makes this visit the account's first.
  // Captured before the stamp above lands, and kept for the session. It is
  // held with the account it was read for. Signing in to another account
  // without signing out clears the cache in place (AuthContext) and leaves
  // this provider mounted, so the last account's answer must not stand for
  // the next one: that one is read again once its own prefs have loaded.
  const [firstRunRead, setFirstRunRead] = useState<{ userId: string; firstRun: boolean } | null>(null);
  if (userId && !isLoading && firstRunRead?.userId !== userId) {
    setFirstRunRead({ userId, firstRun: prefs.firstRunAt === null });
  }
  const firstRun = firstRunRead?.userId === userId ? firstRunRead.firstRun : null;

  // A new account's first visit opens the welcome flow over the dashboard,
  // once. Any visit to the flow this session counts as that once, so leaving
  // it for the dashboard before the user row catches up cannot reopen it.
  // The guard holds the account it was set for, for the same in-place switch
  // as `firstRun`: another account signing in gets a first visit of its own.
  const welcomeOpenedForRef = useRef<string | null>(null);
  useEffect(() => {
    // Nothing is recorded without an account. The user is a dependency, so
    // a visit made while it loads is recorded once it arrives.
    if (!user) return;
    if (pathname === WELCOME_PATH) {
      welcomeOpenedForRef.current = user.user_id;
      return;
    }
    if (welcomeOpenedForRef.current === user.user_id || firstRun === null) return;
    const open = shouldAutoOpenWelcome({
      pathname,
      firstRun,
      onboardingCompleted: user.onboarding_completed === true,
      welcomeSkippedAt: prefs.welcomeSkippedAt,
      createdAt: user.created_at,
      now: Date.now(),
    });
    if (!open) return;
    welcomeOpenedForRef.current = user.user_id;
    navigate(WELCOME_PATH, { replace: true });
  }, [pathname, firstRun, user, prefs.welcomeSkippedAt, navigate]);

  // The brokerage sheet this flow replaced opened on `?onboarding=connect`;
  // a link or a sign-in from an older build still lands there.
  useEffect(() => {
    if (isLegacyOnboardingSearch(search)) navigate(legacyOnboardingTarget(search), { replace: true });
  }, [search, navigate]);

  // First intro for the current route that nothing (server, mirror, session)
  // has recorded as seen. The mirror only suppresses — never triggers.
  // Computed per render, NOT memoized: it reads two refs (mirror, session
  // guard) that a dismissal mutates without changing pathname/prefs — a memo
  // would serve the stale pre-dismiss intro and re-open it.
  // Page intros are a desktop surface: the visual half is hidden on small
  // screens (copy would read as captions for invisible mockups), matching the
  // desktop-only getting-started list. Suppressed — never marked seen — so a
  // later desktop session still offers them.
  const isMobile = useIsMobile();
  const mirror = mirrorRef.current ?? null;
  const mirrorReady = mirrorRef.current !== undefined;
  const eligibleIntro = mirrorReady && !isMobile
    ? (DEPLOYED_INTROS.find(
        (intro) =>
          intro.matchRoute(pathname) &&
          prefs.pageIntrosSeen[intro.id] == null &&
          mirror?.pageIntrosSeen?.[intro.id] == null &&
          !shownIntrosRef.current.has(intro.id)
      ) ?? null)
    : null;

  const activeIntro = useMemo(
    () => DEPLOYED_INTROS.find((intro) => intro.id === activeIntroId) ?? null,
    [activeIntroId]
  );

  const dismissPageIntro = useCallback(() => {
    if (activeIntroId !== null) {
      shownIntrosRef.current.add(activeIntroId);
      markPageIntroSeen(activeIntroId);
    }
    setActiveIntroId(null);
    setPhase('idle');
  }, [activeIntroId, markPageIntroSeen]);

  // The intro could not show (its copy did not load, as when a deploy retired
  // the chunk). Close WITHOUT marking seen, and hold it off for the session so
  // the orchestrator does not reopen it in a loop; the reload the stale-build
  // notice offers brings it back.
  const abandonPageIntro = useCallback(() => {
    if (activeIntroId !== null) shownIntrosRef.current.add(activeIntroId);
    setActiveIntroId(null);
    setPhase('idle');
  }, [activeIntroId]);

  const acknowledgeWhatsNew = useCallback(() => {
    const latest = latestReleaseVersion(ANNOUNCEMENTS);
    if (latest) setLastSeenReleaseVersion(latest);
    setPhase('idle');
  }, [setLastSeenReleaseVersion]);

  // An intro must not outlive its page: browser back/forward or a programmatic
  // redirect can change the route under an open intro. Close WITHOUT marking
  // seen — the user didn't finish it, so it re-offers on its own page.
  useEffect(() => {
    if (phase === 'pageIntro' && activeIntro && !activeIntro.matchRoute(pathname)) {
      setActiveIntroId(null);
      setPhase('idle');
    }
  }, [phase, activeIntro, pathname]);

  // Cross-tab acknowledge: another tab stamping What's-New empties `unseen`
  // while this tab's modal is open. The modal renders null on an empty list
  // and only acknowledging leaves the phase — reset to idle so this tab isn't
  // stuck popup-less (which would block every intro for the session).
  useEffect(() => {
    if (phase === 'whatsNew' && unseen.length === 0) setPhase('idle');
  }, [phase, unseen.length]);

  /** Re-show every page tip + the getting-started list (Settings affordance). */
  const replayGuides = useCallback((): boolean => {
    // Persist first: a refused write (cold cache) makes this a no-op rather
    // than clearing local state for a change the server never recorded — the
    // caller then skips the toast and the user can retry once prefs settle.
    if (!replayGuidesPrefs()) return false;
    // Eligibility checks the mount-time mirror snapshot and the session guard
    // before prefs, so without clearing both the replay would silently
    // suppress the very tips it just cleared until reload.
    shownIntrosRef.current.clear();
    mirrorRef.current = null;
    setActiveIntroId(null);
    setPhase('idle');
    return true;
  }, [replayGuidesPrefs]);

  const resetOnboarding = useCallback((): boolean => {
    if (!resetAll()) return false;
    shownIntrosRef.current.clear();
    mirrorRef.current = null;
    // Re-arm the first-run stamp so a reset behaves like a fresh user in this
    // session (caught-up What's-New stamp re-applies once the reset settles).
    firstRunStampedRef.current = false;
    setActiveIntroId(null);
    setPhase('idle');
    return true;
  }, [resetAll]);

  // A new chat's first turn, which no intro pops over, and the welcome flow's
  // conversation, which the getting-started list stays out of too.
  const { firstTurn, welcome: inWelcomeChat } = useFirstTurnThread(pathname, state);

  // Reactive snooze: snoozing the dashboard banner fires an event; without it
  // the provider wouldn't re-render and the dashboard intro would stay
  // suppressed until some unrelated navigation.
  const personalizationSnoozed = useSyncExternalStore(
    subscribePersonalizationSnooze,
    isPersonalizationSnoozed
  );

  // Suppress while onboarding owns the screen: the welcome flow, a new chat's
  // first turn (the welcome flow's among them), and the dashboard while its
  // banner offers the flow (unless snoozed). Read off `onboarding_completed`
  // alone, as the banner is: `personalization_completed` means the user has a
  // key of their own.
  const onboardingCompleted = user?.onboarding_completed === true;
  const suppress =
    firstTurn ||
    pathname === WELCOME_PATH ||
    isLegacyOnboardingSearch(search) ||
    (!onboardingCompleted && pathname === '/dashboard' && !personalizationSnoozed);

  // Getting-started: auto-complete route-visit tasks. markTaskDone owns the
  // once-per-task guard, so a refused write is not re-sent on the prefs
  // identities its rollback and refetch produce.
  useEffect(() => {
    if (isLoading) return;
    for (const task of DEPLOYED_TASKS) {
      if (!task.visitRoute?.(pathname, search)) continue;
      if (prefs.gettingStartedDoneAt[task.id] != null) continue;
      markTaskDone(task.id);
    }
  }, [isLoading, pathname, search, prefs, markTaskDone]);

  // Derived signals. The workspace probe is its own lightweight query (limit:1
  // keys its own cache entry, distinct from the gallery's limit:20), and it and
  // this Mac's reads run only while their task is still open: once true, a
  // signal is stamped, so later sessions skip the lookup.
  const listShown = !isLoading && prefs.gettingStartedDismissedAt === null;
  const stamped = (id: string) => prefs.gettingStartedDoneAt[id] != null;
  const { data: workspacesData } = useWorkspaces({
    limit: 1,
    enabled: listShown && !stamped('createWorkspace'),
  });
  const thisMac = useThisMacTask(listShown && !stamped('remoteControl'));
  const signals: Record<DoneSignal, boolean> = {
    hasWorkspace: (workspacesData?.workspaces?.length ?? 0) > 0,
    onboardingCompleted,
    thisMacAdded: thisMac.added,
    inDesktopApp: !isLoading && isDesktopShell(),
  };
  useEffect(() => {
    // Every task, not only this app's: the desktop app stamps the task to get
    // it, which only a browser's list shows.
    for (const task of GETTING_STARTED_TASKS) {
      // A saved profile is read live from the user row, so a reset un-checks it.
      if (!task.doneWhen || task.doneWhen === 'onboardingCompleted' || !signals[task.doneWhen]) continue;
      if (prefs.gettingStartedDoneAt[task.id] == null) markTaskDone(task.id);
    }
  }, [signals, prefs, markTaskDone]);

  const gettingStarted = useMemo<GettingStartedState>(() => {
    // This Mac's task shows where this Mac can be a computer, and stays once done.
    const offered = DEPLOYED_TASKS.filter((def) => def.offeredOn !== 'thisMac' || thisMac.offered || stamped(def.id));
    const tasks = offered.map((def) => ({
      def,
      done:
        def.doneWhen === 'onboardingCompleted'
          ? onboardingCompleted
          : stamped(def.id) || (!!def.doneWhen && signals[def.doneWhen]),
    }));
    const doneCount = tasks.filter((t) => t.done).length;
    return {
      // Out of the welcome flow's conversation: the user is answering the
      // agent there, and the list comes back on the next page.
      visible:
        !isLoading &&
        !inWelcomeChat &&
        prefs.gettingStartedDismissedAt === null &&
        doneCount < tasks.length,
      tasks,
      doneCount,
      dismiss: dismissGettingStarted,
      completeTask: markTaskDone,
    };
  }, [prefs, onboardingCompleted, signals, thisMac.offered, isLoading, inWelcomeChat, dismissGettingStarted, markTaskDone]);

  useOnboardingOrchestrator({
    isLoading,
    phase,
    pathname,
    eligibleIntroId: eligibleIntro?.id ?? null,
    hasUnseenWhatsNew: unseen.length > 0,
    suppress,
    showPageIntro: () => {
      if (eligibleIntro) {
        setActiveIntroId(eligibleIntro.id);
        setPhase('pageIntro');
      }
    },
    showWhatsNew: () => setPhase('whatsNew'),
  });

  const value = useMemo<OnboardingContextValue>(
    () => ({
      phase,
      unseen,
      activeIntro,
      dismissPageIntro,
      abandonPageIntro,
      gettingStarted,
      acknowledgeWhatsNew,
      replayGuides,
      resetOnboarding,
      skipWelcome: markWelcomeSkipped,
    }),
    [
      phase,
      unseen,
      activeIntro,
      dismissPageIntro,
      abandonPageIntro,
      gettingStarted,
      acknowledgeWhatsNew,
      replayGuides,
      resetOnboarding,
      markWelcomeSkipped,
    ]
  );

  return <OnboardingContext value={value}>{children}</OnboardingContext>;
}
