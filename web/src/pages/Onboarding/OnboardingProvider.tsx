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
import { useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { useIsMobile } from '@/hooks/useIsMobile';
import {
  isPersonalizationSnoozed,
  subscribePersonalizationSnooze,
} from '@/pages/Dashboard/hooks/useOnboarding';
import { listWatchlists, listWatchlistItems, listPortfolio } from '@/pages/Dashboard/utils/api';
import { isPlatformMode } from '@/config/hostMode';
import { useOnboardingPrefs } from './useOnboardingPrefs';
import { readMirror, type OnboardingMirror } from './mirror';
import { ANNOUNCEMENTS, PAGE_INTROS, GETTING_STARTED_TASKS } from './registry';
import { unseenReleases, latestReleaseVersion } from './engine/whatsNew';
import {
  useOnboardingOrchestrator,
  type OnboardingPhase,
} from './engine/useOnboardingOrchestrator';
import type { AnnouncementDef, GettingStartedTaskDef, PageIntroDef } from './registry';

export interface GettingStartedTaskState {
  def: GettingStartedTaskDef;
  done: boolean;
}

interface GettingStartedState {
  visible: boolean;
  tasks: GettingStartedTaskState[];
  doneCount: number;
  dismiss: () => void;
  /** Stamp a task done (external tasks complete on click — no route to observe). */
  completeTask: (id: string) => void;
}

/** Tasks offered in this deployment — channel integrations are platform-hosted. */
const OFFERED_TASKS = GETTING_STARTED_TASKS.filter((t) => !t.platformOnly || isPlatformMode);

/** Any non-empty string value in a preference object (mirrors the backend's check). */
function hasFilledField(section: unknown): boolean {
  return (
    typeof section === 'object' &&
    section !== null &&
    Object.values(section as Record<string, unknown>).some(
      (v) => typeof v === 'string' && v.trim() !== ''
    )
  );
}

interface OnboardingContextValue {
  phase: OnboardingPhase;
  unseen: AnnouncementDef[];
  // page intros
  activeIntro: PageIntroDef | null;
  dismissPageIntro: () => void;
  // getting-started checklist
  gettingStarted: GettingStartedState;
  // What's-New
  acknowledgeWhatsNew: () => void;
  // settings affordances — return false when the persist write was refused
  // (cold cache), so Settings can skip a "done" toast it can't honor.
  replayGuides: () => boolean;
  resetOnboarding: () => boolean;
}

const OnboardingContext = createContext<OnboardingContextValue | null>(null);

// eslint-disable-next-line react-refresh/only-export-components
export function useOnboarding(): OnboardingContextValue {
  const ctx = useContext(OnboardingContext);
  if (!ctx) throw new Error('useOnboarding must be used within <OnboardingProvider>');
  return ctx;
}

export function OnboardingProvider({ children }: { children: ReactNode }) {
  const { pathname, search } = useLocation();
  const { user } = useUser();
  const { preferences } = usePreferences();
  const {
    prefs,
    isLoading,
    markPageIntroSeen,
    markTaskDone,
    dismissGettingStarted,
    replayGuides: replayGuidesPrefs,
    setLastSeenReleaseVersion,
    ensureFirstRun,
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
  // (page intros, not a What's-New backlog).
  const firstRunStampedRef = useRef(false);
  useEffect(() => {
    if (isLoading || firstRunStampedRef.current) return;
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
  }, [isLoading, prefs.firstRunAt, ensureFirstRun]);

  // First intro for the current route that nothing (server, mirror, session)
  // has recorded as seen. The mirror only suppresses — never triggers.
  // Computed per render, NOT memoized: it reads two refs (mirror, session
  // guard) that a dismissal mutates without changing pathname/prefs — a memo
  // would serve the stale pre-dismiss intro and re-open it.
  // Page intros are a desktop surface: the visual half is hidden on small
  // screens (copy would read as captions for invisible mockups), matching the
  // desktop-only getting-started card. Suppressed — never marked seen — so a
  // later desktop session still offers them.
  const isMobile = useIsMobile();
  const mirror = mirrorRef.current ?? null;
  const mirrorReady = mirrorRef.current !== undefined;
  const eligibleIntro = mirrorReady && !isMobile
    ? (PAGE_INTROS.find(
        (intro) =>
          intro.matchRoute(pathname) &&
          prefs.pageIntrosSeen[intro.id] == null &&
          mirror?.pageIntrosSeen?.[intro.id] == null &&
          !shownIntrosRef.current.has(intro.id)
      ) ?? null)
    : null;

  const activeIntro = useMemo(
    () => PAGE_INTROS.find((intro) => intro.id === activeIntroId) ?? null,
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

  /** Re-show every page tip + the getting-started card (Settings affordance). */
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

  // The interview thread starts as /chat/t/__default__ but is renamed to a
  // real thread id on the first message — and that navigate drops the
  // personalization route state. Remember WHICH thread the interview became
  // and suppress intros only there: the thread intro can't pop mid-interview,
  // but a different thread opened later the same session shows it normally.
  const interviewThreadRef = useRef<string | null>(null);
  const wasInInterviewRef = useRef(false);
  const threadId = pathname.match(/^\/chat\/t\/([^/]+)/)?.[1] ?? null;
  if (threadId === '__default__') {
    wasInInterviewRef.current = true;
  } else if (wasInInterviewRef.current) {
    // First route after the interview: the rename lands on the real thread id
    // (null when the user left the thread view without sending a message).
    interviewThreadRef.current = threadId;
    wasInInterviewRef.current = false;
  }
  const inInterview =
    threadId === '__default__' ||
    (threadId !== null && threadId === interviewThreadRef.current);

  // Reactive snooze: snoozing the dashboard banner fires an event; without it
  // the provider wouldn't re-render and the dashboard intro would stay
  // suppressed until some unrelated navigation.
  const personalizationSnoozed = useSyncExternalStore(
    subscribePersonalizationSnooze,
    isPersonalizationSnoozed
  );

  // Suppress while the personalization flow owns the screen: the dashboard
  // banner (unless snoozed), and the personalization chat itself — clicking
  // "Personalize" navigates to /chat/t/__default__ and no intro must pop over
  // it (snooze is irrelevant mid-flow).
  const personalizationCompleted =
    user?.personalization_completed === true || user?.onboarding_completed === true;
  const suppress =
    inInterview ||
    (!personalizationCompleted &&
      ((pathname === '/dashboard' && !personalizationSnoozed) ||
        pathname === '/chat/t/__default__'));

  // Getting-started: auto-complete route-visit tasks. markTaskDone aborts when
  // already stamped, so repeat visits are write-free.
  useEffect(() => {
    if (isLoading) return;
    for (const task of OFFERED_TASKS) {
      if (task.visitRoute?.(pathname, search) && prefs.gettingStartedDoneAt[task.id] == null) {
        markTaskDone(task.id);
      }
    }
  }, [isLoading, pathname, search, prefs, markTaskDone]);

  // "Tell us your preferences" — live from the prefs the interview writes, so
  // a preferences reset un-checks it. Deliberately NOT the user-row completion
  // flags: personalization_completed means "has BYOK key", and a reset clears
  // onboarding_completed without clearing this data's siblings.
  const hasPreferences = useMemo(
    () =>
      ['risk_preference', 'investment_preference', 'agent_preference'].some((k) =>
        hasFilledField((preferences as Record<string, unknown> | null)?.[k])
      ),
    [preferences]
  );

  // "Tell us your watchlist & portfolio" — does any stock exist? Fetched only
  // while the card still needs it (not dismissed, not yet stamped); once true
  // it's stamped into prefs so later sessions skip the lookup entirely.
  const stocksStamped = prefs.gettingStartedDoneAt['stocks'] != null;
  const { data: hasStocks = false } = useQuery({
    // User-scoped key: the result must never be served to another account
    // within the same QueryClient lifetime.
    queryKey: ['onboarding', 'hasStocks', userId],
    queryFn: async () => {
      // listPortfolio is independent of the watchlists result — start it in
      // parallel so a user with no watchlists (the common new-user case)
      // resolves in one round-trip instead of listWatchlists → listPortfolio in
      // series. .catch keeps it a settled boolean so an early listWatchlists
      // rejection can't leave it floating unhandled.
      const portfolioProbe = listPortfolio()
        .then((r) => ((r as { holdings?: unknown[] }).holdings?.length ?? 0) > 0)
        .catch(() => false);
      const { watchlists } = (await listWatchlists()) as {
        watchlists?: Array<{ watchlist_id: string }>;
      };
      // Item probes fire the moment listWatchlists resolves, in parallel with
      // the still-in-flight portfolio probe.
      const itemProbes = (watchlists ?? []).map(async (wl) => {
        const { items } = (await listWatchlistItems(wl.watchlist_id)) as { items?: unknown[] };
        return (items?.length ?? 0) > 0;
      });
      // allSettled: one failed watchlist probe must not mask stocks another
      // probe found — the result is one-way and gets stamped, so a transient
      // error returning false would stick the task incomplete for the session.
      const results = await Promise.allSettled([portfolioProbe, ...itemProbes]);
      return results.some((r) => r.status === 'fulfilled' && r.value);
    },
    enabled: userId != null && !isLoading && prefs.gettingStartedDismissedAt === null && !stocksStamped,
    staleTime: 60_000,
    retry: false,
    // The probe fans out one request per watchlist; don't re-run the burst on
    // every tab refocus — the result is one-way (false → true → stamped).
    refetchOnWindowFocus: false,
  });
  useEffect(() => {
    if (hasStocks && !stocksStamped) markTaskDone('stocks');
  }, [hasStocks, stocksStamped, markTaskDone]);

  // "Create your first workspace" — any non-flash workspace exists. An
  // independent lightweight probe (limit:1 keys its own cache entry, distinct
  // from the gallery's limit:20); only fetched while the card needs it, and
  // stamped once true like the stocks task.
  const workspaceStamped = prefs.gettingStartedDoneAt['createWorkspace'] != null;
  const { data: workspacesData } = useWorkspaces({
    limit: 1,
    enabled: !isLoading && prefs.gettingStartedDismissedAt === null && !workspaceStamped,
  });
  const hasWorkspace =
    ((workspacesData as { workspaces?: unknown[] } | undefined)?.workspaces?.length ?? 0) > 0;
  useEffect(() => {
    if (hasWorkspace && !workspaceStamped) markTaskDone('createWorkspace');
  }, [hasWorkspace, workspaceStamped, markTaskDone]);

  const gettingStarted = useMemo<GettingStartedState>(() => {
    const tasks = OFFERED_TASKS.map((def) => ({
      def,
      done:
        def.doneWhen === 'hasPreferences'
          ? hasPreferences
          : prefs.gettingStartedDoneAt[def.id] != null ||
            (def.doneWhen === 'hasStocks' && hasStocks) ||
            (def.doneWhen === 'hasWorkspace' && hasWorkspace),
    }));
    const doneCount = tasks.filter((t) => t.done).length;
    return {
      visible:
        !isLoading && prefs.gettingStartedDismissedAt === null && doneCount < tasks.length,
      tasks,
      doneCount,
      dismiss: dismissGettingStarted,
      completeTask: markTaskDone,
    };
  }, [prefs, hasPreferences, hasStocks, hasWorkspace, isLoading, dismissGettingStarted, markTaskDone]);

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
      gettingStarted,
      acknowledgeWhatsNew,
      replayGuides,
      resetOnboarding,
    }),
    [
      phase,
      unseen,
      activeIntro,
      dismissPageIntro,
      gettingStarted,
      acknowledgeWhatsNew,
      replayGuides,
      resetOnboarding,
    ]
  );

  return <OnboardingContext.Provider value={value}>{children}</OnboardingContext.Provider>;
}
