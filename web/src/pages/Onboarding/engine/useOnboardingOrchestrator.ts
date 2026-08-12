import { useEffect, useRef } from 'react';

export type OnboardingPhase = 'idle' | 'pageIntro' | 'whatsNew';

interface OrchestratorParams {
  isLoading: boolean;
  phase: OnboardingPhase;
  /**
   * Drives intro matching upstream, and doubles as the effect re-trigger: the
   * hard suppressors below (open app dialog, personalization banner) can block
   * at the moment prefs resolve, and their clearing doesn't change any other
   * dep — without re-evaluating on navigation a deferred popup would never get
   * a second chance this session.
   */
  pathname: string;
  /**
   * The first intro matching the current route that neither the server prefs,
   * the localStorage mirror, nor this session has seen — null when none.
   * Computed by the provider; the mirror can only suppress, never trigger.
   */
  eligibleIntroId: string | null;
  hasUnseenWhatsNew: boolean;
  /** True when another flow (e.g. the personalization banner) owns the screen. */
  suppress: boolean;
  showPageIntro: () => void;
  showWhatsNew: () => void;
}

/**
 * Decides what to show once prefs resolve. Runs only when idle (an open popup
 * owns the screen). Order: hard suppressors → page intro for the current
 * route → What's-New (once/session). New users are stamped caught-up, so they
 * get intros and no What's-New backlog; the two are mutually exclusive in
 * practice.
 */
export function useOnboardingOrchestrator(p: OrchestratorParams): void {
  const whatsNewShownRef = useRef(false);

  useEffect(() => {
    if (p.isLoading || p.phase !== 'idle') return;

    // Hard suppressors.
    if (p.suppress) return;
    if (
      typeof document !== 'undefined' &&
      document.querySelector('[role="dialog"][data-state="open"]')
    ) {
      return;
    }

    // Contextual intro for the page the user just landed on.
    if (p.eligibleIntroId !== null) {
      p.showPageIntro();
      return;
    }

    // What's-New: returning users, once per session.
    if (!whatsNewShownRef.current && p.hasUnseenWhatsNew) {
      whatsNewShownRef.current = true;
      p.showWhatsNew();
      return;
    }
    // showPageIntro / showWhatsNew are intentionally omitted: the provider
    // recreates them every render in lockstep with eligibleIntroId / phase
    // (both deps below), so the effect always re-runs with a fresh closure.
    // Listing them would just re-fire on every render with no behavior change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [p.isLoading, p.phase, p.eligibleIntroId, p.hasUnseenWhatsNew, p.suppress, p.pathname]);
}
