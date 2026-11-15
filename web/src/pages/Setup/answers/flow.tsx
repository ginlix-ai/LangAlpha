/**
 * Moving through the answer-driven setup: finishing a step (the record says
 * so, then the next step; after the last, the step hands over to /welcome
 * itself), the way back, and the
 * way back to the questions. The first step a surface opens claims setup
 * for it while no surface has.
 */
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { isPlatformMode } from '@/config/hostMode';
import { ONBOARDING_USE_URL } from '@/config/supportLinks';
import { authSessionCheck } from '@/lib/authResets';
import { isDesktopShell, openConsolePage } from '@/lib/desktop';
import { FROM_SETUP, WELCOME_PATH } from '@/pages/Onboarding/welcome/paths';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { SETUP_STEP_PATHS, followingSetupStep, previousSetupStep, type SetupStep } from './setupSteps';
import { useSetupFacts } from './useSetupFacts';

export function useAnswerFlow(step: SetupStep) {
  const setup = useSetupFacts();
  const { rail } = setup;
  const navigate = useNavigate();
  const { update, isUpdating } = useOnboardingProgress();
  const [failed, setFailed] = useState(false);

  // Unclaimed setup (`setup_on` null) is this surface's once one of its steps
  // opens, so the shell of each surface knows whose it is from then on. A
  // refused write is tried again when the window regains focus, and on the
  // next step's mount, while the record stays unclaimed; a claim sent or made
  // holds only for the account it was for.
  const claimed = useRef<(() => boolean) | null>(null);
  const unclaimed = !!setup.progress && setup.answers !== null && setup.progress.setup_on === null
    && !setup.progress.setup_completed_at;
  useEffect(() => {
    if (!unclaimed) return;
    const claim = () => {
      if (claimed.current?.()) return;
      claimed.current = authSessionCheck();
      update({ setup_on: isDesktopShell() ? 'app' : 'browser' }).catch(() => { claimed.current = null; });
    };
    claim();
    window.addEventListener('focus', claim);
    return () => window.removeEventListener('focus', claim);
  }, [unclaimed, update]);

  // A refusal leaves the user on the step, which says so (`finishFailed`).
  const finish = async (): Promise<boolean> => {
    const last = step === 'default';
    setFailed(false);
    try {
      await update(last ? { setup_step: 'default', setup_completed: true } : { setup_step: step });
    } catch {
      setFailed(true);
      return false;
    }
    if (!last) navigate(SETUP_STEP_PATHS[followingSetupStep(rail, step) ?? 'default']);
    return true;
  };

  const previous = previousSetupStep(rail, step);
  // Before the first step: the questions, where this app asks them.
  const backPath = previous ? SETUP_STEP_PATHS[previous] : isPlatformMode ? null : '/setup/use';

  return {
    ...setup,
    first: rail[0] === step,
    finish,
    finishing: isUpdating,
    finishFailed: failed,
    back: backPath ? () => navigate(backPath) : null,
    /** Into /welcome once setup is finished, which opens writing its first line. */
    toWelcome: () => navigate(WELCOME_PATH, { replace: true, state: FROM_SETUP }),
  };
}

/** The way back to the two questions: the hosted onboarding questions on
 *  hosted, this app's on self-hosted. */
export function useChangeAnswers(): () => void {
  const navigate = useNavigate();
  return () => {
    if (ONBOARDING_USE_URL) openConsolePage(ONBOARDING_USE_URL, { inPlace: true });
    else navigate('/setup/use');
  };
}
