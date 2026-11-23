import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { isPlatformMode } from '@/config/hostMode';
import { modelPrefs } from '@/lib/modelPreferences';
import { readOtherPreference } from '@/lib/otherPreference';
import { userLocalStorage, useStorageUser } from '@/lib/userStorage';
import { migrateOnboardingPrefs } from '@/pages/Onboarding/onboardingPrefsSchema';
import { useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { recentThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import { onboardingRoute, type OnboardingRoute } from '@/pages/Setup/answers/gate';
import { useSetupFacts } from '@/pages/Setup/answers/useSetupFacts';
import { usePreferences } from './usePreferences';
import { useSetupGate } from './useSetupGate';
import { useUser } from './useUser';

/**
 * Where onboarding stands for the signed-in account (`onboardingRoute`), the
 * one decision the app shell, /setup and the setup pages read. A read that
 * failed counts as onboarded, so it sends no one anywhere; so does a fact
 * setup resumes on that could not be read, which the setup pages hold on.
 *
 * The account setup stamp and `plan_step_done_at` come from the record,
 * which is read fresh: a preferences read cached before the hosted onboarding
 * set them would not show them.
 *
 * Preferences carry the other onboarded signals, and Settings' reset deletes
 * them. So where none is left, an account that already has a computer or a
 * thread counts as onboarded too, read only then: an existing account is
 * never sent to the hosted onboarding, or the questions, in the middle
 * of a session or on its first visit to the web. A computer setup paired from
 * this Mac is not one of those.
 *
 * Once decided for the account, it stays decided while a read is taken again
 * (a refocus retrying one that failed): going back to loading would unmount
 * the app, and what the user has open in it.
 */
export function useOnboardingRoute(): OnboardingRoute {
  const route = useReadOnboardingRoute();
  const { user } = useUser();
  const by = user?.user_id ?? null;
  const [decided, setDecided] = useState<{ by: string | null; route: OnboardingRoute } | null>(null);
  if (route.kind === 'loading') return decided?.by === by ? decided.route : route;
  if (decided?.by !== by || JSON.stringify(decided.route) !== JSON.stringify(route)) setDecided({ by, route });
  return route;
}

const DONE_KEY = 'la_onboarding_done';
const DONE: OnboardingRoute = { kind: 'done' };

/**
 * The route the app shell renders on. The reads that decide it are a full
 * round trip, and the shell holds every page and its requests behind them, so
 * an account whose onboarding was done on its last visit in this browser
 * renders the app while they run again. Should they now decide otherwise (a
 * provider removed, a reset in another tab), the shell sends the visit there
 * as it would have. The setup pages and the onboarding provider read
 * `useOnboardingRoute`, never this: they act on the decision itself.
 */
export function useShellOnboardingRoute(): OnboardingRoute {
  const route = useOnboardingRoute();
  const storageUser = useStorageUser();
  const remembered = useMemo(() => !!storageUser && userLocalStorage.getItem(DONE_KEY) === '1', [storageUser]);
  useEffect(() => {
    if (!storageUser || route.kind === 'loading') return;
    if (route.kind === 'done') userLocalStorage.setItem(DONE_KEY, '1');
    else userLocalStorage.removeItem(DONE_KEY);
  }, [route.kind, storageUser]);
  return route.kind === 'loading' && remembered ? DONE : route;
}

function useReadOnboardingRoute(): OnboardingRoute {
  const { user, isLoading: userLoading, isError: userFailed } = useUser();
  const { preferences, isLoading: prefsLoading, isLoaded: prefsLoaded } = usePreferences();
  const setup = useSetupFacts();
  const answered = !!setup.answers;
  // The older provider setup only for an account read to have no answers:
  // a record, preferences or profile read that failed sends no one there either.
  const userUnread = userFailed && !user;
  const unread = (setup.progressFailed && !setup.progress) || !prefsLoaded || userUnread;
  const legacy = useSetupGate({ enabled: !setup.isLoading && !answered && !unread });

  const onboarding = migrateOnboardingPrefs(readOtherPreference(preferences, 'onboarding'));
  const setupCompleted = !!setup.progress?.setup_completed_at;
  const signalled = !prefsLoaded || setup.progressFailed || setup.factsFailed || userUnread
    || !!setup.progress?.account_setup_completed_at
    || !!modelPrefs(preferences).preferred_model
    || onboarding.firstRunAt !== null
    || onboarding.welcomeSkippedAt !== null
    || user?.onboarding_completed === true
    || setupCompleted
    || !!setup.progress?.plan_step_done_at;
  const askUsage = !userLoading && !prefsLoading && !setup.isLoading && !signalled;
  const computers = useComputers({ enabled: askUsage });
  const threads = useQuery({ ...recentThreadsQuery(1), enabled: askUsage });
  const pairedHere = setup.progress?.this_mac === 'paired' && computers.data?.computers.some((c) => c.ssh?.helper) ? 1 : 0;
  const used = (computers.data?.computers.length ?? 0) > pairedHere || (threads.data?.threads.length ?? 0) > 0
    || computers.isError || threads.isError;

  if (userLoading || prefsLoading || setup.isLoading || legacy.isLoading) return { kind: 'loading' };
  if (askUsage && (computers.isLoading || threads.isLoading)) return { kind: 'loading' };
  return onboardingRoute({
    platform: isPlatformMode,
    onboarded: signalled || used,
    hasAnswers: answered,
    setupCompleted,
    setupOn: setup.progress?.setup_on ?? null,
    next: setup.factsFailed ? 'done' : setup.next,
    needsProvider: legacy.needsSetup,
    unpaired: !!setup.progress && setup.progress.this_mac === null,
  });
}
