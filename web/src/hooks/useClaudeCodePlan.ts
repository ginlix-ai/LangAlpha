import { useQueryClient } from '@tanstack/react-query';
import { PLAN_OUT_KEY, planOutOf, type PlanClock, type PlanOut } from '@/lib/claudeCodePlan';
import { queryKeys } from '@/lib/queryKeys';
import { useLoadedHomeTimezone } from './useHomeTimezone';
import { useLocale } from './useLocale';
import { useNow } from './useNow';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/** The language and zone a Claude plan's reset is told in (this device's
 *  zone until the profile is read, and for a signed-out reader of a shared
 *  thread). A reader that sets its own `now` when it draws takes these alone,
 *  so it does not turn with the minute. */
export function usePlanReader(): Pick<PlanClock, 'locale' | 'zone'> {
  return { locale: useLocale(), zone: useLoadedHomeTimezone() };
}

/** The clock a Claude plan's reset is told by, turning each minute. */
export function usePlanClock(): PlanClock {
  return { ...usePlanReader(), now: useNow() };
}

/** What Claude Code turns do at a Claude plan's limit, as the user chose.
 *  Stop is the default, so choosing it deletes the key (null). A saved
 *  choice reads the Claude Code plan again: choosing usage credits clears
 *  what the server knew of them, which lifts a hold the picker shows. */
export function usePlanOut() {
  const { preferences } = usePreferences();
  const update = useUpdatePreferences();
  const queryClient = useQueryClient();
  return {
    planOut: planOutOf(preferences?.other_preference),
    set: async (out: PlanOut) => {
      const saved = await update.mutateAsync({ other_preference: { [PLAN_OUT_KEY]: out === 'credits' ? 'credits' : null } });
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCode() });
      return saved;
    },
  };
}
